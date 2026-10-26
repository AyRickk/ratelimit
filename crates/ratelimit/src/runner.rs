//! Starting the service from its settings, as envoyproxy/ratelimit's runner does
//! (`src/service_cmd/runner/runner.go`, `src/server/server_impl.go`), with its log lines.
//! Every failure upstream ends the process with is returned, with upstream's text, as a
//! `logger.Fatal` or as what upstream panics on.

use crate::grpc;
use crate::health::{self, HealthChecker, HealthService};
use crate::limiter::SystemClock;
use crate::log::{self, Failure};
use crate::redis::{RedisCache, RedisConnection, StartupBackoff};
use crate::runtime::{self, FileProvider};
use crate::service::RateLimitService;
use crate::settings::{Lookup, Settings};
use crate::stats::{self, Stats};
use crate::xds::XdsProvider;
use std::sync::{Arc, Mutex};

/// `configureLogger`: the level (a fatal error, written before the format is set), then
/// the format.
fn configure_logger(settings: &Settings) -> Result<(), Failure> {
    let level = log::Level::parse(&settings.log_level)
        .map_err(|e| Failure::Fatal(format!("Could not parse log level. {e}\n")))?;
    log::configure(level, &settings.log_format);
    Ok(())
}

/// The signals the service stops on (SIGINT, SIGTERM, SIGHUP, as the runner's handler), and
/// SIGTERM alone (which the health checker watches).
struct Signals {
    stop: tokio::sync::watch::Receiver<bool>,
    sigterm: tokio::sync::watch::Receiver<bool>,
}

fn signals() -> Result<Signals, String> {
    use tokio::signal::unix::{SignalKind, signal};
    let (stop_tx, stop) = tokio::sync::watch::channel(false);
    let (sigterm_tx, sigterm) = tokio::sync::watch::channel(false);
    let listen = |kind| signal(kind).map_err(|e| e.to_string());
    let mut interrupt = listen(SignalKind::interrupt())?;
    let mut terminate = listen(SignalKind::terminate())?;
    let mut hangup = listen(SignalKind::hangup())?;
    tokio::spawn(async move {
        // Go's names of the signals.
        let name = tokio::select! {
            _ = interrupt.recv() => "interrupt",
            _ = terminate.recv() => {
                let _ = sigterm_tx.send(true);
                "terminated"
            }
            _ = hangup.recv() => "hangup",
        };
        log::info(&format!("Received signal {name}, initiating shutdown"));
        let _ = stop_tx.send(true);
        // Later SIGTERMs still reach the health checker.
        while terminate.recv().await.is_some() {
            let _ = sigterm_tx.send(true);
        }
    });
    Ok(Signals { stop, sigterm })
}

/// Resolves once `receiver` turns true.
async fn shutting_down(mut receiver: tokio::sync::watch::Receiver<bool>) {
    let _ = receiver.wait_for(|down| *down).await;
}

/// `poolStats`: `cx_total` counts the connections made (the pool's first ones, then each
/// reconnection), `cx_local_close` those lost, `cx_active` those open. Made before the pool,
/// as upstream makes it before its startup loop, so its gauge is flushed meanwhile.
struct PoolStats {
    active: crate::stats::Gauge,
    total: crate::stats::Counter,
    local_close: crate::stats::Counter,
}

impl PoolStats {
    fn new(stats: &Stats, scope: &crate::stats::Scope) -> Self {
        Self {
            active: stats.scope_gauge(scope, "cx_active"),
            total: stats.scope_counter(scope, "cx_total"),
            local_close: stats.scope_counter(scope, "cx_local_close"),
        }
    }

    /// `poolTrace` for `pool`: each client's connection is followed from its own events (all
    /// open at first); a failed reconnection, which names no server, changes nothing. With
    /// `health` (`REDIS_HEALTH_CHECK_ACTIVE_CONNECTION`), Redis is healthy again on each new
    /// connection, and unhealthy once none is open.
    fn follow(&self, pool: &fred::clients::Pool, health: Option<Arc<HealthChecker>>) {
        use fred::interfaces::EventInterface;
        use tokio::sync::broadcast::error::RecvError;
        let clients = pool.clients().len();
        self.total.add(clients as u64);
        self.active.add(clients as u64);
        let connected = Arc::new(Mutex::new(vec![true; clients]));
        // `hadConnError`: a failed connection since the last one made.
        let failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        for (index, client) in pool.clients().iter().enumerate() {
            let (mut reconnected, mut errors) = (client.reconnect_rx(), client.error_rx());
            let (connected, health) = (connected.clone(), health.clone());
            let failed = failed.clone();
            let (active, total) = (self.active.clone(), self.total.clone());
            let local_close = self.local_close.clone();
            tokio::spawn(async move {
                loop {
                    let opened = tokio::select! {
                        event = reconnected.recv() => event.map(|_| true),
                        event = errors.recv() => match event {
                            Ok((error, None)) => {
                                failed.store(true, std::sync::atomic::Ordering::SeqCst);
                                let error = error.details().to_string();
                                log::error(&format!("creating redis connection error : {error}"));
                                continue;
                            }
                            event => event.map(|_| false),
                        },
                    };
                    // Counted and reported under the lock, so that health follows the counts.
                    let mut connected = connected.lock().unwrap();
                    match opened {
                        Ok(true) => {
                            total.inc();
                            if failed.swap(false, std::sync::atomic::Ordering::SeqCst) {
                                log::info("redis connection re-established after previous error");
                            }
                            // A connection replaced without its loss seen: that one is closed.
                            match std::mem::replace(&mut connected[index], true) {
                                true => local_close.inc(),
                                false => active.add(1),
                            }
                            if let Some(health) = &health {
                                let _ = health.ok(health::REDIS);
                            }
                        }
                        Ok(false) => {
                            if !std::mem::replace(&mut connected[index], false) {
                                continue;
                            }
                            active.sub(1);
                            local_close.inc();
                            if let Some(health) = &health
                                && active.value() == 0
                            {
                                let _ = health.fail(health::REDIS);
                            }
                        }
                        Err(RecvError::Closed) => return,
                        // The events missed cannot be told apart.
                        Err(RecvError::Lagged(_)) => {}
                    }
                }
            });
        }
    }
}

/// Runs the service until it is told to stop (then stops gracefully): stats, config, Redis,
/// then serving.
pub async fn run(settings: Settings, lookup: Lookup<'_>) -> Result<(), Failure> {
    let SettingsTls {
        redis: redis_tls,
        grpc: grpc_tls,
        xds: xds_tls,
    } = settings_tls(&settings)?;
    // NewRunner: the logger, then the stats sink and its flushes.
    configure_logger(&settings)?;
    let sink = stats::select_sink(&settings.sinks(lookup)?).map_err(stats::sink_failure)?;
    log::info(&format!(
        "Stats flush interval: {}",
        crate::go::format_duration(settings.stats_flush_interval)
    ));
    let interval = settings.stats_flush_interval()?;
    let stats = Arc::new(Stats::new(&settings.extra_tags));
    stats::flush_every(stats.clone(), sink, interval);

    // Run: the signals, then tracing.
    let signals = signals()?;
    if settings.tracing_enabled {
        crate::trace::init(&crate::trace::Settings {
            protocol: settings.tracing_exporter_protocol.clone(),
            service_name: settings.tracing_service_name.clone(),
            service_namespace: settings.tracing_service_namespace.clone(),
            service_instance_id: settings.tracing_service_instance_id.clone(),
            sampling_rate: settings.tracing_sampling_rate,
        })?;
    } else {
        log::info("Tracing disabled");
    }

    // NewServer: the gRPC server's cert provider, the config provider, the health checker
    // (SIGTERM makes it fail).
    // grpc-go's `envconfig`: enforced unless `false`.
    let enforce_alpn =
        !lookup("GRPC_ENFORCE_ALPN_ENABLED").is_some_and(|v| v.eq_ignore_ascii_case("false"));
    let grpc_tls = grpc_tls
        .map(|tls| tls.acceptor(&stats, enforce_alpn))
        .transpose()?;
    let provider = match settings.config_type.as_str() {
        "FILE" => Provider::File(runtime::loader(&settings, &stats)?),
        "GRPC_XDS_SOTW" => {
            let reloaded = settings.clone();
            Provider::Xds(XdsProvider::start(
                &settings,
                xds_tls,
                enforce_alpn,
                move || settings_tls(&reloaded).map(drop),
            ))
        }
        other => {
            return Err(Failure::Fatal(format!(
                "Invalid setting for ConfigType: {other}"
            )));
        }
    };
    // runner.go: the server's reporter is made before the server.
    let server_stats = crate::stats::ServerStats::new(stats.clone());
    let health_service = HealthService::default().with_stats(server_stats.clone());
    // NewServer's own scope (that of the `go.*` and `localcache.*` gauges): its tags count in
    // `reserved_tag` like every tagged scope's.
    let server_scope = stats.scope_with_tags("ratelimit");
    let with_config = settings.healthy_with_at_least_one_config_loaded;
    let health = Arc::new(HealthChecker::new(health_service.clone(), with_config));
    let sigterm_health = health.clone();
    let sigterm = signals.sigterm.clone();
    tokio::spawn(async move {
        shutting_down(sigterm).await;
        let _ = sigterm_health.fail(health::SIGTERM);
    });

    // runner.go: the local cache, whose counts NewServer reports as gauges.
    let local_cache_bytes = settings.redis_cache().local_cache_size_in_bytes;
    let local_cache = (local_cache_bytes > 0)
        .then(|| Arc::new(crate::local_cache::LocalCache::new(local_cache_bytes)));
    if let Some(local_cache) = &local_cache {
        local_cache_stats(
            &stats,
            &server_scope.scope("localcache"),
            local_cache.clone(),
        );
    }

    // createLimiter: Redis, healthy once connected.
    // The per-second pool comes first, as in `NewRateLimiterCacheImplFromSettings`.
    let (per_second_connection, connection) = match settings.backend_type.as_str() {
        "redis" | "" => {
            // fred would open a Unix socket in clear: TLS over one is refused.
            let with_tls = |mut connection: RedisConnection, tls: bool| {
                connection.tls = redis_tls.clone().filter(|_| tls);
                match (connection.tls.is_some(), connection.network.as_str()) {
                    (true, "unix") => Err(format!(
                        "{}_TLS over a Unix socket is not supported",
                        connection.prefix
                    )),
                    _ => Ok(connection),
                }
            };
            (
                settings
                    .redis_persecond
                    .then(|| RedisConnection::per_second_from_settings(&settings))
                    .transpose()?
                    .map(|c| with_tls(c, settings.redis_persecond_tls))
                    .transpose()?,
                with_tls(
                    RedisConnection::from_settings(&settings)?,
                    settings.redis_tls,
                )?,
            )
        }
        "memcache" => {
            return Err("BACKEND_TYPE=memcache is not supported yet"
                .to_string()
                .into());
        }
        other => {
            return Err(Failure::Fatal(format!(
                "Invalid setting for BackendType: {other}"
            )));
        }
    };
    // Each pool connects as `newClientImpl` does, the per-second one first, and makes Redis
    // healthy once connected.
    let active_health = settings.redis_health_check_active_connection;
    let backoff = StartupBackoff::from_settings(&settings);
    // The pool's stats exist before it connects (`newPoolStats`), so are flushed meanwhile.
    let mut per_second = None;
    if let Some(connection) = per_second_connection {
        let pool_stats = PoolStats::new(&stats, &server_scope.scope("redis_per_second_pool"));
        let pool = connection
            .connect(&backoff, shutting_down(signals.stop.clone()))
            .await?;
        pool_stats.follow(&pool, active_health.then(|| health.clone()));
        let _ = health.ok(health::REDIS);
        per_second = Some(pool);
    }
    let pool_stats = PoolStats::new(&stats, &server_scope.scope("redis_pool"));
    let pool = connection
        .connect(&backoff, shutting_down(signals.stop.clone()))
        .await?;
    pool_stats.follow(&pool, active_health.then(|| health.clone()));
    let _ = health.ok(health::REDIS);
    let clock = Arc::new(SystemClock);
    let mut cache = RedisCache::with_local_cache(
        pool,
        clock.clone(),
        stats.clone(),
        settings.redis_cache(),
        local_cache,
    );
    if let Some(pool) = per_second {
        cache = cache.with_per_second(pool);
    }

    // NewService: loads each config the provider sends (with
    // HEALTHY_WITH_AT_LEAST_ONE_CONFIG_LOADED, healthy only with domains); waits for the first
    // one unless FORCE_START_WITHOUT_INITIAL_CONFIG.
    let service = Arc::new(RateLimitService::new(
        cache,
        clock,
        stats.clone(),
        settings.service(),
    ));
    let (first_tx, first_rx) = tokio::sync::oneshot::channel();
    let first = std::sync::Mutex::new(Some(first_tx));
    let (updated, config_health) = (service.clone(), health.clone());
    // Written before the provider's first event, as `NewService` waits for it.
    let wait = !settings.force_start_without_initial_config;
    if wait {
        log::info("Waiting for initial ratelimit config update event");
    } else {
        // `NewService`'s update loop, which takes every event.
        crate::debug!("Waiting for config update event");
    }
    let reread = settings.clone();
    let update =
        move |config: Result<crate::config::RateLimitConfig, crate::config::ConfigError>| {
            let mut first = first.lock().unwrap();
            // The update loop takes every event but the initial one `NewService` waits for.
            let looped = !wait || first.is_none();
            if looped {
                crate::debug!("Setting config retrieved from config provider");
            }
            // `SetConfig`.
            if let (true, Ok(config)) = (with_config, &config) {
                let _ = match config.is_empty_domains() {
                    false => config_health.ok(health::CONFIG),
                    true => config_health.fail(health::CONFIG),
                };
            }
            // `SetConfig` reads the settings again: a TLS file gone panics.
            if config.is_ok()
                && let Err(e) = settings_tls(&reread)
            {
                log::panic_exit(&e);
            }
            updated.update_config(config);
            if looped {
                crate::debug!("Waiting for config update event");
            }
            if let Some(first) = first.take() {
                let _ = first.send(());
            }
        };
    let (_file_provider, xds) = match provider {
        Provider::File(loader) => (
            Some(FileProvider::with_loader(loader, &settings, update)),
            None,
        ),
        Provider::Xds(mut xds) => {
            xds.deliver(update);
            (None, Some(xds))
        }
    };
    if wait {
        let _ = first_rx.await;
        log::info("Successfully loaded the initial ratelimit configs");
        // The update loop starts.
        crate::debug!("Waiting for config update event");
    }

    // Start: gRPC (with its keepalive settings and the health service), then HTTP. On a
    // signal, gRPC stops gracefully, then HTTP closes.
    let mut builder = tonic::transport::Server::builder();
    let age = crate::settings::duration(settings.grpc_max_connection_age);
    let grace = crate::settings::duration(settings.grpc_max_connection_age_grace);
    if !age.is_zero() {
        builder = builder.max_connection_age(age);
    }
    if !grace.is_zero() {
        builder = builder.max_connection_age_grace(grace);
    }
    // otelgrpc's handler, on every service.
    let uds = (!settings.grpc_uds.is_empty()).then(|| settings.grpc_uds.clone());
    let in_flight = crate::drain::InFlight::default();
    let server = builder
        .layer(grpc::Requests)
        .layer(crate::trace::grpc::Layer::new(uds))
        .layer(in_flight.clone())
        .add_service(health_service.server())
        .add_service(grpc::server(service.clone(), server_stats));
    // handleGracefulShutdown: on a signal, the line, then the server stops (its listeners
    // closed, its connections drained).
    let (stopped_tx, server_stopping) = tokio::sync::watch::channel(false);
    {
        let stopping = signals.stop.clone();
        tokio::spawn(async move {
            shutting_down(stopping).await;
            log::info("Context cancelled, stopping server");
            let _ = stopped_tx.send(true);
        });
    }
    let stop = shutting_down(server_stopping.clone());
    // Start: the debug port first; a failure to listen there is logged, not fatal.
    let (grpc_done, grpc_stopped) = tokio::sync::watch::channel(false);
    let debug_address = crate::net::join_host_port(&settings.debug_host, settings.debug_port);
    log::warn(&format!("Listening for debug on '{debug_address}'"));
    let debug = match crate::net::listen(&debug_address, true).await {
        Ok(listener) => {
            // Go names the socket's own address: `[::]:6070` for the dual-stack wildcard.
            let address = listener
                .local_addr()
                .map_or(debug_address.clone(), |a| a.to_string());
            let stopped = stopped(grpc_stopped.clone());
            let service = service.clone();
            Some(tokio::spawn(async move {
                crate::debug::serve(listener, service, stopped).await;
                // `http.Serve` returns once its listener is closed.
                log::info(&format!(
                    "Failed to start debug server 'accept tcp {address}: use of closed network connection'"
                ));
            }))
        }
        Err(e) => {
            let e = crate::net::listen_error("tcp", &debug_address, &e);
            log::error(&format!("Failed to open debug HTTP listener: '{e}'"));
            None
        }
    };
    // The server on its listener's connections, over TLS or not.
    // grpc-go's drain: past its ping's timeout, connections left are closed once idle.
    let drained = {
        let (stopping, in_flight) = (shutting_down(server_stopping.clone()), in_flight.clone());
        async move {
            stopping.await;
            tokio::time::sleep(crate::drain::PING_TIMEOUT).await;
            in_flight.idle().await;
        }
    };
    macro_rules! serve {
        ($server:expr, $incoming:expr, $stop:expr) => {{
            let (server, incoming, stop) = ($server, $incoming, $stop);
            Box::pin(async move {
                tokio::select! {
                    served = server.serve_with_incoming_shutdown(incoming, stop) => {
                        served.map_err(|e| e.to_string())
                    }
                    () = drained => Ok(()),
                }
            })
        }};
    }
    let grpc_served: std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), String>> + Send>,
    > = if !settings.grpc_uds.is_empty() {
        let address = settings.grpc_uds.clone();
        log::warn(&format!("Listening for gRPC on '{address}'"));
        let listener = tokio::net::UnixListener::bind(&address).map_err(|e| {
            let e = crate::net::listen_error("unix", &address, &e);
            Failure::Fatal(format!("Failed to listen for gRPC on '{address}': {e}"))
        })?;
        // Go's Unix listener removes its socket file as it closes.
        let stopping = shutting_down(server_stopping.clone());
        let socket = address.clone();
        let incoming = crate::net::closed_on(
            tokio_stream::wrappers::UnixListenerStream::new(listener),
            async move {
                stopping.await;
                let _ = std::fs::remove_file(socket);
            },
        );
        match &grpc_tls {
            Some(tls) => serve!(server, tls.incoming(incoming), stop),
            None => serve!(server, incoming, stop),
        }
    } else {
        let address = crate::net::join_host_port(&settings.grpc_host, settings.grpc_port);
        log::warn(&format!("Listening for gRPC on '{address}'"));
        let listener = crate::net::listen(&address, true).await.map_err(|e| {
            let e = crate::net::listen_error("tcp", &address, &e);
            Failure::Fatal(format!("Failed to listen for gRPC on '{address}': {e}"))
        })?;
        let incoming = crate::net::closed_on(
            tokio_stream::wrappers::TcpListenerStream::new(listener),
            shutting_down(server_stopping.clone()),
        );
        match &grpc_tls {
            Some(tls) => serve!(server, tls.incoming(incoming), stop),
            None => serve!(server, incoming, stop),
        }
    };
    let http_address = crate::net::join_host_port(&settings.host, settings.port);
    log::warn(&format!("Listening for HTTP on '{http_address}'"));
    let http_listener = crate::net::listen(&http_address, true).await.map_err(|e| {
        let e = crate::net::listen_error("tcp", &http_address, &e);
        Failure::Fatal(format!("Failed to open HTTP listener: '{e}'"))
    })?;
    let http = tokio::spawn(crate::http::serve(
        http_listener,
        health,
        service,
        stopped(grpc_stopped),
    ));
    // Stop: gRPC stops gracefully, then the debug port and the HTTP server close.
    let served = grpc_served.await;
    let _ = grpc_done.send(true);
    if let Some(debug) = debug {
        let _ = debug.await;
    }
    let _ = http.await;
    // Then the config provider stops.
    if let Some(xds) = xds {
        xds.stop().await;
    }
    crate::trace::shutdown().await;
    served.map_err(Failure::from)
}

/// The TLS files `NewSettings` loads: Redis's, the gRPC server's, then the xDS client's (and
/// Memcached's, not supported yet); an error is what upstream panics with.
struct SettingsTls {
    redis: Option<crate::redis::Tls>,
    grpc: Option<crate::grpc_tls::ServerTls>,
    xds: Option<rustls::ClientConfig>,
}

fn settings_tls(settings: &Settings) -> Result<SettingsTls, String> {
    Ok(SettingsTls {
        redis: crate::redis::tls::from_settings(settings)?.map(crate::redis::Tls),
        grpc: crate::grpc_tls::from_settings(settings)?,
        // `ConfigGrpcXdsServerTlsConfig`.
        xds: settings
            .config_grpc_xds_server_use_tls
            .then(|| {
                crate::tls::server_ca_client(
                    &settings.config_grpc_xds_client_tls_cert,
                    &settings.config_grpc_xds_client_tls_key,
                    &settings.config_grpc_xds_server_tls_cacert,
                    false,
                )
            })
            .transpose()?,
    })
}

/// The config provider `CONFIG_TYPE` names.
enum Provider {
    File(runtime::Loader),
    Xds(XdsProvider),
}

/// Resolves once gRPC has stopped.
async fn stopped(mut grpc_stopped: tokio::sync::watch::Receiver<bool>) {
    let _ = grpc_stopped.wait_for(|stopped| *stopped).await;
}

/// `NewLocalCacheStats`: freecache's counts as gauges, read before each flush.
fn local_cache_stats(
    stats: &Arc<Stats>,
    scope: &crate::stats::Scope,
    cache: Arc<crate::local_cache::LocalCache>,
) {
    let gauge = |name| stats.scope_gauge(scope, name);
    let gauges = [
        gauge("evacuateCount"),
        gauge("expiredCount"),
        gauge("entryCount"),
        gauge("averageAccessTime"),
        gauge("hitCount"),
        gauge("missCount"),
        gauge("lookupCount"),
        gauge("overwriteCount"),
    ];
    stats.add_generator(move || {
        let c = cache.counts();
        let counts = [
            c.evacuate,
            c.expired,
            c.entries,
            c.average_access_time,
            c.hits,
            c.misses,
            c.lookups,
            c.overwrites,
        ];
        // `uint64(...)` of each count.
        for (gauge, count) in gauges.iter().zip(counts) {
            gauge.set(count as u64);
        }
    });
}

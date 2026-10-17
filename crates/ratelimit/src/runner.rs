//! Starting the service from its settings, as envoyproxy/ratelimit's runner does
//! (`src/service_cmd/runner/runner.go`, `src/server/server_impl.go`). Every failure upstream
//! ends the process with is returned as an error, with upstream's text.

use crate::grpc;
use crate::health::{self, HealthChecker, HealthService};
use crate::limiter::SystemClock;
use crate::redis::{RedisCache, RedisConnection, StartupBackoff};
use crate::runtime::{self, FileProvider};
use crate::service::RateLimitService;
use crate::settings::{Lookup, Settings};
use crate::stats::{self, Stats};
use std::sync::{Arc, Mutex};

/// `logrus.ParseLevel`.
fn check_log_level(level: &str) -> Result<(), String> {
    match level.to_lowercase().as_str() {
        "panic" | "fatal" | "error" | "warn" | "warning" | "info" | "debug" | "trace" => Ok(()),
        _ => Err(format!(
            "Could not parse log level. not a valid logrus Level: {}",
            crate::go::quote(level)
        )),
    }
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
        tokio::select! {
            _ = interrupt.recv() => {}
            _ = terminate.recv() => {
                let _ = sigterm_tx.send(true);
            }
            _ = hangup.recv() => {}
        }
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

/// `poolStats` and `poolTrace`: `cx_total` counts the connections made (the pool's first ones,
/// then each reconnection), `cx_local_close` those lost, `cx_active` those open. With
/// `REDIS_HEALTH_CHECK_ACTIVE_CONNECTION`, Redis is healthy again on each new connection, and
/// unhealthy once none is open. Each client's connection is followed from its own events
/// (all open at first); a failed reconnection, which names no server, changes nothing.
fn pool_stats(
    pool: &fred::clients::Pool,
    stats: &Stats,
    scope: &crate::stats::Scope,
    health: Option<Arc<HealthChecker>>,
) {
    use fred::interfaces::EventInterface;
    use tokio::sync::broadcast::error::RecvError;
    let active = stats.scope_gauge(scope, "cx_active");
    let total = stats.scope_counter(scope, "cx_total");
    let local_close = stats.scope_counter(scope, "cx_local_close");
    let clients = pool.clients().len();
    total.add(clients as u64);
    active.add(clients as u64);
    let connected = Arc::new(Mutex::new(vec![true; clients]));
    for (index, client) in pool.clients().iter().enumerate() {
        let (mut reconnected, mut errors) = (client.reconnect_rx(), client.error_rx());
        let (connected, health) = (connected.clone(), health.clone());
        let (active, total, local_close) = (active.clone(), total.clone(), local_close.clone());
        tokio::spawn(async move {
            loop {
                let opened = tokio::select! {
                    event = reconnected.recv() => event.map(|_| true),
                    event = errors.recv() => match event {
                        Ok((_, None)) => continue,
                        event => event.map(|_| false),
                    },
                };
                match opened {
                    Ok(true) => {
                        total.inc();
                        // A connection replaced without its loss seen: that one is closed.
                        match std::mem::replace(&mut connected.lock().unwrap()[index], true) {
                            true => local_close.inc(),
                            false => active.add(1),
                        }
                        if let Some(health) = &health {
                            let _ = health.ok(health::REDIS);
                        }
                    }
                    Ok(false) => {
                        if !std::mem::replace(&mut connected.lock().unwrap()[index], false) {
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

/// Runs the service until it is told to stop (then stops gracefully): stats, config, Redis,
/// then serving.
pub async fn run(settings: Settings, lookup: Lookup<'_>) -> Result<(), String> {
    let signals = signals()?;
    // NewRunner: the logger, then the stats sink and its flushes.
    check_log_level(&settings.log_level)?;
    let sink = stats::select_sink(&settings.sinks(lookup)?)?;
    let interval = settings.stats_flush_interval()?;
    let stats = Arc::new(Stats::new(&settings.extra_tags));
    stats::flush_every(stats.clone(), sink, interval);

    crate::settings::refuse_unsupported(&[
        (settings.tracing_enabled, "TRACING_ENABLED"),
        (settings.grpc_server_use_tls, "GRPC_SERVER_USE_TLS"),
    ])?;

    // NewServer: the config provider, the health checker (SIGTERM makes it fail).
    let loader = match settings.config_type.as_str() {
        "FILE" => runtime::loader(&settings, &stats)?,
        "GRPC_XDS_SOTW" => return Err("CONFIG_TYPE=GRPC_XDS_SOTW is not supported yet".into()),
        other => return Err(format!("Invalid setting for ConfigType: {other}")),
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

    // createLimiter: Redis, healthy once connected.
    let connection = match settings.backend_type.as_str() {
        "redis" | "" => RedisConnection::from_settings(&settings)?,
        "memcache" => return Err("BACKEND_TYPE=memcache is not supported yet".into()),
        other => return Err(format!("Invalid setting for BackendType: {other}")),
    };
    let pool = connection
        .connect(
            &StartupBackoff::from_settings(&settings),
            shutting_down(signals.stop.clone()),
        )
        .await?;
    let active_health = settings.redis_health_check_active_connection;
    let scope = server_scope.scope("redis_pool");
    pool_stats(&pool, &stats, &scope, active_health.then(|| health.clone()));
    let _ = health.ok(health::REDIS);
    let clock = Arc::new(SystemClock);
    let cache = RedisCache::new(pool, clock.clone(), stats.clone(), settings.redis_cache());
    if let Some(local_cache) = cache.local_cache() {
        local_cache_stats(&stats, &server_scope.scope("localcache"), local_cache);
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
    let _provider = FileProvider::with_loader(loader, &settings, move |config| {
        if let (true, Ok(config)) = (with_config, &config) {
            let _ = match config.is_empty_domains() {
                false => config_health.ok(health::CONFIG),
                true => config_health.fail(health::CONFIG),
            };
        }
        updated.update_config(config);
        if let Some(first) = first.lock().unwrap().take() {
            let _ = first.send(());
        }
    });
    if !settings.force_start_without_initial_config {
        let _ = first_rx.await;
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
    let server = builder
        .add_service(health_service.server())
        .add_service(grpc::server(service.clone(), server_stats));
    let stop = shutting_down(signals.stop);
    // Start: the debug port first; a failure to listen there is not fatal (upstream logs it).
    let (grpc_done, grpc_stopped) = tokio::sync::watch::channel(false);
    let debug_address = crate::net::join_host_port(&settings.debug_host, settings.debug_port);
    let debug = match crate::net::listen(&debug_address, true).await {
        Ok(listener) => Some(tokio::spawn(crate::debug::serve(
            listener,
            service.clone(),
            stopped(grpc_stopped.clone()),
        ))),
        Err(_) => None,
    };
    let grpc_served: std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), String>> + Send>,
    > = if !settings.grpc_uds.is_empty() {
        let address = settings.grpc_uds.clone();
        let listener = tokio::net::UnixListener::bind(&address)
            .map_err(|e| format!("Failed to listen for gRPC on '{address}': {e}"))?;
        let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);
        Box::pin(async move {
            server
                .serve_with_incoming_shutdown(incoming, stop)
                .await
                .map_err(|e| e.to_string())
        })
    } else {
        let address = crate::net::join_host_port(&settings.grpc_host, settings.grpc_port);
        let listener = crate::net::listen(&address, true)
            .await
            .map_err(|e| format!("Failed to listen for gRPC on '{address}': {e}"))?;
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        Box::pin(async move {
            server
                .serve_with_incoming_shutdown(incoming, stop)
                .await
                .map_err(|e| e.to_string())
        })
    };
    let http_address = crate::net::join_host_port(&settings.host, settings.port);
    let http_listener = crate::net::listen(&http_address, true)
        .await
        .map_err(|e| format!("Failed to open HTTP listener: '{e}'"))?;
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
    served
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
        let counts = [
            cache.evacuate_count(),
            cache.expired_count(),
            cache.entry_count(),
            cache.average_access_time(),
            cache.hit_count(),
            cache.miss_count(),
            cache.lookup_count(),
            cache.overwrite_count(),
        ];
        // `uint64(...)` of each count.
        for (gauge, count) in gauges.iter().zip(counts) {
            gauge.set(count as u64);
        }
    });
}

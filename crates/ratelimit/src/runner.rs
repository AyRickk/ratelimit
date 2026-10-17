//! Starting the service from its settings, as envoyproxy/ratelimit's runner does
//! (`src/service_cmd/runner/runner.go`, `src/server/server_impl.go`). Every failure upstream
//! ends the process with is returned as an error, with upstream's text.

use crate::grpc;
use crate::limiter::SystemClock;
use crate::redis::{RedisCache, RedisConnection, StartupBackoff};
use crate::runtime::{self, FileProvider};
use crate::service::RateLimitService;
use crate::settings::{Lookup, Settings};
use crate::stats::{self, Stats};
use std::sync::Arc;

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

/// Tells when the process gets SIGINT, SIGTERM or SIGHUP, as the runner's signal handler.
fn shutdown_signal() -> Result<tokio::sync::watch::Receiver<bool>, String> {
    use tokio::signal::unix::{SignalKind, signal};
    let (sender, receiver) = tokio::sync::watch::channel(false);
    let listen = |kind| signal(kind).map_err(|e| e.to_string());
    let mut interrupt = listen(SignalKind::interrupt())?;
    let mut terminate = listen(SignalKind::terminate())?;
    let mut hangup = listen(SignalKind::hangup())?;
    tokio::spawn(async move {
        tokio::select! {
            _ = interrupt.recv() => {}
            _ = terminate.recv() => {}
            _ = hangup.recv() => {}
        }
        let _ = sender.send(true);
    });
    Ok(receiver)
}

/// Resolves once `receiver` says the service is shutting down.
async fn shutting_down(mut receiver: tokio::sync::watch::Receiver<bool>) {
    let _ = receiver.wait_for(|down| *down).await;
}

/// Runs the service until it is told to stop (then stops gracefully): stats, config, Redis,
/// then serving.
pub async fn run(settings: Settings, lookup: Lookup<'_>) -> Result<(), String> {
    let shutdown = shutdown_signal()?;
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

    // NewServer: the config provider.
    let loader = match settings.config_type.as_str() {
        "FILE" => runtime::loader(&settings, &stats)?,
        "GRPC_XDS_SOTW" => return Err("CONFIG_TYPE=GRPC_XDS_SOTW is not supported yet".into()),
        other => return Err(format!("Invalid setting for ConfigType: {other}")),
    };

    // createLimiter.
    let connection = match settings.backend_type.as_str() {
        "redis" | "" => RedisConnection::from_settings(&settings)?,
        "memcache" => return Err("BACKEND_TYPE=memcache is not supported yet".into()),
        other => return Err(format!("Invalid setting for BackendType: {other}")),
    };
    let pool = connection
        .connect(
            &StartupBackoff::from_settings(&settings),
            shutting_down(shutdown.clone()),
        )
        .await?;
    let clock = Arc::new(SystemClock);
    let cache = RedisCache::new(pool, clock.clone(), stats.clone(), settings.redis_cache());

    // NewService: loads each config the provider sends; waits for the first one unless
    // FORCE_START_WITHOUT_INITIAL_CONFIG.
    let service = Arc::new(RateLimitService::new(
        cache,
        clock,
        stats.clone(),
        settings.service(),
    ));
    let (first_tx, first_rx) = tokio::sync::oneshot::channel();
    let first = std::sync::Mutex::new(Some(first_tx));
    let updated = service.clone();
    let _provider = FileProvider::with_loader(loader, &settings, move |config| {
        updated.update_config(config);
        if let Some(first) = first.lock().unwrap().take() {
            let _ = first.send(());
        }
    });
    if !settings.force_start_without_initial_config {
        let _ = first_rx.await;
    }

    // startGrpc, with gRPC's keepalive settings; stopped gracefully on a signal.
    let mut builder = tonic::transport::Server::builder();
    let age = crate::settings::duration(settings.grpc_max_connection_age);
    let grace = crate::settings::duration(settings.grpc_max_connection_age_grace);
    if !age.is_zero() {
        builder = builder.max_connection_age(age);
    }
    if !grace.is_zero() {
        builder = builder.max_connection_age_grace(grace);
    }
    let server = builder.add_service(grpc::server(service));
    let stop = shutting_down(shutdown);
    if !settings.grpc_uds.is_empty() {
        let address = settings.grpc_uds.clone();
        let listener = tokio::net::UnixListener::bind(&address)
            .map_err(|e| format!("Failed to listen for gRPC on '{address}': {e}"))?;
        let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);
        return server
            .serve_with_incoming_shutdown(incoming, stop)
            .await
            .map_err(|e| e.to_string());
    }
    let address = crate::net::join_host_port(&settings.grpc_host, settings.grpc_port);
    let listener = crate::net::listen(&address, true)
        .await
        .map_err(|e| format!("Failed to listen for gRPC on '{address}': {e}"))?;
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    server
        .serve_with_incoming_shutdown(incoming, stop)
        .await
        .map_err(|e| e.to_string())
}

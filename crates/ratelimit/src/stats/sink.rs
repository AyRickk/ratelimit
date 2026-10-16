//! Where flushed stats go, chosen like envoyproxy/ratelimit's runner
//! (`src/service_cmd/runner/runner.go`).

use super::{LoggingSink, StatsdSink};

/// A destination for flushed stats.
pub enum Sink {
    /// `DISABLE_STATS`: stats are dropped.
    Null,
    /// The default when no other sink is enabled: JSON lines on stderr.
    Logging(LoggingSink),
    /// `USE_STATSD`: statsd lines over TCP or UDP.
    Statsd(StatsdSink),
}

impl Sink {
    pub fn flush_counter(&self, name: &str, value: u64) {
        match self {
            Sink::Null => {}
            Sink::Logging(sink) => sink.flush_counter(name, value),
            Sink::Statsd(sink) => sink.flush_counter(name, value),
        }
    }

    /// Delivers what was flushed so far.
    pub async fn flush(&self) {
        match self {
            Sink::Null => {}
            Sink::Logging(sink) => sink.flush(),
            Sink::Statsd(sink) => sink.flush().await,
        }
    }
}

/// gostats' `STATSD_PROTOCOL`, which upstream leaves to gostats.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StatsdProtocol {
    #[default]
    Tcp,
    Udp,
}

/// The settings that choose and configure the sink.
#[derive(Debug, Clone)]
pub struct SinkSettings {
    /// `DISABLE_STATS`: wins over every other setting.
    pub disable_stats: bool,
    /// `USE_STATSD`, on by default.
    pub use_statsd: bool,
    /// `USE_DOG_STATSD`.
    pub use_dog_statsd: bool,
    /// `USE_PROMETHEUS`.
    pub use_prometheus: bool,
    /// `STATSD_HOST`.
    pub statsd_host: String,
    /// `STATSD_PORT`.
    pub statsd_port: u16,
    pub statsd_protocol: StatsdProtocol,
}

impl Default for SinkSettings {
    fn default() -> Self {
        Self {
            disable_stats: false,
            use_statsd: true,
            use_dog_statsd: false,
            use_prometheus: false,
            statsd_host: "localhost".into(),
            statsd_port: 8125,
            statsd_protocol: StatsdProtocol::Tcp,
        }
    }
}

const SEVERAL_SINKS: &str = "Error: unable to use more than one stats sink at the same time. Set one of USE_DOG_STATSD, USE_STATSD, USE_PROMETHEUS.";

/// The sink `settings` select, or why the process cannot start. As upstream, `DISABLE_STATS`
/// wins, then DogStatsD, statsd and Prometheus each refuse the others, and stats are logged when
/// none is enabled. Since `USE_STATSD` is on by default, enabling another sink alone is an error.
pub fn select_sink(settings: &SinkSettings) -> Result<Sink, String> {
    let enabled = [
        settings.use_dog_statsd,
        settings.use_statsd,
        settings.use_prometheus,
    ];
    if settings.disable_stats {
        return Ok(Sink::Null);
    }
    if enabled.iter().filter(|on| **on).count() > 1 {
        return Err(SEVERAL_SINKS.into());
    }
    if settings.use_dog_statsd {
        return Err("USE_DOG_STATSD is not supported yet".into());
    }
    if settings.use_prometheus {
        return Err("USE_PROMETHEUS is not supported yet".into());
    }
    if settings.use_statsd {
        return Ok(Sink::Statsd(StatsdSink::new(
            &settings.statsd_host,
            settings.statsd_port,
            settings.statsd_protocol,
        )));
    }
    Ok(Sink::Logging(LoggingSink::stderr()))
}

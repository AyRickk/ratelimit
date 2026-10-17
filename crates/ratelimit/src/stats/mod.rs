//! Counters, named as envoyproxy/ratelimit names them (`src/stats/manager_impl.go`): one per
//! name, kept for the life of the process like gostats' store, so a counter recreated by a
//! config reload or a request keeps its value. Flushing sends each counter's increase since
//! the previous flush to a [`Sink`]; a timer's values go to it at once, as gostats' do.

mod dogstatsd;
mod logging;
pub mod origin;
pub mod prometheus;
mod sink;
mod statsd;

pub use dogstatsd::{DogStatsdSettings, DogStatsdSink, Mogrifier};
pub use logging::LoggingSink;
pub use prometheus::{PrometheusSettings, PrometheusSink};
pub use sink::{Sink, SinkSettings, StatsdProtocol, select_sink};
pub use statsd::StatsdSink;

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock};

/// A counter, shared by everything holding the same name.
#[derive(Debug, Clone, Default)]
pub struct Counter(Arc<CounterValue>);

#[derive(Debug, Default)]
struct CounterValue {
    value: AtomicU64,
    last_sent: AtomicU64,
}

impl Counter {
    pub fn add(&self, n: u64) {
        self.0.value.fetch_add(n, Ordering::Relaxed);
    }

    pub fn inc(&self) {
        self.add(1);
    }

    /// The increase since the previous call (gostats' `latch`), wrapping like its `uint64`.
    fn latch(&self) -> u64 {
        let value = self.0.value.load(Ordering::Relaxed);
        value.wrapping_sub(self.0.last_sent.swap(value, Ordering::Relaxed))
    }
}

/// The sink the store flushes to, once flushing starts.
#[derive(Clone, Default)]
struct SinkCell(Arc<OnceLock<Arc<Sink>>>);

impl std::fmt::Debug for SinkCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SinkCell").finish_non_exhaustive()
    }
}

/// A timer (gostats' `timer`): each value goes to the sink as it is added.
#[derive(Debug, Clone)]
pub struct Timer {
    name: String,
    sink: SinkCell,
}

impl Timer {
    /// `AddValue`.
    pub fn add_value(&self, value: f64) {
        if let Some(sink) = self.sink.0.get() {
            sink.flush_timer(&self.name, value);
        }
    }
}

/// The counters of the service (`ratelimit.service.*`).
#[derive(Debug, Default)]
pub struct Stats {
    counters: Mutex<HashMap<String, Counter>>,
    sink: SinkCell,
    /// `EXTRA_TAGS`, serialized as gostats appends them to every name.
    tags: String,
    /// How many `EXTRA_TAGS` keys are reserved words.
    reserved_tags: u64,
}

/// A scope tagged with `EXTRA_TAGS` (`ScopeWithTags`).
#[derive(Debug, Clone)]
pub struct Scope {
    name: String,
}

const SERVICE: &str = "ratelimit.service";

/// Tag keys gostats counts in `reserved_tag` when a scope uses them.
const RESERVED_TAG_WORDS: &[&str] = &[
    "asg",
    "az",
    "backend",
    "canary",
    "host",
    "period",
    "region",
    "shard",
    "window",
    "source",
    "project",
    "facet",
    "envoyservice",
];

impl Stats {
    /// The service's counters, tagged with `EXTRA_TAGS` (`ScopeWithTags`): each reserved key
    /// counts once in `reserved_tag`, empty keys and values are left out, and `.`, `:` and `|`
    /// in values become `_`.
    pub fn new(extra_tags: &BTreeMap<String, String>) -> Self {
        let stats = Self {
            tags: extra_tags
                .iter()
                .filter(|(key, value)| !key.is_empty() && !value.is_empty())
                .map(|(key, value)| format!(".__{key}={}", value.replace(['.', ':', '|'], "_")))
                .collect(),
            ..Default::default()
        };
        let reserved_tags = extra_tags
            .keys()
            .filter(|key| RESERVED_TAG_WORDS.contains(&key.as_str()))
            .count() as u64;
        let stats = Self {
            reserved_tags,
            ..stats
        };
        stats.count_reserved_tags();
        stats
    }

    /// gostats checks the tags of each scope it creates: every reserved key counts again.
    fn count_reserved_tags(&self) {
        if self.reserved_tags > 0 {
            self.counter("reserved_tag").add(self.reserved_tags);
        }
    }

    /// `ScopeWithTags(name, EXTRA_TAGS)`: a scope whose counters carry the tags.
    pub fn scope_with_tags(&self, name: &str) -> Scope {
        self.count_reserved_tags();
        Scope {
            name: name.to_string(),
        }
    }

    /// The counter `name` of `scope`, with the tags.
    pub fn scope_counter(&self, scope: &Scope, name: &str) -> Counter {
        self.counter(&format!("{}.{name}{}", scope.name, self.tags))
    }

    /// The timer `name` of `scope`, with the tags.
    pub fn scope_timer(&self, scope: &Scope, name: &str) -> Timer {
        Timer {
            name: format!("{}.{name}{}", scope.name, self.tags),
            sink: self.sink.clone(),
        }
    }

    /// The counter named `name`, created at 0 the first time.
    fn counter(&self, name: &str) -> Counter {
        self.counters
            .lock()
            .unwrap()
            .entry(name.to_string())
            .or_default()
            .clone()
    }

    /// The service's counter `name`, with the tags.
    fn service_counter(&self, name: &str) -> Counter {
        self.counter(&format!("{SERVICE}.{name}{}", self.tags))
    }

    /// Sends every counter's increase since the previous flush, when not 0, then flushes `sink`.
    pub async fn flush(&self, sink: &Sink) {
        let counters: Vec<(String, Counter)> = self
            .counters
            .lock()
            .unwrap()
            .iter()
            .map(|(name, counter)| (name.clone(), counter.clone()))
            .collect();
        for (name, counter) in counters {
            let value = counter.latch();
            if value != 0 {
                sink.flush_counter(&name, value);
            }
        }
        sink.flush().await;
    }

    /// Every counter above 0, by name: what gostats would have flushed so far.
    pub fn snapshot(&self) -> BTreeMap<String, u64> {
        self.counters
            .lock()
            .unwrap()
            .iter()
            .map(|(name, counter)| (name.clone(), counter.0.value.load(Ordering::Relaxed)))
            .filter(|(_, value)| *value > 0)
            .collect()
    }

    /// `config_load_success`.
    pub fn config_load_success(&self) -> Counter {
        self.service_counter("config_load_success")
    }

    /// `config_load_error`.
    pub fn config_load_error(&self) -> Counter {
        self.service_counter("config_load_error")
    }

    /// `global_shadow_mode`: requests over the limit let through by global shadow mode.
    pub fn global_shadow_mode(&self) -> Counter {
        self.service_counter("global_shadow_mode")
    }

    /// `call.should_rate_limit.service_error`: invalid requests, or no configuration.
    pub fn service_error(&self) -> Counter {
        self.service_counter("call.should_rate_limit.service_error")
    }

    /// `call.should_rate_limit.redis_error`: backend errors.
    pub fn redis_error(&self) -> Counter {
        self.service_counter("call.should_rate_limit.redis_error")
    }

    /// `domain_not_found` for `domain` (`NewDomainStats`).
    pub fn domain_not_found(&self, domain: &str) -> Counter {
        let domain = sanitize_stat_name(domain);
        self.service_counter(&format!("rate_limit.{domain}.domain_not_found"))
    }

    /// The counters of the limit whose stats key is `key` (`NewStats`).
    pub fn rate_limit(&self, key: &str) -> RateLimitStats {
        let prefix = format!("rate_limit.{}", sanitize_stat_name(key));
        let counter = |name: &str| self.service_counter(&format!("{prefix}.{name}"));
        RateLimitStats {
            total_hits: counter("total_hits"),
            over_limit: counter("over_limit"),
            near_limit: counter("near_limit"),
            over_limit_with_local_cache: counter("over_limit_with_local_cache"),
            within_limit: counter("within_limit"),
            shadow_mode: counter("shadow_mode"),
            total_negative_hits: counter("total_negative_hits"),
        }
    }
}

/// Flushes `stats` to `sink` every `interval` (`STATS_FLUSH_INTERVAL`), the first time one
/// interval from now, like upstream's `store.Start(time.NewTicker(interval))`.
pub fn flush_every(
    stats: Arc<Stats>,
    sink: Sink,
    interval: std::time::Duration,
) -> tokio::task::JoinHandle<()> {
    let sink = Arc::new(sink);
    let _ = stats.sink.0.set(sink.clone());
    tokio::spawn(async move {
        let start = tokio::time::Instant::now() + interval;
        let mut ticks = tokio::time::interval_at(start, interval);
        // Like Go's ticker: after a slow flush, one tick at once, then back on schedule.
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticks.tick().await;
            stats.flush(&sink).await;
        }
    })
}

/// Upstream's `ServerReporter` (`src/metrics/metrics.go`), the gRPC server's interceptor:
/// `ratelimit_server.<method>.total_requests`, and `response_time` in milliseconds.
#[derive(Debug, Clone)]
pub struct ServerStats {
    stats: Arc<Stats>,
    scope: Scope,
}

impl ServerStats {
    /// `NewServerReporter(ScopeWithTags("ratelimit_server", EXTRA_TAGS))`.
    pub fn new(stats: Arc<Stats>) -> Self {
        let scope = stats.scope_with_tags("ratelimit_server");
        Self { stats, scope }
    }

    /// The interceptor around a unary call of `method`, once its request is decoded.
    pub async fn record<F: std::future::Future>(&self, method: &str, call: F) -> F::Output {
        let start = std::time::Instant::now();
        let total_requests = format!("{method}.total_requests");
        self.stats.scope_counter(&self.scope, &total_requests).inc();
        let output = call.await;
        let timer = self
            .stats
            .scope_timer(&self.scope, &format!("{method}.response_time"));
        // `time.Since(start).Milliseconds()`: whole milliseconds.
        timer.add_value(start.elapsed().as_millis() as f64);
        output
    }
}

/// The counters of one limit.
#[derive(Debug, Clone)]
pub struct RateLimitStats {
    pub total_hits: Counter,
    pub over_limit: Counter,
    pub near_limit: Counter,
    pub over_limit_with_local_cache: Counter,
    pub within_limit: Counter,
    pub shadow_mode: Counter,
    pub total_negative_hits: Counter,
}

/// `SanitizeStatName`: `:` and `|` become `_`, and so do the dots of anything shaped like an
/// IPv4 address.
pub fn sanitize_stat_name(name: &str) -> String {
    static IPV4: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r"[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}").unwrap()
    });
    let name = name.replace([':', '|'], "_");
    IPV4.replace_all(&name, |ip: &regex::Captures| ip[0].replace('.', "_"))
        .into_owned()
}

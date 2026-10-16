//! Counters, named as envoyproxy/ratelimit names them (`src/stats/manager_impl.go`): one per
//! name, kept for the life of the process like gostats' store, so a counter recreated by a
//! config reload or a request keeps its value.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

/// A counter, shared by everything holding the same name.
#[derive(Debug, Clone)]
pub struct Counter(Arc<AtomicU64>);

impl Counter {
    pub fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }

    pub fn inc(&self) {
        self.add(1);
    }
}

/// The counters of the service (`ratelimit.service.*`).
#[derive(Debug, Default)]
pub struct Stats {
    counters: Mutex<HashMap<String, Counter>>,
}

const SERVICE: &str = "ratelimit.service";

impl Stats {
    /// The counter named `name`, created at 0 the first time.
    pub fn counter(&self, name: &str) -> Counter {
        self.counters
            .lock()
            .unwrap()
            .entry(name.to_string())
            .or_insert_with(|| Counter(Arc::default()))
            .clone()
    }

    /// Every counter above 0, by name: what gostats would have flushed so far.
    pub fn snapshot(&self) -> BTreeMap<String, u64> {
        self.counters
            .lock()
            .unwrap()
            .iter()
            .map(|(name, counter)| (name.clone(), counter.0.load(Ordering::Relaxed)))
            .filter(|(_, value)| *value > 0)
            .collect()
    }

    /// `config_load_success`.
    pub fn config_load_success(&self) -> Counter {
        self.counter(&format!("{SERVICE}.config_load_success"))
    }

    /// `config_load_error`.
    pub fn config_load_error(&self) -> Counter {
        self.counter(&format!("{SERVICE}.config_load_error"))
    }

    /// `global_shadow_mode`: requests over the limit let through by global shadow mode.
    pub fn global_shadow_mode(&self) -> Counter {
        self.counter(&format!("{SERVICE}.global_shadow_mode"))
    }

    /// `call.should_rate_limit.service_error`: invalid requests, or no configuration.
    pub fn service_error(&self) -> Counter {
        self.counter(&format!("{SERVICE}.call.should_rate_limit.service_error"))
    }

    /// `call.should_rate_limit.redis_error`: backend errors.
    pub fn redis_error(&self) -> Counter {
        self.counter(&format!("{SERVICE}.call.should_rate_limit.redis_error"))
    }

    /// `domain_not_found` for `domain` (`NewDomainStats`).
    pub fn domain_not_found(&self, domain: &str) -> Counter {
        let domain = sanitize_stat_name(domain);
        self.counter(&format!("{SERVICE}.rate_limit.{domain}.domain_not_found"))
    }

    /// The counters of the limit whose stats key is `key` (`NewStats`).
    pub fn rate_limit(&self, key: &str) -> RateLimitStats {
        let prefix = format!("{SERVICE}.rate_limit.{}", sanitize_stat_name(key));
        let counter = |name: &str| self.counter(&format!("{prefix}.{name}"));
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

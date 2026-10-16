//! Counting logic shared by the backends: time, cache keys and descriptor statuses for
//! fixed windows. A port of envoyproxy/ratelimit `src/limiter/{base_limiter,cache_key}.go`
//! and the time helpers of `src/utils`.

use crate::config::RateLimit;
use crate::service::CacheError;
use crate::stats::RateLimitStats;
use ratelimit_proto::envoy::extensions::common::ratelimit::v3::RateLimitDescriptor;
use ratelimit_proto::envoy::service::ratelimit::v3::RateLimitRequest;
use ratelimit_proto::envoy::service::ratelimit::v3::rate_limit_response::{
    Code, DescriptorStatus, rate_limit::Unit,
};

/// The current time, in Unix seconds.
pub trait Clock: Send + Sync + 'static {
    fn unix_now(&self) -> i64;
}

/// The system clock.
pub struct SystemClock;

impl Clock for SystemClock {
    fn unix_now(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs() as i64)
    }
}

/// The length of a fixed window, in seconds (a month is 30 days, a year 365).
pub(crate) fn window_seconds(limit: &RateLimit) -> Result<i64, CacheError> {
    unit_seconds(limit.limit.unit())
}

/// The length of one `unit`, in seconds (a month is 30 days, a year 365).
pub(crate) fn unit_seconds(unit: Unit) -> Result<i64, CacheError> {
    Ok(match unit {
        Unit::Second => 1,
        Unit::Minute => 60,
        Unit::Hour => 60 * 60,
        Unit::Day => 60 * 60 * 24,
        Unit::Week => 60 * 60 * 24 * 7,
        Unit::Month => 60 * 60 * 24 * 30,
        Unit::Year => 60 * 60 * 24 * 365,
        // Upstream panics and the process dies; reported as an error instead (D1).
        Unit::Unknown => return Err(CacheError("invalid rate limit unit UNKNOWN".to_string())),
    })
}

/// The storage key of `limit` for `descriptor` in the window containing `now`:
/// `prefix + domain + "_" + (key "_" value "_")* + windowStart`. With `share_threshold`, an
/// entry that matched a shared wildcard uses the pattern in place of its value.
pub fn cache_key(
    prefix: &str,
    domain: &str,
    descriptor: &RateLimitDescriptor,
    limit: &RateLimit,
    now: i64,
) -> Result<String, CacheError> {
    let mut key = format!("{prefix}{domain}_");
    for (i, entry) in descriptor.entries.iter().enumerate() {
        let shared = limit
            .share_threshold_key_pattern
            .as_ref()
            .and_then(|patterns| patterns.get(i))
            .filter(|pattern| !pattern.is_empty());
        key += &entry.key;
        key += "_";
        key += shared.unwrap_or(&entry.value);
        key += "_";
    }
    let window = window_seconds(limit)?;
    key += &(now / window * window).to_string();
    Ok(key)
}

/// How much a descriptor adds to (or, for a refund, removes from) its counter.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Hits {
    pub value: u64,
    pub refund: bool,
}

/// Per descriptor: its own `hits_addend` when set (0 checks without counting), else the
/// request's, where 0 (unset) means 1.
pub(crate) fn hits(request: &RateLimitRequest) -> Vec<Hits> {
    request
        .descriptors
        .iter()
        .map(|d| Hits {
            value: d
                .hits_addend
                .unwrap_or(u64::from(request.hits_addend.max(1))),
            refund: d.is_negative_hits,
        })
        .collect()
}

fn status(
    code: Code,
    limit: &RateLimit,
    limit_remaining: u32,
    now: i64,
) -> Result<DescriptorStatus, CacheError> {
    let window = window_seconds(limit)?;
    Ok(DescriptorStatus {
        code: code as i32,
        current_limit: Some(limit.limit.clone()),
        limit_remaining,
        duration_until_reset: Some(prost_types::Duration {
            seconds: window - now % window,
            nanos: 0,
        }),
        quota: None,
    })
}

/// Whether a counter of `count` is over `limit`.
pub(crate) fn exceeds(limit: &RateLimit, count: u64) -> bool {
    count > u64::from(limit.limit.requests_per_unit)
}

/// What the backend found for a descriptor.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Count {
    /// Its counter once its hits are applied.
    After(u64),
    /// Over its limit according to the local cache; the backend was not asked.
    OverInLocalCache,
}

/// Counts `hits` towards `limit`'s `total_hits`, or `total_negative_hits` for a refund, as
/// upstream does when it builds the keys, before asking the backend.
pub(crate) fn count_hits(stats: &RateLimitStats, hits: Hits) {
    if hits.refund {
        stats.total_negative_hits.add(hits.value);
    } else {
        stats.total_hits.add(hits.value);
    }
}

/// Splits a request's `hits` between `limit`'s `within_limit`, `near_limit`, `over_limit`,
/// `over_limit_with_local_cache` and `shadow_mode` counters, as upstream's
/// `GetResponseDescriptorStatus`. Usage before the hits is taken as the counter minus the hits,
/// wrapping like upstream's unsigned arithmetic when fewer were counted (D22).
fn count_outcome(
    stats: &RateLimitStats,
    limit: &RateLimit,
    hits: u64,
    count: Count,
    near_limit_ratio: f32,
) {
    let after = match count {
        Count::OverInLocalCache => {
            stats.over_limit.add(hits);
            stats.over_limit_with_local_cache.add(hits);
            if limit.shadow_mode {
                stats.shadow_mode.add(hits);
            }
            return;
        }
        Count::After(after) => after,
    };
    let before = after.wrapping_sub(hits);
    let over = u64::from(limit.limit.requests_per_unit);
    // In float32, as upstream: `floor(float64(float32(limit) * ratio))`.
    let near = f64::from(over as f32 * near_limit_ratio).floor() as u64;
    if after > over {
        // All the hits were over when the counter already was; otherwise only those past the
        // limit, the rest being near it.
        let over_hits = if before >= over { hits } else { after - over };
        stats.over_limit.add(over_hits);
        if before < over {
            stats.near_limit.add(over.wrapping_sub(near.max(before)));
        }
        if limit.shadow_mode {
            stats.shadow_mode.add(over_hits);
        }
    } else {
        if after > near {
            stats
                .near_limit
                .add(if before >= near { hits } else { after - near });
        }
        stats.within_limit.add(hits);
    }
}

/// The status of a descriptor, its outcome counted in its limit's `stats`. Without a limit it
/// is a bare OK. A refund is always OK, with what remains. Otherwise it is over the limit when
/// the counter exceeds it (or the local cache says so); a rule in shadow mode then answers OK,
/// keeping the remaining count of 0.
pub(crate) fn descriptor_status(
    limit: Option<(&RateLimit, &RateLimitStats)>,
    near_limit_ratio: f32,
    hits: Hits,
    count: Count,
    now: i64,
) -> Result<DescriptorStatus, CacheError> {
    let Some((limit, stats)) = limit else {
        return Ok(DescriptorStatus {
            code: Code::Ok as i32,
            ..Default::default()
        });
    };
    if !hits.refund {
        count_outcome(stats, limit, hits.value, count, near_limit_ratio);
    }
    let threshold = u64::from(limit.limit.requests_per_unit);
    match count {
        Count::After(after) if hits.refund => {
            return status(Code::Ok, limit, threshold.saturating_sub(after) as u32, now);
        }
        Count::After(after) if !exceeds(limit, after) => {
            return status(Code::Ok, limit, (threshold - after) as u32, now);
        }
        Count::After(_) | Count::OverInLocalCache => {}
    }
    let code = if limit.shadow_mode {
        Code::Ok
    } else {
        Code::OverLimit
    };
    status(code, limit, 0, now)
}

/// What a request's keys look like before counting, for
/// `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestState {
    /// A key of the request is over its limit in the local cache.
    OverInLocalCache,
    /// The hits will take some keys over their limit.
    SomeGoingOver,
    /// Neither.
    WithinLimits,
}

/// How much to add to a key's counter: upstream's `getHitsAddendValue`. With
/// `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT`, nothing is counted once a key of the request is
/// over in the local cache, and when some keys are about to go over, only those are counted.
pub(crate) fn hits_to_count(
    hits: Hits,
    stop_increment_when_overlimit: bool,
    request: RequestState,
    going_over: bool,
) -> u64 {
    if !stop_increment_when_overlimit {
        return hits.value;
    }
    match request {
        RequestState::OverInLocalCache => 0,
        RequestState::WithinLimits => hits.value,
        RequestState::SomeGoingOver if going_over => hits.value,
        RequestState::SomeGoingOver => 0,
    }
}

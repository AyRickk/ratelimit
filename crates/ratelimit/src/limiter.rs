//! Counting logic shared by the backends: time, cache keys and descriptor statuses for
//! fixed windows. A port of envoyproxy/ratelimit `src/limiter/{base_limiter,cache_key}.go`
//! and the time helpers of `src/utils`.

use crate::config::RateLimit;
use crate::service::CacheError;
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
    Ok(match limit.limit.unit() {
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

/// The status of a descriptor whose counter is `after` once its hits are applied. Without a
/// limit it is a bare OK. A refund is always OK, with what remains. Otherwise it is over the
/// limit when the counter exceeds it, though a rule in shadow mode then still answers OK
/// (keeping the remaining count of 0).
pub(crate) fn descriptor_status(
    limit: Option<&RateLimit>,
    hits: Hits,
    after: u64,
    now: i64,
) -> Result<DescriptorStatus, CacheError> {
    let Some(limit) = limit else {
        return Ok(DescriptorStatus {
            code: Code::Ok as i32,
            ..Default::default()
        });
    };
    let threshold = u64::from(limit.limit.requests_per_unit);
    if hits.refund {
        status(Code::Ok, limit, threshold.saturating_sub(after) as u32, now)
    } else if after > threshold {
        let code = if limit.shadow_mode {
            Code::Ok
        } else {
            Code::OverLimit
        };
        status(code, limit, 0, now)
    } else {
        status(Code::Ok, limit, (threshold - after) as u32, now)
    }
}

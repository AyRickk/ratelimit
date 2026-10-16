//! The Redis backend for fixed windows. A port of envoyproxy/ratelimit
//! `src/redis/fixed_cache_impl.go`: one pipeline of `INCRBY` + `EXPIRE` per request, and a Lua
//! script for refunds.

use crate::config::RateLimit;
use crate::limiter::{self, Clock};
use crate::service::{CacheError, RateLimitCache};
use fred::prelude::*;
use fred::types::{ClusterHash, CustomCommand};
use ratelimit_proto::envoy::service::ratelimit::v3::{
    RateLimitRequest, rate_limit_response::DescriptorStatus,
};
use std::sync::Arc;

/// Upstream's `DecrementScript`, byte for byte: subtract, floor at 0, reset the TTL.
const DECREMENT_SCRIPT: &str = "
local current = redis.call('GET', KEYS[1])                   -- get current count
if current == false then return 0 end                        -- key absent: nothing to refund
local new_val = math.floor(math.max(0, tonumber(current) - tonumber(ARGV[1]))) -- subtract hits, floor at 0
redis.call('SET', KEYS[1], tostring(new_val))                -- persist new value
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))             -- reset TTL
return new_val                                               -- return count after decrement
";

#[derive(Debug, Clone, Default)]
pub struct RedisCacheSettings {
    /// `CACHE_KEY_PREFIX`.
    pub cache_key_prefix: String,
}

pub struct RedisCache {
    client: Client,
    clock: Arc<dyn Clock>,
    settings: RedisCacheSettings,
}

impl RedisCache {
    pub fn new(client: Client, clock: Arc<dyn Clock>, settings: RedisCacheSettings) -> Self {
        Self {
            client,
            clock,
            settings,
        }
    }
}

fn cache_error(e: fred::error::Error) -> CacheError {
    CacheError(e.to_string())
}

impl RateLimitCache for RedisCache {
    async fn do_limit(
        &self,
        request: &RateLimitRequest,
        limits: &[Option<RateLimit>],
    ) -> Result<Vec<DescriptorStatus>, CacheError> {
        let now = self.clock.unix_now();
        let hits = limiter::hits(request);

        // Descriptors with a limit, in order, with their key and window length.
        let mut counted = Vec::new();
        for (i, limit) in limits.iter().enumerate() {
            if let Some(limit) = limit {
                let key = limiter::cache_key(
                    &self.settings.cache_key_prefix,
                    &request.domain,
                    &request.descriptors[i],
                    limit,
                    now,
                )?;
                counted.push((i, key, limiter::window_seconds(limit)?));
            }
        }

        let mut after = vec![0u64; limits.len()];
        if !counted.is_empty() {
            let pipeline = self.client.pipeline();
            for (i, key, expiration) in &counted {
                let hits = hits[*i];
                if hits.refund {
                    // EVAL script 1 key hits ttl, routed by the key (argument 2) in a cluster.
                    let eval = CustomCommand::new_static("EVAL", ClusterHash::Offset(2), false);
                    let args = vec![
                        DECREMENT_SCRIPT.to_string(),
                        "1".to_string(),
                        key.clone(),
                        hits.value.to_string(),
                        expiration.to_string(),
                    ];
                    let () = pipeline.custom(eval, args).await.map_err(cache_error)?;
                } else {
                    let () = pipeline
                        .incr_by(key.as_str(), hits.value as i64)
                        .await
                        .map_err(cache_error)?;
                    let () = pipeline
                        .expire(key.as_str(), *expiration, None)
                        .await
                        .map_err(cache_error)?;
                }
            }
            let results: Vec<Value> = pipeline.all().await.map_err(cache_error)?;
            let mut results = results.into_iter();
            for (i, _, _) in &counted {
                let value = results
                    .next()
                    .ok_or_else(|| CacheError("missing Redis reply".into()))?;
                after[*i] = value.convert::<u64>().map_err(cache_error)?;
                if !hits[*i].refund {
                    results.next(); // EXPIRE
                }
            }
        }

        limits
            .iter()
            .zip(&hits)
            .zip(after)
            .map(|((limit, hits), after)| {
                if hits.refund {
                    limiter::refunded_status(limit.as_ref(), after, now)
                } else {
                    limiter::counted_status(limit.as_ref(), after, now)
                }
            })
            .collect()
    }
}

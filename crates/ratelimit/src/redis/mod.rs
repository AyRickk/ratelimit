//! The Redis backend for fixed windows. A port of envoyproxy/ratelimit
//! `src/redis/fixed_cache_impl.go`: one pipeline of `INCRBY` + `EXPIRE` per request, and a Lua
//! script for refunds. How the service reaches Redis is in [`connection`].

mod connection;

pub use connection::{RedisConnection, StartupBackoff, mask_credentials_in_url};

use crate::config::RateLimit;
use crate::limiter::{self, Clock, Count, Hits, RequestState};
use crate::local_cache::LocalCache;
use crate::service::{CacheError, RateLimitCache};
use crate::stats::{RateLimitStats, Stats};
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

#[derive(Debug, Clone)]
pub struct RedisCacheSettings {
    /// `CACHE_KEY_PREFIX`.
    pub cache_key_prefix: String,
    /// `LOCAL_CACHE_SIZE_IN_BYTES`: memory for remembering keys over their limit, so later
    /// requests skip Redis. 0 disables it.
    pub local_cache_size_in_bytes: usize,
    /// `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT`.
    pub stop_cache_key_increment_when_overlimit: bool,
    /// `NEAR_LIMIT_RATIO`: the share of a limit past which hits count as `near_limit`
    /// (upstream's default: 0.8).
    pub near_limit_ratio: f32,
}

impl Default for RedisCacheSettings {
    fn default() -> Self {
        Self {
            cache_key_prefix: String::new(),
            local_cache_size_in_bytes: 0,
            stop_cache_key_increment_when_overlimit: false,
            near_limit_ratio: 0.8,
        }
    }
}

pub struct RedisCache {
    /// Connections, each request's pipeline on one of them (radix's pool).
    pool: Pool,
    clock: Arc<dyn Clock>,
    stats: Arc<Stats>,
    local_cache: Option<Arc<LocalCache>>,
    settings: RedisCacheSettings,
}

impl RedisCache {
    pub fn new(
        pool: Pool,
        clock: Arc<dyn Clock>,
        stats: Arc<Stats>,
        settings: RedisCacheSettings,
    ) -> Self {
        let local_cache = (settings.local_cache_size_in_bytes > 0)
            .then(|| Arc::new(LocalCache::new(settings.local_cache_size_in_bytes)));
        Self::with_local_cache(pool, clock, stats, settings, local_cache)
    }

    /// The cache using `local_cache` (made by the runner, whose stats read it).
    pub fn with_local_cache(
        pool: Pool,
        clock: Arc<dyn Clock>,
        stats: Arc<Stats>,
        settings: RedisCacheSettings,
        local_cache: Option<Arc<LocalCache>>,
    ) -> Self {
        Self {
            pool,
            clock,
            stats,
            local_cache,
            settings,
        }
    }
}

fn cache_error(e: fred::error::Error) -> CacheError {
    CacheError(e.details().to_string())
}

/// A Redis command as upstream's client (radix) sends it: every argument as a string.
struct Command {
    name: &'static str,
    args: Vec<String>,
}

impl Command {
    /// radix's rendering in errors: `["INCRBY" "key" "1"]`, each part quoted like Go's
    /// `strconv.QuoteToASCII`.
    fn describe(&self) -> String {
        let parts: Vec<String> = std::iter::once(self.name)
            .chain(self.args.iter().map(String::as_str))
            .map(go_quote_ascii)
            .collect();
        format!("[{}]", parts.join(" "))
    }
}

/// Go's `strconv.QuoteToASCII`.
fn go_quote_ascii(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '\x07' => out += "\\a",
            '\x08' => out += "\\b",
            '\x0c' => out += "\\f",
            '\n' => out += "\\n",
            '\r' => out += "\\r",
            '\t' => out += "\\t",
            '\x0b' => out += "\\v",
            '\\' => out += "\\\\",
            '"' => out += "\\\"",
            ' '..='~' => out.push(c),
            c if (c as u32) < 0x80 => out += &format!("\\x{:02x}", c as u32),
            c if (c as u32) < 0x10000 => out += &format!("\\u{:04x}", c as u32),
            c => out += &format!("\\U{:08x}", c as u32),
        }
    }
    out.push('"');
    out
}

/// Sends `commands` in one pipeline and returns their replies, or upstream's error for the
/// first command that failed.
async fn send(client: &Client, commands: &[Command]) -> Result<Vec<Value>, CacheError> {
    let pipeline = client.pipeline();
    for command in commands {
        // EVAL is routed by its key, the third argument, in a cluster.
        let hash = if command.name == "EVAL" {
            ClusterHash::Offset(2)
        } else {
            ClusterHash::FirstKey
        };
        let custom = CustomCommand::new_static(command.name, hash, false);
        let () = pipeline
            .custom(custom, command.args.clone())
            .await
            .map_err(cache_error)?;
    }
    let replies: Vec<Result<Value, fred::error::Error>> = pipeline.try_all().await;
    commands
        .iter()
        .zip(replies)
        .map(|(command, reply)| reply.map_err(|e| command.failed(e.details())))
        .collect()
}

impl Command {
    /// Upstream's error for this command: radix wraps the first failure of a pipeline.
    fn failed(&self, error: impl std::fmt::Display) -> CacheError {
        CacheError(format!(
            "command {} in pipeline returned error: {error}",
            self.describe()
        ))
    }

    /// A reply read as a counter, like radix unmarshals it into a `uint64`: a missing value is 0,
    /// anything else must be an unsigned decimal.
    fn counter(&self, reply: Value) -> Result<u64, CacheError> {
        let text = match reply {
            Value::Null => return Ok(0),
            Value::Integer(i) => i.to_string(),
            Value::String(s) => s.to_string(),
            Value::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
            other => return Err(self.failed(format!("cannot read {other:?} as a counter"))),
        };
        if text.is_empty() {
            return Err(self.failed("empty slice given to parseUint"));
        }
        let mut n: u64 = 0;
        for (i, c) in text.bytes().enumerate() {
            if !c.is_ascii_digit() {
                return Err(self.failed(format!(
                    "invalid character {} at position {i} in parseUint",
                    c as char
                )));
            }
            n = n.wrapping_mul(10).wrapping_add(u64::from(c - b'0'));
        }
        Ok(n)
    }
}

/// A descriptor with a limit, to be checked against Redis or the local cache.
struct Check<'a> {
    index: usize,
    limit: &'a RateLimit,
    stats: RateLimitStats,
    key: String,
    hits: Hits,
    /// Already over its limit according to the local cache: Redis is skipped.
    over_in_local_cache: bool,
    /// With `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT`: these hits will take it over the limit.
    going_over: bool,
}

impl RateLimitCache for RedisCache {
    async fn do_limit(
        &self,
        request: &RateLimitRequest,
        limits: &[Option<RateLimit>],
    ) -> Result<Vec<DescriptorStatus>, CacheError> {
        crate::debug!("starting cache lookup");
        let now = self.clock.unix_now();
        let hits = limiter::hits(request);

        let mut checks = Vec::new();
        for (index, limit) in limits.iter().enumerate() {
            let Some(limit) = limit else { continue };
            let key = limiter::cache_key(
                &self.settings.cache_key_prefix,
                &request.domain,
                &request.descriptors[index],
                limit,
                now,
            )?;
            let hits = hits[index];
            let stats = self.stats.rate_limit(&limit.full_key);
            limiter::count_hits(&stats, hits);
            // Refunds always reach Redis.
            let over_in_local_cache = !hits.refund
                && self
                    .local_cache
                    .as_ref()
                    .is_some_and(|cache| cache.contains(&key, now));
            if over_in_local_cache {
                match limit.shadow_mode {
                    true => crate::debug!(
                        "Cache key {key} would be rate limited but shadow mode is enabled on this rule"
                    ),
                    false => crate::debug!("cache key is over the limit: {key}"),
                }
            }
            checks.push(Check {
                index,
                limit,
                stats,
                key,
                hits,
                over_in_local_cache,
                going_over: false,
            });
        }

        let mut state = if checks.iter().any(|c| c.over_in_local_cache) {
            RequestState::OverInLocalCache
        } else {
            RequestState::WithinLimits
        };
        let stop_increment = self.settings.stop_cache_key_increment_when_overlimit;
        if stop_increment && state == RequestState::WithinLimits {
            self.find_keys_going_over(&mut checks).await?;
            if checks.iter().any(|c| c.going_over) {
                state = RequestState::SomeGoingOver;
            }
        }

        // Per descriptor not answered by the local cache: INCRBY + EXPIRE, or the refund
        // script. Hits go over the wire as signed 64-bit integers, as upstream sends them.
        let mut commands = Vec::new();
        let mut reply_of = Vec::new();
        for c in &checks {
            if c.over_in_local_cache {
                continue;
            }
            crate::debug!("looking up cache key: {}", c.key);
            let expiration = limiter::window_seconds(c.limit)?.to_string();
            reply_of.push((c.index, commands.len()));
            if c.hits.refund {
                let amount = (c.hits.value as i64).to_string();
                let args = vec![
                    DECREMENT_SCRIPT.to_string(),
                    "1".to_string(),
                    c.key.clone(),
                    amount,
                    expiration,
                ];
                commands.push(Command { name: "EVAL", args });
            } else {
                let counted = limiter::hits_to_count(c.hits, stop_increment, state, c.going_over);
                let args = vec![c.key.clone(), (counted as i64).to_string()];
                commands.push(Command {
                    name: "INCRBY",
                    args,
                });
                commands.push(Command {
                    name: "EXPIRE",
                    args: vec![c.key.clone(), expiration],
                });
            }
        }
        // Until the statuses are built.
        let _span = crate::trace::start(
            crate::trace::REDIS,
            "Redis Pipeline Execution",
            crate::trace::Kind::Internal,
            crate::trace::parent().as_ref(),
            || {
                vec![
                    (
                        "pipeline length".into(),
                        crate::trace::Value::Int(commands.len() as i64),
                    ),
                    (
                        "perSecondPipeline length".into(),
                        crate::trace::Value::Int(0),
                    ),
                ]
            },
        );
        let mut counts = vec![Count::After(0); limits.len()];
        if !commands.is_empty() {
            let replies = send(self.pool.next(), &commands).await?;
            for (index, at) in reply_of {
                counts[index] = Count::After(commands[at].counter(replies[at].clone())?);
            }
        }
        for c in &checks {
            if c.over_in_local_cache {
                counts[c.index] = Count::OverInLocalCache;
            } else if let (false, Count::After(after), Some(cache)) =
                (c.hits.refund, counts[c.index], &self.local_cache)
                && limiter::exceeds(c.limit, after)
            {
                // Cached for a whole window, shadow rules included, as upstream.
                if !cache.insert(&c.key, limiter::window_seconds(c.limit)?, now) {
                    crate::log::error(&format!("Failing to set local cache key: {}", c.key));
                }
            }
        }

        let mut checked: Vec<Option<(&RateLimit, &RateLimitStats, &str)>> =
            vec![None; limits.len()];
        for c in &checks {
            checked[c.index] = Some((c.limit, &c.stats, &c.key));
        }
        checked
            .into_iter()
            .zip(&hits)
            .zip(counts)
            .map(|((limit, hits), count)| {
                let ratio = self.settings.near_limit_ratio;
                limiter::descriptor_status(limit, ratio, *hits, count, now)
            })
            .collect()
    }
}

impl RedisCache {
    /// `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT`: reads the counters first to mark the keys
    /// these hits will take over the limit.
    async fn find_keys_going_over(&self, checks: &mut [Check<'_>]) -> Result<(), CacheError> {
        let mut reads: Vec<&mut Check> = checks.iter_mut().filter(|c| !c.hits.refund).collect();
        if reads.is_empty() {
            return Ok(());
        }
        let commands: Vec<Command> = reads
            .iter()
            .map(|c| Command {
                name: "GET",
                args: vec![c.key.clone()],
            })
            .collect();
        let replies = send(self.pool.next(), &commands).await?;
        for ((c, command), reply) in reads.iter_mut().zip(&commands).zip(replies) {
            let before = command.counter(reply)?;
            c.going_over = limiter::exceeds(c.limit, before.wrapping_add(c.hits.value));
        }
        Ok(())
    }
}

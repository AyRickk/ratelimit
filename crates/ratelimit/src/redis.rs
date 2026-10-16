//! The Redis backend for fixed windows. A port of envoyproxy/ratelimit
//! `src/redis/fixed_cache_impl.go`: one pipeline of `INCRBY` + `EXPIRE` per request, and a Lua
//! script for refunds.

use crate::config::RateLimit;
use crate::limiter::{self, Clock, LocalCache};
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
    /// `LOCAL_CACHE_SIZE_IN_BYTES`: memory for remembering keys over their limit, so later
    /// requests skip Redis. 0 disables it.
    pub local_cache_size_in_bytes: usize,
    /// `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT`.
    pub stop_cache_key_increment_when_overlimit: bool,
}

pub struct RedisCache {
    client: Client,
    clock: Arc<dyn Clock>,
    local_cache: Option<LocalCache>,
    settings: RedisCacheSettings,
}

impl RedisCache {
    pub fn new(client: Client, clock: Arc<dyn Clock>, settings: RedisCacheSettings) -> Self {
        let local_cache = (settings.local_cache_size_in_bytes > 0)
            .then(|| LocalCache::new(settings.local_cache_size_in_bytes));
        Self {
            client,
            clock,
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
async fn run(client: &Client, commands: &[Command]) -> Result<Vec<Value>, CacheError> {
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
        .map(|(command, reply)| {
            reply.map_err(|e| {
                CacheError(format!(
                    "command {} in pipeline returned error: {}",
                    command.describe(),
                    e.details()
                ))
            })
        })
        .collect()
}

fn counter(value: Value) -> Result<u64, CacheError> {
    // A missing key reads as 0.
    if value.is_null() {
        return Ok(0);
    }
    value.convert::<u64>().map_err(cache_error)
}

/// A descriptor with a limit, about to be counted in Redis.
struct Counted<'a> {
    index: usize,
    limit: &'a RateLimit,
    key: String,
    hits: limiter::Hits,
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
        let now = self.clock.unix_now();
        let hits = limiter::hits(request);

        let mut counted = Vec::new();
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
            // Refunds always reach Redis.
            let over_in_local_cache = !hits.refund
                && self
                    .local_cache
                    .as_ref()
                    .is_some_and(|cache| cache.contains(&key));
            counted.push(Counted {
                index,
                limit,
                key,
                hits,
                over_in_local_cache,
                going_over: false,
            });
        }
        let any_over_in_local_cache = counted.iter().any(|c| c.over_in_local_cache);

        // STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT: read the counters first to find the keys
        // these hits will take over the limit.
        let mut any_going_over = false;
        if self.settings.stop_cache_key_increment_when_overlimit && !any_over_in_local_cache {
            let reads: Vec<&mut Counted> = counted.iter_mut().filter(|c| !c.hits.refund).collect();
            if !reads.is_empty() {
                let commands: Vec<Command> = reads
                    .iter()
                    .map(|c| Command {
                        name: "GET",
                        args: vec![c.key.clone()],
                    })
                    .collect();
                let values = run(&self.client, &commands).await?;
                for (c, value) in reads.into_iter().zip(values) {
                    let before = counter(value)?;
                    c.going_over = before.wrapping_add(c.hits.value)
                        > u64::from(c.limit.limit.requests_per_unit);
                    any_going_over |= c.going_over;
                }
            }
        }

        // Per descriptor not answered by the local cache: INCRBY + EXPIRE, or the refund
        // script. Hits go over the wire as signed 64-bit integers, as upstream sends them.
        let mut commands = Vec::new();
        let mut replies_at = Vec::new();
        for c in &counted {
            if c.over_in_local_cache {
                continue;
            }
            let expiration = limiter::window_seconds(c.limit)?.to_string();
            replies_at.push((c.index, commands.len()));
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
                let amount =
                    (self.increment(c, any_over_in_local_cache, any_going_over) as i64).to_string();
                commands.push(Command {
                    name: "INCRBY",
                    args: vec![c.key.clone(), amount],
                });
                commands.push(Command {
                    name: "EXPIRE",
                    args: vec![c.key.clone(), expiration],
                });
            }
        }
        let mut after = vec![0u64; limits.len()];
        if !commands.is_empty() {
            let mut replies = run(&self.client, &commands).await?;
            for (index, at) in replies_at {
                after[index] = counter(std::mem::replace(&mut replies[at], Value::Null))?;
            }
        }

        let mut statuses: Vec<DescriptorStatus> = limits
            .iter()
            .zip(&hits)
            .zip(&after)
            .map(|((limit, hits), after)| {
                limiter::descriptor_status(limit.as_ref(), *hits, *after, false, now)
            })
            .collect::<Result<_, _>>()?;
        for c in &counted {
            if c.over_in_local_cache {
                statuses[c.index] =
                    limiter::descriptor_status(Some(c.limit), c.hits, 0, true, now)?;
            } else if !c.hits.refund
                && after[c.index] > u64::from(c.limit.limit.requests_per_unit)
                && let Some(cache) = &self.local_cache
            {
                cache.insert(&c.key, limiter::window_seconds(c.limit)?);
            }
        }
        Ok(statuses)
    }
}

impl RedisCache {
    /// How much to add to a counter: upstream's `getHitsAddendValue`. With
    /// `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT`, nothing is counted once a key of the request is
    /// over in the local cache, and when some keys are about to go over, only those are counted.
    fn increment(&self, c: &Counted, any_over_in_local_cache: bool, any_going_over: bool) -> u64 {
        if !self.settings.stop_cache_key_increment_when_overlimit {
            c.hits.value
        } else if any_over_in_local_cache {
            0
        } else if !any_going_over || c.going_over {
            c.hits.value
        } else {
            0
        }
    }
}

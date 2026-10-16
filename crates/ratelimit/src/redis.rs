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

/// A descriptor counted in Redis.
struct Counted {
    index: usize,
    /// Index of the command whose reply is the counter after this descriptor's hits.
    command: usize,
}

impl RateLimitCache for RedisCache {
    async fn do_limit(
        &self,
        request: &RateLimitRequest,
        limits: &[Option<RateLimit>],
    ) -> Result<Vec<DescriptorStatus>, CacheError> {
        let now = self.clock.unix_now();
        let hits = limiter::hits(request);

        // Per descriptor with a limit, in order: INCRBY + EXPIRE, or the refund script.
        // Hits go over the wire as signed 64-bit integers, as upstream sends them.
        let mut commands = Vec::new();
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
            let expiration = limiter::window_seconds(limit)?.to_string();
            let amount = (hits[index].value as i64).to_string();
            counted.push(Counted {
                index,
                command: commands.len(),
            });
            if hits[index].refund {
                let args = vec![
                    DECREMENT_SCRIPT.to_string(),
                    "1".to_string(),
                    key,
                    amount,
                    expiration,
                ];
                commands.push(Command { name: "EVAL", args });
            } else {
                commands.push(Command {
                    name: "INCRBY",
                    args: vec![key.clone(), amount],
                });
                commands.push(Command {
                    name: "EXPIRE",
                    args: vec![key, expiration],
                });
            }
        }

        let mut after = vec![0u64; limits.len()];
        if !commands.is_empty() {
            let pipeline = self.client.pipeline();
            for command in &commands {
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
            // Like upstream, report the first command that failed.
            for (command, reply) in commands.iter().zip(&replies) {
                if let Err(e) = reply {
                    return Err(CacheError(format!(
                        "command {} in pipeline returned error: {}",
                        command.describe(),
                        e.details()
                    )));
                }
            }
            for Counted { index, command } in counted {
                let reply = replies[command].clone().map_err(cache_error)?;
                after[index] = reply.convert::<u64>().map_err(cache_error)?;
            }
        }

        limits
            .iter()
            .zip(&hits)
            .zip(after)
            .map(|((limit, hits), after)| {
                limiter::descriptor_status(limit.as_ref(), *hits, after, now)
            })
            .collect()
    }
}

//! The Redis backend for fixed windows. A port of envoyproxy/ratelimit
//! `src/redis/fixed_cache_impl.go`: one pipeline of `INCRBY` + `EXPIRE` per request, and a Lua
//! script for refunds. How the service reaches Redis is in [`connection`].

mod aws;
mod connection;
mod live;
pub mod tls;

pub use connection::{Connected, RedisConnection, StartupBackoff, Tls, mask_credentials_in_url};

use crate::config::RateLimit;
use crate::config::window::{Anchored, AnchoredWindow, MAX_ANCHORED_COUNT, OpenOn, Window};
use crate::limiter::{self, CacheSettings, Clock, Count, Hits, RequestState};
use crate::local_cache::LocalCache;
use crate::service::{CacheError, LimitOutcome, LimitReading, RateLimitCache};
use crate::stats::{RateLimitStats, Stats};
use fred::prelude::*;
use fred::types::{ClusterHash, CustomCommand};
use live::LivePool;
use ratelimit_proto::envoy::service::ratelimit::v3::RateLimitRequest;
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

/// An `anchored` window (docs/design.md D9), as [`AnchoredWindow::step`] decides it: the
/// anchor and the count in one hash, the window closed once `now ≥ anchor + duration`, never
/// by the key's absence. ARGV: now, duration, rounding (0: none), hits, opened by a check
/// (1) or a first hit (0), refund (1) or not (0), TTL margin, the largest count (2^53, which
/// hits are capped at too). Returns the anchor (-1: no window), the count, and whether this
/// request opened it.
const ANCHORED_SCRIPT: &str = "
local now, duration, rounding = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local hits, on_check, refund = tonumber(ARGV[4]), ARGV[5] == '1', ARGV[6] == '1'
local anchor = tonumber(redis.call('HGET', KEYS[1], 'a'))
local count = tonumber(redis.call('HGET', KEYS[1], 'c')) or 0
if anchor ~= nil and now - anchor >= duration then anchor = nil end
if refund then
  if anchor == nil then return {-1, 0, 0} end
  count = math.max(0, count - hits)
  redis.call('HSET', KEYS[1], 'c', string.format('%d', count))
  return {anchor, count, 0}
end
if anchor == nil then
  if not on_check and hits == 0 then return {-1, 0, 0} end
  anchor = now
  if rounding > 0 then anchor = now - now % rounding end
  redis.call('HSET', KEYS[1], 'a', string.format('%d', anchor), 'c', string.format('%d', hits))
  redis.call('EXPIRE', KEYS[1], anchor + duration - now + tonumber(ARGV[7]))
  return {anchor, hits, 1}
end
count = math.min(count + hits, tonumber(ARGV[8]))
redis.call('HSET', KEYS[1], 'c', string.format('%d', count))
return {anchor, count, 0}
";

/// How long an anchored window's key outlives it, for replicas whose clocks are behind.
const ANCHORED_MARGIN_SECONDS: i64 = 60;

pub struct RedisCache {
    /// Connections, each request's pipeline on one of them (radix's pool).
    pool: LivePool,
    /// `REDIS_PERSECOND`: the pool for limits in seconds, in a pipeline of their own.
    per_second: Option<LivePool>,
    clock: Arc<dyn Clock>,
    stats: Arc<Stats>,
    local_cache: Option<Arc<LocalCache>>,
    settings: CacheSettings,
}

impl RedisCache {
    pub fn new(
        pool: Pool,
        clock: Arc<dyn Clock>,
        stats: Arc<Stats>,
        settings: CacheSettings,
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
        settings: CacheSettings,
        local_cache: Option<Arc<LocalCache>>,
    ) -> Self {
        Self {
            pool: LivePool::new(pool),
            per_second: None,
            clock,
            stats,
            local_cache,
            settings,
        }
    }

    /// The cache sending limits in seconds to `pool` (`REDIS_PERSECOND`).
    pub fn with_per_second(mut self, pool: Pool) -> Self {
        self.per_second = Some(LivePool::new(pool));
        self
    }

    /// The pipeline a descriptor's `limits` go in: 0 the main one, 1 the per-second one.
    fn pipeline_of(&self, limits: &[RateLimit]) -> usize {
        match &self.per_second {
            Some(_) if limiter::per_second(limits) => 1,
            _ => 0,
        }
    }

    /// The pipelines' pools: the main one, then the per-second one if any.
    fn pools(&self) -> [Option<&LivePool>; 2] {
        [Some(&self.pool), self.per_second.as_ref()]
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
    // A client that is not connected fails the pipeline at once, as radix's dead connection
    // does, rather than holding it until Redis is back.
    let fail_fast = Options {
        fail_fast: true,
        ..Default::default()
    };
    // `with_options(..).pipeline()` would build it on the bare client, through `Deref`.
    let pipeline = fred::clients::Pipeline::from(client.with_options(&fail_fast));
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

/// A descriptor's limit, to be checked against Redis or the local cache.
struct Counted<'a> {
    /// The descriptor's index in the request.
    index: usize,
    /// The limit's index in the descriptor's limits.
    slot: usize,
    limit: &'a RateLimit,
    stats: RateLimitStats,
    key: String,
    hits: Hits,
    /// Already over its limit according to the local cache: Redis is skipped.
    over_in_local_cache: bool,
    /// With `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT`: these hits will take it over the limit.
    going_over: bool,
    /// A `rate_limits:` entry, which `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` leaves alone
    /// (D20).
    listed: bool,
    /// The pipeline it goes in: 0 the main one, 1 the per-second one (`REDIS_PERSECOND`).
    pipeline: usize,
}

impl RateLimitCache for RedisCache {
    async fn lookup(
        &self,
        request: &RateLimitRequest,
        limits: &[Vec<RateLimit>],
    ) -> Result<(i64, Vec<Vec<LimitReading>>), CacheError> {
        let now = self.clock.unix_now();
        Ok((now, self.read_limits(request, limits, now).await?))
    }

    async fn do_limit(
        &self,
        request: &RateLimitRequest,
        limits: &[Vec<RateLimit>],
    ) -> Result<Vec<Vec<LimitOutcome>>, CacheError> {
        crate::debug!("starting cache lookup");
        let now = self.clock.unix_now();
        let hits = limiter::hits(request);

        let mut checks = Vec::new();
        for (index, slot, limit) in limiter::slots(limits) {
            let key = limiter::cache_key(
                &self.settings.cache_key_prefix,
                &request.domain,
                &request.descriptors[index],
                limit,
                now,
                self.settings.use_calendar_month,
            )?;
            let hits = hits[index];
            let stats = self.stats.rate_limit(&limit.full_key);
            limiter::count_hits(&stats, hits);
            // Refunds always reach Redis; the local cache does not hold anchored windows.
            let anchored = matches!(limit.window, Some(Window::Anchored(_)));
            let over_in_local_cache = !hits.refund
                && !anchored
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
            checks.push(Counted {
                index,
                slot,
                limit,
                stats,
                key,
                hits,
                over_in_local_cache,
                going_over: false,
                listed: limit.window.is_some(),
                pipeline: self.pipeline_of(&limits[index]),
            });
        }

        let mut state = if checks.iter().any(|c| c.over_in_local_cache && !c.listed) {
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
        let mut pipelines: [Vec<Command>; 2] = [Vec::new(), Vec::new()];
        let mut reply_of = Vec::new();
        for c in &checks {
            if c.over_in_local_cache {
                continue;
            }
            crate::debug!("looking up cache key: {}", c.key);
            let commands = &mut pipelines[c.pipeline];
            reply_of.push((c.index, c.slot, c.pipeline, commands.len()));
            // An anchored window's key expires with the window, which the script knows.
            if let Some(Window::Anchored(window)) = &c.limit.window {
                commands.push(anchored_command(window, &c.key, c.hits, now));
                continue;
            }
            let calendar = self.settings.use_calendar_month;
            let mut expiration = limiter::expiration(c.limit, now, calendar)?;
            let jitter = self.settings.expiration_jitter_max_seconds;
            if jitter > 0 {
                // Go's int64 addition wraps.
                expiration = expiration.wrapping_add(crate::random::below(jitter as u64) as i64);
            }
            let expiration = expiration.to_string();
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
                let counted = match c.listed {
                    true => c.hits.value,
                    false => limiter::hits_to_count(c.hits, stop_increment, state, c.going_over),
                };
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
                        crate::trace::Value::Int(pipelines[0].len() as i64),
                    ),
                    (
                        "perSecondPipeline length".into(),
                        crate::trace::Value::Int(pipelines[1].len() as i64),
                    ),
                ]
            },
        );
        let mut counts: Vec<Vec<Count>> = limits
            .iter()
            .map(|list| vec![Count::After(0); list.len()])
            .collect();
        // The main pipeline, then the per-second one.
        let mut replies: [Vec<_>; 2] = [Vec::new(), Vec::new()];
        for (pipeline, pool) in self.pools().into_iter().enumerate() {
            if let (Some(pool), false) = (pool, pipelines[pipeline].is_empty()) {
                replies[pipeline] = send(pool.next()?, &pipelines[pipeline]).await?;
            }
        }
        // An anchored window's reset, from its state, and whether this request opened it
        // without hits.
        let mut resets: Vec<Vec<Option<i64>>> =
            limits.iter().map(|list| vec![None; list.len()]).collect();
        let mut opened_without_hits: Vec<Vec<bool>> =
            limits.iter().map(|list| vec![false; list.len()]).collect();
        for (index, slot, pipeline, at) in reply_of {
            let reply = replies[pipeline][at].clone();
            let command = &pipelines[pipeline][at];
            let Some(Window::Anchored(window)) = &limits[index][slot].window else {
                counts[index][slot] = Count::After(command.counter(reply)?);
                continue;
            };
            let (state, opened) = command.anchored(reply)?;
            counts[index][slot] = Count::After(state.map_or(0, |w| w.count));
            resets[index][slot] = Some(window.reset(state, now));
            opened_without_hits[index][slot] = opened && hits[index].value == 0;
        }
        for c in &checks {
            if c.over_in_local_cache {
                counts[c.index][c.slot] = Count::OverInLocalCache;
            } else if let (false, Count::After(after), Some(cache)) =
                (c.hits.refund, counts[c.index][c.slot], &self.local_cache)
                && limiter::exceeds(c.limit, after)
                && !matches!(c.limit.window, Some(Window::Anchored(_)))
            {
                // Cached for a whole window, shadow rules included, as upstream.
                let calendar = self.settings.use_calendar_month;
                let ttl = limiter::expiration(c.limit, now, calendar)?;
                if !cache.insert(&c.key, ttl, now) {
                    crate::log::error(&format!("Failing to set local cache key: {}", c.key));
                }
            }
        }

        let mut statuses: Vec<Vec<LimitOutcome>> = limits
            .iter()
            .map(|list| Vec::with_capacity(list.len()))
            .collect();
        for c in &checks {
            let reset = match resets[c.index][c.slot] {
                Some(reset) => reset,
                None => {
                    let calendar = self.settings.use_calendar_month;
                    limiter::reset(c.limit, &request.descriptors[c.index], now, calendar)?
                }
            };
            let count = counts[c.index][c.slot];
            let ratio = self.settings.near_limit_ratio;
            let status =
                limiter::limit_status((c.limit, &c.stats, &c.key), ratio, c.hits, count, reset);
            statuses[c.index].push(LimitOutcome {
                status,
                opened_without_hits: opened_without_hits[c.index][c.slot],
            });
        }
        Ok(statuses)
    }
}

impl RedisCache {
    /// The quota lookup's reads (ADR 0003): each limit's key at `now`, as a decision would
    /// find it, with `GET` or, for an anchored window, `HMGET` of its anchor and count; on
    /// the pool the decision would use. Nothing is written.
    async fn read_limits(
        &self,
        request: &RateLimitRequest,
        limits: &[Vec<RateLimit>],
        now: i64,
    ) -> Result<Vec<Vec<LimitReading>>, CacheError> {
        let mut pipelines: [Vec<Command>; 2] = [Vec::new(), Vec::new()];
        let mut reply_of = Vec::new();
        for (index, slot, limit) in limiter::slots(limits) {
            let key = limiter::cache_key(
                &self.settings.cache_key_prefix,
                &request.domain,
                &request.descriptors[index],
                limit,
                now,
                self.settings.use_calendar_month,
            )?;
            let pipeline = self.pipeline_of(&limits[index]);
            let command = match &limit.window {
                Some(Window::Anchored(_)) => Command {
                    name: "HMGET",
                    args: vec![key, "a".into(), "c".into()],
                },
                _ => Command {
                    name: "GET",
                    args: vec![key],
                },
            };
            reply_of.push((index, slot, pipeline, pipelines[pipeline].len()));
            pipelines[pipeline].push(command);
        }
        let mut replies: [Vec<_>; 2] = [Vec::new(), Vec::new()];
        for (pipeline, pool) in self.pools().into_iter().enumerate() {
            if let (Some(pool), false) = (pool, pipelines[pipeline].is_empty()) {
                replies[pipeline] = send(pool.next()?, &pipelines[pipeline]).await?;
            }
        }
        let mut readings: Vec<Vec<LimitReading>> = limits
            .iter()
            .map(|list| Vec::with_capacity(list.len()))
            .collect();
        for (index, slot, pipeline, at) in reply_of {
            let (command, reply) = (&pipelines[pipeline][at], replies[pipeline][at].clone());
            let limit = &limits[index][slot];
            let reading = match &limit.window {
                Some(Window::Anchored(window)) => {
                    // Closed as a decision closes it, whether or not the key expired yet.
                    let open = window.open(command.stored_anchored(reply)?, now);
                    LimitReading {
                        used: open.map_or(0, |w| w.count),
                        reset: open.map(|w| window.reset(Some(w), now)),
                    }
                }
                _ => {
                    let calendar = self.settings.use_calendar_month;
                    let descriptor = &request.descriptors[index];
                    LimitReading {
                        used: command.counter(reply)?,
                        reset: Some(limiter::reset(limit, descriptor, now, calendar)?),
                    }
                }
            };
            readings[index].push(reading);
        }
        Ok(readings)
    }
}

/// The script call counting `hits` in an anchored window's `key` at `now`.
fn anchored_command(window: &AnchoredWindow, key: &str, hits: Hits, now: i64) -> Command {
    let flag = |on: bool| if on { "1" } else { "0" }.to_string();
    let on_check = window.open_on == OpenOn::Check;
    let args = vec![
        ANCHORED_SCRIPT.to_string(),
        "1".to_string(),
        key.to_string(),
        now.to_string(),
        window.duration.to_string(),
        window.rounding.unwrap_or(0).to_string(),
        hits.value.min(MAX_ANCHORED_COUNT).to_string(),
        flag(on_check),
        flag(hits.refund),
        ANCHORED_MARGIN_SECONDS.to_string(),
        MAX_ANCHORED_COUNT.to_string(),
    ];
    Command { name: "EVAL", args }
}

impl Command {
    /// An anchored window's stored state, from `HMGET key a c`: none without an anchor.
    fn stored_anchored(&self, reply: Value) -> Result<Option<Anchored>, CacheError> {
        let malformed = || self.failed("an anchored window's state is not 2 integers");
        let Value::Array(values) = reply else {
            return Err(malformed());
        };
        let [anchor, count] = &values[..] else {
            return Err(malformed());
        };
        let number = |value: &Value| match value {
            Value::Null => Ok(None),
            other => other
                .as_str()
                .and_then(|s| s.parse::<i64>().ok())
                .map(Some)
                .ok_or_else(malformed),
        };
        let Some(anchor) = number(anchor)? else {
            return Ok(None);
        };
        // A count the script cannot read, it reads as 0.
        let count = number(count)
            .ok()
            .flatten()
            .and_then(|c| u64::try_from(c).ok());
        Ok(Some(Anchored {
            anchor,
            count: count.unwrap_or(0),
        }))
    }

    /// The anchored script's reply: the open window, if any, and whether it just opened.
    fn anchored(&self, reply: Value) -> Result<(Option<Anchored>, bool), CacheError> {
        let malformed = || self.failed("an anchored window's reply is not 3 integers");
        let Value::Array(values) = reply else {
            return Err(malformed());
        };
        let numbers: Vec<i64> = values.iter().filter_map(Value::as_i64).collect();
        let [anchor, count, opened] = numbers[..] else {
            return Err(malformed());
        };
        // A negative count is no count the script writes.
        let count = u64::try_from(count).map_err(|_| malformed())?;
        let window = (anchor >= 0).then_some(Anchored { anchor, count });
        Ok((window, opened == 1))
    }
}

impl RedisCache {
    /// `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT`: reads the counters first to mark the keys
    /// these hits will take over the limit.
    async fn find_keys_going_over(&self, checks: &mut [Counted<'_>]) -> Result<(), CacheError> {
        // The main pipeline's reads, then the per-second one's.
        for (pipeline, pool) in self.pools().into_iter().enumerate() {
            let Some(pool) = pool else { continue };
            let mut reads: Vec<&mut Counted> = checks
                .iter_mut()
                .filter(|c| !c.hits.refund && !c.listed && c.pipeline == pipeline)
                .collect();
            if reads.is_empty() {
                continue;
            }
            let commands: Vec<Command> = reads
                .iter()
                .map(|c| Command {
                    name: "GET",
                    args: vec![c.key.clone()],
                })
                .collect();
            let replies = send(pool.next()?, &commands).await?;
            for ((c, command), reply) in reads.iter_mut().zip(&commands).zip(replies) {
                let before = command.counter(reply)?;
                c.going_over = limiter::exceeds(c.limit, before.wrapping_add(c.hits.value));
            }
        }
        Ok(())
    }
}

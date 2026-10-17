//! The Redis backend for fixed windows. A port of envoyproxy/ratelimit
//! `src/redis/fixed_cache_impl.go`: one pipeline of `INCRBY` + `EXPIRE` per request, and a Lua
//! script for refunds.

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
    local_cache: Option<LocalCache>,
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
            .then(|| LocalCache::new(settings.local_cache_size_in_bytes));
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
                cache.insert(&c.key, limiter::window_seconds(c.limit)?, now);
            }
        }

        let mut checked: Vec<Option<(&RateLimit, &RateLimitStats)>> = vec![None; limits.len()];
        for c in &checks {
            checked[c.index] = Some((c.limit, &c.stats));
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

/// How to reach Redis, from the `REDIS_*` settings of upstream's main pool
/// (`src/redis/driver_impl.go` `newClientImpl`).
#[derive(Debug, Clone, PartialEq)]
pub struct RedisConnection {
    /// The address (`host:port`) or the Unix socket path.
    pub address: RedisAddress,
    pub username: Option<String>,
    pub password: Option<String>,
    pub database: Option<u8>,
    /// The pool's connections (radix's default for 0: 4).
    pub pool_size: usize,
    /// The connection timeout; none for 0.
    pub timeout: std::time::Duration,
    /// `MaskCredentialsInUrl` of `REDIS_URL`, for messages.
    pub masked_url: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RedisAddress {
    Tcp { host: String, port: u16 },
    Unix(String),
}

impl RedisConnection {
    /// The main pool's connection, or why upstream refuses to start (or what is not supported
    /// yet).
    pub fn from_settings(s: &crate::settings::Settings) -> Result<Self, String> {
        let unsupported = [
            (s.redis_tls, "REDIS_TLS"),
            (s.redis_aws_iam_auth, "REDIS_AWS_IAM_AUTH"),
            (!s.redis_auth_file.is_empty(), "REDIS_AUTH_FILE"),
            (s.redis_persecond, "REDIS_PERSECOND"),
            (
                s.redis_close_connection_on_readonly_error,
                "REDIS_CLOSE_CONNECTION_ON_READONLY_ERROR",
            ),
        ];
        if let Some((_, name)) = unsupported.iter().find(|(on, _)| *on) {
            return Err(format!("{name} is not supported yet"));
        }
        match s.redis_pool_on_empty_behavior.to_uppercase().as_str() {
            "CREATE" => return Err("REDIS_POOL_ON_EMPTY_BEHAVIOR=CREATE is not supported in radix v4. Pool will block instead of creating overflow connections. Remove this setting or set to WAIT, and consider increasing REDIS_POOL_SIZE.".into()),
            "ERROR" => return Err("REDIS_POOL_ON_EMPTY_BEHAVIOR=ERROR is not supported in radix v4. Pool will block instead of failing fast. Remove this setting or set to WAIT, and use context timeouts for fail-fast behavior.".into()),
            _ => {}
        }
        match s.redis_type.to_lowercase().as_str() {
            "single" => {}
            "cluster" | "sentinel" => {
                return Err(format!("REDIS_TYPE={} is not supported yet", s.redis_type));
            }
            other => return Err(format!("Unrecognized redis type {other}")),
        }
        let pool_size = match s.redis_pool_size {
            0 => 4,
            size => usize::try_from(size).map_err(|_| format!("invalid REDIS_POOL_SIZE {size}"))?,
        };
        // radix reads credentials and a database from a redis:// URL, after REDIS_AUTH.
        let (mut username, mut password) = match s.redis_auth.split_once(':') {
            _ if s.redis_auth.is_empty() => (None, None),
            Some((user, pass)) => (Some(user.to_string()), Some(pass.to_string())),
            None => (None, Some(s.redis_auth.clone())),
        };
        let (target, url) = parse_redis_url(&s.redis_url);
        if username.is_none() {
            username = url.username;
        }
        if password.is_none() {
            password = url.password;
        }
        let database = match url.database {
            None => None,
            Some(db) => Some(
                db.parse::<u8>()
                    .map_err(|_| format!("REDIS_URL database {db:?} is not supported"))?,
            ),
        };
        let address = match s.redis_socket_type.as_str() {
            "unix" => RedisAddress::Unix(target),
            "tcp" | "tcp4" | "tcp6" => {
                let (host, port) = target
                    .rsplit_once(':')
                    .and_then(|(h, p)| {
                        Some((h.trim_matches(['[', ']']).to_string(), p.parse().ok()?))
                    })
                    .ok_or_else(|| format!("address {target}: missing port in address"))?;
                RedisAddress::Tcp { host, port }
            }
            other => return Err(format!("REDIS_SOCKET_TYPE={other} is not supported yet")),
        };
        Ok(Self {
            address,
            username: username.filter(|u| !u.is_empty()),
            password: password.filter(|p| !p.is_empty()),
            database,
            pool_size,
            timeout: std::time::Duration::from_nanos(u64::try_from(s.redis_timeout).unwrap_or(0)),
            masked_url: mask_credentials_in_url(&s.redis_url),
        })
    }

    fn config(&self) -> Config {
        let server = match &self.address {
            RedisAddress::Tcp { host, port } => ServerConfig::Centralized {
                server: Server::new(host.as_str(), *port),
            },
            RedisAddress::Unix(path) => ServerConfig::Unix { path: path.into() },
        };
        Config {
            server,
            username: self.username.clone(),
            password: self.password.clone(),
            database: self.database,
            fail_fast: true,
            ..Default::default()
        }
    }

    fn builder(&self) -> Builder {
        let mut builder = Builder::from_config(self.config());
        let timeout = self.timeout;
        builder.with_connection_config(|c| c.connection_timeout = timeout);
        builder
    }

    /// One attempt: connect and expect `PONG` to `PING`.
    async fn probe(&self) -> Result<(), String> {
        let client = self.builder().build().map_err(|e| e.to_string())?;
        client.init().await.map_err(|e| e.details().to_string())?;
        let pong: Result<String, _> = client.ping(None).await;
        let _ = client.quit().await;
        match pong {
            Ok(pong) if pong == "PONG" => Ok(()),
            Ok(pong) => Err(format!(
                "unexpected PING response: {}",
                crate::go::quote(&pong)
            )),
            Err(e) => Err(e.details().to_string()),
        }
    }

    /// `newClientImpl`'s startup: retries until Redis answers `PING`, waiting as
    /// `REDIS_STARTUP_*` say (exponential with jitter), and gives up after
    /// `REDIS_STARTUP_MAX_ELAPSED_TIME` (never for 0). Then the pool, which reconnects lost
    /// connections and pings every `5s / size` like radix's.
    pub async fn connect(&self, startup: &StartupBackoff) -> Result<Pool, String> {
        let start = std::time::Instant::now();
        let mut attempt = 0;
        while let Err(error) = self.probe().await {
            let elapsed = start.elapsed();
            if startup.max_elapsed > std::time::Duration::ZERO && elapsed >= startup.max_elapsed {
                let rounded = (elapsed.as_nanos() as i64 + 500_000) / 1_000_000 * 1_000_000;
                return Err(format!(
                    "timed out waiting for Redis connection to {} after {}: {error}",
                    self.masked_url,
                    crate::go::format_duration(rounded)
                ));
            }
            tokio::time::sleep(startup.duration(attempt)).await;
            attempt += 1;
        }
        let pool = self
            .builder()
            .set_policy(ReconnectPolicy::new_constant(0, 100))
            .build_pool(self.pool_size)
            .map_err(|e| e.to_string())?;
        pool.init().await.map_err(|e| e.details().to_string())?;
        let pinger = pool.clone();
        let interval = std::time::Duration::from_secs(5) / self.pool_size as u32;
        tokio::spawn(async move {
            let mut ticks =
                tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
            loop {
                ticks.tick().await;
                let _: Result<String, _> = pinger.next().ping(None).await;
            }
        });
        Ok(pool)
    }
}

/// `jpillora/backoff` as upstream configures it for Redis startup: `min × 2^attempt`, a random
/// point between `min` and that, within `[min, max]`.
#[derive(Debug, Clone)]
pub struct StartupBackoff {
    pub min: std::time::Duration,
    pub max: std::time::Duration,
    /// Give up after this long; never for 0.
    pub max_elapsed: std::time::Duration,
}

impl StartupBackoff {
    pub fn from_settings(s: &crate::settings::Settings) -> Self {
        let duration =
            |nanos: i64| std::time::Duration::from_nanos(u64::try_from(nanos).unwrap_or(0));
        Self {
            min: duration(s.redis_startup_initial_interval),
            max: duration(s.redis_startup_max_interval),
            max_elapsed: duration(s.redis_startup_max_elapsed_time),
        }
    }

    /// `ForAttempt` with jitter.
    pub fn duration(&self, attempt: u32) -> std::time::Duration {
        use std::time::Duration;
        let min = if self.min.is_zero() {
            Duration::from_millis(100)
        } else {
            self.min
        };
        let max = if self.max.is_zero() {
            Duration::from_secs(10)
        } else {
            self.max
        };
        if min >= max {
            return max;
        }
        let minf = min.as_nanos() as f64;
        let durf = minf * 2f64.powi(attempt.min(1000) as i32);
        let jittered = random_unit() * (durf - minf) + minf;
        if !jittered.is_finite() || jittered > i64::MAX as f64 {
            return max;
        }
        Duration::from_nanos(jittered as u64).clamp(min, max)
    }
}

/// A random number in `[0, 1)` (no cryptographic quality needed for a retry delay).
fn random_unit() -> f64 {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    );
    (hasher.finish() >> 11) as f64 / (1u64 << 53) as f64
}

/// What radix reads from a `redis://` URL (`parseRedisURL`): the host, and credentials and a
/// database from the user info, the path or the query. Any other address is used as it is.
struct RedisUrl {
    username: Option<String>,
    password: Option<String>,
    database: Option<String>,
}

fn parse_redis_url(url: &str) -> (String, RedisUrl) {
    let none = RedisUrl {
        username: None,
        password: None,
        database: None,
    };
    let Some(rest) = url.strip_prefix("redis://") else {
        return (url.to_string(), none);
    };
    let (rest, query) = rest.split_once('?').unwrap_or((rest, ""));
    let (authority, path) = match rest.find('/') {
        Some(at) => (&rest[..at], &rest[at..]),
        None => (rest, ""),
    };
    let (userinfo, host) = match authority.rfind('@') {
        Some(at) => (Some(&authority[..at]), &authority[at + 1..]),
        None => (None, authority),
    };
    let query_value = |name: &str| {
        query
            .split('&')
            .filter_map(|pair| pair.split_once('='))
            .find(|(k, _)| percent_decode(k, true) == name)
            .map(|(_, v)| percent_decode(v, true))
            .filter(|v| !v.is_empty())
    };
    let mut parsed = RedisUrl {
        username: query_value("username"),
        password: query_value("password"),
        database: query_value("db"),
    };
    if let Some(userinfo) = userinfo {
        let (user, pass) = match userinfo.split_once(':') {
            Some((u, p)) => (u, Some(p)),
            None => (userinfo, None),
        };
        if !user.is_empty() {
            parsed.username = Some(percent_decode(user, false));
        }
        if let Some(pass) = pass {
            parsed.password = Some(percent_decode(pass, false));
        }
    }
    if !path.is_empty() && path != "/" {
        parsed.database = Some(percent_decode(&path[1..], false));
    }
    (host.to_string(), parsed)
}

/// `%XX` decoded; in a query, `+` is a space too.
fn percent_decode(s: &str, query: bool) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = |b: u8| (b as char).to_digit(16);
        match (
            bytes[i],
            bytes.get(i + 1).copied().and_then(hex),
            bytes.get(i + 2).copied().and_then(hex),
        ) {
            (b'%', Some(h), Some(l)) => {
                out.push((h * 16 + l) as u8);
                i += 3;
            }
            (b'+', _, _) if query => {
                out.push(b' ');
                i += 1;
            }
            (b, _, _) => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// `MaskCredentialsInUrl`: `redis://user:pass@host` becomes `redis://*****@host`, for each
/// comma-separated URL.
pub fn mask_credentials_in_url(url: &str) -> String {
    url.split(',')
        .map(|u| {
            let parts: Vec<&str> = u.split('@').collect();
            match parts[..] {
                [first, .., last] if first.starts_with("redis://") => {
                    format!("redis://*****@{last}")
                }
                _ => u.to_string(),
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

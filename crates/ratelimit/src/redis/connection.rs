//! How the service reaches Redis: upstream's main pool (`src/redis/driver_impl.go`
//! `newClientImpl`) over radix v4's dialer and pool, whose rules this follows (credentials and
//! a database from a `redis://` URL, `AUTH`, `SELECT`, a periodic `PING`), with upstream's
//! startup retry.

use crate::settings::Settings;
use fred::prelude::*;
use fred::types::{ClusterHash, CustomCommand};
use std::time::Duration;

/// How to reach Redis, from the `REDIS_*` settings of upstream's main pool.
#[derive(Debug, Clone, PartialEq)]
pub struct RedisConnection {
    /// `REDIS_SOCKET_TYPE`: Go's network (`unix`, `tcp`, `tcp4`, `tcp6`).
    pub network: String,
    /// The address radix dials: `REDIS_URL`, or the host of a `redis://` URL.
    pub address: String,
    /// radix's dialer credentials and database; empty when unset.
    pub username: String,
    pub password: String,
    pub database: String,
    /// The pool's connections (radix's default for 0: 4).
    pub pool_size: usize,
    /// The dial timeout; none for 0.
    pub timeout: Duration,
    /// `REDIS_URL` with its credentials masked, for messages.
    pub masked_url: String,
}

impl RedisConnection {
    /// The main pool's connection, or why upstream refuses to start (or what is not supported
    /// yet).
    pub fn from_settings(s: &Settings) -> Result<Self, String> {
        crate::settings::refuse_unsupported(&[
            (s.redis_tls, "REDIS_TLS"),
            (s.redis_aws_iam_auth, "REDIS_AWS_IAM_AUTH"),
            (!s.redis_auth_file.is_empty(), "REDIS_AUTH_FILE"),
            (s.redis_persecond, "REDIS_PERSECOND"),
            (
                s.redis_close_connection_on_readonly_error,
                "REDIS_CLOSE_CONNECTION_ON_READONLY_ERROR",
            ),
        ])?;
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
            _ => return Err(format!("Unrecognized redis type {}", s.redis_type)),
        }
        let pool_size = match s.redis_pool_size {
            0 => 4,
            size => usize::try_from(size).map_err(|_| format!("invalid REDIS_POOL_SIZE {size}"))?,
        };
        // createDialer: REDIS_AUTH is `user:password` or a password; radix then fills in what
        // is still empty from a redis:// URL.
        let (mut username, mut password) = match s.redis_auth.split_once(':') {
            Some((user, pass)) => (user.to_string(), pass.to_string()),
            None => (String::new(), s.redis_auth.clone()),
        };
        let (address, url) = parse_redis_url(&s.redis_url);
        if username.is_empty() {
            username = url.username;
        }
        if password.is_empty() {
            password = url.password;
        }
        Ok(Self {
            network: s.redis_socket_type.clone(),
            address,
            username,
            password,
            database: url.database,
            pool_size,
            timeout: crate::settings::duration(s.redis_timeout),
            masked_url: mask_credentials_in_url(&s.redis_url),
        })
    }

    /// Where to connect, as Go's dialer resolves the address for the network; failures are
    /// dial errors (retried like upstream's).
    async fn server(&self) -> Result<ServerConfig, String> {
        let network = self.network.as_str();
        let family: fn(&std::net::IpAddr) -> bool = match network {
            "unix" => {
                return Ok(ServerConfig::Unix {
                    path: self.address.clone().into(),
                });
            }
            "tcp" => |_| true,
            "tcp4" => std::net::IpAddr::is_ipv4,
            "tcp6" => std::net::IpAddr::is_ipv6,
            _ => return Err(format!("dial {network}: unknown network {network}")),
        };
        // An unparseable redis:// URL is dialled as it is: keep its credentials out of errors.
        let masked = mask_credentials_in_url(&self.address);
        let dial_error =
            |e: String| format!("dial {network}: {}", e.replace(&self.address, &masked));
        let (host, port) = crate::go::split_host_port(&self.address).map_err(dial_error)?;
        let port: u16 = port
            .parse()
            .map_err(|_| dial_error(format!("lookup {network}/{port}: unknown port")))?;
        let addresses: Vec<std::net::IpAddr> = tokio::net::lookup_host((host.as_str(), port))
            .await
            .map_err(|e| dial_error(e.to_string()))?
            .map(|a| a.ip())
            .collect();
        let ip = addresses
            .iter()
            .find(|ip| family(ip))
            .ok_or_else(|| dial_error(format!("address {masked}: no suitable address found")))?;
        Ok(ServerConfig::Centralized {
            server: Server::new(ip.to_string(), port),
        })
    }

    /// A client builder; the probe selects the database by hand (`database` false).
    fn builder(&self, server: ServerConfig, database: bool) -> Builder {
        // radix sends AUTH with a user even when the password is empty.
        let username = (!self.username.is_empty()).then(|| self.username.clone());
        let password = match &username {
            Some(_) => Some(self.password.clone()),
            None => (!self.password.is_empty()).then(|| self.password.clone()),
        };
        let mut builder = Builder::from_config(Config {
            server,
            username,
            password,
            database: self.database.parse().ok().filter(|_| database),
            fail_fast: true,
            ..Default::default()
        });
        let timeout = self.timeout;
        builder.with_connection_config(|c| {
            c.connection_timeout = timeout;
            // radix bounds only the dial, not the commands that set a connection up.
            c.internal_command_timeout = Duration::from_secs(10 * 365 * 24 * 3600);
        });
        builder
    }

    /// One attempt of upstream's loop: connect (AUTH, then the URL's database as given, which
    /// Redis may refuse), expect `PONG` to `PING`, then the pool.
    async fn attempt(&self) -> Result<Pool, String> {
        let server = self.server().await?;
        // The database is selected by hand on the probe, so a value Redis refuses is its error.
        let probe = self
            .builder(server.clone(), false)
            .build()
            .map_err(|e| e.to_string())?;
        probe.init().await.map_err(|e| e.details().to_string())?;
        let checked = async {
            if !self.database.is_empty() {
                let select = CustomCommand::new_static("SELECT", ClusterHash::Random, false);
                let _: Value = probe
                    .custom(select, vec![self.database.clone()])
                    .await
                    .map_err(|e| e.details().to_string())?;
            }
            let pong: String = probe
                .ping(None)
                .await
                .map_err(|e| e.details().to_string())?;
            match pong.as_str() {
                "PONG" => Ok(()),
                _ => Err(format!(
                    "unexpected PING response: {}",
                    crate::go::quote(&pong)
                )),
            }
        }
        .await;
        let _ = probe.quit().await;
        checked?;
        if !self.database.is_empty() && self.database.parse::<u8>().is_err() {
            return Err(format!(
                "REDIS_URL database {} is not supported",
                crate::go::quote(&self.database)
            ));
        }
        let pool = self
            .builder(server, true)
            .set_policy(ReconnectPolicy::new_constant(0, 100))
            .build_pool(self.pool_size)
            .map_err(|e| e.to_string())?;
        pool.init().await.map_err(|e| e.details().to_string())?;
        Ok(pool)
    }

    /// `newClientImpl`'s startup: retries until Redis answers `PING`, waiting as
    /// `REDIS_STARTUP_*` say, and gives up after `REDIS_STARTUP_MAX_ELAPSED_TIME` (never for
    /// 0) or when `cancelled` resolves. Then the pool, which reconnects lost connections and
    /// pings every `5s / size` like radix's.
    pub async fn connect(
        &self,
        startup: &StartupBackoff,
        cancelled: impl std::future::Future<Output = ()>,
    ) -> Result<Pool, String> {
        let start = std::time::Instant::now();
        let mut attempt = 0;
        tokio::pin!(cancelled);
        let pool = loop {
            let error = match self.attempt().await {
                Ok(pool) => break pool,
                Err(error) => error,
            };
            let elapsed = start.elapsed();
            if !startup.max_elapsed.is_zero() && elapsed >= startup.max_elapsed {
                let rounded = (elapsed.as_nanos() as i64 + 500_000) / 1_000_000 * 1_000_000;
                return Err(format!(
                    "timed out waiting for Redis connection to {} after {}: {error}",
                    self.masked_url,
                    crate::go::format_duration(rounded)
                ));
            }
            tokio::select! {
                () = tokio::time::sleep(startup.duration(attempt)) => {}
                () = &mut cancelled => {
                    return Err(format!(
                        "context cancelled while waiting for Redis connection to {}: context canceled",
                        self.masked_url
                    ));
                }
            }
            attempt += 1;
        };
        let pinger = pool.clone();
        let interval = Duration::from_secs(5) / self.pool_size as u32;
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
    pub min: Duration,
    pub max: Duration,
    /// Give up after this long; never for 0.
    pub max_elapsed: Duration,
}

impl StartupBackoff {
    pub fn from_settings(s: &Settings) -> Self {
        Self {
            min: crate::settings::duration(s.redis_startup_initial_interval),
            max: crate::settings::duration(s.redis_startup_max_interval),
            max_elapsed: crate::settings::duration(s.redis_startup_max_elapsed_time),
        }
    }

    /// `ForAttempt` with jitter.
    pub fn duration(&self, attempt: u32) -> Duration {
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
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    hasher.write_u128(now.as_nanos());
    (hasher.finish() >> 11) as f64 / (1u64 << 53) as f64
}

/// What radix reads from a `redis://` URL (`parseRedisURL`, after Go's `url.Parse`): the host,
/// and credentials and a database from the user info, the path or the query (empty when
/// absent). Any other address, or one Go cannot parse, is used as it is.
struct RedisUrl {
    username: String,
    password: String,
    database: String,
}

fn parse_redis_url(url: &str) -> (String, RedisUrl) {
    let mut parsed = RedisUrl {
        username: String::new(),
        password: String::new(),
        database: String::new(),
    };
    let Some(rest) = url.strip_prefix("redis://") else {
        return (url.to_string(), parsed);
    };
    let rest = rest.split_once('#').map_or(rest, |(before, _)| before);
    let (rest, query) = rest.split_once('?').unwrap_or((rest, ""));
    let (authority, path) = match rest.find('/') {
        Some(at) => (&rest[..at], &rest[at..]),
        None => (rest, ""),
    };
    let (userinfo, host) = match authority.rfind('@') {
        Some(at) => (Some(&authority[..at]), &authority[at + 1..]),
        None => (None, authority),
    };
    let unescaped = |s: &str| crate::go::unescape(s, false);
    let (user, pass) = match userinfo.map(|u| u.split_once(':').unwrap_or((u, ""))) {
        Some((user, pass)) => (unescaped(user), unescaped(pass)),
        None => (Some(String::new()), Some(String::new())),
    };
    let database = unescaped(path);
    // Go's url.Parse refuses a host with spaces, as it does bad escapes.
    let valid_host = !host.contains(char::is_whitespace);
    let (Some(user), Some(pass), Some(database), true) = (user, pass, database, valid_host) else {
        return (url.to_string(), parsed);
    };
    // url.Values.Get: the first value of a key; a pair with `;` is dropped.
    let query_value = |name: &str| {
        query
            .split('&')
            .filter(|pair| !pair.contains(';'))
            .map(|pair| pair.split_once('=').unwrap_or((pair, "")))
            .find(|(k, _)| crate::go::unescape(k, true).as_deref() == Some(name))
            .and_then(|(_, v)| crate::go::unescape(v, true))
            .unwrap_or_default()
    };
    parsed.username = query_value("username");
    if !user.is_empty() {
        parsed.username = user;
    }
    parsed.password = query_value("password");
    if userinfo.is_some_and(|u| u.contains(':')) {
        parsed.password = pass;
    }
    parsed.database = query_value("db");
    if !database.is_empty() && database != "/" {
        parsed.database = database[1..].to_string();
    }
    (host.to_string(), parsed)
}

/// `MaskCredentialsInUrl` (`redis://user:pass@host` becomes `redis://*****@host`, for each
/// comma-separated URL), and the `password` and `username` query parameters masked too, which
/// upstream leaves in its messages (`docs/parity.md` §7).
pub fn mask_credentials_in_url(url: &str) -> String {
    url.split(',')
        .map(|u| {
            let parts: Vec<&str> = u.split('@').collect();
            let masked = match parts[..] {
                [first, .., last] if first.starts_with("redis://") => {
                    format!("redis://*****@{last}")
                }
                _ => u.to_string(),
            };
            let Some((base, query)) = masked.split_once('?') else {
                return masked;
            };
            let query: Vec<String> = query
                .split('&')
                .map(|pair| {
                    let (key, _) = pair.split_once('=').unwrap_or((pair, ""));
                    // Compared as the parser reads keys (decoded).
                    let decoded = crate::go::unescape(key, true).unwrap_or_default();
                    match matches!(decoded.as_str(), "password" | "username") {
                        true => format!("{key}=*****"),
                        false => pair.to_string(),
                    }
                })
                .collect();
            format!("{base}?{}", query.join("&"))
        })
        .collect::<Vec<_>>()
        .join(",")
}

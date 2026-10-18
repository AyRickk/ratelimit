//! How the service reaches Redis: upstream's main pool (`src/redis/driver_impl.go`
//! `newClientImpl`) over radix v4's dialer and pool, whose rules this follows (credentials and
//! a database from a `redis://` URL, `AUTH`, `SELECT`, a periodic `PING`), with upstream's
//! startup retry.

use crate::settings::Settings;
use fred::prelude::*;
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
    /// `REDIS_AUTH`, `REDIS_POOL_SIZE`, `REDIS_POOL_ON_EMPTY_BEHAVIOR` and `REDIS_TYPE` as
    /// given, which `newClientImpl` logs and checks when it starts.
    pub auth: String,
    pub pool_size_setting: i64,
    /// `REDIS_PIPELINE_LIMIT`, which radix v4 ignores: upstream only warns.
    pub pipeline_limit: i64,
    pub pool_on_empty_behavior: String,
    pub redis_type: String,
    /// The pool's variables' prefix (`REDIS`, `REDIS_PERSECOND`), for errors naming them.
    pub prefix: &'static str,
}

/// What `newClientImpl` takes for one pool, from its group of settings.
struct PoolSettings<'a> {
    /// The variables' prefix, for errors naming them.
    prefix: &'static str,
    socket_type: &'a str,
    url: &'a str,
    pool_size: i64,
    auth: &'a str,
    timeout: i64,
    pool_on_empty_behavior: &'a str,
    redis_type: &'a str,
    pipeline_limit: i64,
}

impl RedisConnection {
    /// The main pool's connection (`REDIS_*`), or why upstream refuses to start (or what is
    /// not supported yet).
    pub fn from_settings(s: &Settings) -> Result<Self, String> {
        crate::settings::refuse_unsupported(&[
            (s.redis_tls, "REDIS_TLS"),
            // Upstream loads the TLS files for either pool, the per-second one enabled or not.
            (s.redis_persecond_tls, "REDIS_PERSECOND_TLS"),
            (s.redis_aws_iam_auth, "REDIS_AWS_IAM_AUTH"),
            (!s.redis_auth_file.is_empty(), "REDIS_AUTH_FILE"),
            (
                s.redis_close_connection_on_readonly_error,
                "REDIS_CLOSE_CONNECTION_ON_READONLY_ERROR",
            ),
        ])?;
        Self::new(PoolSettings {
            prefix: "REDIS",
            socket_type: &s.redis_socket_type,
            url: &s.redis_url,
            pool_size: s.redis_pool_size,
            auth: &s.redis_auth,
            timeout: s.redis_timeout,
            pool_on_empty_behavior: &s.redis_pool_on_empty_behavior,
            redis_type: &s.redis_type,
            pipeline_limit: s.redis_pipeline_limit,
        })
    }

    /// The per-second pool's connection (`REDIS_PERSECOND_*`), for limits in seconds.
    pub fn per_second_from_settings(s: &Settings) -> Result<Self, String> {
        crate::settings::refuse_unsupported(&[
            (s.redis_persecond_tls, "REDIS_PERSECOND_TLS"),
            (
                s.redis_persecond_aws_iam_auth,
                "REDIS_PERSECOND_AWS_IAM_AUTH",
            ),
            (
                !s.redis_persecond_auth_file.is_empty(),
                "REDIS_PERSECOND_AUTH_FILE",
            ),
        ])?;
        Self::new(PoolSettings {
            prefix: "REDIS_PERSECOND",
            socket_type: &s.redis_persecond_socket_type,
            url: &s.redis_persecond_url,
            pool_size: s.redis_persecond_pool_size,
            auth: &s.redis_persecond_auth,
            timeout: s.redis_persecond_timeout,
            pool_on_empty_behavior: &s.redis_persecond_pool_on_empty_behavior,
            redis_type: &s.redis_persecond_type,
            pipeline_limit: s.redis_persecond_pipeline_limit,
        })
    }

    fn new(p: PoolSettings<'_>) -> Result<Self, String> {
        let pool_size = match p.pool_size {
            0 => 4,
            size => usize::try_from(size)
                .map_err(|_| format!("invalid {}_POOL_SIZE {size}", p.prefix))?,
        };
        // createDialer: the auth is `user:password` or a password; radix then fills in what
        // is still empty from a redis:// URL.
        let (mut username, mut password) = match p.auth.split_once(':') {
            Some((user, pass)) => (user.to_string(), pass.to_string()),
            None => (String::new(), p.auth.to_string()),
        };
        let (address, url) = parse_redis_url(p.url);
        if username.is_empty() {
            username = url.username;
        }
        if password.is_empty() {
            password = url.password;
        }
        Ok(Self {
            network: p.socket_type.to_string(),
            address,
            username,
            password,
            database: url.database,
            pool_size,
            timeout: crate::settings::duration(p.timeout),
            masked_url: mask_credentials_in_url(p.url),
            auth: p.auth.to_string(),
            pool_size_setting: p.pool_size,
            pipeline_limit: p.pipeline_limit,
            pool_on_empty_behavior: p.pool_on_empty_behavior.to_string(),
            redis_type: p.redis_type.to_string(),
            prefix: p.prefix,
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

    /// radix's AUTH arguments: with a user even when the password is empty.
    fn auth_arguments(&self) -> Option<Vec<String>> {
        match (self.username.is_empty(), self.password.is_empty()) {
            (false, _) => Some(vec![self.username.clone(), self.password.clone()]),
            (true, false) => Some(vec![self.password.clone()]),
            (true, true) => None,
        }
    }

    /// A client builder; the probe leaves the database to the dial (`probe`).
    fn builder(&self, server: ServerConfig, probe: bool) -> Builder {
        let (username, password) = match self.auth_arguments() {
            Some(mut arguments) => {
                let password = arguments.pop();
                (arguments.pop(), password)
            }
            None => (None, None),
        };
        let mut builder = Builder::from_config(Config {
            server,
            username,
            password,
            database: self.database.parse().ok().filter(|_| !probe),
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

    /// One attempt of upstream's loop: radix's dial (connect, AUTH, then the URL's database as
    /// given, which Redis may refuse), then `PONG` to `PING`, then the pool. A failed dial is
    /// traced like radix's; `failed` is upstream's `hadConnError`.
    async fn attempt(&self, failed: &mut bool) -> Result<Pool, String> {
        let probe = match self.probe().await {
            Ok(probe) => probe,
            Err(message) => {
                *failed = true;
                crate::log::error(&format!("creating redis connection error : {message}"));
                return Err(message);
            }
        };
        if std::mem::take(failed) {
            crate::log::info("redis connection re-established after previous error");
        }
        self.set_up(probe).await
    }

    /// radix's `Dialer.Dial`: the connection, then AUTH and SELECT as it sends them, which
    /// Redis may refuse. Then the probe, which answers `PING`.
    async fn probe(&self) -> Result<(ServerConfig, Client), String> {
        let server = self.server().await?;
        let mut stream = self.dial(&server).await?;
        let mut commands = Vec::new();
        commands.extend(self.auth_arguments().map(|a| ("AUTH", a)));
        if !self.database.is_empty() {
            commands.push(("SELECT", vec![self.database.clone()]));
        }
        for (name, arguments) in commands {
            // Sent by hand: fred reads Redis' refusal of an unneeded AUTH as `OK`.
            command(&mut stream, name, &arguments).await?;
        }
        drop(stream);
        let probe = self
            .builder(server.clone(), true)
            .build()
            .map_err(|e| e.to_string())?;
        probe.init().await.map_err(|e| e.details().to_string())?;
        Ok((server, probe))
    }

    /// Go's dial of the server: `dial <network> <address>: connect: <errno>`, or a timeout.
    async fn dial(&self, server: &ServerConfig) -> Result<Stream, String> {
        let network = &self.network;
        let (address, connected) = match server {
            ServerConfig::Unix { path } => {
                let address = path.display().to_string();
                let connect = tokio::net::UnixStream::connect(path.clone());
                let connected = async { connect.await.map(|s| Box::new(s) as Stream) };
                (address, self.within_timeout(connected).await)
            }
            ServerConfig::Centralized { server } => {
                let address = crate::net::join_host_port(&server.host, server.port.into());
                let connect =
                    tokio::net::TcpStream::connect((server.host.to_string(), server.port));
                let connected = async { connect.await.map(|s| Box::new(s) as Stream) };
                (address, self.within_timeout(connected).await)
            }
            _ => return Err(format!("dial {network}: unsupported server")),
        };
        match connected {
            Some(Ok(stream)) => Ok(stream),
            Some(Err(e)) if e.raw_os_error().is_some() => Err(format!(
                "dial {network} {address}: connect: {}",
                crate::go::errno_text(&e)
            )),
            Some(Err(e)) => Err(format!("dial {network} {address}: {e}")),
            None => Err(format!("dial {network} {address}: i/o timeout")),
        }
    }

    /// `connect` within the dial timeout (none for 0); `None` once it has passed.
    async fn within_timeout<T>(
        &self,
        connect: impl std::future::Future<Output = std::io::Result<T>>,
    ) -> Option<std::io::Result<T>> {
        match self.timeout.is_zero() {
            true => Some(connect.await),
            false => tokio::time::timeout(self.timeout, connect).await.ok(),
        }
    }

    /// The probe's `PING`, then the pool.
    async fn set_up(&self, (server, probe): (ServerConfig, Client)) -> Result<Pool, String> {
        let checked = async {
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
                "{}_URL database {} is not supported",
                self.prefix,
                crate::go::quote(&self.database)
            ));
        }
        let pool = self
            .builder(server, false)
            .set_policy(radix_reconnects())
            .build_pool(self.pool_size)
            .map_err(|e| e.to_string())?;
        pool.init().await.map_err(|e| e.details().to_string())?;
        Ok(pool)
    }

    /// `newClientImpl`'s startup, with its log lines: retries until Redis answers `PING`,
    /// waiting as `REDIS_STARTUP_*` say, and gives up after `REDIS_STARTUP_MAX_ELAPSED_TIME`
    /// (never for 0) or when `cancelled` resolves. Then the pool, which reconnects lost
    /// connections and pings every `5s / size` like radix's.
    pub async fn connect(
        &self,
        startup: &StartupBackoff,
        cancelled: impl std::future::Future<Output = ()>,
    ) -> Result<Pool, String> {
        let masked = &self.masked_url;
        crate::log::warn(&format!(
            "connecting to redis on {masked} with pool size {}",
            self.pool_size_setting
        ));
        // createDialer.
        if !self.auth.is_empty() {
            match self.auth.split_once(':') {
                Some((user, _)) => crate::log::warn(&format!(
                    "enabling authentication to redis {masked} with user {user}"
                )),
                None => crate::log::warn(&format!(
                    "enabling authentication to redis {masked} without user"
                )),
            }
        }
        if self.pipeline_limit > 0 {
            crate::log::warn(&format!(
                "REDIS_PIPELINE_LIMIT={} is deprecated and has no effect in radix v4. Write buffering is controlled solely by REDIS_PIPELINE_WINDOW.",
                self.pipeline_limit
            ));
        }
        match self.pool_on_empty_behavior.to_uppercase().as_str() {
            "WAIT" => crate::log::warn(&format!(
                "Redis pool {masked}: WAIT is default in radix v4 (blocks until connection available)"
            )),
            "CREATE" => return Err("REDIS_POOL_ON_EMPTY_BEHAVIOR=CREATE is not supported in radix v4. Pool will block instead of creating overflow connections. Remove this setting or set to WAIT, and consider increasing REDIS_POOL_SIZE.".into()),
            "ERROR" => return Err("REDIS_POOL_ON_EMPTY_BEHAVIOR=ERROR is not supported in radix v4. Pool will block instead of failing fast. Remove this setting or set to WAIT, and use context timeouts for fail-fast behavior.".into()),
            _ => crate::log::warn(&format!(
                "Redis pool {masked}: using v4 default (fixed size={}, blocks when full)",
                self.pool_size_setting
            )),
        }
        let start = std::time::Instant::now();
        let mut attempt = 0;
        let mut failed = false;
        tokio::pin!(cancelled);
        let pool = loop {
            match self.redis_type.to_lowercase().as_str() {
                // Upstream logs REDIS_URL as given, credentials included (docs/parity.md §7).
                "single" => crate::log::warn(&format!("Creating single with urls {masked}")),
                "cluster" | "sentinel" => {
                    return Err(format!(
                        "{}_TYPE={} is not supported yet",
                        self.prefix, self.redis_type
                    ));
                }
                _ => return Err(format!("Unrecognized redis type {}", self.redis_type)),
            }
            let error = match self.attempt(&mut failed).await {
                Ok(pool) => break pool,
                Err(message) => message,
            };
            let elapsed = start.elapsed();
            let rounded = (elapsed.as_nanos() as i64 + 500_000) / 1_000_000 * 1_000_000;
            if !startup.max_elapsed.is_zero() && elapsed >= startup.max_elapsed {
                return Err(format!(
                    "timed out waiting for Redis connection to {masked} after {}: {error}",
                    crate::go::format_duration(rounded)
                ));
            }
            let wait = startup.duration(attempt);
            crate::log::warn(&format!(
                "Retrying Redis connection to {masked} in {} (elapsed: {}): {error}",
                crate::go::format_duration(wait.as_nanos() as i64),
                crate::go::format_duration(rounded)
            ));
            tokio::select! {
                () = tokio::time::sleep(wait) => {}
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

/// A dialled connection to Redis: TCP or Unix.
type Stream = Box<dyn Io>;
trait Io: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> Io for T {}

/// One command on `stream`, as radix's `conn.Do` during its dial: an error reply is
/// `response returned from Conn: <error>`.
async fn command(stream: &mut Stream, name: &str, arguments: &[String]) -> Result<(), String> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    let mut request = format!("*{}\r\n", arguments.len() + 1).into_bytes();
    for argument in std::iter::once(name).chain(arguments.iter().map(String::as_str)) {
        request.extend(format!("${}\r\n", argument.len()).into_bytes());
        request.extend(argument.as_bytes());
        request.extend(b"\r\n");
    }
    stream
        .write_all(&request)
        .await
        .map_err(|e| e.to_string())?;
    // AUTH and SELECT answer one line: `+OK`, or `-<error>`.
    let mut reply = Vec::new();
    let read = tokio::io::BufReader::new(stream)
        .read_until(b'\n', &mut reply)
        .await
        .map_err(|e| e.to_string())?;
    if read == 0 {
        return Err("EOF".into());
    }
    let reply = String::from_utf8_lossy(&reply);
    let reply = reply.trim_end_matches(['\r', '\n']);
    match reply.strip_prefix('-') {
        Some(error) => Err(format!("response returned from Conn: {error}")),
        None => Ok(()),
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

/// radix's reconnection after a lost connection: 125 ms, doubled on each failure up to 4 s.
fn radix_reconnects() -> ReconnectPolicy {
    let mut policy = ReconnectPolicy::new_exponential(0, 125, 4000, 2);
    policy.set_jitter(0);
    policy
}

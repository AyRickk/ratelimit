//! Differential tests (docs/design.md D32): every scenario runs against envoyproxy/ratelimit
//! itself (the Go program in `tools/oracle`) and against this implementation, each with its own
//! Redis, on the same simulated clock. Responses and the final Redis contents must be identical.
//!
//! Needs `go` and a Redis-compatible server: `redis-server` by default, or the binary named by
//! `RATELIMIT_REDIS_SERVER` (e.g. `valkey-server`). Both the oracle and this implementation
//! use that server. Without them the tests are skipped, unless `RATELIMIT_REQUIRE_DIFFERENTIAL`
//! is set (as in CI), in which case they fail.

use fred::prelude::*;
use fred::types::{ClusterHash, CustomCommand};
use ratelimit::config::{self, ConfigFile, LoadOptions};
use ratelimit::grpc;
use ratelimit::limiter::Clock;
use ratelimit::redis::{RedisCache, RedisCacheSettings};
use ratelimit::service::{LimitHeaders, RateLimitService, ServiceSettings};
use ratelimit::stats::{
    DogStatsdSettings, DogStatsdSink, Mogrifier, Sink, Stats, StatsdProtocol, StatsdSink,
};
use ratelimit_proto::envoy::extensions::common::ratelimit::v3::{
    RateLimitDescriptor, rate_limit_descriptor::Entry,
};
use ratelimit_proto::envoy::service::ratelimit::v3::{
    RateLimitRequest, RateLimitResponse, rate_limit_service_client::RateLimitServiceClient,
};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpListener;
use tokio::process::{Child, Command};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Channel;

#[derive(Default)]
struct ManualClock(AtomicI64);

impl Clock for ManualClock {
    fn unix_now(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// The Redis-compatible server to run.
fn redis_server() -> String {
    std::env::var("RATELIMIT_REDIS_SERVER").unwrap_or_else(|_| "redis-server".to_string())
}

fn tools_available() -> bool {
    let found = |tool: &str| {
        std::process::Command::new(tool)
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok()
    };
    let available = found("go") && found(&redis_server());
    if !available {
        assert!(
            std::env::var_os("RATELIMIT_REQUIRE_DIFFERENTIAL").is_none(),
            "differential tests need `go` and `{}`",
            redis_server()
        );
        eprintln!(
            "skipping: differential tests need `go` and `{}`",
            redis_server()
        );
    }
    available
}

/// Builds the Go oracle once per test binary.
fn oracle_binary() -> PathBuf {
    static BINARY: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    BINARY
        .get_or_init(|| {
            let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
            let binary = root.join("target/oracle");
            let status = std::process::Command::new("go")
                .args(["build", "-o"])
                .arg(&binary)
                .arg(".")
                .current_dir(root.join("tools/oracle"))
                .status()
                .expect("run go build");
            assert!(status.success(), "go build failed");
            binary
        })
        .clone()
}

async fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// A throwaway Redis server, stopped when dropped.
struct Redis {
    _process: Child,
    /// The harness's own connection, for dumps and setup; its commands are not compared.
    client: fred::clients::Client,
    client_address: String,
    address: String,
    monitor: Monitor,
}

/// Every command the server receives, as `MONITOR` reports it.
struct Monitor {
    lines: Arc<std::sync::Mutex<Vec<String>>>,
}

impl Monitor {
    async fn start(address: &str) -> Self {
        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let (read, mut write) = stream.into_split();
        tokio::io::AsyncWriteExt::write_all(&mut write, b"*1\r\n$7\r\nMONITOR\r\n")
            .await
            .unwrap();
        let mut lines = BufReader::new(read).lines();
        assert_eq!(lines.next_line().await.unwrap().as_deref(), Some("+OK"));
        let collected = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = collected.clone();
        tokio::spawn(async move {
            let _write = write; // keep the connection open
            while let Ok(Some(line)) = lines.next_line().await {
                sink.lock().unwrap().push(line);
            }
        });
        Self { lines: collected }
    }
}

impl Redis {
    async fn start() -> Self {
        // The port is free when chosen but may be taken before the server binds it (by another
        // test's server, which this one would then share): try again on another port.
        for _ in 0..5 {
            if let Some(redis) = Self::start_on(free_port().await).await {
                return redis;
            }
        }
        panic!("could not start {}", redis_server());
    }

    /// The server started on `port`, once it answers and is the one started here.
    async fn start_on(port: u16) -> Option<Self> {
        let mut process = Command::new(redis_server())
            .args([
                "--port",
                &port.to_string(),
                "--save",
                "",
                "--appendonly",
                "no",
            ])
            .stdout(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("start the Redis server");
        let address = format!("127.0.0.1:{port}");
        let config = Config::from_url(&format!("redis://{address}")).unwrap();
        let client = Builder::from_config(config).build().unwrap();
        let mut ready = false;
        for _ in 0..250 {
            if process.try_wait().unwrap().is_some() {
                return None;
            }
            if client.init().await.is_ok() {
                ready = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let server: String = client
            .custom(
                CustomCommand::new_static("INFO", ClusterHash::Random, false),
                vec!["server"],
            )
            .await
            .ok()?;
        let ours = format!("process_id:{}", process.id()?);
        if !ready || !server.lines().any(|line| line.trim() == ours) {
            return None;
        }
        let info: String = client
            .custom(
                CustomCommand::new_static("CLIENT", ClusterHash::Random, false),
                vec!["INFO"],
            )
            .await
            .unwrap();
        let client_address = info
            .split_whitespace()
            .find_map(|field| field.strip_prefix("addr="))
            .unwrap()
            .to_string();
        let monitor = Monitor::start(&address).await;
        Some(Self {
            _process: process,
            client,
            client_address,
            address,
            monitor,
        })
    }

    /// A new connection, for the implementation under test.
    async fn connect(&self) -> fred::clients::Client {
        let config = Config::from_url(&format!("redis://{}", self.address)).unwrap();
        let client = Builder::from_config(config).build().unwrap();
        client.init().await.unwrap();
        client
    }

    /// The data commands received so far from anyone but the harness, in order, as `MONITOR`
    /// renders them (commands run by Lua scripts included).
    async fn commands(&self) -> Vec<String> {
        // Wait until the monitor has seen everything sent before this point.
        let marker = format!("sync-{}", self.monitor.lines.lock().unwrap().len());
        let _: String = self
            .client
            .custom(
                CustomCommand::new_static("ECHO", ClusterHash::Random, false),
                vec![marker.clone()],
            )
            .await
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !self
            .monitor
            .lines
            .lock()
            .unwrap()
            .iter()
            .any(|l| l.contains(&marker))
        {
            assert!(
                std::time::Instant::now() < deadline,
                "MONITOR did not report {marker}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        const CONNECTION: &[&str] = &[
            "\"CLIENT\"",
            "\"HELLO\"",
            "\"PING\"",
            "\"COMMAND\"",
            "\"INFO\"",
            "\"SELECT\"",
        ];
        self.monitor
            .lines
            .lock()
            .unwrap()
            .iter()
            .filter_map(|line| {
                let (source, command) = line.split_once("] ")?;
                let from_harness = source.ends_with(&format!(" {}", self.client_address));
                let connection = CONNECTION.iter().any(|c| command.starts_with(c));
                (!from_harness && !connection).then(|| command.to_string())
            })
            .collect()
    }

    /// Every key with its value and absolute expiry time (Unix seconds).
    async fn dump(&self) -> BTreeMap<String, (String, i64)> {
        let keys: Vec<String> = self
            .client
            .custom(
                CustomCommand::new_static("KEYS", ClusterHash::Random, false),
                vec!["*"],
            )
            .await
            .unwrap();
        // Absolute expiry times (EXPIRETIME), so the result does not depend on when each key is
        // read.
        let pipeline = self.client.pipeline();
        for key in &keys {
            let () = pipeline.get(key.as_str()).await.unwrap();
            let expiretime = CustomCommand::new_static("EXPIRETIME", ClusterHash::FirstKey, false);
            let () = pipeline
                .custom(expiretime, vec![key.as_str()])
                .await
                .unwrap();
        }
        let replies: Vec<Value> = if keys.is_empty() {
            Vec::new()
        } else {
            pipeline.all().await.unwrap()
        };
        keys.into_iter()
            .zip(replies.chunks(2))
            // A key may expire between KEYS and GET.
            .filter(|(_, reply)| !reply[0].is_null())
            .map(|(key, reply)| {
                let value: String = reply[0].clone().convert().unwrap();
                let expires: i64 = reply[1].clone().convert().unwrap();
                (key, (value, expires))
            })
            .collect()
    }
}

/// envoyproxy/ratelimit, controlled over HTTP.
struct Oracle {
    _process: Child,
    client: RateLimitServiceClient<Channel>,
    control: String,
}

/// The environment that configures the oracle's service like `settings`. Upstream reads it on
/// every config load. Nothing else is inherited, so a variable set on the developer's machine
/// cannot change the oracle's answers.
fn oracle_environment(settings: &Settings) -> Vec<(&'static str, String)> {
    let load = &settings.load;
    let extra_tags: Vec<String> = settings
        .extra_tags
        .iter()
        .map(|(key, value)| format!("{key}:{value}"))
        .collect();
    let dogstatsd = settings.dogstatsd.as_ref().map(DogStatsd::environment);
    let settings = &settings.service;
    let mut environment = vec![
        ("EXTRA_TAGS", extra_tags.join(",")),
        (
            "SANITIZE_DESCRIPTOR_METRIC_DOTS",
            load.sanitize_descriptor_metric_dots.to_string(),
        ),
        ("SHADOW_MODE", settings.global_shadow_mode.to_string()),
        ("QUOTA_MODE", settings.global_quota_mode.to_string()),
        (
            "RESPONSE_DYNAMIC_METADATA",
            settings.response_dynamic_metadata.to_string(),
        ),
        (
            "LIMIT_PER_UNIT_HEADERS_ENABLED",
            settings.per_unit_headers.to_string(),
        ),
        (
            "LIMIT_RESPONSE_HEADERS_ENABLED",
            settings.response_headers.is_some().to_string(),
        ),
        (
            "LIMIT_REQUEST_HEADERS_ENABLED",
            settings.request_headers.is_some().to_string(),
        ),
    ];
    if let Some(names) = &settings.response_headers {
        environment.extend([
            ("LIMIT_LIMIT_HEADER", names.limit.clone()),
            ("LIMIT_REMAINING_HEADER", names.remaining.clone()),
            ("LIMIT_RESET_HEADER", names.reset.clone()),
        ]);
    }
    if let Some(names) = &settings.request_headers {
        environment.extend([
            ("LIMIT_REQUEST_LIMIT_HEADER", names.limit.clone()),
            ("LIMIT_REQUEST_REMAINING_HEADER", names.remaining.clone()),
            ("LIMIT_REQUEST_RESET_HEADER", names.reset.clone()),
        ]);
    }
    environment.extend(dogstatsd.into_iter().flatten());
    environment
}

impl Oracle {
    async fn start(
        redis: &Redis,
        all: &Settings,
        statsd: Option<&StatsdServer>,
        dogstatsd: Option<&DogStatsdServer>,
    ) -> Self {
        let settings = &all.cache;
        let near_limit_ratio = settings.near_limit_ratio.to_string();
        let local_cache_size = settings.local_cache_size_in_bytes.to_string();
        let stop_increment = format!(
            "-stop-cache-key-increment={}",
            settings.stop_cache_key_increment_when_overlimit
        );
        let mut process = Command::new(oracle_binary())
            .args(["-redis", &redis.address])
            .args(["-cache-key-prefix", &settings.cache_key_prefix])
            .args(["-local-cache-size", &local_cache_size, &stop_increment])
            .args(["-near-limit-ratio", &near_limit_ratio])
            .args(statsd.map(|server| format!("-statsd={}", server.address)))
            .args(dogstatsd.map(|server| format!("-dogstatsd={}", server.address)))
            .env_clear()
            .envs(oracle_environment(all))
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("start oracle");
        let mut lines = BufReader::new(process.stdout.take().unwrap()).lines();
        let address = |name: &str, line: Option<String>| {
            let line = line.expect("oracle output");
            line.strip_prefix(&format!("{name} "))
                .expect("oracle address")
                .to_string()
        };
        // The oracle waits for Redis before listening; a timeout turns a hang into a failure.
        let mut next_line = async || {
            tokio::time::timeout(std::time::Duration::from_secs(30), lines.next_line())
                .await
                .expect("the oracle did not start within 30 seconds")
                .unwrap()
        };
        let grpc = address("grpc", next_line().await);
        let control = address("control", next_line().await);
        let client = RateLimitServiceClient::connect(format!("http://{grpc}"))
            .await
            .unwrap();
        Self {
            _process: process,
            client,
            control,
        }
    }

    async fn put(&self, path: &str, body: String) {
        let (status, response) = self.send("PUT", path, &body).await;
        assert_eq!(status, 200, "oracle {path}: {response}");
    }

    /// Sends an HTTP/1.0 request (so the answer is not chunked); returns the status and body.
    async fn send(&self, method: &str, path: &str, body: &str) -> (u16, String) {
        let mut stream = tokio::net::TcpStream::connect(&self.control).await.unwrap();
        let request = format!(
            "{method} {path} HTTP/1.0\r\nHost: oracle\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        tokio::io::AsyncWriteExt::write_all(&mut stream, request.as_bytes())
            .await
            .unwrap();
        let mut response = String::new();
        tokio::io::AsyncReadExt::read_to_string(&mut stream, &mut response)
            .await
            .unwrap();
        let status = response[9..12].parse().unwrap();
        let body = response.split_once("\r\n\r\n").unwrap().1.to_string();
        (status, body)
    }

    /// The service's counters above 0 (one that wrapped around to 0 is left out too).
    async fn stats(&self) -> BTreeMap<String, u64> {
        let (status, body) = self.send("GET", "/stats", "").await;
        assert_eq!(status, 200, "oracle /stats: {body}");
        body.lines()
            .map(|line| {
                let (name, value) = line.rsplit_once(' ').unwrap();
                (name.to_string(), value.parse().unwrap())
            })
            .filter(|(_, value)| *value > 0)
            .collect()
    }
}

/// This implementation, served over gRPC like the oracle.
struct Subject {
    client: RateLimitServiceClient<Channel>,
    service: Arc<RateLimitService<RedisCache>>,
    clock: Arc<ManualClock>,
    stats: Arc<Stats>,
    sink: Option<Sink>,
}

impl Subject {
    async fn start(
        redis: &Redis,
        settings: &Settings,
        statsd: Option<&StatsdServer>,
        dogstatsd: Option<&DogStatsdServer>,
    ) -> Self {
        let clock = Arc::new(ManualClock::default());
        let stats = Arc::new(Stats::new(&settings.extra_tags));
        let mut sink = statsd.map(|server| {
            let port = server.address.port();
            Sink::Statsd(StatsdSink::new("127.0.0.1", port, StatsdProtocol::Tcp))
        });
        if let (Some(server), Some(dog)) = (dogstatsd, &settings.dogstatsd) {
            let port = server.address.port();
            let sink_settings = dog.settings();
            sink = Some(Sink::Dogstatsd(
                DogStatsdSink::new("127.0.0.1", port, sink_settings).unwrap(),
            ));
        }
        let cache = RedisCache::new(
            redis.connect().await,
            clock.clone(),
            stats.clone(),
            settings.cache.clone(),
        );
        let service = Arc::new(RateLimitService::new(
            cache,
            clock.clone(),
            stats.clone(),
            settings.service.clone(),
        ));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(grpc::server(service.clone()))
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        let client = RateLimitServiceClient::connect(format!("http://{address}"))
            .await
            .unwrap();
        Self {
            client,
            service,
            clock,
            stats,
            sink,
        }
    }
}

struct Harness {
    oracle: Oracle,
    oracle_redis: Redis,
    subject: Subject,
    subject_redis: Redis,
    load_options: LoadOptions,
    /// With `Settings::statsd`, what the oracle and this implementation sent to statsd.
    statsd: Option<StatsdServers>,
    /// With `Settings::dogstatsd`, what they sent to DogStatsD.
    dogstatsd: Option<DogStatsdServers>,
    flushes: std::sync::atomic::AtomicUsize,
}

/// Everything both sides are configured with.
#[derive(Default)]
struct Settings {
    cache: RedisCacheSettings,
    service: ServiceSettings,
    load: LoadOptions,
    /// `EXTRA_TAGS`.
    extra_tags: BTreeMap<String, String>,
    /// Also send the stats to a statsd server for each side, and compare what they receive.
    statsd: bool,
    /// The same with DogStatsD, configured so.
    dogstatsd: Option<DogStatsd>,
}

/// A mogrifier for both sides: key, pattern, name, tags.
type MogrifierSpec = (
    &'static str,
    &'static str,
    &'static str,
    Vec<(&'static str, &'static str)>,
);

/// The DogStatsD sink's configuration, for both sides.
#[derive(Default, Clone)]
struct DogStatsd {
    /// `DD_ENTITY_ID`, `DD_ENV`, `DD_SERVICE`, `DD_VERSION`.
    entity_id: String,
    env: String,
    service: String,
    version: String,
    mogrifiers: Vec<MogrifierSpec>,
}

impl DogStatsd {
    fn settings(&self) -> DogStatsdSettings {
        DogStatsdSettings {
            entity_id: self.entity_id.clone(),
            env: self.env.clone(),
            service: self.service.clone(),
            version: self.version.clone(),
            mogrifiers: self
                .mogrifiers
                .iter()
                .map(|(key, pattern, name, tags)| {
                    let tags = tags.iter().map(|(k, v)| (k.to_string(), v.to_string()));
                    Mogrifier::new(key, pattern, name, tags.collect()).unwrap()
                })
                .collect(),
            ..Default::default()
        }
    }

    /// The oracle's environment for the same configuration (`DOG_STATSD_MOGRIFIER_<KEY>_*`).
    fn environment(&self) -> Vec<(&'static str, String)> {
        let leak = |s: String| -> &'static str { Box::leak(s.into_boxed_str()) };
        let mut environment = vec![
            ("DD_ENTITY_ID", self.entity_id.clone()),
            ("DD_ENV", self.env.clone()),
            ("DD_SERVICE", self.service.clone()),
            ("DD_VERSION", self.version.clone()),
        ];
        let keys: Vec<&str> = self.mogrifiers.iter().map(|m| m.0).collect();
        environment.push(("USE_DOG_STATSD_MOGRIFIERS", keys.join(",")));
        for (key, pattern, name, tags) in &self.mogrifiers {
            let var = |field: &str| leak(format!("DOG_STATSD_MOGRIFIER_{key}_{field}"));
            let tags: Vec<String> = tags.iter().map(|(k, v)| format!("{k}:{v}")).collect();
            environment.push((var("PATTERN"), pattern.to_string()));
            environment.push((var("NAME"), name.to_string()));
            environment.push((var("TAGS"), tags.join(",")));
        }
        environment
    }
}

/// The DogStatsD servers each side sends its stats to.
struct DogStatsdServers {
    oracle: DogStatsdServer,
    subject: DogStatsdServer,
}

/// A DogStatsD server over UDP, keeping every datagram it receives.
struct DogStatsdServer {
    address: std::net::SocketAddr,
    datagrams: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
}

impl DogStatsdServer {
    async fn start() -> Self {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let datagrams = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = datagrams.clone();
        tokio::spawn(async move {
            let mut buffer = vec![0; 65536];
            while let Ok(n) = socket.recv(&mut buffer).await {
                sink.lock().unwrap().push(buffer[..n].to_vec());
            }
        });
        Self { address, datagrams }
    }

    /// Every line received once `count` flushes have arrived and nothing more for 300 ms (the
    /// client sends every 100 ms, from 32 buffers in turn), sorted; datadog-go's telemetry and
    /// the oracle's Redis client stats are left out. Every datagram holds whole lines, at most
    /// 1432 bytes of them.
    async fn lines(&self, count: usize) -> Vec<String> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let mut seen = 0;
        let mut quiet_since = std::time::Instant::now();
        loop {
            let datagrams = self.datagrams.lock().unwrap().clone();
            if datagrams.len() != seen {
                seen = datagrams.len();
                quiet_since = std::time::Instant::now();
            }
            let text: Vec<String> = datagrams
                .iter()
                .map(|d| String::from_utf8(d.clone()).unwrap())
                .collect();
            let flushes = text
                .iter()
                .flat_map(|d| d.lines())
                .filter(|line| line.starts_with("harness.flushed:"))
                .count();
            if flushes >= count && quiet_since.elapsed() >= std::time::Duration::from_millis(300) {
                for datagram in &text {
                    assert!(
                        datagram.len() <= 1432 && datagram.ends_with('\n'),
                        "datagram {datagram:?}"
                    );
                }
                let mut lines: Vec<String> = text
                    .iter()
                    .flat_map(|d| d.lines())
                    .filter(|line| {
                        !line.starts_with("datadog.dogstatsd.client.")
                            && !line.starts_with("redis.")
                    })
                    .map(str::to_string)
                    .collect();
                lines.sort();
                return lines;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "DogStatsD received {text:?}, expected {count} flushes"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }
}

/// The flush that marks the end of each flush in a statsd stream.
const FLUSHED: &str = "harness.flushed:1|c";

/// The statsd servers each side sends its stats to.
struct StatsdServers {
    oracle: StatsdServer,
    subject: StatsdServer,
}

/// A statsd server over TCP, keeping every line it receives.
struct StatsdServer {
    address: std::net::SocketAddr,
    lines: Arc<std::sync::Mutex<Vec<String>>>,
}

impl StatsdServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let lines = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = lines.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let sink = sink.clone();
                tokio::spawn(async move {
                    let mut stream = BufReader::new(stream).lines();
                    while let Ok(Some(line)) = stream.next_line().await {
                        sink.lock().unwrap().push(line);
                    }
                });
            }
        });
        Self { address, lines }
    }

    /// The lines of the first `count` flushes, each sorted (a flush's order is not defined).
    /// The stats of the oracle's own Redis client are left out.
    async fn flushes(&self, count: usize) -> Vec<Vec<String>> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let lines = self.lines.lock().unwrap().clone();
            let mut flushes = lines
                .split(|line| line == FLUSHED)
                .map(|flush| {
                    let mut flush: Vec<String> = flush
                        .iter()
                        .filter(|line| !line.starts_with("redis."))
                        .cloned()
                        .collect();
                    flush.sort();
                    flush
                })
                .collect::<Vec<_>>();
            flushes.pop(); // after the last marker
            if flushes.len() >= count {
                flushes.truncate(count);
                return flushes;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "statsd received {lines:?}, expected {count} flushes"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }
}

impl Harness {
    async fn start(config: &str) -> Self {
        Self::with_settings(config, RedisCacheSettings::default()).await
    }

    async fn with_prefix(config: &str, prefix: &str) -> Self {
        let settings = RedisCacheSettings {
            cache_key_prefix: prefix.to_string(),
            ..Default::default()
        };
        Self::with_settings(config, settings).await
    }

    async fn with_settings(config: &str, cache: RedisCacheSettings) -> Self {
        let settings = Settings {
            cache,
            ..Default::default()
        };
        Self::configured(config, settings).await
    }

    async fn with_service_settings(config: &str, service: ServiceSettings) -> Self {
        let settings = Settings {
            service,
            ..Default::default()
        };
        Self::configured(config, settings).await
    }

    /// Both sides configured alike: the settings for this implementation, the matching flags
    /// and environment for the oracle.
    async fn configured(config: &str, settings: Settings) -> Self {
        let oracle_redis = Redis::start().await;
        let subject_redis = Redis::start().await;
        let statsd = match settings.statsd {
            true => Some(StatsdServers {
                oracle: StatsdServer::start().await,
                subject: StatsdServer::start().await,
            }),
            false => None,
        };
        let dogstatsd = match settings.dogstatsd {
            Some(_) => Some(DogStatsdServers {
                oracle: DogStatsdServer::start().await,
                subject: DogStatsdServer::start().await,
            }),
            None => None,
        };
        let oracle = Oracle::start(
            &oracle_redis,
            &settings,
            statsd.as_ref().map(|s| &s.oracle),
            dogstatsd.as_ref().map(|s| &s.oracle),
        )
        .await;
        let subject = Subject::start(
            &subject_redis,
            &settings,
            statsd.as_ref().map(|s| &s.subject),
            dogstatsd.as_ref().map(|s| &s.subject),
        )
        .await;
        let mut h = Self {
            oracle,
            oracle_redis,
            subject,
            subject_redis,
            load_options: settings.load,
            statsd,
            dogstatsd,
            flushes: Default::default(),
        };
        h.load_config(config).await.unwrap();
        h
    }

    /// Loads `config` on both sides, as a reload; returns upstream's error if it does not load,
    /// after checking ours is the same and both kept the previous config.
    async fn load_config(&mut self, config: &str) -> Result<(), String> {
        let (status, error) = self.oracle.send("PUT", "/config", config).await;
        let file = ConfigFile {
            name: "config.yaml".into(),
            content: config.into(),
        };
        let loaded = config::load(&[file], &self.load_options);
        let actual = loaded.as_ref().map(|_| ()).map_err(|e| e.to_string());
        self.subject.service.update_config(loaded);
        let expected = match status {
            200 => Ok(()),
            _ => Err(error.trim_end().to_string()),
        };
        assert_eq!(actual, expected, "loading {config}");
        self.assert_same_stats().await;
        expected
    }

    /// Flushes the stats on both sides and compares the counters, and what statsd received.
    async fn assert_same_stats(&self) {
        let expected = self.oracle.stats().await;
        if let Some(sink) = &self.subject.sink {
            self.subject.stats.flush(sink).await;
            sink.flush_counter("harness.flushed", 1);
            sink.flush().await;
        }
        assert_eq!(self.subject.stats.snapshot(), expected, "counters");
        if let Some(dogstatsd) = &self.dogstatsd {
            let count = self.flushes.fetch_add(1, Ordering::SeqCst) + 1;
            assert_eq!(
                dogstatsd.subject.lines(count).await,
                dogstatsd.oracle.lines(count).await,
                "DogStatsD lines"
            );
        }
        if let Some(statsd) = &self.statsd {
            let count = self.flushes.fetch_add(1, Ordering::SeqCst) + 1;
            assert_eq!(
                statsd.subject.flushes(count).await,
                statsd.oracle.flushes(count).await,
                "statsd lines, per flush"
            );
        }
    }

    async fn at(&mut self, now: i64) {
        self.oracle.put("/time", now.to_string()).await;
        self.subject.clock.0.store(now, Ordering::SeqCst);
    }

    /// Sends `request` to both and checks they answer the same; returns the answer.
    async fn call(&mut self, request: RateLimitRequest) -> Result<RateLimitResponse, String> {
        let expected = self.oracle.client.should_rate_limit(request.clone()).await;
        let actual = self.subject.client.should_rate_limit(request.clone()).await;
        let simplify = |r: Result<tonic::Response<RateLimitResponse>, tonic::Status>| {
            r.map(tonic::Response::into_inner)
                .map_err(|s| format!("{:?}: {}", s.code(), s.message()))
        };
        let (mut expected, actual) = (simplify(expected), simplify(actual));
        if let Ok(response) = &mut expected {
            single_space_limit_text(response);
        }
        assert_eq!(actual, expected, "response to {request:?}");
        self.assert_same_stats().await;
        actual
    }

    /// Same keys and values, and expiry times within a second: TTLs run on the servers' real
    /// clocks, and each side sets them a moment apart.
    async fn assert_same_redis(&self) {
        if std::env::var_os("RATELIMIT_SHOW_COMMANDS").is_some() {
            eprintln!("{:#?}", self.oracle_redis.commands().await);
        }
        assert_eq!(
            self.subject_redis.commands().await,
            self.oracle_redis.commands().await,
            "commands received by Redis"
        );
        let (expected, actual) = tokio::join!(self.oracle_redis.dump(), self.subject_redis.dump());
        assert!(
            !expected.is_empty(),
            "the scenario should leave keys in Redis"
        );
        let values = |dump: &BTreeMap<String, (String, i64)>| {
            dump.iter()
                .map(|(k, (v, _))| (k.clone(), v.clone()))
                .collect::<Vec<_>>()
        };
        assert_eq!(values(&actual), values(&expected), "Redis keys and values");
        for (key, (_, expires)) in &expected {
            let actual_expires = actual[key].1;
            assert!(
                (actual_expires - expires).abs() <= 1,
                "expiry of {key}: {actual_expires}, upstream {expires}"
            );
        }
    }

    /// Stores a raw value under `key` in both Redis servers.
    async fn set_raw(&self, key: &str, value: &str) {
        for redis in [&self.oracle_redis, &self.subject_redis] {
            let () = redis
                .client
                .set(key, value, None, None, false)
                .await
                .unwrap();
        }
    }
}

/// Go's text format puts one or two spaces between fields, depending on the binary
/// (protobuf-go's `detrand`); ours always puts one. Applied to the oracle's
/// `descriptors[].limit` metadata.
fn single_space_limit_text(response: &mut RateLimitResponse) {
    use prost_types::value::Kind;
    let Some(metadata) = &mut response.dynamic_metadata else {
        return;
    };
    let Some(Kind::ListValue(descriptors)) = metadata
        .fields
        .get_mut("descriptors")
        .and_then(|v| v.kind.as_mut())
    else {
        return;
    };
    for descriptor in &mut descriptors.values {
        if let Some(Kind::StructValue(descriptor)) = &mut descriptor.kind
            && let Some(Kind::StringValue(limit)) = descriptor
                .fields
                .get_mut("limit")
                .and_then(|v| v.kind.as_mut())
        {
            *limit = limit.split_whitespace().collect::<Vec<_>>().join(" ");
        }
    }
}

fn request(domain: &str, descriptors: &[&[(&str, &str)]]) -> RateLimitRequest {
    RateLimitRequest {
        domain: domain.into(),
        descriptors: descriptors
            .iter()
            .map(|entries| RateLimitDescriptor {
                entries: entries
                    .iter()
                    .map(|(k, v)| Entry {
                        key: k.to_string(),
                        value: v.to_string(),
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        hits_addend: 0,
    }
}

#[tokio::test]
async fn fixed_window_counts_and_resets_like_upstream() {
    if !tools_available() {
        return;
    }
    let mut h = Harness::start(
        "
domain: d
descriptors:
  - key: k
    rate_limit: {unit: second, requests_per_unit: 2}
  - key: m
    rate_limit: {unit: minute, requests_per_unit: 10}
",
    )
    .await;

    h.at(1_000_000_000).await;
    for _ in 0..3 {
        h.call(request(
            "d",
            &[&[("k", "v")], &[("m", "x")], &[("none", "y")]],
        ))
        .await
        .unwrap();
    }
    h.at(1_000_000_001).await;
    h.call(request("d", &[&[("k", "v")]])).await.unwrap();
    h.at(1_000_000_059).await;
    h.call(request("d", &[&[("m", "x")]])).await.unwrap();
    h.assert_same_redis().await;
}

fn with_hits(
    mut request: RateLimitRequest,
    request_hits: u32,
    per_descriptor: &[Option<u64>],
) -> RateLimitRequest {
    request.hits_addend = request_hits;
    for (descriptor, hits) in request.descriptors.iter_mut().zip(per_descriptor) {
        descriptor.hits_addend = *hits;
    }
    request
}

fn refund(mut request: RateLimitRequest, hits: u64) -> RateLimitRequest {
    for descriptor in &mut request.descriptors {
        descriptor.hits_addend = Some(hits);
        descriptor.is_negative_hits = true;
    }
    request
}

const TOKENS: &str = "
domain: llm
descriptors:
  - key: user
    rate_limit: {unit: minute, requests_per_unit: 1000}
  - key: shadow
    shadow_mode: true
    rate_limit: {unit: hour, requests_per_unit: 1}
";

#[tokio::test]
async fn hits_addend_checks_and_counts_like_upstream() {
    if !tools_available() {
        return;
    }
    let mut h = Harness::start(TOKENS).await;
    h.at(1_700_000_000).await;
    let user = || request("llm", &[&[("user", "alice")]]);

    h.call(with_hits(user(), 0, &[Some(0)])).await.unwrap(); // check only
    h.call(with_hits(user(), 300, &[None])).await.unwrap(); // request-level
    h.call(with_hits(user(), 7, &[Some(600)])).await.unwrap(); // descriptor wins
    h.call(with_hits(user(), 0, &[Some(0)])).await.unwrap(); // at 900
    h.call(with_hits(user(), 0, &[Some(200)])).await.unwrap(); // over: 1100
    h.call(with_hits(user(), 0, &[Some(0)])).await.unwrap(); // still over
    h.assert_same_redis().await;
}

#[tokio::test]
async fn refunds_floor_at_zero_and_always_answer_ok() {
    if !tools_available() {
        return;
    }
    let mut h = Harness::start(TOKENS).await;
    h.at(1_700_000_000).await;
    let user = || request("llm", &[&[("user", "bob")]]);

    h.call(refund(user(), 50)).await.unwrap(); // absent key: nothing to refund
    h.call(with_hits(user(), 0, &[Some(1200)])).await.unwrap(); // over
    h.call(refund(user(), 500)).await.unwrap(); // back to 700
    h.call(refund(user(), 5000)).await.unwrap(); // floors at 0
    h.call(with_hits(user(), 0, &[Some(1)])).await.unwrap();
    h.assert_same_redis().await;
}

#[tokio::test]
async fn shadow_mode_rules_count_but_answer_ok() {
    if !tools_available() {
        return;
    }
    let mut h = Harness::start(TOKENS).await;
    h.at(1_700_000_000).await;
    for _ in 0..3 {
        h.call(request("llm", &[&[("shadow", "x")]])).await.unwrap();
    }
    h.assert_same_redis().await;
}

#[tokio::test]
async fn keys_follow_upstream_for_every_unit_and_option() {
    if !tools_available() {
        return;
    }
    let mut h = Harness::with_prefix(
        "
domain: keys
descriptors:
  - key: s
    rate_limit: {unit: second, requests_per_unit: 5}
  - key: m
    rate_limit: {unit: minute, requests_per_unit: 5}
  - key: h
    rate_limit: {unit: hour, requests_per_unit: 5}
  - key: d
    rate_limit: {unit: day, requests_per_unit: 5}
  - key: w
    rate_limit: {unit: week, requests_per_unit: 5}
  - key: mo
    rate_limit: {unit: month, requests_per_unit: 5}
  - key: y
    rate_limit: {unit: year, requests_per_unit: 5}
  - key: files
    value: files/*
    share_threshold: true
    rate_limit: {unit: minute, requests_per_unit: 5}
  - key: parent
    value: p
    descriptors:
      - key: child
        rate_limit: {unit: minute, requests_per_unit: 5}
  - key: open
    rate_limit: {unlimited: true, name: open, replaces: [{name: replaced}]}
  - key: replaced
    rate_limit: {unit: minute, requests_per_unit: 5, name: replaced}
",
        "prefix:",
    )
    .await;
    h.at(1_712_345_678).await;
    for unit in ["s", "m", "h", "d", "w", "mo", "y"] {
        h.call(request("keys", &[&[(unit, "v")]])).await.unwrap();
    }
    h.call(request(
        "keys",
        &[&[("files", "files/a.pdf")], &[("files", "files/b.csv")]],
    ))
    .await
    .unwrap();
    h.call(request("keys", &[&[("parent", "p"), ("child", "c")]]))
        .await
        .unwrap();
    h.call(request("keys", &[&[("open", "x")], &[("replaced", "y")]]))
        .await
        .unwrap();
    h.call(request("keys", &[&[("unknown", "x")]]))
        .await
        .unwrap();
    // A request limit override applies even without a matching rule.
    let mut overridden = request("keys", &[&[("free", "x")]]);
    overridden.descriptors[0].limit = Some(
        ratelimit_proto::envoy::extensions::common::ratelimit::v3::rate_limit_descriptor::RateLimitOverride {
            requests_per_unit: 1,
            unit: 2, // MINUTE
            unit_multiplier: None,
        },
    );
    h.call(overridden.clone()).await.unwrap();
    h.call(overridden).await.unwrap();
    h.assert_same_redis().await;
}

#[tokio::test]
async fn invalid_requests_fail_like_upstream() {
    if !tools_available() {
        return;
    }
    let mut h = Harness::start(TOKENS).await;
    h.at(1_700_000_000).await;
    assert!(h.call(request("", &[&[("user", "x")]])).await.is_err());
    assert!(h.call(request("llm", &[])).await.is_err());
}

#[tokio::test]
async fn huge_refunds_are_sent_as_signed_integers_like_upstream() {
    if !tools_available() {
        return;
    }
    let mut h = Harness::start(TOKENS).await;
    h.at(1_700_000_000).await;
    let user = || request("llm", &[&[("user", "dave")]]);

    h.call(with_hits(user(), 0, &[Some(1)])).await.unwrap();
    // u64::MAX goes over the wire as the signed integer -1, so the refund adds one.
    h.call(refund(user(), u64::MAX)).await.unwrap();
    h.call(with_hits(user(), 0, &[Some(u64::MAX)]))
        .await
        .unwrap();
    h.assert_same_redis().await;
}

#[tokio::test]
async fn redis_errors_are_reported_like_upstream() {
    if !tools_available() {
        return;
    }
    let mut h = Harness::start(TOKENS).await;
    h.at(1_700_000_000).await;
    let window = 1_700_000_000 / 60 * 60;
    h.set_raw(&format!("llm_user_carol_{window}"), "not a number")
        .await;

    let error = h
        .call(request("llm", &[&[("user", "carol")]]))
        .await
        .unwrap_err();
    assert!(error.contains("in pipeline returned error"), "{error}");
    h.call(refund(request("llm", &[&[("user", "carol")]]), 1))
        .await
        .unwrap_err();
}

#[tokio::test]
async fn refunds_reset_the_ttl_to_a_full_window() {
    if !tools_available() {
        return;
    }
    let mut h = Harness::start(TOKENS).await;
    h.at(1_700_000_000).await;
    let user = || request("llm", &[&[("user", "erin")]]);

    h.call(with_hits(user(), 0, &[Some(10)])).await.unwrap();
    // TTLs run on the servers' real clocks: let them drop before refunding.
    tokio::time::sleep(std::time::Duration::from_millis(2200)).await;
    h.call(refund(user(), 4)).await.unwrap();
    h.assert_same_redis().await;
    // A refund in a later window finds no key.
    h.at(1_700_000_060).await;
    h.call(refund(user(), 4)).await.unwrap();
}

#[tokio::test]
async fn empty_values_and_nested_shared_wildcards_make_upstream_keys() {
    if !tools_available() {
        return;
    }
    let mut h = Harness::start(
        "
domain: keys
descriptors:
  - key: k
    rate_limit: {unit: minute, requests_per_unit: 5}
  - key: user
    value: user/*
    share_threshold: true
    descriptors:
      - key: resource
        value: res/*
        share_threshold: true
        descriptors:
          - key: action
            rate_limit: {unit: minute, requests_per_unit: 5}
",
    )
    .await;
    h.at(1_712_345_678).await;
    h.call(request("keys", &[&[("k", "")]])).await.unwrap();
    h.call(request(
        "keys",
        &[&[
            ("user", "user/alice"),
            ("resource", "res/file1"),
            ("action", "read"),
        ]],
    ))
    .await
    .unwrap();
    h.call(request(
        "keys",
        &[&[
            ("user", "user/bob"),
            ("resource", "res/file2"),
            ("action", "write"),
        ]],
    ))
    .await
    .unwrap();
    h.assert_same_redis().await;
}

const OVERLIMIT: &str = "
domain: d
descriptors:
  - key: a
    rate_limit: {unit: minute, requests_per_unit: 5}
  - key: b
    rate_limit: {unit: minute, requests_per_unit: 10}
  - key: shadow
    shadow_mode: true
    rate_limit: {unit: minute, requests_per_unit: 1}
";

fn hits_each(descriptors: &[&[(&str, &str)]], hits: &[u64]) -> RateLimitRequest {
    let hits: Vec<_> = hits.iter().map(|h| Some(*h)).collect();
    with_hits(request("d", descriptors), 0, &hits)
}

#[tokio::test]
async fn stop_cache_key_increment_when_overlimit_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = RedisCacheSettings {
        stop_cache_key_increment_when_overlimit: true,
        ..Default::default()
    };
    let mut h = Harness::with_settings(OVERLIMIT, settings).await;
    h.at(1_700_000_000).await;
    let ab: &[&[(&str, &str)]] = &[&[("a", "x")], &[("b", "y")]];

    h.call(hits_each(ab, &[4, 1])).await.unwrap(); // nobody near: both counted
    h.call(hits_each(ab, &[2, 1])).await.unwrap(); // a goes over: only a counted, b gets 0
    h.call(hits_each(ab, &[1, 1])).await.unwrap();
    h.call(hits_each(&[&[("b", "y")], &[("none", "z")]], &[3, 1]))
        .await
        .unwrap();
    h.call(refund(request("d", &[&[("a", "x")]]), 3))
        .await
        .unwrap();
    h.assert_same_redis().await;
}

#[tokio::test]
async fn local_cache_skips_redis_for_keys_over_the_limit_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = RedisCacheSettings {
        local_cache_size_in_bytes: 1024 * 1024,
        ..Default::default()
    };
    let mut h = Harness::with_settings(OVERLIMIT, settings).await;
    h.at(1_700_000_000).await;
    let ab: &[&[(&str, &str)]] = &[&[("a", "x")], &[("b", "y")]];

    h.call(hits_each(ab, &[6, 1])).await.unwrap(); // a over: cached
    h.call(hits_each(ab, &[1, 1])).await.unwrap(); // a answered from the cache
    h.call(hits_each(&[&[("shadow", "s")]], &[2]))
        .await
        .unwrap(); // shadow rules are cached too
    h.call(hits_each(&[&[("shadow", "s")]], &[1]))
        .await
        .unwrap();
    h.call(refund(request("d", &[&[("a", "x")]]), 6))
        .await
        .unwrap(); // refunds ignore the cache
    h.call(hits_each(ab, &[1, 1])).await.unwrap(); // still cached
    h.at(1_700_000_060).await; // next window: a new key
    h.call(hits_each(ab, &[1, 1])).await.unwrap();
    h.assert_same_redis().await;
}

#[tokio::test]
async fn local_cache_and_stop_increment_together_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = RedisCacheSettings {
        local_cache_size_in_bytes: 1024 * 1024,
        stop_cache_key_increment_when_overlimit: true,
        ..Default::default()
    };
    let mut h = Harness::with_settings(OVERLIMIT, settings).await;
    h.at(1_700_000_000).await;
    let ab: &[&[(&str, &str)]] = &[&[("a", "x")], &[("b", "y")]];

    h.call(hits_each(ab, &[6, 1])).await.unwrap();
    // a is over in the local cache: nothing is incremented, b gets INCRBY 0.
    h.call(hits_each(ab, &[1, 2])).await.unwrap();
    h.call(hits_each(&[&[("b", "y")]], &[1])).await.unwrap();
    h.assert_same_redis().await;
}

#[tokio::test]
async fn keys_too_long_for_freecache_are_never_cached_like_upstream() {
    if !tools_available() {
        return;
    }
    // At freecache's 512 KiB minimum, entries over 2048 / 4 - 24 = 488 bytes are refused.
    let settings = RedisCacheSettings {
        local_cache_size_in_bytes: 1,
        ..Default::default()
    };
    let mut h = Harness::with_settings(OVERLIMIT, settings).await;
    h.at(1_700_000_000).await;
    let long = "v".repeat(600);
    let short = "v".repeat(470);
    for value in [&long, &short] {
        h.call(hits_each(&[&[("a", value)]], &[6])).await.unwrap();
        h.call(hits_each(&[&[("a", value)]], &[1])).await.unwrap();
    }
    h.assert_same_redis().await;
}

#[tokio::test]
async fn unreadable_counters_fail_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = RedisCacheSettings {
        stop_cache_key_increment_when_overlimit: true,
        ..Default::default()
    };
    let mut h = Harness::with_settings(OVERLIMIT, settings).await;
    h.at(1_700_000_000).await;
    let window = 1_700_000_000 / 60 * 60;
    h.set_raw(&format!("d_a_x_{window}"), "abc").await;
    let error = h.call(hits_each(&[&[("a", "x")]], &[1])).await.unwrap_err();
    assert!(error.contains("in parseUint"), "{error}");

    // A counter driven below zero cannot be read as an unsigned number either.
    let mut h = Harness::start(OVERLIMIT).await;
    h.at(1_700_000_000).await;
    h.call(hits_each(&[&[("b", "y")]], &[u64::MAX]))
        .await
        .unwrap_err();
}

#[tokio::test]
async fn stop_increment_branches_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = RedisCacheSettings {
        local_cache_size_in_bytes: 1024 * 1024,
        stop_cache_key_increment_when_overlimit: true,
        ..Default::default()
    };
    let mut h = Harness::with_settings(OVERLIMIT, settings).await;
    h.at(1_700_000_000).await;

    // A brand-new key next to one going over gets INCRBY 0: a ghost key at 0.
    h.call(hits_each(&[&[("a", "x")], &[("b", "new")]], &[6, 1]))
        .await
        .unwrap();
    // A shadow rule going over is counted and cached, but answers OK.
    h.call(hits_each(&[&[("shadow", "s")], &[("b", "y")]], &[2, 1]))
        .await
        .unwrap();
    // A refund next to a key answered by the local cache.
    let mut mixed = hits_each(&[&[("a", "x")], &[("b", "y")]], &[1, 3]);
    mixed.descriptors[1].is_negative_hits = true;
    h.call(mixed).await.unwrap();
    h.assert_same_redis().await;
}

#[tokio::test]
async fn a_full_local_cache_evicts_like_freecache() {
    if !tools_available() {
        return;
    }
    // freecache's minimum: 256 segments of 2 KiB, about 45 of these keys each.
    let settings = RedisCacheSettings {
        local_cache_size_in_bytes: 1,
        ..Default::default()
    };
    let mut h = Harness::with_settings(
        "
domain: d
descriptors:
  - key: k
    rate_limit: {unit: hour, requests_per_unit: 0}
",
        settings,
    )
    .await;
    const KEYS: usize = 30_000;
    const BATCH: usize = 100;
    let values: Vec<String> = (0..KEYS).map(|i| format!("v{i}")).collect();
    let batch = |chunk: &[String]| {
        let descriptors: Vec<Vec<(&str, &str)>> =
            chunk.iter().map(|v| vec![("k", v.as_str())]).collect();
        let refs: Vec<&[(&str, &str)]> = descriptors.iter().map(Vec::as_slice).collect();
        request("d", &refs)
    };

    let start = 1_700_000_000 / 3600 * 3600;
    for (n, chunk) in values.chunks(BATCH).enumerate() {
        // Time moves on, so access times differ and freecache's second chance comes into play.
        h.at(start + (n / 20) as i64).await;
        h.call(batch(chunk)).await.unwrap();
        // Re-reading early keys refreshes their access time.
        if n % 10 == 9 {
            h.call(batch(&values[..BATCH])).await.unwrap();
        }
    }
    // Probe every key: those still cached send nothing to Redis.
    for chunk in values.chunks(BATCH) {
        h.call(batch(chunk)).await.unwrap();
    }
    h.assert_same_redis().await;
}

/// Response headers name the descriptor closest to its limit: fewest remaining, the first on a
/// tie, unless a regular descriptor is over its limit; the reset re-reads the clock.
#[tokio::test]
async fn response_headers_describe_the_closest_descriptor_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = ServiceSettings {
        response_headers: Some(LimitHeaders {
            limit: "X-Limit".into(),
            remaining: "X-Remaining".into(),
            reset: "X-Reset".into(),
        }),
        ..Default::default()
    };
    let mut h = Harness::with_service_settings(
        "
domain: d
descriptors:
  - key: second
    rate_limit: {unit: second, requests_per_unit: 3}
  - key: minute
    rate_limit: {unit: minute, requests_per_unit: 5}
  - key: hour
    rate_limit: {unit: hour, requests_per_unit: 2}
  - key: minute3
    rate_limit: {unit: minute, requests_per_unit: 3}
  - key: free
    rate_limit: {unlimited: true}
",
        settings,
    )
    .await;
    h.at(1_700_000_017).await;
    let all = [
        &[("free", "x")][..],
        &[("none", "x")],
        &[("minute", "x")],
        &[("second", "x")],
        &[("hour", "x")],
    ];
    for _ in 0..4 {
        let response = h.call(request("d", &all)).await.unwrap();
        assert_eq!(response.response_headers_to_add.len(), 3);
    }
    h.at(1_700_000_018).await;
    // A tie: same limit and remaining hits, different resets.
    h.call(request("d", &[&[("minute3", "x")], &[("second", "x")]]))
        .await
        .unwrap();
    h.call(request("d", &[&[("free", "x")], &[("none", "x")]]))
        .await
        .unwrap();
    h.assert_same_redis().await;
}

/// Request headers describe the same descriptor as the response headers, under their own
/// names, and are independent of them.
#[tokio::test]
async fn request_headers_describe_the_closest_descriptor_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = ServiceSettings {
        request_headers: Some(LimitHeaders {
            limit: "X-Request-Limit".into(),
            remaining: "X-Request-Remaining".into(),
            reset: "X-Request-Reset".into(),
        }),
        ..Default::default()
    };
    let mut h = Harness::with_service_settings(
        "
domain: d
descriptors:
  - key: second
    rate_limit: {unit: second, requests_per_unit: 1}
  - key: minute
    rate_limit: {unit: minute, requests_per_unit: 3}
",
        settings,
    )
    .await;
    h.at(1_700_000_017).await;
    for _ in 0..3 {
        let response = h
            .call(request("d", &[&[("minute", "x")], &[("second", "x")]]))
            .await
            .unwrap();
        assert_eq!(response.request_headers_to_add.len(), 3);
    }
    h.call(request("d", &[&[("none", "x")]])).await.unwrap();
    h.assert_same_redis().await;
}

/// Per-unit headers: one pair per unit, in the order units first appear, for the descriptor
/// of that unit with the fewest remaining hits (the first on a tie), before the response
/// headers.
#[tokio::test]
async fn per_unit_headers_describe_each_unit_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = ServiceSettings {
        per_unit_headers: true,
        response_headers: Some(LimitHeaders::default()),
        ..Default::default()
    };
    let mut h = Harness::with_service_settings(
        "
domain: d
descriptors:
  - key: minute5
    rate_limit: {unit: minute, requests_per_unit: 5}
  - key: second
    rate_limit: {unit: second, requests_per_unit: 1}
  - key: minute4
    rate_limit: {unit: minute, requests_per_unit: 4}
  - key: day
    rate_limit: {unit: day, requests_per_unit: 9}
  - key: free
    rate_limit: {unlimited: true}
",
        settings,
    )
    .await;
    h.at(1_700_000_017).await;
    h.call(request("d", &[&[("minute5", "x")]])).await.unwrap();
    // minute5 and minute4 tie at 3 remaining.
    let all = [
        &[("free", "x")][..],
        &[("minute5", "x")],
        &[("second", "x")],
        &[("none", "x")],
        &[("minute4", "x")],
        &[("day", "x")],
    ];
    for _ in 0..3 {
        let response = h.call(request("d", &all)).await.unwrap();
        assert_eq!(response.response_headers_to_add.len(), 3 * 2 + 3);
    }
    h.assert_same_redis().await;
}

/// Dynamic metadata describes the request: its domain, each descriptor's entries and hits,
/// and the request's hits when not 0; set under global shadow mode too.
#[tokio::test]
async fn dynamic_metadata_describes_the_request_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = ServiceSettings {
        response_dynamic_metadata: true,
        global_shadow_mode: true,
        ..Default::default()
    };
    let mut h = Harness::with_service_settings(TOKENS, settings).await;
    h.at(1_700_000_000).await;
    let both = || {
        request(
            "llm",
            &[
                &[("user", "alice"), ("model", "m=1")],
                &[("shadow", "x")],
                &[],
            ],
        )
    };
    for hits in [
        with_hits(both(), 0, &[None, None, None]),
        with_hits(both(), 7, &[Some(0), None, Some(3)]),
        with_hits(both(), 0, &[Some(1 << 60), Some(2), None]),
    ] {
        let response = h.call(hits).await.unwrap();
        assert!(response.dynamic_metadata.is_some());
    }
    h.assert_same_redis().await;
}

/// A client-sent limit appears in the metadata in protobuf's text format.
#[tokio::test]
async fn dynamic_metadata_shows_client_limits_like_upstream() {
    if !tools_available() {
        return;
    }
    use ratelimit_proto::envoy::extensions::common::ratelimit::v3::rate_limit_descriptor::RateLimitOverride;
    let settings = ServiceSettings {
        response_dynamic_metadata: true,
        ..Default::default()
    };
    let mut h = Harness::with_service_settings(TOKENS, settings).await;
    h.at(1_700_000_000).await;
    let mut overridden = request("llm", &[&[("user", "alice")], &[("free", "x")]]);
    overridden.descriptors[0].limit = Some(RateLimitOverride {
        requests_per_unit: 10,
        unit: 2, // MINUTE
        unit_multiplier: None,
    });
    overridden.descriptors[1].limit = Some(RateLimitOverride {
        requests_per_unit: 0,
        unit: 4, // DAY
        unit_multiplier: None,
    });
    let response = h.call(overridden).await.unwrap();
    assert!(response.dynamic_metadata.is_some());
    h.assert_same_redis().await;
}

const WITH_METADATA: &str = "
domain: d
descriptors:
  - key: a
    rate_limit: {unit: minute, requests_per_unit: 10}
    metadata: {route: a, shared: {x: 1, list: [1, two]}, weight: 1.5}
  - key: b
    rate_limit: {unit: minute, requests_per_unit: 10}
    metadata: {route: b, shared: {y: two, x: 9}, b: {nested: {deep: 1}}}
  - key: over
    rate_limit: {unit: minute, requests_per_unit: 0}
    metadata: {over: o}
  - key: free
    rate_limit: {unlimited: true}
    metadata: {free: f}
  - key: replaced
    rate_limit: {unit: minute, requests_per_unit: 10, name: old}
    metadata: {replaced: r}
  - key: replacer
    rate_limit: {unit: minute, requests_per_unit: 10, replaces: [{name: old}]}
    metadata: {replacer: r}
";

/// The metadata of the limits that passed is merged into the response's: the first value wins,
/// structs merge recursively. Over-limit, unlimited and replaced limits add nothing, nor does a
/// limit replaced by a client-sent one.
#[tokio::test]
async fn dynamic_metadata_merges_the_metadata_of_passed_limits_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = ServiceSettings {
        response_dynamic_metadata: true,
        ..Default::default()
    };
    let mut h = Harness::with_service_settings(WITH_METADATA, settings).await;
    h.at(1_700_000_000).await;
    let all = [
        &[("over", "x")][..],
        &[("a", "x")],
        &[("free", "x")],
        &[("b", "x")],
        &[("replaced", "x")],
        &[("replacer", "x")],
        &[("none", "x")],
    ];
    for _ in 0..2 {
        let response = h.call(request("d", &all)).await.unwrap();
        assert!(
            response
                .dynamic_metadata
                .unwrap()
                .fields
                .contains_key("metadata")
        );
    }
    h.call(request("d", &[&[("b", "x")], &[("a", "x")]]))
        .await
        .unwrap();
    let mut overridden = request("d", &[&[("replacer", "x")]]);
    overridden.descriptors[0].limit = Some(
        ratelimit_proto::envoy::extensions::common::ratelimit::v3::rate_limit_descriptor::RateLimitOverride {
            requests_per_unit: 5,
            unit: 2, // MINUTE
            unit_multiplier: None,
        },
    );
    h.call(overridden).await.unwrap();
    h.assert_same_redis().await;
}

const MODELS: &str = "
domain: ai
descriptors:
  - key: backend_name
    value: gpt
    descriptors:
      - key: model_name_override
        quota_mode: true
        rate_limit: {unit: minute, requests_per_unit: 1}
        metadata: {gpt: g}
  - key: backend_name
    value: claude
    descriptors:
      - key: model_name_override
        quota_mode: true
        rate_limit: {unit: minute, requests_per_unit: 5}
        metadata: {claude: c}
  - key: seat
    rate_limit: {unit: hour, requests_per_unit: 1}
";

/// In quota mode the metadata lists the models that still have quota (`passedBackends`), once
/// each; a model whose group is exhausted is left out, along with its limits' metadata, even
/// for its descriptors that passed. Passed regular descriptors naming a model are listed too.
/// A quota descriptor over its limit does not take over the headers.
#[tokio::test]
async fn dynamic_metadata_lists_the_models_with_quota_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = ServiceSettings {
        response_dynamic_metadata: true,
        response_headers: Some(LimitHeaders::default()),
        ..Default::default()
    };
    let mut h = Harness::with_service_settings(MODELS, settings).await;
    h.at(1_700_000_000).await;
    let gpt = &[("backend_name", "gpt"), ("model_name_override", "m1")][..];
    let claude = &[("backend_name", "claude"), ("model_name_override", "m2")][..];
    let regular = &[("model_name_override", "m3")][..];
    let anonymous = &[("tenant", "t")][..];
    h.call(request("ai", &[claude, regular, anonymous]))
        .await
        .unwrap();
    // gpt's first descriptor passes, its second exhausts it.
    h.call(request("ai", &[gpt, claude, claude, gpt, regular]))
        .await
        .unwrap();
    // Every model exhausted: nothing to list.
    h.call(request("ai", &[gpt])).await.unwrap();
    // The headers keep the first descriptor with no hits left, not the exhausted model.
    h.call(request("ai", &[&[("seat", "s")], gpt]))
        .await
        .unwrap();
    // Without a quota descriptor there is no list, even for regular ones naming a model.
    h.call(request("ai", &[regular, &[("backend_name", "other")]]))
        .await
        .unwrap();
    h.assert_same_redis().await;
}

/// Every config load is counted; one that fails is counted as an error and the previous config
/// stays in use.
#[tokio::test]
async fn config_reloads_are_counted_like_upstream() {
    if !tools_available() {
        return;
    }
    let mut h = Harness::start(TOKENS).await;
    h.at(1_700_000_000).await;
    let user = || request("llm", &[&[("user", "alice")]]);
    h.call(user()).await.unwrap();
    let error = h
        .load_config("domain: llm\ndescriptors:\n  - key: user\n    rate_limit: {unit: fortnight, requests_per_unit: 1}\n")
        .await
        .unwrap_err();
    assert!(error.contains("fortnight"), "{error}");
    h.call(user()).await.unwrap();
    h.load_config("domain: llm\ndescriptors:\n  - key: user\n    rate_limit: {unit: minute, requests_per_unit: 1}\n")
        .await
        .unwrap();
    h.call(user()).await.unwrap();
    h.call(user()).await.unwrap();
    h.assert_same_redis().await;
}

/// Hits are split between `within_limit`, `near_limit`, `over_limit` and `shadow_mode` like
/// upstream, with the near threshold computed in float32: 0.29 × 100 is 29 there, 28 in
/// float64.
#[tokio::test]
async fn near_and_over_limit_hits_are_split_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        cache: RedisCacheSettings {
            near_limit_ratio: 0.29,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut h = Harness::configured(
        "
domain: d
descriptors:
  - key: k
    rate_limit: {unit: minute, requests_per_unit: 100}
  - key: shadow
    shadow_mode: true
    rate_limit: {unit: minute, requests_per_unit: 100}
",
        settings,
    )
    .await;
    h.at(1_700_000_000).await;
    let both = || request("d", &[&[("k", "x")], &[("shadow", "x")]]);
    // 28 (under near), 29, 30 (near), 95 (straddles near), 105 (straddles over), 110 (over).
    for hits in [28, 1, 1, 65, 10, 5] {
        h.call(with_hits(both(), hits, &[None, None]))
            .await
            .unwrap();
    }
    // A new window: from 0 straight past the near threshold, then past both.
    h.at(1_700_000_060).await;
    h.call(with_hits(both(), 40, &[None, None])).await.unwrap();
    h.at(1_700_000_120).await;
    h.call(with_hits(both(), 150, &[None, None])).await.unwrap();
    h.assert_same_redis().await;
}

/// With `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT`, a descriptor left uncounted because another
/// is going over still has its hits taken from its counter for the stats, wrapping below 0 as
/// in upstream's unsigned arithmetic.
#[tokio::test]
async fn uncounted_hits_wrap_in_the_stats_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        cache: RedisCacheSettings {
            near_limit_ratio: 0.29,
            stop_cache_key_increment_when_overlimit: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut h = Harness::configured(
        "
domain: d
descriptors:
  - key: small
    rate_limit: {unit: minute, requests_per_unit: 10}
  - key: big
    rate_limit: {unit: minute, requests_per_unit: 100}
",
        settings,
    )
    .await;
    h.at(1_700_000_000).await;
    h.call(with_hits(request("d", &[&[("big", "x")]]), 30, &[None]))
        .await
        .unwrap();
    // `small` goes over, so `big` (30 + 40 = 70, within its limit) is not counted.
    h.call(with_hits(
        request("d", &[&[("small", "x")], &[("big", "x")]]),
        40,
        &[None, None],
    ))
    .await
    .unwrap();
    h.assert_same_redis().await;
}

const NAMED: &str = "
domain: d
descriptors:
  - key: detailed
    detailed_metric: true
    rate_limit: {unit: minute, requests_per_unit: 10}
  - key: metric
    value_to_metric: true
    rate_limit: {unit: minute, requests_per_unit: 10}
  - key: plain
    rate_limit: {unit: minute, requests_per_unit: 10}
";

/// Counters are named after the limit's stats key, sanitized: `:` and `|` and the dots of IPv4
/// addresses become `_`; an unknown domain is counted per descriptor.
#[tokio::test]
async fn counters_are_named_like_upstream() {
    if !tools_available() {
        return;
    }
    let mut h = Harness::start(NAMED).await;
    h.at(1_700_000_000).await;
    for value in ["a:b|c", "10.0.0.1", "v1.2.3.4.5", "1234.5.6.7.8"] {
        h.call(request(
            "d",
            &[
                &[("detailed", value)],
                &[("metric", value)],
                &[("plain", value)],
            ],
        ))
        .await
        .unwrap();
    }
    let mut overridden = request("d", &[&[("client", "192.168.1.1:80")]]);
    overridden.descriptors[0].limit = Some(
        ratelimit_proto::envoy::extensions::common::ratelimit::v3::rate_limit_descriptor::RateLimitOverride {
            requests_per_unit: 5,
            unit: 2, // MINUTE
            unit_multiplier: None,
        },
    );
    h.call(overridden).await.unwrap();
    h.call(request("unknown:1.2.3.4", &[&[("a", "b")], &[("c", "d")]]))
        .await
        .unwrap();
    h.assert_same_redis().await;
}

/// `SANITIZE_DESCRIPTOR_METRIC_DOTS` turns dots into `_` in the keys and values of stats keys
/// built per request.
#[tokio::test]
async fn descriptor_dots_are_sanitized_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        load: LoadOptions {
            sanitize_descriptor_metric_dots: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut h = Harness::configured(NAMED, settings).await;
    h.at(1_700_000_000).await;
    for value in ["a.b", "10.0.0.1"] {
        h.call(request(
            "d",
            &[
                &[("detailed", value)],
                &[("metric", value)],
                &[("plain", value)],
            ],
        ))
        .await
        .unwrap();
    }
    h.call(request("unknown.domain", &[&[("a.b", "c.d")]]))
        .await
        .unwrap();
    h.assert_same_redis().await;
}

/// Global shadow mode counts each request it lets through.
#[tokio::test]
async fn global_shadow_mode_is_counted_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = ServiceSettings {
        global_shadow_mode: true,
        ..Default::default()
    };
    let mut h = Harness::with_service_settings(TOKENS, settings).await;
    h.at(1_700_000_000).await;
    for hits in [600, 600, 1] {
        let response = h
            .call(with_hits(
                request("llm", &[&[("user", "alice")]]),
                hits,
                &[None],
            ))
            .await
            .unwrap();
        assert_eq!(response.overall_code, 1); // OK
    }
    h.assert_same_redis().await;
}

/// With `NEAR_LIMIT_RATIO` above 1 the near threshold is past the limit: hits going over then
/// add the limit minus that threshold to `near_limit`, wrapping below 0 like upstream.
#[tokio::test]
async fn a_near_threshold_past_the_limit_wraps_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        cache: RedisCacheSettings {
            near_limit_ratio: 1.5,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut h = Harness::configured(TOKENS, settings).await;
    h.at(1_700_000_000).await;
    for hits in [900, 200, 50] {
        h.call(with_hits(
            request("llm", &[&[("user", "alice")]]),
            hits,
            &[None],
        ))
        .await
        .unwrap();
    }
    h.assert_same_redis().await;
}

/// The counters reach statsd over TCP as upstream's sink sends them: each flush, every counter's
/// increase since the previous one, when not 0.
#[tokio::test]
async fn counters_are_flushed_to_statsd_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        statsd: true,
        ..Default::default()
    };
    let mut h = Harness::configured(TOKENS, settings).await;
    h.at(1_700_000_000).await;
    let user = || request("llm", &[&[("user", "alice")]]);
    h.call(with_hits(user(), 600, &[None])).await.unwrap();
    h.call(user()).await.unwrap();
    h.call(with_hits(user(), 600, &[None])).await.unwrap();
    h.call(request("unknown", &[&[("a", "b")]])).await.unwrap();
    h.assert_same_redis().await;
}

/// `EXTRA_TAGS` are appended to every name, sorted by key, values sanitized, empty values left
/// out; each reserved key counts in `reserved_tag`.
#[tokio::test]
async fn extra_tags_are_appended_like_upstream() {
    if !tools_available() {
        return;
    }
    let tags = [
        ("zone", "eu.west|1"),
        ("app", "rl|x"),
        ("host", "h1"),
        ("region", ""),
    ];
    let settings = Settings {
        statsd: true,
        extra_tags: tags
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        ..Default::default()
    };
    let mut h = Harness::configured(TOKENS, settings).await;
    h.at(1_700_000_000).await;
    h.call(request("llm", &[&[("user", "alice")], &[("x", "y")]]))
        .await
        .unwrap();
    h.call(request("unknown", &[&[("a", "b")]])).await.unwrap();
    h.assert_same_redis().await;
}

/// The counters reach DogStatsD as upstream's sink sends them: `name:value|c` lines with the
/// global tags, then `EXTRA_TAGS` pulled out of the name, then the first matching mogrifier's.
#[tokio::test]
async fn counters_are_sent_to_dogstatsd_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        dogstatsd: Some(DogStatsd {
            entity_id: "entity".into(),
            env: "prod".into(),
            service: "rl".into(),
            version: "1.2".into(),
            mogrifiers: vec![
                (
                    "HITS",
                    r"^ratelimit\.service\.rate_limit\.(\w+)\.(\w+)\.total_hits$",
                    "ratelimit.hits",
                    vec![("domain", "$1")],
                ),
                (
                    "ANY",
                    r"^ratelimit\.service\.rate_limit\.([^.]+)\.(.+)$",
                    "ratelimit.$2.$9",
                    vec![("domain", "$1")],
                ),
            ],
        }),
        extra_tags: [("cluster", "c1")]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        ..Default::default()
    };
    let mut h = Harness::configured(TOKENS, settings).await;
    h.at(1_700_000_000).await;
    let user = || request("llm", &[&[("user", "alice")]]);
    h.call(with_hits(user(), 600, &[None])).await.unwrap();
    h.call(with_hits(user(), 600, &[None])).await.unwrap();
    h.call(request("unknown", &[&[("a", "b")]])).await.unwrap();
    h.assert_same_redis().await;
}

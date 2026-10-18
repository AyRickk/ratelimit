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
use ratelimit::stats::prometheus::{self, PrometheusSettings, PrometheusSink};
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

/// A port for a server that cannot report the one it gets (upstream's Prometheus listener):
/// taken from a range below the ephemeral ports, so that no other test's ephemeral port can
/// take it between this check and the server's bind, and never handed out twice, since the
/// services bind with `SO_REUSEPORT` and two of them would share a port.
fn reserved_port() -> u16 {
    static GIVEN: std::sync::Mutex<Vec<u16>> = std::sync::Mutex::new(Vec::new());
    const RANGE: u16 = 10_000;
    let base = (std::process::id() % RANGE as u32) as u16;
    let mut given = GIVEN.lock().unwrap();
    (0..RANGE)
        .map(|offset| 20_000 + (base + offset) % RANGE)
        .find(|port| {
            !given.contains(port) && std::net::TcpListener::bind(("127.0.0.1", *port)).is_ok()
        })
        .inspect(|port| given.push(*port))
        .expect("a free port")
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
    async fn connect(&self) -> fred::clients::Pool {
        let config = Config::from_url(&format!("redis://{}", self.address)).unwrap();
        let pool = Builder::from_config(config).build_pool(1).unwrap();
        pool.init().await.unwrap();
        pool
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
    let runtime = settings.runtime.clone();
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
    environment.extend(runtime.into_iter().flatten());
    environment
}

impl Oracle {
    async fn start(
        redis: &Redis,
        all: &Settings,
        statsd: Option<&StatsdServer>,
        dogstatsd: Option<&DogStatsdServer>,
        prometheus: Option<&Prometheus>,
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
            .args(prometheus.map(Prometheus::oracle_args).unwrap_or_default())
            .args(all.runtime.as_ref().map(|_| "-file-provider"))
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
    /// With `Settings::prometheus`: the sink, its exporter's clock and where it serves.
    prometheus: Option<(PrometheusSink, Arc<AtomicI64>, String)>,
    /// With `Settings::runtime`, the file provider feeding the service.
    _provider: Option<ratelimit::runtime::FileProvider>,
}

impl Subject {
    async fn start(
        redis: &Redis,
        settings: &Settings,
        statsd: Option<&StatsdServer>,
        dogstatsd: Option<&DogStatsdServer>,
        prometheus: Option<&Prometheus>,
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
        let prometheus = match prometheus {
            Some(prometheus) => {
                let exporter_clock = Arc::new(AtomicI64::new(0));
                let now = exporter_clock.clone();
                let mapper = prometheus::load_mapper(&prometheus.settings()).unwrap();
                let clock: prometheus::Clock = Arc::new(move || now.load(Ordering::SeqCst));
                let prometheus_sink = PrometheusSink::new(mapper, prometheus.path, clock).unwrap();
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap().to_string();
                let server = prometheus_sink.clone();
                tokio::spawn(async move { server.serve(listener).await });
                sink = Some(Sink::Prometheus(prometheus_sink.clone()));
                Some((prometheus_sink, exporter_clock, address))
            }
            None => None,
        };
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
        let provider = settings.runtime.as_ref().map(|runtime| {
            let lookup = |key: &str| {
                runtime
                    .iter()
                    .chain(&oracle_environment(settings))
                    .find(|(k, _)| *k == key)
                    .map(|(_, v)| v.clone())
            };
            let all = ratelimit::settings::Settings::from_lookup(&lookup).unwrap();
            let service = service.clone();
            ratelimit::runtime::FileProvider::start(&all, &stats, move |config| {
                service.update_config(config)
            })
            .unwrap()
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(
            tonic::transport::Server::builder()
                // The oracle's server has no interceptor: these stats are kept apart.
                .add_service(grpc::server(
                    service.clone(),
                    ratelimit::stats::ServerStats::new(Arc::new(Stats::default())),
                ))
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
            prometheus,
            _provider: provider,
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
    /// With `Settings::prometheus`, where the oracle serves its metrics.
    prometheus: Option<String>,
    prometheus_path: &'static str,
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
    /// Also send the stats to the Prometheus sink on each side, and compare what they serve.
    prometheus: Option<Prometheus>,
    /// Load the config with each side's file provider, configured by these `RUNTIME_*`
    /// variables, instead of pushing it.
    runtime: Option<Vec<(&'static str, String)>>,
}

/// The Prometheus sink's configuration, for both sides.
#[derive(Clone)]
struct Prometheus {
    /// `PROMETHEUS_PATH`.
    path: &'static str,
    /// `PROMETHEUS_MAPPER_YAML`: a file with this content, a missing file, or the default.
    mapper: MapperFile,
    /// `PROMETHEUS_RESPONSE_TIME_AS_MILLISECONDS`.
    milliseconds: bool,
    /// Where the oracle serves it (a free port, as upstream cannot report the one it gets).
    oracle_address: String,
}

#[derive(Clone)]
enum MapperFile {
    Default,
    Content(PathBuf),
    Missing,
}

impl MapperFile {
    /// A temporary file holding `content`.
    async fn write(content: &[u8]) -> Self {
        let path = std::env::temp_dir().join(format!(
            "ratelimit-mapper-{}-{}.yaml",
            std::process::id(),
            free_port().await
        ));
        std::fs::write(&path, content).unwrap();
        MapperFile::Content(path)
    }
}

impl Prometheus {
    async fn new(mapper: Option<&str>) -> Self {
        let mapper = match mapper {
            None => MapperFile::Default,
            Some(content) => MapperFile::write(content.as_bytes()).await,
        };
        Self {
            path: "/metrics",
            mapper,
            milliseconds: false,
            oracle_address: format!("127.0.0.1:{}", reserved_port()),
        }
    }

    fn mapper_path(&self) -> String {
        match &self.mapper {
            MapperFile::Default => String::new(),
            MapperFile::Content(path) => path.to_string_lossy().into_owned(),
            MapperFile::Missing => "/nonexistent/ratelimit-mapper.yaml".into(),
        }
    }

    fn settings(&self) -> PrometheusSettings {
        PrometheusSettings {
            addr: String::new(),
            path: self.path.into(),
            mapper_yaml: self.mapper_path(),
            response_time_as_milliseconds: self.milliseconds,
        }
    }

    fn oracle_args(&self) -> Vec<String> {
        vec![
            format!("-prometheus={}", self.oracle_address),
            format!("-prometheus-path={}", self.path),
            format!("-prometheus-mapper={}", self.mapper_path()),
            format!("-prometheus-ms={}", self.milliseconds),
        ]
    }
}

/// An HTTP answer, as far as the two sides must agree on it.
#[derive(Debug, PartialEq)]
struct Scraped {
    status: u16,
    /// Content-Type, Content-Encoding, Location and X-Content-Type-Options.
    headers: BTreeMap<String, String>,
    /// Decompressed.
    body: String,
}

/// Sends an HTTP/1.0 request to `address`; returns the answer.
async fn http_request(
    address: &str,
    method: &str,
    target: &str,
    headers: &[(&str, &str)],
) -> Scraped {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = None;
    for _ in 0..300 {
        match tokio::net::TcpStream::connect(address).await {
            Ok(connected) => {
                stream = Some(connected);
                break;
            }
            // The oracle's listener starts in the background.
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
        }
    }
    let mut stream = stream.expect("the Prometheus listener");
    let mut request = format!("{method} {target} HTTP/1.0\r\nHost: scrape\r\n");
    for (name, value) in headers {
        request += &format!("{name}: {value}\r\n");
    }
    request += "\r\n";
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let split = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("an HTTP answer");
    let head = String::from_utf8_lossy(&response[..split]).into_owned();
    let raw_body = &response[split + 4..];
    let mut lines = head.lines();
    let status = lines.next().unwrap()[9..12].parse().unwrap();
    let mut kept = BTreeMap::new();
    for line in lines {
        let (name, value) = line.split_once(':').unwrap();
        let name = name.trim().to_ascii_lowercase();
        if matches!(
            name.as_str(),
            "content-type" | "content-encoding" | "location" | "x-content-type-options"
        ) {
            kept.insert(name, value.trim().to_string());
        }
    }
    let body = match kept.get("content-encoding").map(String::as_str) {
        Some("gzip") => {
            let mut text = String::new();
            std::io::Read::read_to_string(&mut flate2::read::GzDecoder::new(raw_body), &mut text)
                .unwrap();
            text
        }
        _ => String::from_utf8_lossy(raw_body).into_owned(),
    };
    Scraped {
        status,
        headers: kept,
        body,
    }
}

/// Upstream's answer without what only Go serves: the `go_*` and `process_*` families
/// (docs/parity.md §7). In Gather errors, which print protobuf messages with randomized
/// spacing and a creation time, spaces are collapsed and the time left out.
fn comparable(mut scraped: Scraped, upstream: bool) -> Scraped {
    if upstream {
        scraped.body = scraped
            .body
            .lines()
            .filter(|line| {
                let name = line
                    .strip_prefix("# HELP ")
                    .or_else(|| line.strip_prefix("# TYPE "))
                    .unwrap_or(line);
                !name.starts_with("go_") && !name.starts_with("process_")
            })
            .map(|line| format!("{line}\n"))
            .collect();
    }
    if scraped.status == 500 {
        let created = regex::Regex::new(r"created_timestamp:\s*\{[^}]*\}\s*").unwrap();
        let body = created.replace_all(&scraped.body, "");
        let spaces = regex::Regex::new(r" +").unwrap();
        scraped.body = spaces.replace_all(&body, " ").replace(" }", "}");
    }
    scraped
}

/// A mogrifier, for both sides.
#[derive(Clone)]
struct MogrifierSpec {
    key: &'static str,
    pattern: &'static str,
    name: &'static str,
    tags: Vec<(&'static str, &'static str)>,
}

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
                .map(|m| {
                    let tags = m.tags.iter().map(|(k, v)| (k.to_string(), v.to_string()));
                    Mogrifier::new(m.key, m.pattern, m.name, tags.collect()).unwrap()
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
        let keys: Vec<&str> = self.mogrifiers.iter().map(|m| m.key).collect();
        environment.push(("USE_DOG_STATSD_MOGRIFIERS", keys.join(",")));
        for m in &self.mogrifiers {
            let var = |field: &str| leak(format!("DOG_STATSD_MOGRIFIER_{}_{field}", m.key));
            let tags: Vec<String> = m.tags.iter().map(|(k, v)| format!("{k}:{v}")).collect();
            environment.push((var("PATTERN"), m.pattern.to_string()));
            environment.push((var("NAME"), m.name.to_string()));
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
            settings.prometheus.as_ref(),
        )
        .await;
        let subject = Subject::start(
            &subject_redis,
            &settings,
            statsd.as_ref().map(|s| &s.subject),
            dogstatsd.as_ref().map(|s| &s.subject),
            settings.prometheus.as_ref(),
        )
        .await;
        let runtime = settings.runtime.is_some();
        let mut h = Self {
            oracle,
            oracle_redis,
            subject,
            subject_redis,
            load_options: settings.load,
            statsd,
            dogstatsd,
            prometheus_path: settings.prometheus.as_ref().map_or("/metrics", |p| p.path),
            prometheus: settings.prometheus.map(|p| p.oracle_address),
            flushes: Default::default(),
        };
        match runtime {
            true => h.await_config_loads(1).await,
            false => h.load_config(config).await.unwrap(),
        }
        h
    }

    /// Waits until each side has loaded a config (or failed to) `count` times in all, then
    /// compares their stats.
    async fn await_config_loads(&self, count: u64) {
        let loads = |stats: &BTreeMap<String, u64>| {
            stats
                .iter()
                .filter(|(name, _)| name.starts_with("ratelimit.service.config_load_"))
                .map(|(_, value)| value)
                .sum::<u64>()
        };
        for _ in 0..500 {
            let upstream = loads(&self.oracle.stats().await);
            let ours = loads(&self.subject.stats.snapshot());
            if upstream >= count && ours >= count {
                self.assert_same_stats().await;
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!("config loads did not reach {count}");
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
            if !matches!(sink, Sink::Prometheus(_)) {
                sink.flush_counter("harness.flushed", 1);
            }
            sink.flush().await;
        }
        assert_eq!(self.subject.stats.snapshot(), expected, "counters");
        let count = self.flushes.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some(dogstatsd) = &self.dogstatsd {
            assert_eq!(
                dogstatsd.subject.lines(count).await,
                dogstatsd.oracle.lines(count).await,
                "DogStatsD lines"
            );
        }
        if let Some(statsd) = &self.statsd {
            assert_eq!(
                statsd.subject.flushes(count).await,
                statsd.oracle.flushes(count).await,
                "statsd lines, per flush"
            );
        }
        if self.prometheus.is_some() {
            self.scrape("GET", self.prometheus_path, &[]).await;
        }
    }

    /// Sends the same request to both Prometheus listeners and checks they answer the same;
    /// returns upstream's answer.
    async fn scrape(&self, method: &str, target: &str, headers: &[(&str, &str)]) -> Scraped {
        let oracle = self.prometheus.as_ref().expect("Settings::prometheus");
        let subject = &self.subject.prometheus.as_ref().unwrap().2;
        let expected = comparable(http_request(oracle, method, target, headers).await, true);
        let actual = comparable(http_request(subject, method, target, headers).await, false);
        assert_eq!(actual, expected, "{method} {target} {headers:?}");
        expected
    }

    /// Flushes `stats` (type, name, value) straight into both Prometheus sinks, then compares
    /// what they serve.
    async fn flush_to_prometheus(&self, stats: &[(&str, &str, f64)]) -> Scraped {
        let body: String = stats
            .iter()
            .map(|(kind, name, value)| format!("{kind} {name} {value}\n"))
            .collect();
        let (status, response) = self.oracle.send("POST", "/flush", &body).await;
        assert_eq!(status, 200, "oracle /flush: {response}");
        let sink = &self.subject.prometheus.as_ref().unwrap().0;
        for (kind, name, value) in stats {
            match *kind {
                "counter" => sink.flush_counter(name, *value as u64),
                "gauge" => sink.flush_gauge(name, *value as u64),
                "timer" => sink.flush_timer(name, *value),
                other => panic!("unknown stat type {other}"),
            }
        }
        self.scrape("GET", self.prometheus_path, &[]).await
    }

    /// Moves statsd_exporter's clock on both sides (nanoseconds), then lets them remove stale
    /// series, and compares what they serve.
    async fn exporter_at(&self, nanos: i64) -> Scraped {
        self.oracle.put("/exporter-time", nanos.to_string()).await;
        let (sink, clock, _) = self.subject.prometheus.as_ref().unwrap();
        clock.store(nanos, Ordering::SeqCst);
        sink.remove_stale_metrics();
        self.scrape("GET", self.prometheus_path, &[]).await
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
                MogrifierSpec {
                    key: "UNKNOWN",
                    pattern: r"^ratelimit\.service\.rate_limit\.(\w+)\.domain_not_found$",
                    name: "ratelimit.unknown",
                    tags: vec![("domain", "$1")],
                },
                MogrifierSpec {
                    key: "HITS",
                    pattern: r"^ratelimit\.service\.rate_limit\.(\w+)\.(\w+)\.total_hits$",
                    name: "ratelimit.hits",
                    tags: vec![("domain", "$1")],
                },
                MogrifierSpec {
                    key: "ANY",
                    pattern: r"^ratelimit\.service\.rate_limit\.([^.]+)\.(.+)$",
                    name: "ratelimit.$2.$9.$1a.$01.$",
                    tags: vec![("domain", "$1")],
                },
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
    h.call(request("unknown", &[&[("a", "b")]])).await.unwrap();
    // Not a word in Go's ASCII `\w`: UNKNOWN does not match, ANY does.
    h.call(request("é٣", &[&[("a", "b")]])).await.unwrap();
    h.assert_same_redis().await;
}

/// Without `DD_ENTITY_ID`, origin detection is on: both sides find the same container ID, if
/// any; the other global tags still come first.
#[tokio::test]
async fn dogstatsd_without_an_entity_id_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        dogstatsd: Some(DogStatsd {
            env: "prod".into(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut h = Harness::configured(TOKENS, settings).await;
    h.at(1_700_000_000).await;
    h.call(request("llm", &[&[("user", "alice")]]))
        .await
        .unwrap();
    h.assert_same_redis().await;
}

const NESTED: &str = "
domain: llm
descriptors:
  - key: user
    rate_limit: {unit: minute, requests_per_unit: 1000}
  - key: org
    descriptors:
      - key: model
        rate_limit: {unit: minute, requests_per_unit: 2}
  - key: shadow
    shadow_mode: true
    rate_limit: {unit: hour, requests_per_unit: 1}
";

/// The counters reach Prometheus through upstream's default mapper: one-key descriptors by its
/// globs, deeper ones by its unanchored regexes, the rest escaped; values in Go's format.
#[tokio::test]
async fn counters_are_exposed_to_prometheus_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        cache: RedisCacheSettings {
            local_cache_size_in_bytes: 1024 * 1024,
            ..Default::default()
        },
        prometheus: Some(Prometheus::new(None).await),
        ..Default::default()
    };
    let mut h = Harness::configured(NESTED, settings).await;
    h.at(1_700_000_000).await;
    let user = || request("llm", &[&[("user", "alice")]]);
    h.call(with_hits(user(), 1_000_000, &[None])).await.unwrap();
    h.call(user()).await.unwrap();
    let model = || request("llm", &[&[("org", "acme"), ("model", "gpt")]]);
    for _ in 0..4 {
        h.call(model()).await.unwrap();
    }
    h.call(request("llm", &[&[("shadow", "s")]])).await.unwrap();
    h.call(request("é-x", &[&[("a", "b")]])).await.unwrap();
    h.call(request("9th", &[&[("a", "b")]])).await.unwrap();
}

/// promhttp's answer: gzip when the first Accept-Encoding lists it, the `escaping` parameter
/// echoed, any method; Go's mux around the path.
#[tokio::test]
async fn prometheus_scrapes_are_answered_like_upstream() {
    if !tools_available() {
        return;
    }
    let settings = Settings {
        prometheus: Some(Prometheus::new(None).await),
        ..Default::default()
    };
    let mut h = Harness::configured(TOKENS, settings).await;
    h.at(1_700_000_000).await;
    h.call(request("llm", &[&[("user", "alice")]]))
        .await
        .unwrap();
    for headers in [
        &[("Accept-Encoding", "gzip")][..],
        &[("Accept-Encoding", "br, gzip;q=0")],
        &[("Accept-Encoding", "GZIP")],
        &[("Accept", "text/plain;version=0.0.4;escaping=underscores")],
        &[("Accept", "application/json;escaping=dots, text/plain;q=0.5")],
        &[("Accept", "text/plain;escaping=bogus")],
        &[(
            "Accept",
            "*/*;escaping=allow-utf-8,text/plain;version=1.0.0",
        )],
    ] {
        h.scrape("GET", "/metrics", headers).await;
    }
    h.scrape("POST", "/metrics", &[]).await;
    h.scrape("HEAD", "/metrics", &[]).await;
    for target in [
        "/metrics?x=1",
        "/metrics/",
        "/",
        "/other",
        "/a/../metrics",
        "//metrics?q",
        "/%6Detrics",
        "/metric%73?a",
        "/%2Fmetrics",
        "/metrics%2F",
    ] {
        h.scrape("GET", target, &[]).await;
    }
}

/// What the mapper decided for one name, shaped like the oracle's JSON.
fn mapped_json(mapped: Option<prometheus::Mapped>) -> serde_json::Value {
    use prometheus::{Action, MatchType, ObserverType};
    use serde_json::json;
    let Some(m) = mapped else {
        return json!({"matched": false, "name": "", "labels": null, "action": "", "help": "",
            "honor_labels": false, "observer_type": "", "match_type": "", "match_metric_type": "",
            "ttl": 0, "scale": null, "histogram_options": null, "summary_options": null});
    };
    let objectives = |o: &Option<Vec<prometheus::Objective>>| {
        o.as_ref().map(|o| {
            o.iter()
                .map(|q| json!({"Quantile": q.quantile, "Error": q.error}))
                .collect::<Vec<_>>()
        })
    };
    json!({
        "matched": true,
        "name": m.name,
        "labels": m.labels,
        "action": match m.action { Action::Map => "map", Action::Drop => "drop" },
        "help": m.help,
        "honor_labels": m.honor_labels,
        "observer_type": match m.observer_type {
            None => "",
            Some(ObserverType::Histogram) => "histogram",
            Some(ObserverType::Summary) => "summary",
        },
        "match_type": match m.match_type { MatchType::Glob => "glob", MatchType::Regex => "regex" },
        "match_metric_type": m.match_metric_type.map_or("", |t| t.as_str()),
        "ttl": m.ttl,
        "scale": m.scale,
        "histogram_options": m.histogram_options.map(|h| json!({
            "Buckets": h.buckets,
            "NativeHistogramBucketFactor": h.native_histogram_bucket_factor,
            "NativeHistogramMaxBuckets": h.native_histogram_max_buckets,
        })),
        "summary_options": m.summary_options.map(|s| json!({
            "Quantiles": objectives(&s.quantiles),
            "MaxAge": s.max_age,
            "AgeBuckets": s.age_buckets,
            "BufCap": s.buf_cap,
        })),
    })
}

/// JSON numbers as floats, so that Go's `1` and Rust's `1.0` compare equal.
fn floats(value: serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match value {
        Value::Number(n) => serde_json::json!(n.as_f64().unwrap()),
        Value::Array(items) => Value::Array(items.into_iter().map(floats).collect()),
        Value::Object(map) => Value::Object(map.into_iter().map(|(k, v)| (k, floats(v))).collect()),
        other => other,
    }
}

/// Mapper files, valid or not, that exercise statsd_exporter's decoding, validation, glob
/// FSM (ordering, prefixes, duplicates, backtracking), regexes and templates.
const MAPPERS: &[&str] = &[
    include_str!("../src/stats/prometheus/default_mapper.yaml"),
    "",
    "mappings:",
    "defaults: {observer_type: histogram, ttl: 1m, buckets: [1, 2], quantiles: [{quantile: 0.4, error: 0.1}]}
mappings:
- match: a.*.c
  name: abc_$1
  labels: {first: $1, both: \"$1-$1\", none: $2, braced: \"${1}x\", zero: $0, pct: \"%d$1%\", big: $10, mix: \"$1 $10\"}
- match: a.b.*
  name: ab
  match_metric_type: counter
  help: The help.
  ttl: 10
- match: a.b
  name: shorter_prefix
- match: a.*.c
  name: duplicate
- match: '*.*.*'
  name: any_three
  action: drop
  match_metric_type: gauge
- match: x.*
  name: x_$1
  timer_type: summary
  scale: 0.5
  summary_options: {max_age: 1h30m, age_buckets: 3, buf_cap: 7}
",
    "defaults: {glob_disable_ordering: true, match_type: regex, observer_type: \"\"}
mappings:
- match: '^ratelimit\\.(\\w+)\\.(?P<rest>.+)$'
  name: rl_${1}
  labels: {lit: \"$$1\", rest: $rest, named: \"${rest}\", missing: $9, tail: \"$1x$\", word: $rest_}
- match: 'b(.)'
  name: unanchored
  match_metric_type: timer
  histogram_options: {buckets: [0.5, 5], native_histogram_bucket_factor: 2}
  observer_type: histogram
- match: a.*
  match_type: glob
  name: glob_$1
- match: a.b
  match_type: glob
  name: glob_exact
",
    "mappings:
- match: a.*
  name: one
- match: '*.b'
  name: two
- match: '*.*.c'
  name: three
- match: a.b.*
  name: four
- match: test.*.*.*
  name: deep_$3_$1
  labels: {kk: v, xx: '$2'}
",
    "mappings:
- {match: a.b, name: \"$1\"}
- {match: a.c, name: ok}",
    "mappings:\n- {match: a, name: n, labels: {a: b}}",
    "mappings:\n- {match: a, name: 1n}",
    "mappings:\n- {match: 'a*', name: n}",
    "mappings:\n- {match: '(', name: n, match_type: regex}",
    "mappings:\n- {match: a, name: n, match_type: other}",
    "mappings:\n- {match: a, name: n, buckets: [1], histogram_options: {buckets: [2]}}",
    "mappings:\n- {match: a, name: n, buckets: [], histogram_options: {buckets: []}}",
    "mappings:\n- {match: a, name: n, observer_type: histogram, summary_options: {}}",
    "mappings:\n- {match: a, name: n, observer_type: summary, histogram_options: ~}",
    "mappings:\n- {match: a, name: n, scale: '0.5'}",
    "mappings:\n- {match: a, name: n, ttl: 1.5x}",
    "mappings:\n- {match: a, name: n, ttl: 2.5}\n- {match: b, name: m, ttl: '-1h2m3.5s'}",
    "mappings:\n- {match: a, name: n, unknown_key: 1, action: ''}\nother: true",
    "mappings:\n- ~\n",
    "mappings: 3",
    "defaults: {ttl: 5s, match_type: glob}\nmappings:\n- {match: a, name: n, ttl: 0}\n- {match: b, name: m, match_metric_type: observer, quantiles: [{quantile: 0.1}]}",
    // Repeated keys and merges: scalars replaced, maps and structs added to.
    "base: &base {name: base_$1, labels: {kind: a, id: x}, histogram_options: {buckets: [1]}}
mappings:
- <<: *base
  match: m.*
  labels: {id: $1}
  histogram_options: {native_histogram_max_buckets: 3}
  observer_type: histogram
  name: merged_$1
- match: n.*
  name: first
  name: second
  labels: {aa: x}
  labels: ~
  labels: {bb: \"%x-%X-%q-%T-%d-$1\"}
defaults: {ttl: 1s}
defaults: {match_type: glob}",
    "mappings:\n- {match: a, name: n, help: x, help: [1]}",
    "registerer: x",
    "registerer: ~\nmappingscount: ~",
    "mappingscount: 1",
    "logger: {a: b}",
    "fsm: {backtrackingneeded: yes, orderingdisabled: 2}",
    "fsm: {backtrackingneeded: yes}\nmappings:\n- {match: a, name: n}",
    "fsm: 3",
    "mappings:\n- {match: a.*.*, name: \"n_$2\", labels: {ll: \"${2}${1}\"}}\n- {match: a.*.*, name: second}\n- {match: a.x.*, name: specific}",
];

/// Names to map: the stats the service emits, and every short path over a few segments that
/// the mapper files above care about.
fn mapper_names() -> Vec<String> {
    let mut names: Vec<String> = [
        "ratelimit.service.rate_limit.llm.user_alice.total_hits",
        "ratelimit.service.rate_limit.llm.org_acme.model_gpt.near_limit",
        "ratelimit.service.rate_limit.llm.org_acme.model_gpt.over_limit_with_local_cache",
        "ratelimit.service.rate_limit.a.b.c.d.within_limit",
        "ratelimit.service.rate_limit.x.shadow_mode",
        "ratelimit.service.call.should_rate_limit.redis_error",
        "ratelimit_server.ShouldRateLimit.response_time",
        "ratelimit_server.ShouldRateLimit.total_requests",
        "ratelimit.service.config_load_success",
        "ratelimit.service.config_load_error",
        "test.dispatcher.FooProcessor.send.success",
        "x.y",
        "",
        "a..c",
        "a.*.c",
        "é.b.c",
        "m.x",
        "n.x",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let segments = ["a", "b", "c", "x", "*", "test"];
    let mut paths = vec![String::new()];
    for _ in 0..4 {
        let mut longer = Vec::new();
        for path in &paths {
            for segment in segments {
                let name = match path.is_empty() {
                    true => segment.to_string(),
                    false => format!("{path}.{segment}"),
                };
                longer.push(name);
            }
        }
        names.extend(longer.iter().cloned());
        paths = longer;
    }
    names
}

/// statsd_exporter's mapper, ported: the same files load or fail, and every name maps to the
/// same name, labels and options.
#[tokio::test]
async fn the_prometheus_mapper_maps_like_statsd_exporter() {
    if !tools_available() {
        return;
    }
    let h = Harness::start(TOKENS).await;
    let names = mapper_names();
    let types = [
        prometheus::MetricType::Counter,
        prometheus::MetricType::Gauge,
        prometheus::MetricType::Observer,
    ];
    for yaml in MAPPERS {
        let (status, error) = h.oracle.send("PUT", "/mapper", yaml).await;
        let mapper = prometheus::Mapper::from_yaml(yaml);
        let outcome = match &mapper {
            Ok(_) => 200,
            Err(prometheus::MapperError::Invalid(_)) => 422,
            Err(prometheus::MapperError::Panic(_)) => 500,
        };
        assert_eq!(
            outcome, status,
            "loading {yaml}: upstream {error}, ours {mapper:?}"
        );
        let mapper = mapper.unwrap_or_default();
        let mut body = String::new();
        let mut expected_order = Vec::new();
        for name in &names {
            for t in types {
                body += &format!("{} {name}\n", t.as_str());
                expected_order.push((t, name));
            }
        }
        let (status, response) = h.oracle.send("POST", "/map", &body).await;
        assert_eq!(status, 200, "{response}");
        assert_eq!(response.lines().count(), expected_order.len(), "{response}");
        for ((t, name), line) in expected_order.into_iter().zip(response.lines()) {
            let expected: serde_json::Value = serde_json::from_str(line).unwrap();
            let actual = mapped_json(mapper.get_mapping(name, t));
            assert_eq!(
                floats(actual),
                floats(expected),
                "{} {name} with {yaml}",
                t.as_str()
            );
        }
    }
}

/// Gauges and timers too: response times in a histogram scaled to seconds, other observers
/// by the defaults; a name taken by another type is a conflict, a negative scaled counter an
/// error; histogram and counter names that collide make the scrape fail.
#[tokio::test]
async fn gauges_and_timers_reach_prometheus_like_upstream() {
    if !tools_available() {
        return;
    }
    let mapper = "
defaults: {observer_type: histogram, buckets: [1, 10, .inf]}
mappings:
- match: ratelimit_server.*.response_time
  name: response_time_seconds
  timer_type: histogram
  scale: 0.001
  labels: {grpc_method: $1}
- match: neg.*
  name: negative
  scale: -1
- match: empty.*
  name: $2
- match: drop.*
  name: dropped
  action: drop
";
    let settings = Settings {
        prometheus: Some(Prometheus::new(Some(mapper)).await),
        ..Default::default()
    };
    let h = Harness::configured(TOKENS, settings).await;
    h.flush_to_prometheus(&[
        (
            "timer",
            "ratelimit_server.ShouldRateLimit.response_time",
            3.0,
        ),
        (
            "timer",
            "ratelimit_server.ShouldRateLimit.response_time",
            1500.0,
        ),
        (
            "timer",
            "ratelimit_server.ShouldRateLimit.response_time",
            20_000.0,
        ),
        ("timer", "other.timer", 0.5),
        ("timer", "other.timer", 12.0),
        ("gauge", "cache.size", 10.0),
        ("gauge", "cache.size", 3.0),
        ("gauge", "local.cache", 1_234_567.0),
        ("counter", "neg.x", 2.0),
        ("counter", "empty.x", 1.0),
        ("counter", "drop.x", 1.0),
        ("gauge", "other.timer", 1.0),
        ("counter", "other_timer_bucket", 1.0),
        ("counter", "plain", 1.0),
        ("counter", "dash--es---here-", 1.0),
        ("counter", "late_sum", 1.0),
        ("timer", "late", 1.0),
    ])
    .await;
    // A counter named like a histogram's series: upstream's registry takes it, Gather refuses.
    h.flush_to_prometheus(&[("counter", "other_timer_sum", 1.0)])
        .await;
}

/// A custom mapper file: its help texts and labels; a series not updated within its TTL
/// disappears and restarts from 0.
#[tokio::test]
async fn a_prometheus_mapper_file_is_used_like_upstream() {
    if !tools_available() {
        return;
    }
    let mapper = "
mappings:
- match: ratelimit.service.rate_limit.*.*.*
  name: rl_$3
  help: Rate limit counters.
  ttl: 2s
  labels: {domain: $1, descriptor: $2}
";
    let settings = Settings {
        prometheus: Some(Prometheus::new(Some(mapper)).await),
        ..Default::default()
    };
    let mut h = Harness::configured(TOKENS, settings).await;
    h.at(1_700_000_000).await;
    h.exporter_at(1_000_000_000).await;
    h.call(request("llm", &[&[("user", "alice")]]))
        .await
        .unwrap();
    h.exporter_at(2_000_000_000).await;
    h.call(request("llm", &[&[("user", "bob")]])).await.unwrap();
    h.exporter_at(3_000_000_001).await;
    let scraped = h.exporter_at(4_000_000_001).await;
    assert!(!scraped.body.contains("alice"), "{}", scraped.body);
    h.call(request("llm", &[&[("user", "alice")]]))
        .await
        .unwrap();
}

/// Series the registry takes but Gather refuses make every scrape fail with its error: a
/// reserved label name, a series that duplicates one of statsd_exporter's own.
#[tokio::test]
async fn invalid_series_fail_prometheus_scrapes_like_upstream() {
    if !tools_available() {
        return;
    }
    for mapping in [
        "{match: ratelimit.service.config_load_success, name: loads, labels: {__reserved: x}}",
        // The same help, as which family Go collects first, and so which help it expects, is
        // random.
        "{match: ratelimit.service.config_load_success, name: statsd_exporter_events_total, \
         labels: {type: counter}, help: The total number of StatsD events seen.}",
    ] {
        let mapper = format!("mappings:\n- {mapping}\n");
        let settings = Settings {
            prometheus: Some(Prometheus::new(Some(&mapper)).await),
            ..Default::default()
        };
        let h = Harness::configured(TOKENS, settings).await;
        let scraped = h
            .scrape("GET", "/metrics", &[("Accept-Encoding", "gzip")])
            .await;
        assert_eq!(scraped.status, 500, "{}", scraped.body);
    }
}

/// A mapper file that cannot be read, is not UTF-8 (UTF-16 with a byte order mark is), or does
/// not load maps nothing: the default mapper is not used instead.
#[tokio::test]
async fn an_unusable_prometheus_mapper_file_maps_nothing_like_upstream() {
    if !tools_available() {
        return;
    }
    let utf16: Vec<u8> = [0xFF, 0xFE]
        .into_iter()
        .chain(
            "mappings: [{match: ratelimit.service.config_load_success, name: utf16}]"
                .encode_utf16()
                .flat_map(u16::to_le_bytes),
        )
        .collect();
    let files: [Option<Vec<u8>>; 4] = [
        None,
        Some(b"mappings: [{match: a, name: 1}]".to_vec()),
        Some(
            b"# \xff\nmappings: [{match: ratelimit.service.config_load_success, name: x}]".to_vec(),
        ),
        Some(utf16),
    ];
    for file in files {
        let mut prometheus = Prometheus::new(None).await;
        prometheus.mapper = match file {
            None => MapperFile::Missing,
            Some(bytes) => MapperFile::write(&bytes).await,
        };
        let settings = Settings {
            prometheus: Some(prometheus),
            ..Default::default()
        };
        let mut h = Harness::configured(TOKENS, settings).await;
        h.at(1_700_000_000).await;
        h.call(request("llm", &[&[("user", "alice")]]))
            .await
            .unwrap();
        h.flush_to_prometheus(&[("gauge", "ratelimit.localcache.entryCount", 2.0)])
            .await;
    }
}

/// `PROMETHEUS_RESPONSE_TIME_AS_MILLISECONDS` drops the default mapper's scaling; a subtree
/// `PROMETHEUS_PATH` serves everything under it and redirects to it.
#[tokio::test]
async fn prometheus_options_like_upstream() {
    if !tools_available() {
        return;
    }
    let mut prometheus = Prometheus::new(None).await;
    prometheus.milliseconds = true;
    prometheus.path = "/stats/";
    let settings = Settings {
        prometheus: Some(prometheus),
        ..Default::default()
    };
    let h = Harness::configured(TOKENS, settings).await;
    for target in [
        "/stats/",
        "/stats",
        "/stats/x?y",
        "/metrics",
        "/stats/../stats/",
        "/%73tats",
        "/%73tats/x%20y",
    ] {
        h.scrape("GET", target, &[]).await;
    }
    let time = "ratelimit_server.ShouldRateLimit.response_time";
    let body = h
        .oracle
        .send("POST", "/flush", &format!("timer {time} 7\n"))
        .await;
    assert_eq!(body.0, 200);
    h.subject
        .prometheus
        .as_ref()
        .unwrap()
        .0
        .flush_timer(time, 7.0);
    h.scrape("GET", "/stats/", &[]).await;
}

/// Looks variables up in `environment`, as an environment would.
fn lookup_in<'a>(environment: &'a [(&'a str, String)]) -> impl Fn(&str) -> Option<String> + 'a {
    move |key| {
        environment
            .iter()
            .find(|(k, _)| *k == key)
            .map(|(_, v)| v.clone())
    }
}

/// Upstream's `settings.NewSettings()` for `environment`, from the oracle: the settings as JSON,
/// or the panic's text.
async fn upstream_settings(environment: &[(&str, &str)]) -> Result<serde_json::Value, String> {
    let output = Command::new(oracle_binary())
        .arg("-settings")
        .env_clear()
        .envs(environment.iter().copied())
        .output()
        .await
        .expect("run the oracle");
    let printed: serde_json::Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|_| panic!("oracle output: {}", String::from_utf8_lossy(&output.stdout)));
    match printed.get("error") {
        Some(error) => Err(error.as_str().unwrap().to_string()),
        None => Ok(serde_json::json!({"settings": printed["settings"], "sink": printed["sink"]})),
    }
}

/// How selecting our stats sink fails for `settings`, or "" (the Prometheus sink, which would
/// listen, is left out as on the oracle).
fn sink_failure(
    settings: &ratelimit::settings::Settings,
    lookup: ratelimit::settings::Lookup,
) -> String {
    let sinks = match settings.sinks(lookup) {
        Ok(sinks) => sinks,
        Err(e) => return e,
    };
    if !sinks.disable_stats && !sinks.use_dog_statsd && !sinks.use_statsd && sinks.use_prometheus {
        return String::new();
    }
    match ratelimit::stats::select_sink(&sinks) {
        Ok(_) => String::new(),
        Err(e) => e,
    }
}

/// Our settings, shaped like the oracle's JSON.
fn settings_json(settings: &ratelimit::settings::Settings) -> serde_json::Value {
    use ratelimit::settings::Value;
    let values = settings.values().into_iter().map(|(key, value)| {
        let json = match value {
            Value::Str(s) => serde_json::json!(s),
            Value::Int(i) => serde_json::json!(i),
            Value::Bool(b) => serde_json::json!(b),
            Value::Float(f) => serde_json::json!(ratelimit::go::format_float(f)),
            Value::List(l) => serde_json::json!(l),
            Value::Map(m) => serde_json::json!(m),
        };
        (key.to_string(), json)
    });
    serde_json::Value::Object(values.collect())
}

/// Every variable is read as envconfig reads it: same values, or the same first error.
#[tokio::test]
async fn settings_are_read_like_upstream() {
    if !tools_available() {
        return;
    }
    let ints = [
        "",
        "0",
        "00",
        "-1",
        "+7",
        "0x1F",
        "0X1f",
        "0o17",
        "0O17",
        "017",
        "08",
        "0b101",
        "1_000",
        "0x_1f",
        "0_7",
        "_1",
        "1_",
        "1__0",
        "0x",
        "0b",
        "0o",
        "1.0",
        " 1",
        "9223372036854775807",
        "9223372036854775808",
        "-9223372036854775808",
        "-9223372036854775809",
        "99999999999999999999x",
        "0xFFFFFFFFFFFFFFFF",
        "-0x8000000000000000",
    ];
    let bools = [
        "", "1", "t", "T", "TRUE", "true", "True", "0", "f", "F", "FALSE", "false", "False", "yes",
        "tRue", " true",
    ];
    let durations = [
        "",
        "0",
        "-0",
        "+0",
        "1s",
        "1.5h",
        "1h2m3.5s",
        "-1m",
        ".5s",
        "5.s",
        "1",
        "1x",
        "1µs",
        "1μs",
        "1us",
        "9223372036854775807ns",
        "9223372036854775808ns",
        "-9223372036854775808ns",
        "2562047h",
        "2562048h",
        "0.0000000001s",
        "1.123456789123s",
        "s",
        ".s",
        "1ss",
        "1s2",
        "3m.5s",
        "-",
        "1e3s",
        "1é",
        "é",
        "1\"s\\",
        "1\t",
    ];
    let floats = [
        "",
        "0.8",
        "1e39",
        "-1e39",
        "3.4028235e38",
        "3.4028236e38",
        "1e309",
        "1.7976931348623157e308",
        "1e-50",
        "inf",
        "+Inf",
        "-infinity",
        "INFINITY",
        "infinit",
        "nan",
        "NaN",
        "+nan",
        "-NaN",
        "0x1p-2",
        "0x1.8p1",
        "0X1P3",
        "0x1p",
        "0x.8p1",
        "0xp1",
        "1_000.5",
        "1__0",
        "_1",
        "1e",
        ".5",
        "5.",
        ".",
        "1E+3",
        "1e-3",
        "0x10",
        " 1",
        "1_e3",
        "1e_3",
        "0.1_5",
        "0x1p-1074",
        "0x0.8p-1022",
        "0x1.0000000000000801p0",
        "0x1.0000010000000000001p0",
        "0x0p999999999999",
        "0x1p99999999999999999999",
        "0x1p-99999999999",
        "0x1.fffffffffffff8p1023",
        "0x1p-1075",
        "0x1p-149",
        "0x_1p0",
        "0x1p_0",
        "0x1p0x",
        "0x1.8",
        "0x.p1",
        "0x1p+",
        "-0x0p0",
    ];
    let lists = ["", " ", "a", "a,b", "a,,b", ",", " a , b "];
    let maps = [
        "",
        " ",
        "a:b",
        "a:b,c:d",
        "a:b,a:c",
        "a",
        "a:b:c",
        "a:b,",
        ":",
        "a:",
        ":b",
        "é",
        "a\tb",
        "a\u{1}b",
        "\u{a0}",
        "\u{200b}",
        "\u{feff}",
        "\u{85}",
        "\u{2028}",
        "\u{ad}",
        "\u{1f600}",
        "\u{e0001}",
    ];
    let mut cases: Vec<Vec<(&str, &str)>> = vec![vec![]];
    for (variables, values) in [
        (
            &[
                "PORT",
                "XDS_CLIENT_MAX_MSG_SIZE_IN_BYTES",
                "EXPIRATION_JITTER_MAX_SECONDS",
            ][..],
            &ints[..],
        ),
        (&["USE_STATSD", "RUNTIME_WATCH_ROOT"], &bools),
        (&["REDIS_TIMEOUT", "RUNTIME_SUBDIRECTORY"], &durations),
        (
            &["NEAR_LIMIT_RATIO", "XDS_CLIENT_BACKOFF_RANDOM_FACTOR"],
            &floats,
        ),
        (&["USE_DOG_STATSD_MOGRIFIERS", "MEMCACHE_HOST_PORT"], &lists),
        (
            &["EXTRA_TAGS", "CONFIG_GRPC_XDS_CLIENT_ADDITIONAL_HEADERS"],
            &maps,
        ),
    ] {
        for variable in variables {
            for value in values {
                cases.push(vec![(variable, value)]);
            }
        }
    }
    // The sinks: selection, gostats' own checks, DogStatsD mogrifiers from the environment.
    let statsd_off = ("USE_STATSD", "false");
    let dog = ("USE_DOG_STATSD", "true");
    let mogrifiers = ("USE_DOG_STATSD_MOGRIFIERS", "a,b");
    for case in [
        vec![("STATSD_PORT", "0x1F")],
        vec![("STATSD_PORT", "+8125")],
        vec![("STATSD_PORT", "1_000")],
        vec![("GOSTATS_FLUSH_INTERVAL_SECONDS", "x")],
        vec![("GOSTATS_FLUSH_INTERVAL_SECONDS", "99999999999999999999999x")],
        vec![("GOSTATS_FLUSH_INTERVAL_SECONDS", "9223372036854775808")],
        vec![("GOSTATS_FLUSH_INTERVAL_SECONDS", "-9223372036854775808")],
        vec![("GOSTATS_FLUSH_INTERVAL_SECONDS", "+")],
        vec![("GOSTATS_FLUSH_INTERVAL_SECONDS", "")],
        vec![("GOSTATS_LOGGING_SINK_DISABLED", "yes")],
        vec![("GOSTATS_LOGGING_SINK_DISABLED", "yes"), statsd_off],
        vec![dog],
        vec![
            dog,
            statsd_off,
            ("STATSD_PORT", "0x1F"),
            ("GOSTATS_FLUSH_INTERVAL_SECONDS", "x"),
        ],
        vec![dog, statsd_off, ("USE_PROMETHEUS", "true")],
        vec![("DISABLE_STATS", "true"), dog, ("STATSD_PORT", "x1")],
        vec![dog, statsd_off, mogrifiers],
        vec![
            dog,
            statsd_off,
            mogrifiers,
            ("DOG_STATSD_MOGRIFIER_A_PATTERN", "^a"),
        ],
        vec![
            dog,
            statsd_off,
            mogrifiers,
            ("PATTERN", "^x"),
            ("NAME", "n"),
        ],
        vec![
            dog,
            statsd_off,
            mogrifiers,
            ("PATTERN", "^x"),
            ("NAME", "n"),
            ("TAGS", "t"),
        ],
        vec![
            dog,
            statsd_off,
            mogrifiers,
            ("PATTERN", "^x"),
            ("NAME", "n"),
            ("TAGS", "t:"),
        ],
        vec![
            dog,
            statsd_off,
            mogrifiers,
            ("PATTERN", "^x"),
            ("NAME", "n"),
            ("TAGS", ":v"),
        ],
        vec![
            dog,
            statsd_off,
            mogrifiers,
            ("PATTERN", "^x"),
            ("NAME", "n"),
            ("DOG_STATSD_MOGRIFIER_B_PATTERN", ""),
        ],
        vec![
            dog,
            statsd_off,
            ("USE_DOG_STATSD_MOGRIFIERS", "lower"),
            ("DOG_STATSD_MOGRIFIER_LOWER_PATTERN", "x"),
            ("DOG_STATSD_MOGRIFIER_LOWER_NAME", "y"),
        ],
        vec![dog, statsd_off, ("STATSD_PORT", "70000")],
        vec![
            dog,
            statsd_off,
            ("USE_DOG_STATSD_MOGRIFIERS", "ß"),
            ("DOG_STATSD_MOGRIFIER_ß_PATTERN", "x"),
            ("DOG_STATSD_MOGRIFIER_ß_NAME", "y"),
        ],
        vec![dog, statsd_off, ("STATSD_HOST", "a:b")],
    ] {
        cases.push(case);
    }
    // envconfig also reads the fields of the untagged `*tls.Config` settings.
    for (variable, value) in [
        ("REDISTLSCONFIG_MINVERSION", "abc"),
        ("REDISTLSCONFIG_MINVERSION", "70000"),
        ("REDISTLSCONFIG_MAXVERSION", "0x304"),
        ("GRPCSERVERTLSCONFIG_INSECURESKIPVERIFY", "x"),
        ("CONFIGGRPCXDSTLSCONFIG_CLIENTAUTH", "z"),
        ("MEMCACHETLSCONFIG_CURVEPREFERENCES", "1,x"),
        ("MEMCACHETLSCONFIG_CURVEPREFERENCES", " "),
        ("REDISTLSCONFIG_CIPHERSUITES", "+1"),
        ("REDISTLSCONFIG_NAMETOCERTIFICATE", "a"),
        ("REDISTLSCONFIG_NAMETOCERTIFICATE", "a:b"),
        ("REDISTLSCONFIG_RENEGOTIATION", "1_0"),
        ("REDISTLSCONFIG_SERVERNAME", "anything"),
        ("REDISTLSCONFIG_SESSIONTICKETKEY", "x"),
        ("REDISTLSCONFIG_CERTIFICATES", "x,y"),
        ("REDISTLSCONFIG_ROOTCAS", "x"),
    ] {
        cases.push(vec![(variable, value)]);
    }
    // In field order: GrpcServerTlsConfig comes before GRPC_MAX_CONNECTION_AGE.
    cases.push(vec![
        ("GRPC_MAX_CONNECTION_AGE", "x"),
        ("GRPCSERVERTLSCONFIG_MINVERSION", "y"),
    ]);
    // Two invalid values: the first field in upstream's order is reported.
    cases.push(vec![
        ("TRACING_SAMPLING_RATE", "x"),
        ("HOST", "h"),
        ("PORT", "y"),
    ]);
    cases.push(vec![
        ("REDIS_URL", ""),
        ("PROMETHEUS_ADDR", ""),
        ("LOG_LEVEL", "debug"),
    ]);
    for environment in cases {
        let lookup = |key: &str| {
            environment
                .iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| v.to_string())
        };
        let ours = ratelimit::settings::Settings::from_lookup(&lookup)
            .map(|s| serde_json::json!({"settings": settings_json(&s), "sink": sink_failure(&s, &lookup)}))
            .map_err(|e| e.to_string());
        let upstream = upstream_settings(&environment).await;
        match (&ours, &upstream) {
            (Ok(ours), Ok(upstream)) => {
                assert_eq!(ours["settings"], upstream["settings"], "{environment:?}");
                assert_eq!(ours["sink"], upstream["sink"], "sink for {environment:?}");
            }
            _ => assert_eq!(ours, upstream, "{environment:?}"),
        }
    }
}

/// A temporary directory, removed when dropped (even when a test fails).
struct TempDir(PathBuf);

impl std::ops::Deref for TempDir {
    type Target = std::path::Path;
    fn deref(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A fresh temporary directory.
fn temp_dir(name: &str) -> TempDir {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "ratelimit-{name}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    TempDir(dir)
}

/// Writes `files` (path → contents; a contents of `->target` makes a symlink) under `root`.
fn write_tree(root: &std::path::Path, files: &[(&str, &[u8])]) {
    for (path, contents) in files {
        let path = root.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        match contents.strip_prefix(b"->") {
            Some(target) => {
                std::os::unix::fs::symlink(std::str::from_utf8(target).unwrap(), &path).unwrap()
            }
            None if contents == b"/" => std::fs::create_dir_all(&path).unwrap(),
            None => std::fs::write(&path, contents).unwrap(),
        }
    }
}

/// Upstream's first runtime snapshot for `environment`, from the oracle.
async fn upstream_snapshot(environment: &[(&str, String)]) -> serde_json::Value {
    let output = Command::new(oracle_binary())
        .arg("-runtime-snapshot")
        .env_clear()
        .envs(environment.iter().map(|(k, v)| (*k, v.as_str())))
        .output()
        .await
        .expect("run the oracle");
    serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|_| panic!("oracle output: {}", String::from_utf8_lossy(&output.stdout)))
}

/// Ours, shaped like the oracle's JSON.
fn our_snapshot(environment: &[(&str, String)]) -> serde_json::Value {
    use base64::Engine;
    let settings = ratelimit::settings::Settings::from_lookup(&lookup_in(environment)).unwrap();
    let stats = Stats::new(&settings.extra_tags);
    match ratelimit::runtime::loader(&settings, &stats) {
        Err(e) => serde_json::json!({ "error": e }),
        Ok(loader) => {
            let snapshot: serde_json::Map<String, serde_json::Value> = loader
                .snapshot()
                .entries
                .into_iter()
                .map(|(k, v)| {
                    let encoded = base64::engine::general_purpose::STANDARD.encode(v);
                    (k, serde_json::json!(encoded))
                })
                .collect();
            let counters: serde_json::Map<String, serde_json::Value> = stats
                .snapshot()
                .into_iter()
                .filter(|(name, _)| name.starts_with("runtime.") || name == "reserved_tag")
                .map(|(k, v)| (k, serde_json::json!(v)))
                .collect();
            serde_json::json!({ "snapshot": snapshot, "counters": counters })
        }
    }
}

/// goruntime's loader takes the same first snapshot of a directory as upstream: keys from
/// relative paths, symlinked files read through, symlinked directories and unreadable entries
/// counted as failures, dotfiles skipped only when asked, the nil loader without a
/// subdirectory; and fails to start the same way.
#[tokio::test]
async fn runtime_snapshots_are_taken_like_upstream() {
    if !tools_available() {
        return;
    }
    let tree: &[(&str, &[u8])] = &[
        ("v1/ratelimit/config/a.yaml", b"domain: a\n"),
        ("v1/ratelimit/config/b.yml", b"x"),
        ("v1/ratelimit/config/.hidden", b"h"),
        ("v1/ratelimit/config/sub/c.yaml", b"c"),
        ("v1/ratelimit/config/.dotdir/d.yaml", b"d"),
        ("v1/ratelimit/config/link.yaml", b"->a.yaml"),
        ("v1/ratelimit/config/dirlink", b"->sub"),
        ("v1/ratelimit/config/dangling", b"->nowhere"),
        ("v1/ratelimit/config/latin1", b"caf\xe9"),
        ("v1/ratelimit/config/empty", b""),
        ("v1/ratelimit/config.yaml", b"root level"),
        ("v1/ratelimit/other/e.yaml", b"e"),
        ("v1/ratelimit/config/..2024_01/k.yaml", b"k"),
        ("v1/ratelimit/config/..data", b"->..2024_01"),
        ("v1/ratelimit/config/k.yaml", b"->..data/k.yaml"),
        ("v1/ratelimit/afile", b"a file as the app directory"),
        ("v1/ratelimit/.x/y", b"y"),
        ("current", b"->v1"),
    ];
    let root = temp_dir("runtime");
    write_tree(&root, tree);
    use std::os::unix::ffi::OsStrExt;
    let latin1_name = std::ffi::OsStr::from_bytes(b"caf\xe9.yaml");
    // macOS refuses names that are not UTF-8.
    if cfg!(target_os = "linux") {
        std::fs::write(
            root.join("v1/ratelimit/config").join(latin1_name),
            b"latin1",
        )
        .unwrap();
    }
    let unreadable = root.join("v1/ratelimit/config/secret");
    std::fs::write(&unreadable, b"s").unwrap();
    let locked = root.join("v1/ratelimit/config/locked");
    std::fs::create_dir_all(&locked).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o000)).unwrap();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
    let path = |p: &str| root.join(p).to_string_lossy().into_owned();
    let env = |pairs: &[(&'static str, String)]| pairs.to_vec();
    let mut cases = Vec::new();
    for ignore in ["false", "true"] {
        let ignore = ("RUNTIME_IGNOREDOTFILES", ignore.to_string());
        cases.push(env(&[
            ("RUNTIME_ROOT", path("current")),
            ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
            ignore.clone(),
        ]));
        for (subdirectory, app) in [
            ("ratelimit", "config"),
            ("ratelimit", "afile"),
            ("ratelimit", ".x"),
            ("", "ratelimit"),
        ] {
            cases.push(env(&[
                ("RUNTIME_ROOT", path("current")),
                ("RUNTIME_SUBDIRECTORY", subdirectory.into()),
                ("RUNTIME_APPDIRECTORY", app.into()),
                ("RUNTIME_WATCH_ROOT", "false".into()),
                ignore.clone(),
            ]));
        }
    }
    // A missing RUNTIME_ROOT is a failure; a missing watched directory, an error.
    cases.push(env(&[
        ("RUNTIME_ROOT", path("missing")),
        ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
    ]));
    cases.push(env(&[
        ("RUNTIME_ROOT", path("missing/deeper")),
        ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
    ]));
    cases.push(env(&[
        ("RUNTIME_ROOT", path("current")),
        ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
        ("RUNTIME_APPDIRECTORY", "missing".into()),
        ("RUNTIME_WATCH_ROOT", "false".into()),
    ]));
    // Relative paths, down to `.` (the test's directory, the same on both sides).
    cases.push(env(&[
        ("RUNTIME_ROOT", ".".into()),
        ("RUNTIME_SUBDIRECTORY", ".".into()),
        ("RUNTIME_APPDIRECTORY", "src".into()),
        ("RUNTIME_WATCH_ROOT", "false".into()),
    ]));
    cases.push(env(&[
        ("RUNTIME_ROOT", ".".into()),
        ("RUNTIME_SUBDIRECTORY", ".".into()),
        ("RUNTIME_APPDIRECTORY", ".".into()),
        ("RUNTIME_WATCH_ROOT", "false".into()),
        ("RUNTIME_IGNOREDOTFILES", "true".into()),
    ]));
    // Without a subdirectory: the nil loader; the runtime scope still counts reserved tags.
    cases.push(env(&[
        ("RUNTIME_ROOT", path("current")),
        ("EXTRA_TAGS", "host:h,az:z".into()),
    ]));
    cases.push(env(&[
        ("RUNTIME_ROOT", path("current")),
        ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
        ("EXTRA_TAGS", "host:h.1,app:x".into()),
    ]));
    for environment in cases {
        let (mut ours, mut upstream) = (
            our_snapshot(&environment),
            upstream_snapshot(&environment).await,
        );
        // Off Linux, upstream's fsnotify uses kqueue, whose errors read differently.
        if !cfg!(target_os = "linux")
            && ours.get("error").is_some()
            && upstream.get("error").is_some()
        {
            ours["error"] = serde_json::json!("error");
            upstream["error"] = serde_json::json!("error");
        }
        assert_eq!(ours, upstream, "{environment:?}");
    }
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
}

const LLM_CONFIG: &[u8] = b"domain: llm\ndescriptors:\n  - key: user\n    rate_limit: {unit: minute, requests_per_unit: 2}\n";
const OTHER_CONFIG: &[u8] = b"domain: other\ndescriptors:\n  - key: k\n    rate_limit: {unit: hour, requests_per_unit: 1}\n";

/// `RUNTIME_*` variables for a runtime directory at `root`, watched as a symlink
/// (`root/current`) or as a directory.
fn runtime_environment(root: &std::path::Path, watch_root: bool) -> Vec<(&'static str, String)> {
    vec![
        (
            "RUNTIME_ROOT",
            root.join("current").to_string_lossy().into_owned(),
        ),
        ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
        ("RUNTIME_APPDIRECTORY", "config".into()),
        ("RUNTIME_WATCH_ROOT", watch_root.to_string()),
        ("RUNTIME_IGNOREDOTFILES", "true".into()),
    ]
}

async fn file_provider_harness(root: &std::path::Path, watch_root: bool) -> Harness {
    let settings = Settings {
        runtime: Some(runtime_environment(root, watch_root)),
        extra_tags: [("host", "h")]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        ..Default::default()
    };
    Harness::configured("", settings).await
}

/// The file provider loads the config files of the runtime directory at start, as upstream:
/// in watch-root mode only those under the app directory; a bad file fails the whole load.
#[tokio::test]
async fn the_file_provider_loads_the_runtime_directory_like_upstream() {
    if !tools_available() {
        return;
    }
    for watch_root in [true, false] {
        let root = temp_dir("provider");
        let wide: Vec<u8> = [0xFF, 0xFE]
            .into_iter()
            .chain(
                "domain: wide\ndescriptors:\n  - key: k\n    rate_limit: {unit: second, requests_per_unit: 1}\n"
                    .encode_utf16()
                    .flat_map(u16::to_le_bytes),
            )
            .collect();
        write_tree(
            &root,
            &[
                ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
                ("v1/ratelimit/config/wide.yaml", &wide),
                ("v1/ratelimit/config/nested/other.yaml", OTHER_CONFIG),
                ("v1/ratelimit/config/.ignored.yaml", b"not: yaml: at all"),
                ("v1/ratelimit/elsewhere.yaml", b"domain: elsewhere\n"),
                ("current", b"->v1"),
            ],
        );
        let mut h = file_provider_harness(&root, watch_root).await;
        h.at(1_700_000_000).await;
        for _ in 0..3 {
            h.call(request("llm", &[&[("user", "a")]])).await.unwrap();
        }
        h.call(request("other", &[&[("k", "v")]])).await.unwrap();
        h.call(request("wide", &[&[("k", "v")]])).await.unwrap();
        h.call(request("elsewhere", &[&[("k", "v")]]))
            .await
            .unwrap();
    }
    let root = temp_dir("provider");
    write_tree(
        &root,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("v1/ratelimit/config/zz.yaml", b"domain: [unclosed\n"),
            ("current", b"->v1"),
        ],
    );
    let mut h = file_provider_harness(&root, true).await;
    h.at(1_700_000_000).await;
    let _ = h.call(request("llm", &[&[("user", "a")]])).await;
    // Bytes yaml.v2 cannot read fail the load too.
    for bad in [
        &b"domain: caf\xe9\n"[..],
        b"domain: a\x01\n",
        b"\xff\xfed\x00\x00\xdc",
    ] {
        let root = temp_dir("provider");
        write_tree(
            &root,
            &[("v1/ratelimit/config/bad.yaml", bad), ("current", b"->v1")],
        );
        let mut h = file_provider_harness(&root, true).await;
        let _ = h.call(request("a", &[&[("k", "v")]])).await;
    }
}

/// yaml.v2's reader refuses control characters, and skips a byte order mark.
#[tokio::test]
async fn config_characters_are_read_like_upstream() {
    if !tools_available() {
        return;
    }
    let mut h = Harness::start(TOKENS).await;
    for config in [
        "domain: a\x01",
        "domain: a\x7f",
        "# \x0b\ndomain: a",
        "domain: a\u{fffe}",
    ] {
        assert!(h.load_config(config).await.is_err(), "{config:?}");
    }
    h.load_config("\u{feff}domain: bom\n").await.unwrap();
    // NEL breaks lines in YAML 1.1; LS stays a character in a scalar.
    for config in [
        "domain: tab\tand\u{a0}\u{85}descriptors: []\n",
        "domain: \"a\u{2028}b\"\n",
        "domain: a\u{85}descriptors:\u{85}  - key: k\u{85}    rate_limit: {unit: second, requests_per_unit: 1}\n",
    ] {
        let _ = h.load_config(config).await;
    }
}

/// Reloads follow the same file system events as upstream's (inotify, so Linux only): an
/// atomic symlink swap in watch-root mode, a file moved into the directory otherwise.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn the_file_provider_reloads_like_upstream() {
    if !tools_available() {
        return;
    }
    // Watch root: v2 replaces v1 by renaming a new symlink over `current`.
    let root = temp_dir("reload");
    write_tree(
        &root,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("v2/ratelimit/config/other.yaml", OTHER_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let mut h = file_provider_harness(&root, true).await;
    h.at(1_700_000_000).await;
    h.call(request("llm", &[&[("user", "a")]])).await.unwrap();
    std::os::unix::fs::symlink("v2", root.join("next")).unwrap();
    std::fs::rename(root.join("next"), root.join("current")).unwrap();
    h.await_config_loads(2).await;
    h.call(request("llm", &[&[("user", "a")]])).await.unwrap();
    h.call(request("other", &[&[("k", "v")]])).await.unwrap();
    // Edits inside the target do not reload, nor does a change of the symlink's own times
    // (a chmod event on it).
    std::fs::write(root.join("v2/ratelimit/config/llm.yaml"), LLM_CONFIG).unwrap();
    let touched = std::process::Command::new("touch")
        .arg("-h")
        .arg(root.join("current"))
        .status()
        .unwrap();
    assert!(touched.success());
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    h.call(request("llm", &[&[("user", "a")]])).await.unwrap();

    // Directory: a file moved in reloads; one moved out does not (a rename), a deleted one does.
    let root = temp_dir("reload");
    write_tree(
        &root,
        &[
            ("current/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("staging/other.yaml", OTHER_CONFIG),
        ],
    );
    let mut h = file_provider_harness(&root, false).await;
    h.at(1_700_000_000).await;
    h.call(request("other", &[&[("k", "v")]])).await.unwrap();
    std::fs::rename(
        root.join("staging/other.yaml"),
        root.join("current/ratelimit/config/other.yaml"),
    )
    .unwrap();
    h.await_config_loads(2).await;
    h.call(request("other", &[&[("k", "v")]])).await.unwrap();
    std::fs::rename(
        root.join("current/ratelimit/config/llm.yaml"),
        root.join("staging/llm.yaml"),
    )
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    h.call(request("llm", &[&[("user", "a")]])).await.unwrap();
    std::fs::remove_file(root.join("current/ratelimit/config/other.yaml")).unwrap();
    h.await_config_loads(3).await;
    h.call(request("llm", &[&[("user", "a")]])).await.unwrap();
    h.call(request("other", &[&[("k", "v")]])).await.unwrap();
}

/// Upstream's own service binary (`src/service_cmd`), built at the pinned commit.
fn upstream_binary() -> PathBuf {
    static BINARY: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    BINARY
        .get_or_init(|| {
            let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
            let binary = root.join("target/upstream-ratelimit");
            let status = std::process::Command::new("go")
                .args(["build", "-o"])
                .arg(&binary)
                .arg("./upstream")
                .current_dir(root.join("tools/oracle"))
                .status()
                .expect("run go build");
            assert!(status.success(), "go build failed");
            binary
        })
        .clone()
}

/// A service binary running with `environment`, killed when dropped.
struct Service {
    _process: Child,
    grpc: String,
    /// `HOST:PORT`.
    http: String,
    /// `DEBUG_HOST:DEBUG_PORT`.
    debug: String,
}

impl Service {
    async fn start(binary: &std::path::Path, environment: &[(&str, String)]) -> Self {
        let (grpc_port, http_port, debug_port) =
            (reserved_port(), reserved_port(), reserved_port());
        // The caller's variables come last, so that they win.
        let mut defaults = vec![
            ("GRPC_HOST", "127.0.0.1".to_string()),
            ("GRPC_PORT", grpc_port.to_string()),
            ("HOST", "127.0.0.1".into()),
            ("PORT", http_port.to_string()),
            ("DEBUG_HOST", "127.0.0.1".into()),
            ("DEBUG_PORT", debug_port.to_string()),
        ];
        defaults.extend(environment.iter().cloned());
        let environment = defaults;
        let process = Command::new(binary)
            .env_clear()
            .envs(environment.iter().map(|(k, v)| (*k, v.as_str())))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("start the service");
        Self {
            _process: process,
            grpc: format!("http://127.0.0.1:{grpc_port}"),
            http: format!("127.0.0.1:{http_port}"),
            debug: format!("127.0.0.1:{debug_port}"),
        }
    }

    async fn client(&self) -> RateLimitServiceClient<Channel> {
        for _ in 0..300 {
            if let Ok(client) = RateLimitServiceClient::connect(self.grpc.clone()).await {
                return client;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        panic!("the service at {} did not start", self.grpc);
    }
}

/// What both binaries share: the runtime directory, Redis over TCP, no stats, no jitter.
fn binary_environment(runtime: &std::path::Path, redis: &Redis) -> Vec<(&'static str, String)> {
    vec![
        (
            "RUNTIME_ROOT",
            runtime.join("current").to_string_lossy().into_owned(),
        ),
        ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
        ("REDIS_SOCKET_TYPE", "tcp".into()),
        ("REDIS_URL", redis.address.clone()),
        ("DISABLE_STATS", "true".into()),
        ("EXPIRATION_JITTER_MAX_SECONDS", "0".into()),
        ("LOG_LEVEL", "error".into()),
    ]
}

/// Our binary serves like upstream's: the config from the runtime directory, the limits in
/// Redis, over gRPC.
#[tokio::test]
async fn the_binary_serves_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("binary");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let (upstream_redis, our_redis) = (Redis::start().await, Redis::start().await);
    let upstream = Service::start(
        &upstream_binary(),
        &binary_environment(&runtime, &upstream_redis),
    )
    .await;
    let ours = Service::start(
        std::path::Path::new(env!("CARGO_BIN_EXE_ratelimit")),
        &binary_environment(&runtime, &our_redis),
    )
    .await;
    let (mut upstream_client, mut our_client) = (upstream.client().await, ours.client().await);
    for request in [
        request("llm", &[&[("user", "a")]]),
        request("llm", &[&[("user", "a")]]),
        request("llm", &[&[("user", "a")]]),
        request("unknown", &[&[("k", "v")]]),
        request("", &[&[("k", "v")]]),
    ] {
        let simplify = |r: Result<tonic::Response<RateLimitResponse>, tonic::Status>| {
            r.map(tonic::Response::into_inner)
                .map_err(|s| format!("{:?}: {}", s.code(), s.message()))
        };
        let expected = simplify(upstream_client.should_rate_limit(request.clone()).await);
        let mut actual = simplify(our_client.should_rate_limit(request.clone()).await);
        // Each side reads its own clock: the time to reset may differ by a second.
        if let (Ok(expected), Ok(actual)) = (&expected, &mut actual) {
            for (e, a) in expected.statuses.iter().zip(actual.statuses.iter_mut()) {
                if let (Some(e), Some(a)) = (&e.duration_until_reset, &mut a.duration_until_reset) {
                    assert!((e.seconds - a.seconds).abs() <= 1, "{e:?} {a:?}");
                    a.seconds = e.seconds;
                    a.nanos = e.nanos;
                }
            }
        }
        assert_eq!(actual, expected, "response to {request:?}");
    }
}

/// What a binary prints when it cannot start, after it exits (it must).
async fn startup_failure(binary: &std::path::Path, environment: &[(&str, String)]) -> String {
    let child = Command::new(binary)
        .env_clear()
        .envs(environment.iter().map(|(k, v)| (*k, v.as_str())))
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("start the service");
    let output = tokio::time::timeout(std::time::Duration::from_secs(30), child.wait_with_output())
        .await
        .unwrap_or_else(|_| panic!("{} did not exit with {environment:?}", binary.display()))
        .unwrap();
    assert!(
        !output.status.success(),
        "{} started with {environment:?}",
        binary.display()
    );
    // logrus quotes its messages.
    String::from_utf8_lossy(&output.stderr)
        .replace("\\\"", "\"")
        .replace("\\n", "\n")
}

/// Our binary refuses to start where upstream's does (exits or panics), with its message.
#[tokio::test]
async fn the_binary_fails_to_start_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("binary");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let redis = Redis::start().await;
    let busy = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let busy_port = busy.local_addr().unwrap().port().to_string();
    let cases: Vec<(Vec<(&'static str, String)>, String)> = vec![
        (vec![("PORT", "x".into())], "envconfig.Process: assigning PORT to Port: converting 'x' to type int. details: strconv.ParseInt: parsing \"x\": invalid syntax".into()),
        (vec![("LOG_LEVEL", "bogus".into())], "Could not parse log level. not a valid logrus Level: \"bogus\"".into()),
        (vec![("DISABLE_STATS", "false".into()), ("USE_DOG_STATSD", "true".into())], "Error: unable to use more than one stats sink at the same time. Set one of USE_DOG_STATSD, USE_STATSD, USE_PROMETHEUS.".into()),
        (vec![("DISABLE_STATS", "false".into()), ("STATS_FLUSH_INTERVAL", "0".into())], "non-positive interval for NewTicker".into()),
        (vec![("CONFIG_TYPE", "bogus".into())], "Invalid setting for ConfigType: bogus".into()),
        (vec![("RUNTIME_WATCH_ROOT", "false".into()), ("RUNTIME_APPDIRECTORY", "missing".into())], format!("unable to watch file ({}/ratelimit/missing)", runtime.join("current").display())),
        (vec![("BACKEND_TYPE", "bogus".into())], "Invalid setting for BackendType: bogus".into()),
        (vec![("REDIS_POOL_ON_EMPTY_BEHAVIOR", "create".into())], "REDIS_POOL_ON_EMPTY_BEHAVIOR=CREATE is not supported in radix v4.".into()),
        (vec![("REDIS_POOL_ON_EMPTY_BEHAVIOR", "Error".into())], "REDIS_POOL_ON_EMPTY_BEHAVIOR=ERROR is not supported in radix v4. Pool will block instead of failing fast. Remove this setting or set to WAIT, and use context timeouts for fail-fast behavior.".into()),
        (vec![("REDIS_TYPE", "Foo".into())], "Unrecognized redis type Foo".into()),
        // Redis refuses the database: its error ends the message.
        (vec![("REDIS_URL", format!("redis://{}/abc", redis.address)), ("REDIS_STARTUP_MAX_ELAPSED_TIME", "1s".into())], "value is not an integer or out of range".into()),
        (vec![("REDIS_URL", "::1:6379".into()), ("REDIS_STARTUP_MAX_ELAPSED_TIME", "1s".into())], "timed out waiting for Redis connection to ::1:6379 after ".into()),
        (vec![("REDIS_URL", "127.0.0.1:1".into()), ("REDIS_STARTUP_MAX_ELAPSED_TIME", "1s".into())], "timed out waiting for Redis connection to 127.0.0.1:1 after ".into()),
        (vec![("GRPC_PORT_BUSY", busy_port.clone())], format!("Failed to listen for gRPC on '127.0.0.1:{busy_port}': ")),
    ];
    for (overrides, message) in cases {
        let mut environment = binary_environment(&runtime, &redis);
        environment.extend([("GRPC_HOST", "127.0.0.1".to_string())]);
        environment.extend([
            ("PORT", reserved_port().to_string()),
            ("DEBUG_PORT", reserved_port().to_string()),
        ]);
        for (key, value) in overrides {
            let key = if key == "GRPC_PORT_BUSY" {
                "GRPC_PORT"
            } else {
                key
            };
            environment.retain(|(k, _)| *k != key);
            environment.push((key, value));
        }
        if !environment.iter().any(|(k, _)| *k == "GRPC_PORT") {
            environment.push(("GRPC_PORT", reserved_port().to_string()));
        }
        let upstream = startup_failure(&upstream_binary(), &environment).await;
        assert!(
            upstream.contains(&message),
            "upstream with {environment:?}:\n{upstream}"
        );
        let ours = startup_failure(
            std::path::Path::new(env!("CARGO_BIN_EXE_ratelimit")),
            &environment,
        )
        .await;
        assert!(
            ours.contains(&message),
            "ours with {environment:?}:\n{ours}"
        );
        // A `logger.Fatal` upstream: the same logrus lines.
        let fatal = upstream.contains("level=fatal");
        if fatal {
            let lines = |stderr: &str| logrus_lines(&stderr.replace("\n", "\\n"), &[]);
            assert_eq!(lines(&ours), lines(&upstream), "{environment:?}");
        }
    }
    drop(busy);
}

/// A gRPC client over the Unix socket at `path`.
async fn unix_client(path: &std::path::Path) -> RateLimitServiceClient<Channel> {
    for _ in 0..300 {
        let path = path.to_path_buf();
        let connector = tower::service_fn(move |_: tonic::transport::Uri| {
            let path = path.clone();
            async move {
                let stream = tokio::net::UnixStream::connect(path).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
            }
        });
        let endpoint = tonic::transport::Endpoint::from_static("http://[::]:50051");
        if let Ok(channel) = endpoint.connect_with_connector(connector).await {
            return RateLimitServiceClient::new(channel);
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("no gRPC server on {}", path.display());
}

/// With `GRPC_UDS`, both binaries serve gRPC on that Unix socket instead.
#[tokio::test]
async fn the_binary_serves_on_a_unix_socket_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("uds");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let (upstream_redis, our_redis) = (Redis::start().await, Redis::start().await);
    let mut clients = Vec::new();
    let mut services = Vec::new();
    for (binary, redis, socket) in [
        (
            upstream_binary(),
            &upstream_redis,
            runtime.join("upstream.sock"),
        ),
        (
            PathBuf::from(env!("CARGO_BIN_EXE_ratelimit")),
            &our_redis,
            runtime.join("ours.sock"),
        ),
    ] {
        let mut environment = binary_environment(&runtime, redis);
        environment.push(("GRPC_UDS", socket.to_string_lossy().into_owned()));
        services.push(Service::start(&binary, &environment).await);
        clients.push(unix_client(&socket).await);
    }
    let request = request("llm", &[&[("user", "a")]]);
    let expected = clients[0]
        .should_rate_limit(request.clone())
        .await
        .unwrap()
        .into_inner();
    let actual = clients[1]
        .should_rate_limit(request)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(actual.overall_code, expected.overall_code);
    assert_eq!(actual.statuses.len(), expected.statuses.len());
    assert_eq!(
        actual.statuses[0].limit_remaining,
        expected.statuses[0].limit_remaining
    );
}

/// On SIGTERM both binaries stop serving gracefully and exit successfully.
#[tokio::test]
async fn the_binary_stops_on_sigterm_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("sigterm");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let redis = Redis::start().await;
    for binary in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_ratelimit")),
    ] {
        let mut service = Service::start(&binary, &binary_environment(&runtime, &redis)).await;
        let mut client = service.client().await;
        client
            .should_rate_limit(request("llm", &[&[("user", "a")]]))
            .await
            .unwrap();
        let pid = service._process.id().unwrap();
        let killed = std::process::Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status()
            .unwrap();
        assert!(killed.success());
        let status =
            tokio::time::timeout(std::time::Duration::from_secs(30), service._process.wait())
                .await
                .expect("the service did not stop")
                .unwrap();
        assert!(status.success(), "{}: {status}", binary.display());
    }
}

type HealthClient = ratelimit_proto::grpc::health::v1::health_client::HealthClient<Channel>;

/// Both binaries started alike, ready to serve.
async fn both_services(
    environment: impl Fn(&Redis) -> Vec<(&'static str, String)>,
) -> (Vec<Service>, Vec<Redis>) {
    let redis = vec![Redis::start().await, Redis::start().await];
    let mut services = Vec::new();
    for (binary, redis) in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_ratelimit")),
    ]
    .iter()
    .zip(&redis)
    {
        let service = Service::start(binary, &environment(redis)).await;
        service.client().await;
        services.push(service);
    }
    (services, redis)
}

/// `/healthcheck` and gorilla/mux's routing around it answer like upstream's.
#[tokio::test]
async fn the_healthcheck_answers_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("health");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let (services, _redis) = both_services(|redis| binary_environment(&runtime, redis)).await;
    for (method, target) in [
        ("GET", "/healthcheck"),
        ("POST", "/healthcheck"),
        ("HEAD", "/healthcheck"),
        ("GET", "/healthcheck/"),
        ("GET", "//healthcheck"),
        ("GET", "/x/../healthcheck?y=1"),
        ("GET", "/health%63heck"),
        ("GET", "/other"),
        ("GET", "/"),
        ("GET", "/a%20b/../c%2Fd"),
    ] {
        let upstream = http_request(&services[0].http, method, target, &[]).await;
        let ours = http_request(&services[1].http, method, target, &[]).await;
        assert_eq!(ours, upstream, "{method} {target}");
    }
}

/// The gRPC health service answers like grpc-go's: "" and "ratelimit" serving, other names
/// unknown; with HEALTHY_WITH_AT_LEAST_ONE_CONFIG_LOADED, a config without domains is
/// unhealthy.
#[tokio::test]
async fn health_is_reported_like_upstream() {
    if !tools_available() {
        return;
    }
    use ratelimit_proto::grpc::health::v1::{HealthCheckRequest, HealthListRequest};
    let check = |service: &str| HealthCheckRequest {
        service: service.into(),
    };
    let summary = |r: Result<
        tonic::Response<ratelimit_proto::grpc::health::v1::HealthCheckResponse>,
        tonic::Status,
    >| {
        r.map(|r| r.into_inner().status)
            .map_err(|s| format!("{:?}: {}", s.code(), s.message()))
    };
    for empty in [false, true] {
        let runtime = temp_dir("health");
        let files: &[(&str, &[u8])] = match empty {
            false => &[
                ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
                ("current", b"->v1"),
            ],
            true => &[("v1/ratelimit/config", b"/"), ("current", b"->v1")],
        };
        write_tree(&runtime, files);
        let (services, _redis) = both_services(|redis| {
            let mut environment = binary_environment(&runtime, redis);
            environment.push(("HEALTHY_WITH_AT_LEAST_ONE_CONFIG_LOADED", "true".into()));
            environment
        })
        .await;
        let mut answers = Vec::new();
        for service in &services {
            let mut client = HealthClient::connect(service.grpc.clone()).await.unwrap();
            let mut statuses = Vec::new();
            for name in ["", "ratelimit", "other"] {
                statuses.push(summary(client.check(check(name)).await));
            }
            let mut listed: Vec<(String, i32)> = client
                .list(HealthListRequest {})
                .await
                .unwrap()
                .into_inner()
                .statuses
                .into_iter()
                .map(|(k, v)| (k, v.status))
                .collect();
            listed.sort();
            let http = http_request(&service.http, "GET", "/healthcheck", &[]).await;
            answers.push((statuses, listed, http.status, http.body));
        }
        assert_eq!(answers[1], answers[0], "empty config: {empty}");
    }
}

/// On SIGTERM the service turns unhealthy at once (an open Watch sees it, /healthcheck
/// answers 500) and stops once the Watch ends.
#[tokio::test]
async fn sigterm_turns_health_off_like_upstream() {
    if !tools_available() {
        return;
    }
    use ratelimit_proto::grpc::health::v1::HealthCheckRequest;
    let runtime = temp_dir("health");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let (mut services, _redis) = both_services(|redis| binary_environment(&runtime, redis)).await;
    let mut seen = Vec::new();
    for service in &mut services {
        let mut client = HealthClient::connect(service.grpc.clone()).await.unwrap();
        let request = HealthCheckRequest {
            service: "ratelimit".into(),
        };
        let mut watch = client.watch(request).await.unwrap().into_inner();
        let first = tokio::time::timeout(std::time::Duration::from_secs(10), watch.message())
            .await
            .expect("no initial health status")
            .unwrap()
            .unwrap()
            .status;
        let pid = service._process.id().unwrap().to_string();
        assert!(
            std::process::Command::new("kill")
                .args(["-TERM", &pid])
                .status()
                .unwrap()
                .success()
        );
        let second = tokio::time::timeout(std::time::Duration::from_secs(10), watch.message())
            .await
            .expect("no health change after SIGTERM")
            .unwrap()
            .unwrap()
            .status;
        let http = http_request(&service.http, "GET", "/healthcheck", &[])
            .await
            .status;
        drop(watch);
        drop(client);
        let status =
            tokio::time::timeout(std::time::Duration::from_secs(30), service._process.wait())
                .await
                .expect("the service did not stop")
                .unwrap();
        seen.push((first, second, http, status.success()));
    }
    assert_eq!(seen[1], seen[0]);
}

/// With REDIS_HEALTH_CHECK_ACTIVE_CONNECTION, losing Redis makes the service unhealthy;
/// without it, health does not follow Redis.
#[tokio::test]
async fn redis_connections_drive_health_like_upstream() {
    if !tools_available() {
        return;
    }
    for active in [true, false] {
        let runtime = temp_dir("health");
        write_tree(
            &runtime,
            &[
                ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
                ("current", b"->v1"),
            ],
        );
        let (services, redis) = both_services(|redis| {
            let mut environment = binary_environment(&runtime, redis);
            environment.push(("REDIS_HEALTH_CHECK_ACTIVE_CONNECTION", active.to_string()));
            environment
        })
        .await;
        for redis in &redis {
            let _: Result<(), _> = redis
                .client
                .custom(
                    CustomCommand::new_static("SHUTDOWN", ClusterHash::Random, false),
                    vec!["NOSAVE"],
                )
                .await;
        }
        let expected = if active { 500 } else { 200 };
        if !active {
            // Give a wrong implementation time to turn unhealthy before checking it did not.
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
        for service in &services {
            let mut status = 0;
            for _ in 0..100 {
                status = http_request(&service.http, "GET", "/healthcheck", &[])
                    .await
                    .status;
                if status == expected {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            assert_eq!(status, expected, "{} with active={active}", service.grpc);
        }
        if !active {
            continue;
        }
        // Redis back on the same port: healthy again once the pool reconnects.
        let mut restarted = Vec::new();
        for redis in &redis {
            let port = redis.address.rsplit_once(':').unwrap().1.parse().unwrap();
            restarted.push(Redis::start_on(port).await.expect("Redis restarted"));
        }
        let mut recovered = Vec::new();
        for service in &services {
            let mut status = 0;
            for _ in 0..150 {
                status = http_request(&service.http, "GET", "/healthcheck", &[])
                    .await
                    .status;
                if status == 200 {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            recovered.push(status);
        }
        assert_eq!(recovered, [200, 200]);
    }
}

/// Requests Go's HTTP server answers before routing (a bad escape, a missing or malformed
/// Host, `OPTIONS *`) and the router's redirects for absolute-form targets and non-UTF-8
/// paths are answered like upstream: the status line's code and reason, the headers but the
/// date, and the body.
#[tokio::test]
async fn the_http_server_reads_requests_like_upstream() {
    if !tools_available() {
        return;
    }
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let runtime = temp_dir("http");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let (services, _redis) = both_services(|redis| binary_environment(&runtime, redis)).await;
    for request in [
        "GET /%zz HTTP/1.0\r\n\r\n",
        "GET /a/../%zz HTTP/1.0\r\n\r\n",
        "GET /healthcheck HTTP/1.1\r\nConnection: close\r\n\r\n",
        "GET /healthcheck HTTP/1.1\r\nHost: a\"b\r\nConnection: close\r\n\r\n",
        "GET /healthcheck HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n",
        "OPTIONS * HTTP/1.0\r\n\r\n",
        "OPTIONS * HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n",
        "GET http://h:1/a/../healthcheck?q HTTP/1.0\r\n\r\n",
        "GET http://h:1/healthcheck HTTP/1.0\r\n\r\n",
        "GET /a/../%ff HTTP/1.0\r\n\r\n",
        "HEAD /healthcheck HTTP/1.0\r\n\r\n",
        "GET /nothing HTTP/1.0\r\n\r\n",
    ] {
        let mut answers = Vec::new();
        for service in &services {
            let mut stream = tokio::net::TcpStream::connect(&service.http).await.unwrap();
            stream.write_all(request.as_bytes()).await.unwrap();
            let mut response = Vec::new();
            tokio::time::timeout(
                std::time::Duration::from_secs(10),
                stream.read_to_end(&mut response),
            )
            .await
            .expect("the connection is closed")
            .unwrap();
            let response = String::from_utf8_lossy(&response).into_owned();
            let (head, body) = response.split_once("\r\n\r\n").expect("an HTTP answer");
            let mut lines = head.lines();
            // The code and reason; the version follows the request's in hyper.
            let status = lines.next().unwrap()[9..].to_string();
            // Go ends its own 400s by closing the connection; hyper sends a length too.
            let refused = status.starts_with("400");
            let mut headers: Vec<String> = lines
                .map(str::to_ascii_lowercase)
                .filter(|line| !line.starts_with("date:"))
                .filter(|line| !(refused && line.starts_with("content-length:")))
                .collect();
            headers.sort();
            answers.push((status, headers, body.to_string()));
        }
        assert_eq!(answers[1], answers[0], "{request:?}");
    }
}

/// `/json`'s decoding (`protojson.Unmarshal` into a `RateLimitRequest`) accepts and rejects
/// what upstream's does, into the same request: JSON syntax, field names, `null`, numbers,
/// enums, strings and their escapes.
#[test]
fn json_requests_are_decoded_like_upstream() {
    if !tools_available() {
        return;
    }
    let mut inputs: Vec<Vec<u8>> = [
        // Syntax.
        "",
        " ",
        "{}",
        " {} ",
        "{}\n",
        "{} {}",
        "{},",
        "{}x",
        "[]",
        "null",
        "{",
        "}",
        "{\"domain\":\"d\",}",
        "{,}",
        "{\"domain\" \"d\"}",
        "{\"domain\":}",
        "{\"domain\":\"a\" \"descriptors\":[]}",
        "\t{\r\n\"domain\"\t:\n\"d\"\r}",
        "{\"domain\":\"d\"}\u{0}",
        "\u{feff}{}",
        "{}\u{b}",
        // Field names: either form, once; no unknown or extension names.
        "{\"hitsAddend\":1}",
        "{\"hits_addend\":1}",
        "{\"hitsAddend\":1,\"hits_addend\":2}",
        "{\"domain\":\"a\",\"domain\":\"b\"}",
        "{\"Domain\":\"a\"}",
        "{\"other\":1}",
        "{\"[x.y]\":1}",
        "{\"hitsaddend\":1}",
        "{\"dom\\u0061in\":\"a\"}",
        // null: unset, but the field counts as seen.
        "{\"domain\":null}",
        "{\"domain\":null,\"domain\":\"a\"}",
        "{\"descriptors\":null}",
        "{\"descriptors\":[null]}",
        "{\"descriptors\":[{\"limit\":null,\"hitsAddend\":null}]}",
        "{\"descriptors\":[{\"entries\":[null]}]}",
        "{\"hitsAddend\":null}",
        // Lists and messages.
        "{\"descriptors\":[]}",
        "{\"descriptors\":[[]]}",
        "{\"descriptors\":{}}",
        "{\"descriptors\":[{},{}]}",
        "{\"descriptors\":[{\"entries\":[{\"key\":\"k\",\"value\":\"v\"},{\"key\":\"k2\"}]}]}",
        "{\"descriptors\":[{\"entries\":[{\"key\":1}]}]}",
        "{\"descriptors\":[{\"entries\":[{\"key\":\"k\",\"x\":1}]}]}",
        "{\"descriptors\":[{\"limit\":{\"requestsPerUnit\":5,\"unit\":\"SECOND\"}}]}",
        "{\"descriptors\":[{\"limit\":{\"requests_per_unit\":\"5\",\"unit\":2}}]}",
        "{\"descriptors\":[{\"limit\":{\"unit\":\"second\"}}]}",
        "{\"descriptors\":[{\"limit\":{\"unit\":\"WEEK\"}}]}",
        "{\"descriptors\":[{\"limit\":{\"unit\":99}}]}",
        "{\"descriptors\":[{\"limit\":{\"unit\":-1}}]}",
        "{\"descriptors\":[{\"limit\":{\"unit\":\"2\"}}]}",
        "{\"descriptors\":[{\"limit\":{\"unit\":2.0}}]}",
        "{\"descriptors\":[{\"limit\":{\"unit\":2147483648}}]}",
        "{\"descriptors\":[{\"limit\":{\"unit\":-2147483648}}]}",
        "{\"descriptors\":[{\"limit\":{\"unit\":true}}]}",
        "{\"descriptors\":[{\"limit\":[]}]}",
        "{\"descriptors\":[{\"isNegativeHits\":true}]}",
        "{\"descriptors\":[{\"is_negative_hits\":false}]}",
        "{\"descriptors\":[{\"isNegativeHits\":\"true\"}]}",
        "{\"descriptors\":[{\"isNegativeHits\":1}]}",
        "{\"descriptors\":[{\"hitsAddend\":\"18446744073709551615\"}]}",
        "{\"descriptors\":[{\"hitsAddend\":18446744073709551616}]}",
        "{\"descriptors\":[{\"hitsAddend\":0}]}",
        "{\"descriptors\":[{\"hitsAddend\":{\"value\":1}}]}",
        // Strings: escapes, surrogates, UTF-8, control characters.
        "{\"domain\":\"a\\\"b\\\\c\\/d\\b\\f\\n\\r\\t\"}",
        "{\"domain\":\"\\u00e9\\u0000\"}",
        "{\"domain\":\"\\u00E9\"}",
        "{\"domain\":\"\\ud83d\\ude00\"}",
        "{\"domain\":\"\\ud83d\"}",
        "{\"domain\":\"\\ude00\\ud83d\"}",
        "{\"domain\":\"\\ud83d\\u0041\"}",
        "{\"domain\":\"\\ud83dx\\ude00\"}",
        "{\"domain\":\"\\u12\"}",
        "{\"domain\":\"\\u12g4\"}",
        "{\"domain\":\"\\x\"}",
        "{\"domain\":\"\\'\"}",
        "{\"domain\":\"a\tb\"}",
        "{\"domain\":\"a\u{7f}b\"}",
        "{\"domain\":\"é😀\u{fffd}\"}",
        "{\"domain\":\"a",
        "{\"domain\":\"a\\",
        "{\"domain\":\"\\ud83d\\ude0",
        // Literals.
        "{\"descriptors\":[{\"isNegativeHits\":truex}]}",
        "{\"descriptors\":[{\"isNegativeHits\":tru}]}",
        "{\"domain\":nul}",
        "{\"domain\":nullx}",
        "{\"domain\":NULL}",
    ]
    .iter()
    .map(|s| s.as_bytes().to_vec())
    .collect();
    inputs.extend([
        b"{\"domain\":\"\xff\"}".to_vec(),
        b"{\"domain\":\"\xc3\"}".to_vec(),
        b"{\"domain\":\"\xed\xa0\x80\"}".to_vec(),
        b"{\"domain\":\"\xc0\xaf\"}".to_vec(),
        b"{\"\xff\":1}".to_vec(),
    ]);
    // Numbers, bare and in strings, in each kind of numeric field.
    let numbers = [
        "0",
        "-0",
        "1",
        "01",
        "-1",
        "1.0",
        "1.5",
        "1.",
        ".5",
        "1e2",
        "1E2",
        "1e+2",
        "1e-2",
        "10e-1",
        "100e-2",
        "1e",
        "1e+",
        "1e-",
        "1ex",
        "1.0e1",
        "0.5e1",
        "0.05e1",
        "5e-1",
        "0e5",
        "-0.0",
        "1e010",
        "1e08",
        "4294967295",
        "4294967296",
        "18446744073709551615",
        "18446744073709551616",
        "1e19",
        "1e20",
        "2e19",
        "1e99999999999",
        "15e-1",
        "-15e-1",
        "100e-1",
        "+1",
        "0x10",
        "1_000",
        "Infinity",
        "NaN",
        "-",
    ];
    for number in numbers {
        for template in [
            "{\"hitsAddend\":N}",
            "{\"hitsAddend\":\"N\"}",
            "{\"descriptors\":[{\"hitsAddend\":N}]}",
            "{\"descriptors\":[{\"hitsAddend\":\"N\"}]}",
            "{\"descriptors\":[{\"limit\":{\"unit\":N}}]}",
        ] {
            inputs.push(template.replace('N', number).into_bytes());
        }
    }
    // A number then a delimiter: the tokenizer takes `1e`, `1e+` as numbers.
    for tail in ["1e,", "1e]", "1e}", "1e ", "1e+,", "1e-}", "1.5e,"] {
        inputs.push(format!("{{\"hitsAddend\":{tail}\"domain\":\"d\"}}").into_bytes());
        inputs.push(
            format!("{{\"descriptors\":[{{\"hitsAddend\":{tail}\"entries\":[]}}]}}").into_bytes(),
        );
    }
    // Strings holding numbers: no space around, only the first token counts.
    for text in [
        "\\u0031",
        "\\u00312",
        "\\u0020\\u0031",
        " 1",
        "1 ",
        "1 2",
        "1,2",
        "1}",
        "1]",
        "1\u{b}2",
        "1\u{a0}",
        "\u{a0}1",
        "1\u{2028}",
        "\"1\"",
        "[1]",
        "",
        "1e,",
    ] {
        let text = text.replace('"', "\\\"");
        inputs.push(format!("{{\"hitsAddend\":\"{text}\"}}").into_bytes());
    }

    // Generated: fragments of the schema and of JSON put together at random (a fixed seed).
    let fragments = [
        "{",
        "}",
        "[",
        "]",
        ",",
        ":",
        " ",
        "\"domain\"",
        "\"descriptors\"",
        "\"entries\"",
        "\"key\"",
        "\"value\"",
        "\"limit\"",
        "\"unit\"",
        "\"requestsPerUnit\"",
        "\"hitsAddend\"",
        "\"hits_addend\"",
        "\"isNegativeHits\"",
        "\"SECOND\"",
        "\"d\"",
        "\"\\u00e9\"",
        "null",
        "true",
        "false",
        "0",
        "1",
        "-1",
        "1e2",
        "1.5",
        "\"7\"",
        "4294967296",
        "{}",
        "[]",
    ];
    let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
    let mut next = |bound: usize| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        (seed % bound as u64) as usize
    };
    for _ in 0..3000 {
        let mut input = String::from("{\"descriptors\":[{");
        for _ in 0..1 + next(12) {
            input += fragments[next(fragments.len())];
        }
        if next(2) == 0 {
            input += "}]}";
        }
        inputs.push(input.into_bytes());
    }
    // Generated objects: each field under either name (or an unknown one), any kind of value.
    let values = [
        "null",
        "true",
        "false",
        "0",
        "-0",
        "7",
        "1e1",
        "1.0",
        "4294967295",
        "4294967296",
        "\"3\"",
        "\" 3\"",
        "\"SECOND\"",
        "\"DAY\"",
        "\"x\"",
        "\"\\ud83d\\ude00\"",
        "{}",
        "[]",
        "[{}]",
        "{\"key\":\"k\"}",
        "[{\"key\":\"k\",\"value\":\"v\"}]",
        "18446744073709551615",
    ];
    let names = [
        "domain",
        "descriptors",
        "hitsAddend",
        "hits_addend",
        "entries",
        "limit",
        "isNegativeHits",
        "is_negative_hits",
        "requestsPerUnit",
        "requests_per_unit",
        "unit",
        "key",
        "value",
        "other",
    ];
    let object = |next: &mut dyn FnMut(usize) -> usize| {
        let fields: Vec<String> = (0..next(4))
            .map(|_| {
                format!(
                    "\"{}\":{}",
                    names[next(names.len())],
                    values[next(values.len())]
                )
            })
            .collect();
        format!("{{{}}}", fields.join(","))
    };
    for _ in 0..3000 {
        let descriptors: Vec<String> = (0..next(3)).map(|_| object(&mut next)).collect();
        let limit = object(&mut next);
        let input = match next(3) {
            0 => object(&mut next),
            1 => format!("{{\"descriptors\":[{}]}}", descriptors.join(",")),
            _ => format!("{{\"domain\":\"d\",\"descriptors\":[{{\"limit\":{limit}}}]}}"),
        };
        inputs.push(input.into_bytes());
    }

    let mut oracle = std::process::Command::new(oracle_binary())
        .arg("-protojson")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("start the oracle");
    let lines: String = inputs
        .iter()
        .map(|input| format!("{}\n", hex(input)))
        .collect();
    // Written while the answers are read, so that neither pipe fills up.
    let mut stdin = oracle.stdin.take().unwrap();
    let writer =
        std::thread::spawn(move || std::io::Write::write_all(&mut stdin, lines.as_bytes()));
    let output = oracle.wait_with_output().unwrap();
    writer.join().unwrap().unwrap();
    let upstream: Vec<String> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(String::from)
        .collect();
    assert_eq!(upstream.len(), inputs.len());
    let accepted = upstream
        .iter()
        .filter(|line| line.starts_with("ok"))
        .count();
    assert!(accepted > 1000, "only {accepted} inputs accepted");
    for (input, upstream) in inputs.iter().zip(&upstream) {
        let ours = match ratelimit::protojson::decode_request(input) {
            Ok(request) => format!("ok {}", hex(&prost::Message::encode_to_vec(&request))),
            Err(_) => "error".to_string(),
        };
        assert_eq!(&ours, upstream, "{:?}", String::from_utf8_lossy(input));
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// `/json` answers like upstream's: statuses (200, 429, 400, 500), headers and the protojson
/// body, with metadata, response headers and every method. Go adds a space after every comma
/// or after none, depending on its build: those spaces are left out of the comparison.
#[tokio::test]
async fn json_is_served_like_upstream() {
    if !tools_available() {
        return;
    }
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let runtime = temp_dir("json");
    let metadata = "domain: meta
descriptors:
  - key: a
    rate_limit: {unit: hour, requests_per_unit: 10, name: named}
    metadata: {s: \"q\\\"b\\\\<>&\\u2028\\x01\\t\", n: 3, f: 1.5, tiny: 0.0000001, big: 1.0e+21, neg: -0.0, l: [1, two, {x: null}], t: true}
  - key: nan
    rate_limit: {unit: hour, requests_per_unit: 10}
    metadata: {bad: .nan}
";
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("v1/ratelimit/config/meta.yaml", metadata.as_bytes()),
            ("current", b"->v1"),
        ],
    );
    let (services, _redis) = both_services(|redis| {
        let mut environment = binary_environment(&runtime, redis);
        environment.push(("RESPONSE_DYNAMIC_METADATA", "true".into()));
        environment.push(("LIMIT_RESPONSE_HEADERS_ENABLED", "true".into()));
        environment
    })
    .await;
    let send = |address: String, method: &'static str, target: &'static str, body: String| async move {
        let mut stream = tokio::net::TcpStream::connect(&address).await.unwrap();
        let request = format!(
            "{method} {target} HTTP/1.0\r\nHost: h\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = Vec::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            stream.read_to_end(&mut response),
        )
        .await
        .expect("the connection is closed")
        .unwrap();
        String::from_utf8(response).unwrap()
    };
    // The status, the headers but the date and framing, and the body without Go's spaces.
    let simplify = |response: &str| -> (String, Vec<String>, String) {
        let (head, body) = response.split_once("\r\n\r\n").expect("an HTTP answer");
        let mut lines = head.lines();
        let status = lines.next().unwrap()[9..].to_string();
        let mut headers: Vec<String> = lines
            .map(str::to_ascii_lowercase)
            .filter(|l| !l.starts_with("date:") && !l.starts_with("content-length:"))
            .collect();
        headers.sort();
        let text = without_go_spaces(body);
        (status, headers, text)
    };
    let user = r#"{"domain":"llm","descriptors":[{"entries":[{"key":"user","value":"a"}]}]}"#;
    let cases = [
        ("POST", "/json", user.to_string()),
        ("POST", "/json", user.to_string()),
        ("POST", "/json", user.to_string()),
        ("GET", "/json", r#"{"domain":"llm","descriptors":[{"entries":[{"key":"user","value":"b"}]}],"hitsAddend":2}"#.into()),
        ("PUT", "/json", r#"{"domain":"meta","descriptors":[{"entries":[{"key":"a"}]}]}"#.into()),
        ("POST", "/json", r#"{"domain":"meta","descriptors":[{"entries":[{"key":"nan"}]}]}"#.into()),
        ("POST", "/json", r#"{"domain":"unknown","descriptors":[{"entries":[{"key":"k"}]}]}"#.into()),
        ("POST", "/json", r#"{"domain":"llm","descriptors":[{"entries":[{"key":"user","value":"c"}],"limit":{"requestsPerUnit":5,"unit":"HOUR"}}]}"#.into()),
        ("POST", "/json", r#"{"domain":"llm","descriptors":[{"entries":[{"key":"user","value":"d"}],"hitsAddend":0}]}"#.into()),
        ("POST", "/json", "{}".into()),
        ("POST", "/json", r#"{"domain":"llm"}"#.into()),
        ("POST", "/json", "not json".into()),
        ("POST", "/json", String::new()),
        ("DELETE", "/json?x=1", user.to_string()),
        (
            "HEAD",
            "/json",
            r#"{"domain":"llm","descriptors":[{"entries":[{"key":"user","value":"h"}]}]}"#.into(),
        ),
        ("POST", "/json/", user.to_string()),
    ];
    let reset = regex::Regex::new(r#""durationUntilReset":"(\d+)s""#).unwrap();
    for (method, target, body) in cases {
        let upstream = send(services[0].http.clone(), method, target, body.clone()).await;
        let ours = send(services[1].http.clone(), method, target, body.clone()).await;
        let (mut upstream, mut ours) = (simplify(&upstream), simplify(&ours));
        // The metadata's prototext limits: one or two spaces between fields in Go (§7).
        upstream.2 = upstream.2.replace("  unit:", " unit:");
        // Each side reads its own clock: the time to reset may differ by a second.
        let resets = |text: &str| -> Vec<i64> {
            reset
                .captures_iter(text)
                .map(|c| c[1].parse().unwrap())
                .collect()
        };
        let (expected, actual) = (resets(&upstream.2), resets(&ours.2));
        assert_eq!(expected.len(), actual.len(), "{method} {target} {body}");
        for (e, a) in expected.iter().zip(&actual) {
            assert!((e - a).abs() <= 1, "{e} {a}");
        }
        let mut times = expected.iter();
        ours.2 = reset
            .replace_all(&ours.2, |_: &regex::Captures| {
                format!("\"durationUntilReset\":\"{}s\"", times.next().unwrap())
            })
            .into_owned();
        assert_eq!(ours, upstream, "{method} {target} {body}");
    }
    // A client limit with an unknown unit makes upstream panic, which closes the connection
    // with no answer; it is a bad request here. `unitMultiplier` is unknown to upstream.
    for unit in ["", r#","unit":-1"#, r#","unit":8"#] {
        let request = format!(
            r#"{{"domain":"llm","descriptors":[{{"entries":[{{"key":"user","value":"e"}}],"limit":{{"requestsPerUnit":5{unit}}}}}]}}"#
        );
        let upstream = send(services[0].http.clone(), "POST", "/json", request.clone()).await;
        assert_eq!(upstream, "", "{request}");
        let ours = send(services[1].http.clone(), "POST", "/json", request.clone()).await;
        assert_eq!(simplify(&ours).0, "400 Bad Request", "{request}");
    }
    let multiplier = r#"{"domain":"llm","descriptors":[{"entries":[{"key":"user","value":"f"}],"limit":{"requestsPerUnit":5,"unit":"HOUR","unitMultiplier":2}}]}"#;
    let upstream = send(services[0].http.clone(), "POST", "/json", multiplier.into()).await;
    assert_eq!(simplify(&upstream).0, "400 Bad Request");
    let ours = simplify(&send(services[1].http.clone(), "POST", "/json", multiplier.into()).await);
    assert_eq!(ours.0, "200 OK");
    assert!(
        ours.2.contains("unit:HOUR unit_multiplier:{value:2}"),
        "{}",
        ours.2
    );
    // Bodies up to gRPC's largest message (4 MiB) are read, larger ones refused.
    let request = r#"{"domain":"d","descriptors":[{"entries":[{"key":"k"}]}]}"#;
    for (size, status) in [(4 << 20, "200 OK"), ((4 << 20) + 1, "400 Bad Request")] {
        let body = format!("{request}{}", " ".repeat(size - request.len()));
        let ours = simplify(&send(services[1].http.clone(), "POST", "/json", body).await);
        assert_eq!(ours.0, status, "{size}");
    }
}

/// `/json`'s encoding writes responses like `protojson.Marshal` (Go's spaces after commas
/// left out), and refuses the same ones: every field, enum numbers with no name, Duration and
/// Timestamp fractions and ranges, base64 padding, numbers in the metadata.
#[test]
fn json_responses_are_encoded_like_upstream() {
    if !tools_available() {
        return;
    }
    use prost_types::{ListValue, Struct, Timestamp, Value, value::Kind};
    use ratelimit_proto::envoy::config::core::v3::HeaderValue;
    use ratelimit_proto::envoy::service::ratelimit::v3::rate_limit_response::{
        DescriptorStatus, Quota, RateLimit, quota::ExpirationSpecifier,
    };
    let number = |n: f64| Value {
        kind: Some(Kind::NumberValue(n)),
    };
    let metadata = |values: Vec<(&str, Value)>| Struct {
        fields: values
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    };
    let with_metadata = |values: Vec<(&str, Value)>| RateLimitResponse {
        dynamic_metadata: Some(metadata(values)),
        ..Default::default()
    };
    let with_reset = |seconds: i64, nanos: i32| RateLimitResponse {
        statuses: vec![DescriptorStatus {
            duration_until_reset: Some(prost_types::Duration { seconds, nanos }),
            ..Default::default()
        }],
        ..Default::default()
    };
    let with_time = |seconds: i64, nanos: i32| RateLimitResponse {
        quota: Some(Quota {
            expiration_specifier: Some(ExpirationSpecifier::ValidUntil(Timestamp {
                seconds,
                nanos,
            })),
            ..Default::default()
        }),
        ..Default::default()
    };
    let header = |raw: &[u8]| HeaderValue {
        key: "k".into(),
        value: String::new(),
        raw_value: raw.to_vec(),
    };
    let mut responses = vec![
        RateLimitResponse::default(),
        RateLimitResponse {
            overall_code: 1,
            statuses: vec![
                DescriptorStatus {
                    code: 2,
                    current_limit: Some(RateLimit {
                        name: "n\"\\\u{1}\u{1f}\u{7f}é\u{2028}<>&".into(),
                        requests_per_unit: 5,
                        unit: 7,
                        unit_multiplier: None,
                    }),
                    limit_remaining: 3,
                    duration_until_reset: Some(prost_types::Duration {
                        seconds: 5,
                        nanos: 0,
                    }),
                    quota: Some(Quota {
                        requests: 2,
                        expiration_specifier: None,
                        id: "q".into(),
                    }),
                },
                DescriptorStatus::default(),
                DescriptorStatus {
                    current_limit: Some(RateLimit::default()),
                    ..Default::default()
                },
            ],
            response_headers_to_add: vec![
                header(&[]),
                header(&[1]),
                header(&[1, 2]),
                header(&[0, 1, 2, 255]),
            ],
            request_headers_to_add: vec![HeaderValue {
                key: String::new(),
                value: "v".into(),
                raw_value: vec![],
            }],
            raw_body: b"body\xff".to_vec(),
            dynamic_metadata: Some(Struct::default()),
            quota: Some(Quota::default()),
        },
        // Enum numbers with no name.
        RateLimitResponse {
            overall_code: 9,
            statuses: vec![DescriptorStatus {
                code: -3,
                current_limit: Some(RateLimit {
                    unit: 42,
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        },
        with_metadata(vec![
            (
                "null",
                Value {
                    kind: Some(Kind::NullValue(0)),
                },
            ),
            (
                "b",
                Value {
                    kind: Some(Kind::BoolValue(false)),
                },
            ),
            (
                "s",
                Value {
                    kind: Some(Kind::StringValue("x\ty".into())),
                },
            ),
            ("é", number(1.0)),
            ("B", number(-0.0)),
            (
                "",
                Value {
                    kind: Some(Kind::ListValue(ListValue { values: vec![] })),
                },
            ),
            (
                "nested",
                Value {
                    kind: Some(Kind::StructValue(metadata(vec![(
                        "l",
                        Value {
                            kind: Some(Kind::ListValue(ListValue {
                                values: vec![
                                    number(2.5),
                                    Value {
                                        kind: Some(Kind::StructValue(Struct::default())),
                                    },
                                ],
                            })),
                        },
                    )]))),
                },
            ),
        ]),
    ];
    for n in [
        1e-10,
        1.5e-100,
        1e100,
        1e21,
        1e20,
        999999999999999999999.0,
        123456789.123,
        -2.5e-7,
        1e-6,
        9.99e-7,
        5e-324,
        f64::MAX,
        f64::MIN_POSITIVE,
        0.1,
        1.0 / 3.0,
        1e15,
        1e16,
        1e-5,
        123e300,
    ] {
        responses.push(with_metadata(vec![("n", number(n))]));
    }
    for (seconds, nanos) in [
        (0, 0),
        (1, 500_000_000),
        (0, 1),
        (0, 1_000),
        (0, 1_000_000),
        (0, -1_000),
        (-1, -500_000_000),
        (315_576_000_000, 999_999_999),
        (-315_576_000_000, -999_999_999),
        (315_576_000_001, 0),
        (0, 1_000_000_000),
        (0, -1_000_000_000),
        (1, -1),
        (-1, 1),
    ] {
        responses.push(with_reset(seconds, nanos));
    }
    for (seconds, nanos) in [
        (0, 0),
        (-1, 0),
        (951_782_400, 0),
        (1_700_000_000, 120_000_000),
        (253_402_300_799, 999_999_999),
        (-62_135_596_800, 0),
        (-62_135_596_801, 0),
        (253_402_300_800, 0),
        (0, -1),
        (0, 1_000_000_000),
        (68_256_000, 7),
        (-2_208_988_800, 0),
    ] {
        responses.push(with_time(seconds, nanos));
    }
    // What protojson refuses to write.
    responses.push(with_metadata(vec![("unset", Value { kind: None })]));
    for n in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        responses.push(with_metadata(vec![("n", number(n))]));
    }

    let mut oracle = std::process::Command::new(oracle_binary())
        .arg("-protojson-marshal")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("start the oracle");
    let lines: String = responses
        .iter()
        .map(|response| format!("{}\n", hex(&prost::Message::encode_to_vec(response))))
        .collect();
    let mut stdin = oracle.stdin.take().unwrap();
    let writer =
        std::thread::spawn(move || std::io::Write::write_all(&mut stdin, lines.as_bytes()));
    let output = oracle.wait_with_output().unwrap();
    writer.join().unwrap().unwrap();
    let upstream: Vec<String> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(without_go_spaces)
        .collect();
    assert_eq!(upstream.len(), responses.len());
    for (response, upstream) in responses.iter().zip(&upstream) {
        let ours = match ratelimit::protojson::encode_response(response) {
            Ok(json) => format!("ok {json}"),
            Err(_) => "error".to_string(),
        };
        assert_eq!(&ours, upstream, "{response:?}");
    }
}

/// `text` without the spaces Go's protojson may put after commas, outside strings.
fn without_go_spaces(text: &str) -> String {
    let (mut out, mut in_string, mut escaped, mut after_comma) =
        (String::new(), false, false, false);
    for c in text.chars() {
        if !(after_comma && c == ' ') {
            out.push(c);
        }
        after_comma = !in_string && c == ',';
        match c {
            _ if escaped => escaped = false,
            '\\' if in_string => escaped = true,
            '"' => in_string = !in_string,
            _ => {}
        }
    }
    out
}

/// Sends `request` as is to `address`; returns the answer once the connection closes.
async fn send_raw(address: &str, request: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        stream.read_to_end(&mut response),
    )
    .await
    .expect("the connection is closed")
    .unwrap();
    String::from_utf8_lossy(&response).into_owned()
}

/// An answer's status line (but its version), headers (but the date) and body.
fn answer_parts(response: &str) -> (String, Vec<String>, String) {
    let (head, body) = response.split_once("\r\n\r\n").expect("an HTTP answer");
    let mut lines = head.lines();
    let status = lines.next().unwrap()[9..].to_string();
    // Go ends its own 400s by closing the connection; hyper sends a length too (§7).
    let refused = status.starts_with("400");
    let mut headers: Vec<String> = lines
        .map(str::to_ascii_lowercase)
        .filter(|l| !l.starts_with("date:"))
        .filter(|l| !(refused && l.starts_with("content-length:")))
        .collect();
    headers.sort();
    (status, headers, body.to_string())
}

/// The debug port answers like upstream's: the endpoints' list for any path `/` catches,
/// Go's mux redirects, `/rlconfig` (whose order is random upstream: compared line by line),
/// `/stats`' `cmdline`, and pprof's answer for profiles it does not have.
#[tokio::test]
async fn the_debug_port_answers_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("debug");
    let dump = "domain: dump
descriptors:
  - key: a
    rate_limit: {unit: week, requests_per_unit: 3}
    descriptors:
      - key: b
        value: v
        shadow_mode: true
        rate_limit: {unit: second, requests_per_unit: 1}
      - key: c
        rate_limit: {unlimited: true, requests_per_unit: 7}
  - key: w
    value: pre*
    rate_limit: {unit: day, requests_per_unit: 9}
  - key: q
    quota_mode: true
    rate_limit: {unit: month, requests_per_unit: 2}
  - key: nolimit
    descriptors:
      - key: deep
        rate_limit: {unit: year, requests_per_unit: 4}
";
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("v1/ratelimit/config/dump.yaml", dump.as_bytes()),
            ("current", b"->v1"),
        ],
    );
    let (services, _redis) = both_services(|redis| binary_environment(&runtime, redis)).await;
    for (method, target) in [
        ("GET", "/"),
        ("HEAD", "/"),
        ("POST", "/"),
        ("GET", "/foo"),
        ("GET", "/stats/"),
        ("GET", "/debug"),
        ("GET", "/debug/pprof"),
        ("GET", "/debug/pprof?x=1"),
        ("HEAD", "/debug/pprof"),
        ("POST", "/debug/pprof"),
        ("GET", "/debug/ppro%66"),
        ("GET", "/a/../rlconfig"),
        ("GET", "/a/../%2Fx"),
        ("GET", "/a/../b!"),
        ("GET", "//stats"),
        ("GET", "/x/../debug/pprof"),
        ("GET", "/debug/pprof/cmdline"),
        ("GET", "/debug/pprof/symbol"),
        ("GET", "/debug/pprof/profile/"),
        ("GET", "/debug/pprof/x/y"),
        ("GET", "/debug/pprof/trace/"),
        ("GET", "/%zz"),
        ("GET", "/a|b"),
        ("GET", "/x/../a|b"),
    ] {
        let request = format!("{method} {target} HTTP/1.0\r\nHost: h\r\n\r\n");
        let upstream = answer_parts(&send_raw(&services[0].debug, &request).await);
        let ours = answer_parts(&send_raw(&services[1].debug, &request).await);
        assert_eq!(ours, upstream, "{method} {target}");
    }
    for request in [
        "GET * HTTP/1.0\r\n\r\n",
        "HEAD * HTTP/1.1\r\nHost: h\r\n\r\n",
        "GET http://h/a/../stats HTTP/1.0\r\n\r\n",
        "CONNECT h:1 HTTP/1.0\r\n\r\n",
        "CONNECT /debug/pprof HTTP/1.0\r\n\r\n",
        "GET /a/../x?q=\u{e9} HTTP/1.0\r\n\r\n",
        "GET /debug/pprof?\u{e9} HTTP/1.0\r\n\r\n",
        "GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\nConnection: close\r\n\r\n",
    ] {
        let upstream = answer_parts(&send_raw(&services[0].debug, request).await);
        let ours = answer_parts(&send_raw(&services[1].debug, request).await);
        assert_eq!(ours, upstream, "{request:?}");
    }
    // The config, in any order.
    for target in ["/rlconfig", "/rlconfi%67"] {
        let request = format!("GET {target} HTTP/1.0\r\nHost: h\r\n\r\n");
        let lines = |response: &str| {
            let (status, headers, body) = answer_parts(response);
            let mut lines: Vec<String> = body.lines().map(String::from).collect();
            lines.sort();
            (status, headers, lines, body.ends_with('\n'))
        };
        let upstream = lines(&send_raw(&services[0].debug, &request).await);
        let ours = lines(&send_raw(&services[1].debug, &request).await);
        assert_eq!(ours, upstream, "{target}");
        assert!(ours.2.len() > 5, "{ours:?}");
    }
    // `/stats`: the command line as JSON, then upstream's `memstats` only.
    let request = "GET /stats HTTP/1.0\r\nHost: h\r\n\r\n";
    let (upstream_status, upstream_headers, upstream_body) =
        answer_parts(&send_raw(&services[0].debug, request).await);
    let (status, headers, body) = answer_parts(&send_raw(&services[1].debug, request).await);
    assert_eq!(
        (
            &status,
            &headers
                .iter()
                .filter(|h| !h.starts_with("content-length"))
                .collect::<Vec<_>>()
        ),
        (
            &upstream_status,
            &upstream_headers
                .iter()
                .filter(|h| !h.starts_with("content-length"))
                .collect::<Vec<_>>()
        )
    );
    let upstream_lines: Vec<&str> = upstream_body.lines().collect();
    assert_eq!(upstream_lines.len(), 2);
    assert!(
        upstream_lines[1].starts_with("memstats: {"),
        "{}",
        upstream_lines[1]
    );
    let binary = |path: &std::path::Path| {
        format!(
            "cmdline: [{}]",
            serde_json::to_string(&path.display().to_string()).unwrap()
        )
    };
    assert_eq!(upstream_lines[0], binary(&upstream_binary()));
    assert_eq!(
        body,
        format!(
            "{}\n",
            binary(std::path::Path::new(env!("CARGO_BIN_EXE_ratelimit")))
        )
    );
    // pprof cannot profile here: its own answer when profiling does not start.
    for (target, text) in [
        (
            "/debug/pprof/profile",
            "Could not enable CPU profiling: not available",
        ),
        (
            "/debug/pprof/trace",
            "Could not enable tracing: not available",
        ),
    ] {
        let request = format!("GET {target} HTTP/1.0\r\nHost: h\r\n\r\n");
        let (status, _, body) = answer_parts(&send_raw(&services[1].debug, &request).await);
        assert_eq!(
            (status.as_str(), body.as_str()),
            ("500 Internal Server Error", format!("{text}\n").as_str())
        );
    }
}

/// The content type Go's server sniffs for a body (`http.DetectContentType`), and Go's JSON
/// strings (`json.Marshal`, as `/stats` writes the command line), for bytes of any kind.
#[test]
fn bodies_are_sniffed_and_strings_quoted_like_go() {
    if !tools_available() {
        return;
    }
    let mut inputs: Vec<Vec<u8>> = [
        &b""[..],
        b"text",
        b"  \t\n<html>",
        b"<HTML ",
        b"<html",
        b"<htmlx",
        b"<!DOCTYPE html>",
        b"<!doctype HTML ",
        b"<head>",
        b"<script ",
        b"<iframe>",
        b"<h1>",
        b"<div>",
        b"<font>",
        b"<table>",
        b"<a>",
        b"<a",
        b"<style>",
        b"<title>",
        b"<b>",
        b"<body>",
        b"<br>",
        b"<p>",
        b"<!-->",
        b"<!--x",
        b" <?xml",
        b"<?XML",
        b"%PDF-1",
        b"%!PS-Adobe-3",
        b"\xFE\xFF\x00\x00",
        b"\xFF\xFE\x00\x01",
        b"\xEF\xBB\xBFx",
        b"\xEF\xBB\xBF",
        b"\x00\x00\x01\x00",
        b"\x00\x00\x02\x00",
        b"BMx",
        b"GIF87a",
        b"GIF89a!",
        b"RIFF1234WEBPVP8",
        b"\x89PNG\x0D\x0A\x1A\x0A",
        b"\xFF\xD8\xFF\xE0",
        b"FORM1234AIFF",
        b"ID3x",
        b"OggS\x00",
        b"MThd\x00\x00\x00\x06",
        b"RIFF1234AVI ",
        b"RIFF1234WAVE",
        b"\x1A\x45\xDF\xA3",
        b"\x00\x01\x00\x00",
        b"OTTO",
        b"ttcf",
        b"wOFF",
        b"wOF2",
        b"\x1F\x8B\x08",
        b"PK\x03\x04",
        b"Rar!\x1A\x07\x00",
        b"Rar!\x1A\x07\x01\x00",
        b"\x00asm",
        b"a\x00b",
        b"a\x08",
        b"a\x0b",
        b"a\x0c",
        b"a\x1b",
        b"a\x1c",
        b"a\x7f",
        b"\xff\xfe",
        b"\x80",
        b"\x00\x00\x00\x18ftypmp42\x00\x00\x00\x00mp41isom",
        b"\x00\x00\x00\x10ftypisom\x00\x00\x00\x00",
        b"\x00\x00\x00\x14ftypxxxxmp4\x00\x00\x00\x00\x00",
        // JSON strings.
        b"\"\\/",
        b"\x01\x1f\x08\x0c\n\r\t",
        b"<>&",
        "\u{2028}\u{2029}é😀".as_bytes(),
        b"\xe2\x82",
        b"\xe2\x82A",
        b"\xed\xa0\x80",
        b"\xc0\xaf",
        b"\xf4\x90\x80\x80",
        b"a\xffb\xfe",
        "\u{fffd}".as_bytes(),
    ]
    .iter()
    .map(|b| b.to_vec())
    .collect();
    let mut font = vec![0u8; 34];
    font.extend_from_slice(b"LP");
    inputs.push(font);
    let mut webm_font = b"\x1A\x45\xDF\xA3".to_vec();
    webm_font.resize(34, 0);
    webm_font.extend_from_slice(b"LP");
    inputs.push(webm_font);
    let mut long = vec![b'a'; 512];
    long.push(0);
    inputs.push(long);
    let mut late_html = vec![b' '; 600];
    late_html.extend_from_slice(b"<html>");
    inputs.push(late_html);

    let mut oracle = std::process::Command::new(oracle_binary())
        .arg("-sniff")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("start the oracle");
    let lines: String = inputs
        .iter()
        .map(|input| format!("{}\n", hex(input)))
        .collect();
    let mut stdin = oracle.stdin.take().unwrap();
    let writer =
        std::thread::spawn(move || std::io::Write::write_all(&mut stdin, lines.as_bytes()));
    let output = oracle.wait_with_output().unwrap();
    writer.join().unwrap().unwrap();
    let upstream: Vec<String> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(String::from)
        .collect();
    assert_eq!(upstream.len(), inputs.len());
    for (input, upstream) in inputs.iter().zip(&upstream) {
        let ours = format!(
            "{}\t{}",
            ratelimit::goserver::detect_content_type(input),
            ratelimit::go::json_string(input)
        );
        assert_eq!(&ours, upstream, "{input:?}");
    }
}

/// A debug port that cannot be listened on is not fatal: the service serves anyway.
#[tokio::test]
async fn a_busy_debug_port_is_ignored_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("busy-debug");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    // Taken without SO_REUSEPORT, so that neither service can share it.
    let busy = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = busy.local_addr().unwrap().port().to_string();
    let (services, _redis) = both_services(|redis| {
        let mut environment = binary_environment(&runtime, redis);
        environment.push(("DEBUG_PORT", port.clone()));
        environment
    })
    .await;
    for service in &services {
        let status = http_request(&service.http, "GET", "/healthcheck", &[])
            .await
            .status;
        assert_eq!(status, 200, "{}", service.grpc);
    }
}

/// The calls whose server stats are compared: `ShouldRateLimit`, health `Check` (one for a
/// service it does not know) and `List`, which upstream's interceptor counts, and a `Watch`,
/// which it does not (streams are not intercepted).
async fn server_calls(service: &Service) {
    use ratelimit_proto::grpc::health::v1::{HealthCheckRequest, HealthListRequest};
    let mut client = service.client().await;
    for _ in 0..3 {
        client
            .should_rate_limit(request("llm", &[&[("user", "a")]]))
            .await
            .unwrap();
    }
    let mut health = HealthClient::connect(service.grpc.clone()).await.unwrap();
    for name in ["ratelimit", "unknown"] {
        let _ = health
            .check(HealthCheckRequest {
                service: name.into(),
            })
            .await;
    }
    health.list(HealthListRequest {}).await.unwrap();
    let mut watch = health
        .watch(HealthCheckRequest {
            service: "ratelimit".into(),
        })
        .await
        .unwrap()
        .into_inner();
    watch.message().await.unwrap();
}

/// Statsd lines summed by name: counters by value, timers by count once their value (a few
/// milliseconds, read from each side's clock) is checked to be small.
fn server_stat_lines(lines: &[String]) -> BTreeMap<String, u64> {
    let mut sums = BTreeMap::new();
    for line in lines {
        let Some((name, rest)) = line.split_once(':') else {
            continue;
        };
        if !name.starts_with("ratelimit_server.") && name != "reserved_tag" {
            continue;
        }
        let (value, kind) = rest.split_once('|').unwrap();
        let kind = kind.split('|').next().unwrap();
        let value: f64 = value.parse().unwrap();
        let (key, add) = match kind {
            "ms" => {
                assert!((0.0..1000.0).contains(&value), "{line}");
                assert_eq!(value.trunc(), value, "{line}");
                (format!("{name}|ms {}", rest.split_once('|').unwrap().1), 1)
            }
            _ => (
                format!("{name}|{}", rest.split_once('|').unwrap().1),
                value as u64,
            ),
        };
        *sums.entry(key).or_default() += add;
    }
    sums
}

/// The gRPC server's stats (`ratelimit_server.<method>.total_requests` and `response_time`)
/// reach statsd, DogStatsD and Prometheus like upstream's, with `EXTRA_TAGS`.
#[tokio::test]
async fn server_stats_are_reported_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("server-stats");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let stats_environment = |redis: &Redis, sink: Vec<(&'static str, String)>| {
        let mut environment = binary_environment(&runtime, redis);
        environment.extend([
            ("DISABLE_STATS", "false".to_string()),
            ("STATS_FLUSH_INTERVAL", "100ms".into()),
        ]);
        environment.extend(sink);
        environment
    };
    let tagged = ("EXTRA_TAGS", "host:h,k:v".to_string());

    // statsd over TCP.
    let servers = [StatsdServer::start().await, StatsdServer::start().await];
    let mut sums = Vec::new();
    let redis = [Redis::start().await, Redis::start().await];
    for ((binary, redis), server) in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_ratelimit")),
    ]
    .iter()
    .zip(&redis)
    .zip(&servers)
    {
        let sink = vec![
            ("STATSD_HOST", "127.0.0.1".to_string()),
            ("STATSD_PORT", server.address.port().to_string()),
            tagged.clone(),
        ];
        let service = Service::start(binary, &stats_environment(redis, sink)).await;
        server_calls(&service).await;
        sums.push(wait_for_server_stats(|| server.lines.lock().unwrap().clone()).await);
    }
    assert!(
        sums[0]
            .keys()
            .any(|k| k.contains("ShouldRateLimit.response_time")),
        "{:?}",
        sums[0]
    );
    assert_eq!(sums[1], sums[0], "statsd");

    // DogStatsD.
    let servers = [
        DogStatsdServer::start().await,
        DogStatsdServer::start().await,
    ];
    let mut sums = Vec::new();
    for ((binary, redis), server) in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_ratelimit")),
    ]
    .iter()
    .zip(&redis)
    .zip(&servers)
    {
        let sink = vec![
            ("USE_STATSD", "false".to_string()),
            ("USE_DOG_STATSD", "true".into()),
            ("STATSD_HOST", "127.0.0.1".into()),
            ("STATSD_PORT", server.address.port().to_string()),
            tagged.clone(),
        ];
        let service = Service::start(binary, &stats_environment(redis, sink)).await;
        server_calls(&service).await;
        let lines = || {
            let datagrams = server.datagrams.lock().unwrap();
            datagrams
                .iter()
                .flat_map(|d| {
                    String::from_utf8_lossy(d)
                        .lines()
                        .map(String::from)
                        .collect::<Vec<_>>()
                })
                .collect()
        };
        sums.push(wait_for_server_stats(lines).await);
    }
    assert!(
        sums[0]
            .keys()
            .any(|k| k.contains("ShouldRateLimit.response_time")),
        "{:?}",
        sums[0]
    );
    assert_eq!(sums[1], sums[0], "DogStatsD");

    // Prometheus with the default mapper: a counter and a histogram per method; with
    // `EXTRA_TAGS` the names are not mapped, and the timers become summaries. Bucket and
    // quantile values depend on each side's timing and are left out.
    for tags in [None, Some(tagged.clone())] {
        let mut series = Vec::new();
        for (binary, redis) in [
            upstream_binary(),
            PathBuf::from(env!("CARGO_BIN_EXE_ratelimit")),
        ]
        .iter()
        .zip(&redis)
        {
            let address = format!("127.0.0.1:{}", reserved_port());
            let mut sink = vec![
                ("USE_STATSD", "false".to_string()),
                ("USE_PROMETHEUS", "true".into()),
                ("PROMETHEUS_ADDR", address.clone()),
            ];
            sink.extend(tags.clone());
            let service = Service::start(binary, &stats_environment(redis, sink)).await;
            server_calls(&service).await;
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            let lines = loop {
                let scraped = http_request(&address, "GET", "/metrics", &[]).await;
                let lines: Vec<String> = scraped
                    .body
                    .lines()
                    .filter(|l| {
                        l.starts_with("# TYPE ratelimit_s")
                            || (l.starts_with("ratelimit_s")
                                && (l.contains("total_requests") || l.contains("_count")))
                    })
                    .map(String::from)
                    .collect();
                // 6 calls counted, and 6 timed.
                let total: f64 = lines
                    .iter()
                    .filter(|l| !l.starts_with('#'))
                    .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
                    .sum();
                if total >= 12.0 || std::time::Instant::now() > deadline {
                    break lines;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            };
            series.push(lines);
        }
        assert!(
            series[0].iter().any(|l| l.contains("_count")),
            "{:?}",
            series[0]
        );
        assert_eq!(series[1], series[0], "Prometheus with {tags:?}");
    }
}

/// The server's stats from `lines` once every call of `server_calls` is in them (3
/// `ShouldRateLimit`, 2 `Check`, 1 `List`, each counted and timed), or after 10 s.
async fn wait_for_server_stats(lines: impl Fn() -> Vec<String>) -> BTreeMap<String, u64> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let sums = server_stat_lines(&lines());
        let calls: u64 = sums
            .iter()
            .filter(|(name, _)| name.starts_with("ratelimit_server."))
            .map(|(_, n)| n)
            .sum();
        if calls >= 12 || std::time::Instant::now() > deadline {
            return sums;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// Timers mapped to summaries (the default observer type) reach Prometheus like upstream's:
/// statsd_exporter's quantiles, the defaults' or the mapping's (a quantile given twice keeps
/// its last error), the mapping's options in full, the streams' estimates over many
/// observations and buffer flushes, and the names a summary takes from other metrics.
#[tokio::test]
async fn summaries_reach_prometheus_like_upstream() {
    if !tools_available() {
        return;
    }
    let dense: Vec<String> = (1..20)
        .map(|i| format!("{{quantile: {}, error: 0.005}}", i as f64 / 20.0))
        .chain(["{quantile: 0.99, error: 0.0005}".to_string()])
        .collect();
    let mapper = format!(
        "
defaults:
  summary_options:
    quantiles: [{{quantile: 0.3, error: 0.03}}, {{quantile: 0.7, error: 0.01}}]
mappings:
- match: labelled.*
  name: labelled
  labels: {{kind: $1}}
- match: custom.*
  name: custom
  summary_options:
    quantiles: [{{quantile: 0.25, error: 0.001}}, {{quantile: 0.75, error: 0.02}}, {{quantile: 0.25, error: 0.2}}]
    buf_cap: 3
- match: empty.*
  name: empty
  summary_options: {{quantiles: [], age_buckets: 2}}
- match: legacy.*
  name: legacy
  summary_options: {{quantiles: [{{quantile: 0.1, error: 0.01}}, {{quantile: 0.999, error: 0.0001}}]}}
- match: dense.*
  name: dense
  summary_options: {{quantiles: [{}]}}
- match: hist.*
  name: plain_timer_sum
  observer_type: histogram
- match: na.*
  name: nm
  labels: {{lv: $1}}
- match: nb.*
  name: nm
  labels: {{lv: $1}}
  summary_options: {{max_age: -1s}}
- match: helpa.*
  name: helped
  help: a
- match: helpb.*
  name: helped
  help: b
  labels: {{lv: $1}}
",
        dense.join(", ")
    );
    let settings = Settings {
        prometheus: Some(Prometheus::new(Some(&mapper)).await),
        ..Default::default()
    };
    let h = Harness::configured(TOKENS, settings).await;
    // Each side's age buckets turn every 2 minutes from its series' creation: the test runs
    // well within one, so that both sides' streams hold the same observations.
    // Values in a shuffled order, with repeats, negatives and large ones.
    let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut next = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    let mut values: Vec<f64> = (0..4000)
        .map(|_| (next() % 100_000) as f64 - 1000.0)
        .collect();
    values.extend([0.0, -0.5, 1e9, 3.25, 3.25, 3.25]);
    let batch = |name: &'static str, values: &[f64]| -> Vec<(&'static str, &'static str, f64)> {
        values.iter().map(|v| ("timer", name, *v)).collect()
    };
    // A counter named like a summary's `_sum` makes the summary a conflict.
    h.flush_to_prometheus(&[("counter", "pre_sum", 1.0), ("timer", "pre", 1.0)])
        .await;
    h.flush_to_prometheus(&batch("plain_timer", &values[..10]))
        .await;
    for name in [
        "labelled.a",
        "labelled.b",
        "custom.x",
        "empty.x",
        "legacy.x",
        "dense.x",
    ] {
        h.flush_to_prometheus(&batch(name, &values[..7])).await;
    }
    // Scraped between batches, so that each Write flushes the buffers in between.
    let mut scraped = None;
    for chunk in values.chunks(800) {
        let mut stats = batch("plain_timer", chunk);
        for name in ["custom.x", "dense.x", "labelled.a", "empty.x"] {
            stats.extend(batch(name, chunk));
        }
        scraped = Some(h.flush_to_prometheus(&stats).await);
    }
    let body = scraped.unwrap().body;
    for series in [
        "dense{quantile=\"0.95\"}",
        "custom{quantile=\"0.25\"}",
        "empty{quantile=\"0.7\"}",
        "legacy{quantile=\"0.999\"}",
        "labelled{kind=\"a\",quantile=\"0.3\"}",
        "plain_timer_count 4016",
    ] {
        assert!(body.contains(series), "{series} in {body}");
    }
    // Zeros of either sign are `0`; a mapping with a negative `max_age` reuses the vector
    // another made (only a new vector would panic upstream).
    let scraped = h
        .flush_to_prometheus(&[
            ("timer", "zero.x", -0.0),
            ("timer", "na.x", 1.0),
            ("timer", "nb.y", 2.0),
        ])
        .await;
    assert!(
        scraped.body.contains("zero_x{quantile=\"0.3\"} 0\n"),
        "{}",
        scraped.body
    );
    assert!(
        scraped.body.contains("nm{lv=\"y\",quantile=\"0.3\"} 2\n"),
        "{}",
        scraped.body
    );
    // A summary's `_count` taken first, a counter's name taken by a summary.
    h.flush_to_prometheus(&[
        ("counter", "taken", 1.0),
        ("timer", "taken", 1.0),
        ("counter", "late_count", 1.0),
        ("timer", "late", 1.0),
    ])
    .await;
    // The same name with other help (and labels): Gather refuses the second summary.
    h.flush_to_prometheus(&[("timer", "helpa.x", 1.0), ("timer", "helpb.y", 2.0)])
        .await;
    // A gauge named like a summary's `_sum` is refused; a histogram is registered, but
    // Gather refuses it.
    h.flush_to_prometheus(&[("gauge", "plain_timer_sum", 1.0)])
        .await;
    h.flush_to_prometheus(&[("timer", "hist.x", 1.0)]).await;
}

/// The latest value of each gauge and the sum of each counter in statsd `lines`, for the
/// names `keep` selects.
fn stat_values(lines: &[String], keep: impl Fn(&str) -> bool) -> BTreeMap<String, u64> {
    let mut values = BTreeMap::new();
    for line in lines {
        let Some((name, rest)) = line.split_once(':') else {
            continue;
        };
        if !keep(name) {
            continue;
        }
        let (value, kind) = rest.split_once('|').unwrap();
        let value: u64 = value.parse().unwrap();
        match kind {
            "g" => {
                values.insert(format!("{name}|g"), value);
            }
            _ => *values.entry(format!("{name}|{kind}")).or_default() += value,
        }
    }
    values
}

/// The gauges and pool stats upstream flushes like upstream: `runtime.num_values`, the local
/// cache's counts (`localcache.*`, but its average access time, read from each side's clock),
/// and the Redis pool's connections at startup and after Redis comes back.
#[tokio::test]
async fn gauges_are_reported_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("gauges");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            // Two files with the same key count once; dotfiles are ignored.
            ("v1/ratelimit/other/b", b"1"),
            ("v1/ratelimit/other.b", b"2"),
            ("v1/ratelimit/other/c", b"3"),
            ("v1/ratelimit/.hidden", b"4"),
            ("current", b"->v1"),
        ],
    );
    let servers = [StatsdServer::start().await, StatsdServer::start().await];
    let redis = [Redis::start().await, Redis::start().await];
    let mut services = Vec::new();
    for ((binary, redis), server) in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_ratelimit")),
    ]
    .iter()
    .zip(&redis)
    .zip(&servers)
    {
        let mut environment = binary_environment(&runtime, redis);
        environment.extend([
            ("DISABLE_STATS", "false".to_string()),
            ("STATS_FLUSH_INTERVAL", "100ms".into()),
            ("STATSD_HOST", "127.0.0.1".into()),
            ("STATSD_PORT", server.address.port().to_string()),
            ("RUNTIME_IGNOREDOTFILES", "true".into()),
            ("REDIS_POOL_SIZE", "2".into()),
            ("LOCAL_CACHE_SIZE_IN_BYTES", "1048576".into()),
        ]);
        services.push(Service::start(binary, &environment).await);
    }
    // Over the limit (2 a minute): the key goes to the local cache, then hits it. Each call
    // goes to both sides in turn, so that a minute cannot end between them.
    let mut clients = [services[0].client().await, services[1].client().await];
    for user in ["a", "a", "a", "a", "a", "b"] {
        for client in &mut clients {
            client
                .should_rate_limit(request("llm", &[&[("user", user)]]))
                .await
                .unwrap();
        }
    }
    let values = settled_gauges(&servers, |v| {
        v.get("ratelimit.localcache.hitCount|g") == Some(&2)
    })
    .await;
    assert_eq!(values[1], values[0]);
    let hits = values[0].get("ratelimit.localcache.hitCount|g");
    // The third call over the limit fills the cache, the next two hit it.
    assert_eq!(hits, Some(&2), "{:?}", values[0]);
    assert_eq!(
        values[0].get("runtime.num_values|g"),
        Some(&3),
        "{:?}",
        values[0]
    );
    assert_eq!(
        values[0].get("ratelimit.redis_pool.cx_active|g"),
        Some(&2),
        "{:?}",
        values[0]
    );

    // Redis back on the same port: our pool replaces its two connections at once. Radix only
    // finds a dead connection when it uses it (docs/parity.md §7), so upstream's count is not
    // compared here.
    for redis in &redis {
        let _: Result<(), _> = redis
            .client
            .custom(
                CustomCommand::new_static("SHUTDOWN", ClusterHash::Random, false),
                vec!["NOSAVE"],
            )
            .await;
    }
    let mut restarted = Vec::new();
    for redis in &redis {
        let port = redis.address.rsplit_once(':').unwrap().1.parse().unwrap();
        restarted.push(Redis::start_on(port).await.expect("Redis restarted"));
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let expected = [
        ("ratelimit.redis_pool.cx_active|g", 2),
        ("ratelimit.redis_pool.cx_local_close|c", 2),
        ("ratelimit.redis_pool.cx_total|c", 4),
    ];
    let ours = loop {
        let values = stat_values(&servers[1].lines.lock().unwrap(), |n| {
            n.starts_with("ratelimit.redis_pool.")
        });
        let pool: Vec<(&str, u64)> = expected
            .iter()
            .map(|(k, _)| (*k, values.get(*k).copied().unwrap_or(0)))
            .collect();
        if pool == expected || std::time::Instant::now() > deadline {
            break pool;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    };
    assert_eq!(ours, expected);
}

/// The pool, local cache and runtime stats of both statsd servers once they are the same and
/// `expected` holds, or after 15 s; the local cache's average access time, read from each
/// side's clock, as 0.
async fn settled_gauges(
    servers: &[StatsdServer; 2],
    expected: impl Fn(&BTreeMap<String, u64>) -> bool,
) -> Vec<BTreeMap<String, u64>> {
    let keep = |name: &str| {
        name.starts_with("ratelimit.redis_pool.")
            || name.starts_with("ratelimit.localcache.")
            || name == "runtime.num_values"
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        let values: Vec<BTreeMap<String, u64>> = servers
            .iter()
            .map(|s| {
                let mut v = stat_values(&s.lines.lock().unwrap(), keep);
                if let Some(time) = v.get_mut("ratelimit.localcache.averageAccessTime|g") {
                    *time = 0;
                }
                v
            })
            .collect();
        if (values[0] == values[1] && expected(&values[0])) || std::time::Instant::now() > deadline
        {
            return values;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// Before Redis answers, the pool's and the local cache's gauges are already flushed, at 0:
/// upstream makes them before its startup loop.
#[tokio::test]
async fn gauges_are_flushed_while_redis_is_unreachable_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("gauges-no-redis");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let unreachable = format!("127.0.0.1:{}", reserved_port());
    let servers = [StatsdServer::start().await, StatsdServer::start().await];
    let mut services = Vec::new();
    for (binary, server) in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_ratelimit")),
    ]
    .iter()
    .zip(&servers)
    {
        let environment = vec![
            (
                "RUNTIME_ROOT",
                runtime.join("current").to_string_lossy().into_owned(),
            ),
            ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
            ("REDIS_SOCKET_TYPE", "tcp".into()),
            ("REDIS_URL", unreachable.clone()),
            ("LOG_LEVEL", "error".into()),
            ("STATS_FLUSH_INTERVAL", "100ms".into()),
            ("STATSD_HOST", "127.0.0.1".into()),
            ("STATSD_PORT", server.address.port().to_string()),
            ("LOCAL_CACHE_SIZE_IN_BYTES", "1048576".into()),
        ];
        services.push(Service::start(binary, &environment).await);
    }
    // The runtime loader, started before Redis, has its one file.
    let values = settled_gauges(&servers, |v| v.len() == 10).await;
    assert_eq!(values[1], values[0]);
    assert_eq!(values[0].len(), 10, "{:?}", values[0]);
    let zeros = values[0]
        .iter()
        .filter(|(k, _)| *k != "runtime.num_values|g");
    assert!(zeros.clone().all(|(_, v)| *v == 0), "{:?}", values[0]);
}

/// Runs `binary` with `environment` until it serves gRPC, sends it one request, stops it
/// with SIGTERM, and returns what it wrote to stderr.
async fn logged_run(
    binary: &std::path::Path,
    environment: &[(&str, String)],
    grpc: &str,
) -> String {
    let child = Command::new(binary)
        .env_clear()
        .envs(environment.iter().map(|(k, v)| (*k, v.as_str())))
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("start the service");
    let mut client = None;
    for _ in 0..300 {
        if let Ok(c) = RateLimitServiceClient::connect(grpc.to_string()).await {
            client = Some(c);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let mut client = client.expect("the service serves");
    client
        .should_rate_limit(request("llm", &[&[("user", "a")]]))
        .await
        .unwrap();
    drop(client);
    let pid = child.id().unwrap().to_string();
    assert!(
        std::process::Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .unwrap()
            .success()
    );
    let output = tokio::time::timeout(std::time::Duration::from_secs(30), child.wait_with_output())
        .await
        .expect("the service stops")
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// The logrus lines of `stderr` (gostats' own JSON lines left out), sorted, with the time and
/// what differs between the two runs (`replace`) masked.
fn logrus_lines(stderr: &str, replace: &[(String, &str)]) -> Vec<String> {
    let time = regex::Regex::new(r#"(time="|"@timestamp":")[^"]+""#).unwrap();
    let mut lines: Vec<String> = stderr
        .lines()
        .filter(|l| !l.contains("gostats.loggingsink"))
        .map(|l| {
            let mut l = time.replace(l, "${1}TIME\"").into_owned();
            for (from, to) in replace {
                l = l.replace(from.as_str(), to);
            }
            l
        })
        .collect();
    lines.sort();
    lines
}

/// Upstream's log lines, in its text and JSON formats, at the info level: the sink, tracing,
/// the runtime directory's unreadable entries, Redis, the config, the listeners, and the
/// shutdown on SIGTERM; and the time's shape in the local time zone.
#[tokio::test]
async fn logs_are_written_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("logs");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("v1/ratelimit/dangling", b"->missing"),
            ("v1/ratelimit/linked", b"->config"),
            ("current", b"->v1"),
        ],
    );
    let time_shape = |stderr: &str, key: &str| -> Vec<String> {
        let digits = regex::Regex::new(r"\d").unwrap();
        let time = regex::Regex::new(&format!(r#"{key}"([^"]+)""#)).unwrap();
        // The fraction's trailing zeros are trimmed: its length varies.
        let fraction = regex::Regex::new(r"\.N+").unwrap();
        let mut shapes: Vec<String> = time
            .captures_iter(stderr)
            .map(|c| digits.replace_all(&c[1], "N").into_owned())
            .map(|shape| fraction.replace(&shape, ".F").into_owned())
            .collect();
        shapes.sort();
        shapes.dedup();
        shapes
    };
    for (format, tz, time_key, offset) in [
        ("text", None, "time=", None),
        (
            "json",
            Some("Asia/Kolkata"),
            "\"@timestamp\":",
            Some("+05:30\""),
        ),
        ("text", Some(""), "time=", Some("Z\"")),
    ] {
        let redis = [Redis::start().await, Redis::start().await];
        let mut runs = Vec::new();
        for (binary, redis) in [
            upstream_binary(),
            PathBuf::from(env!("CARGO_BIN_EXE_ratelimit")),
        ]
        .iter()
        .zip(&redis)
        {
            let (grpc, http, debug) = (reserved_port(), reserved_port(), reserved_port());
            let mut environment = binary_environment(&runtime, redis);
            environment.retain(|(k, _)| *k != "LOG_LEVEL");
            environment.extend([
                ("LOG_LEVEL", "info".to_string()),
                ("LOG_FORMAT", format.into()),
                ("GRPC_HOST", "127.0.0.1".into()),
                ("GRPC_PORT", grpc.to_string()),
                ("HOST", "127.0.0.1".into()),
                ("PORT", http.to_string()),
                // The wildcard: Go names the dual-stack socket `[::]` once it closes.
                ("DEBUG_HOST", "0.0.0.0".into()),
                ("DEBUG_PORT", debug.to_string()),
            ]);
            if let Some(tz) = tz {
                environment.push(("TZ", tz.into()));
            }
            let stderr =
                logged_run(binary, &environment, &format!("http://127.0.0.1:{grpc}")).await;
            if let Some(offset) = offset {
                assert!(stderr.contains(offset), "{offset} in {stderr}");
            }
            let replace = [
                (runtime.display().to_string(), "RUNTIME"),
                (redis.address.clone(), "REDIS"),
                (grpc.to_string(), "GRPC"),
                (http.to_string(), "HTTP"),
                (debug.to_string(), "DEBUG"),
            ];
            runs.push((
                logrus_lines(&stderr, &replace),
                time_shape(&stderr, time_key),
            ));
        }
        assert!(runs[0].0.len() > 5, "{:?}", runs[0].0);
        assert_eq!(runs[1].0, runs[0].0, "{format} {tz:?}");
        assert_eq!(runs[1].1, runs[0].1, "{format} {tz:?}");
    }
}

/// Upstream's Redis startup lines: the pool, the authentication, each failed connection
/// (unreachable, or refused by Redis: AUTH, the database), each retry (its wait and elapsed
/// time masked: they are random), then the fatal line once it gives up.
#[tokio::test]
async fn redis_startup_is_logged_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("redis-logs");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let unreachable = format!("127.0.0.1:{}", reserved_port());
    let redis = Redis::start().await;
    let refused_database = format!("redis://{}/abc", redis.address);
    let waits = regex::Regex::new(r"in [^ ]+ \(elapsed: [^)]+\)").unwrap();
    let after = regex::Regex::new(r"after [^:]+:").unwrap();
    for (url, auth, behavior) in [
        (&unreachable, "user:secret", "wait"),
        (&unreachable, "secret", "other"),
        (&redis.address, "secret", "wait"),
        (&refused_database, "", "wait"),
    ] {
        let environment = vec![
            (
                "RUNTIME_ROOT",
                runtime.join("current").to_string_lossy().into_owned(),
            ),
            ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
            ("REDIS_SOCKET_TYPE", "tcp".into()),
            ("REDIS_URL", url.clone()),
            ("REDIS_AUTH", auth.into()),
            ("REDIS_POOL_SIZE", "3".into()),
            ("REDIS_PIPELINE_LIMIT", "8".into()),
            ("REDIS_POOL_ON_EMPTY_BEHAVIOR", behavior.into()),
            ("REDIS_STARTUP_INITIAL_INTERVAL", "50ms".into()),
            ("REDIS_STARTUP_MAX_INTERVAL", "100ms".into()),
            ("REDIS_STARTUP_MAX_ELAPSED_TIME", "1s".into()),
            ("DISABLE_STATS", "true".into()),
            ("LOG_LEVEL", "warn".into()),
        ];
        let lines = |stderr: &str| {
            // The logrus lines: upstream's panic (our fatal line, §7) is compared elsewhere.
            let mut lines: Vec<String> = logrus_lines(stderr, &[])
                .into_iter()
                .filter(|l| l.starts_with("time=") && !l.contains("level=fatal"))
                .map(|l| waits.replace(&l, "in WAIT (elapsed: ELAPSED)").into_owned())
                .map(|l| after.replace(&l, "after ELAPSED:").into_owned())
                .collect();
            lines.dedup();
            lines
        };
        let upstream = lines(&startup_failure(&upstream_binary(), &environment).await);
        let ours = lines(
            &startup_failure(
                std::path::Path::new(env!("CARGO_BIN_EXE_ratelimit")),
                &environment,
            )
            .await,
        );
        assert!(
            upstream.iter().any(|l| l.contains("Retrying")),
            "{upstream:?}"
        );
        assert_eq!(ours, upstream, "{url} {auth} {behavior}");
    }
}

/// A Redis with a password: the right one serves (the startup probe authenticates too), a
/// wrong one is refused at each attempt, as upstream logs it.
#[tokio::test]
async fn a_password_protected_redis_is_logged_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("redis-password");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let redis = Redis::start().await;
    // Set once the harness's connections are open: they stay authenticated.
    let mut stream = tokio::net::TcpStream::connect(&redis.address)
        .await
        .unwrap();
    tokio::io::AsyncWriteExt::write_all(
        &mut stream,
        b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$11\r\nrequirepass\r\n$2\r\npw\r\n",
    )
    .await
    .unwrap();
    let mut reply = String::new();
    BufReader::new(&mut stream)
        .read_line(&mut reply)
        .await
        .unwrap();
    assert_eq!(reply, "+OK\r\n");
    let waits = regex::Regex::new(r"in [^ ]+ \(elapsed: [^)]+\)").unwrap();
    let after = regex::Regex::new(r"after [^:]+:").unwrap();
    let redis_lines = |stderr: &str| {
        let mut lines: Vec<String> = logrus_lines(stderr, &[])
            .into_iter()
            .filter(|l| l.starts_with("time=") && !l.contains("level=fatal"))
            .filter(|l| l.contains("redis") || l.contains("Redis"))
            .map(|l| waits.replace(&l, "in WAIT (elapsed: ELAPSED)").into_owned())
            .map(|l| after.replace(&l, "after ELAPSED:").into_owned())
            .collect();
        lines.dedup();
        lines
    };
    for auth in ["pw", "default:pw", "wrong"] {
        let mut runs = Vec::new();
        for binary in [
            upstream_binary(),
            PathBuf::from(env!("CARGO_BIN_EXE_ratelimit")),
        ] {
            let grpc = reserved_port();
            let environment = vec![
                (
                    "RUNTIME_ROOT",
                    runtime.join("current").to_string_lossy().into_owned(),
                ),
                ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
                ("REDIS_SOCKET_TYPE", "tcp".into()),
                ("REDIS_URL", redis.address.clone()),
                ("REDIS_AUTH", auth.into()),
                ("REDIS_STARTUP_INITIAL_INTERVAL", "50ms".into()),
                ("REDIS_STARTUP_MAX_INTERVAL", "100ms".into()),
                ("REDIS_STARTUP_MAX_ELAPSED_TIME", "1s".into()),
                ("DISABLE_STATS", "true".into()),
                ("LOG_LEVEL", "info".into()),
                ("GRPC_HOST", "127.0.0.1".into()),
                ("GRPC_PORT", grpc.to_string()),
                ("HOST", "127.0.0.1".into()),
                ("PORT", reserved_port().to_string()),
                ("DEBUG_HOST", "127.0.0.1".into()),
                ("DEBUG_PORT", reserved_port().to_string()),
            ];
            let stderr = match auth {
                "wrong" => startup_failure(&binary, &environment).await,
                _ => logged_run(&binary, &environment, &format!("http://127.0.0.1:{grpc}")).await,
            };
            runs.push(redis_lines(&stderr));
        }
        assert!(runs[0].len() > 2, "{auth}: {:?}", runs[0]);
        assert_eq!(runs[1], runs[0], "{auth}");
    }
}

/// A Redis that answers only after the first dials fail: each failure, each retry, then the
/// connection restored, as upstream logs them.
#[tokio::test]
async fn a_late_redis_is_logged_like_upstream() {
    if !tools_available() {
        return;
    }
    let runtime = temp_dir("late-redis");
    write_tree(
        &runtime,
        &[
            ("v1/ratelimit/config/llm.yaml", LLM_CONFIG),
            ("current", b"->v1"),
        ],
    );
    let waits = regex::Regex::new(r"in [^ ]+ \(elapsed: [^)]+\)").unwrap();
    let mut runs = Vec::new();
    for binary in [
        upstream_binary(),
        PathBuf::from(env!("CARGO_BIN_EXE_ratelimit")),
    ] {
        let port = reserved_port();
        let grpc = reserved_port();
        let environment = vec![
            (
                "RUNTIME_ROOT",
                runtime.join("current").to_string_lossy().into_owned(),
            ),
            ("RUNTIME_SUBDIRECTORY", "ratelimit".into()),
            ("REDIS_SOCKET_TYPE", "tcp".into()),
            ("REDIS_URL", format!("127.0.0.1:{port}")),
            ("REDIS_STARTUP_INITIAL_INTERVAL", "100ms".into()),
            ("REDIS_STARTUP_MAX_INTERVAL", "200ms".into()),
            ("DISABLE_STATS", "true".into()),
            ("LOG_LEVEL", "info".into()),
            ("GRPC_HOST", "127.0.0.1".into()),
            ("GRPC_PORT", grpc.to_string()),
            ("HOST", "127.0.0.1".into()),
            ("PORT", reserved_port().to_string()),
            ("DEBUG_HOST", "127.0.0.1".into()),
            ("DEBUG_PORT", reserved_port().to_string()),
        ];
        let late = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(800)).await;
            Redis::start_on(port).await.expect("Redis started")
        });
        let stderr = logged_run(&binary, &environment, &format!("http://127.0.0.1:{grpc}")).await;
        let _redis = late.await.unwrap();
        let mut lines: Vec<String> = logrus_lines(&stderr, &[(port.to_string(), "REDIS")])
            .into_iter()
            .filter(|l| l.contains("redis") || l.contains("Redis"))
            .map(|l| waits.replace(&l, "in WAIT (elapsed: ELAPSED)").into_owned())
            .collect();
        lines.dedup();
        runs.push(lines);
    }
    assert!(
        runs[0].iter().any(|l| l.contains("re-established")),
        "{:?}",
        runs[0]
    );
    assert_eq!(runs[1], runs[0]);
}

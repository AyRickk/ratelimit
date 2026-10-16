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
use ratelimit::service::{RateLimitService, ServiceSettings};
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
        let port = free_port().await;
        let process = Command::new(redis_server())
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
        for _ in 0..100 {
            if client.init().await.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
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
        Self {
            _process: process,
            client,
            client_address,
            address,
            monitor,
        }
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

impl Oracle {
    async fn start(redis: &Redis, settings: &RedisCacheSettings) -> Self {
        let local_cache_size = settings.local_cache_size_in_bytes.to_string();
        let stop_increment = format!(
            "-stop-cache-key-increment={}",
            settings.stop_cache_key_increment_when_overlimit
        );
        let mut process = Command::new(oracle_binary())
            .args(["-redis", &redis.address])
            .args(["-cache-key-prefix", &settings.cache_key_prefix])
            .args(["-local-cache-size", &local_cache_size, &stop_increment])
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
        let grpc = address("grpc", lines.next_line().await.unwrap());
        let control = address("control", lines.next_line().await.unwrap());
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
        let mut stream = tokio::net::TcpStream::connect(&self.control).await.unwrap();
        let request = format!(
            "PUT {path} HTTP/1.1\r\nHost: oracle\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        tokio::io::AsyncWriteExt::write_all(&mut stream, request.as_bytes())
            .await
            .unwrap();
        let mut response = String::new();
        tokio::io::AsyncReadExt::read_to_string(&mut stream, &mut response)
            .await
            .unwrap();
        assert!(
            response.starts_with("HTTP/1.1 200"),
            "oracle {path}: {response}"
        );
    }
}

/// This implementation, served over gRPC like the oracle.
struct Subject {
    client: RateLimitServiceClient<Channel>,
    service: Arc<RateLimitService<RedisCache>>,
    clock: Arc<ManualClock>,
}

impl Subject {
    async fn start(redis: &Redis, settings: &RedisCacheSettings) -> Self {
        let clock = Arc::new(ManualClock::default());
        let cache = RedisCache::new(redis.connect().await, clock.clone(), settings.clone());
        let service = Arc::new(RateLimitService::new(cache, ServiceSettings::default()));
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
        }
    }
}

struct Harness {
    oracle: Oracle,
    oracle_redis: Redis,
    subject: Subject,
    subject_redis: Redis,
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

    /// Both sides configured alike: `settings` for this implementation, the matching flags for
    /// the oracle.
    async fn with_settings(config: &str, settings: RedisCacheSettings) -> Self {
        let oracle_redis = Redis::start().await;
        let subject_redis = Redis::start().await;
        let oracle = Oracle::start(&oracle_redis, &settings).await;
        let subject = Subject::start(&subject_redis, &settings).await;
        oracle.put("/config", config.to_string()).await;
        let file = ConfigFile {
            name: "config.yaml".into(),
            content: config.into(),
        };
        subject
            .service
            .set_config(config::load(&[file], &LoadOptions::default()).unwrap());
        Self {
            oracle,
            oracle_redis,
            subject,
            subject_redis,
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
        let (expected, actual) = (simplify(expected), simplify(actual));
        assert_eq!(actual, expected, "response to {request:?}");
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

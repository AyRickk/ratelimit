//! Differential tests (docs/design.md D32): every scenario runs against envoyproxy/ratelimit
//! itself (the Go program in `tools/oracle`) and against this implementation, each with its own
//! Redis, on the same simulated clock. Responses and the final Redis contents must be identical.
//!
//! Needs `go` and `redis-server` on the PATH. Without them the tests are skipped, unless
//! `RATELIMIT_REQUIRE_DIFFERENTIAL` is set (as in CI), in which case they fail.

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

fn tools_available() -> bool {
    let found = |tool: &str| {
        std::process::Command::new(tool)
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok()
    };
    let available = found("go") && found("redis-server");
    if !available {
        assert!(
            std::env::var_os("RATELIMIT_REQUIRE_DIFFERENTIAL").is_none(),
            "differential tests need `go` and `redis-server`"
        );
        eprintln!("skipping: differential tests need `go` and `redis-server`");
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
    client: fred::clients::Client,
    address: String,
}

impl Redis {
    async fn start() -> Self {
        let port = free_port().await;
        let process = Command::new("redis-server")
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
            .expect("start redis-server");
        let address = format!("127.0.0.1:{port}");
        let config = Config::from_url(&format!("redis://{address}")).unwrap();
        let client = Builder::from_config(config).build().unwrap();
        for _ in 0..100 {
            if client.init().await.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        Self {
            _process: process,
            client,
            address,
        }
    }

    /// Every key with its value and TTL.
    async fn dump(&self) -> BTreeMap<String, (String, i64)> {
        let keys: Vec<String> = self
            .client
            .custom(
                CustomCommand::new_static("KEYS", ClusterHash::Random, false),
                vec!["*"],
            )
            .await
            .unwrap();
        let mut dump = BTreeMap::new();
        for key in keys {
            let value: String = self.client.get(&key).await.unwrap();
            let ttl: i64 = self.client.ttl(&key).await.unwrap();
            dump.insert(key, (value, ttl));
        }
        dump
    }
}

/// envoyproxy/ratelimit, controlled over HTTP.
struct Oracle {
    _process: Child,
    client: RateLimitServiceClient<Channel>,
    control: String,
}

impl Oracle {
    async fn start(redis: &Redis, prefix: &str) -> Self {
        let mut process = Command::new(oracle_binary())
            .args(["-redis", &redis.address, "-cache-key-prefix", prefix])
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
    async fn start(redis: &Redis, prefix: &str) -> Self {
        let clock = Arc::new(ManualClock::default());
        let settings = RedisCacheSettings {
            cache_key_prefix: prefix.to_string(),
        };
        let cache = RedisCache::new(redis.client.clone(), clock.clone(), settings);
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
        Self::with_prefix(config, "").await
    }

    async fn with_prefix(config: &str, prefix: &str) -> Self {
        let oracle_redis = Redis::start().await;
        let subject_redis = Redis::start().await;
        let oracle = Oracle::start(&oracle_redis, prefix).await;
        let subject = Subject::start(&subject_redis, prefix).await;
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

    async fn assert_same_redis(&self) {
        let expected = self.oracle_redis.dump().await;
        assert!(
            !expected.is_empty(),
            "the scenario should leave keys in Redis"
        );
        assert_eq!(self.subject_redis.dump().await, expected, "Redis contents");
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

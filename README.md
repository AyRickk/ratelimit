# ratelimit

A Rust rewrite of [envoyproxy/ratelimit](https://github.com/envoyproxy/ratelimit), compatible with it at commit [`f4e7669`](https://github.com/envoyproxy/ratelimit/commit/f4e7669). To replace envoy ratelimit behind Envoy, Envoy Gateway, Istio or agentgateway, you only change the image: the environment variables, the YAML configuration, the gRPC and HTTP API, the metrics, the log lines and the image's layout stay the same.

## Status

- **Compatibility:** every behaviour listed in [`docs/parity.md`](docs/parity.md) is proven by a test that runs upstream's binary and this one side by side and compares their answers, Redis or Memcached keys, stats and logs. The few places where this service differs on purpose (an upstream panic becomes an error, a stricter TLS client check, …) are listed in its §7.
- **Extensions (not built yet):** several limits per descriptor (`rate_limits:`), windows of any duration (`fixed` with an `offset`), plus the `anchored`, `sliding` and `token_bucket` window types and the `overflow: envoy | strict` counting modes. They are designed in [`docs/design.md`](docs/design.md) and its ADRs, and are opt-in: a configuration upstream accepts keeps working the same.
- **Releases:** none yet. The first one is `v0.1.0`, once the extensions are in. It will be published as `ghcr.io/ayrickk/ratelimit` for linux/amd64 and linux/arm64, with an SBOM and provenance, signed with cosign.

## Supported

Every upstream feature is supported:

- **Backends:** Redis (single, sentinel, cluster, TLS, AWS IAM, per-second pool) and Memcached (with SRV discovery).
- **Configuration:** files with hot reload, or xDS.
- **Limiting:** the local cache, `quota_mode` and `share_threshold`.
- **API:** the `LIMIT_*` headers, dynamic metadata, `/json`, the debug port and `grpc.health.v1`.
- **Stats sinks:** statsd, DogStatsD and Prometheus (statsd_exporter's mapper).
- **Tracing:** OTLP traces.
- **Network:** gRPC over TLS with certificate reload, and gRPC on a Unix socket.

## Building and testing

```sh
cargo build --release -p ratelimit       # target/release/ratelimit
docker build -t ratelimit .               # the image: /bin/ratelimit on distroless
```

The differential tests build upstream's service from [`tools/oracle`](tools/oracle) and need `go`, `redis-server` and `memcached` on the `PATH`. Without them, those tests are skipped:

```sh
RATELIMIT_REQUIRE_DIFFERENTIAL=1 cargo test --workspace
RATELIMIT_REDIS_SERVER=/path/to/valkey-server cargo test --workspace   # against Valkey
```

How the work is organised (slices, tests, reviews, conventions) is in [`AGENTS.md`](AGENTS.md).

## License

[Apache-2.0](LICENSE). [`NOTICE`](NOTICE) credits envoyproxy/ratelimit, the Envoy API and the other projects this one ports. The image ships both files, with the licenses of the crates the binary is built from, in `/usr/share/doc/ratelimit/`.

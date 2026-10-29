# ratelimit

A Rust rewrite of [envoyproxy/ratelimit](https://github.com/envoyproxy/ratelimit), compatible with it at commit [`f4e7669`](https://github.com/envoyproxy/ratelimit/commit/f4e7669). To replace envoy ratelimit behind Envoy, Envoy Gateway, Istio or agentgateway, you only change the image: the environment variables, the YAML configuration, the gRPC and HTTP API, the metrics, the log lines and the image's layout stay the same.

## Status

- **Compatibility:** every behaviour listed in [`docs/parity.md`](docs/parity.md) is proven by a test that runs upstream's binary and this one side by side and compares their answers, Redis or Memcached keys, stats and logs. The few places where this service differs on purpose (an upstream panic becomes an error, a stricter TLS client check, …) are listed in its §7.
- **Extensions:** opt-in, so a configuration upstream accepts keeps working the same. They are designed in [`docs/design.md`](docs/design.md) and its ADRs, and listed in [`docs/parity.md`](docs/parity.md) §8:
  - several named limits per descriptor (`rate_limits:`);
  - `fixed` windows of any duration, with an `offset` (constant or hashed from the descriptor);
  - `anchored` windows, opened by a request and lasting their duration (an LLM's five-hour session);
  - a quota lookup, `POST /quota`, which shows every limit's usage and reset without counting ([ADR 0003](docs/adr/0003-quota-lookup.md)).

  Not built yet: the `sliding` and `token_bucket` windows and `overflow: strict`, refused at load.
- **Releases:** images are published as `ghcr.io/ayrickk/ratelimit:<version>` for linux/amd64 and linux/arm64, with an SBOM and provenance, signed with cosign (keyless):

  ```sh
  cosign verify ghcr.io/ayrickk/ratelimit:0.1.0 \
    --certificate-identity https://github.com/AyRickk/ratelimit/.github/workflows/release.yml@refs/tags/v0.1.0 \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com
  ```

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

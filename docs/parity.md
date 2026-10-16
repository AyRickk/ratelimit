# Parity inventory with envoy ratelimit

Reference: `envoyproxy/ratelimit` at commit `f4e7669` (2026-10-06). Rule (D3): the first version ships only when **every line** is `✅` with a test proving it, or `≠` (deliberate, justified divergence). Deliberate divergences are listed at the end.

Status legend: `⬜` to do · `🚧` in progress · `✅` done and proven · `≠` deliberate divergence · `—` ignored, as upstream.

Section 1 is **generated** from `src/settings/settings.go` (134 variables); regenerate it whenever the reference is upgraded.

## 1. Environment variables

| Variable | Upstream default | Status | Test |
|---|---|---|---|
| `HOST` | `0.0.0.0` | ⬜ | |
| `PORT` | `8080` | ⬜ | |
| `DEBUG_HOST` | `0.0.0.0` | ⬜ | |
| `DEBUG_PORT` | `6070` | ⬜ | |
| `GRPC_UDS` | — | ⬜ | |
| `GRPC_HOST` | `0.0.0.0` | ⬜ | |
| `GRPC_PORT` | `8081` | ⬜ | |
| `GRPC_MAX_CONNECTION_AGE` | `24h` | ⬜ | |
| `GRPC_MAX_CONNECTION_AGE_GRACE` | `1h` | ⬜ | |
| `GRPC_SERVER_USE_TLS` | `false` | ⬜ | |
| `GRPC_SERVER_TLS_CERT` | — | ⬜ | |
| `GRPC_SERVER_TLS_KEY` | — | ⬜ | |
| `GRPC_CLIENT_TLS_CACERT` | — | ⬜ | |
| `GRPC_CLIENT_TLS_SAN` | — | ⬜ | |
| `LOG_LEVEL` | `WARN` | ⬜ | |
| `LOG_FORMAT` | `text` | ⬜ | |
| `CONFIG_TYPE` | `FILE` | ⬜ | |
| `FORCE_START_WITHOUT_INITIAL_CONFIG` | `false` | ⬜ | |
| `CONFIG_GRPC_XDS_NODE_ID` | `default` | ⬜ | |
| `CONFIG_GRPC_XDS_NODE_METADATA` | — | ⬜ | |
| `CONFIG_GRPC_XDS_SERVER_URL` | `localhost:18000` | ⬜ | |
| `CONFIG_GRPC_XDS_SERVER_CONNECT_RETRY_INTERVAL` | `3s` | ⬜ | |
| `CONFIG_GRPC_XDS_CLIENT_ADDITIONAL_HEADERS` | — | ⬜ | |
| `CONFIG_GRPC_XDS_SERVER_USE_TLS` | `false` | ⬜ | |
| `CONFIG_GRPC_XDS_CLIENT_TLS_CERT` | — | ⬜ | |
| `CONFIG_GRPC_XDS_CLIENT_TLS_KEY` | — | ⬜ | |
| `CONFIG_GRPC_XDS_SERVER_TLS_CACERT` | — | ⬜ | |
| `CONFIG_GRPC_XDS_SERVER_TLS_SAN` | — | ⬜ | |
| `XDS_CLIENT_BACKOFF_INITIAL_INTERVAL` | `10s` | ⬜ | |
| `XDS_CLIENT_BACKOFF_MAX_INTERVAL` | `60s` | ⬜ | |
| `XDS_CLIENT_BACKOFF_RANDOM_FACTOR` | `0.5` | ⬜ | |
| `XDS_CLIENT_BACKOFF_JITTER` | `true` | ⬜ | |
| `XDS_CLIENT_MAX_MSG_SIZE_IN_BYTES` | — | ⬜ | |
| `USE_DOG_STATSD` | `false` | ⬜ | |
| `USE_DOG_STATSD_MOGRIFIERS` | — | ⬜ | |
| `USE_STATSD` | `true` | ⬜ | |
| `STATSD_HOST` | `localhost` | ⬜ | |
| `STATSD_PORT` | `8125` | ⬜ | |
| `EXTRA_TAGS` | — | ⬜ | |
| `STATS_FLUSH_INTERVAL` | `10s` | ⬜ | |
| `DISABLE_STATS` | `false` | ⬜ | |
| `SANITIZE_DESCRIPTOR_METRIC_DOTS` | `false` | ⬜ | |
| `USE_PROMETHEUS` | `false` | ⬜ | |
| `PROMETHEUS_ADDR` | `:9090` | ⬜ | |
| `PROMETHEUS_PATH` | `/metrics` | ⬜ | |
| `PROMETHEUS_MAPPER_YAML` | — | ⬜ | |
| `PROMETHEUS_RESPONSE_TIME_AS_MILLISECONDS` | `false` | ⬜ | |
| `RUNTIME_ROOT` | `/srv/runtime_data/current` | ⬜ | |
| `RUNTIME_SUBDIRECTORY` | — | ⬜ | |
| `RUNTIME_APPDIRECTORY` | `config` | ⬜ | |
| `RUNTIME_IGNOREDOTFILES` | `false` | ⬜ | |
| `RUNTIME_WATCH_ROOT` | `true` | ⬜ | |
| `EXPIRATION_JITTER_MAX_SECONDS` | `300` | ⬜ | |
| `LOCAL_CACHE_SIZE_IN_BYTES` | `0` | 🚧 | behaviour: section 4; not read from the environment yet |
| `NEAR_LIMIT_RATIO` | `0.8` | ⬜ | |
| `CACHE_KEY_PREFIX` | — | 🚧 | behaviour: `tests/differential.rs`; not read from the environment yet |
| `BACKEND_TYPE` | `redis` | ⬜ | |
| `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` | `false` | 🚧 | behaviour: section 4; not read from the environment yet |
| `USE_CALENDAR_MONTH_RATE_LIMIT` | `false` | ⬜ | |
| `LIMIT_RESPONSE_HEADERS_ENABLED` | `false` | 🚧 | behaviour: `tests/differential.rs`; not read from the environment yet |
| `LIMIT_LIMIT_HEADER` | `RateLimit-Limit` | 🚧 | behaviour: `tests/differential.rs`; not read from the environment yet |
| `LIMIT_REMAINING_HEADER` | `RateLimit-Remaining` | 🚧 | behaviour: `tests/differential.rs`; not read from the environment yet |
| `LIMIT_RESET_HEADER` | `RateLimit-Reset` | 🚧 | behaviour: `tests/differential.rs`; not read from the environment yet |
| `LIMIT_PER_UNIT_HEADERS_ENABLED` | `false` | 🚧 | behaviour: `tests/differential.rs`; not read from the environment yet |
| `LIMIT_REQUEST_HEADERS_ENABLED` | `false` | 🚧 | behaviour: `tests/differential.rs`; not read from the environment yet |
| `LIMIT_REQUEST_LIMIT_HEADER` | `RateLimit-Limit` | 🚧 | behaviour: `tests/differential.rs`; not read from the environment yet |
| `LIMIT_REQUEST_REMAINING_HEADER` | `RateLimit-Remaining` | 🚧 | behaviour: `tests/differential.rs`; not read from the environment yet |
| `LIMIT_REQUEST_RESET_HEADER` | `RateLimit-Reset` | 🚧 | behaviour: `tests/differential.rs`; not read from the environment yet |
| `HEALTHY_WITH_AT_LEAST_ONE_CONFIG_LOADED` | `false` | ⬜ | |
| `REDIS_SOCKET_TYPE` | `unix` | ⬜ | |
| `REDIS_TYPE` | `SINGLE` | ⬜ | |
| `REDIS_URL` | `/var/run/nutcracker/ratelimit.sock` | ⬜ | |
| `REDIS_POOL_SIZE` | `10` | ⬜ | |
| `REDIS_AUTH` | — | ⬜ | |
| `REDIS_AUTH_FILE` | — | ⬜ | |
| `REDIS_AWS_IAM_AUTH` | `false` | ⬜ | |
| `REDIS_AWS_IAM_CACHE_NAME` | — | ⬜ | |
| `REDIS_AWS_IAM_USER_ID` | — | ⬜ | |
| `REDIS_AWS_IAM_SERVERLESS` | `false` | ⬜ | |
| `REDIS_AWS_IAM_REGION` | — | ⬜ | |
| `REDIS_TLS` | `false` | ⬜ | |
| `REDIS_TLS_CLIENT_CERT` | — | ⬜ | |
| `REDIS_TLS_CLIENT_KEY` | — | ⬜ | |
| `REDIS_TLS_CACERT` | — | ⬜ | |
| `REDIS_TLS_SKIP_HOSTNAME_VERIFICATION` | `false` | ⬜ | |
| `REDIS_PIPELINE_WINDOW` | `0` | ⬜ | |
| `REDIS_PIPELINE_LIMIT` | `0` | ⬜ | |
| `REDIS_CLUSTER_PIPELINE_PARALLELISM` | `1` | ⬜ | |
| `REDIS_PERSECOND` | `false` | ⬜ | |
| `REDIS_PERSECOND_SOCKET_TYPE` | `unix` | ⬜ | |
| `REDIS_PERSECOND_TYPE` | `SINGLE` | ⬜ | |
| `REDIS_PERSECOND_URL` | `/var/run/nutcracker/ratelimitpersecond.sock` | ⬜ | |
| `REDIS_PERSECOND_POOL_SIZE` | `10` | ⬜ | |
| `REDIS_PERSECOND_AUTH` | — | ⬜ | |
| `REDIS_PERSECOND_AUTH_FILE` | — | ⬜ | |
| `REDIS_PERSECOND_AWS_IAM_AUTH` | `false` | ⬜ | |
| `REDIS_PERSECOND_AWS_IAM_CACHE_NAME` | — | ⬜ | |
| `REDIS_PERSECOND_AWS_IAM_USER_ID` | — | ⬜ | |
| `REDIS_PERSECOND_AWS_IAM_SERVERLESS` | `false` | ⬜ | |
| `REDIS_PERSECOND_AWS_IAM_REGION` | — | ⬜ | |
| `REDIS_PERSECOND_TLS` | `false` | ⬜ | |
| `REDIS_SENTINEL_AUTH` | — | ⬜ | |
| `REDIS_PERSECOND_SENTINEL_AUTH` | — | ⬜ | |
| `REDIS_PERSECOND_PIPELINE_WINDOW` | `0` | ⬜ | |
| `REDIS_PERSECOND_PIPELINE_LIMIT` | `0` | ⬜ | |
| `REDIS_PERSECOND_CLUSTER_PIPELINE_PARALLELISM` | `1` | ⬜ | |
| `REDIS_HEALTH_CHECK_ACTIVE_CONNECTION` | `false` | ⬜ | |
| `REDIS_CLOSE_CONNECTION_ON_READONLY_ERROR` | `false` | ⬜ | |
| `REDIS_TIMEOUT` | `10s` | ⬜ | |
| `REDIS_PERSECOND_TIMEOUT` | `10s` | ⬜ | |
| `REDIS_POOL_ON_EMPTY_BEHAVIOR` | `WAIT` | ⬜ | |
| `REDIS_PERSECOND_POOL_ON_EMPTY_BEHAVIOR` | `WAIT` | ⬜ | |
| `REDIS_STARTUP_INITIAL_INTERVAL` | `1s` | ⬜ | |
| `REDIS_STARTUP_MAX_INTERVAL` | `30s` | ⬜ | |
| `REDIS_STARTUP_MAX_ELAPSED_TIME` | `0` | ⬜ | |
| `MEMCACHE_HOST_PORT` | — | ⬜ | |
| `MEMCACHE_MAX_IDLE_CONNS` | `2` | ⬜ | |
| `MEMCACHE_SRV` | — | ⬜ | |
| `MEMCACHE_SRV_REFRESH` | `0` | ⬜ | |
| `MEMCACHE_TLS` | `false` | ⬜ | |
| `MEMCACHE_TLS_CLIENT_CERT` | — | ⬜ | |
| `MEMCACHE_TLS_CLIENT_KEY` | — | ⬜ | |
| `MEMCACHE_TLS_CACERT` | — | ⬜ | |
| `MEMCACHE_TLS_SKIP_HOSTNAME_VERIFICATION` | `false` | ⬜ | |
| `SHADOW_MODE` | `false` | ⬜ | |
| `QUOTA_MODE` | `false` | ⬜ | |
| `RESPONSE_DYNAMIC_METADATA` | `false` | 🚧 | behaviour: `tests/differential.rs`; not read from the environment yet |
| `MERGE_DOMAIN_CONFIG` | `false` | ⬜ | |
| `TRACING_ENABLED` | `false` | ⬜ | |
| `TRACING_SERVICE_NAME` | `RateLimit` | ⬜ | |
| `TRACING_SERVICE_NAMESPACE` | — | ⬜ | |
| `TRACING_SERVICE_INSTANCE_ID` | — | ⬜ | |
| `TRACING_EXPORTER_PROTOCOL` | `http` | ⬜ | |
| `TRACING_SAMPLING_RATE` | `1` | ⬜ | |
| `GRPC_SERVER_TLS_CA_CERT` | (injected by Envoy Gateway, read by no upstream code) | — ignored, as upstream | |
| `OTEL_EXPORTER_OTLP_*` | (read by the OpenTelemetry SDK) | ⬜ | |

## 2. YAML configuration

| Item | Status | Test |
|---|---|---|
| Recognised keys: `domain`, `key`, `value`, `descriptors`, `rate_limit`, `unit`, `requests_per_unit`, `unlimited`, `shadow_mode`, `quota_mode`, `name`, `replaces`, `detailed_metric`, `value_to_metric`, `share_threshold`, `metadata` | ✅ | `tests/config.rs` |
| Unknown key ⇒ config error (recursive allow-list, `metadata` excluded) | ✅ | `tests/config.rs` |
| Units `second` … `year` (case-insensitive), month = 30 d, year = 365 d | ✅ | `tests/config.rs`, `tests/differential.rs` |
| `unlimited` + `unit` ⇒ error; `requests_per_unit: 0` accepted | ✅ | `tests/config.rs` |
| `replaces`: empty name or self-replacement ⇒ error; names need not be unique | ✅ | `tests/config.rs` |
| Matching: exact `key_value`, then trailing **and middle** wildcards, then key only | ✅ | `tests/config.rs` |
| `share_threshold` (pattern in the Redis key and the stats key) | ✅ | `tests/config.rs`, `tests/differential.rs` |
| `detailed_metric`, `value_to_metric` (enriched stats key) | ✅ | `tests/config.rs` |
| Duplicate domain ⇒ error, unless `MERGE_DOMAIN_CONFIG`; duplicate composite key ⇒ error | ✅ | `tests/config.rs` |
| Reload: error ⇒ `config_load_error` + previous config kept | ⬜ | |
| Startup without a valid config ⇒ gRPC error on every call (no exit) | ⬜ | |
| File provider: goruntime, `SymlinkRefresher` / `DirectoryRefresher`, `runtime.*` scope | ⬜ | |
| xDS SotW provider (`RateLimitConfig`, ACK/NACK, backoff, mTLS) | ⬜ | |

## 3. gRPC and HTTP API

| Item | Status | Test |
|---|---|---|
| `envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit` (no v2) | ✅ | `tests/grpc.rs`, `tests/differential.rs` |
| Request `hits_addend` (0 ⇒ 1 unless a per-descriptor addend) and per-descriptor (0 = check only) | ✅ | `tests/differential.rs` |
| `is_negative_hits`: floor 0, always `OK`, TTL reset to the full duration | ✅ | `tests/differential.rs` |
| Client-sent `limit`: replaces the config, without shadow / `replaces` / `unlimited` | ✅ | `tests/config.rs`, `tests/differential.rs` |
| Response fields: `overall_code`, `statuses[]`, `current_limit` (with `name`), `limit_remaining`, `duration_until_reset` | ✅ | `tests/differential.rs` |
| Empty domain or empty list ⇒ error; unknown domain ⇒ `OK` + `domain_not_found` | 🚧 | `tests/service.rs`, `tests/differential.rs` (`domain_not_found` stat missing) |
| `LIMIT_*` headers: response, per unit, and request | ✅ | `tests/differential.rs` |
| Dynamic metadata (`RESPONSE_DYNAMIC_METADATA`): `domain`, `descriptors`, `hitsAddend`, merged `metadata` | ✅ | `tests/differential.rs`, `tests/service.rs` |
| `quota_mode`: `backend_name` / `model_name_override` groups, `OVER_LIMIT` when all exhausted, `passedBackends` per group | ✅ | `tests/service.rs`, `tests/differential.rs` |
| `replaces`: two passes, not transitive | ✅ | `tests/service.rs`, `tests/differential.rs` |
| Per-rule and global shadow mode (`global_shadow_mode`) | 🚧 | `tests/service.rs`, `tests/differential.rs` (`global_shadow_mode` stat missing) |
| `POST /json`: protojson, 200 / 429 / 400 / 500 | ⬜ | |
| `/healthcheck`: 500 with no body when unhealthy; SIGTERM ⇒ unhealthy + graceful shutdown | ⬜ | |
| `grpc.health.v1`, service `ratelimit` | ⬜ | |
| Debug port: `/`, `/rlconfig` (`Dump()` format), `/stats`, `/debug/pprof/*` | ⬜ | |
| `GRPC_UDS`, max connection age, client mTLS + SAN, server certificate hot reload | ⬜ | |

## 4. Algorithm and storage

| Item | Status | Test |
|---|---|---|
| Key `prefix + domain_key_value_…_bucketStart`, epoch-aligned; UTC calendar month | 🚧 | `tests/differential.rs` (calendar month missing) |
| Redis: `INCRBY` + `EXPIRE (unit + jitter)` pipeline | 🚧 | `tests/differential.rs` (jitter missing) |
| `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT`: `GET` pipeline then `INCRBY`, siblings at 0, ghost keys | ✅ | `tests/differential.rs` |
| Local cache of over-limit keys (set before the shadow check, TTL = unit), freecache's size limits and eviction | ✅ | `tests/differential.rs`, `tests/freecache.rs` |
| `near_limit` threshold in `float32`; near / over split of `hitsAddend` | ⬜ | |
| `REDIS_PERSECOND` (routing of `second` keys) | ⬜ | |
| Redis single / sentinel / cluster, TLS, AUTH (file), AWS IAM, startup retry | ⬜ | |
| Memcached: `GET` then asynchronous increments, `add` when absent, SRV, TLS, absolute expiry > 30 d, key ≤ 250 B | ⬜ | |

## 5. Metrics, logs, traces

| Item | Status | Test |
|---|---|---|
| Per key: `total_hits`, `over_limit`, `near_limit`, `over_limit_with_local_cache`, `within_limit`, `shadow_mode`, `total_negative_hits` | ⬜ | |
| `domain_not_found`, `global_shadow_mode`, `config_load_success`, `config_load_error` | ⬜ | |
| `call.should_rate_limit.redis_error` / `service_error` | ⬜ | |
| `ratelimit_server.<method>.total_requests` / `response_time` | ⬜ | |
| `redis_pool.cx_*`, `localcache.*`, `memcache.*`, `runtime.*` | ⬜ | |
| Exclusive sinks: statsd over **TCP** (default), DogStatsD (+ mogrifiers), native Prometheus + statsd_exporter mapper, log | ⬜ | |
| `SanitizeStatName`, `EXTRA_TAGS`, `STATS_FLUSH_INTERVAL` | ⬜ | |
| `LOG_FORMAT=json` (`@timestamp`, `@message`) | ⬜ | |
| OTLP traces: spans `ShouldRateLimit Execution`, `Redis Pipeline Execution` and their attributes | ⬜ | |

## 6. Image and deployment

| Item | Status | Test |
|---|---|---|
| Binary `/bin/ratelimit`, distroless without a shell, amd64 + arm64 | ⬜ | |
| Any UID (65534), read-only FS, no capabilities | ⬜ | |
| Ports 8080 / 8081 / 6070, `RUNTIME_ROOT=/srv/runtime_data/current` | ⬜ | |
| Dual-stack listening (`HOST="::"`), `SO_REUSEPORT` | ⬜ | |
| `/healthcheck` served after the initial Redis connection | ⬜ | |
| Deployment generated by Envoy Gateway (xDS TLS) works by changing only the image | ⬜ | |

## 7. Deliberate divergences

| Upstream behaviour | Our behaviour | Reason |
|---|---|---|
| Client `limit` with `unit: UNKNOWN` ⇒ the process panics | gRPC error `INVALID_ARGUMENT` | D1: crashes are not reproduced |
| Invalid environment variable re-read on reload ⇒ panic | config error | D1 |
| `descriptors[].limit` in metadata as Go prototext, one or two spaces between fields depending on the build | the same text, always one space; `unit_multiplier:{value:N}` when set | Go randomises the spacing; `unit_multiplier` is an extension |
| Merging `metadata` writes into the configuration's own structs: a nested struct merged once keeps the other limit's fields in later responses, and concurrent requests race on it | merged into copies | D1: upstream bug; responses only differ once the configuration has been altered |
| `unit_multiplier` ignored in overrides and absent from responses | read, applied, echoed; 0 rejected | extension (current Envoy proto) |
| Go stats (`go.*`), `/stats` expvar, pprof | Rust process stats, documented equivalents | no Rust equivalent |
| A refund has no effect on the local cache | clears the local entry (new types only) | D18 |
| YAML syntax error text and line (Go's libyaml port) | the Rust parser's wording and position | different parser; the message structure is kept |
| `invalid map key` message escapes strings with Go's `%#v` | Rust escaping for unusual characters | only differs for non-ASCII or control characters |
| `!!binary` decoding to invalid UTF-8 ⇒ `error parsing metadata` | decoded lossily | Rust strings are UTF-8 |

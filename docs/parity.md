# Parity inventory with envoy ratelimit

Reference: `envoyproxy/ratelimit` at commit `f4e7669` (2026-10-06). Rule (D3): the first version ships only when **every line** is `✅` with a test proving it, or `≠` (deliberate, justified divergence). Deliberate divergences are listed at the end.

Status legend: `⬜` to do · `🚧` in progress · `✅` done and proven · `≠` deliberate divergence · `—` ignored, as upstream.

Section 1 is **generated** from `src/settings/settings.go` (134 variables); regenerate it whenever the reference is upgraded.

## 1. Environment variables

Reading them (values, defaults, set-but-empty values, the first error and its text, including the `<SETTING>_<FIELD>` variables envconfig reads for each untagged `*tls.Config` setting, such as `REDISTLSCONFIG_MINVERSION`) is proven for every variable by `settings_are_read_like_upstream` in `tests/differential.rs`, against upstream's `settings.NewSettings()`. The rows below track what each one does.

| Variable | Upstream default | Status | Test |
|---|---|---|---|
| `HOST` | `0.0.0.0` | ✅ | `tests/differential.rs` (`the_healthcheck_answers_like_upstream`), listening like `GRPC_HOST` |
| `PORT` | `8080` | ✅ | `tests/differential.rs` (`the_healthcheck_answers_like_upstream`, `the_binary_*`) |
| `DEBUG_HOST` | `0.0.0.0` | ✅ | `tests/differential.rs` (`the_debug_port_answers_like_upstream`) |
| `DEBUG_PORT` | `6070` | ✅ | `tests/differential.rs` (`the_debug_port_answers_like_upstream`, `a_busy_debug_port_is_ignored_like_upstream`) |
| `GRPC_UDS` | — | ✅ | `the_binary_serves_on_a_unix_socket_like_upstream` in `tests/differential.rs` |
| `GRPC_HOST` | `0.0.0.0` | ✅ | `tests/differential.rs` (`the_binary_*`), listening like Go (dual-stack wildcard, `SO_REUSEPORT`) |
| `GRPC_PORT` | `8081` | ✅ | `tests/differential.rs` (`the_binary_*`); a busy port fails like upstream |
| `GRPC_MAX_CONNECTION_AGE` | `24h` | ✅ | applied to the gRPC server (tonic's keepalive settings) |
| `GRPC_MAX_CONNECTION_AGE_GRACE` | `1h` | ✅ | applied to the gRPC server (tonic's keepalive settings) |
| `GRPC_SERVER_USE_TLS` | `false` | ⬜ | |
| `GRPC_SERVER_TLS_CERT` | — | ⬜ | |
| `GRPC_SERVER_TLS_KEY` | — | ⬜ | |
| `GRPC_CLIENT_TLS_CACERT` | — | ⬜ | |
| `GRPC_CLIENT_TLS_SAN` | — | ⬜ | |
| `LOG_LEVEL` | `WARN` | 🚧 | an invalid value fails at startup like upstream (`the_binary_fails_to_start_like_upstream`); logging itself not yet |
| `LOG_FORMAT` | `text` | ⬜ | |
| `CONFIG_TYPE` | `FILE` | 🚧 | `FILE`, and an invalid value: `the_binary_fails_to_start_like_upstream`; `GRPC_XDS_SOTW` not supported yet |
| `FORCE_START_WITHOUT_INITIAL_CONFIG` | `false` | ✅ | the service waits for the file provider's first load unless set (the end state is the same, as upstream) |
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
| `USE_DOG_STATSD` | `false` | ✅ | behaviour: `tests/differential.rs`, `tests/dogstatsd.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `USE_DOG_STATSD_MOGRIFIERS` | — | ✅ | behaviour: `tests/differential.rs`, `tests/dogstatsd.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `DOG_STATSD_MOGRIFIER_<NAME>_PATTERN`, `_NAME`, `_TAGS` | — | ✅ | behaviour: `tests/differential.rs`, `tests/dogstatsd.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `DD_ENTITY_ID`, `DD_ENV`, `DD_SERVICE`, `DD_VERSION`, `DD_ORIGIN_DETECTION_ENABLED` | — (read by datadog-go, not by `settings.go`) | ✅ | behaviour: `tests/differential.rs`, `tests/dogstatsd.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `USE_STATSD` | `true` | ✅ | behaviour: `tests/stats.rs` (selection); read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `STATSD_HOST` | `localhost` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `STATSD_PORT` | `8125` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `EXTRA_TAGS` | — | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) (a value containing `:` makes upstream panic at startup) |
| `STATS_FLUSH_INTERVAL` | `10s` | ✅ | behaviour: `tests/stats.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `DISABLE_STATS` | `false` | ✅ | behaviour: `tests/stats.rs` (selection); read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `STATSD_PROTOCOL` | `tcp` (read by gostats, not by `settings.go`) | ✅ | behaviour (`tcp`, `udp`): `tests/differential.rs`, `tests/stats.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) (gostats also accepts `udp4`, `udp6` and any other network name) |
| `GOSTATS_FLUSH_INTERVAL_SECONDS`, `GOSTATS_LOGGING_SINK_DISABLED` | `5`, `false` (read by gostats, not by `settings.go`) | ✅ | only checked (an invalid value fails when the statsd sink is created), as upstream: `tests/differential.rs` |
| `SANITIZE_DESCRIPTOR_METRIC_DOTS` | `false` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `USE_PROMETHEUS` | `false` | ✅ | behaviour: `tests/stats.rs`, `tests/prometheus.rs` (selection); read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `PROMETHEUS_ADDR` | `:9090` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `PROMETHEUS_PATH` | `/metrics` | ✅ | behaviour: `tests/differential.rs`, `tests/prometheus.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `PROMETHEUS_MAPPER_YAML` | — | ✅ | behaviour: `tests/differential.rs`, `tests/prometheus.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `PROMETHEUS_RESPONSE_TIME_AS_MILLISECONDS` | `false` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `RUNTIME_ROOT` | `/srv/runtime_data/current` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `RUNTIME_SUBDIRECTORY` | — | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `RUNTIME_APPDIRECTORY` | `config` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `RUNTIME_IGNOREDOTFILES` | `false` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `RUNTIME_WATCH_ROOT` | `true` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `EXPIRATION_JITTER_MAX_SECONDS` | `300` | ⬜ | |
| `LOCAL_CACHE_SIZE_IN_BYTES` | `0` | ✅ | behaviour: section 4; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `NEAR_LIMIT_RATIO` | `0.8` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `CACHE_KEY_PREFIX` | — | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `BACKEND_TYPE` | `redis` | 🚧 | `redis`, and an invalid value: `the_binary_fails_to_start_like_upstream`; `memcache` not supported yet |
| `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` | `false` | ✅ | behaviour: section 4; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `USE_CALENDAR_MONTH_RATE_LIMIT` | `false` | ⬜ | |
| `LIMIT_RESPONSE_HEADERS_ENABLED` | `false` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `LIMIT_LIMIT_HEADER` | `RateLimit-Limit` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `LIMIT_REMAINING_HEADER` | `RateLimit-Remaining` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `LIMIT_RESET_HEADER` | `RateLimit-Reset` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `LIMIT_PER_UNIT_HEADERS_ENABLED` | `false` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `LIMIT_REQUEST_HEADERS_ENABLED` | `false` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `LIMIT_REQUEST_LIMIT_HEADER` | `RateLimit-Limit` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `LIMIT_REQUEST_REMAINING_HEADER` | `RateLimit-Remaining` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `LIMIT_REQUEST_RESET_HEADER` | `RateLimit-Reset` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `HEALTHY_WITH_AT_LEAST_ONE_CONFIG_LOADED` | `false` | ✅ | `health_is_reported_like_upstream` in `tests/differential.rs` |
| `REDIS_SOCKET_TYPE` | `unix` | ✅ | `tcp`: `tests/differential.rs`; `unix`: `tests/runner.rs` |
| `REDIS_TYPE` | `SINGLE` | 🚧 | `SINGLE`, and an invalid value: `tests/runner.rs`, `the_binary_fails_to_start_like_upstream`; `CLUSTER`, `SENTINEL` not supported yet |
| `REDIS_URL` | `/var/run/nutcracker/ratelimit.sock` | ✅ | `tests/differential.rs`; `redis://` URLs (user, password, database): `tests/runner.rs` |
| `REDIS_POOL_SIZE` | `10` | ✅ | `tests/runner.rs` (0 is radix's 4) |
| `REDIS_AUTH` | — | ✅ | `user:password` or a password: `tests/runner.rs` |
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
| `REDIS_HEALTH_CHECK_ACTIVE_CONNECTION` | `false` | ✅ | `redis_connections_drive_health_like_upstream` in `tests/differential.rs` |
| `REDIS_CLOSE_CONNECTION_ON_READONLY_ERROR` | `false` | ⬜ | |
| `REDIS_TIMEOUT` | `10s` | ✅ | the connection timeout, as radix's dialer: `tests/runner.rs` |
| `REDIS_PERSECOND_TIMEOUT` | `10s` | ⬜ | |
| `REDIS_POOL_ON_EMPTY_BEHAVIOR` | `WAIT` | ✅ | `CREATE`/`ERROR` refused at startup: `tests/runner.rs`, `the_binary_fails_to_start_like_upstream` |
| `REDIS_PERSECOND_POOL_ON_EMPTY_BEHAVIOR` | `WAIT` | ⬜ | |
| `REDIS_STARTUP_INITIAL_INTERVAL` | `1s` | ✅ | `tests/runner.rs` |
| `REDIS_STARTUP_MAX_INTERVAL` | `30s` | ✅ | `tests/runner.rs` |
| `REDIS_STARTUP_MAX_ELAPSED_TIME` | `0` | ✅ | `the_binary_fails_to_start_like_upstream` |
| `MEMCACHE_HOST_PORT` | — | ⬜ | |
| `MEMCACHE_MAX_IDLE_CONNS` | `2` | ⬜ | |
| `MEMCACHE_SRV` | — | ⬜ | |
| `MEMCACHE_SRV_REFRESH` | `0` | ⬜ | |
| `MEMCACHE_TLS` | `false` | ⬜ | |
| `MEMCACHE_TLS_CLIENT_CERT` | — | ⬜ | |
| `MEMCACHE_TLS_CLIENT_KEY` | — | ⬜ | |
| `MEMCACHE_TLS_CACERT` | — | ⬜ | |
| `MEMCACHE_TLS_SKIP_HOSTNAME_VERIFICATION` | `false` | ⬜ | |
| `SHADOW_MODE` | `false` | ✅ | `tests/service.rs`, `tests/differential.rs`; read by the binary: `tests/settings.rs` |
| `QUOTA_MODE` | `false` | ✅ | `tests/service.rs`, `tests/differential.rs`; read by the binary: `tests/settings.rs` |
| `RESPONSE_DYNAMIC_METADATA` | `false` | ✅ | behaviour: `tests/differential.rs`; read by the binary: `tests/settings.rs`, `tests/differential.rs` (`the_binary_*`) |
| `MERGE_DOMAIN_CONFIG` | `false` | ✅ | `tests/config.rs`; read by the binary: `tests/settings.rs` |
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
| Reload: error ⇒ `config_load_error` + previous config kept | ✅ | `tests/differential.rs` |
| YAML 1.1 input as yaml.v2 reads it: control characters refused, byte order marks (UTF-8, UTF-16) read, NEL as a line break | ✅ | `tests/differential.rs` |
| Quoted scalars continued on less indented lines: accepted by yaml.v2, refused by the Rust parser | ⬜ | |
| Startup without a valid config ⇒ gRPC error on every call (no exit) | ✅ | the service answers errors until a config loads: `tests/differential.rs` (a bad file at start) |
| File provider: goruntime's loader (snapshot keys, dotfiles, symlinks, failures, nil loader), `SymlinkRefresher` / `DirectoryRefresher` on inotify events, reload coalescing, `runtime` scope | ✅ | snapshots and the first load: `tests/differential.rs`; reloads: `tests/differential.rs` on Linux (inotify, as upstream's fsnotify) |
| xDS SotW provider (`RateLimitConfig`, ACK/NACK, backoff, mTLS) | ⬜ | |

## 3. gRPC and HTTP API

| Item | Status | Test |
|---|---|---|
| `envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit` (no v2) | ✅ | `tests/grpc.rs`, `tests/differential.rs` |
| Request `hits_addend` (0 ⇒ 1 unless a per-descriptor addend) and per-descriptor (0 = check only) | ✅ | `tests/differential.rs` |
| `is_negative_hits`: floor 0, always `OK`, TTL reset to the full duration | ✅ | `tests/differential.rs` |
| Client-sent `limit`: replaces the config, without shadow / `replaces` / `unlimited` | ✅ | `tests/config.rs`, `tests/differential.rs` |
| Response fields: `overall_code`, `statuses[]`, `current_limit` (with `name`), `limit_remaining`, `duration_until_reset` | ✅ | `tests/differential.rs` |
| Empty domain or empty list ⇒ error; unknown domain ⇒ `OK` + `domain_not_found` | ✅ | `tests/service.rs`, `tests/differential.rs` |
| `LIMIT_*` headers: response, per unit, and request | ✅ | `tests/differential.rs` |
| Dynamic metadata (`RESPONSE_DYNAMIC_METADATA`): `domain`, `descriptors`, `hitsAddend`, merged `metadata` | ✅ | `tests/differential.rs`, `tests/service.rs` |
| `quota_mode`: `backend_name` / `model_name_override` groups, `OVER_LIMIT` when all exhausted, `passedBackends` per group | ✅ | `tests/service.rs`, `tests/differential.rs` |
| `replaces`: two passes, not transitive | ✅ | `tests/service.rs`, `tests/differential.rs` |
| Per-rule and global shadow mode (`global_shadow_mode`) | ✅ | `tests/service.rs`, `tests/differential.rs` |
| `/json` (any method): protojson, 200 / 429 / 400 / 500 | ✅ | `tests/differential.rs` (`json_requests_are_decoded_like_upstream` against `protojson.Unmarshal`, `json_is_served_like_upstream`) |
| SIGINT, SIGTERM, SIGHUP ⇒ gRPC stopped gracefully, exit status 0 (during the Redis startup wait: an error) | ✅ | `the_binary_stops_on_sigterm_like_upstream` in `tests/differential.rs` |
| `/healthcheck`: 500 with no body when unhealthy, unhealthy once shutting down; gorilla/mux routing (clean-path 301, 404); what Go's server answers itself (bad escapes, `Host` checks, `OPTIONS *`) | ✅ | `tests/differential.rs` (`the_healthcheck_answers_like_upstream`, `the_http_server_reads_requests_like_upstream`, `sigterm_turns_health_off_like_upstream`) |
| `grpc.health.v1`, service `ratelimit` | ✅ | `tests/differential.rs` (`health_is_reported_like_upstream`, `sigterm_turns_health_off_like_upstream`), `tests/health.rs` |
| Debug port: `/`, `/rlconfig` (`Dump()` format), `/stats`, `/debug/pprof/*`; Go's `ServeMux` (307 redirects, escaped paths), sniffed content types; a port it cannot listen on is not fatal | ✅ | `tests/differential.rs` (`the_debug_port_answers_like_upstream`, `bodies_are_sniffed_and_strings_quoted_like_go`, `a_busy_debug_port_is_ignored_like_upstream`) |
| `GRPC_UDS` | ✅ | `tests/differential.rs` |
| Max connection age | ✅ | the gRPC server's keepalive settings |
| Client mTLS + SAN, server certificate hot reload | ⬜ | |

## 4. Algorithm and storage

| Item | Status | Test |
|---|---|---|
| Key `prefix + domain_key_value_…_bucketStart`, epoch-aligned; UTC calendar month | 🚧 | `tests/differential.rs` (calendar month missing) |
| Redis: `INCRBY` + `EXPIRE (unit + jitter)` pipeline | 🚧 | `tests/differential.rs` (jitter missing) |
| `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT`: `GET` pipeline then `INCRBY`, siblings at 0, ghost keys | ✅ | `tests/differential.rs` |
| Local cache of over-limit keys (set before the shadow check, TTL = unit), freecache's size limits and eviction | ✅ | `tests/differential.rs`, `tests/freecache.rs` |
| `near_limit` threshold in `float32`; near / over split of `hitsAddend` | ✅ | `tests/differential.rs` |
| `REDIS_PERSECOND` (routing of `second` keys) | ⬜ | |
| Redis single (TCP or Unix socket, `redis://` URL, AUTH, pool, startup retry with backoff) | ✅ | `tests/runner.rs`, `tests/differential.rs` (`the_binary_*`) |
| Redis sentinel / cluster, TLS, AUTH file, AWS IAM, close on `READONLY` | ⬜ | |
| Memcached: `GET` then asynchronous increments, `add` when absent, SRV, TLS, absolute expiry > 30 d, key ≤ 250 B | ⬜ | |

## 5. Metrics, logs, traces

| Item | Status | Test |
|---|---|---|
| Per key: `total_hits`, `over_limit`, `near_limit`, `over_limit_with_local_cache`, `within_limit`, `shadow_mode`, `total_negative_hits` | ✅ | `tests/differential.rs` |
| `domain_not_found`, `global_shadow_mode`, `config_load_success`, `config_load_error` | ✅ | `tests/differential.rs` |
| `call.should_rate_limit.redis_error` / `service_error` | ✅ | `tests/differential.rs` |
| `ratelimit_server.<method>.total_requests` / `response_time` (health `Check` and `List` included) | ⬜ | |
| `runtime.load_attempts`, `runtime.load_failures` (with `EXTRA_TAGS`, `reserved_tag` counted again) | ✅ | `tests/differential.rs` |
| `redis_pool.cx_*`, `localcache.*`, `memcache.*`, `runtime.num_values` (gauges) | ⬜ | |
| Exclusive sinks: selection and its fatal error, statsd over **TCP** (default) or UDP, log | ✅ | statsd over TCP: `tests/differential.rs`; selection, UDP datagrams, dropping, reconnecting, log lines: `tests/stats.rs` |
| DogStatsD (+ mogrifiers, global tags, origin detection) | ✅ | lines and tags: `tests/differential.rs`; datagrams, `int64` values, mogrifier rules and errors, origin detection: `tests/dogstatsd.rs` |
| Native Prometheus + statsd_exporter mapper: counters, gauges, histograms, TTLs, self-metrics, scrape answers | ✅ | mapper (loading, globs, regexes, templates) against statsd_exporter itself, series, text format, Gather errors, HTTP answers: `tests/differential.rs`; what panics upstream: `tests/prometheus.rs` |
| Prometheus summaries (observers not mapped to a histogram) | ⬜ | upstream's only timer, `response_time`, is a histogram with the default mapper |
| `SanitizeStatName` | ✅ | `tests/differential.rs` |
| `EXTRA_TAGS`, `STATS_FLUSH_INTERVAL` | ✅ | `EXTRA_TAGS`, `reserved_tag`: `tests/differential.rs`, `tests/stats.rs`; flush interval: `tests/stats.rs` |
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
| An invalid environment variable (or `DOG_STATSD_MOGRIFIER_*`, or gostats' own checks) ⇒ panic at startup | an error with the same text | D1 |
| A variable whose value is not valid UTF-8 keeps its bytes (and Go's quoting shows them as `\xNN` in errors) | read with U+FFFD in place of invalid sequences | Rust strings are UTF-8 |
| `descriptors[].limit` in metadata as Go prototext, one or two spaces between fields depending on the build | the same text, always one space; `unit_multiplier:{value:N}` when set | Go randomises the spacing; `unit_multiplier` is an extension |
| Merging `metadata` writes into the configuration's own structs: a nested struct merged once keeps the other limit's fields in later responses, and concurrent requests race on it | merged into copies | D1: upstream bug; responses only differ once the configuration has been altered |
| `unit_multiplier` ignored in overrides and absent from responses | read, applied, echoed; 0 rejected | extension (current Envoy proto) |
| Go stats (`go.*`) | Rust process stats, documented equivalents | no Rust equivalent |
| The debug port's `/stats` prints expvar's `cmdline` and `memstats` (Go's `runtime.MemStats`) | `cmdline` only | No Rust equivalent; zeros or other figures under that name would mislead its readers |
| pprof serves the Go runtime's profiles (`goroutine`, `heap`, `allocs`, `block`, `mutex`, `threadcreate`, `goroutineleak`) and an HTML index at `/debug/pprof/`, CPU profiles and traces at `/debug/pprof/profile` and `/debug/pprof/trace` | `404 Unknown profile` for those names and the index (pprof's answer for any other name); `500 Could not enable CPU profiling: not available` and `500 Could not enable tracing: not available` | No Go runtime to profile; pprof's own error answers |
| `/rlconfig` lists domains and descriptors in Go's random map order | sorted by domain, then by key at each level | Deterministic |
| The Prometheus registry also holds Go's `go_*` and `process_*` collectors (a mapped name colliding with them makes scrapes fail) | not exposed | no Rust equivalent; see the row above |
| Go's default mux also serves `/debug/pprof/*` and `/debug/vars` on `PROMETHEUS_ADDR` (405 for other methods) | 404 | the same pprof and expvar question as above |
| A `PROMETHEUS_PATH` Go's mux panics on (not a clean absolute path), or reads as a method, a host or wildcards | error at startup; method, host and wildcard patterns are not supported | D1; not needed by the targeted deployments |
| A Prometheus mapper file using the top-level `buckets` or `quantiles` in a way statsd_exporter warns about, or setting `registerer`, `mappingscount` or `logger`, makes upstream panic at startup (its mapper has no logger) | error at startup | D1 |
| A histogram series with an `le` label or buckets not in increasing order makes upstream's exporter panic, which stops the process | the event counts in `statsd_exporter_events_conflict_total` | D1 |
| A scrape asking for protobuf gets it | the text format (with the same `Content-Type` parameters) | Prometheus asks for text unless native histograms are enabled |
| When a Prometheus scrape fails, Go gathers concurrently: the order of several errors, and which of two help texts counts as expected, are random; the messages print metrics as Go prototext with random spacing and their creation time | registration order; single spaces, no creation time; Go's `%q` escaping of non-printable non-ASCII characters is not reproduced | deterministic |
| The text format sorts series with the same label values but different label names in a random order; gzip output is Go's | by label names; the same text, compressed by `flate2` | deterministic; the decompressed body is identical |
| A Prometheus mapper template with `%` directives using flags, widths or precisions (e.g. `%5s`) next to a `$n` is formatted by Go's `fmt` | printed as an unknown verb (`%!5(string=…)`) | not worth porting `fmt`; `%s`, `%v`, `%q`, `%x`, `%X`, `%T`, other verbs, `%%` and missing or extra arguments match |
| `PROMETHEUS_ADDR` with a service name for its port (`:http`) listens on that port | nothing listens | not needed by the targeted deployments |
| An `Accept` header with more than 12 clauses of equal weight is ordered by Go's pdqsort | kept in order | the order only matters for the `escaping` parameter |
| Off Linux, fsnotify watches the runtime directory with kqueue | polled every 100 ms (development only: the image is Linux, where both use inotify) | not a deployment target |
| A runtime directory that cannot be watched: the error names Go's errno (`no such file or directory (syscall.Errno 0x2)`) | the same form, with the C library's text for the errno | matches glibc's texts for common errors |
| U+2028 and U+2029 also break lines in YAML 1.1 (kept as characters in scalars) | characters only | the Rust parser is YAML 1.2; converting them would change scalars |
| yaml.v2 reads only the first YAML document of a file: a control character or invalid byte after a later `---` (and beyond its 512-byte read-ahead) is not seen; and it decodes as it parses, so a syntax error before a bad byte further on is reported first | the whole file is checked first | rare inputs; the file is refused either way in the second case |
| A snapshot's files are loaded in Go's random map order (which decides which error is reported, or the merge order) | in key order | one of upstream's orders, deterministic |
| A startup failure is a logrus `level=fatal` line (exit status 1) or a Go panic with its stack trace (exit status 2) | the message alone on stderr, exit status 1 | the message is the same; the log format comes with `LOG_FORMAT` |
| radix opens one Redis connection at startup and the rest in the background | all the pool's connections at startup | the same once connected; a pool partly reachable at startup is not a target |
| Redis connection errors read as Go's `net` errors (`dial tcp 127.0.0.1:1: connect: connection refused`) | as fred's | different libraries; the messages around them are upstream's |
| `GRPC_MAX_CONNECTION_AGE` gets ±10% of jitter (grpc-go) | applied as it is (tonic) | spreads reconnections only |
| A `redis://` URL's `password` and `username` query parameters appear in startup messages (upstream masks only the user info) | masked too | keeps secrets out of logs |
| A refund has no effect on the local cache | clears the local entry (new types only) | D18 |
| YAML syntax error text and line (Go's libyaml port) | the Rust parser's wording and position | different parser; the message structure is kept |
| `invalid map key` message escapes strings with Go's `%#v` | Rust escaping for unusual characters | only differs for non-ASCII or control characters |
| `!!binary` decoding to invalid UTF-8 ⇒ `error parsing metadata` | decoded lossily | Rust strings are UTF-8 |
| Negative or NaN `NEAR_LIMIT_RATIO`: the near threshold is converted to an integer by the CPU (amd64 wraps a negative value, arm64 gives 0) | 0 | Go leaves out-of-range float conversions to the platform |
| datadog-go sends its own telemetry every 10 s (`datadog.dogstatsd.client.*`, tagged `client:go`, `client_version:5.4.0` and the global tags) | not sent | a Rust client reporting as `client:go` would mislead; the agent only uses it for client health |
| A mogrifier's tags come in Go's random map order | sorted by tag name | deterministic |
| DogStatsD lines are packed by 32 workers, by name hash | one buffer: the same lines, in other datagrams | datagram boundaries carry no meaning for the agent |
| Mogrifier `PATTERN`s are Go RE2 | Rust `regex`, with `\d`, `\w`, `\s`, `\b` and their negations made ASCII as in Go; syntax only one of the two accepts (e.g. `\Q…\E`, `\b{start}`) differs | different engine |
| An invalid mogrifier ⇒ panic at startup | configuration error | D1 |
| Error texts: an invalid mogrifier `PATTERN` (after `failed to compile pattern for <NAME>: <PATTERN>: `), a DogStatsD address that does not resolve or cannot be dialled (e.g. port 0) | the Rust regex and resolver texts | different libraries; the message prefixes are kept |
| A `STATSD_HOST` starting with `unix://` sends DogStatsD over a Unix socket | not supported: the address does not resolve | not needed by the targeted deployments; can be added |
| The statsd sink logs Go `net` errors (`connection error: dial tcp 127.0.0.1:8125: connect: connection refused`) | the same messages with the operating system's error text | Rust has no `net.OpError`; the message prefixes are kept |
| After a failed write to statsd, gostats resends a buffer from the first unwritten byte, so the new connection can start mid-line | resends from the start of the first line not fully written | D1: a corrupt line is a bug |
| statsd buffers are cut at the same line boundaries as gostats' but are not timed alike (gostats also pushes its buffer every second), so while the server is unreachable which lines are dropped can differ; the queue is bounded alike (about 4 MiB) | buffers pushed only when full or flushed | Not observable while the server is reachable |
| Go's server refuses some requests itself with a `400` closed by the connection (no `Content-Length`); among them a request line it cannot parse (e.g. a raw space in the target) or an invalid header name or value | the same answers with a `Content-Length` for a bad `%` escape and a missing or malformed `Host`; hyper's empty `400` for the others | Different HTTP server; both refuse the request, and the bodies read alike |
| A gRPC health `Watch` stream that falls behind gets only the latest status (grpc-go keeps one pending update) | every change in order, duplicates skipped; past 64 pending changes the oldest are dropped | The final status is the same |
| On every HTTP port, a request target with a fragment (`/healthcheck#x`) keeps it in the path (and the query), so it is not found | the fragment is dropped (hyper's URI parser) | Clients do not send fragments |
| On the debug and Prometheus ports, an absolute target without a path (`GET http://h`) is redirected to `/` | served as `/` | hyper gives such a target the path `/` |
| With `REDIS_HEALTH_CHECK_ACTIVE_CONNECTION`, a lost connection is noticed when radix next uses it (a command, or its PING every 5 s / pool size) | as soon as the socket closes | Earlier, not different: the same states in the same order |
| `/json` refuses `unitMultiplier` (or `unit_multiplier`, even `null`) in a client limit with `400`: Go's type has no such field | read and applied as over gRPC, echoed in the metadata's limit | Extension (see `unit_multiplier` above): requests upstream refuses, none it accepts |
| On `/json`, a client limit with an unknown unit (none, `0`, a negative number, or one above `7`) makes the service panic, and Go's server closes the connection with no answer | `400 Bad Request` | D1 |
| `/json` bodies over 2048 bytes are chunked (HTTP/1.1) or ended by closing the connection (HTTP/1.0), with no `Content-Length` | always a `Content-Length` | Different HTTP server; the bodies read alike |
| `/json` answers with a space after every comma, or after none, depending on the build (protojson's `detrand`) | never | Go randomises it on purpose |
| `/json` reads a body of any length | a body over 4 MiB (grpc-go's largest message) gets `400 Bad Request` | D1: an unbounded body can exhaust the memory and get the process killed |

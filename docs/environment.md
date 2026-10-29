# Environment variables

The service is configured by environment variables only. They have the same names, defaults and parsing as envoyproxy/ratelimit at commit `f4e7669`, so an existing deployment keeps its variables. This page lists every one of them, then the variables of the libraries the service reproduces (AWS, OpenTelemetry, gRPC, statsd, DogStatsD).

The rate limit rules themselves are YAML files, described in [Configuration](configuration.md). What is proven against upstream, variable by variable, is in [parity.md §1](parity.md#1-environment-variables). Where this service deliberately behaves differently, the row says so and links [parity.md §7](parity.md#7-deliberate-divergences).

## A minimal deployment

Redis over TCP, rules reloaded when a file changes, metrics for Prometheus:

```sh
REDIS_SOCKET_TYPE=tcp
REDIS_URL=redis:6379
RUNTIME_ROOT=/data
RUNTIME_SUBDIRECTORY=ratelimit
RUNTIME_WATCH_ROOT=false
USE_STATSD=false
USE_PROMETHEUS=true
LOG_LEVEL=info
```

The rules are then read from every file in `/data/ratelimit/config/`. gRPC listens on 8081, HTTP on 8080, the debug port on 6070, and Prometheus on 9090 at `/metrics`.

## How values are read

- **Unset and empty are different.** A variable set to an empty value counts as set: it replaces the default. An empty boolean, integer or duration is an error, and an empty string is used as it is.
- **Booleans** are Go's `strconv.ParseBool`: `1`, `t`, `T`, `TRUE`, `true`, `True` for true; `0`, `f`, `F`, `FALSE`, `false`, `False` for false. Anything else, `yes` and `on` included, is an error.
- **Integers** are Go's, in any base: `8080`, `0x1F90`, `0o17620`, `0b1`, with `_` between digits. A leading `0` means octal, so `08080` is an error.
- **Durations** are Go durations: a sign, then numbers with the units `ns`, `us` (or `µs`), `ms`, `s`, `m`, `h`, such as `10s`, `1m30s`, `250ms`. A bare `0` is accepted; other numbers need a unit.
- **Floats** are Go's `strconv.ParseFloat`: `0.8`, `1`, `1e-1`.
- **Lists** are split on `,`, without trimming spaces.
- **Maps** are `key:value` pairs separated by `,`. Each pair must have exactly one `:`, so a value cannot contain `:`.

The first invalid value stops the service at startup, with upstream's error text (`envconfig.Process: assigning <NAME> to <Field>: converting '<value>' to type <type>. details: ...`). Upstream panics there; this service writes a `level=fatal` line and exits with status 1 ([§7](parity.md#7-deliberate-divergences)).

Secrets go only in `REDIS_AUTH`, `REDIS_PERSECOND_AUTH`, `REDIS_SENTINEL_AUTH`, `REDIS_PERSECOND_SENTINEL_AUTH`, the files named by `REDIS_AUTH_FILE` and `REDIS_PERSECOND_AUTH_FILE`, the user info of a `redis://` URL, the TLS key files, and the AWS and OpenTelemetry variables. Prefer the `*_AUTH_FILE` variables: the file is read again at each connection, so a rotated secret needs no restart.

## Server and ports

| Variable | Default | Description |
|---|---|---|
| `HOST` | `0.0.0.0` | Address of the HTTP listener: `/healthcheck`, `/json` and `/quota` (the [quota lookup](quota-lookup.md), an extension). |
| `PORT` | `8080` | Port of the HTTP listener. A port in use stops the service at startup. |
| `DEBUG_HOST` | `0.0.0.0` | Address of the debug listener: `/rlconfig` (the loaded rules), `/stats`, `/debug/pprof/`. Go runtime profiles and memory statistics do not exist here: those paths answer `404` or `500`, and `/stats` has no `memstats` ([§7](parity.md#7-deliberate-divergences)). |
| `DEBUG_PORT` | `6070` | Port of the debug listener. A port in use is logged and ignored, as upstream. |
| `GRPC_UDS` | (empty) | Path of a Unix socket for gRPC. When set, gRPC listens on this socket only, and `GRPC_HOST` and `GRPC_PORT` are ignored. The socket file is removed on shutdown. |
| `GRPC_HOST` | `0.0.0.0` | Address of the gRPC listener (Envoy's rate limit service). |
| `GRPC_PORT` | `8081` | Port of the gRPC listener. A port in use stops the service at startup. |
| `GRPC_MAX_CONNECTION_AGE` | `24h` | Duration. A gRPC connection older than this is sent a `GOAWAY`, so that clients reconnect and re-resolve DNS. Upstream adds ±10% of random jitter; this service applies the value as it is ([§7](parity.md#7-deliberate-divergences)). |
| `GRPC_MAX_CONNECTION_AGE_GRACE` | `1h` | Duration. Time given to calls in flight after the `GOAWAY`, after which the connection is closed. |
| `GRPC_SERVER_USE_TLS` | `false` | Boolean. Serves gRPC over TLS, on TCP or on `GRPC_UDS`. Clients must negotiate the `h2` protocol (ALPN), unless `GRPC_ENFORCE_ALPN_ENABLED=false`. Plaintext clients are closed. |
| `GRPC_SERVER_TLS_CERT` | (empty) | Path of the server's PEM certificate chain, used with `GRPC_SERVER_TLS_KEY`. Loaded at startup (a file that does not load is fatal), then reloaded when its directory changes. |
| `GRPC_SERVER_TLS_KEY` | (empty) | Path of the server's PEM private key, in the same directory as the certificate. RSA keys need at least 2048 bits; ECDSA keys must be P-256 or P-384 ([§7](parity.md#7-deliberate-divergences)). |
| `GRPC_CLIENT_TLS_CACERT` | (empty) | Path of a PEM CA. When set, every gRPC client must present a certificate issued by this CA (mutual TLS). Upstream also accepts a client certificate from any CA the system trusts; this service accepts only this CA ([§7](parity.md#7-deliberate-divergences)). |
| `GRPC_CLIENT_TLS_SAN` | (empty) | With `GRPC_CLIENT_TLS_CACERT`: a DNS name or IP address the client certificate must be valid for. A certificate that does not match is refused, and the refusal is logged. |

## Logging

| Variable | Default | Description |
|---|---|---|
| `LOG_LEVEL` | `WARN` | `panic`, `fatal`, `error`, `warn` (or `warning`), `info`, `debug` or `trace`, in any case. Any other value is fatal at startup. The startup lines (`Listening for ...`) are written at `warn`. |
| `LOG_FORMAT` | `text` | `json` (in any case) writes one JSON object per line, with `@timestamp`, `@message` and `level`. Any other value writes logrus' text format (`time=... level=... msg=...`). |

Log timestamps use the local time zone, read from `TZ` as Go reads it: unset means `/etc/localtime`; empty or `UTC` means UTC; otherwise a zone file path or a zone name such as `Europe/Paris`. A zone that does not load is UTC.

## Configuration source

`CONFIG_TYPE` selects where the rules come from: files on disk, or an xDS management server.

### Files

| Variable | Default | Description |
|---|---|---|
| `CONFIG_TYPE` | `FILE` | `FILE` reads the rules from files (the variables below). `GRPC_XDS_SOTW` receives them from an xDS server (the next table). Any other value is fatal at startup. |
| `FORCE_START_WITHOUT_INITIAL_CONFIG` | `false` | Boolean. By default the service waits for its first configuration before it answers. With `true` it answers at once, with no rules until a configuration arrives. With xDS and an unreachable server, `false` means no service at all until the server answers. |
| `RUNTIME_ROOT` | `/srv/runtime_data/current` | The root of the rules directory. Every file in `RUNTIME_ROOT/RUNTIME_SUBDIRECTORY/RUNTIME_APPDIRECTORY/` is read, whatever its extension: a README or an editor's swap file there is a load error. |
| `RUNTIME_SUBDIRECTORY` | (empty) | The directory under `RUNTIME_ROOT`. With `RUNTIME_WATCH_ROOT=true`, it must be set: when it is empty, no rules are loaded at all, and the log says `no runtime configuration. using nil loader.` (upstream's goruntime does the same). |
| `RUNTIME_APPDIRECTORY` | `config` | The directory under the subdirectory that holds the YAML files. |
| `RUNTIME_IGNOREDOTFILES` | `false` | Boolean. Skips files and directories whose name starts with `.`, without reading inside them. Set `true` for Kubernetes ConfigMaps, whose `..data` directories would otherwise load each file twice. |
| `RUNTIME_WATCH_ROOT` | `true` | Boolean. With `true`, only a change of `RUNTIME_ROOT` itself reloads the rules: the symlink swap that upstream's deployments use. Editing a file in place does nothing. With `false`, the app directory is watched, and any file written, created, removed or `chmod`-ed there reloads every file. Set `false` for in-place edits, and for Kubernetes ConfigMaps with `RUNTIME_IGNOREDOTFILES=true`. |
| `MERGE_DOMAIN_CONFIG` | `false` | Boolean. By default, two files with the same `domain` are an error. With `true`, their descriptors are merged into one domain. |

### xDS

| Variable | Default | Description |
|---|---|---|
| `CONFIG_GRPC_XDS_NODE_ID` | `default` | The node ID sent in every discovery request. |
| `CONFIG_GRPC_XDS_NODE_METADATA` | (empty) | The node's metadata: comma-separated `KEY=VALUE` entries, split at the first `=`. Entries without `=` are skipped. |
| `CONFIG_GRPC_XDS_SERVER_URL` | `localhost:18000` | The management server, as a gRPC target: `host:port`, `dns:///host:port`, `passthrough:///host:port` or `unix:/path`. Other forms of `unix://` and `unix-abstract:` are not supported ([§7](parity.md#7-deliberate-divergences)). |
| `CONFIG_GRPC_XDS_SERVER_CONNECT_RETRY_INTERVAL` | `3s` | Duration. Read and validated, but it has no effect, as upstream. |
| `CONFIG_GRPC_XDS_CLIENT_ADDITIONAL_HEADERS` | (empty) | Map. Headers sent with each stream, as `name:value,name:value`. Names are lowercased; values of `-bin` headers are base64. A name or value gRPC refuses fails the stream. |
| `CONFIG_GRPC_XDS_SERVER_USE_TLS` | `false` | Boolean. Connects to the server over TLS. The server's certificate is verified, and `h2` must be negotiated unless `GRPC_ENFORCE_ALPN_ENABLED=false`. |
| `CONFIG_GRPC_XDS_CLIENT_TLS_CERT` | (empty) | Path of a PEM client certificate, with `CONFIG_GRPC_XDS_CLIENT_TLS_KEY`, for mutual TLS. |
| `CONFIG_GRPC_XDS_CLIENT_TLS_KEY` | (empty) | Path of the PEM key of the client certificate. |
| `CONFIG_GRPC_XDS_SERVER_TLS_CACERT` | (empty) | Path of a PEM CA trusted for the server, in addition to the system's roots. |
| `CONFIG_GRPC_XDS_SERVER_TLS_SAN` | (empty) | The name the server's certificate must be valid for. By default, the host of the URL. |
| `XDS_CLIENT_BACKOFF_INITIAL_INTERVAL` | `10s` | Duration. The first wait before opening a new stream after one fails. |
| `XDS_CLIENT_BACKOFF_MAX_INTERVAL` | `60s` | Duration. The longest wait between streams. |
| `XDS_CLIENT_BACKOFF_RANDOM_FACTOR` | `0.5` | Float. The factor the wait is multiplied by at each attempt (jpillora/backoff). Below 1, as with the default, the wait stays at the initial interval. |
| `XDS_CLIENT_BACKOFF_JITTER` | `true` | Boolean. Adds randomness to each wait. |
| `XDS_CLIENT_MAX_MSG_SIZE_IN_BYTES` | (empty) | Integer. The largest response accepted; a larger one fails the stream. Unset or 0 means gRPC's default of 4 MiB. |

The TLS files of every setting (gRPC server, xDS, Redis, Memcached) are read at startup whatever `CONFIG_TYPE` and `BACKEND_TYPE` are, and again at each configuration update. A file that has disappeared by then stops the service.

## Backend and keys

| Variable | Default | Description |
|---|---|---|
| `BACKEND_TYPE` | `redis` | `redis` (or empty) counts in Redis. `memcache` counts in Memcached. Any other value is fatal at startup. On Memcached, the health check reports healthy once the client is made; upstream's never does ([§7](parity.md#7-deliberate-divergences)). `anchored` windows need Redis. |
| `CACHE_KEY_PREFIX` | (empty) | A string put before every counter key, in Redis and in Memcached. Use it to share one server between deployments. |
| `EXPIRATION_JITTER_MAX_SECONDS` | `300` | Integer. A random number of seconds, from 0 to just below this value, added to each key's expiry, so that keys do not all expire at once. 0 adds none. |
| `STOP_CACHE_KEY_INCREMENT_WHEN_OVERLIMIT` | `false` | Boolean. Redis only. When one descriptor of a request is already over its limit, the other descriptors' counters are not incremented. It has no effect on `rate_limits:` lists. |
| `HEALTHY_WITH_AT_LEAST_ONE_CONFIG_LOADED` | `false` | Boolean. With `true`, the health check starts unhealthy and turns healthy once a configuration with at least one domain is loaded; it turns unhealthy again if a later configuration has none. |

## Redis

One Redis serves every limit, unless `REDIS_PERSECOND=true` sends the per-second limits to a second one (the next section).

`REDIS_URL` depends on `REDIS_TYPE` and `REDIS_SOCKET_TYPE`:

- **`SINGLE`:** one address. With `REDIS_SOCKET_TYPE=unix`, a socket path. With `tcp`, `host:port`, or a URL `redis://[user[:password]@]host:port[/db]`. The URL can also give `?username=`, `?password=` and `?db=`.
- **`SENTINEL`:** the primary's name, then the sentinels: `mymaster,sentinel-1:26379,sentinel-2:26379`. At least one sentinel is required. The sentinels are dialled over TCP and tried in the order given; upstream tries them in random order ([§7](parity.md#7-deliberate-divergences)).
- **`CLUSTER`:** the nodes to start from, comma-separated: `node-1:6379,node-2:6379`. The nodes are dialled over TCP.

| Variable | Default | Description |
|---|---|---|
| `REDIS_SOCKET_TYPE` | `unix` | `unix`, `tcp`, `tcp4` or `tcp6`. The default expects a local socket; set `tcp` to reach Redis at `host:port`. Any other value fails every connection attempt. |
| `REDIS_TYPE` | `SINGLE` | `SINGLE`, `SENTINEL` or `CLUSTER`, in any case. Any other value is fatal at startup. |
| `REDIS_URL` | `/var/run/nutcracker/ratelimit.sock` | Where Redis is, in the format above. Credentials in the URL, including the `username` and `password` query parameters, are masked in the logs; upstream prints the query parameters ([§7](parity.md#7-deliberate-divergences)). |
| `REDIS_POOL_SIZE` | `10` | Integer. The number of connections. 0 means 4. |
| `REDIS_AUTH` | (empty) | Secret. `password`, or `user:password`. It wins over credentials in `REDIS_URL`. Cannot be combined with `REDIS_AUTH_FILE` or `REDIS_AWS_IAM_AUTH`. |
| `REDIS_AUTH_FILE` | (empty) | Path of a file holding `password` or `user:password`, trimmed. Read again at each connection, so a rotated secret works without a restart. A missing or empty file fails the connection. |
| `REDIS_AWS_IAM_AUTH` | `false` | Boolean. Authenticates to AWS ElastiCache with IAM: each connection sends a signed token, reused for 14 minutes. The AWS credentials are found as the Go SDK finds them ([AWS variables](#aws)). Requires `REDIS_AWS_IAM_CACHE_NAME` and `REDIS_AWS_IAM_USER_ID`. ElastiCache requires TLS for IAM, so set `REDIS_TLS=true` too. |
| `REDIS_AWS_IAM_CACHE_NAME` | (empty) | The serverless cache's name, or the replication group ID. Not the endpoint's host name. |
| `REDIS_AWS_IAM_USER_ID` | (empty) | The ElastiCache user to connect as. |
| `REDIS_AWS_IAM_SERVERLESS` | `false` | Boolean. Set `true` for a serverless cache, whose tokens are signed differently. |
| `REDIS_AWS_IAM_REGION` | (empty) | The cache's region. By default, the AWS configuration's (`AWS_REGION`, `AWS_DEFAULT_REGION`, the profile). No region at all is fatal at startup. |
| `REDIS_TLS` | `false` | Boolean. Connects over TLS, with the files below. TLS over a Unix socket is refused at startup ([§7](parity.md#7-deliberate-divergences)). |
| `REDIS_TLS_CLIENT_CERT` | (empty) | Path of a PEM client certificate, for servers that require one. Also used by `REDIS_PERSECOND_TLS`. |
| `REDIS_TLS_CLIENT_KEY` | (empty) | Path of the PEM key of the client certificate. |
| `REDIS_TLS_CACERT` | (empty) | Path of a PEM CA trusted in addition to the system's roots. |
| `REDIS_TLS_SKIP_HOSTNAME_VERIFICATION` | `false` | Boolean. Verifies the certificate chain but not the host name, for servers whose certificate names another host (such as GCP Memorystore). |
| `REDIS_PIPELINE_WINDOW` | `0` | Duration. Only read in `CLUSTER` mode, where a positive value is logged at debug level. Upstream buffers writes for this long; this service sends each request's commands as one pipeline to the nodes of their slots, with the same commands and answers ([§7](parity.md#7-deliberate-divergences)). |
| `REDIS_PIPELINE_LIMIT` | `0` | Integer. Deprecated, no effect. A positive value logs a warning, as upstream. |
| `REDIS_CLUSTER_PIPELINE_PARALLELISM` | `1` | Integer. In `CLUSTER` mode: 0 means as many as `REDIS_POOL_SIZE`, and a value above the pool size is lowered to it; both are logged. Negative is fatal at startup. The pipelines are scheduled differently from upstream ([§7](parity.md#7-deliberate-divergences)). |
| `REDIS_HEALTH_CHECK_ACTIVE_CONNECTION` | `false` | Boolean. With `true`, the health check turns unhealthy when Redis has no active connection, and healthy again when it has one. This service notices a closed connection as soon as the socket closes; upstream at its next use ([§7](parity.md#7-deliberate-divergences)). |
| `REDIS_CLOSE_CONNECTION_ON_READONLY_ERROR` | `false` | Boolean. Closes a connection that gets a `READONLY` reply, In `CLUSTER` mode, so that the pool reconnects to the current primary after a failover. In `SINGLE` mode, as upstream, it only logs a warning. |
| `REDIS_TIMEOUT` | `10s` | Duration. The connection timeout. 0 means none. |
| `REDIS_POOL_ON_EMPTY_BEHAVIOR` | `WAIT` | When every connection is busy, a request waits for one. `CREATE` and `ERROR` are not supported (as upstream, since its move to radix v4) and are fatal at startup. Any other value logs a warning and waits. |
| `REDIS_STARTUP_INITIAL_INTERVAL` | `1s` | Duration. At startup, the first wait before trying Redis again after a failed connection. The wait doubles, with jitter, at each attempt. |
| `REDIS_STARTUP_MAX_INTERVAL` | `30s` | Duration. The longest wait between startup attempts. |
| `REDIS_STARTUP_MAX_ELAPSED_TIME` | `0` | Duration. Gives up and stops the service after this long without a connection. 0 means retry forever. |
| `REDIS_SENTINEL_AUTH` | (empty) | Secret. `password` or `user:password` for the sentinels, in `SENTINEL` mode. `REDIS_AUTH` is for Redis itself. |

## Redis per second

With `REDIS_PERSECOND=true`, limits per second go to a second Redis. A `rate_limits:` list whose windows include one under a minute goes there whole. The variables mirror those of the main Redis. `REDIS_CLOSE_CONNECTION_ON_READONLY_ERROR`, `REDIS_HEALTH_CHECK_ACTIVE_CONNECTION`, the `REDIS_STARTUP_*` variables and the TLS files (`REDIS_TLS_CLIENT_CERT`, `REDIS_TLS_CLIENT_KEY`, `REDIS_TLS_CACERT`, `REDIS_TLS_SKIP_HOSTNAME_VERIFICATION`) apply to both. The per-second pool connects first.

| Variable | Default | Description |
|---|---|---|
| `REDIS_PERSECOND` | `false` | Boolean. Enables the per-second Redis. |
| `REDIS_PERSECOND_SOCKET_TYPE` | `unix` | As `REDIS_SOCKET_TYPE`. |
| `REDIS_PERSECOND_TYPE` | `SINGLE` | As `REDIS_TYPE`. |
| `REDIS_PERSECOND_URL` | `/var/run/nutcracker/ratelimitpersecond.sock` | As `REDIS_URL`. |
| `REDIS_PERSECOND_POOL_SIZE` | `10` | As `REDIS_POOL_SIZE`. |
| `REDIS_PERSECOND_AUTH` | (empty) | Secret. As `REDIS_AUTH`. |
| `REDIS_PERSECOND_AUTH_FILE` | (empty) | As `REDIS_AUTH_FILE`. |
| `REDIS_PERSECOND_AWS_IAM_AUTH` | `false` | As `REDIS_AWS_IAM_AUTH`. None of the main pool's IAM settings carry over. |
| `REDIS_PERSECOND_AWS_IAM_CACHE_NAME` | (empty) | As `REDIS_AWS_IAM_CACHE_NAME`. |
| `REDIS_PERSECOND_AWS_IAM_USER_ID` | (empty) | As `REDIS_AWS_IAM_USER_ID`. |
| `REDIS_PERSECOND_AWS_IAM_SERVERLESS` | `false` | As `REDIS_AWS_IAM_SERVERLESS`. |
| `REDIS_PERSECOND_AWS_IAM_REGION` | (empty) | As `REDIS_AWS_IAM_REGION`. |
| `REDIS_PERSECOND_TLS` | `false` | As `REDIS_TLS`, with the same files. The files are loaded even when `REDIS_PERSECOND` is `false`. |
| `REDIS_PERSECOND_SENTINEL_AUTH` | (empty) | Secret. As `REDIS_SENTINEL_AUTH`. |
| `REDIS_PERSECOND_PIPELINE_WINDOW` | `0` | As `REDIS_PIPELINE_WINDOW`. |
| `REDIS_PERSECOND_PIPELINE_LIMIT` | `0` | As `REDIS_PIPELINE_LIMIT`. |
| `REDIS_PERSECOND_CLUSTER_PIPELINE_PARALLELISM` | `1` | As `REDIS_CLUSTER_PIPELINE_PARALLELISM`, bounded by `REDIS_PERSECOND_POOL_SIZE`. |
| `REDIS_PERSECOND_TIMEOUT` | `10s` | As `REDIS_TIMEOUT`. |
| `REDIS_PERSECOND_POOL_ON_EMPTY_BEHAVIOR` | `WAIT` | As `REDIS_POOL_ON_EMPTY_BEHAVIOR`. |

## Memcached

Used with `BACKEND_TYPE=memcache`. Increments are sent in the background, so concurrent requests can briefly exceed a limit, as upstream. Memcached keys are at most 250 bytes.

| Variable | Default | Description |
|---|---|---|
| `MEMCACHE_HOST_PORT` | (empty) | List. The servers, as `host:port`, comma-separated; an entry with a `/` is a Unix socket path. Each key goes to the server its CRC-32 picks, so give every replica the same list in the same order. The names are resolved at startup; if one does not resolve, there is no server at all. An empty host or port, or a service name as the port, does not resolve here ([§7](parity.md#7-deliberate-divergences)). Cannot be combined with `MEMCACHE_SRV`. |
| `MEMCACHE_MAX_IDLE_CONNS` | `2` | Integer. Idle connections kept per server. |
| `MEMCACHE_SRV` | (empty) | A DNS SRV name, `_service._proto.name`, whose targets are the servers. A name that does not parse or resolve is fatal at startup. |
| `MEMCACHE_SRV_REFRESH` | `0` | Duration. Looks up `MEMCACHE_SRV` again at this interval and replaces the servers. 0 never does. |
| `MEMCACHE_TLS` | `false` | Boolean. Connects over TLS, with the files below. Over a Unix socket, the server name sent is `memcached.invalid`, so the certificate is accepted only with `MEMCACHE_TLS_SKIP_HOSTNAME_VERIFICATION=true` ([§7](parity.md#7-deliberate-divergences)). |
| `MEMCACHE_TLS_CLIENT_CERT` | (empty) | Path of a PEM client certificate. |
| `MEMCACHE_TLS_CLIENT_KEY` | (empty) | Path of the PEM key of the client certificate. |
| `MEMCACHE_TLS_CACERT` | (empty) | Path of a PEM CA trusted for the servers. |
| `MEMCACHE_TLS_SKIP_HOSTNAME_VERIFICATION` | `false` | Boolean. Verifies the certificate chain but not the host name. |

## Limits behaviour

| Variable | Default | Description |
|---|---|---|
| `SHADOW_MODE` | `false` | Boolean. Global shadow mode: every decision is counted as usual, but a request over its limit is answered `OK`. The `global_shadow_mode` stat counts the requests it let through. |
| `QUOTA_MODE` | `false` | Boolean. Treats every descriptor as `quota_mode: true`. A descriptor over its limit then denies the request only when every quota group of the request is exhausted. |
| `RESPONSE_DYNAMIC_METADATA` | `false` | Boolean. Adds the request to the response's dynamic metadata: its `domain`, its `descriptors`, `hitsAddend` when not 0, the `metadata` of the limits that passed and, in quota mode, the `passedBackends`. |
| `USE_CALENDAR_MONTH_RATE_LIMIT` | `false` | Boolean. A `rate_limit: {unit: month}` limit counts calendar months in UTC (the 1st to the last day) for its keys, expiry and reset, instead of 30-day windows from the epoch. It does not apply to `rate_limits:` windows. |
| `NEAR_LIMIT_RATIO` | `0.8` | Float. A hit above this fraction of the limit, but within it, counts in the `near_limit` stat. A negative value counts as 0; upstream's result depends on the CPU ([§7](parity.md#7-deliberate-divergences)). |
| `LOCAL_CACHE_SIZE_IN_BYTES` | `0` | Integer. Size of an in-memory cache of the keys that are over their limit, so that Redis or Memcached is not asked again until the window ends. 0 disables it; any other value below 512 KiB, negative included, is raised to 512 KiB. `fixed` windows of `rate_limits:` lists are cached; `anchored` ones never are. |
| `LIMIT_RESPONSE_HEADERS_ENABLED` | `false` | Boolean. Adds three headers to the response, for the descriptor closest to its limit: the limit, the hits remaining, and the seconds until the reset. |
| `LIMIT_LIMIT_HEADER` | `RateLimit-Limit` | The name of the limit header. |
| `LIMIT_REMAINING_HEADER` | `RateLimit-Remaining` | The name of the remaining header. |
| `LIMIT_RESET_HEADER` | `RateLimit-Reset` | The name of the reset header. |
| `LIMIT_PER_UNIT_HEADERS_ENABLED` | `false` | Boolean. Adds `RateLimit-Limit-<Unit>` and `RateLimit-Remaining-<Unit>` headers for each unit, from the limit with the fewest hits remaining. It works with the three headers above. For `rate_limits:` lists, see [Headers, stats and metrics](configuration.md#headers-stats-and-metrics). |
| `LIMIT_REQUEST_HEADERS_ENABLED` | `false` | Boolean. Adds the same three headers to the request Envoy forwards upstream, so the backend can see the remaining quota. |
| `LIMIT_REQUEST_LIMIT_HEADER` | `RateLimit-Limit` | The name of the request's limit header. |
| `LIMIT_REQUEST_REMAINING_HEADER` | `RateLimit-Remaining` | The name of the request's remaining header. |
| `LIMIT_REQUEST_RESET_HEADER` | `RateLimit-Reset` | The name of the request's reset header. |

## Stats

Exactly one sink can be enabled: statsd (the default), DogStatsD or Prometheus.

- `DISABLE_STATS=true` wins over the rest: no stats are sent, and the `USE_*` variables are not checked.
- `USE_STATSD` is `true` by default. Enabling `USE_PROMETHEUS` or `USE_DOG_STATSD` alone is therefore fatal: `Error: unable to use more than one stats sink at the same time.` Set `USE_STATSD=false` with it.
- With all three `false`, the stats are written to the log.

| Variable | Default | Description |
|---|---|---|
| `USE_STATSD` | `true` | Boolean. Sends stats to a statsd server at `STATSD_HOST:STATSD_PORT`. |
| `STATSD_HOST` | `localhost` | Host of the statsd or DogStatsD server. A `unix://` address, which upstream's DogStatsD accepts, does not resolve here ([§7](parity.md#7-deliberate-divergences)). |
| `STATSD_PORT` | `8125` | Integer. Port of the statsd or DogStatsD server. |
| `USE_DOG_STATSD` | `false` | Boolean. Sends stats to DogStatsD, with tags, at `STATSD_HOST:STATSD_PORT`. |
| `USE_DOG_STATSD_MOGRIFIERS` | (empty) | List. The names of the mogrifiers that rewrite stat names into a name and tags, applied in this order. Each is defined by `DOG_STATSD_MOGRIFIER_<NAME>_*`, below. |
| `EXTRA_TAGS` | (empty) | Map. Tags added to every stat, as `key:value,key:value`. |
| `STATS_FLUSH_INTERVAL` | `10s` | Duration. How often stats are flushed. It must be positive: 0 or less is fatal at startup. |
| `DISABLE_STATS` | `false` | Boolean. Disables stats entirely. |
| `SANITIZE_DESCRIPTOR_METRIC_DOTS` | `false` | Boolean. Replaces `.` with `_` in descriptor keys and values in stat names, so that a dotted value does not add levels to the name. Matching is unchanged. |
| `USE_PROMETHEUS` | `false` | Boolean. Serves Prometheus metrics, mapped from the statsd names. |
| `PROMETHEUS_ADDR` | `:9090` | The Prometheus listener, as `host:port`. A service name as the port (`:http`) is not supported, and Go's `/debug/pprof/` and `/debug/vars` are not served there ([§7](parity.md#7-deliberate-divergences)). |
| `PROMETHEUS_PATH` | `/metrics` | The metrics path. It must be a clean absolute path; another value is fatal at startup, where upstream panics or treats it as a method or host pattern ([§7](parity.md#7-deliberate-divergences)). |
| `PROMETHEUS_MAPPER_YAML` | (empty) | Path of a statsd_exporter mapping file. By default, the shipped mapper: upstream's, plus a `limit` label for `rate_limits:` lists and the quota lookup's stats. A file that cannot be read or is invalid gives a mapper that maps nothing, as upstream; only an error that upstream panics on stops the service. |
| `PROMETHEUS_RESPONSE_TIME_AS_MILLISECONDS` | `false` | Boolean. With the shipped mapper, reports response times in milliseconds instead of seconds. Ignored with `PROMETHEUS_MAPPER_YAML`. |

These variables are read by the stats libraries upstream uses, and by this service in the same way. They have no row above because they are not part of upstream's settings:

- `STATSD_PROTOCOL`: `tcp` by default, so statsd is sent over TCP. `udp`, `udp4` or `udp6` sends it over UDP.
- `GOSTATS_FLUSH_INTERVAL_SECONDS` and `GOSTATS_LOGGING_SINK_DISABLED`: only checked when the statsd sink is used. An integer and a boolean; an invalid value is fatal at startup. gostats also checks `USE_STATSD` and `STATSD_PORT` then, `STATSD_PORT` in decimal only.
- `DOG_STATSD_MOGRIFIER_<NAME>_PATTERN`, `DOG_STATSD_MOGRIFIER_<NAME>_NAME` and `DOG_STATSD_MOGRIFIER_<NAME>_TAGS`, for each name of `USE_DOG_STATSD_MOGRIFIERS` (upper-cased): a regular expression matched against the stat name, the new name, and the tags as a map (`domain:$1,descriptor:$2`). `$1`, `$2`, ... are the pattern's groups. When a variable is unset, the bare `PATTERN`, `NAME` or `TAGS` is used. The pattern is a Rust regular expression, with `\d`, `\w`, `\s` and `\b` limited to ASCII as in Go; syntax that only one of the two engines accepts differs ([§7](parity.md#7-deliberate-divergences)). An invalid mogrifier is fatal at startup.
- `DD_ENTITY_ID`, `DD_ENV`, `DD_SERVICE`, `DD_VERSION` and `DD_ORIGIN_DETECTION_ENABLED`: read by the DogStatsD sink as datadog-go reads them. Origin detection is on unless `DD_ORIGIN_DETECTION_ENABLED` is a valid false boolean.

## Tracing

Spans are exported with OTLP, over HTTP or gRPC. The collector is configured by the `OTEL_*` variables in the next section.

| Variable | Default | Description |
|---|---|---|
| `TRACING_ENABLED` | `false` | Boolean. Exports a span for each gRPC call. When `false`, the log says `Tracing disabled`. |
| `TRACING_SERVICE_NAME` | `RateLimit` | The `service.name` resource attribute. It always wins over `OTEL_SERVICE_NAME` and over a `service.name` in `OTEL_RESOURCE_ATTRIBUTES`. |
| `TRACING_SERVICE_NAMESPACE` | (empty) | The `service.namespace` resource attribute. |
| `TRACING_SERVICE_INSTANCE_ID` | (empty) | The `service.instance.id` resource attribute, such as the pod name. By default, a random version 4 UUID. |
| `TRACING_EXPORTER_PROTOCOL` | `http` | `http` (or empty) for OTLP over HTTP, `grpc` for OTLP over gRPC. Any other value is fatal at startup. The HTTP exporter uses HTTP/1.1 and no proxy; upstream honours `HTTPS_PROXY` and `HTTP_PROXY` ([§7](parity.md#7-deliberate-divergences)). |
| `TRACING_SAMPLING_RATE` | `1` | Float, from 0 (never) to 1 (always). The fraction of calls traced. `NaN` never samples ([§7](parity.md#7-deliberate-divergences)). |

## Other variables

These variables belong to the libraries upstream uses (the OpenTelemetry SDK, grpc-go, the AWS SDK for Go). This service reads them as those libraries do. None has a default of its own.

### OpenTelemetry

Each `OTEL_EXPORTER_OTLP_<NAME>` has a `OTEL_EXPORTER_OTLP_TRACES_<NAME>` form, which wins over it. Values are trimmed; an empty one counts as unset.

| Variable | Default | Description |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | (empty) | The collector's URL, such as `http://collector:4318`. Over HTTP, `/v1/traces` is appended to its path (a `TRACES_` endpoint is used as it is). An `http` or `unix` scheme means no TLS. By default, `localhost:4318` over HTTP and `localhost:4317` over gRPC, with TLS. An invalid URL is logged without its text, which can hold credentials ([§7](parity.md#7-deliberate-divergences)). |
| `OTEL_EXPORTER_OTLP_INSECURE` | (empty) | `true` (in any case) disables TLS; any other value enables it. |
| `OTEL_EXPORTER_OTLP_CERTIFICATE` | (empty) | Path of a PEM CA to verify the collector with, instead of the system's roots. |
| `OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE` | (empty) | Path of a PEM client certificate, with `OTEL_EXPORTER_OTLP_CLIENT_KEY`. |
| `OTEL_EXPORTER_OTLP_CLIENT_KEY` | (empty) | Path of the PEM key of the client certificate. |
| `OTEL_EXPORTER_OTLP_HEADERS` | (empty) | Headers sent with each export, as `name=value,name=value` (values percent-decoded). Can hold credentials: an invalid entry is logged by position and name only ([§7](parity.md#7-deliberate-divergences)). |
| `OTEL_EXPORTER_OTLP_COMPRESSION` | (empty) | `gzip` compresses the exports; anything else does not. |
| `OTEL_EXPORTER_OTLP_TIMEOUT` | (empty) | Integer, in milliseconds. The timeout of one export. By default, 10000. |
| `OTEL_SERVICE_NAME` | (empty) | Overridden by `TRACING_SERVICE_NAME`, which always has a value: it has no effect. |
| `OTEL_RESOURCE_ATTRIBUTES` | (empty) | Extra resource attributes, as `key=value,key=value` (values percent-decoded). The `service.*` attributes of the `TRACING_*` variables win. |
| `OTEL_BSP_MAX_QUEUE_SIZE` | (empty) | Integer. Spans waiting for export; more are dropped. By default, 2048. |
| `OTEL_BSP_MAX_EXPORT_BATCH_SIZE` | (empty) | Integer. Spans per export. By default, 512; at most the queue size. |
| `OTEL_BSP_SCHEDULE_DELAY` | (empty) | Integer, in milliseconds. The longest wait before a batch is exported. By default, 5000. |
| `OTEL_BSP_EXPORT_TIMEOUT` | (empty) | Integer, in milliseconds. The timeout of a batch's export; 0 or less means none. By default, 30000. |
| `OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT` | (empty) | Integer. The most attributes a span keeps. By default, `OTEL_ATTRIBUTE_COUNT_LIMIT`, else 128. |
| `OTEL_ATTRIBUTE_COUNT_LIMIT` | (empty) | Integer. The fallback for `OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT`. |
| `OTEL_SPAN_ATTRIBUTE_VALUE_LENGTH_LIMIT` | (empty) | Integer. The longest string attribute, in characters; longer ones are truncated. By default, `OTEL_ATTRIBUTE_VALUE_LENGTH_LIMIT`, else no limit. |
| `OTEL_ATTRIBUTE_VALUE_LENGTH_LIMIT` | (empty) | Integer. The fallback for `OTEL_SPAN_ATTRIBUTE_VALUE_LENGTH_LIMIT`. |
| `OTEL_SEMCONV_STABILITY_OPT_IN` | (empty) | The span attributes of gRPC calls. By default, the current RPC conventions (`rpc.system.name`, a full `rpc.method`, `rpc.response.status_code`). `rpc/old` gives the previous ones (`rpc.system`, `rpc.service`, a short `rpc.method`). `rpc/dup` gives both. |

### gRPC

| Variable | Default | Description |
|---|---|---|
| `GRPC_ENFORCE_ALPN_ENABLED` | (empty) | `false` (in any case) lets TLS clients that negotiate no protocol use the gRPC server, and lets the xDS client use such a server. Any other value, or none, requires `h2`. |
| `GRPC_SERVER_TLS_CA_CERT` | (empty) | Ignored, as upstream. Envoy Gateway sets it, but no code reads it: use `GRPC_CLIENT_TLS_CACERT`. |

grpc-go's own logger (`GRPC_GO_LOG_SEVERITY_LEVEL`, `GRPC_GO_LOG_VERBOSITY_LEVEL`) does not exist here: those variables have no effect ([§7](parity.md#7-deliberate-divergences)).

### AWS

Read only with `REDIS_AWS_IAM_AUTH` or `REDIS_PERSECOND_AWS_IAM_AUTH`. The credentials are found in the Go SDK's order: these variables, then a web identity token, the shared files' profile, the container's credentials, then the instance's (IMDS).

| Variable | Default | Description |
|---|---|---|
| `AWS_ACCESS_KEY_ID` | (empty) | Secret. Static credentials, with `AWS_SECRET_ACCESS_KEY`. `AWS_ACCESS_KEY` is accepted too. |
| `AWS_SECRET_ACCESS_KEY` | (empty) | Secret. `AWS_SECRET_KEY` is accepted too. |
| `AWS_SESSION_TOKEN` | (empty) | Secret. The session token of temporary static credentials. |
| `AWS_WEB_IDENTITY_TOKEN_FILE` | (empty) | Path of a web identity token (IRSA), exchanged for credentials of `AWS_ROLE_ARN`. |
| `AWS_ROLE_ARN` | (empty) | The role to assume with the web identity token. |
| `AWS_ROLE_SESSION_NAME` | (empty) | The session name for that role. |
| `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` | (empty) | The container credentials endpoint (ECS task role). |
| `AWS_CONTAINER_CREDENTIALS_FULL_URI` | (empty) | The container credentials endpoint, as a full URI (EKS Pod Identity). |
| `AWS_PROFILE` | (empty) | The profile of the shared files. A profile named here must exist. |
| `AWS_DEFAULT_PROFILE` | (empty) | The profile when `AWS_PROFILE` is unset. The container's and IMDS' own settings are read from `AWS_PROFILE`'s profile or `default`, not from this one ([§7](parity.md#7-deliberate-divergences)). |
| `AWS_CONFIG_FILE` | (empty) | Path of the shared config file. By default, `~/.aws/config`. |
| `AWS_SHARED_CREDENTIALS_FILE` | (empty) | Path of the shared credentials file. By default, `~/.aws/credentials`. |
| `AWS_REGION` | (empty) | The region, when `REDIS_AWS_IAM_REGION` is unset. |
| `AWS_DEFAULT_REGION` | (empty) | The region when `AWS_REGION` is unset too. |
| `AWS_CA_BUNDLE` | (empty) | Path of a PEM bundle trusted for the AWS endpoints. |
| `AWS_ENDPOINT_URL` | (empty) | An endpoint override for every AWS service, the STS client included; upstream's STS client ignores it ([§7](parity.md#7-deliberate-divergences)). |
| `AWS_ENDPOINT_URL_STS` | (empty) | An endpoint override for STS only. |

## TLS fields of the settings

Upstream's settings also hold four TLS configurations (`GRPCSERVERTLSCONFIG`, `CONFIGGRPCXDSTLSCONFIG`, `REDISTLSCONFIG`, `MEMCACHETLSCONFIG`). For each, the settings library reads variables named after Go's `tls.Config` fields, such as `REDISTLSCONFIG_MINVERSION` or `REDISTLSCONFIG_INSECURESKIPVERIFY`. Upstream ignores or overwrites their values, so they have no effect. An invalid value (a `MINVERSION` that is not a 16-bit integer, say) is still fatal at startup, as upstream.

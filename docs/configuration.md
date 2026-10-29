# Configuration

This service reads the same environment variables and YAML files as envoyproxy/ratelimit. Everything upstream documents in [its README](https://github.com/envoyproxy/ratelimit/blob/f4e7669/README.md) applies as written:

- the variables;
- `domain`, `descriptors`, `rate_limit: {unit, requests_per_unit}`;
- `unlimited`, `shadow_mode`, `quota_mode`, `replaces`;
- `detailed_metric`, `value_to_metric`, `share_threshold`;
- wildcards, the local cache and xDS.

This page covers what is new: several named limits per descriptor, with windows of any duration.

- [Running the service](#running-the-service)
- [`rate_limits:`](#rate_limits)
- [Windows](#windows): [`fixed`](#fixed), [`anchored`](#anchored), [durations](#durations)
- [Decisions with several limits](#decisions-with-several-limits)
- [Headers, stats and metrics](#headers-stats-and-metrics)
- [Moving an existing limit to a list](#moving-an-existing-limit-to-a-list)
- [Errors at load](#errors-at-load)

To see the remaining usage and reset of every limit, see the [quota lookup](quota-lookup.md). To put the service behind agentgateway for LLM token quotas, see [the agentgateway guide](agentgateway.md).

## Running the service

The image's defaults are upstream's:

- `RUNTIME_ROOT=/srv/runtime_data/current`;
- gRPC on 8081, HTTP on 8080 (`/json`, `/quota`, `/healthcheck`), the debug port on 6070.

The configuration files live in `$RUNTIME_ROOT/$RUNTIME_SUBDIRECTORY/config/*.yaml`. The variables people most often miss:

| Variable | Default | Note |
|---|---|---|
| `RUNTIME_SUBDIRECTORY` | empty | The directory under `RUNTIME_ROOT` that holds `config/`. |
| `RUNTIME_WATCH_ROOT` | `true` | With `true`, only a change of `RUNTIME_ROOT` itself, such as a symlink swap, reloads the configuration. Set `false` to reload when a file under the subdirectory changes, as in-place edits and Kubernetes ConfigMaps need. |
| `REDIS_SOCKET_TYPE` | `unix` | Set `tcp` to reach Redis at `host:port`. |
| `REDIS_URL` | `/var/run/nutcracker/ratelimit.sock` | `host:port` with `tcp`. |
| `USE_STATSD` | `true` | Stats go to a statsd server at `STATSD_HOST:STATSD_PORT`. Set `false` without one, or use `USE_PROMETHEUS=true` with `USE_STATSD=false`. |
| `LIMIT_RESPONSE_HEADERS_ENABLED` | `false` | `RateLimit-Limit`, `RateLimit-Remaining` and `RateLimit-Reset` on responses. |
| `LIMIT_PER_UNIT_HEADERS_ENABLED` | `false` | One pair of headers per unit, [below](#headers-stats-and-metrics). |

## `rate_limits:`

A descriptor takes either `rate_limit:` (upstream's single limit) or `rate_limits:`, a list of named limits. Each limit in the list has its own window, and every one of them applies to every request that matches the descriptor.

```yaml
domain: llm
descriptors:
  - key: user
    rate_limits:
      - name: session
        requests_per_unit: 4000000
        window: {type: anchored, duration: 5h}
      - name: weekly
        requests_per_unit: 40000000
        window: {type: fixed, duration: 7d, offset: hashed}
```

| Key | Required | Meaning |
|---|---|---|
| `name` | yes | 1 to 64 letters, digits, `_` or `-`, starting with a letter or digit, unique in the list. It names the limit in responses, headers, stats and the quota lookup. Renaming it starts the limit's counters again. |
| `requests_per_unit` | yes | The number of hits allowed per window: requests, or tokens when the caller sends `hits_addend`. |
| `window` | yes | The window, [below](#windows). |
| `replaces` | no | The names of other descriptors' limits this one replaces, with upstream's semantics: a list limit can replace, and be replaced by, any named limit. It cannot replace a limit of its own list. |

Some keys stay at the **descriptor** level and apply to every limit of its list:

- `shadow_mode` and `quota_mode`;
- `detailed_metric` and `value_to_metric`;
- `share_threshold`.

`overflow: envoy`, the default, counts every hit, denied requests included, as upstream does. `overflow: strict` is designed but not built yet, and is refused at load.

A list entry takes a `window`, not a `unit`, and cannot be `unlimited`. For an unlimited descriptor, keep `rate_limit: {unlimited: true}`.

The value of each descriptor entry is part of every limit's storage key, as upstream's is. A list on `key: user` therefore counts each user separately: `user=alice` and `user=bob` have their own windows and counters.

## Windows

### `fixed`

A window of fixed length. Windows follow each other without gaps; when one ends, its counter starts again from 0.

```yaml
window: {type: fixed, duration: 5h}                                       # 00:00, 05:00, 10:00, … UTC
window: {type: fixed, duration: 7d, offset: 4d}                           # Monday 00:00 UTC to Monday
window: {type: fixed, duration: 7d, offset: hashed}                       # a start of its own per descriptor
window: {type: fixed, duration: 7d, offset: hashed, offset_rounding: 1d}  # …at a whole day
```

The windows start at multiples of `duration` since the Unix epoch (1970-01-01 00:00 UTC, a Thursday), shifted by the `offset`:

- **No `offset`, or `offset: 0`:** the epoch's windows. A `7d` window starts on Thursdays at 00:00 UTC.
- **`offset: <duration>`:** shifted by a constant, in UTC; `0 ≤ offset < duration`. `offset: 4d` on a `7d` window starts it on Mondays at 00:00 UTC. There is no time-zone option: a constant offset cannot follow daylight saving time.
- **`offset: hashed`:** a start of its own for each descriptor, hashed from its entries (`user=alice`). This spreads the weekly resets of users over the week, as Claude's weekly limit does. It is stateless: no key is stored, it is the same on every replica and survives a Redis flush.
  - `offset_rounding` rounds the start to a multiple of it, and must divide `duration`. The default is the largest of `1h`, `1m` and `1s` that divides `duration`.
  - The start moves when the descriptor's entries change (adding, removing or renaming one), or when `duration` or `offset_rounding` changes. It does not move when the domain or `CACHE_KEY_PREFIX` changes.

A window always lasts exactly its duration. `USE_CALENDAR_MONTH_RATE_LIMIT` only applies to `rate_limit: {unit: month}`. A duration that does not divide 24 h, such as `5h`, cannot be aligned on the day by any offset.

`fixed` windows are counted on Redis and on Memcached.

### `anchored`

A window that a request opens and that lasts its duration from then: a session, like Claude's five hours. Each descriptor value has its own session: Alice's first request at 04:52 opens hers until 09:52, Bob's at 05:32 opens his until 10:32.

```yaml
window: {type: anchored, duration: 5h}
window: {type: anchored, duration: 5h, start_rounding: 1h}   # opened at 04:52, anchored at 04:00, ends at 09:00
window: {type: anchored, duration: 5h, open_on: first_hit}
```

- **Opening:** when no session is open, a request opens one. Its **anchor** is the request's time, rounded down to `start_rounding` if set (in UTC), and the session ends at `anchor + duration`. The anchor never moves while the session is open.
- **`open_on`:**
  - `check` (the default): any request opens it, even one with 0 hits, and even one that another limit denies. agentgateway sends a 0-hit check before calling the model, so the session starts at the user's first message.
  - `first_hit`: only a request whose hits are counted opens it.
- **After the end:** the session closes at `anchor + duration`, and nothing happens until the next request, which opens the next session. A user who comes back at 14:10 gets a session from 14:10 to 19:10.
  - A late amendment, such as a stream that started before the end and finished after, is an ordinary request on a closed window. It opens the next session and is counted in it.
- **Refunds** (`is_negative_hits`) take hits back, but never open or close a session.
- **The reset** (`duration_until_reset`, `RateLimit-Reset`) is the time left until `anchor + duration`.
- **Windows opened without hits:** a request that another limit denies still opens a session with `open_on: check`. Those sessions are counted in the `opened_without_hits` stat. Use `open_on: first_hit` if that is unwanted.

`anchored` windows are counted on Redis only, in one script per request: they are refused at load with `BACKEND_TYPE=memcache`. The local cache never holds them, so Redis is asked every time.

### Durations

A duration is whole numbers of weeks, days, hours, minutes then seconds, each at most once and in that order, with nothing between them:

- `90s`, `5h`, `7d`, `2w`, `1h30m` and `1d12h` are valid;
- `30m1h` (out of order), `1.5h`, `5 h` and `0s` are not.

The longest window is 2³² − 1 seconds, about 136 years.

## Decisions with several limits

A decision counts the request's hits in every limit of the descriptor's list, then answers one status for that descriptor (Envoy's API has one per descriptor):

- **If some limits are over:** of those, the one whose reset is furthest away.
- **Otherwise:** the one with the fewest hits remaining.
- **On a tie:** the first in the list.

`current_limit` carries that limit's `name`, its `requests_per_unit`, and its window as a unit and a multiplier: a `5h` window is `HOUR` × 5 (`unit_multiplier: 5`), and a `7d` window is `WEEK`. `limit_remaining` and `duration_until_reset` come from the same limit.

`shadow_mode` and `quota_mode` then apply to that status as they do upstream. With `quota_mode`, a descriptor counts as exhausted when at least one of its limits is.

## Headers, stats and metrics

**Headers.** With `LIMIT_RESPONSE_HEADERS_ENABLED=true`, `RateLimit-Limit`, `RateLimit-Remaining` and `RateLimit-Reset` describe the request's most limiting status, as upstream's do. With `LIMIT_PER_UNIT_HEADERS_ENABLED=true` as well, every limit is described:

- **One pair per unit:** a pair of headers for each unit, with the multiplier in its name when it is not 1.
- **Grouping:** limits of the same unit and multiplier are grouped as upstream groups its units, and legacy limits keep upstream's headers.

```text
RateLimit-Limit-5Hours: 3        RateLimit-Remaining-5Hours: 2     (a 5h window)
RateLimit-Limit-Weeks: 5         RateLimit-Remaining-Weeks: 4      (a 7d window)
RateLimit-Limit-Hours: 9         RateLimit-Remaining-Hours: 8      (rate_limit: {unit: hour})
```

**Stats.** Each list limit has its own stats, named after its descriptor with `~<name>` appended, even when the list has one limit:

```text
ratelimit.service.rate_limit.llm.user~session.total_hits
ratelimit.service.rate_limit.llm.user~session.over_limit
ratelimit.service.rate_limit.llm.user~session.opened_without_hits
```

`detailed_metric: true` puts the value in the name, as upstream does: `llm.user_alice~session`.

**Prometheus.** With `USE_PROMETHEUS=true` and the shipped mapper, the limit's name becomes a `limit` label:

```text
ratelimit_service_rate_limit_total_hits{domain="llm",key1="user",limit="session"}
```

**`/rlconfig`** on the debug port prints a line per limit, with its window.

## Moving an existing limit to a list

A limit's storage key includes its window, so moving `rate_limit: {unit: hour, requests_per_unit: 100}` to a `rate_limits:` entry with `window: {type: fixed, duration: 1h}` starts its counters from 0. Changing a limit's `name`, `duration` or `offset` does the same. The keys of the old limit expire on their own.

## Errors at load

A configuration with a mistake is refused, as upstream refuses its own. The error is logged (`Error loading new configuration: …`) and counted in `ratelimit.service.config_load_error`, and the configuration in use stays in place. The messages name the descriptor and the limit (`user~session: …`), for example:

- `should not specify both rate_limit and rate_limits`
- `invalid rate_limits name 'a b': 1 to 64 letters, digits, '_' or '-', starting with a letter or digit`
- `duplicate rate_limits name`
- `window type is required`, `unknown window type 'x'`
- `window type 'sliding' is not supported yet`, and the same for `sliding_approx` and `token_bucket`
- `invalid window duration '1.5h'`
- `offset '7d' must be shorter than the window duration '7d'`
- `offset_rounding '5h' must divide the window duration '7d'`
- `start_rounding is for anchored windows`, `offset is for fixed windows`
- `only fixed windows can be counted on Memcached`
- `overflow: strict is not supported yet`

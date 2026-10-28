# ADR 0003 — The quota lookup

- **Status:** accepted (2026-10-28)
- **Sources:** issue #47; `docs/design.md` D9, D12, D16, D17, D18, D24, D29; ADR 0001, ADR 0002

## Context

Users of an LLM behind agentgateway must see their quota ("Session: 40 % — resets in 2 h 05 · Week: 62 % — resets Monday 10:00") in OpenCode, Open WebUI and a CLI. `/json` cannot show it:

- it is a decision, so a 0-hit check opens an anchored window;
- it answers one representative status per descriptor;
- it counts rate-limit stats.

A side service reading Redis would have to redo the keys (ADR 0001), the hashed phase (ADR 0002) and the anchored state, and would drift from what is enforced. agentgateway has no read API, and its `x-ratelimit-*` headers carry one limit, one request late.

## Decision

A **quota lookup** at `POST /quota`, on the HTTP port (`PORT`, 8080) next to `/json`, unauthenticated, for internal callers. agentgateway exposes it to users as its own HTTP route (`/v1/quota`) behind the JWT: it builds the descriptors from the identity with CEL, and relays the answer as is.

### Request

The body is a `RateLimitRequest` in JSON, read by `/json`'s parser, with its `400` messages. Other methods answer `405`. `hits_addend` and `is_negative_hits` are ignored. A client-sent `limit` replaces the configured limits as it does in a decision.

**Contract:** the caller sends the same descriptors as the requests it reports on. `replaces` works across the request's descriptors, so another set can show limits the real requests do not have, or hide ones they do.

### What it reads

It reads exactly what the decision would at the same `now`:

- the same rules, `replaces` included;
- the same keys, phases and Redis client (`REDIS_PERSECOND` routing included).

It changes nothing:

- no write and no `EXPIRE`;
- no window opened or closed;
- no local cache access;
- no rate-limit stat.

A fixed window reads only its current window's key. An anchored window is closed when `now ≥ anchor + duration`, as in a decision, even while its key has not expired yet; a count the script cannot read is 0, as for the script. On Memcached, a read-only `GetMulti` of the fixed keys. Where a decision would count on from 0 after a failed `GetMulti` (Memcached is not atomic, D24), the lookup answers `500`: it cannot announce what it did not read.

### Response

`200`, one entry per descriptor in request order, each with its limits in config order. Limits that `replaces` removes are left out.

```json
{
  "now": "2026-10-28T14:00:00Z",
  "descriptors": [
    {
      "limits": [
        {
          "name": "five_hour",
          "window": { "type": "anchored", "duration_s": 18000 },
          "limit": 4000000, "used": 1600000, "remaining": 2400000, "used_pct": 40.0,
          "open": true,
          "resets_at": "2026-10-28T16:05:00Z", "resets_in_s": 7500,
          "shadow_mode": false, "quota_mode": false
        },
        {
          "name": "weekly",
          "window": { "type": "fixed", "duration_s": 604800 },
          "limit": 40000000, "used": 24800000, "remaining": 15200000, "used_pct": 62.0,
          "resets_at": "2026-11-02T10:00:00Z", "resets_in_s": 417600,
          "shadow_mode": false, "quota_mode": false
        }
      ]
    }
  ]
}
```

- **`used`** is the raw counter. It can pass `limit` in `overflow: envoy`.
- **`remaining`** is never below 0.
- **`used_pct`** is `used / limit × 100`, rounded to one decimal and not capped (103.5). A limit of 0 gives 100 once used, else 0.
- **Times:** `resets_at` is in UTC (RFC 3339, whole seconds), and `resets_in_s` is the time from `now`.
- **Anchored windows** have `open`. A closed session answers `used: 0`, `remaining: limit`, `open: false`, and `resets_at` and `resets_in_s` as `null`, as Claude shows nothing before the first message.
- **A `rate_limit:`** shows its `name` (empty without one) and `"window": {"type": "unit", "unit": "HOUR"}`.
- **An `unlimited` limit** is `{"name": …, "unlimited": true}`, with no figures.
- **`shadow_mode` and `quota_mode`** are flags. The figures are the same either way: quota groups depend on the whole request's verdict, which a lookup does not have.
- **A descriptor that no rule matches** gives `"limits": []`.

### Errors and observability

| Case | Answer |
|---|---|
| An invalid body, no domain or descriptors, no configuration loaded yet | `400 {"error": …}`, with the message a decision gives |
| Unknown domain | `404 {"error": "unknown domain 'x'"}` |
| Backend error | `500`, with the error text as `/json` gives it |

Neither counts a rate-limit stat (no `domain_not_found`). The lookup has its own stats:

- `ratelimit.quota_lookup.total_requests`;
- `ratelimit.quota_lookup.error.<cause>`, with `invalid_request`, `unknown_domain` or `backend` as the cause;
- `ratelimit.quota_lookup.response_time`, in ms.

The shipped Prometheus mapper turns them into `ratelimit_quota_lookup_total_requests`, `ratelimit_quota_lookup_errors{cause=…}` and `ratelimit_quota_lookup_response_time_seconds`.

### Consistency, as a property test

At a `now`, a lookup, then a decision with N hits per descriptor. Each descriptor's status before shadow mode and quota groups must be OK ⇔ `used + N ≤ limit` for each of its limits (at N ≥ 1, `remaining ≥ N`). The test covers:

- fixed windows, with and without an offset;
- anchored windows;
- `rate_limit:`;
- `replaces`;
- a client `limit`.

It runs without the local cache.

## Consequences

- The format is a contract with agentgateway's routes and the clients that read it. A field may be added; none may be renamed or removed without a major version.
- With the local cache on, a fixed key it holds as over the limit is denied by a decision without Redis. If Redis lost that key (a flush, an eviction), the lookup shows the limit as available while decisions deny it until the local entry expires. The lookup does not read the local cache: freecache's `Get` moves its access time and hit counters, which the `localcache.*` gauges export.
- A lookup is as costly as a decision on Redis, a read per limit. agentgateway's callers should poll at human pace, around a minute.

## Rejected alternatives

- **`/json` with 0 hits**: it opens sessions, shows one limit, and counts stats.
- **A side service reading Redis**: it would redo the keys, phases and anchored state, and drift from what is enforced.
- **The debug port, or a gRPC method**: agentgateway would need transcoding, while the HTTP + JSON route keeps CEL building the body and the answer relayed as is.

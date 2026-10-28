# ADR 0002 — Origin of fixed windows (`offset`)

- **Status:** accepted (2026-10-16)
- **Sources:** `docs/design.md` D8, D9; [What is the Max plan](https://support.claude.com/en/articles/11049741-what-is-the-max-plan); ccusage `rust/crates/ccusage/src/blocks.rs`

## Context

The reference aligns every fixed window on the Unix epoch (`bucketStart = (now / D) × D`), or on the start of the UTC month with `USE_CALENDAR_MONTH_RATE_LIMIT`.

We want to reproduce Claude's limits. The official documentation describes the weekly limit as: "The weekly limit resets at a fixed time each week that is assigned to your account. Your reset day and time stay the same regardless of when you start using Claude or when your subscription begins". That is a **fixed window whose origin depends on the account**, not a chain of windows anchored on first use. The 5 h session is an `anchored` window (see D9) and is not covered by this ADR.

An earlier draft had an `anchored` `recurring` mode, then an `offset: from_first_hit`. Both derive the phase from first use, which literally contradicts the source, and both need a phase key with a TTL (an idle client would change reset day) or without one (memory leak).

## Decision

`fixed` accepts an optional origin:

```yaml
window: { type: fixed, duration: 7d }                                   # origin = epoch (default, upstream behaviour)
window: { type: fixed, duration: 7d, offset: 4d }                       # Monday 00:00 UTC (the epoch is a Thursday)
window: { type: fixed, duration: 7d, offset: hashed, offset_rounding: 1h }  # an origin per "account"
```

With a phase φ, the window starts at `now − (now − φ).rem_euclid(D)` and `duration_until_reset = D − (now − φ).rem_euclid(D)`, rounded up to the second (D12).

### `offset: <duration>`

- Constant, **UTC only**.
- Rule: `0 ≤ offset < duration`, otherwise a config error (no silent normalisation). `offset: 0` is the epoch's origin: the same windows, so the same keys.
- Local time cannot be held with a constant offset (daylight saving changes: a local week of 167 h or 169 h). An IANA `timezone:` option is explicitly deferred.

### `offset: hashed`

- **Hash input**: the descriptor's entries only, as `key=value` joined by `\0`, **after** the `share_threshold` substitution, in **raw** form (before the escaping of ADR 0001). Neither prefix nor domain.
- **Frozen algorithm**: 64-bit FNV-1a over the UTF-8 bytes of that input. φ = `(h mod (D / R)) × R`, with R = `offset_rounding`.
- **`offset_rounding`** must divide `duration`, and `0 < R ≤ D`, otherwise a config error. Default: the largest of `1h`, `1m`, `1s` that divides the duration (5h → 1h; 90s → 1s; 7d → 1h).
- Stateless: no Lua, no key, no TTL. Stable over time, identical on every replica, survives a `FLUSHALL`, works on Memcached.

### Common rules

- A list window always lasts exactly its duration: `USE_CALENDAR_MONTH_RATE_LIMIT` does not apply to it, so `offset` never meets a calendar month.
- `offset` is part of the key's shape (ADR 0001): `fixed 7d` and `fixed 7d offset 4d` on the same descriptor are two distinct limits.
- For a duration that does not divide 24 h (e.g. 5 h), no offset aligns the windows on the day; to be said in the documentation.

## What moves (or not) the reset day with `hashed`

| Change | Phase moves? |
|---|---|
| Renaming the domain, changing `CACHE_KEY_PREFIX` | no |
| Migrating from `rate_limit:` to `rate_limits:` | no (but the counter restarts from zero, ADR 0001) |
| Same descriptor in two domains | same phase (counters stay separate) |
| Adding, removing or renaming a descriptor entry | **yes** |
| Changing `duration` or `offset_rounding` | **yes** |
| Changing the hash algorithm or input | **yes, for everyone**: a breaking change |

## Consequences

- Claude's weekly limit is reproducible without extra state. Resets are spread uniformly, which avoids a Monday 00:00 rush.
- The reset time of a particular account cannot be chosen.
- The `recurring` mode of `anchored` is removed.

## Rejected alternatives

- **`anchored` in `recurring` mode** and **`offset: from_first_hit`**: the phase depends on first use, contrary to the source; a phase key is needed (with a TTL ⇒ an idle client changes reset day; without ⇒ leak); three round trips on Memcached.
- **Hashing the full key (prefix and domain included)**: renaming a domain or changing the prefix would move every account's reset day.
- **IANA time zone in v1**: ambiguous semantics around daylight saving changes; deferred.

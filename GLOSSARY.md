# ratelimit

A Rust rewrite of envoyproxy/ratelimit: a global rate limit service that Envoy, Envoy Gateway or agentgateway asks whether a request may proceed, extended with several windows per descriptor for LLM quotas.

## Deciding

**Decision**:
The answer to a rate limit request: it counts the request's hits and says OK or OVER_LIMIT.
_Avoid_: rate limit check, enforcement call

**Check**:
A decision with no hits, sent before the work it guards.
_Avoid_: probe, ping

**Amendment**:
A decision sent after the work it guards, carrying the work's real cost.
_Avoid_: correction, post-hit

**Refund**:
A decision taking hits back (`is_negative_hits`).
_Avoid_: credit, negative hit

**Representative status**:
The one status a descriptor's decision answers when several limits apply to it.
_Avoid_: reduced status, main limit

## Limits and windows

**Limit**:
A number of hits allowed per window, for the requests matching a descriptor.
_Avoid_: rule, quota rule

**Window**:
The span of time a limit's hits are counted over.
_Avoid_: bucket, period

**Fixed window**:
A window of fixed length starting at the epoch, or at a phase.
_Avoid_: calendar window, tumbling window

**Phase**:
How long after the epoch a fixed window's windows start, constant or hashed from the descriptor.
_Avoid_: offset (the config key), alignment

**Anchored window**:
A window that a request opens and that lasts its duration from its anchor.
_Avoid_: rolling window, session window

**Anchor**:
When an anchored window opened, rounded down to the window's rounding.
_Avoid_: start time, opening time

**Session**:
An open anchored window, as a user sees it: Claude's five-hour session.
_Avoid_: conversation

## Reading

**Quota lookup**:
A snapshot of every limit applying to some descriptors at an instant: what each has used and when it resets, with no effect on anything.
_Avoid_: quota check, dry run, status query

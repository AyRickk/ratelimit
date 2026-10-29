# Documentation

## Using the service

- [Configuration](configuration.md): running the service, `rate_limits:` lists, `fixed` and `anchored` windows, headers, stats and metrics, errors at load.
- [The quota lookup](quota-lookup.md): `POST /quota`, its request, response and errors.
- [agentgateway](agentgateway.md): LLM token quotas behind agentgateway, from descriptors to the user's quota page.

For everything this service shares with envoyproxy/ratelimit, [upstream's README](https://github.com/envoyproxy/ratelimit/blob/f4e7669/README.md) applies as written: the environment variables, the YAML configuration, the API, the backends, the stats and the local cache.

## How it is built

- [Design](design.md): the goal and every design decision (D1–D34), with the build order.
- [Parity](parity.md): what is proven against upstream and by which test, the deliberate divergences (§7), and the extensions (§8).
- [Architecture decision records](adr/): storage keys, the origin of fixed windows, the quota lookup.
- [Glossary](../GLOSSARY.md): the words used across the docs and the code.

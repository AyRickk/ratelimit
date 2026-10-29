# Contributing

Thanks for your interest. This project is a drop-in rewrite of envoyproxy/ratelimit, so most contributions are about one of two things: matching upstream exactly, or the extensions designed in [`docs/design.md`](docs/design.md).

## Before you start

- **Bugs:** open an issue with the bug form. Say whether upstream behaves the same: a difference from upstream that is not listed in [`docs/parity.md`](docs/parity.md) §7 is a bug.
- **Features:** open an issue first. Anything that changes behaviour upstream also has needs a design decision (`docs/design.md`), and a one-way decision needs an ADR (`docs/adr/`).
- **Security issues:** never in a public issue; see [`SECURITY.md`](SECURITY.md).

## Development

You need Rust 1.99 or later. The differential tests also need `go`, `redis-server` and `memcached` on the `PATH`.

```sh
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
RATELIMIT_REQUIRE_DIFFERENTIAL=1 cargo test --workspace
RATELIMIT_REDIS_SERVER=/path/to/valkey-server RATELIMIT_REQUIRE_DIFFERENTIAL=1 cargo test --workspace
cargo deny check
```

The differential tests build upstream's service at the commit pinned in [`tools/oracle/go.mod`](tools/oracle/go.mod). They run it next to this one and compare answers, keys, TTLs, Redis commands, stats and logs. Behaviour that upstream has is proven that way, not by hand-written expectations.

## Making a change

1. **Tests first.** Write a test that fails without your change: a differential scenario for upstream behaviour, or a test through the public interface for an extension. Tests use the domain's words ([`GLOSSARY.md`](GLOSSARY.md)).
2. **Docs.** Mark the rows of `docs/parity.md` your change proves (✅ with the test names), or add a §7 row for a deliberate divergence.
3. **Commits and PR titles** follow [Conventional Commits](https://www.conventionalcommits.org/) (`feat(redis): …`, `fix: …`, `docs: …`). `main` has a linear history: PRs are squash-merged (the title becomes the commit subject) or rebase-merged (each commit must then follow the convention).
4. **The PR** fills the template: what changes, the evidence (the test that failed and now passes), and whether the change is easy to revert.

The required checks must pass: `check`, `rust (redis)` and `rust (valkey)`. Everything in the repository is written in English.

## License

Contributions are licensed under [Apache-2.0](LICENSE), as the project is (section 5 of the license).

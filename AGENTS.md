## Agent skills

### Issue tracker

Issues are tracked in GitHub Issues for AyRickk/ratelimit (via `gh`). See `docs/agents/issue-tracker.md`.

### Triage labels

Uses the five default triage labels (needs-triage, needs-info, ready-for-agent, ready-for-human, wontfix). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `GLOSSARY.md` and `docs/adr/` at the repo root. See `docs/agents/domain.md`.

## Git conventions

- Commit messages and PR titles follow [Conventional Commits](https://www.conventionalcommits.org/): `<type>(<optional scope>): <description>`, with `type` one of `build`, `chore`, `ci`, `docs`, `feat`, `fix`, `perf`, `refactor`, `revert`, `style`, `test`. Checked on every PR by `.github/workflows/conventional-commits.yml`.
- `main` has a linear history: PRs are merged by squash (the PR title becomes the commit subject) or rebase (each commit lands as-is, so each must follow the convention). Merge commits are disabled.
- Commits and PRs carry no AI attribution (no `Co-Authored-By` trailer, no "Generated with" line).

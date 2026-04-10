# Community Guide

Welcome to the Fuse community! Here's how to get involved.

## Get Help

| Channel | Use For |
|---------|---------|
| [GitHub Discussions](https://github.com/seraphjiang/fuse/discussions) | Questions, ideas, show & tell |
| [GitHub Issues](https://github.com/seraphjiang/fuse/issues) | Bug reports, feature requests |
| [OpenSearch Forum](https://forum.opensearch.org/) | OpenSearch integration questions |
| [Docs Site](https://seraphjiang.github.io/fuse/) | Guides, API reference, tutorials |

## Report a Bug

[Open a GitHub issue](https://github.com/seraphjiang/fuse/issues/new) with:

- **Steps to reproduce** — query, config, curl command
- **Expected vs actual** — what you expected and what happened
- **Error output** — full error message or response body
- **Environment** — OS, Rust version, Fuse version (`curl /api/fuse/health`)
- **Connector** — which datasource type (OpenSearch, DynamoDB, etc.)

Minimal reproduction is the fastest path to a fix.

## Request a Feature

[Open a GitHub issue](https://github.com/seraphjiang/fuse/issues/new) with:

- **Problem** — what you're trying to do and why current features don't cover it
- **Proposed solution** — how you'd like it to work (query example, API shape)
- **Alternatives** — other approaches you've considered

Check the [Roadmap](https://seraphjiang.github.io/fuse/roadmap.html) first — it may already be planned.

## Request a Connector

We prioritize connectors by community demand. Include:

- Datasource name and version
- Your use case (what queries you'd run through Fuse)
- Auth method (IAM, API key, password, OAuth)
- Link to the datasource's query API docs

## Contribute Code

See [CONTRIBUTING.md](https://github.com/seraphjiang/fuse/blob/main/CONTRIBUTING.md) for the full guide. Quick version:

```bash
git clone https://github.com/seraphjiang/fuse
cd fuse
cargo build --release
cargo test --all-targets
```

1. Fork → branch → code → test → commit (with DCO sign-off) → PR
2. All PRs need: `cargo fmt`, `cargo clippy` clean, `cargo test` passing
3. New features need tests

### Good First Issues

Look for issues labeled [`good first issue`](https://github.com/seraphjiang/fuse/labels/good%20first%20issue) — these are scoped, well-documented, and have mentoring available.

### Contribution Ideas

| Area | Examples |
|------|---------|
| Connectors | New datasource (see [Connector Dev Guide](connector-development-guide.md)) |
| Charts | New visualization type (see [Dashboard Guide](dashboard-user-guide.md)) |
| Docs | Fix typos, add examples, translate guides |
| Tests | Improve coverage, add edge cases |
| Performance | Pushdown optimization, caching improvements |

## Stay Updated

- **[Releases](https://github.com/seraphjiang/fuse/releases)** — changelog for each version
- **[Roadmap](https://seraphjiang.github.io/fuse/roadmap.html)** — what's shipped, in progress, and planned
- **[Blog](https://seraphjiang.github.io/fuse/blog-sprint-5-release.html)** — release announcements

## Code of Conduct

We follow the [Amazon Open Source Code of Conduct](https://aws.github.io/code-of-conduct). Be respectful, constructive, and inclusive.

## Security

Found a vulnerability? **Do not open a public issue.** See [SECURITY.md](https://github.com/seraphjiang/fuse/blob/main/SECURITY.md) for responsible disclosure.

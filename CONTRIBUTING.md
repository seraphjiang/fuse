# Contributing to Fuse

Thank you for your interest in Fuse! This document explains how to get
involved — whether you're fixing a bug, adding a feature, or building a new
connector.

## Getting Started

1. Fork the repository and clone your fork.
2. Create a feature branch from `main`:
   ```bash
   git checkout -b feat/my-change
   ```
3. Make your changes, commit, and push to your fork.
4. Open a Pull Request against `main`.

## Developer Certificate of Origin (DCO)

All commits **must** include a `Signed-off-by` line certifying that you wrote
or have the right to submit the code under the project's license. Add it with
the `-s` flag:

```bash
git commit -s -m "feat: add Prometheus connector"
```

This produces:

```
feat: add Prometheus connector

Signed-off-by: Your Name <your.email@example.com>
```

PRs without DCO sign-off will not be merged.

## Code Style

- **Format**: Run `cargo fmt --all` before committing. CI enforces this.
- **Lint**: Run `cargo clippy --all-targets -- -D warnings`. Zero warnings required.
- **Commit messages**: Follow [Conventional Commits](https://www.conventionalcommits.org/)
  (`feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`).

## Pull Request Checklist

Before submitting your PR, verify:

- [ ] `cargo fmt --all -- --check` passes
- [ ] `cargo clippy --all-targets -- -D warnings` passes
- [ ] `cargo test --all-targets` passes
- [ ] No new compiler warnings
- [ ] Documentation updated (README, doc comments, guides) if applicable
- [ ] New public APIs have doc comments
- [ ] Commit messages follow Conventional Commits with DCO sign-off

## What to Contribute

### Bug Fixes & Features

1. Check [existing issues](https://github.com/seraphjiang/fuse/issues) first.
2. For large changes, open an issue to discuss the approach before coding.
3. Include tests for new functionality.

### New Connectors

We welcome connectors for any datasource. Follow the step-by-step guide:

📖 **[Writing a Fuse Connector](docs/guides/writing-a-connector.md)**

The fastest way to start: copy `crates/fuse-connectors/example/` — a minimal
working connector with inline comments explaining every method.

The guide covers the `FederatedConnector` trait, factory registration, config,
and testing patterns. The OpenSearch connector is the reference implementation.

When submitting a connector PR, also include:
- A sample `[[connector]]` block for `fuse.toml`
- Integration tests with a mock connector
- A brief section in the README or a standalone doc

### Documentation

Docs improvements are always welcome — typo fixes, better examples, new guides.
No issue required for small doc changes.

## Reporting Issues

Use the GitHub issue templates:

- **Bug Report** — Include steps to reproduce, expected vs actual behavior, and
  your environment (OS, Rust version, OpenSearch version).
- **Feature Request** — Describe the problem, your proposed solution, and
  alternatives you've considered.
- **Connector Request** — Describe the datasource, your use case, auth method,
  and example queries.

## Development Setup

```bash
# Prerequisites: Rust stable, libssl-dev, Docker (optional)
./scripts/setup-dev.sh

# Build & test
cargo check
cargo test --all-targets

# Local dev with OpenSearch
docker compose up -d
FUSE_CONFIG=fuse.toml cargo run -p fuse-server
# Open http://localhost:9400/ for the query playground
```

## Code of Conduct

This project follows the
[Amazon Open Source Code of Conduct](https://aws.github.io/code-of-conduct).
Please report unacceptable behavior to the project maintainers.

## License

By contributing, you agree that your contributions will be licensed under the
Apache License 2.0.

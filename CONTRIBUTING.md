# Contributing to Fuse

Thank you for your interest in Fuse! Whether you're fixing a bug, adding a connector, or improving docs — this guide covers everything you need.

## Development Setup

### Prerequisites

- Rust stable (1.85+): `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
- OpenSSL dev: `apt install libssl-dev pkg-config` (Debian/Ubuntu) or `yum install openssl-devel` (RHEL)
- Docker (optional, for local OpenSearch)

### Build and Test

```bash
git clone https://github.com/seraphjiang/fuse
cd fuse

# Verify prerequisites
./scripts/setup-dev.sh

# Build
cargo build --release

# Run all tests (unit + integration)
cargo test --all-targets

# Lint
cargo clippy --all-targets -- -D warnings

# Format check
cargo fmt --all -- --check
```

### Run Locally

```bash
# Option 1: With Docker (starts OpenSearch + sample data)
docker compose up -d
cargo run -p fuse-server

# Option 2: With your own datasources
# Edit fuse.toml, then:
cargo run -p fuse-server -- --config fuse.toml
```

Open [http://localhost:9400](http://localhost:9400) for the playground, [http://localhost:9400/dashboard](http://localhost:9400/dashboard) for dashboards.

## Code Style

### Formatting and Linting

- `cargo fmt --all` before every commit. CI enforces this.
- `cargo clippy --all-targets -- -D warnings` — zero warnings required.
- No `unwrap()` in handler/execution paths. Use `?` or explicit error handling.

### Naming Conventions

| Item | Convention | Example |
|------|-----------|---------|
| Crates | `fuse-connector-{name}` | `fuse-connector-mongodb` |
| Structs | PascalCase | `MongoDbConnector` |
| Traits | PascalCase | `FederatedConnector` |
| Functions | snake_case | `translate_filter` |
| Constants | SCREAMING_SNAKE | `DEFAULT_TIMEOUT_MS` |
| Config keys | snake_case | `max_concurrent_queries` |
| Connector type strings | lowercase kebab | `"csv-json"`, `"s3-o11y"` |

### Inclusive Language

| Don't use | Use instead |
|-----------|-------------|
| master | primary, main |
| slave | replica, secondary |
| whitelist | allowlist |
| blacklist | denylist |

## Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types:** `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `chore`, `ci`

**Rules:**
- Imperative mood: "Add" not "Added" or "Adds"
- No period at end of subject
- Subject ≤ 50 characters
- Body wrapped at 72 characters
- Explain what and why, not how

**Example:**
```
feat(connector): Add MongoDB connector with BSON filter pushdown

Implement FederatedConnector for MongoDB with filter translation to
BSON query documents, projection pushdown, and limit support.
Connection pooling via the official mongodb driver.
```

## Pull Request Process

1. **Branch** from `main`:
   ```bash
   git checkout -b feat/my-change
   ```

2. **Build and test** before committing:
   ```bash
   cargo fmt --all
   cargo clippy --all-targets -- -D warnings
   cargo test --all-targets
   ```

3. **Commit** with DCO sign-off:
   ```bash
   git commit -s -m "feat: description"
   ```

4. **Push** and open a PR against `main`.

### PR Checklist

- [ ] `cargo fmt --all -- --check` passes
- [ ] `cargo clippy --all-targets -- -D warnings` passes
- [ ] `cargo test --all-targets` passes with 0 failures
- [ ] New code has tests (at least one per public function)
- [ ] New public APIs have doc comments
- [ ] Documentation updated if applicable
- [ ] Commit messages follow Conventional Commits with DCO sign-off

### Developer Certificate of Origin (DCO)

All commits must include `Signed-off-by` certifying you have the right to submit the code. Add with `-s`:

```bash
git commit -s -m "feat: add Prometheus connector"
```

## How to Add a Connector

We welcome connectors for any datasource. Full tutorial:

📖 **[Connector Development Guide](docs/guides/connector-development-guide.md)**

Quick summary:
1. Copy `crates/fuse-connectors/example/` as a template
2. Implement `FederatedConnector` (8 methods) and `ConnectorFactory`
3. Register in `crates/fuse-server/src/main.rs`
4. Add tests (unit + SDK `smoke_test`)
5. Add sample config block for `fuse.toml`

Reference implementations: OpenSearch (full pushdown), DynamoDB (filter translation), PostgreSQL (SQL passthrough), MongoDB (BSON).

## How to Add a Chart Type

The visualization platform uses [Apache ECharts](https://echarts.apache.org/). To add a new chart type:

1. Edit `playground/index.html` — add an `<option>` to the `#chart-type` select
2. Add a case in `buildChartOption()` that returns an ECharts option object
3. Optionally update `suggestChart()` auto-detection rules
4. Test in the playground with representative data

For dashboard panels, the same chart types are available in `playground/dashboard.html`.

See the [Dashboard User Guide](docs/guides/dashboard-user-guide.md) for chart type descriptions and data patterns.

## Reporting Issues

Use [GitHub Issues](https://github.com/seraphjiang/fuse/issues) with these templates:

### Bug Report

Include:
- Steps to reproduce (query, config, curl command)
- Expected vs actual behavior
- Error message or response body
- Environment: OS, Rust version, connector type and version
- Output of `curl http://localhost:9400/api/fuse/health`

### Feature Request

Include:
- Problem description (what you're trying to do)
- Proposed solution
- Alternatives considered
- Example query or API call showing desired behavior

### Connector Request

Include:
- Datasource name and version
- Your use case (what queries you'd run)
- Auth method (API key, IAM, password, etc.)
- Link to the datasource's query API docs

## Project Structure

```
crates/
├── fuse-core/           # Connector trait, SubQuery, config, errors
├── fuse-engine/         # DataFusion planner, PPL parser, JOINs, caching
├── fuse-connectors/     # 14 connector implementations + example template
├── fuse-connector-sdk/  # MockConnector, smoke_test, assertion helpers
└── fuse-server/         # REST API (axum), streaming, rate limiting
playground/              # Query playground + dashboard UI
docs/                    # Guides, API spec, architecture, RFCs
```

## Useful Links

- [Architecture](docs/architecture.md) — how federated query execution works
- [API Reference](docs/guides/api-reference-guide.md) — all endpoints with examples
- [Performance Tuning](docs/guides/performance-tuning-guide.md) — optimization guide
- [Migration Guide](docs/guides/migration-guide.md) — OpenSearch → Fuse
- [OpenAPI Spec](docs/api/openapi.yaml) — machine-readable API definition

## Code of Conduct

This project follows the [Amazon Open Source Code of Conduct](https://aws.github.io/code-of-conduct). Report unacceptable behavior to the project maintainers.

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.

## Multi-Agent Development

Fuse uses a multi-agent hive development model with sprint-based planning:

- **Agents:** pm (coordination), ai-lead (engine/connectors), fee (frontend/docs), sde (infrastructure/SDKs), security (hardening/review)
- **Sprints:** 2-week cycles with backlog in `.fuse-project/backlog/`
- **Steering rules:** See `.fuse-project/team/STEERING.md` (19 rules)
- **Key rules:** Every change needs tests, build must be green, announce `[WORKING]` on shared files, check `git log` before starting any item

### Avoiding Duplicate Work

1. Always `tell` the specific agent AND broadcast assignments
2. Agents must check `git log --oneline -20` before starting any item
3. Any agent adding fields to shared structs (AppState, TenantConfig) must update ALL test constructors in the same commit

# API Stability Guarantee

Fuse follows [Semantic Versioning 2.0.0](https://semver.org/). This document defines what is covered by the stability guarantee and how changes are communicated.

## Version Format

```
MAJOR.MINOR.PATCH
```

- **MAJOR** (1.x → 2.x) — breaking changes to the public API
- **MINOR** (1.0 → 1.1) — new features, backward-compatible
- **PATCH** (1.0.0 → 1.0.1) — bug fixes, backward-compatible

## What's Covered

The stability guarantee applies to:

| Surface | Covered | Examples |
|---------|---------|---------|
| REST API endpoints | ✅ | `/api/fuse/query`, `/api/fuse/datasources` |
| REST API request/response JSON shapes | ✅ | Field names, types, nesting |
| SQL syntax | ✅ | SELECT, JOIN, UNION, WHERE, GROUP BY, window functions |
| PPL syntax | ✅ | source, where, stats, eval, sort, head, lookup |
| Config file format (`fuse.toml`) | ✅ | `[[datasource]]`, `[engine]`, `[[tenant]]`, `[[api_key]]` |
| CLI commands and flags | ✅ | `fuse query`, `fuse serve`, `fuse health` |
| SDK public APIs | ✅ | `FuseClient.query()`, `FuseClient.explain()` |
| HTTP status codes | ✅ | 200, 400, 401, 403, 404, 429, 500 |
| Exit codes | ✅ | 0–5 as documented in CLI reference |

## What's Not Covered

| Surface | Not Covered | Reason |
|---------|-------------|--------|
| Internal Rust APIs | ❌ | Crate internals may change between minor versions |
| WASM plugin SDK | ❌ (until v2.0) | Plugin interface is experimental |
| Query plan format | ❌ | EXPLAIN output structure may change |
| Prometheus metric names | ❌ | Metrics may be added, renamed, or removed |
| Log format/messages | ❌ | Log output is for debugging, not parsing |
| Performance characteristics | ❌ | Latency and throughput may vary |

## Backward Compatibility Rules

### Adding (non-breaking)

These changes are allowed in MINOR releases:
- New REST API endpoints
- New optional fields in request/response JSON
- New SQL/PPL functions and syntax
- New config file sections and optional keys
- New CLI commands and optional flags
- New SDK methods

### Changing (breaking)

These changes require a MAJOR release:
- Removing or renaming REST API endpoints
- Removing or renaming JSON fields
- Changing the type of existing JSON fields
- Removing SQL/PPL syntax
- Removing or renaming config keys
- Removing CLI commands or flags
- Removing SDK methods
- Changing HTTP status codes for existing error conditions

## Deprecation Process

1. **Announce** — deprecated feature is documented in release notes with `[DEPRECATED]` tag
2. **Warn** — server logs a warning when the deprecated feature is used
3. **Minimum lifetime** — deprecated features remain functional for at least 2 minor releases
4. **Remove** — feature is removed in the next major release

### Example Timeline

```
v1.2.0 — /api/fuse/old-endpoint marked [DEPRECATED], warning logged on use
v1.3.0 — still functional, warning logged
v1.4.0 — still functional, warning logged
v2.0.0 — removed
```

### Deprecation in Config

Deprecated config keys are accepted with a warning:

```
WARN: Config key 'rate_limit' is deprecated. Use 'rate_limit_global' instead. Will be removed in v2.0.0.
```

## API Versioning

The REST API uses URL-based versioning when breaking changes are needed:

```
/api/fuse/query          ← current (v1)
/api/v2/fuse/query       ← future breaking change
```

Both versions run simultaneously during the transition period. The unversioned path always points to the latest stable version.

## SDK Versioning

SDKs follow the same semver as the server:

| Server | Python SDK | TypeScript SDK |
|--------|-----------|---------------|
| 1.0.x | 1.0.x | 1.0.x |
| 1.1.x | 1.1.x | 1.1.x |
| 2.0.x | 2.0.x | 2.0.x |

SDKs are backward-compatible with older server versions within the same major version. A v1.1 SDK works with a v1.0 server (new methods return errors for unsupported endpoints).

## Reporting Compatibility Issues

If you encounter a backward-incompatible change in a minor or patch release, [open a GitHub issue](https://github.com/seraphjiang/fuse/issues/new) with the `compatibility` label. These are treated as P0 bugs.

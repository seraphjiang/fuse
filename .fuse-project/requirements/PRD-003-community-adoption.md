# PRD-003: Community Adoption Plan

**Status:** Draft
**Date:** 2026-04-09
**Priority:** P1

## Goal

Get early adopters using Fuse and contributing connectors. Build credibility
in the OpenSearch community before proposing upstream integration.

## Current Readiness Assessment

### What's Ready ✅
- Core engine with DataFusion federation (SQL + PPL)
- 3 connectors: OpenSearch, S3/Parquet, Prometheus
- Connector SDK with prelude, test utilities, and authoring guide
- REST API (7 endpoints) with OpenAPI spec
- Cross-type JOIN execution (hash-join, semi-join)
- Cost-based query optimizer
- Spark delegation interface
- Live playground at https://fuse.huanji.profile.aws.dev
- CI/CD pipeline (CodeCommit → CodeBuild → ECS)
- 70+ tests passing

### Gaps for Community Adoption

| Gap | Impact | Priority | Backlog |
|-----|--------|----------|---------|
| No OSD plugin | Users can't use Fuse from Dashboards UI | High | #070 |
| No published crate on crates.io | Can't `cargo add fuse-connector-sdk` | High | #071 |
| No demo video / blog post | Hard to understand value without seeing it | High | #072 |
| Playground has no real federated demo data | Can't show cross-cluster query in action | High | #073 |
| No CONTRIBUTING.md with DCO/CLA | Blocks external PRs | Medium | #074 |
| No GitHub Issues templates | Poor contributor experience | Medium | #075 |
| No GitHub Actions CI (only CodePipeline) | PRs from forks can't run CI | Medium | #076 |
| No benchmarks published | Can't prove performance claims | Medium | #077 |
| No error handling guide | Connector authors hit walls | Low | #078 |
| No changelog automation | Hard to track releases | Low | #079 |

## Adoption Strategy

### Phase A: Developer Preview (Weeks 1-2)

**Goal:** Get 5-10 developers to try Fuse and give feedback.

1. Publish `fuse-connector-sdk` to crates.io
2. Write a blog post: "Introducing Fuse: Federated Queries Across OpenSearch, S3, and Prometheus"
3. Record a 5-min demo video showing:
   - Query across 2 OpenSearch clusters
   - JOIN OpenSearch logs with S3 archived data
   - Correlate logs with Prometheus metrics
4. Post to OpenSearch community forum
5. Share in relevant Slack channels / Discord
6. Create GitHub Issues templates (bug, feature, connector request)
7. Add CONTRIBUTING.md with DCO sign-off requirement

### Phase B: Connector Ecosystem (Weeks 3-4)

**Goal:** Get community-contributed connectors.

1. Publish "Write Your First Connector" tutorial (docs/guides/writing-a-connector.md exists)
2. Create connector request template on GitHub
3. Seed with "good first connector" issues:
   - CloudWatch Logs connector
   - DynamoDB connector
   - PostgreSQL/MySQL (JDBC) connector
   - Elasticsearch connector (for migration users)
4. Offer to pair-program with first connector contributors
5. Publish connector compatibility matrix

### Phase C: OSD Integration Proposal (Weeks 5-8)

**Goal:** Propose Fuse as an official OpenSearch Dashboards plugin.

1. Build OSD plugin that:
   - Adds "Federated Query" tab to Discover
   - Extends query bar with multi-datasource selector
   - Registers Fuse API as a backend
2. Open RFC on opensearch-project/OpenSearch-Dashboards
3. Present at OpenSearch community meeting
4. Address feedback, iterate on design
5. Submit PR for review

## Actionable Items

| ID | Item | Owner | Priority | Status |
|----|------|-------|----------|--------|
| 070 | OSD plugin (basic: query bar + results) | — | P1 | todo |
| 071 | Publish fuse-connector-sdk to crates.io | — | P1 | todo |
| 072 | Blog post + demo video | — | P1 | todo |
| 073 | Federated demo data (logs across 2 clusters) | infra | P1 | todo |
| 074 | CONTRIBUTING.md with DCO | — | P1 | todo |
| 075 | GitHub Issues templates (bug, feature, connector) | — | P2 | todo |
| 076 | GitHub Actions CI for fork PRs | explorer | P2 | done (.github/workflows/ci.yml exists) |
| 077 | Performance benchmarks (benches/) | — | P2 | todo |
| 078 | Error handling guide for connector authors | — | P3 | todo |
| 079 | Changelog automation (release-please or similar) | — | P3 | todo |
| 080 | OpenSearch community forum post | — | P1 | todo |
| 081 | RFC on opensearch-project/OpenSearch-Dashboards | — | P1 | todo (after OSD plugin) |

## Success Metrics

| Metric | Target | Timeframe |
|--------|--------|-----------|
| GitHub stars | 50 | 4 weeks |
| External contributors | 3 | 6 weeks |
| Community connectors | 2 | 8 weeks |
| OpenSearch forum engagement | 20 replies | 4 weeks |
| OSD plugin RFC accepted | Yes | 8 weeks |

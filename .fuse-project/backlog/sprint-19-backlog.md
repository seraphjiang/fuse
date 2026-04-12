# Sprint 19 Backlog

Seeded from overnight run findings (2026-04-12).

| ID | Item | Priority | Owner | Status | Notes |
|----|------|----------|-------|--------|-------|
| 1900 | Fix cross-type JOIN: OpenSearch↔DynamoDB "no common column" | P0 | sde | done | test agent found: both tables have user_id but JOIN fails. Likely schema type mismatch. |
| 1901 | DynamoDB connector: 500 on nonexistent tables → clean 404 | P1 | sde | done | test agent found during fuzz/endpoint testing. |
| 1902 | Webhook/CDC auth: hardcoded auth_enabled=true | P1 | security | done | sde found: require_role rejects even when auth disabled globally. |
| 1903 | Chaos tests: add #[serial] or per-test state | P2 | — | todo | Global static state causes flaky tests under parallel execution. |
| 1904 | DynamoDB connector: consistent 500s on playground | P2 | — | todo | test agent fuzz testing: 92% clean, DDB always 500s (IAM/config?). |
| 1905 | Anomaly trend break test: flaky under parallel | P3 | — | todo | Passes solo, fails with --test-threads>1. Float sensitivity. |
| 1906 | Server returns 500 for timeout + oversized queries | P2 | — | todo | test agent chaos testing: should return 408/413 not 500. |
| 1907 | 14 API endpoints not yet deployed to playground | P2 | — | todo | test agent endpoint coverage: 22/36 pass, 14 skipped. |

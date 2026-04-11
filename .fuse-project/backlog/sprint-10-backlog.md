# Sprint 10 Backlog — Federation & Advanced Features

**Sprint:** 10
**Start:** 2026-04-11
**Focus:** Fuse-to-Fuse federation, VS Code extension, Go SDK, advanced SQL

## P0: Federation

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1000 | Fuse-to-Fuse connector | planner | todo | Connect Fuse instances, federate across clusters |
| 1001 | Cross-cluster query routing | planner | todo | Route subqueries to appropriate Fuse instance |
| 1002 | Federation health + topology API | explorer | todo | GET /api/fuse/federation — show connected instances |

## P1: Developer Experience

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1010 | VS Code extension | frontend | todo | Syntax highlighting, query execution, results panel, IntelliSense |
| 1011 | Go SDK client | explorer | todo | Same API surface as Python/TypeScript |
| 1012 | CLI tool (fuse-cli) | explorer | todo | fuse query, fuse health, fuse datasources, fuse explain |

## P1: Advanced SQL

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1020 | CREATE TABLE AS SELECT (CTAS) | planner | todo | Materialize query results into a new datasource |
| 1021 | INSERT INTO ... SELECT | planner | todo | Write query results to a writable connector |
| 1022 | Transaction support (BEGIN/COMMIT/ROLLBACK) | planner | todo | Multi-statement transactions for writable connectors |

## P2: Testing & Docs

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1030 | Federation integration tests | tester | todo | 2 Fuse instances, cross-cluster JOIN |
| 1031 | VS Code extension E2E tests | tester | todo | Extension activation, query execution, results |
| 1032 | CLI tool tests | tester | todo | All subcommands, error handling |
| 1040 | Federation architecture guide | docs | todo | Multi-cluster setup, routing, topology |
| 1041 | VS Code extension guide | docs | todo | Install, configure, usage |
| 1042 | CLI reference | docs | todo | All commands, flags, examples |

## P2: Connectors

| ID | Item | Owner | Status | Notes |
|----|------|-------|--------|-------|
| 1050 | Apache Spark connector | general | todo | Spark SQL, Thrift/Arrow Flight |
| 1051 | Amazon Athena connector | general | todo | SQL pushdown, S3 results |

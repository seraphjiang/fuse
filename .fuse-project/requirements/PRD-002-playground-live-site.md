# PRD-002: Playground & Live Test Site

**Status:** Approved
**Date:** 2026-04-09
**Priority:** P0

## Goal

Deploy Fuse as a live test site accessible via VPN for interactive testing.
Dual-push to GitHub + CodeCommit, CI/CD via AWS CodePipeline.

## Architecture

```
GitHub + CodeCommit (dual remote, git push hits both)
        │
   CodePipeline (triggered by CodeCommit push)
        │
   CodeBuild (cargo build --release, Docker image)
        │
   ECR (fuse-server image)
        │
   ECS Fargate (fuse-server container)
        │
   ALB (internal, IP-restricted)
        │
   OpenSearch Serverless (test data)
```

## AWS Account

- Account: 544277935543
- Region: us-west-2

## Access Control

NOT public. Restricted to Amazon VPN IPs only.

Initial allowlist:
- 205.251.233.0/24 (Amazon corporate VPN — GlobalProtect)

Known Amazon VPN CIDR blocks (to be confirmed/expanded):
- 205.251.233.0/24
- 72.21.198.0/24
- 54.239.0.0/16 (Amazon corporate range)

User will add more IPs later. ALB security group controls access.

## Requirements

### Infrastructure
- [ ] CodeCommit repo `fuse` in us-west-2
- [ ] Dual remote: `git push` sends to both GitHub and CodeCommit
- [ ] ECR repository for fuse-server Docker image
- [ ] CodePipeline: CodeCommit → CodeBuild → ECR → ECS deploy
- [ ] CodeBuild project with Rust toolchain (cargo build --release)
- [ ] ECS Fargate cluster + service + task definition
- [ ] ALB with IP-restricted security group (Amazon VPN only)
- [ ] OpenSearch Serverless collection for test data

### Application
- [ ] Dockerfile for fuse-server (multi-stage: build + runtime)
- [ ] docker-compose.yml for local dev (fuse-server + OpenSearch)
- [ ] Health check endpoint works on ECS
- [ ] fuse.toml configured to point at OpenSearch Serverless

### CI/CD
- [ ] Push to main triggers pipeline
- [ ] Build, test, deploy automated
- [ ] Rollback on failed health check

## Success Criteria

- `git push` deploys to ECS within 10 minutes
- ALB endpoint returns 200 on GET /api/fuse/health
- POST /api/fuse/query returns results from OpenSearch Serverless
- Only accessible from Amazon VPN IPs

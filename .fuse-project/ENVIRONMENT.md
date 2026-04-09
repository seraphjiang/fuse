# Fuse — Environment & Deployment Info

Shared context for all agents and contributors. Read this first.

## Live Playground

| Item | Value |
|------|-------|
| URL | https://fuse.huanji.profile.aws.dev |
| ALB | fuse-playground-alb-556139505.us-west-2.elb.amazonaws.com |
| Health | GET https://fuse.huanji.profile.aws.dev/api/fuse/health |
| Access | Amazon VPN only (205.251.233.0/24) |
| Region | us-west-2 |
| Account | 544277935543 |

## Source Repositories

| Remote | URL |
|--------|-----|
| GitHub | https://github.com/seraphjiang/fuse |
| CodeCommit | https://git-codecommit.us-west-2.amazonaws.com/v1/repos/fuse |

`git push origin main` pushes to BOTH remotes. CodeCommit push triggers the CI/CD pipeline.

## CI/CD Pipeline

```
git push → CodeCommit → CodePipeline (fuse-pipeline)
                              │
                        CodeBuild (fuse-build)
                              │
                        ECR (544277935543.dkr.ecr.us-west-2.amazonaws.com/fuse-server)
                              │
                        ECS Fargate (fuse-playground cluster, fuse-server service)
                              │
                        ALB → https://fuse.huanji.profile.aws.dev
```

## AWS Resources

| Resource | Name / ID |
|----------|-----------|
| ECS Cluster | fuse-playground |
| ECS Service | fuse-server |
| ECR Repo | fuse-server |
| ALB | fuse-playground-alb |
| ALB SG | sg-0ca2bb0b962384d87 (inbound 80/443 from VPN) |
| ECS SG | sg-09a1ba60cd4a3eaa2 (inbound 9400 from ALB) |
| Target Group | fuse-server-tg (health: GET /api/fuse/health) |
| CodePipeline | fuse-pipeline |
| CodeBuild | fuse-build |
| ACM Cert | arn:aws:acm:us-west-2:544277935543:certificate/fe32fac4-5e0e-40f4-bcf4-954c06541962 |
| OS Serverless | fuse-cluster-a (epk7ap540halh4ufyff6), fuse-cluster-b (wg2fj60hpfsc9ziwv0u0) |

## API Endpoints (Playground)

```bash
# Health
curl https://fuse.huanji.profile.aws.dev/api/fuse/health

# List datasources
curl https://fuse.huanji.profile.aws.dev/api/fuse/datasources

# Query (SQL)
curl -X POST https://fuse.huanji.profile.aws.dev/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.logs WHERE status = 500", "format": "sql"}'

# Query (PPL)
curl -X POST https://fuse.huanji.profile.aws.dev/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "source = cluster_a.logs | where status >= 500 | stats count() by service", "format": "ppl"}'

# Validate
curl -X POST https://fuse.huanji.profile.aws.dev/api/fuse/query/validate \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.logs", "format": "sql"}'

# Explain
curl -X POST https://fuse.huanji.profile.aws.dev/api/fuse/query/explain \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.logs WHERE status = 500", "format": "sql"}'
```

## Local Development

```bash
# Prerequisites
source ~/.cargo/env
cd ~/wss/fuse

# Build
cargo build --release

# Test
cargo test --all-targets

# Run locally
FUSE_CONFIG=fuse.toml cargo run -p fuse-server

# Docker
docker-compose up
```

## Key Files

| File | Purpose |
|------|---------|
| fuse.toml | Connector config (local dev) |
| Dockerfile | Multi-stage Rust build |
| buildspec.yml | CodeBuild spec |
| docker-compose.yml | Local dev (fuse-server + OpenSearch) |
| playground/index.html | Query playground UI |
| .fuse-project/backlog/backlog.md | Work items |
| .fuse-project/team/RECOVERY.md | Team rebuild guide |

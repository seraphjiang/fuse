# CI/CD Architecture

## Overview

```
Developer pushes to main
        │
        ├──→ GitHub (github.com/seraphjiang/fuse)
        │         └── GitHub Actions (lint, test)
        │
        └──→ CodeCommit (us-west-2)
                  └── CodePipeline
                        │
                   CodeBuild (Docker build + push to ECR)
                        │
                   ECS Fargate Deploy
                        │
                   ALB (VPN-only)
                        │
                   fuse.huanji.profile.aws.dev
```

## Dual Remote Setup

`git push origin` pushes to both GitHub and CodeCommit simultaneously.

```bash
# Verify remotes
git remote -v
# origin  https://github.com/seraphjiang/fuse.git (fetch)
# origin  https://git-codecommit.us-west-2.amazonaws.com/v1/repos/fuse (push)
# origin  https://github.com/seraphjiang/fuse.git (push)
```

## AWS Resources (us-west-2, account 544277935543)

| Resource | Name / ARN | Purpose |
|----------|-----------|---------|
| CodeCommit | fuse | Source repo (triggers pipeline) |
| ECR | fuse-server | Docker image registry |
| CodeBuild | fuse-build | Builds Docker image, pushes to ECR |
| ECS Cluster | fuse-playground | Fargate cluster |
| ECS Service | fuse-server | Runs fuse-server container (512 CPU, 1024 MiB) |
| ALB | fuse-playground-alb | Load balancer, HTTP:80 → ECS:9400 |
| ALB SG | sg-0ca2bb0b962384d87 | Inbound 80/443 from 205.251.233.0/24 only |
| ECS SG | sg-09a1ba60cd4a3eaa2 | Inbound 9400 from ALB SG only |
| Target Group | fuse-server-tg | Health check: GET /api/fuse/health |
| CodePipeline | fuse-pipeline | CodeCommit → CodeBuild → ECS deploy |
| S3 | fuse-pipeline-artifacts-544277935543 | Pipeline artifacts |

## Pipeline Stages

### Stage 1: Source (CodeCommit)
- Branch: main
- Trigger: push to main

### Stage 2: Build (CodeBuild)
- Buildspec: `buildspec.yml`
- Builds multi-stage Docker image (Rust compile → slim runtime)
- Pushes to ECR with `latest` and commit-hash tags
- Outputs `imagedefinitions.json` for ECS deploy

### Stage 3: Deploy (ECS)
- Rolling update to ECS Fargate service
- Health check must pass before old task is drained
- Rollback on failed health check

## Custom Domain: fuse.huanji.profile.aws.dev

### Setup Steps (TODO)
1. Create Route 53 hosted zone for `huanji.profile.aws.dev` (or use existing)
2. Request ACM certificate for `fuse.huanji.profile.aws.dev` in us-west-2
3. Add HTTPS:443 listener to ALB with ACM cert
4. Create Route 53 A record (alias) pointing `fuse.huanji.profile.aws.dev` → ALB
5. Update ALB security group to allow 443

### DNS Record
```
fuse.huanji.profile.aws.dev → ALIAS → fuse-playground-alb-556139505.us-west-2.elb.amazonaws.com
```

## Access Control

NOT public. Restricted to Amazon VPN IPs.

Current allowlist (ALB security group):
- 205.251.233.0/24 (Amazon corporate VPN)

To add more IPs:
```bash
aws ec2 authorize-security-group-ingress \
  --group-id sg-0ca2bb0b962384d87 \
  --protocol tcp --port 80 --cidr <NEW_CIDR> \
  --region us-west-2
```

## Deployment Flow

```bash
# 1. Make changes
# 2. Build locally to verify
source ~/.cargo/env && cargo check && cargo test

# 3. Commit and push (triggers pipeline)
git add -A && git commit -m "feat: ..." && git push origin main

# 4. Monitor pipeline
aws codepipeline get-pipeline-state --name fuse-pipeline --region us-west-2

# 5. Check deployment
curl http://fuse-playground-alb-556139505.us-west-2.elb.amazonaws.com/api/fuse/health
```

## Rollback

```bash
# Manual rollback to previous task definition
aws ecs update-service \
  --cluster fuse-playground \
  --service fuse-server \
  --task-definition fuse-server:<PREVIOUS_REVISION> \
  --region us-west-2
```

## Local Development

```bash
# Start local OpenSearch + fuse-server
docker-compose up

# Or run natively
cargo run -p fuse-server
# (requires fuse.toml with connector config)
```

# Fuse Playground — Deployment & Operations Guide

## Architecture

```
GitHub + CodeCommit (dual remote)
        │
   CodePipeline (fuse-pipeline)
        │
   CodeBuild (fuse-build, ~15 min)
        │
   ECR (fuse-server:latest)
        │
   ECS Fargate (fuse-playground cluster)
        │
   ALB (fuse-playground-alb, HTTPS only)
        │
   Route 53 (fuse.huanji.profile.aws.dev)
```

## AWS Resources (us-west-2, account 544277935543)

| Resource | Name/ID |
|----------|---------|
| CodeCommit | `fuse` |
| ECR | `fuse-server` (544277935543.dkr.ecr.us-west-2.amazonaws.com/fuse-server) |
| CodeBuild | `fuse-build` |
| CodePipeline | `fuse-pipeline` |
| ECS Cluster | `fuse-playground` |
| ECS Service | `fuse-server` (Fargate, 512 CPU, 1024 MiB, auto-scales 1-3) |
| ALB | `fuse-playground-alb` (fuse-playground-alb-556139505.us-west-2.elb.amazonaws.com) |
| ALB SG | `sg-0ca2bb0b962384d87` (inbound 80/443 from 205.251.233.0/24) |
| ECS SG | `sg-09a1ba60cd4a3eaa2` (inbound 9400 from ALB SG) |
| Target Group | `fuse-server-tg` (port 9400, health: GET /api/fuse/health) |
| ACM Cert | `fe32fac4-5e0e-40f4-bcf4-954c06541962` |
| Route 53 | `fuse.huanji.profile.aws.dev` → ALB alias |
| S3 Artifacts | `fuse-pipeline-artifacts-544277935543` |
| CloudWatch | Dashboard: `fuse-playground`, Alarms: `fuse-playground-5xx`, `fuse-playground-unhealthy-target` |
| Lambda | `fuse-log-shipper` (CW → S3 O11y) |

## OpenSearch Serverless (us-west-2)

| Collection | ID | Endpoint |
|------------|-----|----------|
| fuse-cluster-a | epk7ap540halh4ufyff6 | https://epk7ap540halh4ufyff6.us-west-2.aoss.amazonaws.com |
| fuse-cluster-b | wg2fj60hpfsc9ziwv0u0 | https://wg2fj60hpfsc9ziwv0u0.us-west-2.aoss.amazonaws.com |

Both have `application_logs` index with 220 docs each (440 total).

## IAM Roles

| Role | Purpose |
|------|---------|
| `fuse-codebuild-role` | CodeBuild: ECR push, CW logs, S3 artifacts, CodeCommit pull |
| `fuse-ecs-task-execution-role` | ECS: pull ECR images, CW logs |
| `fuse-ecs-task-role` | App: AOSS access, S3 O11y read |
| `fuse-pipeline-role` | Pipeline: CodeCommit, CodeBuild, ECS deploy, S3 artifacts |
| `fuse-log-shipper-role` | Lambda: CW logs read, S3 write |

## Deploying

Push to `main` triggers the pipeline automatically:
```bash
git push origin main  # pushes to both GitHub and CodeCommit
```

Pipeline stages: Source → Build (~15 min) → Deploy (~3 min)

## Manual Operations

```bash
# Check pipeline status
aws codepipeline get-pipeline-state --name fuse-pipeline --region us-west-2

# Force new ECS deployment (e.g., after IAM changes)
aws ecs update-service --cluster fuse-playground --service fuse-server --force-new-deployment --region us-west-2

# Check target health
aws elbv2 describe-target-health --target-group-arn arn:aws:elasticloadbalancing:us-west-2:544277935543:targetgroup/fuse-server-tg/f7747c07fccfb281 --region us-west-2

# View ECS logs
aws logs tail /ecs/fuse-server --follow --region us-west-2

# Retrigger pipeline
aws codepipeline start-pipeline-execution --name fuse-pipeline --region us-west-2
```

## Access

- URL: https://fuse.huanji.profile.aws.dev
- Restricted to Amazon VPN (205.251.233.0/24)
- HTTP automatically redirects to HTTPS

## Monitoring

- Dashboard: https://us-west-2.console.aws.amazon.com/cloudwatch/home?region=us-west-2#dashboards/dashboard/fuse-playground
- Alarms: 5xx errors (>10 in 5 min), unhealthy targets (3 consecutive checks)
- Logs: CloudWatch `/ecs/fuse-server` → Lambda → `s3://s3-query-logs-544277935543-us-west-1/fuse/`

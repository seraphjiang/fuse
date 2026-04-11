# Fuse on AWS ECS Fargate

Terraform module deploying Fuse with ALB, ECS Fargate, and optional ElastiCache Redis.

## Usage

```hcl
module "fuse" {
  source     = "./deploy/terraform/aws-ecs"
  vpc_id     = "vpc-abc123"
  subnet_ids = ["subnet-a", "subnet-b"]
  image      = "ghcr.io/seraphjiang/fuse-server:1.4.0"
}
```

## Resources Created

- Application Load Balancer (public)
- ECS Fargate cluster + service (2 tasks default)
- ElastiCache Redis (optional, for stateless mode)
- Auto-scaling (CPU-based, 2-10 tasks)
- CloudWatch log group (30-day retention)
- Security groups (ALB → ECS → Redis)
- IAM roles (task execution + task)

## Variables

| Name | Default | Description |
|------|---------|-------------|
| `vpc_id` | required | VPC ID |
| `subnet_ids` | required | Subnet IDs (2+ for HA) |
| `image` | `ghcr.io/.../fuse-server:latest` | Container image |
| `cpu` | 1024 | Task CPU (1 vCPU) |
| `memory` | 2048 | Task memory (2 GB) |
| `desired_count` | 2 | Initial task count |
| `max_count` | 10 | Max auto-scale tasks |
| `redis_enabled` | true | Enable ElastiCache |
| `redis_node_type` | cache.t4g.micro | Redis instance type |

## Outputs

- `alb_dns` — ALB DNS name
- `fuse_url` — Full Fuse URL
- `redis_endpoint` — Redis address

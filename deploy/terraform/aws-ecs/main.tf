# Fuse on AWS ECS Fargate + ALB + ElastiCache Redis
# Usage: terraform init && terraform apply -var="vpc_id=vpc-xxx" -var="subnet_ids=[\"subnet-a\",\"subnet-b\"]"

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = { source = "hashicorp/aws", version = ">= 5.0" }
  }
}

variable "name" { default = "fuse" }
variable "region" { default = "us-west-2" }
variable "vpc_id" { type = string }
variable "subnet_ids" { type = list(string) }
variable "image" { default = "ghcr.io/seraphjiang/fuse-server:latest" }
variable "cpu" { default = 1024 }
variable "memory" { default = 2048 }
variable "desired_count" { default = 2 }
variable "max_count" { default = 10 }
variable "redis_enabled" { default = true }
variable "redis_node_type" { default = "cache.t4g.micro" }
variable "allowed_cidrs" {
  description = "CIDRs allowed to reach the ALB (default: open to all)"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

provider "aws" { region = var.region }

# --- Security Groups ---

resource "aws_security_group" "alb" {
  name_prefix = "${var.name}-alb-"
  vpc_id      = var.vpc_id
  ingress { from_port = 80; to_port = 80; protocol = "tcp"; cidr_blocks = var.allowed_cidrs }
  ingress { from_port = 443; to_port = 443; protocol = "tcp"; cidr_blocks = var.allowed_cidrs }
  egress  { from_port = 0; to_port = 0; protocol = "-1"; cidr_blocks = ["0.0.0.0/0"] }
  tags = { Name = "${var.name}-alb" }
}

resource "aws_security_group" "ecs" {
  name_prefix = "${var.name}-ecs-"
  vpc_id      = var.vpc_id
  ingress { from_port = 9400; to_port = 9400; protocol = "tcp"; security_groups = [aws_security_group.alb.id] }
  egress  { from_port = 0; to_port = 0; protocol = "-1"; cidr_blocks = ["0.0.0.0/0"] }
  tags = { Name = "${var.name}-ecs" }
}

resource "aws_security_group" "redis" {
  count       = var.redis_enabled ? 1 : 0
  name_prefix = "${var.name}-redis-"
  vpc_id      = var.vpc_id
  ingress { from_port = 6379; to_port = 6379; protocol = "tcp"; security_groups = [aws_security_group.ecs.id] }
  tags = { Name = "${var.name}-redis" }
}

# --- ALB ---

resource "aws_lb" "this" {
  name               = var.name
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets            = var.subnet_ids
}

resource "aws_lb_target_group" "this" {
  name        = var.name
  port        = 9400
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"
  health_check {
    path                = "/api/fuse/health"
    interval            = 15
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.this.arn
  port              = 80
  protocol          = "HTTP"
  default_action { type = "forward"; target_group_arn = aws_lb_target_group.this.arn }
}

# --- ElastiCache Redis ---

resource "aws_elasticache_subnet_group" "this" {
  count      = var.redis_enabled ? 1 : 0
  name       = var.name
  subnet_ids = var.subnet_ids
}

resource "aws_elasticache_cluster" "this" {
  count                = var.redis_enabled ? 1 : 0
  cluster_id           = var.name
  engine               = "redis"
  node_type            = var.redis_node_type
  num_cache_nodes      = 1
  port                 = 6379
  subnet_group_name    = aws_elasticache_subnet_group.this[0].name
  security_group_ids   = [aws_security_group.redis[0].id]
}

# --- ECS ---

resource "aws_ecs_cluster" "this" {
  name = var.name
  setting { name = "containerInsights"; value = "enabled" }
}

resource "aws_iam_role" "task_execution" {
  name = "${var.name}-task-exec"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Action = "sts:AssumeRole", Effect = "Allow", Principal = { Service = "ecs-tasks.amazonaws.com" } }]
  })
}

resource "aws_iam_role_policy_attachment" "task_execution" {
  role       = aws_iam_role.task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "task" {
  name = "${var.name}-task"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Action = "sts:AssumeRole", Effect = "Allow", Principal = { Service = "ecs-tasks.amazonaws.com" } }]
  })
}

resource "aws_cloudwatch_log_group" "this" {
  name              = "/ecs/${var.name}"
  retention_in_days = 30
}

resource "aws_ecs_task_definition" "this" {
  family                   = var.name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.cpu
  memory                   = var.memory
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([{
    name      = "fuse-server"
    image     = var.image
    essential = true
    portMappings = [{ containerPort = 9400, protocol = "tcp" }]
    environment = concat(
      [{ name = "RUST_LOG", value = "info" }],
      var.redis_enabled ? [{ name = "FUSE_REDIS_URL", value = "redis://${aws_elasticache_cluster.this[0].cache_nodes[0].address}:6379" }] : []
    )
    logConfiguration = {
      logDriver = "awslogs"
      options   = { "awslogs-group" = aws_cloudwatch_log_group.this.name, "awslogs-region" = var.region, "awslogs-stream-prefix" = "fuse" }
    }
    healthCheck = {
      command     = ["CMD-SHELL", "curl -sf http://localhost:9400/api/fuse/health || exit 1"]
      interval    = 15
      timeout     = 5
      retries     = 3
      startPeriod = 10
    }
  }])
}

resource "aws_ecs_service" "this" {
  name            = var.name
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.this.arn
  desired_count   = var.desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets         = var.subnet_ids
    security_groups = [aws_security_group.ecs.id]
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.this.arn
    container_name   = "fuse-server"
    container_port   = 9400
  }
}

# --- Auto Scaling ---

resource "aws_appautoscaling_target" "this" {
  max_capacity       = var.max_count
  min_capacity       = var.desired_count
  resource_id        = "service/${aws_ecs_cluster.this.name}/${aws_ecs_service.this.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"
}

resource "aws_appautoscaling_policy" "cpu" {
  name               = "${var.name}-cpu"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.this.resource_id
  scalable_dimension = aws_appautoscaling_target.this.scalable_dimension
  service_namespace  = aws_appautoscaling_target.this.service_namespace
  target_tracking_scaling_policy_configuration {
    predefined_metric_specification { predefined_metric_type = "ECSServiceAverageCPUUtilization" }
    target_value = 70.0
  }
}

# --- Outputs ---

output "alb_dns" { value = aws_lb.this.dns_name }
output "fuse_url" { value = "http://${aws_lb.this.dns_name}" }
output "redis_endpoint" { value = var.redis_enabled ? aws_elasticache_cluster.this[0].cache_nodes[0].address : "disabled" }
output "ecs_cluster" { value = aws_ecs_cluster.this.name }
output "ecs_service" { value = aws_ecs_service.this.name }
output "log_group" { value = aws_cloudwatch_log_group.this.name }
output "task_role_arn" { value = aws_iam_role.task.arn }

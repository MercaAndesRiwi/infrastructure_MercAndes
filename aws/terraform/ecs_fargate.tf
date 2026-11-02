# ==============================================================================
# Amazon ECS Cluster and Fargate Task Definitions for APIs (SCRUM-107)
# ==============================================================================

# ECS Cluster
resource "aws_ecs_cluster" "main" {
  name = "mercaandes-ecs-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = {
    Project     = "MercaAndes"
    Environment = var.environment
  }
}

# ECS Task Execution Role (Allows ECS to pull images from ECR and write to CloudWatch)
resource "aws_iam_role" "ecs_execution_role" {
  name = "mercaandes-ecs-task-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_execution_policy" {
  role       = aws_iam_role.ecs_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# ------------------------------------------------------------------------------
# 1. Task Definition: Marketplace API (Fargate)
# ------------------------------------------------------------------------------
resource "aws_ecs_task_definition" "marketplace_api" {
  family                   = "mercaandes-marketplace-api"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.fargate_cpu
  memory                   = var.fargate_memory
  execution_role_arn       = aws_iam_role.ecs_execution_role.arn
  task_role_arn            = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/mercaandes-api-ec2-role"

  container_definitions = jsonencode([
    {
      name      = "marketplace-api"
      image     = var.marketplace_api_image
      essential = true
      portMappings = [
        {
          containerPort = var.app_port
          hostPort      = var.app_port
          protocol      = "tcp"
        }
      ]
      environment = [
        { name = "APP_ENV", value = var.environment },
        { name = "PORT", value = tostring(var.app_port) }
      ]
      healthCheck = {
        command     = ["CMD-SHELL", "curl -f http://localhost:${var.app_port}${var.health_check_path} || exit 1"]
        interval    = 30
        timeout     = 5
        retries     = 3
        startPeriod = 15
      }
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.marketplace_api.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }
    }
  ])

  tags = {
    Project   = "MercaAndes"
    Component = "Marketplace-API"
  }
}

# ------------------------------------------------------------------------------
# 2. Task Definition: Inventory API (Fargate)
# ------------------------------------------------------------------------------
resource "aws_ecs_task_definition" "inventory_api" {
  family                   = "mercaandes-inventory-api"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.fargate_cpu
  memory                   = var.fargate_memory
  execution_role_arn       = aws_iam_role.ecs_execution_role.arn
  task_role_arn            = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/mercaandes-api-ec2-role"

  container_definitions = jsonencode([
    {
      name      = "inventory-api"
      image     = var.inventory_api_image
      essential = true
      portMappings = [
        {
          containerPort = var.app_port
          hostPort      = var.app_port
          protocol      = "tcp"
        }
      ]
      environment = [
        { name = "APP_ENV", value = var.environment },
        { name = "PORT", value = tostring(var.app_port) }
      ]
      healthCheck = {
        command     = ["CMD-SHELL", "curl -f http://localhost:${var.app_port}${var.health_check_path} || exit 1"]
        interval    = 30
        timeout     = 5
        retries     = 3
        startPeriod = 15
      }
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.inventory_api.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }
    }
  ])

  tags = {
    Project   = "MercaAndes"
    Component = "Inventory-API"
  }
}

# Data source for account ID
data "aws_caller_identity" "current" {}

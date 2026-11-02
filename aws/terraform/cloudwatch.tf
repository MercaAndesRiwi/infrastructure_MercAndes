# ==============================================================================
# CloudWatch Log Groups Configuration (SCRUM-109 / US-01)
# Centralized logging for the 5 components:
# 1. Marketplace API
# 2. Inventory API
# 3. Promotions API
# 4. Batch Node (Airflow + Spark)
# 5. Streaming Node (Kafka + RabbitMQ)
# ==============================================================================

variable "log_retention_days" {
  description = "Retention period for CloudWatch log events in days"
  type        = number
  default     = 30
}

# 1. Log Group - Marketplace API
resource "aws_cloudwatch_log_group" "marketplace_api" {
  name              = "/ecs/mercaandes-marketplace-api"
  retention_in_days = var.log_retention_days

  tags = {
    Project     = "MercaAndes"
    Component   = "Marketplace-API"
    ManagedBy   = "Terraform"
    Environment = var.environment
  }
}

# 2. Log Group - Inventory API
resource "aws_cloudwatch_log_group" "inventory_api" {
  name              = "/ecs/mercaandes-inventory-api"
  retention_in_days = var.log_retention_days

  tags = {
    Project     = "MercaAndes"
    Component   = "Inventory-API"
    ManagedBy   = "Terraform"
    Environment = var.environment
  }
}

# 3. Log Group - Promotions API
resource "aws_cloudwatch_log_group" "promotions_api" {
  name              = "/ecs/mercaandes-promotions-api"
  retention_in_days = var.log_retention_days

  tags = {
    Project     = "MercaAndes"
    Component   = "Promotions-API"
    ManagedBy   = "Terraform"
    Environment = var.environment
  }
}

# 4. Log Group - Batch Node (Airflow + Spark)
resource "aws_cloudwatch_log_group" "batch_node" {
  name              = "/ecs/mercaandes-batch-node"
  retention_in_days = var.log_retention_days

  tags = {
    Project     = "MercaAndes"
    Component   = "Batch-Node"
    ManagedBy   = "Terraform"
    Environment = var.environment
  }
}

# 5. Log Group - Streaming Node (Kafka + RabbitMQ)
resource "aws_cloudwatch_log_group" "streaming_node" {
  name              = "/ecs/mercaandes-streaming-node"
  retention_in_days = var.log_retention_days

  tags = {
    Project     = "MercaAndes"
    Component   = "Streaming-Node"
    ManagedBy   = "Terraform"
    Environment = var.environment
  }
}

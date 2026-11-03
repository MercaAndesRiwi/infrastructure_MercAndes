variable "aws_region" {
  description = "AWS deployment region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment name"
  type        = string
  default     = "production"
}

variable "vpc_id" {
  description = "The ID of the VPC where resources are deployed"
  type        = string
  default     = "vpc-placeholder"
}

variable "database_subnet_ids" {
  description = "List of private subnet IDs for RDS DB Subnet Group"
  type        = list(string)
  default     = ["subnet-data-az1", "subnet-data-az2"]
}

variable "batch_node_sg_id" {
  description = "Security Group ID of the Batch Node (Airflow / Spark)"
  type        = string
  default     = "sg-batch-node"
}

variable "streaming_node_sg_id" {
  description = "Security Group ID of the Streaming Node (Kafka / RabbitMQ)"
  type        = string
  default     = "sg-streaming-node"
}

variable "apis_sg_id" {
  description = "Security Group ID of the ECS Fargate APIs"
  type        = string
  default     = "sg-alb-apis"
}

# --- RDS PostgreSQL Configuration (SCRUM-95 & SCRUM-96) ---
variable "db_instance_class" {
  description = "RDS DB instance class"
  type        = string
  default     = "db.t4g.micro"
}

variable "db_allocated_storage" {
  description = "Allocated storage in GB for RDS"
  type        = number
  default     = 20
}

variable "db_name" {
  description = "Initial database name for the Gold serving layer"
  type        = string
  default     = "merca_gold"
}

variable "db_username" {
  description = "Master username for RDS PostgreSQL"
  type        = string
  default     = "merca_admin"
}

variable "db_password" {
  description = "Master password for RDS PostgreSQL"
  type        = string
  default     = "MercaAndes2026Secure!"
  sensitive   = true
}

variable "db_backup_retention_period" {
  description = "Number of days to retain automated backups (SCRUM-96)"
  type        = number
  default     = 7
}

variable "db_backup_window" {
  description = "Daily time range during which automated backups are created (UTC)"
  type        = string
  default     = "03:00-04:00"
}

variable "db_maintenance_window" {
  description = "Weekly time range during which system maintenance can occur (UTC)"
  type        = string
  default     = "Sun:04:30-Sun:05:30"
}

# --- ECS Fargate Variables ---
variable "marketplace_api_image" {
  description = "Docker image URI for Marketplace API"
  type        = string
  default     = "api-orders:latest"
}

variable "inventory_api_image" {
  description = "Docker image URI for Inventory API"
  type        = string
  default     = "api-sellers:latest"
}

variable "promotions_api_image" {
  description = "Docker image URI for Promotions API"
  type        = string
  default     = "api-promotions:latest"
}

variable "fargate_cpu" {
  description = "Fargate instance CPU units to provision (1 vCPU = 1024 CPU units)"
  type        = string
  default     = "256"
}

variable "fargate_memory" {
  description = "Fargate instance memory to provision (in MiB)"
  type        = string
  default     = "512"
}

variable "app_port" {
  description = "Port exposed by the docker image to redirect traffic to"
  type        = number
  default     = 8000
}

variable "app_count" {
  description = "Number of docker containers to run"
  type        = number
  default     = 1
}

variable "health_check_path" {
  description = "HTTP path for health check"
  type        = string
  default     = "/health"
}

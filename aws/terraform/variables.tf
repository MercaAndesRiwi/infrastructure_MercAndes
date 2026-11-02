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

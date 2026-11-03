# ==============================================================================
# Amazon RDS PostgreSQL Configuration (SCRUM-94 / US-05)
# Subtasks:
# - SCRUM-95: Provision RDS PostgreSQL on private subnet.
# - SCRUM-96: Configure automatic backups (window & retention).
# - SCRUM-98: Configure Security Group access strictly from Batch, Streaming, and APIs.
# ==============================================================================

# ------------------------------------------------------------------------------
# 1. DB Subnet Group (Private Data Subnets) - SCRUM-95
# ------------------------------------------------------------------------------
resource "aws_db_subnet_group" "rds" {
  name        = "mercaandes-rds-subnet-group"
  description = "Private data subnets for MercaAndes RDS PostgreSQL instance"
  subnet_ids  = var.database_subnet_ids

  tags = {
    Project     = "MercaAndes"
    Component   = "RDS-PostgreSQL"
    Environment = var.environment
  }
}

# ------------------------------------------------------------------------------
# 2. Security Group for RDS (Restricted to internal VPC engines) - SCRUM-98
# ------------------------------------------------------------------------------
resource "aws_security_group" "rds" {
  name        = "mercaandes-sg-rds"
  description = "Security group for RDS PostgreSQL; restricts ingress strictly to Batch Node, Streaming Node, and APIs"
  vpc_id      = var.vpc_id

  # Ingress strictly on port 5432 from authorized source security groups
  ingress {
    description     = "PostgreSQL access from Batch Node (Airflow / Spark)"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [var.batch_node_sg_id]
  }

  ingress {
    description     = "PostgreSQL access from Streaming Node (Kafka / RabbitMQ / Spark Streaming)"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [var.streaming_node_sg_id]
  }

  ingress {
    description     = "PostgreSQL access from ECS Fargate APIs"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [var.apis_sg_id]
  }

  # Standard egress
  egress {
    description = "Allow all outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Project     = "MercaAndes"
    Component   = "RDS-SecurityGroup"
    Environment = var.environment
  }
}

# ------------------------------------------------------------------------------
# 3. RDS PostgreSQL Instance - SCRUM-95 & SCRUM-96
# ------------------------------------------------------------------------------
resource "aws_db_instance" "postgres" {
  identifier                  = "mercaandes-rds-gold-serving"
  engine                      = "postgres"
  engine_version              = "16.3"
  instance_class              = var.db_instance_class
  allocated_storage           = var.db_allocated_storage
  max_allocated_storage       = 100
  storage_type                = "gp3"
  storage_encrypted           = true
  publicly_accessible         = false

  db_name                     = var.db_name
  username                    = var.db_username
  password                    = var.db_password
  port                        = 5432

  db_subnet_group_name        = aws_db_subnet_group.rds.name
  vpc_security_group_ids      = [aws_security_group.rds.id]

  # SCRUM-96: Automatic Backups & Maintenance Window
  backup_retention_period     = var.db_backup_retention_period
  backup_window               = var.db_backup_window
  maintenance_window          = var.db_maintenance_window
  copy_tags_to_snapshot       = true
  delete_automated_backups    = false
  skip_final_snapshot         = var.environment != "production"
  final_snapshot_identifier   = "mercaandes-rds-gold-final-snapshot"
  deletion_protection         = var.environment == "production"

  auto_minor_version_upgrade  = true
  allow_major_version_upgrade = false

  tags = {
    Project     = "MercaAndes"
    Layer       = "Gold-Serving-Layer"
    Component   = "RDS-PostgreSQL"
    Environment = var.environment
  }
}

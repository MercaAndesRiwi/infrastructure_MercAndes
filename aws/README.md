# MercaAndes AWS Infrastructure Deployment Guide

This directory contains the Infrastructure as Code (IaC) and configuration files for the MercaAndes Omnichannel Platform:

- **SCRUM-107**: ECS Fargate Task Definitions for Marketplace API, Inventory API, and Promotions API.
- **SCRUM-109**: Centralized CloudWatch Logging for the 5 decoupled platform engines.
- **SCRUM-94 / US-05**: RDS PostgreSQL as a Service Layer (Gold / Serving Layer):
  - **SCRUM-95**: Provision RDS PostgreSQL on private subnet.
  - **SCRUM-96**: Configure automated backups (7-day retention, maintenance windows).
  - **SCRUM-97**: Consolidated MER DDL schema (`dim_*`, `fact_*`, multi-country POS).
  - **SCRUM-98**: Security Group access restricted strictly to Batch, Streaming, and APIs.

---

## Architecture Summary

| Component | Launch Type / Service | Port / Engine | Network / Log Group | Security / Access |
| :--- | :--- | :--- | :--- | :--- |
| **Marketplace API** | AWS Fargate | 8000 (FastAPI) | Private App Subnets / `/ecs/mercaandes-marketplace-api` | Ingress from ALB (`sg-alb-apis`) |
| **Inventory API** | AWS Fargate | 8000 (FastAPI) | Private App Subnets / `/ecs/mercaandes-inventory-api` | Ingress from ALB (`sg-alb-apis`) |
| **Promotions API** | AWS Fargate | 8000 (FastAPI) | Private App Subnets / `/ecs/mercaandes-promotions-api` | Ingress from ALB (`sg-alb-apis`) |
| **Batch Node** | Amazon EC2 | 8080/7077 (Airflow+Spark) | Private App Subnets / `/ecs/mercaandes-batch-node` | Ingress from VPC internal |
| **Streaming Node** | Amazon EC2 | 9092/5672 (Kafka+RabbitMQ)| Private App Subnets / `/ecs/mercaandes-streaming-node` | Ingress from VPC internal |
| **RDS PostgreSQL (Gold)** | Amazon RDS | 5432 (PostgreSQL 16.3)| Private Data Subnets (`mercaandes-rds-subnet-group`) | Ingress strictly Batch, Streaming & APIs |

---

## Directory Structure

```
aws/
├── README.md
├── sql/
│   └── 01_consolidated_mer_gold_schema.sql  # Consolidated MER DDL script (SCRUM-97)
├── task-definitions/
│   ├── marketplace-api-task.json             # Fargate Task Definition (SCRUM-107)
│   ├── inventory-api-task.json               # Fargate Task Definition (SCRUM-107)
│   ├── promotions-api-task.json              # Fargate Task Definition
│   ├── batch-node-task.json                  # EC2 Task Definition
│   └── streaming-node-task.json              # EC2 Task Definition
└── terraform/
    ├── cloudwatch.tf                         # Log groups & metric filters (SCRUM-109)
    ├── ecs_fargate.tf                        # Fargate service definitions (SCRUM-107)
    ├── rds.tf                                # RDS instance, subnet group & SG (SCRUM-94/95/96/98)
    ├── variables.tf                          # Input variables for ECS and RDS
    └── outputs.tf                            # Output endpoints and identifiers
```

---

## Deployment Instructions

### 1. Provisioning Infrastructure via Terraform

```bash
cd aws/terraform
terraform init
terraform plan -out=tfplan
terraform apply tfplan
```

### 2. RDS PostgreSQL Configuration (SCRUM-94)

The RDS PostgreSQL instance is deployed in dedicated private data subnets without public IPs:

- **Subnet Group (`aws_db_subnet_group.rds`)**: Uses `var.database_subnet_ids` across two availability zones.
- **Security Group (`mercaandes-sg-rds`)**: Only allows inbound port 5432 traffic from:
  - `var.batch_node_sg_id` (`sg-batch-node`)
  - `var.streaming_node_sg_id` (`sg-streaming-node`)
  - `var.apis_sg_id` (`sg-alb-apis`)
- **Backups (`SCRUM-96`)**:
  - Retention: `7 days`
  - Backup Window: `03:00-04:00 UTC`
  - Maintenance Window: `Sun:04:30-Sun:05:30 UTC`
  - Encrypted storage: `gp3` (20GB initial, autoscaling up to 100GB).

### 3. Executing the Consolidated MER DDL Script (SCRUM-97)

Once the RDS instance is available, connect from an authorized VPC host (e.g. Batch Node EC2 or Bastion):

```bash
# Obtain RDS endpoint from Terraform output
RDS_HOST=$(terraform output -raw rds_address)

# Execute the consolidated DDL script
PGPASSWORD="<db_password>" psql -h "$RDS_HOST" -U merca_admin -d merca_gold -f ../sql/01_consolidated_mer_gold_schema.sql
```

The script idempotently creates:
1. **Shared Dimensions (`public` schema)**:
   - `dim_currency`, `dim_country`, `dim_geolocation`, `dim_customer`
   - `dim_branch`, `dim_seller`, `dim_product_category`, `dim_product`
   - `dim_source`, `dim_exchange_rate`, `dim_holiday_type`, `dim_holiday`, `dim_holiday_type_bridge`
2. **Centralized Facts (`public` schema)**:
   - `fact_sale`, `fact_sale_item`, `fact_order`, `fact_order_item`
3. **Gold Serving Analytical Tables (`public` schema)**:
   - `gold_stockout_risk`, `hot_inventory_risk`
4. **Country-Specific POS Schemas**:
   - `colombia`, `peru`, `ecuador`, `bolivia`, `chile` (each with `stores`, `cashiers`, `pos_transactions`, `pos_transaction_items`, `inventory_snapshots`).

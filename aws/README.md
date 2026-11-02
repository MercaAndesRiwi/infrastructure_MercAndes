# MercaAndes AWS ECS & CloudWatch Deployment (SCRUM-107 & SCRUM-109)

This directory contains the Infrastructure as Code (IaC) and configuration files for:
- **SCRUM-107**: ECS Fargate Task Definitions for Marketplace API and Inventory API.
- **SCRUM-109**: Centralized CloudWatch Logging for the 5 platform components.

---

## Architecture Summary

| Component | Launch Type | Port | Log Group (CloudWatch) | Stream Prefix |
| :--- | :--- | :--- | :--- | :--- |
| **Marketplace API** | AWS Fargate | 8000 | `/ecs/mercaandes-marketplace-api` | `ecs` |
| **Inventory API** | AWS Fargate | 8000 | `/ecs/mercaandes-inventory-api` | `ecs` |
| **Promotions API** | AWS Fargate | 8000 | `/ecs/mercaandes-promotions-api` | `ecs` |
| **Batch Node (Airflow+Spark)** | Amazon EC2 | 8080/7077 | `/ecs/mercaandes-batch-node` | `airflow`, `spark` |
| **Streaming Node (Kafka+RabbitMQ)** | Amazon EC2 | 9092/5672 | `/ecs/mercaandes-streaming-node` | `kafka`, `rabbitmq` |

---

## Directory Structure

```
aws/
├── README.md
├── task-definitions/
│   ├── marketplace-api-task.json
│   ├── inventory-api-task.json
│   ├── promotions-api-task.json
│   ├── batch-node-task.json
│   └── streaming-node-task.json
└── terraform/
    ├── cloudwatch.tf
    ├── ecs_fargate.tf
    ├── variables.tf
    └── outputs.tf
```

---

## How to Register & Deploy

### Method 1: Via AWS CLI (JSON Task Definitions)

1. Set your AWS Account ID:
   ```bash
   export AWS_ACCOUNT_ID="<your-aws-account-id>"
   ```
2. Replace `${AWS_ACCOUNT_ID}` and register the Task Definitions:
   ```bash
   # Marketplace API
   envsubst < aws/task-definitions/marketplace-api-task.json > /tmp/marketplace.json
   aws ecs register-task-definition --cli-input-json file:///tmp/marketplace.json

   # Inventory API
   envsubst < aws/task-definitions/inventory-api-task.json > /tmp/inventory.json
   aws ecs register-task-definition --cli-input-json file:///tmp/inventory.json
   ```

### Method 2: Via Terraform

```bash
cd aws/terraform
terraform init
terraform plan
terraform apply
```

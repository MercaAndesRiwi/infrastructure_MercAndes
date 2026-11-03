# ADR-013: Serverless Fargate Task Definitions and Centralized CloudWatch Observability for MercaAndes APIs and Processing Engines

- **Status:** Approved (Ready for Review)
- **Date:** 2026-11-02
- **Authors:** Jayzir Martinez (MercaAndes Infrastructure Team)
- **Related Requirement:** US-01: ECS Cluster: Batch Node and Streaming Node (EC2) + APIs on Fargate/ALB (Tickets: SCRUM-103, SCRUM-107, SCRUM-109)

---

## Context
Following the foundation established in ADR-012 (which provisioned the ECS cluster, dedicated EC2 compute nodes for Batch/Streaming, and the public Application Load Balancer), the platform requires:
1. Packaging and defining the runtime specifications for the operational APIs (Marketplace API and Inventory API) as serverless tasks on AWS Fargate.
2. Establishing a centralized logging and observability architecture across all five core decoupled platform engines (Marketplace API, Inventory API, Promotions API, Batch Node, and Streaming Node).
3. Enforcing automated health checks (/health on port 8000) for seamless integration with the ALB target group (tg-apis-fargate).

---

## Decision Drivers
1. **Workload Isolation & Operational Simplicity:** Stateless APIs must scale independently without consuming CPU/memory from persistent Spark or Kafka workloads.
2. **Strict Observability:** Centralized, durable log aggregation for distributed troubleshooting and incident triage.
3. **Automated Eviction:** Automatic failover via HTTP health checks on port 8000.
4. **Least-Privilege Security:** Elimination of static AWS credentials in container definitions; integration with IAM execution roles.
5. **Cost & Lifecycle Control:** Automatic retention windows to avoid uncontrolled CloudWatch storage costs.

---

## Considered Options

### Option 1 — Host-Level Local File Logging on Shared EC2
- *Description:* Run the APIs on the existing EC2 instances and log directly to container stdout or local disk files.
- *Rejected:* Violates the course requirement for Fargate + ALB isolation. Local logs are lost upon container restart or EC2 replacement, providing zero distributed searchability.

### Option 2 — Self-Managed Logging Stack (ELK / OpenSearch on EC2)
- *Description:* Deploy an Elasticsearch/Logstash/Kibana cluster inside the VPC.
- *Rejected:* Introduces heavy maintenance overhead, requires dedicated memory that exceeds the t3.medium baseline, and adds unnecessary recurring operational costs for an academic/staging project.

### Option 3 — AWS Fargate with Native awslogs Driver & Centralized CloudWatch Groups (Selected)
- *Description:* Define Fargate task specifications (0.25 vCPU, 512 MiB) running in awsvpc network mode, routing container logs directly to dedicated Amazon CloudWatch Log Groups via the awslogs driver with 30-day retention policies.
- *Accepted:* Fully satisfies course requirements, provides zero server maintenance, enables instant ALB health monitoring, and guarantees persistent, queryable logs.

---

## Decision

We defined and versioned the Fargate Task Definitions and CloudWatch Log Groups using Infrastructure as Code (Terraform) and native ECS JSON templates:

### 1. Fargate Task Definitions for APIs (SCRUM-107)
- **Runtime & Network:** Configured for FARGATE compatibility with awsvpc network mode.
- **Sizing:** Sized at 256 CPU units (0.25 vCPU) and 512 MiB RAM per container to optimize resource consumption.
- **Ports & Environment:** Port 8000/tcp mapped for FastAPI/Uvicorn, with APP_ENV=production.
- **Health Probes:** Integrated container health check executing curl -f http://localhost:8000/health || exit 1.
- **Execution Role:** Configured to assume ecsTaskExecutionRole for pulling images from Amazon ECR and writing log streams.

### 2. Centralized CloudWatch Logging (SCRUM-109)
Provisioned five dedicated CloudWatch Log Groups with a 30-day retention policy:
- `/ecs/mercaandes-marketplace-api` (Stream prefix: ecs)
- `/ecs/mercaandes-inventory-api` (Stream prefix: ecs)
- `/ecs/mercaandes-promotions-api` (Stream prefix: ecs)
- `/ecs/mercaandes-batch-node` (Stream prefixes: airflow, spark)
- `/ecs/mercaandes-streaming-node` (Stream prefixes: kafka, rabbitmq)

---

## Consequences

### Positive
- **Native ALB Compatibility:** IP-mode target groups can register and route traffic immediately to Fargate tasks upon launch.
- **Unified Observability:** Errors across batch, streaming, and REST APIs are queryable from a single CloudWatch dashboard.
- **Cost Predictability:** 30-day log expiration prevents unbounded storage accrual.
- **Declarative IaC:** All configurations are versioned in Git (aws/task-definitions/ and aws/terraform/).

### Negative / Trade-offs & Operational Findings
- **IAM Prerequisite:** The deployment requires the account administrator to provision ecsTaskExecutionRole (or add ecs-tasks.amazonaws.com to the trust policy of mercaandes-api-ec2-role) before the AWS Fargate agent can pull images in the live environment.

---

## Implementation
1. Added JSON task definitions: `aws/task-definitions/marketplace-api-task.json` and `inventory-api-task.json`.
2. Created Terraform resources: `aws/terraform/ecs_fargate.tf` and `cloudwatch.tf`.
3. Pushed implementation branch `feature/scrum-107-109-fargate-ecs-cloudwatch` to GitHub.
4. Transitioned tickets SCRUM-107 and SCRUM-109 to En revision.

---

## Scope & References
- Covers task definitions, health checks, and logging configurations for SCRUM-107 and SCRUM-109 under parent story SCRUM-103.
- References: ADR-011 (Networking), ADR-012 (ECS Hybrid Architecture), AWS ECS Fargate Developer Guide.

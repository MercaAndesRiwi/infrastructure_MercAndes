# ADR-014: Amazon RDS PostgreSQL Gold Serving Layer and Consolidated Relational Schema

- **Status:** Approved (Ready for Review)
- **Date:** 2026-11-02
- **Authors:** Jayzir Martinez (MercaAndes Infrastructure Team)
- **Related Requirement:** US-05: RDS PostgreSQL as a service layer (Gold / Serving Layer) (Tickets: SCRUM-94, SCRUM-95, SCRUM-96, SCRUM-97, SCRUM-98)

---

## Context

The MercaAndes Omnichannel Data Platform follows a Medallion Architecture (Bronze -> Silver -> Gold). The Gold layer serves two primary operational and analytical consumers:
1. High-concurrency, low-latency operational REST APIs (Marketplace API, Inventory API, Promotions API) deployed on AWS ECS Fargate.
2. Executive business intelligence dashboards (Power BI) and ad-hoc analytical queries requiring relational integrity, dimensional modeling (Star/Snowflake schema), and multi-country POS visibility.

To support these requirements, the platform requires:
1. **Private Network Placement (SCRUM-95):** Provisioning an isolated relational database engine within private VPC data subnets with zero public internet exposure.
2. **Automated Durability & Backups (SCRUM-96):** Configuring automated daily snapshots, a defined backup window, maintenance schedule, and point-in-time recovery capabilities.
3. **Consolidated Relational Schema (SCRUM-97):** Executing an idempotent DDL script deploying centralized dimensions (`dim_*`), transactional facts (`fact_*`), real-time operational risk indicators, and country-isolated POS schemas (`colombia`, `peru`, `ecuador`, `bolivia`, `chile`).
4. **Strict Network Security Boundary (SCRUM-98):** Enforcing Security Group ingress rules strictly restricted to authorized internal compute engines (Batch Node, Streaming Node, and ECS APIs) on port 5432.

---

## Decision Drivers

1. **Low-Latency Query Serving:** Sub-millisecond to low-millisecond response times for API endpoints checking customer data, product catalogs, branch details, and inventory stockout risks.
2. **ACID Transactions & Relational Integrity:** Enforcing foreign key constraints across countries, currencies, product hierarchies, and transaction line items.
3. **Zero-Trust Network Isolation:** Preventing public IPv4/IPv6 assignment; eliminating attack surfaces from the public internet.
4. **Disaster Recovery & Operational Stability:** Automated daily backups with guaranteed retention to meet recovery point objectives (RPO < 24h).
5. **Cost Predictability:** Right-sizing database compute (`db.t4g.micro` with gp3 storage) for development and staging workloads without incurring excessive cluster overhead.

---

## Considered Options

### Option 1 — Querying Directly Against S3 Data Lake (Amazon Athena / S3 Parquet)
- *Description:* Keep Gold tables solely in Amazon S3 as Parquet/Delta files and query them via Amazon Athena from APIs and Power BI.
- *Rejected:* Amazon Athena introduces high query latency (typically 1.5 - 5 seconds per query) and per-query scan costs, making it unviable for real-time transactional APIs (FastAPI) and responsive dashboards.

### Option 2 — Self-Managed PostgreSQL on an EC2 Instance
- *Description:* Install and manage PostgreSQL inside a Linux EC2 instance in the VPC.
- *Rejected:* High administrative overhead: requires manual configuration of EBS snapshot backups, point-in-time recovery, operating system security patching, and failover mechanics.

### Option 3 — Amazon RDS PostgreSQL 16 in Private Data Subnets (Selected)
- *Description:* Deploy a fully managed Amazon RDS PostgreSQL 16.3 instance inside a dedicated DB Subnet Group spanning multiple availability zones, attached to a restrictive Security Group allowing ingress only from Batch, Streaming, and API compute layers.
- *Accepted:* Fully satisfies all operational, performance, and security criteria. Provides native automated snapshots, storage encryption (`gp3`), point-in-time recovery, and seamless integration with Terraform.

---

## Decision

We defined and provisioned the RDS PostgreSQL Gold Serving Layer using Terraform and an idempotent SQL DDL script:

### 1. Private Subnet Placement (SCRUM-95)
- Created `aws_db_subnet_group.rds` (`mercaandes-rds-subnet-group`) utilizing private data subnets across distinct AZs (`var.database_subnet_ids`).
- Configured `publicly_accessible = false` on `aws_db_instance.postgres`.

### 2. Automated Backups & Maintenance (SCRUM-96)
- **Backup Retention:** Configured `backup_retention_period = 7` days.
- **Daily Backup Window:** `03:00-04:00 UTC` (outside peak business hours).
- **Maintenance Window:** `Sun:04:30-Sun:05:30 UTC`.
- **Snapshot Copy:** `copy_tags_to_snapshot = true`.
- **Storage:** 20 GB `gp3` storage with storage encryption enabled (`storage_encrypted = true`) and autoscaling up to 100 GB.

### 3. Consolidated MER DDL Schema (SCRUM-97)
- Executed `aws/sql/01_consolidated_mer_gold_schema.sql` (and `postgres/ddl/01_consolidated_mer_gold_schema.sql`) creating:
  - **Shared Centralized Dimensions (`public`):**
    - `dim_currency`, `dim_country`, `dim_geolocation`, `dim_customer`
    - `dim_branch`, `dim_seller`, `dim_product_category`, `dim_product`
    - `dim_source`, `dim_exchange_rate`, `dim_holiday_type`, `dim_holiday`, `dim_holiday_type_bridge`
  - **Consolidated Facts (`public`):**
    - `fact_sale`, `fact_sale_item`, `fact_order`, `fact_order_item`
  - **Gold Serving Analytical Tables (`public`):**
    - `gold_stockout_risk`, `hot_inventory_risk`
  - **Country POS Schemas:**
    - `colombia`, `peru`, `ecuador`, `bolivia`, `chile` (stores, cashiers, POS transactions, transaction items, inventory snapshots).

### 4. Security Group Isolation (SCRUM-98)
- Created `aws_security_group.rds` (`mercaandes-sg-rds`) with ingress on port 5432 strictly limited to:
  - `var.batch_node_sg_id` (`sg-batch-node`)
  - `var.streaming_node_sg_id` (`sg-streaming-node`)
  - `var.apis_sg_id` (`sg-alb-apis`)
- Zero ingress from 0.0.0.0/0 or untrusted VPC subnets.

---

## Consequences

### Positive
- **Guaranteed Isolation:** Zero public exposure; access is physically confined to internal VPC compute nodes and services.
- **Consistent Relational Model:** Standardized Star/Snowflake schema accessible by both analytical tools (Power BI) and operational services.
- **Automated Disaster Recovery:** Automated 7-day retention and point-in-time recovery eliminate risk of data loss.
- **Auditable & Declarative:** Fully managed through Terraform and versioned in Git.

### Negative / Trade-offs & Operational Findings
- **BI Ingress via VPC:** External Power BI Desktop connections require either a Bastion Host with SSH tunneling, AWS Client VPN, or an on-premises Power BI Gateway hosted on an EC2 instance within the VPC.

---

## Implementation Files
- `aws/terraform/rds.tf`: Terraform configuration for RDS instance, subnet group, and security group.
- `aws/terraform/variables.tf`: Input variables for database credentials, retention periods, and subnet/SG IDs.
- `aws/terraform/outputs.tf`: Database endpoint, address, port, and security group ID outputs.
- `aws/sql/01_consolidated_mer_gold_schema.sql`: Complete DDL schema script.
- `postgres/ddl/01_consolidated_mer_gold_schema.sql`: Mirrored DDL script.
- Git Branch: `feature/scrum-94-rds-gold-serving-layer`

---

## Scope & References
- Covers requirements for parent story `SCRUM-94` and subtasks `SCRUM-95`, `SCRUM-96`, `SCRUM-97`, `SCRUM-98`.
- References: ADR-008 (MER Design), ADR-011 (VPC Network Topology), ADR-012 (ECS Architecture).

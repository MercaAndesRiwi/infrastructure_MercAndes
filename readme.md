# Data Platform — MercaAndes

Centralized data and orchestration platform for MercaAndes: **PostgreSQL (RDS), RabbitMQ, Kafka, Apache Spark (1 Master + 2 Workers), Apache Airflow (CeleryExecutor), API Microservices, and Alert/Notification Workers**.

---

## 1. Prerequisites

- **Docker Desktop** (Docker Compose v2) with at least **8 GB RAM** allocated.
- **PowerShell** (Windows) or **Bash** (Linux/macOS).
- **Python 3.12+** installed.
- **Git** configured with access to the MercaAndesRiwi organization.

---

## 2. Step-by-Step Local Execution

This platform orchestrates and integrates the 7 repositories of the **MercaAndes** ecosystem:

1. `infrastructure_MercAndes` (`data-platform`): Central orchestrator (this repository).
2. `Transactional-POS-database`: Alembic migrations and transactional POS seeds (`pos-database`).
3. `Inventory_Api`: Inventory query FastAPI microservice (`inventory-api`, port `8002`).
4. `Marketplace_Api`: Marketplace order events FastAPI microservice and Kafka producer (`marketplace-api`, port `8003`).
5. `MercaAndes_Spark_Gold_Transformations`: PySpark transformations mounted into the Spark cluster.
6. `api-exchange-rates`: Batch ingestion of currency exchange rates into PostgreSQL (`exchange-rates-seed`).
7. `api-holidays`: Batch ingestion of public holidays into PostgreSQL (`holidays-seed`).

---

### Step 2.1: Automatic Workspace Bootstrap and Environment Variables

To ensure seamless compatibility across any machine (creates the `.env` file and clones/validates sister repositories):

**On Windows (PowerShell):**

```powershell
.\scripts\bootstrap-workspace.ps1
```

**On Linux / macOS (Bash):**

```bash
./scripts/bootstrap-workspace.sh
```

> **Note:** If you prefer configuring `.env` manually:
>
> ```powershell
> Copy-Item .env.example .env
> ```
>
> Edit the `.env` file accordingly (AWS S3, credentials, and SMTP).

---

### Step 2.2: Build and Start Containers

From the root of `data-platform`:

```powershell
docker compose up -d --build
```

---

### Step 2.3: Verify Service Health

Once services are running, execute the quick smoke test script:

```powershell
.\scripts\smoke.ps1
```

**Ports and Services Matrix:**


| Service                       | URL / Port                     | Role / Function                                                     |
| :------------------------------ | :------------------------------- | :-------------------------------------------------------------------- |
| **inventory-api**             | `http://localhost:8002/`       | Deterministic stock and inventory lookup (F3)                       |
| **marketplace-api**           | `http://localhost:8003/`       | Marketplace order events and Kafka producer (F4)                    |
| **f7-weekly-promotions**      | `http://localhost:8004/health` | Weekly promotions ingestion to S3/Kafka/RabbitMQ (F7)               |
| **reports-api**               | `http://localhost:8005/health` | Enqueues operational report requests (US-03)                        |
| **pos-database**              | Batch Task (`restart: no`)     | Alembic migrations + POS sales and customer seeding (F2)            |
| **exchange-rates-seed**       | Batch Task (`restart: no`)     | Daily currency exchange rates ingestion from Frankfurter API        |
| **holidays-seed**             | Batch Task (`restart: no`)     | Public holidays ingestion from Nager.Date API for stockout rules    |
| **notification-worker**       | Background Worker              | RabbitMQ consumer and transactional email dispatcher                |
| **operational-alerts-worker** | Background Worker              | Stockout and data quality alert consumer (US-03)                    |
| **reports-worker**            | Background Worker              | Asynchronous risk report generator (US-03)                          |
| **Airflow UI**                | `http://localhost:8080`        | CeleryExecutor orchestrator (User: `airflow` / Password: `airflow`) |
| **Airflow Workers**           | 2 Background Workers           | Worker 1 and Worker 2 executing DAG tasks in parallel               |
| **Airflow Flower**            | `http://localhost:5555`        | Celery workers and task queues visual monitor                       |
| **Spark Master UI**           | `http://localhost:8081`        | Spark Cluster (Master + 2 Workers on `:7077`)                       |
| **RabbitMQ Mgmt**             | `http://localhost:15672`       | Message broker (User: `guest_user` / Password in `.env`)            |
| **Kafka UI (Kafdrop)**        | `http://localhost:9000`        | Kafka topics and partitions monitor (`:9092`)                       |

---

### Step 2.4: Stop the Platform

```powershell
docker compose down
```

---

## 3. Purchase Simulation and Email Notifications

The purchase workflow generates the corporate confirmation message and dispatches it via SMTP/SES or through the RabbitMQ asynchronous worker (`mercaandes.notifications.purchase`).

### Execution Options:

```powershell
# Option A: Fetch a real order directly from PostgreSQL (merca_pos):
python scripts/simulate_purchase.py --from-db --email "my-email@gmail.com"
python scripts/simulate_purchase.py --from-db --order-id "<ORDER_ID>" --email "my-email@gmail.com"

# Option B: Pass dynamic custom products via JSON file or CLI JSON string:
python scripts/simulate_purchase.py --customer "Carlos Gomez" --email "carlos@gmail.com" --products scripts/sample_order.json
python scripts/simulate_purchase.py --customer "Carlos Gomez" --email "carlos@gmail.com" --products '[{"name":"Arroz Diana 1kg","qty":2,"unit_price":4500},{"name":"Leche 1L","qty":1,"unit_price":4200}]'

# Option C: Standard default simulation:
python scripts/simulate_purchase.py --customer "Carlos Mendoza" --email "customer@mercaandes.com" --order-id "MA-2026-89412"

# Restart notification worker after updating SMTP settings in .env:
docker compose restart notification-worker
```

**Generated Notification Message:**

```text
¡Hola, [Customer Name]! 

¡Buenas noticias! Tu pedido ha sido procesado con éxito.

Conoce los detalles de tu compra:
- Empresa: MercaAndes
- N° de Pedido: MA-2026-89412
- Estado: Confirmado / En preparación
- Fecha de Compra: 28-10-2026
- Productos:
  * 1x Aceite de Oliva Extra Virgen (500ml) - $35.000
  * 2x Café Molido Especial (500g) - $48.000
- Total pagado: $83.000

Puedes realizar el seguimiento de tu envío ingresando a Mis Compras > Detalle de Pedido.

¡Gracias por comprar en MercaAndes!
Atentamente,
Equipo de Atención al Cliente | MercaAndes
```

---

## 4. Airflow DAG Orchestration (Strict Order)

Access `http://localhost:8080`. All DAGs feature standardized operational attributes (`retries=3`, `retry_delay=5min`, `execution_timeout=30min`, `max_active_runs=1`).

Execute in the following sequential order:

```
[1. f1_kaggle_historical_load]  (@once - Initial historical ingestion to S3 Bronze)
              │
              ▼
[2. bronze_to_silver_cleaning]  (@daily - Data Cleaning RN-10/11 -> Spark RN-01/02/03/06)
              │
              ▼
[3. silver_to_gold_business_rules] (@daily - Parallel on 2 workers: RN-04 Stockout + RN-09 Accuracy)
              │
              ▼
[4. seller_scoring]            (@weekly - RN-07 Score -> RN-08 Status)
              │
              ▼
[5. f7_promotions_weekly]      (@weekly - RN-05 S3 Ingestion + RN-12 Silent Store Alerts)
```

### 5 DAGs Specification:


| DAG                                 | Schedule  | Main Tasks / TaskGroups                                                                                                               | Business Rules / Architecture                                                                    |
| :------------------------------------ | :---------- | :-------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------- |
| **`f1_kaggle_historical_load`**     | `@once`   | `upload_local_f1_to_s3` $\rightarrow$ `validate_f1_bronze` $\rightarrow$ `spark_f1_bronze_to_silver_init`                             | Olist Kaggle historical load to S3 Bronze and Spark Silver tables init                           |
| **`bronze_to_silver_cleaning`**     | `@daily`  | `start` $\rightarrow$ `TaskGroup(bronze_cleaning)` $\rightarrow$ `TaskGroup(spark_silver)` $\rightarrow$ `verify` $\rightarrow$ `end` | RN-10 (Deduplication), RN-11 (MD5 PII Anonymization), RN-01/02/06 (Net Sales), RN-03 (Inventory) |
| **`silver_to_gold_business_rules`** | `@daily`  | `start` $\rightarrow$ [`TaskGroup(stockout_risk)` \|\| `TaskGroup(seller_accuracy)`] $\rightarrow$ `end`                              | RN-04 (Stockout risk & RabbitMQ alerts), RN-09 (Seller inventory accuracy)                       |
| **`seller_scoring`**                | `@weekly` | `start` $\rightarrow$ `validate_prerequisites` $\rightarrow$ `TaskGroup(seller_evaluation)` $\rightarrow$ `end`                       | RN-07 (Score 0-100), RN-08 (Observation / Suspended Status)                                      |
| **`f7_promotions_weekly`**          | `@weekly` | `start` $\rightarrow$ [`TaskGroup(promotions_ingestion)` \|\| `TaskGroup(store_monitoring)`] $\rightarrow$ `end`                      | RN-05 (Coverage validation $\ge$ 7 days), RN-12 (RabbitMQ silent store alerts)                   |

---

## 5. Explicit Resource Names


| Resource                           | Exact System Name                     | Usage                                         |
| :----------------------------------- | :-------------------------------------- | :---------------------------------------------- |
| **Kafka Orders Topic**             | `mercaandes.orders.events`            | `marketplace-api`                             |
| **Kafka Promotions Topic**         | `mercaandes.promotions.weekly`        | `f7-weekly-promotions`                        |
| **RabbitMQ Purchases Queue**       | `mercaandes.notifications.purchase`   | `simulate_purchase.py`, `notification-worker` |
| **RabbitMQ Stockout Alerts Queue** | `alertas.riesgo_quiebre`              | `silver_to_gold_business_rules` (RN-04)       |
| **RabbitMQ Silent Store Queue**    | `mercaandes.alerts.silent-store`      | `f7_promotions_weekly` (RN-12)                |
| **RabbitMQ F7 Ingestion Queue**    | `mercaandes.promotions.weekly.ingest` | `f7-weekly-promotions`                        |
| **Relational Database**            | `merca_pos`                           | PostgreSQL / Amazon RDS                       |

---

## 6. Operational RabbitMQ Messaging (US-03)

The operational message routes, consumers, retry policy, dead-letter queues, report API, and ten-minute durability procedure are documented in [RabbitMQ operational messaging](docs/rabbitmq-operational-messaging.md).

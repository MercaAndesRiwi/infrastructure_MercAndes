# Data Platform — MercaAndes

Centralized Big Data, Messaging, and Orchestration Platform for **MercaAndes**:

- **PostgreSQL 16**: Relational and Dimensional POS database (`merca_pos`).
- **RabbitMQ**: Asynchronous messaging broker and operational event queues.
- **Apache Kafka & Kafdrop**: Event streaming engine and visual topic monitor.
- **Apache Spark 3.5**: Distributed batch processing cluster (Master + Worker).
- **Apache Airflow 3.1.5**: CeleryExecutor pipeline orchestrator with pre-baked dependencies.
- **FastAPI Microservices**: Marketplace API (Orders/Purchases) and Inventory API.
- **Notification Worker**: Asynchronous email dispatcher via RabbitMQ and SMTP.

---

## 1. Prerequisites

Before starting, ensure your system has:

- **Docker Desktop** (Docker Compose v2) with WSL2 enabled.
- **Git** installed.
- **At least 8 GB of RAM** (16 GB recommended for running all containers simultaneously).

---

## 2. Quickstart: From Clone to Execution

Follow these simple steps to run the complete platform from scratch on any computer:

### Step 1: Clone the Repository

```bash
git clone URl_Repository
cd insfractucture_MercAndes
```

### Step 2: Configure Environment Variables

Initialize your `.env` configuration file from the template:

**Windows (PowerShell):**

```powershell
Copy-Item .env.example .env
```

**Linux / macOS (Bash):**

```bash
cp .env.example .env
```

> **Email Notifications (Optional):**
> To receive real order confirmation emails in your Gmail inbox, open `.env` and set:
>
> ```ini
> SMTP_HOST=smtp.gmail.com
> SMTP_PORT=587
> SMTP_USER=your_email@gmail.com
> SMTP_PASSWORD=your_google_app_password
> EMAIL_FROM=your_email@gmail.com
> ```

### Step 3: Start the Platform with Docker

Build and launch all services in detached mode with a single command:

```bash
docker compose up -d --build
```

Docker will automatically:

1. Build the Airflow image with all required dependencies pre-installed.
2. Initialize PostgreSQL schemas, currency exchange rates, and public holidays.
3. Start RabbitMQ and declare operational queues.
4. Start Apache Kafka and create the required order topics.
5. Launch the Spark cluster, Airflow orchestrator, and the Marketplace API.

The same startup also applies the idempotent `gold_seller_score` schema to the shared PostgreSQL database. This works with both a new database volume and an existing volume. The Inventory API starts after that schema step completes.

---

## 3. Real Customer Purchase & Email Dispatch

You can simulate a real end-to-end customer purchase directly from your web browser:

1. Open the **Marketplace API Swagger UI** in your browser:
   👉 **[http://localhost:8003/docs](http://localhost:8003/docs)**
2. Locate the endpoint:
   `POST /api/v1/marketplace/purchase`
3. Click **Try it out**.
4. The request body is pre-filled with customer details and cart items:
   ```json
   {
     "customer_name": "Luigui Garizado",
     "customer_email": "your_email@gmail.com",
     "products": [
       {
         "name": "Aceite de Oliva Extra Virgen (500ml)",
         "quantity": 1,
         "unit_price": 35000.0
       },
       {
         "name": "Café Molido Especial (500g)",
         "quantity": 2,
         "unit_price": 24000.0
       }
     ]
   }
   ```
5. Click **Execute**.

### What happens behind the scenes:

- The Marketplace API generates an official Order ID (e.g. `MA-2026-A1B2C`) and returns `201 Created`.
- The purchase event is published to RabbitMQ (`mercaandes.notifications.purchase`).
- The **Notification Worker** immediately consumes the event, renders the MercaAndes template, and sends the confirmation email to your inbox.

To monitor notification worker logs in real time:

```bash
docker logs -f platform-notification-worker
```

---

## 4. Web Dashboards and Service Endpoints

Once the platform is running, access all services via your browser:


| Service                 | Web URL                                                  | Credentials / Notes                                  |
| :------------------------ | :--------------------------------------------------------- | :----------------------------------------------------- |
| **Marketplace API**     | [http://localhost:8003/docs](http://localhost:8003/docs) | Interactive Swagger UI for purchases and orders (F4) |
| **Inventory API**       | [http://localhost:8002/docs](http://localhost:8002/docs) | Stock and inventory lookup endpoint (F3)             |
| **Airflow Web UI**      | [http://localhost:8080](http://localhost:8080)           | User: `airflow` / Password: `airflow`                |
| **Airflow Flower**      | [http://localhost:5555](http://localhost:5555)           | Celery distributed worker monitor                    |
| **Spark Master UI**     | [http://localhost:8081](http://localhost:8081)           | Apache Spark cluster status and jobs                 |
| **RabbitMQ Management** | [http://localhost:15672](http://localhost:15672)         | User: `guest_user` / Password in `.env`              |
| **Kafka UI (Kafdrop)**  | [http://localhost:9000](http://localhost:9000)           | Kafka topics, partitions, and message browser        |

---

## 5. Airflow DAG Orchestration Pipeline

Access the Airflow UI at `http://localhost:8080` to orchestrate data lake transformations in sequence:

```
[1. f1_kaggle_historical_load]     (@once  - Initial historical ingestion to S3 Bronze)
              │
              ▼
[2. bronze_to_silver_cleaning]     (@daily - Anonymization, deduplication & Silver tables)
              │
              ▼
[3. silver_to_gold_business_rules] (@daily - Parallel execution: Stockout risk & Seller accuracy)
              │
              ▼
[4. seller_scoring]                (@weekly - PySpark score + SQL seller status)
              │
              ▼
[5. f7_promotions_weekly]          (@weekly - Weekly promotions ingestion & silent store alerts)
```

El DAG `seller_scoring` se ejecuta semanalmente. Primero consume los eventos pendientes de Marketplace desde Kafka y carga reseñas del CSV histórico Olist para órdenes con un solo vendedor. Después espera los datos de inventario y las equivalencias de IDs, valida las entradas Silver, inicia Spark y actualiza el estado del vendedor. El cálculo SQL anterior del puntaje fue retirado para que RN-07 tenga una sola implementación.

---

## 6. Seller score job (US-07)

The Spark transformation remains in `MercaAndes_Spark_Gold_Transformations`. This repository adds its Docker runtime, creates `gold_seller_score` in the shared PostgreSQL database, and lets the API read that persisted result. The dimension table remains `public.dim_seller`; the score output is stored separately in `public.gold_seller_score`.

The job runtime uses a Python 3.11 virtual environment created with `uv`, Java 17, PySpark 3.5.1, and psycopg2 2.9.9. The versions match the Spark project. The current score job uses Spark local mode, matching its existing Spark session configuration. Airflow workers use the Docker socket to start the short-lived job container; therefore they have permission to manage containers through the local Docker engine.

The job reads relational Silver tables through JDBC; no Parquet files are required. Marketplace events are landed idempotently in PostgreSQL by a small Uv-based service, then materialized as dispatch and cancellation rows. Olist reviews are loaded from the existing historical CSVs, and reviews from orders with multiple sellers are skipped. Since the simulated catalogs do not share seller identities, Airflow maps source IDs to a common `(country, seller_type)` cohort UUID in `public.dim_seller_source_key`. The Inventory API has no stock count history, so Airflow creates deterministic simulated accuracy observations. If Kafka has no Marketplace events, repeatable sample dispatch and cancellation rows keep the demo runnable. These rules are for simulated data, not production identity resolution or measured stockout rates. Details are in [`inputs/seller_score/README.md`](inputs/seller_score/README.md).

Start the platform, including the Uv-based Spark runtime, with:

```bash
docker compose up -d --build
```

Airflow creates new DAGs paused by default in this Compose setup. Unpause `seller_scoring` in the Airflow UI to enable its weekly schedule. After the DAG writes a row, query `GET /api/v1/sellers/{seller_id}/score` with an API `SEL-*` ID or mapped `dim_seller` UUID. IDs in the same country/type cohort intentionally return the same demo score. A seller without a score returns `404`; if the score table is unavailable, the API returns `503`.

## 7. Stopping the Platform

To stop all running containers cleanly:

```bash
docker compose down
```

To stop containers and delete persisted volumes (clean slate):

```bash
docker compose down -v
```

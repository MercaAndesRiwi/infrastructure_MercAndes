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

| Service                       | Web URL                                                 | Credentials / Notes                                              |
| :---------------------------- | :------------------------------------------------------ | :--------------------------------------------------------------- |
| **Marketplace API**     | [http://localhost:8003/docs](http://localhost:8003/docs) | Interactive Swagger UI for purchases and orders (F4)             |
| **Inventory API**       | [http://localhost:8002/docs](http://localhost:8002/docs) | Stock and inventory lookup endpoint (F3)                         |
| **Airflow Web UI**      | [http://localhost:8080](http://localhost:8080)           | User: `airflow` / Password: `MercaAndes2026!` (ver `.env`) |
| **Airflow Flower**      | [http://localhost:5555](http://localhost:5555)           | Celery distributed worker monitor                                |
| **Spark Master UI**     | [http://localhost:8081](http://localhost:8081)           | Apache Spark cluster status and jobs                             |
| **RabbitMQ Management** | [http://localhost:15672](http://localhost:15672)         | User: `guest_user` / Password in `.env`                      |
| **Kafka UI (Kafdrop)**  | [http://localhost:9000](http://localhost:9000)           | Kafka topics, partitions, and message browser                    |

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

The `seller_scoring` DAG runs weekly. It prepares its inputs, checks that seller IDs are mapped, runs the Spark calculation, and updates seller status. To run it immediately, trigger it from the Airflow UI.

---

## 6. Seller scoring

The weekly `seller_scoring` DAG calculates seller scores in Spark. The current score is saved in PostgreSQL and can be read from the Inventory API at `GET /api/v1/sellers/{seller_id}/score`.

| Term                      | Meaning in this platform                                                                                                 |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| Silver inputs             | PostgreSQL tables containing dispatch, reviews, inventory checks, and cancellations.                                     |
| `dim_seller_source_key` | Maps a source seller ID to the UUID used in `dim_seller`. For demo data, IDs may share a country/type group and score. |
| JDBC                      | The connector Spark uses to read and write PostgreSQL tables.                                                            |
| Staging table             | An intermediate table Spark writes; the job copies its rows to Gold and then removes it.                                 |

Inventory accuracy and sample Marketplace records are simulated for the local demo. They are inputs to the score, not measurements from a real inventory audit.

### Run and check the score

1. Open Airflow at [http://localhost:8080](http://localhost:8080), find `seller_scoring`, and choose **Trigger DAG**.
2. Confirm the tasks finish successfully, including the Spark task.
3. Query the Inventory API using a seeded seller ID, for example:

   ```bash
   curl http://localhost:8002/api/v1/sellers/SEL-001/score
   ```

The response contains the saved score, its four components, and the calculation timestamp. If no score has been written yet, the API returns `404`.

## 7. Stopping the Platform

To stop all running containers cleanly:

```bash
docker compose down
```

To stop containers and delete persisted volumes (clean slate):

```bash
docker compose down -v
```

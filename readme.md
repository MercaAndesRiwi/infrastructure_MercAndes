# Data Platform — MercaAndes

Plataforma centralizada de orquestación y datos de MercaAndes: **PostgreSQL (RDS), RabbitMQ, Kafka, Apache Spark (1 Master + 2 Workers), Apache Airflow (CeleryExecutor), Microservicios API y Workers de Alertas y Notificaciones**.

---

## 1. Requisitos Previos

- **Docker Desktop** (Docker Compose v2) con mínimo **8 GB de RAM** asignados.
- **PowerShell** (Windows) o **Bash** (Linux/macOS).
- **Python 3.12+** instalado.
- **Git** configurado con acceso a la organización MercaAndesRiwi.

---

## 2. Paso a Paso de Ejecución Local

Esta plataforma orquesta e integra los 7 repositorios de la solución **MercaAndes**:
1. `infrastructure_MercAndes` (`data-platform`): Orquestador central (este repositorio).
2. `Transactional-POS-database`: Migraciones Alembic y semillas del POS transaccional (`pos-database`).
3. `Inventory_Api`: Microservicio FastAPI de consulta de inventario (`inventory-api`, puerto `8002`).
4. `Marketplace_Api`: Microservicio FastAPI y productor Kafka de pedidos (`marketplace-api`, puerto `8003`).
5. `MercaAndes_Spark_Gold_Transformations`: Transformaciones PySpark montadas en el clúster Spark.
6. `api-exchange-rates`: Ingesta batch de tasas de cambio a PostgreSQL (`exchange-rates-seed`).
7. `api-holidays`: Ingesta batch de días festivos a PostgreSQL (`holidays-seed`).

---

### Paso 2.1: Bootstrap Automático del Workspace y Variables de Entorno

Para garantizar compatibilidad en cualquier equipo (crea el archivo `.env` y clona/valida los repositorios complementarios):

**En Windows (PowerShell):**
```powershell
.\scripts\bootstrap-workspace.ps1
```

**En Linux / macOS (Bash):**
```bash
./scripts/bootstrap-workspace.sh
```

> **Nota:** Si prefieres configurar `.env` manualmente:
> ```powershell
> Copy-Item .env.example .env
> ```
> Edite el archivo `.env` según corresponda (AWS S3, credenciales y SMTP).

---

### Paso 2.2: Construir e Iniciar Contenedores

Desde la raíz de `data-platform`:

```powershell
docker compose up -d --build
```

---

### Paso 2.3: Verificar Salud de los Servicios

Una vez iniciados los servicios, ejecute el script de verificación rápida:

```powershell
.\scripts\smoke.ps1
```

**Matriz de Puertos y Servicios:**

| Servicio | URL / Puerto | Función |
| :--- | :--- | :--- |
| **inventory-api** | `http://localhost:8002/` | Consulta de inventario y stock determinista (F3) |
| **marketplace-api** | `http://localhost:8003/` | Eventos de pedidos Marketplace y productor Kafka (F4) |
| **f7-weekly-promotions** | `http://localhost:8004/health` | Ingesta de promociones semanales en S3/Kafka/RabbitMQ (F7) |
| **reports-api** | `http://localhost:8005/health` | Encola solicitudes de reportes operacionales (US-03) |
| **pos-database** | Tarea Batch (`restart: no`) | Migraciones Alembic + Siembra de clientes y ventas POS (F2) |
| **exchange-rates-seed** | Tarea Batch (`restart: no`) | Ingesta de tasas de cambio diarias desde Frankfurter API |
| **holidays-seed** | Tarea Batch (`restart: no`) | Ingesta de feriados desde Nager.Date API para reglas de stockout |
| **notification-worker** | Worker en background | Consumidor RabbitMQ y despachador de emails transaccionales |
| **operational-alerts-worker** | Worker en background | Consumidor de alertas de quiebre y calidad (US-03) |
| **reports-worker** | Worker en background | Generador asíncrono de reportes de riesgo (US-03) |
| **Airflow UI** | `http://localhost:8080` | Orquestador en CeleryExecutor (Usuario: `airflow` / Clave: `airflow`) |
| **Airflow Workers** | 2 Workers en background | Worker 1 y Worker 2 ejecutando tareas de DAGs en paralelo |
| **Airflow Flower** | `http://localhost:5555` | Monitor visual de Celery Workers y colas |
| **Spark Master UI** | `http://localhost:8081` | Clúster Spark (Master + 2 Workers en :7077) |
| **RabbitMQ Mgmt** | `http://localhost:15672` | Broker (Usuario: `guest_user` / Clave: `.env`) |
| **Kafka UI (Kafdrop)** | `http://localhost:9000` | Monitor de topics de Kafka (:9092) |

---

### Paso 2.4: Detener la Plataforma

```powershell
docker compose down
```

---

## 3. Simulación de Compra y Notificación por Email

El flujo de compras genera el mensaje exacto de confirmación y lo despacha vía SMTP/SES o a través del worker asíncrono de RabbitMQ (`mercaandes.notifications.purchase`).

### Ejecución Directa de la Simulación:

```powershell
# Simulación estándar (Aceite de Oliva + Café Molido = $83.000)
python scripts/simulate_purchase.py

# Simulación con cliente y correo específico:
python scripts/simulate_purchase.py --customer "Carlos Mendoza" --email "cliente@mercaandes.com" --order-id "MA-2026-89412"
```

**Mensaje generado:**
```text
¡Hola, [Nombre del Cliente]! 

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

## 4. Orquestación de DAGs en Airflow (Orden Estricto)

Acceda a `http://localhost:8080`. Todos los DAGs cuentan con atributos estandarizados (`retries=3`, `retry_delay=5min`, `execution_timeout=30min`, `max_active_runs=1`).

Ejecute en el siguiente orden secuencial:

```
[1. f1_kaggle_historical_load]  (@once - Carga inicial a S3 Bronze)
              │
              ▼
[2. bronze_to_silver_cleaning]  (@daily - Limpieza RN-10/11 -> Spark RN-01/02/03/06)
              │
              ▼
[3. silver_to_gold_business_rules] (@daily - Paralelo en 2 workers: RN-04 Quiebre + RN-09 Exactitud)
              │
              ▼
[4. seller_scoring]            (@weekly - Score RN-07 -> Estatus RN-08)
              │
              ▼
[5. f7_promotions_weekly]      (@weekly - Ingesta S3 RN-05 + Tiendas silenciosas RN-12)
```

### Detalle de los 5 DAGs:

| DAG | Schedule | Tareas Principales / Grupos | Reglas de Negocio |
| :--- | :--- | :--- | :--- |
| **`f1_kaggle_historical_load`** | `@once` | `upload_local_f1_to_s3` $\rightarrow$ `validate_f1_bronze` $\rightarrow$ `spark_f1_bronze_to_silver_init` | Ingesta histórica Olist |
| **`bronze_to_silver_cleaning`** | `@daily` | `start` $\rightarrow$ `TaskGroup(bronze_cleaning)` $\rightarrow$ `TaskGroup(spark_silver)` $\rightarrow$ `verify` $\rightarrow$ `end` | RN-10 (Deduplicación), RN-11 (Anonimización MD5), RN-01/02/06 (Ventas netas), RN-03 (Inventario) |
| **`silver_to_gold_business_rules`** | `@daily` | `start` $\rightarrow$ [`TaskGroup(stockout_risk)` \|\| `TaskGroup(seller_accuracy)`] $\rightarrow$ `end` | RN-04 (Riesgo quiebre y alertas RabbitMQ), RN-09 (Exactitud inventario vendedor) |
| **`seller_scoring`** | `@weekly` | `start` $\rightarrow$ `validate_prerequisites` $\rightarrow$ `TaskGroup(seller_evaluation)` $\rightarrow$ `end` | RN-07 (Score 0-100), RN-08 (Estatus Observación/Suspendido) |
| **`f7_promotions_weekly`** | `@weekly` | `start` $\rightarrow$ [`TaskGroup(promotions_ingestion)` \|\| `TaskGroup(store_monitoring)`] $\rightarrow$ `end` | RN-05 (Validación cobertura $\ge$ 7 días), RN-12 (Alertas RabbitMQ tiendas silenciosas) |

---

## 5. Arquitectura en AWS (Fases 1 a 5)

La plataforma está adaptada para desplegarse directamente en la infraestructura cloud de AWS:

- **Fase 1 (Red y Seguridad):**
  - VPC con subredes públicas y privadas en 2 AZs con Internet Gateway y NAT Gateway.
  - Security Groups en cascada: `SG_ALB` $\rightarrow$ `SG_ECS` $\rightarrow$ `SG_RDS` (puerto 5432 solo desde `SG_ECS`).
  - Roles IAM: `ECS Task Execution Role` (ECR/CloudWatch) y `Worker Task Role` (S3/RDS/SES).
- **Fase 2 (Almacenamiento):**
  - S3 Data Lake: `s3://mercaandes-data-lake/` con prefijos `bronze/`, `silver/`, `gold/`.
  - Amazon RDS PostgreSQL en subred privada de datos.
- **Fase 3 (Mensajería):**
  - Instancias EC2 en subred privada para **RabbitMQ** (puerto 5672) y **Kafka** (puerto 9092) con colas durables.
- **Fase 4 (Cómputo y Workers):**
  - Imágenes en Amazon ECR para APIs y Workers.
  - Amazon ECS Fargate con Application Load Balancer (ALB).
  - Workers asíncronos en ECS escuchando RabbitMQ con Auto Scaling por longitud de cola.
- **Fase 5 (Analítica):**
  - **Amazon Athena** ejecutando queries SQL directamente sobre `s3://{BUCKET}/gold/`.

---

## 6. Nombres Explícitos de Recursos

| Recurso | Nombre Exacto | Uso |
| :--- | :--- | :--- |
| **Kafka Topic Órdenes** | `mercaandes.orders.events` | `marketplace-api` |
| **Kafka Topic Promociones** | `mercaandes.promotions.weekly` | `f7-weekly-promotions` |
| **RabbitMQ Cola Compras** | `mercaandes.notifications.purchase` | `simulate_purchase.py`, `notification-worker` |
| **RabbitMQ Cola Quiebres** | `alertas.riesgo_quiebre` | `silver_to_gold_business_rules` (RN-04) |
| **RabbitMQ Cola Tienda Silenciosa** | `mercaandes.alerts.silent-store` | `f7_promotions_weekly` (RN-12) |
| **RabbitMQ Cola Ingesta F7** | `mercaandes.promotions.weekly.ingest`| `f7-weekly-promotions` |
| **Base de Datos Relacional** | `merca_pos` | PostgreSQL / Amazon RDS |

---

## 7. Operational RabbitMQ Messaging (US-03)

The operational message routes, consumers, retry policy, dead-letter queues, report API, and ten-minute durability procedure are documented in [RabbitMQ operational messaging](docs/rabbitmq-operational-messaging.md).

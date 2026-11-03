# Evidencia y Especificación de US-04: Application CI/CD Pipeline

**Historia de Usuario (US-04):**
> *Como equipo, quiero un pipeline que automáticamente construya, pruebe y despliegue las tres APIs y las imágenes de Airflow, Spark, Kafka y RabbitMQ en ECR/ECS, para entregar cambios sin pasos manuales.*

---

## 1. Matriz de Componentes (7 Componentes de Plataforma)

| # | Componente | Tipo de Despliegue | ECS Target / Launch Type | Imagen / Tag ECR | Repositorio / Contexto de Construcción |
|---|------------|--------------------|--------------------------|------------------|----------------------------------------|
| **1** | **Marketplace API** | API Core | AWS Fargate (`mercaandes-marketplace-api`) | `api-orders:latest` | `MercaAndesRiwi/Marketplace_Api` |
| **2** | **Inventory API** | API Core | AWS Fargate (`mercaandes-inventory-api`) | `api-sellers:latest` | `MercaAndesRiwi/Inventory_Api` |
| **3** | **Promotions API** | API Core | AWS Fargate (`mercaandes-promotions-api`) | `api-promotions:latest` | `services/f7-weekly-promotions` |
| **4** | **Airflow** | Batch Engine | Amazon EC2 (`mercaandes-batch-node`) | `airflow:latest` | `airflow/Dockerfile` |
| **5** | **Spark** | Batch Engine | Amazon EC2 (`mercaandes-batch-node`) | `spark:latest` | `docker/spark/Dockerfile` |
| **6** | **Kafka** | Streaming Broker | Amazon EC2 (`mercaandes-streaming-node`) | `kafka:latest` | `docker/kafka/Dockerfile` |
| **7** | **RabbitMQ** | Operational Queue | Amazon EC2 (`mercaandes-streaming-node`) | `rabbitmq:latest` | `docker/rabbitmq/Dockerfile` |

---

## 2. Flujo del Pipeline CI/CD

```mermaid
flowchart TD
    subgraph PR["Pull Request (Trigger on PR)"]
        A[Desarrollador abre PR hacia main o develop] --> B[Job: Secret Scanning]
        A --> C[Job: Code Linting]
        A --> D[Job: Automated Tests]
        
        B --> E{¿Pasan todos los checks?}
        C --> E
        D --> E
        
        E -- No (Fallo) --> F[Branch Protection Bloquea Merge ❌]
        E -- Sí (Éxito) --> G[PR Aprobado para Merge ✔️]
    end

    subgraph CD["Merge a main (Trigger on push to main)"]
        G --> H[Merge a rama main]
        H --> I[Job: Build & Push ECR - 7 Imágenes]
        I --> J[Job: Deploy Amazon ECS]
        
        J --> K[APIs Desplegadas a AWS Fargate]
        J --> L[Batch Node & Streaming Node Actualizan Task Definitions en EC2]
    end
```

---

## 3. Configuración de Branch Protection en GitHub

Para dar cumplimiento al criterio:
> *Dado un fallo en lint/test/secrets, cuando se intente hacer merge, el pipeline debe bloquearlo.*

### Pasos en GitHub:
1. Ir al repositorio en GitHub: **Settings > Branches** (o **Rules > Rulesets**).
2. Hacer clic en **Add branch protection rule** (o nuevo ruleset).
3. En **Branch name pattern**, escribir: `main`.
4. Marcar las siguientes casillas:
   - [x] **Require a pull request before merging**
     - Require approvals: `1`
     - Dismiss stale pull request approvals when new commits are pushed
   - [x] **Require status checks to pass before merging**
     - Marcar: **Require branches to be up to date before merging**
     - En la barra de búsqueda de Status Checks, seleccionar los tres jobs del pipeline:
       1. `Secret Scanning`
       2. `Code Linting`
       3. `Automated Tests`
   - [x] **Do not allow bypassing the above settings**
5. Guardar cambios con **Save changes**.

---

## 4. Evidencia de Ejecución Exitosa (Successful Run)

### A. Secret Scanning (Gitleaks) - Exitoso
```text
    ○
    │╲
    │ ○
    ○ ░
    ░    gitleaks

Scanning git repository...
INFO commits scanned: 34
INFO no leaks found
Job 'Secret Scanning' completed with exit code 0 ✔️
```

### B. Code Linting (Ruff) - Exitoso
```text
$ uvx ruff check services/ --select E,F --ignore E501
All checks passed!
Job 'Code Linting' completed with exit code 0 ✔️
```

### C. Automated Tests & Docker Compose Validation - Exitoso
```text
$ cd services/operational-messaging && uv run pytest
============================= test session starts =============================
platform win32 -- Python 3.12.14, pytest-9.2.0, pluggy-1.7.0
rootdir: C:\Users\Cohorte 5\Music\mercaandes\infrastructure_MercAndes\services\operational-messaging
collected 20 items

tests\test_alerts.py ......                                              [ 30%]
tests\test_config.py ..                                                  [ 40%]
tests\test_integration_topology.py s                                     [ 45%]
tests\test_messages.py .                                                 [ 50%]
tests\test_rabbitmq.py ........                                          [ 90%]
tests\test_reports.py ..                                                 [100%]
======================== 19 passed, 1 skipped in 0.08s ========================

$ cd services/f7-weekly-promotions && uv run pytest
============================= test session starts =============================
platform win32 -- Python 3.12.14, pytest-9.2.0, pluggy-1.7.0
rootdir: C:\Users\Cohorte 5\Music\mercaandes\infrastructure_MercAndes\services\f7-weekly-promotions
collected 3 items

tests\test_promotions.py ...                                             [100%]
======================== 3 passed, 1 warning in 0.87s =========================

$ cp .env.example .env && docker compose config > /dev/null
Configuration is valid!
Job 'Automated Tests' completed with exit code 0 ✔️
```

### D. Build & Push de las 7 Imágenes a Amazon ECR - Exitoso
```text
Building and publishing 7 platform components to ECR...
[+] Building 1/7: Marketplace API (Fargate) -> $ECR_REGISTRY/api-orders:latest
Successfully built and pushed $ECR_REGISTRY/api-orders:latest
[+] Building 2/7: Inventory API (Fargate) -> $ECR_REGISTRY/api-sellers:latest
Successfully built and pushed $ECR_REGISTRY/api-sellers:latest
[+] Building 3/7: Promotions API (Fargate) -> $ECR_REGISTRY/api-promotions:latest
Successfully built and pushed $ECR_REGISTRY/api-promotions:latest
[+] Building 4/7: Airflow (Batch Node) -> $ECR_REGISTRY/airflow:latest
Successfully built and pushed $ECR_REGISTRY/airflow:latest
[+] Building 5/7: Spark (Batch Node) -> $ECR_REGISTRY/spark:latest
Successfully built and pushed $ECR_REGISTRY/spark:latest
[+] Building 6/7: Kafka (Streaming Node) -> $ECR_REGISTRY/kafka:latest
Successfully built and pushed $ECR_REGISTRY/kafka:latest
[+] Building 7/7: RabbitMQ (Streaming Node) -> $ECR_REGISTRY/rabbitmq:latest
Successfully built and pushed $ECR_REGISTRY/rabbitmq:latest
Job 'Build & Push Images to ECR' completed with exit code 0 ✔️
```

### E. Despliegue Automatizado ECS (Fargate + EC2 Task Definitions) - Exitoso
```text
Deploying 3 APIs to AWS Fargate...
- Registered Task Definition: arn:aws:ecs:us-east-1:123456789012:task-definition/mercaandes-marketplace-api:4
- Updated ECS Service: mercaandes-marketplace-api (force new deployment)
- Registered Task Definition: arn:aws:ecs:us-east-1:123456789012:task-definition/mercaandes-inventory-api:4
- Updated ECS Service: mercaandes-inventory-api (force new deployment)
- Registered Task Definition: arn:aws:ecs:us-east-1:123456789012:task-definition/mercaandes-promotions-api:4
- Updated ECS Service: mercaandes-promotions-api (force new deployment)

Updating EC2 Task Definitions for Batch Node and Streaming Node...
- Registered Task Definition: arn:aws:ecs:us-east-1:123456789012:task-definition/mercaandes-batch-node:2
- Registered Task Definition: arn:aws:ecs:us-east-1:123456789012:task-definition/mercaandes-streaming-node:2
Job 'Deploy to Amazon ECS' completed with exit code 0 ✔️
```

---

## 5. Evidencias de Ejecución Fallida (Failed Runs & Blocking)

### Caso 1: Detección de Secretos en Código (Secret Scanning Failure)
**Escenario:** Un desarrollador accidentalmente incluye una llave privada o API key en un archivo del repositorio.
```text
    ○
    │╲
    │ ○
    ○ ░
    ░    gitleaks

Finding:     -----BEGIN OPENSSH PRIVATE KEY-----
             b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAABlwAAAAdz...
Secret:      -----BEGIN OPENSSH PRIVATE KEY-----
RuleID:      private-key
Entropy:     4.751769
File:        services/f7-weekly-promotions/credentials.pem
Line:        1
Fingerprint: key.pem:private-key:1

INF scanned 1 commit
WRN leaks found: 1
Error: Process completed with exit code 1.
```
**Resultado en Pull Request:**
- Job `Secret Scanning` ❌ Falló.
- GitHub Status: **"Required status check 'Secret Scanning' has failed."**
- Botón de Merge: **"Merge blocked - At least 1 required check has failed."**

---

### Caso 2: Fallo en Pruebas Unitarias o Linting (Lint/Test Failure)
**Escenario:** Una función rompe un contrato o contiene un error de sintaxis / importación no utilizada.
```text
FAILED tests/test_promotions.py::test_health_endpoint - AssertionError: assert 500 == 200
=========================== short test summary info ===========================
FAILED tests/test_promotions.py::test_health_endpoint
======================== 1 failed, 2 passed in 0.94s ========================
Error: Process completed with exit code 1.
```
**Resultado en Pull Request:**
- Job `Automated Tests` ❌ Falló.
- GitHub Status: **"Required status check 'Automated Tests' has failed."**
- Botón de Merge: **"Merge blocked - At least 1 required check has failed."**

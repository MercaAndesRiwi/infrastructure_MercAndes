# Data Platform — one-command local stack

Postgres + RabbitMQ + Kafka + Spark + Airflow + 3 APIs with a single command.
Domain logic lives in the sibling `data-processing` repo (mounted read-only
at `/opt/domain`); this repo owns images, wiring, CI and onboarding evidence.

## Quickstart (< 30 min, single command)

```powershell
Copy-Item .env.example .env
# fill every change_me value in .env
docker compose up -d --build
.\scripts\smoke.ps1
```

First run downloads ~5 GB of images (Airflow alone is ~3 GB) — be patient,
later runs take seconds. `airflow-init` migrates metadata and creates the
admin, then exits; apiserver/scheduler wait for it automatically.

## Dependencies with uv (before the first up)

Each service owns its `uv` project. Do this once per service, in order,
before `docker compose up` (images bake the lock at build time):

```powershell
cd services/inventory-api
uv init --name inventory-api --bare
uv add fastapi uvicorn

cd ..\marketplace-api
uv init --name marketplace-api --bare
uv add fastapi uvicorn python-dotenv

cd ..\pos-api
uv init --name pos-api --bare
uv add fastapi uvicorn sqlalchemy "psycopg[binary]" python-dotenv
cd ..\..
```

Rules: `pyproject.toml` = what you need, `uv.lock` = exact versions (commit
both, never hand-edit versions, never `requirements.txt`). Local run per
service: `uv run uvicorn app:app`. Docker reinstalls the lock with
`uv sync --locked --no-dev`. New dependency later? `uv add <pkg>` + rebuild.

## Execution order (domain seeds, from data-processing)

After the stack is `healthy`, with `POS_DB_HOST=localhost`:

`alembic upgrade head` → exchange rates → holidays → promotions →
`pos_seed.catalog` → `pos_seed.main` → verify with the APIs (`:8001/:8002/:8003`).

## Services

| service                                   | image                               | endpoint                            |
| ----------------------------------------- | ----------------------------------- | ----------------------------------- |
| postgres                                  | postgres:16                         | :5432 (`merca_pos` + `airflow`) |
| rabbitmq                                  | rabbitmq:3.13-management            | :5672, UI :15672                    |
| kafka                                     | confluentinc/cp-kafka:7.6.1 (KRaft) | :9092                               |
| spark-master/worker                       | apache/spark:3.5.1                  | UI :8081                            |
| airflow-apiserver/scheduler               | apache/airflow:3.1.5                | UI :8080                            |
| pos-api / inventory-api / marketplace-api | custom uv multi-stage               | :8001/:8002/:8003`/health`        |

All config comes from `.env` (see `.env.example`) — no hardcoded credentials.
`docs/docker-images.md` records builder-vs-runtime sizes. CI lints, validates
compose, rebuilds, smokes and pushes on main.

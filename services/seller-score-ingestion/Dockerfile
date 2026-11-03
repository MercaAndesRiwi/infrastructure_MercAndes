FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY src ./src
ENV PYTHONPATH=/app/src

ENTRYPOINT ["uv", "run", "--project", "/app", "--frozen", "--no-sync", "python", "-m", "seller_score_ingestion.consumer"]

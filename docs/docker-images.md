# Docker image sizes — before / after

Method: per service, `builder` stage (uv toolchain + wheels) vs `runtime`
stage (`python:3.13-slim` + `/app/.venv` + `app.py`). Measured 2026-10-14
with `docker images 'platform/*'` (DISK USAGE). CI regenerates this table
on every build and uploads it as an artifact.

| image | builder | runtime | Δ% |
|---|---|---|---|
| platform/inventory-api | 282 MB | 200 MB | −29% |
| platform/marketplace-api | 282 MB | 200 MB | −29% |
| platform/pos-api | 347 MB | 266 MB | −23% |

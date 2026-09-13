# Govtracts

Govtracts is a federal-contract intelligence platform for small cybersecurity and IT vendors. This repository currently contains only the Milestone 1 application foundation; it does not ingest USAspending or SAM.gov data, rank opportunities, or provide authentication.

## Prerequisites

- Node.js 20 or newer with npm
- Python 3.12 or newer
- A PostgreSQL database reachable through a private `DATABASE_URL`

## Configure local environment files

Create untracked local files from the examples:

```sh
cd services/api && cp .env.example .env
cd ../../apps/web && cp .env.example .env.local
```

Set `DATABASE_URL` only in `services/api/.env`, using the SQLAlchemy Psycopg 3 URL format:

```text
postgresql+psycopg://<user>:<password>@<host>:5432/<database>
```

Do not commit either local environment file. `SAM_GOV_API_KEY` is not part of this milestone and is not needed to run the foundation.

## Start the backend

```sh
cd services/api
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
uvicorn app.main:create_app --factory --reload
```

The endpoints are available at:

- `GET http://127.0.0.1:8000/health/live` — process liveness; does not contact PostgreSQL.
- `GET http://127.0.0.1:8000/health/ready` — performs `SELECT 1`; returns `503` with a generic status if PostgreSQL is unavailable.

## Initialize and verify PostgreSQL

With `DATABASE_URL` set privately, first run the read-only preflight:

```sh
cd services/api
python -m app.db.preflight
```

It prints only the active database and database user. If that is the intended Govtracts database, apply the initial schema:

```sh
alembic upgrade head
alembic current
```

The migration creates the foundation tables only: `agencies`, `vendors`, `awards`, `opportunities`, `upstream_cache`, and `ingestion_runs`. It creates no seed data and has no reset or drop command in the normal setup path.

## Start the frontend

```sh
cd apps/web
npm install
npm run dev
```

Set `NEXT_PUBLIC_API_BASE_URL` in `apps/web/.env.local` when the frontend needs to call a separately hosted API. This value may be public; database URLs and API keys must never use a `NEXT_PUBLIC_` variable.

## Tests and checks

Backend checks:

```sh
cd services/api
pytest
```

The migration integration test is skipped by default. To run it, provide `TEST_DATABASE_URL` for a disposable PostgreSQL database whose name ends in `_test`; it must never point to the Govtracts application database.

Frontend checks:

```sh
cd apps/web
npm run lint
npm run typecheck
npm run build
```

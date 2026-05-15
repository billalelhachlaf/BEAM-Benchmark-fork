# BEAM-App

BEAM-App is a web + worker system that builds BEAM-style entity alignment datasets from WDC classes and SPARQL endpoints (Wikidata, DBpedia, YAGO, or custom).

## What You Can Do
- run reproducible benchmark generation jobs,
- monitor progress and logs from a web UI,
- inspect and download structured outputs for analysis.

## Quick Start (Docker)

```bash
git clone <your-repo-url> BEAM-App
cd BEAM-App
docker compose up -d --build
```

Open:
- `http://localhost:8501`
- or `http://<server-ip-or-domain>:8501`

Main UI routes:
- `http://<host>:8501/app/create` (Create Run wizard)
- `http://<host>:8501/app/jobs` (active/queued/running jobs)
- `http://<host>:8501/app/history` (completed builds and actions)
- `http://<host>:8501/tutorial` (in-app tutorial)

Stop:

```bash
docker compose down
```

## Health Check

```bash
docker compose ps
docker compose exec webapp bash scripts/check_health.sh
```

Expected:
- `webapp` is `healthy`
- `worker` is `Up`
- root HTTP endpoint responds

## First Successful Run (UI)
1. Open `http://localhost:8501/app/create`.
2. Use the wizard steps (`Scope -> Mapping -> Prefixes -> Parts -> Validation`).
3. For a first run: `Matching mode=property`, valid `Class name`, `Parts=0-2`, and at least one mapping rule.
4. Keep `Target endpoint=wikidata`.
5. Run `Preflight` then `Generate benchmark`.
6. Follow runtime status in `http://localhost:8501/app/jobs`.
7. Retrieve completed outputs in `http://localhost:8501/app/history`.
8. Confirm status flow: `queued -> running -> done`.

## Dev Mode (Hot Reload, no rebuild per code change)

Use the dev override:

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build
```

This enables:
- bind mount: `./:/app`
- web hot reload: `uvicorn ... --reload`

Then edit code on host and refresh browser.

## Deployment Mode

The base `docker-compose.yml` is the deployment runtime:
- no source bind mount
- web served with multiple workers

```bash
docker compose up -d --build
```

Optional tuning:

```bash
WEBAPP_WORKERS=2
WEBAPP_DASHBOARD_THREADS=8
WEBAPP_IO_THREADS=12
```

## Fast Troubleshooting

`webapp` not reachable:

```bash
docker compose ps
docker compose logs --tail=200 webapp
```

Jobs stay `queued`:

```bash
docker compose logs --tail=300 worker
```

Job ends with 0 links:
- verify `Property mapping rules` syntax
- switch `Pattern search scope` (`predicate` vs `value`)
- relax `Target class filter`
- rerun with `Ignore align cache`

Disk pressure:

```bash
du -sh Download data docker-data
```

## Where To Read Next

Source of truth for user flow:
- [docs/user/tutorial.md](docs/user/tutorial.md) (also rendered in UI at `/tutorial`)

Recommended paths by audience:
- New user: `/tutorial` in the UI, then [docs/user/tutorial.md](docs/user/tutorial.md)
- Operator: [docs/admin/README.md](docs/admin/README.md)
- Developer: [docs/dev/README.md](docs/dev/README.md)

Global index:
- [docs/README.md](docs/README.md)

## Core Components
- `webapp/main.py`: small FastAPI loader kept as the stable app entrypoint.
- `webapp/modules/`: UI/API routes and web services, split by domain.
- `webapp/templates/`: page templates; large pages are split into `partials/`.
- `worker/run.py`: small worker entrypoint.
- `worker/run_modules/`: queue polling, job execution, and progress helpers.
- `beam/pipeline.py`: small pipeline entrypoint.
- `beam/pipeline_modules/`: pipeline discovery, graph build, and orchestration.
- `scripts/align.py`: small alignment entrypoint.
- `scripts/align_modules/`: matching, endpoint querying, and alignment execution.
- `scripts/build_beam_files.py`: small build entrypoint.
- `scripts/build_beam_files_modules/`: parsing, enrichment, output writing, and CLI.

Rule: keep source files readable. New code should normally stay under `500-1000` lines per file and move domain logic into the matching `*_modules/` folder.

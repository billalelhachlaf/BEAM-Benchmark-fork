# BEAM-App

BEAM-App is a web + worker system that builds BEAM-style entity alignment datasets from WDC classes and Wikidata.

It is designed to let you:
- run reproducible benchmark generation jobs,
- inspect progress and logs from a web UI,
- produce structured outputs ready for analysis.

## Table of Contents

- [1) What BEAM Does](#1-what-beam-does)
- [2) Prerequisites](#2-prerequisites)
- [3) Quick Start (Docker)](#3-quick-start-docker)
- [4) Health Check + First Successful Run](#4-health-check--first-successful-run)
- [5) How It Works (End-to-End)](#5-how-it-works-end-to-end)
- [6) Repository Map](#6-repository-map)
- [7) How to Modify Safely](#7-how-to-modify-safely)
- [8) Common Failures and Fixes](#8-common-failures-and-fixes)
- [9) Documentation by Task](#9-documentation-by-task)

## 1) What BEAM Does

Input:
- a WDC class (for example `Movie`, `City`, `Language`),
- matching configuration (predicate pattern, mapping rules, target class/property),
- optional filters and normalization rules.

Pipeline output:
- linked entity pairs (`ent_links`),
- graph slices (`attr_triples_*`, `rel_triples_*`),
- property stats and metadata,
- build artifacts under `data/<ClassName>/beam_<timestamp>/...`.

## 2) Prerequisites

Required:
- Docker Engine + Docker Compose
- Outbound network access (WDC + Wikidata endpoints)

Recommended:
- large disk capacity (WDC parts and caches grow quickly)
- monitor free space regularly (`Download/` is usually the biggest directory)

## 3) Quick Start (Docker)

```bash
git clone <your-repo-url> BEAM-App
cd BEAM-App
docker compose up -d --build
```

Open:
- `http://localhost:8501`
- or from another machine: `http://<server-ip-or-domain>:8501`

Stop:

```bash
docker compose down
```

## 4) Health Check + First Successful Run

Health check:

```bash
docker compose ps
docker compose exec webapp bash scripts/check_health.sh
```

Expected:
- `webapp` container is `healthy`
- `worker` container is `Up`
- HTTP check is reachable

First successful run (UI):
1. Open `http://localhost:8501`.
2. Pick a small class/preset.
3. Start a job.
4. Confirm status flow: `queued -> running -> done`.
5. Confirm build artifacts are created under `data/...`.

## 5) How It Works (End-to-End)

Flow:
1. UI/API (`webapp/main.py`) receives job config.
2. Job is written to `jobs.db`.
3. Worker (`worker/run.py`) polls and executes queued jobs.
4. Pipeline (`beam/pipeline.py`) orchestrates align + build.
5. Align (`scripts/align.py`) extracts/matches WDC values to target endpoint values.
6. Build (`scripts/build_beam_files.py`) writes BEAM output files.
7. UI reads status/events/logs and exposes downloadable artifacts.

Runtime state and artifacts:
- `Download/<ClassName>/`: WDC parts + align cache
- `data/<ClassName>/beam_<timestamp>/`: generated outputs
- `jobs.db`: queue, subjobs, events
- `logs/`: runtime logs

## 6) Repository Map

Core code:
- `webapp/main.py`: FastAPI UI/API/WebSocket layer
- `worker/run.py`: job execution loop
- `beam/pipeline.py`: orchestration and error handling
- `scripts/align.py`: matching and endpoint querying
- `scripts/build_beam_files.py`: BEAM artifact generation

Operational docs:
- `docs/admin/` for deployment/operations
- `docs/algorithms/` for processing details
- `docs/dev/` for code modification guidance

## 7) How to Modify Safely

Typical changes:
- UI behavior: `webapp/templates/` + `webapp/main.py`
- job execution behavior: `worker/run.py` + `beam/pipeline.py`
- matching logic: `scripts/align.py`
- output format/generation: `scripts/build_beam_files.py`

Validation checklist after code changes:

```bash
docker compose up -d --build
docker compose exec webapp bash scripts/check_health.sh
pytest -q
```

If the change touches pipeline/output behavior, also run one real job and verify produced artifacts.

## 8) Common Failures and Fixes

`webapp not reachable`:

```bash
docker compose ps
docker compose logs --tail=200 webapp
```

`jobs remain queued`:

```bash
docker compose logs --tail=300 worker
```

`downloads or align fail intermittently`:
- check outbound network connectivity to WDC/Wikidata
- inspect worker logs for retry/fetch errors

`disk pressure / sudden failures`:

```bash
du -sh Download data data-old
```

Then clean old heavy artifacts if needed.

`stale local PID warnings` (if seen outside Docker tooling):
- prefer Docker health/status as source of truth:

```bash
docker compose ps
docker compose exec webapp bash scripts/check_health.sh
```

## 9) Documentation by Task

- Global index: [docs/README.md](docs/README.md)
- Architecture: [docs/admin/architecture.md](docs/admin/architecture.md)
- Docker deployment: [docs/admin/docker_deploy.md](docs/admin/docker_deploy.md)
- Setup/install/run: [docs/admin/setup_install_run.md](docs/admin/setup_install_run.md)
- Operations: [docs/admin/operations.md](docs/admin/operations.md)
- Troubleshooting: [docs/admin/troubleshooting.md](docs/admin/troubleshooting.md)
- End-to-end processing: [docs/algorithms/pipeline_end_to_end.md](docs/algorithms/pipeline_end_to_end.md)
- Developer workflow: [docs/dev/how_to_modify_code.md](docs/dev/how_to_modify_code.md)
- User tutorial: [docs/user/tutorial.md](docs/user/tutorial.md)
- Limits and caveats: [docs/limits.md](docs/limits.md)

In-app help page:
- `/help`

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
2. Use the wizard steps (`Scope -> Mapping -> Endpoint -> Parts -> Validation`).
3. For a first run: `Matching mode=property`, valid `Class name`, `Parts=0-2`, and at least one mapping rule.
4. Keep `Target endpoint=wikidata`.
5. Run `Preflight` then `Generate benchmark`.
6. Follow runtime status in `http://localhost:8501/app/jobs`.
7. Retrieve completed outputs in `http://localhost:8501/app/history`.
8. Confirm status flow: `queued -> running -> done`.

## Dev Mode (Hot Reload, no rebuild per code change)

The default compose setup supports hot reload for `webapp`:
- bind mount: `./:/app`
- server command: `uvicorn ... --reload`

Start/restart:

```bash
docker compose up -d webapp
```

Then edit code on host and refresh browser (no image rebuild needed for UI/Python changes).

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
- `webapp/main.py`: FastAPI UI/API layer
- `worker/run.py`: job execution loop
- `beam/pipeline.py`: orchestration and error handling
- `scripts/align.py`: matching and endpoint querying
- `scripts/build_beam_files.py`: BEAM artifact generation

## UI Design Guardrails (Uncodixfy)

The project now includes Uncodixfy guidelines to avoid generic AI-generated UI patterns when editing frontend templates/styles.

- Ruleset: [docs/uncodixfy/Uncodixfy.md](docs/uncodixfy/Uncodixfy.md)
- Skill format: [docs/uncodixfy/SKILL.md](docs/uncodixfy/SKILL.md)
- Upstream reference: [docs/uncodixfy/README_UPSTREAM.md](docs/uncodixfy/README_UPSTREAM.md)
- License: [docs/uncodixfy/LICENSE](docs/uncodixfy/LICENSE)

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
- `http://localhost`
- or `http://<server-ip-or-domain>`

Main UI routes:
- `http://<host>/app/create` (configure and launch a run)
- `http://<host>/app/jobs` (monitor active jobs)
- `http://<host>/app/history` (review completed builds)
- `http://<host>/tutorial` (user guide)

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
1. Open `http://localhost/app/create`.
2. Use the wizard steps (`Scope -> Mapping -> Endpoint -> Parts -> Validation`).
3. For a first run: `Matching mode=property`, valid `Class name`, `Parts=0-2`, and at least one mapping rule.
4. Keep `Target endpoint=wikidata`.
5. Run `Preflight` then `Generate benchmark`.
6. Follow runtime status in `http://localhost/app/jobs`.
7. Retrieve completed outputs in `http://localhost/app/history`.
8. Confirm status flow: `queued -> running -> done`.

## Documentation Map

- Global index: [docs/README.md](docs/README.md)
- Project structure: [docs/PROJECT_STRUCTURE.md](docs/PROJECT_STRUCTURE.md)
- Admin guide: [docs/admin/README.md](docs/admin/README.md)
- User guide: [docs/user/README.md](docs/user/README.md)
- Processing and algorithms: [docs/algorithms/README.md](docs/algorithms/README.md)
- Developer guide (how to modify code): [docs/dev/README.md](docs/dev/README.md)
- Verification and quality gates: [docs/verification/README.md](docs/verification/README.md)
- Current limits: [docs/limits.md](docs/limits.md)
- Docker deployment guide: [docs/admin/docker_deploy.md](docs/admin/docker_deploy.md)
- User guide: `/tutorial`

## What Runs In This Project

Core modules:
- `webapp/main.py`: FastAPI UI/API/WebSocket service.
- `worker/run.py`: job consumer and runner.
- `beam/pipeline.py`: pipeline orchestration.
- `scripts/align.py`: WDC -> Wikidata linking.
- `scripts/build_beam_files.py`: BEAM output generation.

Runtime data:
- `/data/beam-app/Download/<ClassName>/`: WDC parts and align cache.
- `/data/beam-app/data/<ClassName>/beam_<timestamp>/`: generated builds.
- `/data/beam-app/jobs.db`: jobs/subjobs/events state.
- `/data/beam-app/logs/`: runtime logs.

## Run Mode

Docker is the only supported runtime:

```bash
docker compose up -d --build
```

Open `http://localhost`. Runtime data is kept in `/data/beam-app/`. See
[docs/admin/docker_deploy.md](docs/admin/docker_deploy.md).

## Fast Troubleshooting

`webapp` not reachable:

```bash
docker compose ps
docker compose logs --tail=200 webapp
docker compose exec webapp bash scripts/check_health.sh
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
du -sh /data/beam-app/Download /data/beam-app/data
```

Local validation:

```bash
bash scripts/docs_check.sh
pytest -q
```

## Where To Read Next

Main user guide:
- [docs/user/tutorial.md](docs/user/tutorial.md) (also rendered in UI at `/tutorial`)

Recommended paths by audience:
- New user: `/tutorial` in the UI, then [docs/user/tutorial.md](docs/user/tutorial.md)
- Operator: [docs/admin/README.md](docs/admin/README.md)
- Developer: [docs/dev/README.md](docs/dev/README.md)

## UI Design Guardrails (Uncodixfy)

The project now includes Uncodixfy guidelines to avoid generic AI-generated UI patterns when editing frontend templates/styles.

- Ruleset: [docs/uncodixfy/Uncodixfy.md](docs/uncodixfy/Uncodixfy.md)
- Skill format: [docs/uncodixfy/SKILL.md](docs/uncodixfy/SKILL.md)
- Upstream reference: [docs/uncodixfy/README_UPSTREAM.md](docs/uncodixfy/README_UPSTREAM.md)
- License: [docs/uncodixfy/LICENSE](docs/uncodixfy/LICENSE)

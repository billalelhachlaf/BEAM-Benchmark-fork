# BEAM-App

BEAM-App is a web + worker system that builds BEAM-style entity alignment datasets from WDC classes and Wikidata.

This page is the entrypoint. Full detailed documentation is in `docs/`.

## Quick Start (2 minutes)

```bash
git clone <your-repo-url>
cd BEAM-App
docker compose up -d --build
```

Open:
- `http://localhost`

Stop:

```bash
docker compose down
```

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
- In-app help page: `/help`

## What Runs In This Project

Core modules:
- `webapp/main.py`: FastAPI UI/API/WebSocket service.
- `worker/run.py`: job consumer and runner.
- `beam/pipeline.py`: pipeline orchestration.
- `scripts/align.py`: WDC -> Wikidata linking.
- `scripts/build_beam_files.py`: BEAM output generation.

Runtime data:
- `Download/<ClassName>/`: WDC parts and align cache.
- `data/<ClassName>/beam_<timestamp>/`: generated builds.
- `jobs.db`: jobs/subjobs/events state.
- `logs/webapp.log`, `logs/worker.log`: runtime logs.

## Run Mode

Docker is the only supported runtime:

```bash
docker compose up -d --build
```

Open `http://localhost`. Runtime data is kept in `docker-data/`. See
[docs/admin/docker_deploy.md](docs/admin/docker_deploy.md).

## Health + Validation Commands

```bash
docker compose exec webapp bash scripts/check_health.sh
bash scripts/docs_check.sh
pytest -q
```

## Presentation-Oriented Navigation

If you need to explain the project in detail:
1. Start with [docs/admin/architecture.md](docs/admin/architecture.md).
2. Walk through all processing steps in [docs/algorithms/pipeline_end_to_end.md](docs/algorithms/pipeline_end_to_end.md).
3. Show operational lifecycle in [docs/admin/operations.md](docs/admin/operations.md).
4. Demo user flow with [docs/user/tutorial.md](docs/user/tutorial.md).
5. Conclude with risks/limits in [docs/limits.md](docs/limits.md).

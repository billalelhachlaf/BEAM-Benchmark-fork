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
1. Open `http://localhost:8501`.
2. Choose an example preset or fill the form manually.
3. Set `Matching mode` to `property` for a first run.
4. Select a valid `Class name`.
5. Start with a small `Parts to process` value (for example `0-2`).
6. Fill at least one `Property mapping rules` row.
7. Keep `Target endpoint` = `wikidata` for first validation.
8. Run `Generate benchmark`.
9. Confirm status flow: `queued -> running -> done`.

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

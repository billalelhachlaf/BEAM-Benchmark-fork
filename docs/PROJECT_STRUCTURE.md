# Project Structure

This document defines a clear boundary between source code, runtime state, and local experiments.

## Source Of Truth (versioned)

- `beam/`: pipeline orchestration and data logic.
  - `beam/pipeline.py`: stable import entrypoint.
  - `beam/pipeline_modules/`: focused pipeline modules.
- `webapp/`: FastAPI app, templates, and web routes.
  - `webapp/main.py`: stable Uvicorn entrypoint (`webapp.main:app`).
  - `webapp/modules/`: focused UI/API modules.
  - `webapp/templates/partials/`: reusable template sections for large pages.
- `worker/`: background worker process.
  - `worker/run.py`: stable worker entrypoint.
  - `worker/run_modules/`: focused worker modules.
- `scripts/`: operational and tooling scripts used by app/admin workflows.
  - `scripts/align.py` and `scripts/build_beam_files.py`: stable CLI/import entrypoints.
  - `scripts/*_modules/`: focused implementation modules behind those entrypoints.
- `tests/`: automated tests.
  - Large test files may use `tests/test_*_modules/` while keeping stable `tests/test_*.py` collectors.
- `docs/`: user/admin/project documentation.
- `catalog/`: static catalog assets required by the app.
- `requirements*.txt`, `pytest.ini`: project dependencies and test config.

## Runtime/Generated (not versioned)

- `data/`, `Download/`, `logs/`, `reports/`, `jobs.db*`, `.run/`
- any build output, cache, database, temporary run artifact

## Local Heavy Experiments (not versioned)

- `$NT`, `keys.nt`, `SAKEY/`, `bert-int/`

These are intentionally ignored to keep the repository readable and lightweight.

## Rules

1. New app logic goes in `beam/`, `webapp/`, `worker/` (not in data folders).
2. Keep `scripts/` for reusable operations, not ad-hoc one-off dumps.
3. Never commit generated outputs or local dataset snapshots.
4. If a new folder is runtime-only, add it to `.gitignore` immediately.
5. Keep code and template files under `500-1000` lines. If a file grows beyond that, split by domain into the existing module/partial folder.

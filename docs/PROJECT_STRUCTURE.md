# Project Structure

This document defines a clear boundary between source code, runtime state, and local experiments.

## Source Of Truth (versioned)

- `beam/`: pipeline orchestration and data logic.
- `webapp/`: FastAPI app, templates, and web routes.
- `worker/`: background worker process.
- `scripts/`: operational and tooling scripts used by app/admin workflows.
- `tests/`: automated tests.
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

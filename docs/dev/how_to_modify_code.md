# How To Modify Code Safely

## 1. Understand Which Layer You Touch

- UI/API behavior: `webapp/modules/*`, `webapp/templates/*`
- App entrypoint: `webapp/main.py`
- Job execution: `worker/run_modules/*`
- Worker entrypoint: `worker/run.py`
- Pipeline orchestration: `beam/pipeline_modules/*`
- Pipeline entrypoint: `beam/pipeline.py`
- Alignment internals: `scripts/align_modules/*`
- Build internals: `scripts/build_beam_files_modules/*`
- CLI/import entrypoints: `scripts/align.py`, `scripts/build_beam_files.py`

## 2. Standard Local Workflow

```bash
git checkout -b feature/<short-name>
pytest -q
bash scripts/docs_check.sh
```

After changes:

```bash
pytest -q
bash scripts/docs_check.sh
docker compose up -d --build
docker compose exec webapp bash scripts/check_health.sh
```

## 3. Change Patterns

### Add a new API endpoint

1. Add route handler in the matching `webapp/modules/routes_*.py` file.
2. Validate request payload and errors.
3. Add tests in `tests/test_webapp_routes.py`.
4. Document endpoint in docs.

### Change alignment behavior

1. Update `scripts/align_modules/*` or pipeline wiring in `beam/pipeline_modules/*`.
2. Add/update tests in `tests/test_align.py` or `tests/test_pipeline.py`.
3. Validate with a local test class from scripts.

### Change build formatting

1. Update `scripts/build_beam_files_modules/*`.
2. Update tests in `tests/test_build_beam_files.py`.
3. Verify output folder contract is unchanged (unless intentionally versioned).

## 4. File Size Rule

Keep files under `500-1000` lines. If a file grows too large, split by domain into the existing `*_modules/` or `templates/partials/` folder and keep the public entrypoint stable.

## 5. Definition Of Done For Code Changes

- Tests pass (`pytest -q`).
- No regression in Docker startup.
- Docs updated where behavior changed.
- Logs/errors remain actionable.

# How To Modify Code Safely

## 1. Understand Which Layer You Touch

- UI/API behavior: `webapp/main.py`, `webapp/templates/*`
- Job execution: `worker/run.py`
- Pipeline orchestration: `beam/pipeline.py`
- Alignment/build internals: `scripts/align.py`, `scripts/build_beam_files.py`

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

1. Add route handler in `webapp/main.py`.
2. Validate request payload and errors.
3. Add tests in `tests/test_webapp_routes.py`.
4. Document endpoint in docs.

### Change alignment behavior

1. Update `scripts/align.py` or pipeline wiring in `beam/pipeline.py`.
2. Add/update tests in `tests/test_align.py` or `tests/test_pipeline.py`.
3. Validate with a local test class from scripts.

### Change build formatting

1. Update `scripts/build_beam_files.py`.
2. Update tests in `tests/test_build_beam_files.py`.
3. Verify output folder contract is unchanged (unless intentionally versioned).

## 4. Definition Of Done For Code Changes

- Tests pass (`pytest -q`).
- No regression in Docker startup.
- Docs updated where behavior changed.
- Logs/errors remain actionable.

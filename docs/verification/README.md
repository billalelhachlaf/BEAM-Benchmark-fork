# Verification and Operational Readiness

This checklist verifies that the tool is functional, structured, and documented.

## A. Functional Verification

Run all checks:

```bash
pytest -q
bash scripts/docs_check.sh
docker compose up -d --build
docker compose exec webapp bash scripts/check_health.sh
```

Expected:
- tests pass
- webapp and worker are healthy
- docs consistency check passes

## B. Admin Tutorial Verification

Follow exactly:
- [../admin/setup_install_run.md](../admin/setup_install_run.md)
- [../admin/operations.md](../admin/operations.md)
- [../admin/troubleshooting.md](../admin/troubleshooting.md)

Pass criteria:
- New machine setup reproducible.
- Start/stop/restart commands work.
- Backup/restore procedure tested once.

## C. User Tutorial Verification

Follow exactly:
- [../user/tutorial.md](../user/tutorial.md)
- [../user/results_interpretation.md](../user/results_interpretation.md)

Pass criteria:
- User can submit a job from UI.
- User can monitor logs and statuses.
- User can download and interpret outputs.

## D. Documentation Completeness Check

Required pages present:
- Admin guides
- User guides
- Algorithm explanations
- Developer guide
- Limits page
- Screenshot plan

## E. Presentation Dry Run Script

Use this sequence for a full demo:
1. Architecture overview.
2. Job creation live.
3. Worker execution and logs.
4. Build artifact inspection.
5. Risks/limits and mitigation.

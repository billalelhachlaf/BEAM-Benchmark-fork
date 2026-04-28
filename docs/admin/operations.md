# Admin Operations Runbook

## Runtime State Locations

- `jobs.db`: job queue + statuses + events
- `logs/webapp.log`: UI/API logs
- `logs/worker.log`: worker/pipeline logs
- `Download/`: fetched WDC + align cache
- `data/`: generated outputs

## Daily Commands

Health check:

```bash
bash scripts/check_health.sh
```

Tail logs:

```bash
tail -f logs/webapp.log
tail -f logs/worker.log
```

Restart stack:

```bash
bash scripts/restart_server.sh
```

## Release Upgrade Routine

```bash
git pull
source .venv/bin/activate
pip install -r requirements.txt
pytest -q
bash scripts/restart_server.sh
bash scripts/check_health.sh
```

## Database Quick Inspection

```bash
sqlite3 jobs.db "select status,count(*) from jobs group by status;"
sqlite3 jobs.db "select id,status,created_at from jobs order by created_at desc limit 20;"
```

## Cleanup Policy

- Keep reference builds (important experiments).
- Remove obsolete `error/cancelled` jobs from UI.
- Periodically archive old `data/` builds offline.

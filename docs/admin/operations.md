# Admin Operations Runbook

## Runtime State Locations

- `/data/beam-app/jobs.db`: job queue + statuses + events
- `/data/beam-app/logs/`: runtime logs
- `/data/beam-app/Download/`: fetched WDC + align cache
- `/data/beam-app/data/`: generated outputs

## Daily Commands

Health check:

```bash
docker compose exec webapp bash scripts/check_health.sh
```

Tail logs:

```bash
docker compose logs -f
```

Restart stack:

```bash
docker compose restart
```

## Release Upgrade Routine

```bash
git pull
docker compose up -d --build
docker compose exec webapp bash scripts/check_health.sh
```

## Database Quick Inspection

```bash
docker compose exec -T webapp python - <<'PY'
import sqlite3
conn = sqlite3.connect("jobs.db")
for row in conn.execute("select status,count(*) from jobs group by status"):
    print(row)
PY
```

## Cleanup Policy

- Keep reference builds (important experiments).
- Remove obsolete `error/cancelled` jobs from UI.
- Periodically archive old `data/` builds offline.

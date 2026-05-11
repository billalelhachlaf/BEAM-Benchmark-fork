# Admin Operations Runbook

## Runtime State Locations

- `docker-data/jobs.db`: job queue + statuses + events
- `docker-data/logs/`: runtime logs
- `docker-data/Download/`: fetched WDC + align cache
- `docker-data/data/`: generated outputs

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

## Post-deploy Functional Check (Tutorial-Centered)

After each deploy, validate user guidance path:
1. Open `/` and confirm dashboard renders.
2. Open `/tutorial` and confirm content loads (not fallback error).
3. Confirm tutorial links/anchors are visible.
4. Launch one small job and verify status changes `queued -> running`.

Quick HTTP probe from host:

```bash
curl -I http://127.0.0.1:8501/tutorial
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

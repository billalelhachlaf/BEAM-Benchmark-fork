# Troubleshooting Guide

## Webapp not reachable

Checks:

```bash
docker compose ps
docker compose exec webapp bash scripts/check_health.sh
docker compose logs --tail=200 webapp
```

Common causes:
- container not started
- host port `80` already in use
- firewall/proxy restrictions

## Worker not consuming jobs

Checks:

```bash
docker compose logs --tail=300 worker
docker compose exec -T webapp python - <<'PY'
import sqlite3
conn = sqlite3.connect("jobs.db")
for row in conn.execute("select status,count(*) from jobs group by status"):
    print(row)
PY
```

Actions:
- `docker compose restart`
- inspect latest job errors
- verify runtime permissions for `/data/beam-app/`

## Jobs fail with 0 links

Likely causes:
- wrong predicate mapping
- too strict property/class filters
- sparse source values

Actions:
- test with smaller parts
- try alternative matching recipe
- rerun align without cache

## Slow / unstable runs

Actions:
- reduce `parts_spec`
- decrease concurrency if memory pressure
- increase Wikidata timeout/retries

## SAKEY errors

Checks:
- `SAKEY` runner availability
- resource pressure (RAM/CPU)
- run artifacts and logs under data runtime paths

## Tutorial page unavailable or incomplete

Symptoms:
- `/tutorial` returns non-200
- Tutorial page shows source-not-found fallback
- Tutorial opens but content is empty

Checks:

```bash
docker compose ps
docker compose exec webapp bash scripts/check_health.sh
docker compose logs --tail=200 webapp
```

Likely causes:
- webapp container restarted with stale/missing docs mount
- docs path mismatch for tutorial source file
- image built from outdated branch/revision

Actions:
- verify `docs/user/tutorial.md` exists in deployed workspace/image
- redeploy current branch: `docker compose up -d --build`
- recheck `/tutorial` and dashboard tutorial link

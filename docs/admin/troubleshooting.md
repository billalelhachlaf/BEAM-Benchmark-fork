# Troubleshooting Guide

## Webapp not reachable

Checks:

```bash
bash scripts/check_health.sh
ss -ltnp | rg ':8501'
tail -n 200 logs/webapp.log
```

Common causes:
- process not started
- wrong bind host/port
- firewall/proxy restrictions

## Worker not consuming jobs

Checks:

```bash
tail -n 300 logs/worker.log
sqlite3 jobs.db "select status,count(*) from jobs group by status;"
```

Actions:
- restart services
- inspect latest job errors
- verify runtime permissions for `data/` and `Download/`

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

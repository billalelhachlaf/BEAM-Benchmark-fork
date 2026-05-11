# Deployment On A New VM

Docker is the only supported runtime for deployment.

## 1) Clone

```bash
git clone <repo-url> BEAM-App
cd BEAM-App
```

## 2) Bootstrap (one command)

```bash
docker compose up -d --build
```

This creates the `webapp` and `worker` containers and stores runtime state in
`/data/beam-app/` by default.

UI:
- local: `http://127.0.0.1`
- remote: `http://<vm-ip>`

Main UI routes:
- `/app/create`
- `/app/jobs`
- `/app/history`
- `/tutorial`

## 3) Health check

```bash
docker compose exec webapp bash scripts/check_health.sh
```

## 4) Stop services

```bash
docker compose down
```

## 5) Reset to fresh runtime instance

```bash
docker compose down
mkdir -p /data/beam-app/db_backups
cp /data/beam-app/jobs.db /data/beam-app/db_backups/jobs_$(date +%Y%m%d_%H%M%S).db
rm -f /data/beam-app/jobs.db /data/beam-app/jobs.db-shm /data/beam-app/jobs.db-wal
docker compose up -d
```

What it does:
- backs up and recreates the job database
- keep presets (presets are code-defined)
- keeps `/data/beam-app/Download/` and `/data/beam-app/data/`

## 6) Troubleshooting

- Port busy:
```bash
sudo lsof -i :80
docker compose down
```

- Web not reachable:
```bash
docker compose logs --tail=100 webapp
```

- Worker not progressing jobs:
```bash
docker compose logs --tail=100 worker
```

- DB reset needed:
```bash
docker compose down
rm -f /data/beam-app/jobs.db /data/beam-app/jobs.db-shm /data/beam-app/jobs.db-wal
docker compose up -d
```

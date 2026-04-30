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
`docker-data/`.

UI:
- local: `http://127.0.0.1`
- remote: `http://<vm-ip>`

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
mkdir -p docker-data/db_backups
cp docker-data/jobs.db docker-data/db_backups/jobs_$(date +%Y%m%d_%H%M%S).db
rm -f docker-data/jobs.db docker-data/jobs.db-shm docker-data/jobs.db-wal
docker compose up -d
```

What it does:
- backs up and recreates the job database
- keep presets (presets are code-defined)
- keeps `docker-data/Download/` and `docker-data/data/`

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
rm -f docker-data/jobs.db docker-data/jobs.db-shm docker-data/jobs.db-wal
docker compose up -d
```

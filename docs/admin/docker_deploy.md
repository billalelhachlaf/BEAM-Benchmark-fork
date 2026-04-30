# Docker Deployment

Docker Compose is the only supported runtime path for BEAM-App deployment.

## Runtime State

Docker stores runtime data on the host in:

```text
docker-data/
  jobs.db
  Download/
  data/
  logs/
  run/
  reports/
```

The container sees these paths through symlinks at the existing application
locations:

- `jobs.db`
- `Download/`
- `data/`
- `logs/`
- `.run/`
- `reports/`

Keeping one host directory mounted at `/state` avoids losing generated builds,
WDC parts, align caches, logs, and job history when containers are recreated.

## Start

```bash
docker compose up -d --build
```

Open:

- `http://localhost`
- `http://<server-ip>`

The web application is exposed on host port `80`. The internal container port is
an implementation detail.

## Configure

You can override runtime settings with shell variables or a local `.env` file.
Common values:

```bash
MAX_CONCURRENT_JOBS=2
JOB_POLL_INTERVAL=1
MAX_WORKERS_PER_JOB=8
ALIGN_MAX_WORKERS=8
WIKIDATA_QUERY_TIMEOUT=300
SAKEY_MAX_CONCURRENT=1
```

Then restart:

```bash
docker compose up -d
```

## Operations

Show status:

```bash
docker compose ps
```

Follow logs:

```bash
docker compose logs -f
```

Restart:

```bash
docker compose restart
```

Stop without deleting runtime data:

```bash
docker compose down
```

Run the app health check from inside the web container:

```bash
docker compose exec webapp bash scripts/check_health.sh
```

## Upgrade

```bash
git pull
docker compose up -d --build
```

Do not delete `docker-data/` unless you intentionally want to remove local jobs,
builds, downloads, caches, and logs.

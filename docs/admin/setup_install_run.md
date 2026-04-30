# Admin Tutorial: Docker Setup, Installation, Run

## 1. Prerequisites

- Linux host (Ubuntu/Debian recommended)
- Docker Engine with Docker Compose
- `git`
- Outbound network access for Wikidata/WDC retrieval

## 2. Install

```bash
git clone <repo-url>
cd BEAM-App
```

## 3. Configure Environment

Optional values can be placed in `.env` or exported in the shell before
starting Docker:

```bash
MAX_CONCURRENT_JOBS=2
JOB_POLL_INTERVAL=1
ALIGN_MAX_WORKERS=8
WIKIDATA_QUERY_TIMEOUT=300
BEAM_STATE_DIR=/data/beam-app
```

## 4. Start

```bash
docker compose up -d --build
```

Then check health:

```bash
docker compose exec webapp bash scripts/check_health.sh
```

Open:
- `http://127.0.0.1`
- `http://<server-ip>`

## 5. Stop / Restart

```bash
docker compose down
docker compose restart
```

## 6. Smoke Test Procedure

1. Open dashboard.
2. Launch a small test-class run.
3. Confirm state transitions: `queued -> running -> done`.
4. Open build detail page.
5. Download generated build.

## 7. Operational Security Basics

- Do not expose `jobs.db` publicly.
- Keep `Download/` and `data/` private.
- Keep `/data/beam-app/` private and backed up.
- Restrict SSH and firewall access.

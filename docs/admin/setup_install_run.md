# Admin Tutorial: Setup, Installation, Run

## 1. Prerequisites

- Linux host (Ubuntu/Debian recommended)
- Python 3.8+
- `git`, `bash`, `curl`
- Outbound network access for Wikidata/WDC retrieval

## 2. Install

```bash
git clone <repo-url>
cd BEAM-App
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Optional dev/test dependencies:

```bash
pip install -r requirements-dev.txt
```

## 3. Configure Environment (Optional)

Example:

```bash
export MAX_CONCURRENT_JOBS=4
export JOB_POLL_INTERVAL=1
export ALIGN_MAX_WORKERS=8
export WIKIDATA_QUERY_TIMEOUT=300
```

## 4. Start Services

```bash
bash scripts/run_server.sh
```

Then check health:

```bash
bash scripts/check_health.sh
```

Open:
- `http://127.0.0.1:8501` (local)
- `http://<server-ip>:8501` (remote)

## 5. Stop / Restart

```bash
bash scripts/stop_server.sh
bash scripts/restart_server.sh
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
- Put app behind a reverse proxy for internet exposure.
- Restrict SSH and firewall access.

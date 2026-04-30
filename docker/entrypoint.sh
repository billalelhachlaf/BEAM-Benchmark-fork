#!/usr/bin/env sh
set -eu

mkdir -p /state/Download /state/data /state/logs /state/run /state/reports

python - <<'PY'
import fcntl
from pathlib import Path

lock_path = Path("/state/.init.lock")
lock_path.parent.mkdir(parents=True, exist_ok=True)

with lock_path.open("w") as lock:
    fcntl.flock(lock, fcntl.LOCK_EX)
    from beam.db import init_db

    init_db()
    fcntl.flock(lock, fcntl.LOCK_UN)
PY

exec "$@"

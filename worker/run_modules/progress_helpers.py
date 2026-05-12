import json
import multiprocessing as mp
import os
import re
import signal
import sys
import threading
import time
import statistics
import traceback
from collections import deque
from pathlib import Path
from contextlib import redirect_stdout, redirect_stderr

from beam import db
from beam.pipeline import generate_benchmark, PipelineError, is_align_cache_reusable

_CPU_COUNT = max(1, os.cpu_count() or 1)
# Hard cap at 2 concurrent jobs to keep queue behavior predictable.
# You can still lower it via env (e.g. MAX_CONCURRENT_JOBS=1).
MAX_CONCURRENT_JOBS = max(1, min(2, int(os.environ.get("MAX_CONCURRENT_JOBS", "2"))))
POLL_INTERVAL = float(os.environ.get("JOB_POLL_INTERVAL", "1"))
MAX_WORKERS_PER_JOB = int(os.environ.get("MAX_WORKERS_PER_JOB", str(_CPU_COUNT)))
JOB_WORKER_CPU_SHARE = float(os.environ.get("JOB_WORKER_CPU_SHARE", "0.95"))
JOB_STUCK_TIMEOUT_S = int(os.environ.get("JOB_STUCK_TIMEOUT_S", os.environ.get("JOB_STUCK_TIMEOUT", "1800")))


def _normalize_eta_hint(value):
    raw = str(value or "").strip()
    if not raw:
        return None
    compact = re.sub(r"\s+", "", raw.lower())
    if compact in {"n/a", "na", "-", "—"}:
        return None

    digits = re.findall(r"\d", compact)
    # Treat purely-zero ETA hints (0s, 0m0s, 0h00m00s, 00:00, etc.) as unknown.
    if digits and all(d == "0" for d in digits):
        tail = re.sub(r"[0-9:\.]", "", compact)
        if not tail or re.fullmatch(r"[hms]+", tail):
            return None
    return raw


def _format_eta_seconds(seconds):
    try:
        total = int(float(seconds))
    except Exception:
        return None
    if total <= 0:
        return None
    m, s = divmod(total, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h{m:02d}m{s:02d}s"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


def _extract_progress_pct(msg):
    if not msg:
        return None
    match = re.search(r"(?<!\d)(\d{1,3}(?:\.\d+)?)\s*%", str(msg))
    if not match:
        return None
    try:
        pct = float(match.group(1))
    except Exception:
        return None
    if pct < 0:
        return 0.0
    if pct > 100:
        return 100.0
    return pct


def _extract_batch_progress(msg):
    if not msg:
        return None, None
    match = re.search(r"\bbatches\s+(\d+)\s*/\s*(\d+)\b", str(msg), flags=re.IGNORECASE)
    if not match:
        return None, None
    try:
        done = int(match.group(1))
        total = int(match.group(2))
    except Exception:
        return None, None
    if total <= 0:
        return None, None
    done = max(0, min(done, total))
    return done, total


def _parse_eta_seconds(value):
    raw = str(value or "").strip().lower()
    if not raw:
        return None
    compact = re.sub(r"\s+", "", raw)
    if compact in {"n/a", "na", "-", "—"}:
        return None
    if re.fullmatch(r"\d+:\d{2}(?::\d{2})?", compact):
        parts = [int(x) for x in compact.split(":")]
        if len(parts) == 2:
            m, s = parts
            total = (m * 60) + s
        else:
            h, m, s = parts
            total = (h * 3600) + (m * 60) + s
        return total if total > 0 else None
    chunks = re.findall(r"(\d+)\s*([hms])", compact)
    if not chunks:
        return None
    total = 0
    for num, unit in chunks:
        n = int(num)
        if unit == "h":
            total += n * 3600
        elif unit == "m":
            total += n * 60
        elif unit == "s":
            total += n
    return total if total > 0 else None


def _cfg_eta_fingerprint(params):
    data = params if isinstance(params, dict) else {}
    mode = str(data.get("matching_mode") or "").strip().lower()
    if mode not in {"property", "sameas", "sameas_or_property"}:
        mode = "sameas" if bool(data.get("wdc_value_is_wikidata")) else "property"
    def _txt(k):
        return str(data.get(k) or "").strip()
    def _b(k):
        return bool(data.get(k))
    strict_duplicate_key_filter = bool(data.get("strict_duplicate_key_filter"))
    return (
        mode,
        _txt("class_name"),
        _txt("parts_spec"),
        _txt("wdc_predicate_pattern"),
        _txt("wikidata_property"),
        _txt("wkd_class"),
        _txt("ignore_chars"),
        _b("use_local_only"),
        strict_duplicate_key_filter,
    )


def _estimate_eta_baselines(params, limit=250):
    """Return median historical durations (seconds) for comparable runs."""
    rows = db.list_jobs(limit=max(20, int(limit)))
    target_fp = _cfg_eta_fingerprint(params)
    target_class = target_fp[0]
    total_same = []
    build_same = []
    total_class = []
    build_class = []
    for row in rows:
        if str(row["status"] or "") != "done":
            continue
        try:
            started = float(row["started_at"] or 0.0)
            ended = float(row["ended_at"] or 0.0)
        except Exception:
            continue
        if started <= 0 or ended <= started:
            continue
        total_dur = ended - started
        if total_dur <= 0 or total_dur > 48 * 3600:
            continue
        try:
            cfg = json.loads(row["params_json"] or "{}")
            if not isinstance(cfg, dict):
                cfg = {}
        except Exception:
            cfg = {}
        fp = _cfg_eta_fingerprint(cfg)
        if not fp[0]:
            continue
        build_dur = None
        try:
            sj = db.get_subjob(int(row["id"]), "build")
            if sj:
                bs = float(sj["started_at"] or 0.0)
                be = float(sj["ended_at"] or 0.0)
                if bs > 0 and be > bs:
                    d = be - bs
                    if 0 < d <= 48 * 3600:
                        build_dur = d
        except Exception:
            build_dur = None
        if fp[0] == target_class:
            total_class.append(total_dur)
            if build_dur is not None:
                build_class.append(build_dur)
        if fp == target_fp:
            total_same.append(total_dur)
            if build_dur is not None:
                build_same.append(build_dur)
    total_pool = total_same or total_class
    build_pool = build_same or build_class
    out = {}
    if total_pool:
        out["total_s"] = max(1, int(statistics.median(total_pool)))
    if build_pool:
        out["build_s"] = max(1, int(statistics.median(build_pool)))
    return out


def _should_mark_job_stuck(last_activity_ts, now_ts, timeout_s):
    if timeout_s is None:
        return False
    try:
        timeout_s = int(timeout_s)
    except Exception:
        return False
    if timeout_s <= 0:
        return False
    try:
        last_ts = float(last_activity_ts or 0.0)
        now_ts = float(now_ts or 0.0)
    except Exception:
        return False
    if last_ts <= 0 or now_ts <= 0:
        return False
    return (now_ts - last_ts) >= timeout_s


def _cpu_workers_for(job_count):
    share = JOB_WORKER_CPU_SHARE
    if share <= 0 or share > 1.0:
        share = 0.95
    active = max(1, job_count)
    workers = max(1, int((_CPU_COUNT * share) / active))
    return max(1, min(workers, MAX_WORKERS_PER_JOB, _CPU_COUNT))


def _terminate_process_tree(proc, grace_s=0.5):
    """Best-effort termination for a worker process and its process group."""
    if not proc:
        return True
    if not proc.is_alive():
        return True

    # Try graceful stop on the full process group first.
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except Exception:
        try:
            proc.terminate()
        except Exception:
            pass

    deadline = time.time() + max(0.1, grace_s)
    while proc.is_alive() and time.time() < deadline:
        time.sleep(0.05)

    if not proc.is_alive():
        return True

    # Escalate to SIGKILL if still running.
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except Exception:
        try:
            os.kill(proc.pid, signal.SIGKILL)
        except Exception:
            pass

    proc.join(timeout=0.2)
    return not proc.is_alive()


def _pid_alive(pid):
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
        return True
    except Exception:
        return False


def _terminate_by_ids(pid=None, pgid=None, grace_s=0.5):
    if not pid and not pgid:
        return True
    try:
        if pgid:
            os.killpg(int(pgid), signal.SIGTERM)
        elif pid:
            os.kill(int(pid), signal.SIGTERM)
    except Exception:
        pass
    deadline = time.time() + max(0.1, grace_s)
    while time.time() < deadline:
        if not _pid_alive(pid):
            return True
        time.sleep(0.05)
    try:
        if pgid:
            os.killpg(int(pgid), signal.SIGKILL)
        elif pid:
            os.kill(int(pid), signal.SIGKILL)
    except Exception:
        pass
    time.sleep(0.1)
    return not _pid_alive(pid)


def _cancel_if_active(job_id, subjob_type):
    row = db.get_subjob(job_id, subjob_type)
    if not row:
        return
    if row["status"] in ("done", "error", "cancelled", "interrupted"):
        return
    db.update_subjob(row["id"], status="cancelled", ended_at=time.time())


def _error_if_active(job_id, subjob_type, reason, ended_at=None):
    row = db.get_subjob(job_id, subjob_type)
    if not row:
        return
    if row["status"] in ("done", "error", "cancelled", "interrupted"):
        return
    db.update_subjob(
        row["id"],
        status="error",
        ended_at=ended_at or time.time(),
        progress_text=str(reason or "Job marked as error"),
    )


def _align_cache_ready(params):
    try:
        return bool(is_align_cache_reusable(params))
    except Exception:
        return False


def _safe_json_dumps(value):
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except Exception:
        return json.dumps({})


def _checkpoint_for_job(job):
    try:
        raw = job["checkpoint_json"] if job else None
        if not raw:
            return {}
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _recover_stale_running_jobs():
    """Recover jobs left in running state after worker/server restarts."""
    now = time.time()
    stale = db.list_jobs_by_status("running")
    for job in stale:
        job_id = job["id"]
        pid = job["job_pid"]
        pgid = job["job_pgid"]
        alive = _pid_alive(pid)

        if alive and job["cancel_requested"]:
            _terminate_by_ids(pid=pid, pgid=pgid, grace_s=0.5)
            alive = _pid_alive(pid)

        if alive:
            # Process is still alive but orphaned from this worker loop; stop it for deterministic recovery.
            _terminate_by_ids(pid=pid, pgid=pgid, grace_s=0.5)

        if job["cancel_requested"]:
            db.update_job(
                job_id,
                status="cancelled",
                ended_at=now,
                error_message="Cancelled (recovered after restart)",
                checkpoint_json=None,
                checkpoint_at=None,
            )
            _cancel_if_active(job_id, "align")
            _cancel_if_active(job_id, "build")
            db.insert_event(job_id, "system", "Recovered stale running state after restart (cancelled)")
        else:
            # Auto-restart non-cancelled jobs.
            try:
                params = json.loads(job["params_json"] or "{}")
            except Exception:
                params = {}
            ckpt = _checkpoint_for_job(job)
            job_result_path = ""
            try:
                job_result_path = str(job["result_path"] or "").strip()
            except Exception:
                job_result_path = ""
            resume_out_dir = (
                str(ckpt.get("resume_out_dir") or "").strip()
                or job_result_path
                or str(params.get("resume_out_dir") or "").strip()
            )
            restart_build_only = (job["phase"] == "build") and _align_cache_ready(params)
            if restart_build_only:
                params["require_cached_align"] = True
                params["skip_build"] = False
                params["force_align"] = False
                if resume_out_dir:
                    params["resume_build"] = True
                    params["resume_out_dir"] = resume_out_dir
                db.update_subjob_by_type(job_id, "align", status="done", ended_at=now, cancel_requested=0)
                db.update_subjob_by_type(job_id, "build", status="queued", started_at=None, ended_at=None, cancel_requested=0)
            else:
                params.pop("resume_build", None)
                params.pop("resume_out_dir", None)
                db.update_subjob_by_type(job_id, "align", status="queued", started_at=None, ended_at=None, cancel_requested=0)
                db.update_subjob_by_type(job_id, "build", status="queued", started_at=None, ended_at=None, cancel_requested=0)
            db.update_job(
                job_id,
                status="queued",
                phase=None,
                cancel_requested=0,
                started_at=None,
                ended_at=None,
                interrupted=1,
                progress_text=None,
                progress_pct=None,
                current_step=None,
                current_file=None,
                job_pid=None,
                job_pgid=None,
                result_path=resume_out_dir if restart_build_only and resume_out_dir else None,
                error_message="Auto-requeued after restart",
                params_json=_safe_json_dumps(params),
                checkpoint_json=_safe_json_dumps(
                    {
                        "phase": "build" if restart_build_only else "align",
                        "step": "queued",
                        "resume_out_dir": resume_out_dir if restart_build_only and resume_out_dir else None,
                        "reason": "recovered_after_restart",
                        "ts": now,
                    }
                ) if restart_build_only else None,
                checkpoint_at=now if restart_build_only else None,
            )
            if restart_build_only:
                db.insert_event(job_id, "system", "Recovered stale running state after restart (auto-requeued: build)")
            else:
                db.insert_event(job_id, "system", "Recovered stale running state after restart (auto-requeued: full)")


def _reconcile_terminal_subjobs():
    """Ensure terminal job states are reflected on subjobs after restarts/code upgrades."""
    terminal_statuses = ("error", "cancelled", "interrupted")
    now = time.time()
    for status in terminal_statuses:
        rows = db.list_jobs_by_status(status)
        for job in rows:
            job_id = job["id"]
            for sj in db.list_subjobs(job_id):
                if sj["status"] in ("queued", "running"):
                    db.update_subjob(sj["id"], status=status, ended_at=now)


def _looks_like_skipped_build_reason(text):
    msg = str(text or "").strip().lower()
    if not msg:
        return False
    return ("build skipped" in msg) or ("no alignments found" in msg)


def _reconcile_skipped_build_jobs():
    """
    Normalize inconsistent skipped-build states:
    rows persisted as done while build was skipped due to 0 alignments
    are rewritten to error so UI state stays consistent.
    """
    now = time.time()
    rows = list(db.list_jobs_by_status("done")) + list(db.list_jobs_by_status("error"))
    for job in rows:
        job_id = job["id"]
        build_row = db.get_subjob(job_id, "build")
        if not build_row:
            continue

        build_step = str(build_row["current_step"] or "").strip().lower()
        build_msg = str(build_row["progress_text"] or "").strip()
        job_msg = str(job["progress_text"] or "").strip()
        err_msg = str(job["error_message"] or "").strip()

        skipped = (
            build_step == "skipped"
            or _looks_like_skipped_build_reason(build_msg)
            or _looks_like_skipped_build_reason(job_msg)
            or _looks_like_skipped_build_reason(err_msg)
        )
        if not skipped:
            continue

        # Never rewrite true completed builds.
        result_path = str(job["result_path"] or "").strip()
        has_build_done = bool(result_path and (Path(result_path) / "BUILD_DONE").exists())
        if has_build_done and job["status"] == "done":
            continue

        reason = build_msg or job_msg or err_msg or "No alignments found (0); build skipped."
        if job["status"] == "done":
            db.update_job(
                job_id,
                status="error",
                phase="build",
                ended_at=job["ended_at"] or now,
                result_path=None if not has_build_done else result_path,
                progress_text=reason,
                error_message=reason,
            )
        if build_row["status"] != "error":
            db.update_subjob_by_type(
                job_id,
                "build",
                status="error",
                ended_at=build_row["ended_at"] or now,
                progress_text=reason,
                current_step="skipped",
            )
            db.insert_event(
                job_id,
                "system",
                "Reconciled skipped build result to error state",
                phase="build",
                kind="reconcile",
                step="skipped",
                worker="build",
            )



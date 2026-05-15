def _enqueue_sakey_run(class_name: str, parts_spec: str, mins: int, timeout_hours: float):
    _sakey_reconcile_inflight_runs()
    duplicate = _sakey_find_active_duplicate(class_name, parts_spec, mins, timeout_hours)
    if duplicate:
        _sakey_log(duplicate, "Duplicate launch ignored (same class/parts/mins/timeout).")
        return duplicate

    run_id = f"sakey_{time.strftime('%Y%m%d_%H%M%S')}_{int(time.time() * 1000) % 1000:03d}"
    created_at = time.time()
    meta = {
        "run_id": run_id,
        "class_name": _clean_text(class_name),
        "parts_spec": _clean_text(parts_spec) or "all",
        "mins": int(max(1, mins)),
        "timeout_hours": float(max(0.1, timeout_hours)),
        "status": "queued",
        "created_at": created_at,
        "created_at_h": _fmt_ts(created_at),
        "started_at": None,
        "ended_at": None,
        "error": "",
        "keys_candidates_count": 0,
        "key_summary": {},
    }
    with _SAKEY_RUN_LOCK:
        _sakey_write_meta(run_id, meta)
    _sakey_log(run_id, "Queued")
    Thread(target=_run_sakey_worker, args=(run_id,), daemon=True).start()
    return run_id


def _sakey_resolve_artifact(run_id: str, name: str):
    d = _sakey_run_dir(run_id)
    if not d:
        return None
    safe = _clean_text(name)
    if not safe or "/" in safe or "\\" in safe or safe.startswith("."):
        return None
    p = (d / safe).resolve()
    try:
        p.relative_to(d.resolve())
    except Exception:
        return None
    if not p.exists() or not p.is_file():
        return None
    return p


def _sakey_tail_log(run_id: str, max_lines: int = 120):
    d = _sakey_run_dir(run_id)
    if not d:
        return []
    p = d / "run.log"
    if not p.exists() or not p.is_file():
        return []
    try:
        lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception:
        return []
    max_lines = max(10, min(int(max_lines or 120), 500))
    return lines[-max_lines:]


def _sakey_tail_file(path: Path, max_lines: int = 120):
    if not path or not path.exists() or not path.is_file():
        return []
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception:
        return []
    max_lines = max(10, min(int(max_lines or 120), 500))
    return lines[-max_lines:]


def _sakey_load_keys_candidates(run_id: str, limit: int = 500):
    d = _sakey_run_dir(run_id)
    if not d:
        return []
    report = d / "SAKEY_REPORT.json"
    if not report.exists() or not report.is_file():
        return []
    try:
        payload = json.loads(report.read_text(encoding="utf-8"))
    except Exception:
        return []
    rows = payload.get("keys_candidates")
    if not isinstance(rows, list):
        return []
    out = []
    for row in rows[: max(1, min(int(limit or 500), 2000))]:
        if not isinstance(row, dict):
            continue
        props = row.get("props")
        if not isinstance(props, list):
            props = []
            key_text = _clean_text(str(row.get("key", "")))
            if key_text and " + " in key_text:
                props = []
                for tok in key_text.split(" + "):
                    pn = _normalize_prop_iri(tok)
                    if pn:
                        props.append(pn)
        else:
            norm_props = []
            for tok in props:
                pn = _normalize_prop_iri(tok)
                if pn:
                    norm_props.append(pn)
            props = norm_props
        out.append(
            {
                "key": _clean_text(str(row.get("key", ""))),
                "condition": _clean_text(str(row.get("condition", ""))),
                "type": _clean_text(str(row.get("type", row.get("condition", "")))),
                "support": _clean_text(str(row.get("support", ""))),
                "support_num": _safe_int(row.get("support_num"), None),
                "coverage": _clean_text(str(row.get("coverage", ""))),
                "coverage_num": _safe_float(row.get("coverage_num"), None),
                "key_size": _safe_int(row.get("key_size"), len(props) if props else 0) or 0,
                "score": _clean_text(str(row.get("score", ""))),
                "props": props,
            }
        )
    return out


def _sakey_list_artifacts(run_id: str):
    d = _sakey_run_dir(run_id)
    if not d:
        return []
    out = []
    for name in ("run.log", "input.nt", "sakey.out", "input.tsv", "vickey.out", "SAKEY_REPORT.json", "SAKEY_KEYS.tsv"):
        p = d / name
        if not p.exists() or not p.is_file():
            continue
        try:
            st = p.stat()
            size_h = _fmt_size(int(st.st_size))
        except Exception:
            size_h = ""
        out.append(
            {
                "name": name,
                "size_h": size_h,
                "url": f"/sakey/runs/{quote(run_id, safe='')}/artifact/{quote(name, safe='')}",
            }
        )
    return out


def _sakey_page_payload(
    class_name: str = "",
    run_id: str = "",
    test_mode: bool = False,
    key_order_by: str = "",
    key_min_support: str = "",
    key_only_almost: Optional[str] = None,
    key_max_size: str = "",
    key_q: str = "",
):
    _sakey_reconcile_inflight_runs()
    key_filters = _parse_sakey_filter_params(
        order_by=key_order_by,
        min_support=key_min_support,
        only_almost=key_only_almost,
        max_key_size=key_max_size,
        q=key_q,
    )
    class_options = _sakey_collect_class_options(test_mode=bool(test_mode))
    selected_class = _clean_text(class_name)
    if selected_class and selected_class not in class_options:
        selected_class = ""

    runs = _sakey_list_runs(limit=80, class_name=selected_class or "")
    legacy_runs = _sakey_list_legacy_runs(limit=120)
    if selected_class:
        legacy_runs = [r for r in legacy_runs if _clean_text(str(r.get("class_name", ""))) == selected_class]
    runs.extend(legacy_runs)
    runs.sort(key=lambda r: float(r.get("created_at", 0.0) or 0.0), reverse=True)
    runs = runs[:50]
    active_states = {"queued", "waiting", "running"}
    active_jobs = []
    history_runs = []
    for r in runs:
        st = _clean_text(str(r.get("status", ""))).lower()
        if st in active_states:
            active_jobs.append(r)
        else:
            history_runs.append(r)
    selected_run = None
    wanted_run_id = _clean_text(run_id)
    if wanted_run_id:
        for r in runs:
            if _clean_text(str(r.get("run_id", ""))) == wanted_run_id:
                selected_run = r
                break
    if not selected_run and runs and selected_class:
        for r in runs:
            if _clean_text(str(r.get("class_name", ""))) == selected_class:
                selected_run = r
                break
    if not selected_run and runs and not selected_class:
        selected_run = runs[0]

    keys_candidates = []
    log_tail = []
    artifacts = []
    form_parts_spec = "all"
    form_mins = 3
    form_timeout_hours = 48.0
    if selected_run:
        sid = _clean_text(str(selected_run.get("run_id", "")))
        if not bool(selected_run.get("legacy")):
            form_parts_spec = _clean_text(str(selected_run.get("parts_spec", ""))) or "all"
            try:
                form_mins = max(1, int(selected_run.get("mins", 3) or 3))
            except Exception:
                form_mins = 3
            try:
                form_timeout_hours = max(0.1, float(selected_run.get("timeout_hours", 48.0) or 48.0))
            except Exception:
                form_timeout_hours = 48.0
        if bool(selected_run.get("legacy")):
            out_p = Path(str(selected_run.get("legacy_out_path", "")))
            _summary, keys_candidates = _sakey_parse_keys_from_output(out_p, limit=800)
            legacy_meta_path = out_p.with_suffix(".meta")
            if legacy_meta_path.exists() and keys_candidates:
                try:
                    meta_lines = legacy_meta_path.read_text(encoding="utf-8", errors="ignore").splitlines()
                    dataset_path = ""
                    for ln in meta_lines:
                        if ln.startswith("dataset="):
                            dataset_path = _clean_text(ln.split("=", 1)[1])
                            break
                    if dataset_path:
                        ds = (Path(__file__).resolve().parents[1] / "SAKEY" / dataset_path).resolve()
                        if ds.exists():
                            metrics = _sakey_compute_row_metrics(ds, keys_candidates)
                            ks = dict((selected_run or {}).get("key_summary") or {})
                            ks["subjects_count_sample"] = metrics.get("subjects")
                            ks["metrics_lines_scanned"] = metrics.get("lines")
                            ks["metrics_sampled"] = bool(metrics.get("sampled"))
                            if isinstance(selected_run, dict):
                                selected_run["key_summary"] = ks
                except Exception:
                    pass
            log_tail = _sakey_tail_file(out_p, max_lines=100)
            log_p = Path(str(selected_run.get("legacy_log_path", "")))
            if log_p.exists():
                log_tail = _sakey_tail_file(log_p, max_lines=100)
            artifacts = []
        else:
            keys_candidates = _sakey_load_keys_candidates(sid, limit=800)
            needs_metrics = bool(keys_candidates) and all(row.get("support_num") is None for row in keys_candidates)
            if needs_metrics:
                nt_path = Path(str((selected_run or {}).get("nt_path", "")))
                if nt_path.exists():
                    metrics = _sakey_compute_row_metrics(nt_path, keys_candidates)
                    key_summary = dict((selected_run or {}).get("key_summary") or {})
                    key_summary["subjects_count_sample"] = metrics.get("subjects")
                    key_summary["metrics_lines_scanned"] = metrics.get("lines")
                    key_summary["metrics_sampled"] = bool(metrics.get("sampled"))
                    rep_json, rep_tsv = _sakey_write_report_files(sid, key_summary, keys_candidates)
                    _sakey_update_meta(
                        sid,
                        key_summary=key_summary,
                        keys_candidates_count=len(keys_candidates),
                        report_json=str(rep_json) if rep_json else "",
                        report_tsv=str(rep_tsv) if rep_tsv else "",
                    )
                    if isinstance(selected_run, dict):
                        selected_run["key_summary"] = key_summary
            log_tail = _sakey_tail_log(sid, max_lines=100)
            artifacts = _sakey_list_artifacts(sid)

    keys_candidates = _sakey_apply_filters_and_sort(keys_candidates, key_filters)

    return {
        "class_options": class_options,
        "selected_class": selected_class,
        "sakey_max_concurrent": _SAKEY_MAX_CONCURRENT,
        "runs": runs,
        "active_jobs": active_jobs,
        "history_runs": history_runs,
        "selected_run": selected_run,
        "selected_run_id": _clean_text(str((selected_run or {}).get("run_id", ""))),
        "form_parts_spec": form_parts_spec,
        "form_mins": form_mins,
        "form_timeout_hours": form_timeout_hours,
        "key_filters": key_filters,
        "keys_candidates": keys_candidates,
        "log_tail": log_tail,
        "artifacts": artifacts,
    }



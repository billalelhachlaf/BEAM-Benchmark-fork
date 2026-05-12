

def _build_result_path_aliases(build_dir: Path):
    aliases = set()
    try:
        resolved = build_dir.resolve()
    except Exception:
        resolved = build_dir

    for candidate in (build_dir, resolved):
        txt = _clean_text(str(candidate))
        if not txt:
            continue
        aliases.add(txt)
        aliases.add(os.path.normpath(txt))

    try:
        cwd_resolved = Path.cwd().resolve()
        rel = resolved.relative_to(cwd_resolved)
        rel_txt = str(rel)
        aliases.add(rel_txt)
        aliases.add(os.path.normpath(rel_txt))
        aliases.add(f"./{rel_txt}")
    except Exception:
        pass

    normalized = {_clean_text(a.rstrip("/\\")) for a in aliases if _clean_text(a)}
    return {a for a in normalized if a}


def _delete_jobs_for_build_dir(build_dir: Path, scan_limit: int = 50000) -> int:
    aliases = _build_result_path_aliases(build_dir)
    target_norm = _normalized_path_text(str(build_dir))
    to_delete_ids = set()

    # Delete exact-path variants without relying on recency limits.
    for alias in aliases:
        try:
            db.delete_jobs_by_result_path(alias)
        except Exception:
            continue

    # Fallback for unusual historical path spellings that still point to the same directory.
    for row in db.list_jobs(limit=scan_limit):
        try:
            rp = _clean_text(row["result_path"])
        except Exception:
            rp = ""
        if not rp:
            continue
        if rp in aliases or os.path.normpath(rp) in aliases or _normalized_path_text(rp) == target_norm:
            try:
                to_delete_ids.add(int(row["id"]))
            except Exception:
                continue

    for jid in to_delete_ids:
        try:
            db.delete_job(jid)
        except Exception:
            continue
    return len(to_delete_ids)


def _bool_from_any(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "on"}


def _find_job_params_by_result_path(result_path: str, limit: int = 4000):
    target = str(result_path or "").strip()
    if not target:
        return None
    for row in db.list_jobs(limit=limit):
        rp = str(row["result_path"] or "").strip()
        if rp != target:
            continue
        params = _safe_json_loads(row["params_json"])
        if isinstance(params, dict) and params:
            return params
    return None


def _rerun_params_from_build_config(build_dir: Path, class_name: str):
    cfg_path = build_dir / "BUILD_CONFIG.json"
    cfg = {}
    if cfg_path.exists() and cfg_path.is_file():
        cfg = _safe_json_loads(cfg_path.read_text(encoding="utf-8"))
    cfg = cfg if isinstance(cfg, dict) else {}
    fallback = _find_job_params_by_result_path(str(build_dir))
    fallback = fallback if isinstance(fallback, dict) else {}

    def _pick(key, default=""):
        v = cfg.get(key, None)
        if v is None and fallback:
            v = fallback.get(key, None)
        if v is None:
            v = default
        return v

    raw_params = {
        "matching_mode": _normalize_matching_mode(
            _clean_text(str(_pick("matching_mode", ""))),
            fallback_wdc_value_is_wikidata=_bool_from_any(_pick("wdc_value_is_wikidata", False)),
        ),
        "class_name": _clean_text(str(_pick("class_name", class_name))),
        "parts_spec": _clean_text(str(_pick("parts_spec", "all"))),
        "wdc_predicate_pattern": _clean_text(str(_pick("wdc_predicate_pattern", ""))),
        "wdc_pattern_search_in": _clean_text(str(_pick("wdc_pattern_search_in", "predicate"))),
        "target_endpoint": _clean_text(str(_pick("target_endpoint", "wikidata"))),
        "target_endpoint_url": _clean_text(str(_pick("target_endpoint_url", ""))),
        "target_prefixes": _clean_text(str(_pick("target_prefixes", ""))),
        "property_mapping_rules": _clean_text(str(_pick("property_mapping_rules", ""))),
        "target_property": _clean_text(str(_pick("target_property", _pick("wikidata_property", "")))),
        "target_class": _clean_text(str(_pick("target_class", _pick("wkd_class", "")))),
        "wikidata_property": _clean_text(str(_pick("wikidata_property", ""))),
        "wkd_class": _clean_text(str(_pick("wkd_class", ""))),
        "ignore_chars": _clean_text(str(_pick("ignore_chars", "spaces;-;."))),
        "force_align": _bool_from_any(_pick("force_align", False)),
        "use_local_only": _bool_from_any(_pick("use_local_only", False)),
        "strict_duplicate_key_filter": True,
    }
    return _validate_and_normalize_job_params(raw_params)


def _job_outputs(job):
    out = {"build_done": False, "build_out_with": None, "build_out_without": None, "build_done_file": None}
    result_path = job["result_path"]
    if result_path:
        base = Path(result_path)
        out["build_done_file"] = str(base / "BUILD_DONE")
        if (base / "BUILD_DONE").exists():
            out["build_done"] = True
        if (base / "with_link_code").exists():
            out["build_out_with"] = str(base / "with_link_code")
        if (base / "without_link_code").exists():
            out["build_out_without"] = str(base / "without_link_code")
    return out


def _safe_json_loads(raw: Optional[str]):
    try:
        return json.loads(raw or "{}")
    except Exception:
        return {}


def _looks_like_skipped_build_reason(text: Optional[str]) -> bool:
    msg = str(text or "").strip().lower()
    if not msg:
        return False
    return ("build skipped" in msg) or ("no alignments found" in msg)


def _build_dashboard_state(job_limit: int = 50, build_limit: int = 200, test_mode: Optional[bool] = None):
    all_jobs = [dict(j) for j in db.list_jobs(limit=job_limit)]
    jobs_by_id = {j["id"]: j for j in all_jobs}
    # Always include truly active jobs even if they are outside the recency window.
    for st in ("running", "queued"):
        for row in db.list_jobs_by_status(st):
            jid = row["id"]
            if jid not in jobs_by_id:
                jobs_by_id[jid] = dict(row)
    all_jobs = sorted(jobs_by_id.values(), key=lambda r: int(r.get("id") or 0), reverse=True)
    all_jobs_params = {j["id"]: _safe_json_loads(j.get("params_json")) for j in all_jobs}
    if test_mode is not None:
        desired = bool(test_mode)
        all_jobs = [
            j for j in all_jobs
            if _is_test_class_name(all_jobs_params.get(j["id"], {}).get("class_name")) == desired
        ]
    active_jobs = [j for j in all_jobs if j["status"] in {"running", "queued"}]
    builds = _scan_builds(limit=build_limit)
    if test_mode is not None:
        desired = bool(test_mode)
        builds = [b for b in builds if _is_test_class_name(b.get("class_name")) == desired]

    build_params = {}
    for j in all_jobs:
        rp = j.get("result_path")
        if not rp or rp in build_params:
            continue
        params = _safe_json_loads(j.get("params_json"))
        if params:
            build_params[rp] = params

    for b in builds:
        params = b.get("build_config") or build_params.get(b["path"])
        if params:
            b["config"] = params
        else:
            b["config"] = {
                "class_name": b["class_name"],
                "build_name": b["build_name"],
                "result_path": b["path"],
                "config_source": "inferred",
            }
        parts = b["config"].get("parts_manifest")
        if not isinstance(parts, list):
            parts = []
        b["parts_manifest"] = parts
        b["parts_count"] = b["config"].get("parts_count", len(parts))
        b["parts_total_size_human"] = b["config"].get("parts_total_size_human")
        b["config_groups"] = _build_config_groups(b["config"])

    jobs_outputs = {}
    jobs_times = {}
    jobs_params = {}
    jobs_subjobs = {}
    for j in all_jobs:
        jid = j["id"]
        jobs_outputs[jid] = _job_outputs(j)
        jobs_times[jid] = {
            "created": _fmt_ts(j.get("created_at")),
            "started": _fmt_ts(j.get("started_at")),
            "ended": _fmt_ts(j.get("ended_at")),
        }
        jobs_params[jid] = all_jobs_params.get(jid, {})
        jobs_subjobs[jid] = [dict(s) for s in db.list_subjobs(jid)]

    # Safety: normalize inconsistent rows persisted as "done" when build was skipped.
    for j in all_jobs:
        if j.get("status") != "done":
            continue
        jid = j["id"]
        if jobs_outputs.get(jid, {}).get("build_done"):
            continue
        build_row = next((s for s in jobs_subjobs.get(jid, []) if s.get("type") == "build"), None)
        build_step = str((build_row or {}).get("current_step") or "").strip().lower()
        build_msg = str((build_row or {}).get("progress_text") or "").strip()
        job_msg = str(j.get("progress_text") or "").strip()
        err_msg = str(j.get("error_message") or "").strip()
        skipped = (
            build_step == "skipped"
            or _looks_like_skipped_build_reason(build_msg)
            or _looks_like_skipped_build_reason(job_msg)
            or _looks_like_skipped_build_reason(err_msg)
        )
        if not skipped:
            continue
        reason = build_msg or job_msg or err_msg or "No alignments found (0); build skipped."
        j["status"] = "error"
        j["phase"] = j.get("phase") or "build"
        j["error_message"] = reason

    # Keep done jobs visible when there is no downloadable build output,
    # except dangling rows where result_path points to a deleted/non-existent build dir.
    jobs_for_panel = []
    for j in all_jobs:
        if j["status"] != "done":
            jobs_for_panel.append(j)
            continue
        out = jobs_outputs.get(j["id"], {})
        if out.get("build_done"):
            continue
        result_path = _clean_text(j.get("result_path"))
        if result_path:
            try:
                if not Path(result_path).exists():
                    continue
            except Exception:
                pass
        jobs_for_panel.append(j)

    return {
        "all_jobs": all_jobs,
        "active_jobs": active_jobs,
        "jobs_for_panel": jobs_for_panel,
        "builds": builds,
        "jobs_outputs": jobs_outputs,
        "jobs_times": jobs_times,
        "jobs_params": jobs_params,
        "jobs_subjobs": jobs_subjobs,
    }



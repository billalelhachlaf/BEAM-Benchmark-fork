@app.get("/api/class_parts/{class_name}")
def class_parts_api(class_name: str):
    return _build_class_parts_info(class_name)


@app.get("/api/preflight")
def preflight_api(
    class_name: str,
    parts_spec: str = "all",
    matching_mode: str = "property",
    wdc_predicate_pattern: str = "",
    wdc_pattern_search_in: str = "predicate",
    target_endpoint: str = "wikidata",
    target_endpoint_url: str = "",
    target_prefixes: str = "",
    property_mapping_rules: str = "",
    target_property: str = "",
    target_class: str = "",
    wikidata_property: str = "",
    wkd_class: str = "",
    ignore_chars: str = "",
    use_local_only: bool = False,
    include_wikidata_preview: bool = True,
    scan_limit_lines: int = 30000,
):
    return _build_preflight_report(
        class_name=class_name,
        parts_spec=parts_spec,
        matching_mode=matching_mode,
        wdc_predicate_pattern=wdc_predicate_pattern,
        wdc_pattern_search_in=wdc_pattern_search_in,
        target_endpoint=target_endpoint,
        target_endpoint_url=target_endpoint_url,
        target_prefixes=target_prefixes,
        property_mapping_rules=property_mapping_rules,
        target_property=target_property,
        target_class=target_class,
        wikidata_property=wikidata_property,
        wkd_class=wkd_class,
        ignore_chars=ignore_chars,
        use_local_only=bool(use_local_only),
        include_wikidata_preview=bool(include_wikidata_preview),
        scan_limit_lines=int(scan_limit_lines),
    )


@app.post("/jobs/{job_id}/cancel")
def cancel_job(job_id: int):
    job = db.get_job(job_id)
    if not job:
        return RedirectResponse(url="/", status_code=303)
    if job["status"] not in {"running", "queued"}:
        return RedirectResponse(url="/", status_code=303)
    db.request_cancel(job_id)
    db.request_cancel_subjob(job_id, "align")
    db.request_cancel_subjob(job_id, "build")
    if job["status"] == "queued":
        db.update_subjob_by_type(job_id, "align", status="cancelled", ended_at=time.time())
        db.update_subjob_by_type(job_id, "build", status="cancelled", ended_at=time.time())
    db.insert_event(job_id, "system", "Cancel requested (job)")
    return RedirectResponse(url="/", status_code=303)


@app.post("/jobs/{job_id}/cancel_subjob/{subjob_type}")
def cancel_subjob(job_id: int, subjob_type: str):
    if subjob_type not in {"align", "build"}:
        return RedirectResponse(url="/", status_code=303)
    job = db.get_job(job_id)
    if not job:
        return RedirectResponse(url="/", status_code=303)
    if job["status"] not in {"running", "queued"}:
        return RedirectResponse(url="/", status_code=303)
    sj = db.get_subjob(job_id, subjob_type)
    if not sj or sj["status"] not in {"running", "queued"}:
        return RedirectResponse(url="/", status_code=303)

    db.request_cancel_subjob(job_id, subjob_type)
    if subjob_type == "align":
        # Align cancel implies full job cancel and build cancel.
        db.request_cancel(job_id)
        db.request_cancel_subjob(job_id, "build")
        db.insert_event(job_id, "system", "Cancel requested (align; build will be cancelled too)")
    else:
        # Build cancel does not interrupt align. If already in build, stop current process.
        if job["phase"] == "build":
            db.request_cancel(job_id)
        db.insert_event(job_id, "system", "Cancel requested (build only)")

    if job["status"] == "queued":
        if subjob_type == "align":
            db.update_subjob_by_type(job_id, "align", status="cancelled", ended_at=time.time())
            db.update_subjob_by_type(job_id, "build", status="cancelled", ended_at=time.time())
        else:
            db.update_subjob_by_type(job_id, "build", status="cancelled", ended_at=time.time())
    return RedirectResponse(url="/", status_code=303)


@app.post("/jobs/{job_id}/rerun")
def rerun_job(job_id: int):
    job = db.get_job(job_id)
    if not job:
        return RedirectResponse(url="/", status_code=303)
    try:
        params = json.loads(job["params_json"])
    except Exception:
        return RedirectResponse(url="/", status_code=303)
    # Always enforce one-to-one behavior in reruns.
    params["strict_duplicate_key_filter"] = True
    db.insert_job(params)
    return RedirectResponse(url="/", status_code=303)


@app.post("/jobs/{job_id}/rerun_nocache")
def rerun_job_nocache(job_id: int):
    job = db.get_job(job_id)
    if not job:
        return RedirectResponse(url="/", status_code=303)
    try:
        params = json.loads(job["params_json"])
    except Exception:
        return RedirectResponse(url="/", status_code=303)
    # Always enforce one-to-one behavior in reruns.
    params["strict_duplicate_key_filter"] = True
    params["force_align"] = True
    params["skip_build"] = False
    params.pop("require_cached_align", None)
    db.insert_job(params)
    return RedirectResponse(url="/", status_code=303)


@app.post("/jobs/{job_id}/rerun_align")
def rerun_align(job_id: int):
    job = db.get_job(job_id)
    if not job:
        return RedirectResponse(url="/", status_code=303)
    try:
        params = json.loads(job["params_json"])
    except Exception:
        return RedirectResponse(url="/", status_code=303)
    # Always enforce one-to-one behavior in reruns.
    params["strict_duplicate_key_filter"] = True
    params["skip_build"] = True
    db.insert_job(params)
    return RedirectResponse(url="/", status_code=303)


@app.post("/jobs/{job_id}/rerun_build")
def rerun_build(job_id: int):
    job = db.get_job(job_id)
    if not job:
        return RedirectResponse(url="/", status_code=303)
    try:
        params = json.loads(job["params_json"])
    except Exception:
        return RedirectResponse(url="/", status_code=303)
    # Always enforce one-to-one behavior in reruns.
    params["strict_duplicate_key_filter"] = True
    params["require_cached_align"] = True
    params["skip_build"] = False
    db.insert_job(params)
    return RedirectResponse(url="/", status_code=303)


@app.post("/jobs/{job_id}/delete")
def delete_job(job_id: int):
    job = db.get_job(job_id)
    if not job:
        return RedirectResponse(url="/", status_code=303)
    # Never delete active jobs to avoid orphaned worker processes.
    if job["status"] in {"running", "queued"}:
        return RedirectResponse(url="/", status_code=303)
    db.delete_job(job_id)
    return RedirectResponse(url="/", status_code=303)


@app.post("/jobs/delete_stopped")
def delete_stopped_jobs():
    # Remove only non-active jobs; keep running/queued jobs intact.
    for row in db.list_jobs(limit=50000):
        status = str(row["status"] or "").strip().lower()
        if status in {"running", "queued"}:
            continue
        try:
            db.delete_job(int(row["id"]))
        except Exception:
            continue
    return RedirectResponse(url="/", status_code=303)


@app.post("/jobs")
def create_job(
    matching_mode: str = Form("property"),
    class_name: str = Form(...),
    parts_spec: str = Form(""),
    wdc_predicate_pattern: str = Form(""),
    wdc_pattern_search_in: str = Form("predicate"),
    target_endpoint: str = Form("wikidata"),
    target_endpoint_url: str = Form(""),
    target_prefixes: str = Form(""),
    property_mapping_rules: str = Form(""),
    target_property: str = Form(""),
    target_class: str = Form(""),
    wikidata_property: str = Form(""),
    wkd_class: str = Form(""),
    ignore_chars: str = Form(""),
    force_align: Optional[str] = Form(None),
    use_local_only: Optional[str] = Form(None),
):
    raw_params = {
        "matching_mode": _clean_text(matching_mode),
        "class_name": _clean_text(class_name),
        "parts_spec": _clean_text(parts_spec),
        "wdc_predicate_pattern": _clean_text(wdc_predicate_pattern),
        "wdc_pattern_search_in": _clean_text(wdc_pattern_search_in),
        "target_endpoint": _clean_text(target_endpoint),
        "target_endpoint_url": _clean_text(target_endpoint_url),
        "target_prefixes": _clean_text(target_prefixes),
        "property_mapping_rules": _clean_text(property_mapping_rules),
        "target_property": _clean_text(target_property),
        "target_class": _clean_text(target_class),
        "wikidata_property": _clean_text(wikidata_property),
        "wkd_class": _clean_text(wkd_class),
        "ignore_chars": _clean_text(ignore_chars),
        "force_align": bool(force_align),
        "use_local_only": bool(use_local_only),
        "strict_duplicate_key_filter": True,
    }
    params, validation_error = _validate_and_normalize_job_params(raw_params)
    if validation_error:
        return RedirectResponse(url=f"/?form_error={quote_plus(validation_error)}", status_code=303)
    db.insert_job(params)
    return RedirectResponse(url="/", status_code=303)


@app.get("/refresh_classes")
def refresh_classes():
    try:
        _refresh_wdc_classes_from_remote()
    except Exception as exc:
        msg = f"Class refresh failed; local cache/catalog kept unchanged. ({exc})"
        return RedirectResponse(url=f"/?form_error={quote_plus(msg)}", status_code=303)
    return RedirectResponse(url="/", status_code=303)


@app.get("/builds/{class_name}/{build_name}/download")
def download_build(class_name: str, build_name: str):
    build_dir = _resolve_build_dir(class_name, build_name)
    if not build_dir:
        return RedirectResponse(url="/", status_code=303)
    data_root = Path("data").resolve()
    build_config = _load_build_config(build_dir)
    endpoint_token = _endpoint_filename_token(build_config)
    class_token = _safe_filename_token(class_name, fallback="class")
    build_token = _safe_filename_token(build_name, fallback="build")
    fd, zip_path = tempfile.mkstemp(prefix=f"beam_{class_name}_{build_name}_", suffix=".zip")
    os.close(fd)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for fp in build_dir.rglob("*"):
            if fp.is_file():
                zf.write(fp, arcname=str(fp.resolve().relative_to(data_root)))
    filename = f"{class_token}_{build_token}_{endpoint_token}.zip"
    return FileResponse(
        zip_path,
        media_type="application/zip",
        filename=filename,
        background=BackgroundTask(_safe_unlink, zip_path),
    )


@app.get("/builds/{class_name}/{build_name}/sakey/download/{artifact_idx}")
def download_sakey_artifact(class_name: str, build_name: str, artifact_idx: int):
    build_dir = _resolve_build_dir(class_name, build_name)
    if not build_dir:
        return RedirectResponse(url="/", status_code=303)
    path = _resolve_sakey_artifact(build_dir, artifact_idx)
    if not path:
        raise HTTPException(status_code=404, detail="SAKEY artifact not found.")
    return FileResponse(
        str(path),
        media_type="application/octet-stream",
        filename=path.name,
    )


@app.get("/builds/download_selected")
@app.get("/builds/download_selected/")
def download_selected_builds_get():
    return RedirectResponse(url="/?form_error=Select+one+or+more+builds+before+downloading.", status_code=303)


@app.post("/builds/download_selected")
@app.post("/builds/download_selected/")
def download_selected_builds(selected_builds: str = Form("[]")):
    try:
        parsed = json.loads(_clean_text(selected_builds) or "[]")
    except Exception:
        parsed = []
    refs = []
    if isinstance(parsed, list):
        refs = parsed

    unique_keys = set()
    selected_dirs = []
    for item in refs[:300]:
        class_name = ""
        build_name = ""
        if isinstance(item, dict):
            class_name = _clean_text(str(item.get("class_name", "")))
            build_name = _clean_text(str(item.get("build_name", "")))
        elif isinstance(item, str):
            if "::" in item:
                left, right = item.split("::", 1)
                class_name = _clean_text(left)
                build_name = _clean_text(right)
        if not class_name or not build_name:
            continue
        key = f"{class_name}::{build_name}"
        if key in unique_keys:
            continue
        unique_keys.add(key)
        build_dir = _resolve_build_dir(class_name, build_name)
        if not build_dir:
            continue
        build_config = _load_build_config(build_dir)
        endpoint_token = _endpoint_filename_token(build_config)
        class_token = _safe_filename_token(class_name, fallback="class")
        build_token = _safe_filename_token(build_name, fallback="build")
        folder_prefix = f"{class_token}_{build_token}_{endpoint_token}"
        selected_dirs.append((class_name, build_name, build_dir, folder_prefix))

    if not selected_dirs:
        return RedirectResponse(url="/?form_error=No+valid+build+selected+for+download.", status_code=303)

    data_root = Path("data").resolve()
    fd, zip_path = tempfile.mkstemp(prefix="beam_selected_builds_", suffix=".zip")
    os.close(fd)

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for class_name, _, build_dir, folder_prefix in selected_dirs:
            class_token = _safe_filename_token(class_name, fallback="class")
            for fp in build_dir.rglob("*"):
                if not fp.is_file():
                    continue
                try:
                    rel = fp.resolve().relative_to(build_dir.resolve())
                    arcname = str(Path(class_token) / folder_prefix / rel)
                except Exception:
                    arcname = str(Path(class_token) / folder_prefix / fp.name)
                zf.write(fp, arcname=arcname)

    ts = time.strftime("%Y%m%d_%H%M%S")
    filename = f"selected_builds_{len(selected_dirs)}_{ts}.zip"
    return FileResponse(
        zip_path,
        media_type="application/zip",
        filename=filename,
        background=BackgroundTask(_safe_unlink, zip_path),
    )


@app.post("/builds/{class_name}/{build_name}/delete")
def delete_build(class_name: str, build_name: str):
    build_dir = _resolve_build_dir(class_name, build_name)
    if not build_dir:
        return RedirectResponse(url="/", status_code=303)
    try:
        _delete_jobs_for_build_dir(build_dir)
    except Exception:
        pass
    shutil.rmtree(build_dir, ignore_errors=True)
    return RedirectResponse(url="/", status_code=303)


@app.post("/builds/purge_low_links")
def purge_low_link_builds(max_links: int = Form(10)):
    try:
        threshold = int(max_links)
    except Exception:
        threshold = 10
    threshold = max(0, threshold)

    purged = 0
    # Use a high scan limit so this action can clean the full history.
    for build in _scan_builds(limit=100000):
        class_name = str(build.get("class_name") or "").strip()
        build_name = str(build.get("build_name") or "").strip()
        if not class_name or not build_name:
            continue
        variant = build.get("with_link") or build.get("without_link")
        if not isinstance(variant, dict):
            continue
        try:
            links_count = int(variant.get("links_count") or 0)
        except Exception:
            links_count = 0
        if links_count >= threshold:
            continue
        build_dir = _resolve_build_dir(class_name, build_name)
        if not build_dir:
            continue
        try:
            _delete_jobs_for_build_dir(build_dir)
        except Exception:
            pass
        shutil.rmtree(build_dir, ignore_errors=True)
        purged += 1
    return RedirectResponse(url=f"/?purged={purged}", status_code=303)


@app.post("/builds/{class_name}/{build_name}/rerun")
def rerun_build_from_build_card(class_name: str, build_name: str):
    build_dir = _resolve_build_dir(class_name, build_name)
    if not build_dir:
        return RedirectResponse(url="/", status_code=303)
    try:
        params, validation_error = _rerun_params_from_build_config(build_dir, class_name)
        if validation_error:
            msg = f"Cannot rerun build: {validation_error}"
            return RedirectResponse(url=f"/?form_error={quote_plus(msg)}", status_code=303)
        db.insert_job(params)
    except Exception as exc:
        msg = f"Cannot rerun build: {exc}"
        return RedirectResponse(url=f"/?form_error={quote_plus(msg)}", status_code=303)
    return RedirectResponse(url="/", status_code=303)


@app.websocket("/ws/logs/{job_id}")
async def ws_logs(websocket: WebSocket, job_id: int):
    await websocket.accept()
    try:
        job = db.get_job(job_id)
        if not job:
            await websocket.send_text("Job not found")
            await websocket.close()
            return
        last_id = 0
        def _event_payload(row):
            meta = None
            try:
                if row["meta_json"]:
                    meta = json.loads(row["meta_json"])
            except Exception:
                meta = None
            return {
                "type": "event",
                "id": row["id"],
                "ts": row["ts"],
                "level": row["level"],
                "message": row["message"],
                "phase": row["phase"],
                "kind": row["kind"],
                "step": row["step"],
                "worker": row["worker"],
                "progress_pct": row["progress_pct"],
                "meta": meta,
            }
        # send recent history
        rows = db.list_events(job_id, since_id=None, limit=200)
        for r in rows:
            await websocket.send_text(json.dumps(_event_payload(r)))
            last_id = r["id"]
        while True:
            # Push updates at a fixed cadence even if client pings stall.
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=0.5)
            except asyncio.TimeoutError:
                pass
            job = db.get_job(job_id)
            if job:
                payload = {
                    "type": "progress",
                    "status": job["status"],
                    "cancel_requested": job["cancel_requested"],
                    "phase": job["phase"],
                    "progress_text": job["progress_text"],
                    "progress_pct": job["progress_pct"],
                    "current_step": job["current_step"],
                    "current_file": job["current_file"],
                    "result_path": job["result_path"],
                    "align_dir": job["align_dir"],
                    "reused_align": bool(job["reused_align"]),
                    "error_message": job["error_message"],
                    "final_links_count": job["final_links_count"],
                    "outputs": _job_outputs(job),
                    "subjobs": [dict(s) for s in db.list_subjobs(job_id)],
                }
                await websocket.send_text(json.dumps(payload))
            rows = db.list_events(job_id, since_id=last_id, limit=200)
            if rows:
                for r in rows:
                    await websocket.send_text(json.dumps(_event_payload(r)))
                    last_id = r["id"]
    except WebSocketDisconnect:
        return

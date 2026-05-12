@app.on_event("startup")
def _init_db():
    db.init_db()


def _render_index_page(
    request: Request,
    app_view: str = "create",
    preset: Optional[str] = None,
    recent: Optional[int] = None,
    form_error: Optional[str] = None,
    test_mode: Optional[str] = None,
):
    is_test_mode = _bool_from_any(test_mode)
    # Seed from local catalog first. Do not auto-scrape remote stats on startup.
    if not db.list_wdc_classes():
        _seed_wdc_classes_from_local_catalog()
    local_rows = _discover_local_class_rows("Download")
    if local_rows:
        try:
            db.upsert_wdc_classes(local_rows)
        except Exception:
            pass

    form = _default_form()
    visible_presets = _filter_presets_by_mode(is_test_mode)
    selected_preset = ""
    if preset:
        if preset in visible_presets:
            form.update(visible_presets[preset])
            selected_preset = preset

    if recent:
        job = db.get_job(recent)
        if job:
            try:
                params = json.loads(job["params_json"])
                if _is_test_class_name(params.get("class_name")) == is_test_mode:
                    form.update(params)
            except Exception:
                pass

    _sync_target_alias_fields(form)
    form["matching_mode"] = _normalize_matching_mode(
        form.get("matching_mode"),
        fallback_wdc_value_is_wikidata=bool(form.get("wdc_value_is_wikidata")),
    )

    wdc_classes = [dict(r) for r in db.list_wdc_classes()]
    wdc_classes = [r for r in wdc_classes if _is_test_class_name(r.get("class_name")) == is_test_mode]
    class_meta = {r["class_name"]: r for r in wdc_classes}

    class_parts_info = None
    if form.get("class_name") and form.get("class_name") in class_meta:
        class_parts_info = _build_class_parts_info(form["class_name"])

    recent_presets = _get_recent_presets(test_mode=is_test_mode)
    dashboard = _build_dashboard_state(job_limit=50, build_limit=200, test_mode=is_test_mode)
    jobs = dashboard["jobs_for_panel"]
    builds = dashboard["builds"]
    jobs_outputs = {j["id"]: dashboard["jobs_outputs"][j["id"]] for j in jobs}
    jobs_times = {j["id"]: dashboard["jobs_times"][j["id"]] for j in jobs}
    jobs_params = {j["id"]: dashboard["jobs_params"][j["id"]] for j in jobs}
    jobs_subjobs = {j["id"]: dashboard["jobs_subjobs"][j["id"]] for j in jobs}

    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "app_view": app_view if app_view in {"create", "jobs", "history"} else "create",
            "form": form,
            "presets": visible_presets,
            "selected_preset": selected_preset,
            "recent_presets": recent_presets,
            "jobs": jobs,
            "jobs_outputs": jobs_outputs,
            "jobs_times": jobs_times,
            "jobs_params": jobs_params,
            "jobs_subjobs": jobs_subjobs,
            "builds": builds,
            "class_meta": class_meta,
            "class_parts_info": class_parts_info,
            "form_error": _clean_text(form_error),
            "is_test_mode": is_test_mode,
            "target_endpoints": [
                {"key": k, "label": v.get("label", k), "default_url": v.get("default_url", "")}
                for k, v in TARGET_ENDPOINTS.items()
            ],
        },
    )


@app.get("/", response_class=HTMLResponse)
def index(
    request: Request,
    preset: Optional[str] = None,
    recent: Optional[int] = None,
    form_error: Optional[str] = None,
    test_mode: Optional[str] = None,
):
    return _render_index_page(
        request=request,
        app_view="create",
        preset=preset,
        recent=recent,
        form_error=form_error,
        test_mode=test_mode,
    )


@app.get("/app/create", response_class=HTMLResponse)
def app_create(
    request: Request,
    preset: Optional[str] = None,
    recent: Optional[int] = None,
    form_error: Optional[str] = None,
    test_mode: Optional[str] = None,
):
    return _render_index_page(
        request=request,
        app_view="create",
        preset=preset,
        recent=recent,
        form_error=form_error,
        test_mode=test_mode,
    )


@app.get("/app/jobs", response_class=HTMLResponse)
def app_jobs(
    request: Request,
    test_mode: Optional[str] = None,
):
    return _render_index_page(
        request=request,
        app_view="jobs",
        test_mode=test_mode,
    )


@app.get("/app/history", response_class=HTMLResponse)
def app_history(
    request: Request,
    test_mode: Optional[str] = None,
):
    return _render_index_page(
        request=request,
        app_view="history",
        test_mode=test_mode,
    )


@app.get("/tutorial", response_class=HTMLResponse)
def tutorial_page(
    request: Request,
    test_mode: Optional[str] = None,
):
    is_test_mode = _bool_from_any(test_mode)
    payload = _load_tutorial_page_data()
    return templates.TemplateResponse(
        request,
        "tutorial.html",
        {
            "is_test_mode": is_test_mode,
            "tutorial_ok": payload["ok"],
            "tutorial_error": payload["error"],
            "tutorial_html": payload["html"],
            "tutorial_sections": payload["sections"],
            "tutorial_source_path": payload["source_path"],
        },
    )


@app.get("/sakey", response_class=HTMLResponse)
def sakey_page(
    request: Request,
    class_name: str = "",
    run_id: str = "",
    key_order_by: str = "",
    key_min_support: str = "",
    key_only_almost: Optional[str] = None,
    key_max_size: str = "",
    key_q: str = "",
    test_mode: Optional[str] = None,
    form_error: Optional[str] = None,
):
    is_test_mode = _bool_from_any(test_mode)
    if not db.list_wdc_classes():
        _seed_wdc_classes_from_local_catalog()
    local_rows = _discover_local_class_rows("Download")
    if local_rows:
        try:
            db.upsert_wdc_classes(local_rows)
        except Exception:
            pass
    payload = _sakey_page_payload(
        class_name=class_name,
        run_id=run_id,
        test_mode=is_test_mode,
        key_order_by=key_order_by,
        key_min_support=key_min_support,
        key_only_almost=key_only_almost,
        key_max_size=key_max_size,
        key_q=key_q,
    )

    return templates.TemplateResponse(
        request,
        "sakey.html",
        {
            "is_test_mode": is_test_mode,
            "form_error": _clean_text(form_error),
            **payload,
        },
    )


@app.post("/sakey/run")
def sakey_run(
    class_name: str = Form(""),
    parts_spec: str = Form("all"),
    mins: int = Form(3),
    timeout_hours: float = Form(48.0),
    test_mode: Optional[str] = Form(None),
):
    is_test_mode = _bool_from_any(test_mode)
    if not db.list_wdc_classes():
        _seed_wdc_classes_from_local_catalog()
    cname = _clean_text(class_name)
    if not cname:
        query = "test_mode=1&" if is_test_mode else ""
        query += f"form_error={quote_plus('Class name is required.')}"
        return RedirectResponse(url=f"/sakey?{query}", status_code=303)
    run_id = _enqueue_sakey_run(
        class_name=cname,
        parts_spec=_clean_text(parts_spec) or "all",
        mins=max(1, int(mins or 3)),
        timeout_hours=max(0.1, float(timeout_hours or 48.0)),
    )
    query = []
    if is_test_mode:
        query.append("test_mode=1")
    query.append(f"class_name={quote_plus(cname)}")
    query.append(f"run_id={quote_plus(run_id)}")
    return RedirectResponse(url=f"/sakey?{'&'.join(query)}", status_code=303)


@app.get("/api/sakey/status")
def sakey_status_api(
    class_name: str = "",
    run_id: str = "",
    key_order_by: str = "",
    key_min_support: str = "",
    key_only_almost: Optional[str] = None,
    key_max_size: str = "",
    key_q: str = "",
    test_mode: Optional[str] = None,
):
    is_test_mode = _bool_from_any(test_mode)
    payload = _sakey_page_payload(
        class_name=class_name,
        run_id=run_id,
        test_mode=is_test_mode,
        key_order_by=key_order_by,
        key_min_support=key_min_support,
        key_only_almost=key_only_almost,
        key_max_size=key_max_size,
        key_q=key_q,
    )
    return {"ok": True, **payload}


@app.get("/sakey/runs/{run_id}/artifact/{name}")
def sakey_download_artifact(run_id: str, name: str):
    p = _sakey_resolve_artifact(run_id, name)
    if not p:
        raise HTTPException(status_code=404, detail="Artifact not found.")
    return FileResponse(
        str(p),
        media_type="application/octet-stream",
        filename=p.name,
    )


@app.get("/builds/{class_name}/{build_name}", response_class=HTMLResponse)
def build_detail_page(
    request: Request,
    class_name: str,
    build_name: str,
    test_mode: Optional[str] = None,
):
    is_test_mode = _bool_from_any(test_mode)
    build_dir = _resolve_build_dir(class_name, build_name)
    if not build_dir:
        query = "test_mode=1&" if is_test_mode else ""
        query += f"form_error={quote_plus('Build not found.')}"
        return RedirectResponse(url=f"/?{query}", status_code=303)

    build = _build_summary_from_dir(build_dir)
    if not build:
        query = "test_mode=1&" if is_test_mode else ""
        query += f"form_error={quote_plus('Build not found.')}"
        return RedirectResponse(url=f"/?{query}", status_code=303)

    return templates.TemplateResponse(
        request,
        "build_detail.html",
        {
            "build": build,
            "is_test_mode": is_test_mode,
        },
    )


@app.get("/builds/{class_name}/{build_name}/links", response_class=HTMLResponse)
def build_links_page(
    request: Request,
    class_name: str,
    build_name: str,
    variant: Optional[str] = None,
    q: str = "",
    offset: int = 0,
    limit: int = 30,
    test_mode: Optional[str] = None,
):
    is_test_mode = _bool_from_any(test_mode)
    build_dir = _resolve_build_dir(class_name, build_name)
    if not build_dir:
        query = "test_mode=1&" if is_test_mode else ""
        query += f"form_error={quote_plus('Build not found.')}"
        return RedirectResponse(url=f"/?{query}", status_code=303)

    build = {
        "class_name": class_name,
        "build_name": build_name,
    }
    build_config = _load_build_config(build_dir)
    build["linking_combinations"] = _extract_linking_combinations(build_config)

    variant_dir, variant_name = _resolve_link_explorer_variant_dir(build_dir, variant=variant)
    if not variant_dir or not variant_name:
        query = "test_mode=1&" if is_test_mode else ""
        query += f"form_error={quote_plus('No link files available for this build.')}"
        return RedirectResponse(url=f"/?{query}", status_code=303)

    ent_links_path = variant_dir / "ent_links"
    page = _scan_ent_links_page(ent_links_path, offset=offset, limit=limit, query=q)
    rows = page["rows"]
    total = page["total"]

    available_variants = []
    for name in _LINK_EXPLORER_VARIANTS:
        p = build_dir / name
        if not p.exists() or not p.is_dir():
            continue
        available_variants.append(
            {
                "name": name,
                "has_ent_links": (p / "ent_links").exists(),
            }
        )
    if not available_variants:
        available_variants = [{"name": variant_name, "has_ent_links": ent_links_path.exists()}]

    return templates.TemplateResponse(
        request,
        "link_explorer.html",
        {
            "build": build,
            "is_test_mode": is_test_mode,
            "selected_variant": variant_name,
            "available_variants": available_variants,
            "initial_query": _clean_text(q),
            "initial_offset": max(0, int(offset)),
            "initial_limit": max(1, min(int(limit), 200)),
            "initial_total": total,
            "initial_has_more": bool(page.get("has_more", False)),
            "initial_rows": rows,
            "initial_detail": None,
            "linking_combinations": build.get("linking_combinations", []),
        },
    )


@app.get("/api/builds/{class_name}/{build_name}/links")
def build_links_api(
    class_name: str,
    build_name: str,
    variant: Optional[str] = None,
    q: str = "",
    offset: int = 0,
    limit: int = 30,
):
    build_dir = _resolve_build_dir(class_name, build_name)
    if not build_dir:
        raise HTTPException(status_code=404, detail="Build not found.")
    variant_dir, variant_name = _resolve_link_explorer_variant_dir(build_dir, variant=variant)
    if not variant_dir or not variant_name:
        raise HTTPException(status_code=404, detail="No link files available for this build.")

    ent_links_path = variant_dir / "ent_links"
    page = _scan_ent_links_page(ent_links_path, offset=offset, limit=limit, query=q)
    return {
        "ok": True,
        "class_name": class_name,
        "build_name": build_name,
        "variant": variant_name,
        "q": _clean_text(q),
        "offset": max(0, int(offset)),
        "limit": max(1, min(int(limit), 200)),
        "total": page["total"],
        "has_more": bool(page.get("has_more", False)),
        "rows": page["rows"],
    }


@app.get("/api/builds/{class_name}/{build_name}/link")
def build_link_detail_api(
    class_name: str,
    build_name: str,
    idx: int,
    variant: Optional[str] = None,
    wait_ms: int = 250,
):
    build_dir = _resolve_build_dir(class_name, build_name)
    if not build_dir:
        raise HTTPException(status_code=404, detail="Build not found.")
    variant_dir, variant_name = _resolve_link_explorer_variant_dir(build_dir, variant=variant)
    if not variant_dir or not variant_name:
        raise HTTPException(status_code=404, detail="No link files available for this build.")

    wait_ms = max(0, min(int(wait_ms), 5000))
    key, status, payload, fut = _start_link_detail_build(build_dir, variant_dir, variant_name, idx)
    if status != "ready":
        payload, status = _read_link_detail_future(key, fut, wait_ms=wait_ms)

    if status != "ready":
        return {
            "ok": True,
            "class_name": class_name,
            "build_name": build_name,
            "variant": variant_name,
            "idx": int(idx),
            "pending": True,
            "cache_key": key,
        }
    if not payload:
        raise HTTPException(status_code=404, detail="Link not found at this index.")
    return {
        "ok": True,
        "class_name": class_name,
        "build_name": build_name,
        "variant": variant_name,
        "pending": False,
        "cache_key": key,
        "detail": payload,
    }


@app.get("/api/builds/{class_name}/{build_name}/node")
def build_link_node_api(
    class_name: str,
    build_name: str,
    node: str,
    side: str = "wdc",
    variant: Optional[str] = None,
):
    node_value = _clean_text(node)
    if not node_value:
        raise HTTPException(status_code=400, detail="node is required.")
    build_dir = _resolve_build_dir(class_name, build_name)
    if not build_dir:
        raise HTTPException(status_code=404, detail="Build not found.")
    variant_dir, variant_name = _resolve_link_explorer_variant_dir(build_dir, variant=variant)
    if not variant_dir or not variant_name:
        raise HTTPException(status_code=404, detail="No link files available for this build.")

    payload = _build_node_payload(variant_dir, side, node_value)
    return {
        "ok": True,
        "class_name": class_name,
        "build_name": build_name,
        "variant": variant_name,
        "node": payload,
    }


@app.get("/api/dashboard")
def dashboard_api(job_limit: int = 80, build_limit: int = 200, test_mode: Optional[bool] = None):
    job_limit = max(1, min(int(job_limit), 200))
    build_limit = max(1, min(int(build_limit), 200))
    dashboard = _build_dashboard_state(job_limit=job_limit, build_limit=build_limit, test_mode=test_mode)

    jobs = []
    for j in dashboard["all_jobs"]:
        jid = j["id"]
        jobs.append(
            {
                **j,
                "times": dashboard["jobs_times"].get(jid, {}),
                "params": dashboard["jobs_params"].get(jid, {}),
                "outputs": dashboard["jobs_outputs"].get(jid, {}),
                "subjobs": dashboard["jobs_subjobs"].get(jid, []),
            }
        )

    builds = []
    for b in dashboard["builds"]:
        builds.append(
            {
                "class_name": b.get("class_name"),
                "build_name": b.get("build_name"),
                "path": b.get("path"),
                "done_at": b.get("done_at"),
                "is_completed": bool(b.get("is_completed")),
                "done_label": b.get("done_label") or "Last update",
                "with_link": b.get("with_link"),
                "without_link": b.get("without_link"),
                "variants_same": b.get("variants_same"),
                "config_groups": b.get("config_groups") or [],
                "endpoint_label": b.get("endpoint_label") or "Wikidata",
                "linking_elements_text": b.get("linking_elements_text") or "",
                "linking_stats_text": b.get("linking_stats_text") or "",
            }
        )

    return {
        "server_ts": time.time(),
        "job_count": len(jobs),
        "active_job_count": len(dashboard["active_jobs"]),
        "visible_job_count": len(dashboard["jobs_for_panel"]),
        "build_count": len(builds),
        "active_job_ids": [j["id"] for j in dashboard["active_jobs"]],
        "visible_job_ids": [j["id"] for j in dashboard["jobs_for_panel"]],
        "jobs": jobs,
        "builds": builds,
    }



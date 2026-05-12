

def test_link_explorer_page_and_api(monkeypatch, test_wdc_classes):
    client, _web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    build_name = "beam_20260212_120012"
    build_root = Path("data") / "TestClass" / build_name
    _make_build_tree(build_root)
    _write_link_explorer_variant(build_root / "with_link_code")

    with client:
        page = client.get(f"/builds/TestClass/{build_name}/links?test_mode=1")
        links_resp = client.get(f"/api/builds/TestClass/{build_name}/links?variant=with_link_code")
        detail_resp = client.get(f"/api/builds/TestClass/{build_name}/link?variant=with_link_code&idx=0")
        node_resp = client.get(
            f"/api/builds/TestClass/{build_name}/node",
            params={
                "variant": "with_link_code",
                "side": "wdc",
                "node": "http://example.org/wdc/entity1",
            },
        )

    assert page.status_code == 200
    assert "Link Explorer" in page.text
    assert "Equivalent properties (WDC -> target)" in page.text
    assert "Simple view: property equivalents + recursive IRI tree" in page.text
    assert "IRI WDC" not in page.text
    assert "IRI Wikidata" not in page.text

    assert links_resp.status_code == 200
    links_payload = links_resp.json()
    assert links_payload["ok"] is True
    assert links_payload["variant"] == "with_link_code"
    assert links_payload["total"] >= 1
    assert links_payload["rows"][0]["wdc_iri"] == "http://example.org/wdc/entity1"

    assert detail_resp.status_code == 200
    detail_payload = detail_resp.json()
    assert detail_payload["ok"] is True
    detail = detail_payload["detail"]
    assert detail["wdc_iri"] == "http://example.org/wdc/entity1"
    assert detail["wikidata_uri"] == "http://www.wikidata.org/entity/Q100"
    assert any(
        row.get("wdc_short_property") == "name" and row.get("wikidata_short_property") == "label"
        for row in detail.get("property_matches", [])
    )
    assert any(
        str(row.get("wikidata_short_property", "")).lower() == "p1329"
        and row.get("wikidata_property_label") == "phone number"
        for row in detail.get("property_matches", [])
    )

    assert node_resp.status_code == 200
    node_payload = node_resp.json()
    assert node_payload["ok"] is True
    assert node_payload["node"]["side"] == "wdc"
    assert node_payload["node"]["node"] == "http://example.org/wdc/entity1"
    assert node_payload["node"]["summary_label"] == "Alpha City"
    assert node_payload["node"]["attr_count"] >= 1


def test_link_explorer_links_api_fast_mode_for_large_files(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    build_name = "beam_20260212_120012_fast"
    build_root = Path("data") / "TestClass" / build_name
    _make_build_tree(build_root, links_count=6)

    # Force fast scan mode on this tiny fixture to validate behavior.
    monkeypatch.setattr(web_main, "_LINK_EXPLORER_FAST_SCAN_BYTES", 1)

    with client:
        links_resp = client.get(
            f"/api/builds/TestClass/{build_name}/links",
            params={"variant": "with_link_code", "offset": 0, "limit": 2},
        )
        filtered_resp = client.get(
            f"/api/builds/TestClass/{build_name}/links",
            params={"variant": "with_link_code", "q": "entity", "offset": 0, "limit": 2},
        )

    assert links_resp.status_code == 200
    links_payload = links_resp.json()
    assert links_payload["ok"] is True
    assert links_payload["total"] is None
    assert links_payload["has_more"] is True
    assert len(links_payload["rows"]) == 2

    # With a query, we still compute exact totals.
    assert filtered_resp.status_code == 200
    filtered_payload = filtered_resp.json()
    assert filtered_payload["ok"] is True
    assert filtered_payload["total"] == 6
    assert filtered_payload["has_more"] is True
    assert len(filtered_payload["rows"]) == 2


def test_link_explorer_falls_back_to_wikidata_property_meta(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    build_name = "beam_20260212_120013"
    build_root = Path("data") / "TestClass" / build_name
    _make_build_tree(build_root)
    _write_link_explorer_variant(build_root / "with_link_code")

    (build_root / "with_link_code" / "prop_stats_wd.tsv").write_text(
        "predicate\tcount\tlabel\tdescription\n"
        "http://www.wikidata.org/prop/direct/p1329\t1\t\t\n",
        encoding="utf-8",
    )

    def fake_wikidata_meta(prop_id, language="en"):
        if prop_id == "P1329":
            return "phone number", "telephone number of subject"
        return "", ""

    monkeypatch.setattr(web_main, "_fetch_wikidata_property_meta", fake_wikidata_meta)

    with client:
        detail_resp = client.get(f"/api/builds/TestClass/{build_name}/link?variant=with_link_code&idx=0")

    assert detail_resp.status_code == 200
    detail = detail_resp.json()["detail"]
    assert any(
        str(row.get("wikidata_short_property", "")).lower() == "p1329"
        and row.get("wikidata_property_label") == "phone number"
        and row.get("wikidata_property_description") == "telephone number of subject"
        for row in detail.get("property_matches", [])
    )


def test_link_explorer_node_summary_local_and_wikidata_entity_fallback(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    build_name = "beam_20260212_120017"
    build_root = Path("data") / "TestClass" / build_name
    _make_build_tree(build_root)
    _write_link_explorer_variant(build_root / "with_link_code")

    (build_root / "with_link_code" / "attr_triples_2").write_text(
        'http://www.wikidata.org/entity/Q100\thttp://www.wikidata.org/prop/direct/P1329\t"+331234567"\n'
        '_:b1\thttp://www.w3.org/2000/01/rdf-schema#label\t"Nested blank node"\n'
        '_:b1\thttp://schema.org/description\t"nested description from local triples"\n',
        encoding="utf-8",
    )
    (build_root / "with_link_code" / "rel_triples_2").write_text(
        "http://www.wikidata.org/entity/Q100\thttp://www.wikidata.org/prop/direct/P527\t_:b1\n",
        encoding="utf-8",
    )

    def fake_wikidata_entity_meta(entity_id, language="en"):
        if entity_id == "Q100":
            return "Alpha City WD", "city in fallback metadata"
        return "", ""

    monkeypatch.setattr(web_main, "_fetch_wikidata_entity_meta", fake_wikidata_entity_meta)

    with client:
        wd_resp = client.get(
            f"/api/builds/TestClass/{build_name}/node",
            params={
                "variant": "with_link_code",
                "side": "wd",
                "node": "http://www.wikidata.org/entity/Q100",
            },
        )
        blank_resp = client.get(
            f"/api/builds/TestClass/{build_name}/node",
            params={
                "variant": "with_link_code",
                "side": "wd",
                "node": "_:b1",
            },
        )

    assert wd_resp.status_code == 200
    wd_node = wd_resp.json()["node"]
    assert wd_node["summary_label"] == "Alpha City WD"
    assert wd_node["summary_description"] == "city in fallback metadata"

    assert blank_resp.status_code == 200
    blank_node = blank_resp.json()["node"]
    assert blank_node["summary_label"] == "Nested blank node"
    assert blank_node["summary_description"] == "nested description from local triples"


def test_link_explorer_aligns_by_values_when_property_names_do_not_match(monkeypatch, test_wdc_classes):
    client, _web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    build_name = "beam_20260212_120014"
    build_root = Path("data") / "TestClass" / build_name
    _make_build_tree(build_root)
    _write_link_explorer_value_fallback_variant(build_root / "with_link_code")

    with client:
        detail_resp = client.get(f"/api/builds/TestClass/{build_name}/link?variant=with_link_code&idx=0")

    assert detail_resp.status_code == 200
    detail = detail_resp.json()["detail"]
    assert any(
        row.get("wdc_short_property") == "snarcRef"
        and str(row.get("wikidata_short_property", "")).lower() == "p12749"
        and row.get("match_reason") == "value_fallback"
        for row in detail.get("property_matches", [])
    )


def test_link_explorer_aligns_by_values_when_wikidata_property_has_multiple_values(monkeypatch, test_wdc_classes):
    client, _web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    build_name = "beam_20260212_120015"
    build_root = Path("data") / "TestClass" / build_name
    _make_build_tree(build_root)
    _write_link_explorer_value_fallback_multivalue_variant(build_root / "with_link_code")

    with client:
        detail_resp = client.get(f"/api/builds/TestClass/{build_name}/link?variant=with_link_code&idx=0")

    assert detail_resp.status_code == 200
    detail = detail_resp.json()["detail"]
    assert any(
        row.get("wdc_short_property") == "snarcRef"
        and str(row.get("wikidata_short_property", "")).lower() == "p12749"
        and row.get("match_reason") == "value_fallback"
        for row in detail.get("property_matches", [])
    )


def test_link_explorer_does_not_align_on_weak_numeric_value_only(monkeypatch, test_wdc_classes):
    client, _web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    build_name = "beam_20260212_120016"
    build_root = Path("data") / "TestClass" / build_name
    _make_build_tree(build_root)
    _write_link_explorer_weak_numeric_variant(build_root / "with_link_code")

    with client:
        detail_resp = client.get(f"/api/builds/TestClass/{build_name}/link?variant=with_link_code&idx=0")

    assert detail_resp.status_code == 200
    detail = detail_resp.json()["detail"]
    assert not any(
        row.get("wdc_short_property") == "aggregateRating"
        and str(row.get("wikidata_short_property", "")).lower() == "sitelinks"
        for row in detail.get("property_matches", [])
    )


def test_delete_build_removes_directory_and_job_rows(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    build_name = "beam_20260212_120001"
    build_root = Path("data") / "TestClass" / build_name
    _make_build_tree(build_root)

    job_id_abs = web_main.db.insert_job({"class_name": "TestClass"})
    web_main.db.update_job(job_id_abs, status="done", result_path=str(build_root.resolve()))
    job_id_rel = web_main.db.insert_job({"class_name": "TestClass"})
    web_main.db.update_job(job_id_rel, status="done", result_path=str(build_root))
    job_id_dot_rel = web_main.db.insert_job({"class_name": "TestClass"})
    web_main.db.update_job(job_id_dot_rel, status="done", result_path=f"./{build_root}")

    with client:
        resp = client.post(f"/builds/TestClass/{build_name}/delete", follow_redirects=False)

    assert resp.status_code == 303
    assert not build_root.exists()
    assert web_main.db.get_job(job_id_abs) is None
    assert web_main.db.get_job(job_id_rel) is None
    assert web_main.db.get_job(job_id_dot_rel) is None


def test_purge_low_links_builds_removes_only_under_threshold(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    low_build_name = "beam_20260212_lowlinks"
    high_build_name = "beam_20260212_highlinks"
    low_build_root = Path("data") / "TestClass" / low_build_name
    high_build_root = Path("data") / "TestClass" / high_build_name
    _make_build_tree(low_build_root, links_count=2)
    _make_build_tree(high_build_root, links_count=12)

    low_job_id = web_main.db.insert_job({"class_name": "TestClass"})
    web_main.db.update_job(low_job_id, status="done", result_path=str(low_build_root.resolve()))
    low_job_rel_id = web_main.db.insert_job({"class_name": "TestClass"})
    web_main.db.update_job(low_job_rel_id, status="done", result_path=str(low_build_root))
    high_job_id = web_main.db.insert_job({"class_name": "TestClass"})
    web_main.db.update_job(high_job_id, status="done", result_path=str(high_build_root.resolve()))

    with client:
        resp = client.post("/builds/purge_low_links", data={"max_links": "10"}, follow_redirects=False)

    assert resp.status_code == 303
    assert "purged=1" in (resp.headers.get("location") or "")
    assert not low_build_root.exists()
    assert high_build_root.exists()
    assert web_main.db.get_job(low_job_id) is None
    assert web_main.db.get_job(low_job_rel_id) is None
    assert web_main.db.get_job(high_job_id) is not None


def test_rerun_build_from_card_queues_new_job(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    build_name = "beam_20260212_120123"
    build_root = Path("data") / "TestClass" / build_name
    _make_build_tree(build_root)

    with client:
        resp = client.post(f"/builds/TestClass/{build_name}/rerun", follow_redirects=False)

    assert resp.status_code == 303
    jobs = web_main.db.list_jobs(limit=1)
    assert len(jobs) == 1
    params = json.loads(jobs[0]["params_json"])
    assert params["class_name"] == "TestClass"
    assert params["parts_spec"] == "all"
    assert params["wdc_predicate_pattern"] == "name"
    assert params["wikidata_property"] == "rdfs:label"


def test_rerun_build_from_card_handles_insert_error(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    build_name = "beam_20260212_120124"
    build_root = Path("data") / "TestClass" / build_name
    _make_build_tree(build_root)

    monkeypatch.setattr(web_main.db, "insert_job", lambda _params: (_ for _ in ()).throw(RuntimeError("db is locked")))

    with client:
        resp = client.post(f"/builds/TestClass/{build_name}/rerun", follow_redirects=False)

    assert resp.status_code == 303
    loc = resp.headers.get("location", "")
    assert "form_error=" in loc
    assert "Cannot+rerun+build%3A+db+is+locked" in loc


def test_dashboard_api_returns_live_jobs_and_builds(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    build_name = "beam_20260212_120777"
    build_root = Path("data") / "TestClass" / build_name
    _make_build_tree(build_root)

    running_job_id = web_main.db.insert_job({"class_name": "TestClass", "parts_spec": "all"})
    web_main.db.update_job(
        running_job_id,
        status="running",
        phase="build",
        progress_text="building...",
        progress_pct=55.0,
    )
    web_main.db.update_subjob_by_type(
        running_job_id,
        "build",
        status="running",
        progress_text="build step",
        current_step="build_wd",
    )

    done_job_id = web_main.db.insert_job({"class_name": "TestClass"})
    web_main.db.update_job(done_job_id, status="done", result_path=str(build_root.resolve()))

    with client:
        resp = client.get("/api/dashboard")

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["build_count"] >= 1
    build_entry = next((b for b in payload["builds"] if b["build_name"] == build_name), None)
    assert build_entry is not None
    assert any(g.get("title") == "Input" for g in build_entry.get("config_groups", []))
    assert build_entry["with_link"]["sample_links"]
    assert build_entry["with_link"]["top_wdc_props"]
    assert build_entry["with_link"]["top_wd_props"]
    assert isinstance(build_entry["with_link"]["qa_warnings"], list)
    assert "via iata" in build_entry.get("linking_stats_text", "")
    assert payload["job_count"] >= 2
    assert running_job_id in payload["active_job_ids"]
    assert done_job_id not in payload["active_job_ids"]
    assert running_job_id in payload["visible_job_ids"]
    assert done_job_id not in payload["visible_job_ids"]

    jobs = {j["id"]: j for j in payload["jobs"]}
    assert jobs[running_job_id]["status"] == "running"
    assert jobs[running_job_id]["outputs"]["build_done"] is False
    assert isinstance(jobs[running_job_id]["subjobs"], list)
    assert jobs[done_job_id]["status"] == "done"
    assert jobs[done_job_id]["outputs"]["build_done"] is True


def test_dashboard_api_backfills_legacy_link_source_stats(monkeypatch, test_wdc_classes):
    client, _web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    build_name = "beam_20260317_legacy"
    build_root = Path("data") / "TestClass" / build_name
    _make_build_tree(build_root, links_count=7, class_name="TestClass")

    cfg_path = build_root / "BUILD_CONFIG.json"
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    cfg["matching_mode"] = "sameas"
    cfg["wdc_predicate_pattern"] = "sameAs"
    cfg["target_property"] = ""
    cfg["wikidata_property"] = ""
    cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

    # Legacy stats: missing links_by_source_* rows.
    (build_root / "BUILD_STATS.json").write_text(
        json.dumps(
            {
                "class_name": "TestClass",
                "build_name": build_name,
                "links_before_filters": 21,
                "links_count_with_link_code": 7,
            }
        ),
        encoding="utf-8",
    )

    with client:
        resp = client.get("/api/dashboard")
    assert resp.status_code == 200
    payload = resp.json()
    entry = next((b for b in payload.get("builds", []) if b.get("build_name") == build_name), None)
    assert entry is not None
    assert "7 via sameas" in (entry.get("linking_stats_text") or "")

    reloaded = json.loads((build_root / "BUILD_STATS.json").read_text(encoding="utf-8"))
    assert reloaded.get("links_by_source_after_filter") == [{"source": "via sameas", "count": 7}]
    assert reloaded.get("links_by_source_align") == [{"source": "via sameas", "count": 21}]


def test_dashboard_api_filters_test_mode(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)

    test_build_name = "beam_20260212_test"
    prod_build_name = "beam_20260212_prod"
    test_build_root = Path("data") / "TestClass" / test_build_name
    prod_build_root = Path("data") / "City" / prod_build_name
    _make_build_tree(test_build_root, class_name="TestClass")
    _make_build_tree(prod_build_root, class_name="City")

    test_job_id = web_main.db.insert_job({"class_name": "TestClass", "parts_spec": "all"})
    web_main.db.update_job(test_job_id, status="running")
    prod_job_id = web_main.db.insert_job({"class_name": "City", "parts_spec": "all"})
    web_main.db.update_job(prod_job_id, status="running")

    with client:
        test_resp = client.get("/api/dashboard?test_mode=1")
        prod_resp = client.get("/api/dashboard?test_mode=0")

    assert test_resp.status_code == 200
    assert prod_resp.status_code == 200
    test_payload = test_resp.json()
    prod_payload = prod_resp.json()

    assert any(b["class_name"] == "TestClass" and b["build_name"] == test_build_name for b in test_payload["builds"])
    assert all(b["class_name"] != "City" for b in test_payload["builds"])
    assert test_job_id in [j["id"] for j in test_payload["jobs"]]
    assert prod_job_id not in [j["id"] for j in test_payload["jobs"]]

    assert any(b["class_name"] == "City" and b["build_name"] == prod_build_name for b in prod_payload["builds"])
    assert all(b["class_name"] != "TestClass" for b in prod_payload["builds"])
    assert prod_job_id in [j["id"] for j in prod_payload["jobs"]]
    assert test_job_id not in [j["id"] for j in prod_payload["jobs"]]


def test_delete_stopped_jobs_keeps_only_active(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)

    running_id = web_main.db.insert_job({"class_name": "City"})
    web_main.db.update_job(running_id, status="running")
    queued_id = web_main.db.insert_job({"class_name": "City"})
    web_main.db.update_job(queued_id, status="queued")
    done_id = web_main.db.insert_job({"class_name": "City"})
    web_main.db.update_job(done_id, status="done")
    error_id = web_main.db.insert_job({"class_name": "City"})
    web_main.db.update_job(error_id, status="error")
    cancelled_id = web_main.db.insert_job({"class_name": "City"})
    web_main.db.update_job(cancelled_id, status="cancelled")

    with client:
        resp = client.post("/jobs/delete_stopped", follow_redirects=False)

    assert resp.status_code == 303
    assert web_main.db.get_job(running_id) is not None
    assert web_main.db.get_job(queued_id) is not None
    assert web_main.db.get_job(done_id) is None
    assert web_main.db.get_job(error_id) is None
    assert web_main.db.get_job(cancelled_id) is None


def test_dashboard_api_keeps_failed_job_visible_when_no_build_output(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)

    failed_no_build_job_id = web_main.db.insert_job({"class_name": "City", "parts_spec": "1"})
    web_main.db.update_job(
        failed_no_build_job_id,
        status="error",
        result_path=None,
        error_message="No alignments found (0); build skipped.",
    )

    done_with_build_job_id = web_main.db.insert_job({"class_name": "City", "parts_spec": "1"})
    fake_build = Path("data") / "City" / "beam_20260216_150000"
    _make_build_tree(fake_build)
    web_main.db.update_job(done_with_build_job_id, status="done", result_path=str(fake_build.resolve()))

    with client:
        resp = client.get("/api/dashboard")

    assert resp.status_code == 200
    payload = resp.json()
    assert failed_no_build_job_id in payload["visible_job_ids"]
    assert done_with_build_job_id not in payload["visible_job_ids"]


def test_dashboard_api_normalizes_done_skipped_build_to_error(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)

    reason = "No alignments found (0); build skipped."
    job_id = web_main.db.insert_job({"class_name": "City", "parts_spec": "1"})
    web_main.db.update_job(
        job_id,
        status="done",
        phase="build",
        progress_text=reason,
        error_message=None,
        result_path=None,
    )
    web_main.db.update_subjob_by_type(job_id, "align", status="done")
    web_main.db.update_subjob_by_type(
        job_id,
        "build",
        status="done",
        current_step="skipped",
        progress_text=reason,
    )

    with client:
        resp = client.get("/api/dashboard")

    assert resp.status_code == 200
    payload = resp.json()
    assert job_id in payload["visible_job_ids"]
    row = next(j for j in payload["jobs"] if j["id"] == job_id)
    assert row["status"] == "error"
    assert "no alignments found" in (row.get("error_message") or "").lower()


def test_dashboard_api_hides_done_jobs_with_missing_result_path(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)

    missing_path_job_id = web_main.db.insert_job({"class_name": "TestClass", "parts_spec": "all"})
    web_main.db.update_job(
        missing_path_job_id,
        status="done",
        result_path=str((Path("data") / "TestClass" / "beam_missing_12345").resolve()),
        progress_text="done",
    )

    with client:
        resp = client.get("/api/dashboard?test_mode=1")

    assert resp.status_code == 200
    payload = resp.json()
    assert missing_path_job_id not in payload["visible_job_ids"]


def test_class_parts_api_reports_downloaded_and_missing(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    class_dir = Path("Download") / "TestClass"
    class_dir.mkdir(parents=True, exist_ok=True)
    (class_dir / "part_0001.nq").write_text("<s> <p> <o> .\n", encoding="utf-8")
    (class_dir / "part_0003.nq").write_text("<s> <p> <o> .\n", encoding="utf-8")

    monkeypatch.setattr(web_main, "_discover_online_part_numbers", lambda class_name: ([1, 2, 3, 4], None))

    with client:
        resp = client.get("/api/class_parts/TestClass")

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["class_name"] == "TestClass"
    assert payload["downloaded_part_numbers"] == [1, 3]
    assert payload["not_downloaded_online_part_numbers"] == [2, 4]
    assert payload["downloaded_parts_count"] == 2
    assert payload["not_downloaded_online_parts_count"] == 2


def test_class_parts_api_infers_missing_from_catalog_count(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    web_main.db.upsert_wdc_classes(
        [
            {
                "class_name": "Movie",
                "num_parts": 13,
                "size_human": "24.9 GB",
            }
        ]
    )

    class_dir = Path("Download") / "Movie"
    class_dir.mkdir(parents=True, exist_ok=True)
    for i in range(12):
        (class_dir / f"part_{i:04d}.nq").write_text("<s> <p> <o> .\n", encoding="utf-8")
    (class_dir / "part_0999.nq").write_text("<s> <p> <o> .\n", encoding="utf-8")

    monkeypatch.setattr(web_main, "_discover_online_part_numbers", lambda class_name: (list(range(12)), None))

    with client:
        resp = client.get("/api/class_parts/Movie")

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["class_name"] == "Movie"
    assert payload["class_num_parts"] == 13
    assert payload["online_available_count"] == 13
    assert payload["online_available_ranges"] == "0-12"
    assert payload["downloaded_part_ranges"] == "0-11"
    assert payload["not_downloaded_online_part_ranges"] == "12"
    assert payload["not_downloaded_online_part_numbers"] == [12]
    assert payload["local_only_part_numbers"] == [999]

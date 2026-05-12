import importlib
import io
import json
import zipfile
from pathlib import Path

from bs4 import BeautifulSoup
from fastapi.testclient import TestClient


def _write_variant_files(variant_dir: Path, links_count: int = 2):
    variant_dir.mkdir(parents=True, exist_ok=True)
    ent_links_lines = ["wdc_iri\twikidata_uri\n"]
    for idx in range(max(0, int(links_count))):
        ent_links_lines.append(
            f"http://example.org/wdc/entity{idx + 1}\thttp://www.wikidata.org/entity/Q{515 + idx}\n"
        )
    (variant_dir / "ent_links").write_text("".join(ent_links_lines), encoding="utf-8")
    (variant_dir / "attr_triples_1").write_text("s\tp\to\n", encoding="utf-8")
    (variant_dir / "rel_triples_1").write_text("s\tp\to\n", encoding="utf-8")
    (variant_dir / "attr_triples_2").write_text("s\tp\to\n", encoding="utf-8")
    (variant_dir / "rel_triples_2").write_text("s\tp\to\n", encoding="utf-8")
    (variant_dir / "prop_stats_wdc.tsv").write_text(
        "property\tcount\nhttp://schema.org/name\t2\nhttp://schema.org/url\t2\n",
        encoding="utf-8",
    )
    (variant_dir / "prop_stats_wd.tsv").write_text(
        "property\tcount\nhttp://www.wikidata.org/prop/direct/P31\t2\n",
        encoding="utf-8",
    )


def _make_build_tree(build_root: Path, links_count: int = 2, class_name: str = "TestClass"):
    build_root.mkdir(parents=True, exist_ok=True)
    (build_root / "BUILD_DONE").write_text("2026-02-12 12:00:00", encoding="utf-8")
    (build_root / "BUILD_CONFIG.json").write_text(
        json.dumps(
            {
                "matching_mode": "property",
                "class_name": class_name,
                "parts_spec": "all",
                "wdc_predicate_pattern": "name",
                "wikidata_property": "rdfs:label",
                "wkd_class": "Q515",
                "ignore_chars": "spaces;-;.",
                "force_align": False,
                "use_local_only": True,
                "strict_duplicate_key_filter": False,
                "build_name": build_root.name,
                "result_path": str(build_root),
                "parts_count": 2,
                "parts_total_size_human": "2.0 MB",
                "parts_manifest": [
                    {"name": "part_0001.nq", "size_human": "1.0 MB"},
                    {"name": "part_0002.nq", "size_human": "1.0 MB"},
                ],
            }
        ),
        encoding="utf-8",
    )
    (build_root / "BUILD_STATS.json").write_text(
        json.dumps(
            {
                "class_name": class_name,
                "build_name": build_root.name,
                "links_by_source_after_filter": [
                    {"source": "via iata", "count": 582},
                    {"source": "via wikidata", "count": 2},
                ],
            }
        ),
        encoding="utf-8",
    )
    _write_variant_files(build_root / "with_link_code", links_count=links_count)
    _write_variant_files(build_root / "without_link_code", links_count=links_count)


def _write_link_explorer_variant(variant_dir: Path):
    variant_dir.mkdir(parents=True, exist_ok=True)
    (variant_dir / "ent_links").write_text(
        "wdc_iri\twikidata_uri\n"
        "http://example.org/wdc/entity1\thttp://www.wikidata.org/entity/Q100\n",
        encoding="utf-8",
    )
    (variant_dir / "attr_triples_1").write_text(
        'http://example.org/wdc/entity1\thttp://schema.org/name\t"Alpha City"\n'
        'http://example.org/wdc/entity1\thttp://schema.org/telephone\t"+33 1 23 45 67"\n',
        encoding="utf-8",
    )
    (variant_dir / "rel_triples_1").write_text(
        "http://example.org/wdc/entity1\thttp://schema.org/sameAs\thttp://www.wikidata.org/entity/Q100\n",
        encoding="utf-8",
    )
    (variant_dir / "attr_triples_2").write_text(
        'http://www.wikidata.org/entity/Q100\thttp://www.w3.org/2000/01/rdf-schema#label\t"Alpha City"\n'
        'http://www.wikidata.org/entity/Q100\thttp://www.wikidata.org/prop/direct/P1329\t"+331234567"\n',
        encoding="utf-8",
    )
    (variant_dir / "rel_triples_2").write_text(
        "http://www.wikidata.org/entity/Q100\thttp://www.wikidata.org/prop/direct/P31\thttp://www.wikidata.org/entity/Q515\n",
        encoding="utf-8",
    )
    (variant_dir / "prop_stats_wdc.tsv").write_text(
        "property\tcount\nhttp://schema.org/name\t1\nhttp://schema.org/telephone\t1\n",
        encoding="utf-8",
    )
    (variant_dir / "prop_stats_wd.tsv").write_text(
        "predicate\tcount\tlabel\tdescription\n"
        "http://www.w3.org/2000/01/rdf-schema#label\t1\tlabel\titem label\n"
        "http://www.wikidata.org/prop/direct/p1329\t1\tphone number\ttelephone number of subject\n"
        "http://www.wikidata.org/prop/direct/p31\t1\tinstance of\tthat class of which this subject is a particular example\n",
        encoding="utf-8",
    )


def _write_link_explorer_value_fallback_variant(variant_dir: Path):
    variant_dir.mkdir(parents=True, exist_ok=True)
    (variant_dir / "ent_links").write_text(
        "wdc_iri\twikidata_uri\n"
        "http://example.org/wdc/entity-snarc\thttp://www.wikidata.org/entity/Q145892\n",
        encoding="utf-8",
    )
    (variant_dir / "attr_triples_1").write_text(
        'http://example.org/wdc/entity-snarc\thttp://example.org/vocab/snarcRef\t"SNARC-7788"\n',
        encoding="utf-8",
    )
    (variant_dir / "rel_triples_1").write_text("", encoding="utf-8")
    (variant_dir / "attr_triples_2").write_text(
        'http://www.wikidata.org/entity/Q145892\thttp://www.wikidata.org/prop/direct/P12749\t"SNARC7788"\n',
        encoding="utf-8",
    )
    (variant_dir / "rel_triples_2").write_text("", encoding="utf-8")
    (variant_dir / "prop_stats_wdc.tsv").write_text(
        "predicate\tcount\tlabel\tdescription\n"
        "http://example.org/vocab/snarcRef\t1\tSNARC ref\tcustom id in source catalog\n",
        encoding="utf-8",
    )
    (variant_dir / "prop_stats_wd.tsv").write_text(
        "predicate\tcount\tlabel\tdescription\n"
        "http://www.wikidata.org/prop/direct/P12749\t1\tSNARC ID\tunique identifier for people, places and organisations represented in Welsh collections\n",
        encoding="utf-8",
    )


def _write_link_explorer_value_fallback_multivalue_variant(variant_dir: Path):
    variant_dir.mkdir(parents=True, exist_ok=True)
    (variant_dir / "ent_links").write_text(
        "wdc_iri\twikidata_uri\n"
        "http://example.org/wdc/entity-snarc-multi\thttp://www.wikidata.org/entity/Q145892\n",
        encoding="utf-8",
    )
    (variant_dir / "attr_triples_1").write_text(
        'http://example.org/wdc/entity-snarc-multi\thttp://example.org/vocab/snarcRef\t"SNARC-7788"\n',
        encoding="utf-8",
    )
    (variant_dir / "rel_triples_1").write_text("", encoding="utf-8")
    (variant_dir / "attr_triples_2").write_text(
        'http://www.wikidata.org/entity/Q145892\thttp://www.wikidata.org/prop/direct/P12749\t"SNARC7788"\n'
        'http://www.wikidata.org/entity/Q145892\thttp://www.wikidata.org/prop/direct/P12749\t"SNARC-0000"\n'
        'http://www.wikidata.org/entity/Q145892\thttp://www.wikidata.org/prop/direct/P12749\t"SNARC-9999"\n',
        encoding="utf-8",
    )
    (variant_dir / "rel_triples_2").write_text("", encoding="utf-8")
    (variant_dir / "prop_stats_wdc.tsv").write_text(
        "predicate\tcount\tlabel\tdescription\n"
        "http://example.org/vocab/snarcRef\t1\tSNARC ref\tcustom id in source catalog\n",
        encoding="utf-8",
    )
    (variant_dir / "prop_stats_wd.tsv").write_text(
        "predicate\tcount\tlabel\tdescription\n"
        "http://www.wikidata.org/prop/direct/P12749\t3\tSNARC ID\tunique identifier for people, places and organisations represented in Welsh collections\n",
        encoding="utf-8",
    )


def _write_link_explorer_weak_numeric_variant(variant_dir: Path):
    variant_dir.mkdir(parents=True, exist_ok=True)
    (variant_dir / "ent_links").write_text(
        "wdc_iri\twikidata_uri\n"
        "http://example.org/wdc/entity-num\thttp://www.wikidata.org/entity/Q999\n",
        encoding="utf-8",
    )
    (variant_dir / "attr_triples_1").write_text(
        'http://example.org/wdc/entity-num\thttp://schema.org/aggregateRating\t"6"\n',
        encoding="utf-8",
    )
    (variant_dir / "rel_triples_1").write_text("", encoding="utf-8")
    (variant_dir / "attr_triples_2").write_text(
        'http://www.wikidata.org/entity/Q999\thttp://schema.org/sitelinks\t"6"\n',
        encoding="utf-8",
    )
    (variant_dir / "rel_triples_2").write_text("", encoding="utf-8")
    (variant_dir / "prop_stats_wdc.tsv").write_text(
        "predicate\tcount\tlabel\tdescription\n"
        "http://schema.org/aggregateRating\t1\taggregate rating\taverage rating\n",
        encoding="utf-8",
    )
    (variant_dir / "prop_stats_wd.tsv").write_text(
        "predicate\tcount\tlabel\tdescription\n"
        "http://schema.org/sitelinks\t1\tsitelinks\tnumber of sitelinks\n",
        encoding="utf-8",
    )


def _client_with_test_classes(monkeypatch, test_wdc_classes):
    import beam.db as beam_db
    import webapp.main as web_main

    catalog_path = Path("wdc_classes_catalog.test.json")
    catalog_path.write_text(json.dumps(list(test_wdc_classes)), encoding="utf-8")
    monkeypatch.setenv("WDC_CLASSES_CATALOG_PATH", str(catalog_path))

    importlib.reload(web_main)
    monkeypatch.setattr(web_main.db, "DB_PATH", beam_db.DB_PATH)
    monkeypatch.setattr(web_main, "fetch_wdc_classes", lambda: list(test_wdc_classes))
    return TestClient(web_main.app), web_main


def test_index_populates_testclass_list(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    with client:
        resp = client.get("/")

    assert resp.status_code == 200
    soup = BeautifulSoup(resp.text, "html.parser")
    mode_select = soup.find("select", {"id": "matching-mode-select"})
    assert mode_select is not None
    mode_values = {opt.get("value", "") for opt in mode_select.find_all("option")}
    assert {"property", "sameas"} <= mode_values
    assert "identifier" not in mode_values
    pattern_list = soup.find("div", {"id": "wdc-pattern-list"})
    assert pattern_list is not None
    pattern_hidden_input = soup.find("input", {"id": "wdc-pattern-input"})
    assert pattern_hidden_input is not None
    assert pattern_hidden_input.get("type") == "hidden"
    pattern_add_btn = soup.find("button", {"id": "wdc-pattern-add-btn"})
    assert pattern_add_btn is not None
    assert soup.find("div", {"id": "ready-checklist"}) is not None
    assert soup.find("input", {"id": "history-search-input"}) is not None
    assert soup.find("select", {"id": "history-sort-select"}) is not None
    assert soup.find("form", {"id": "purge-low-links-form"}) is not None
    preset_select = soup.find("select", {"id": "preset-select"})
    assert preset_select is not None
    preset_values = {opt.get("value", "") for opt in preset_select.find_all("option")}
    assert "testclass_quick" not in preset_values
    class_select = soup.find("select", {"id": "class-name-select"})
    assert class_select is not None
    class_values = {opt.get("value", "") for opt in class_select.find_all("option")}
    assert "TestClass" not in class_values

    rows = [dict(r) for r in web_main.db.list_wdc_classes()]
    assert any(row["class_name"] == "TestClass" for row in rows)


def test_index_no_preset_defaults_parts_spec_to_all(monkeypatch, test_wdc_classes):
    client, _web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    with client:
        resp = client.get("/")

    assert resp.status_code == 200
    soup = BeautifulSoup(resp.text, "html.parser")
    parts_input = soup.find("input", {"id": "parts-spec-input"})
    assert parts_input is not None
    assert parts_input.get("value") == "all"


def test_index_keeps_selected_preset_in_dropdown(monkeypatch, test_wdc_classes):
    client, _web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    with client:
        resp = client.get("/?test_mode=1&preset=testclass_quick")

    assert resp.status_code == 200
    soup = BeautifulSoup(resp.text, "html.parser")
    select = soup.find("select", {"id": "preset-select"})
    assert select is not None
    selected = select.find("option", {"value": "testclass_quick"})
    assert selected is not None
    assert selected.has_attr("selected")


def test_index_test_mode_shows_test_presets_only(monkeypatch, test_wdc_classes):
    client, _web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    with client:
        resp = client.get("/?test_mode=1")

    assert resp.status_code == 200
    soup = BeautifulSoup(resp.text, "html.parser")
    preset_select = soup.find("select", {"id": "preset-select"})
    assert preset_select is not None
    preset_values = {opt.get("value", "") for opt in preset_select.find_all("option")}
    assert "testclass_quick" in preset_values
    assert "code_movie" not in preset_values
    class_select = soup.find("select", {"id": "class-name-select"})
    assert class_select is not None
    class_values = {opt.get("value", "") for opt in class_select.find_all("option")}
    assert "TestClass" in class_values


def test_index_normal_mode_hides_test_jobs_and_history(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)

    test_build_name = "beam_20260212_hidden_test"
    prod_build_name = "beam_20260212_visible_prod"
    test_build_root = Path("data") / "TestClass" / test_build_name
    prod_build_root = Path("data") / "City" / prod_build_name
    _make_build_tree(test_build_root, class_name="TestClass")
    _make_build_tree(prod_build_root, class_name="City")

    test_job_id = web_main.db.insert_job({"class_name": "TestClass", "parts_spec": "all"})
    web_main.db.update_job(test_job_id, status="error", error_message="test job")
    prod_job_id = web_main.db.insert_job({"class_name": "City", "parts_spec": "all"})
    web_main.db.update_job(prod_job_id, status="error", error_message="prod job")

    with client:
        resp = client.get("/")

    assert resp.status_code == 200
    soup = BeautifulSoup(resp.text, "html.parser")
    build_cards = soup.select("#build-list .build")
    assert build_cards
    build_classes = [c.get("data-class-name", "") for c in build_cards]
    assert all(not cls.lower().startswith("testclass") for cls in build_classes)
    assert any(cls == "City" for cls in build_classes)

    job_cards = soup.select(".job[data-job-id]")
    assert job_cards
    job_classes = [c.get("data-class-name", "") for c in job_cards]
    assert all(not cls.lower().startswith("testclass") for cls in job_classes)
    assert any(cls == "City" for cls in job_classes)
    class_select = soup.find("select", {"id": "class-name-select"})
    assert class_select is not None
    class_values = {opt.get("value", "") for opt in class_select.find_all("option")}
    assert "TestClass" not in class_values


def test_refresh_classes_updates_cache(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    with client:
        resp = client.get("/refresh_classes", follow_redirects=False)

    assert resp.status_code == 303
    rows = [dict(r) for r in web_main.db.list_wdc_classes()]
    assert {r["class_name"] for r in rows} >= {"TestClass", "TestClassTwo"}


def test_refresh_classes_failure_keeps_existing_cache(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    web_main.db.upsert_wdc_classes(
        [
            {"class_name": "CachedOnly", "num_parts": 7, "size_human": "7.0 MB"},
        ]
    )
    monkeypatch.setattr(web_main, "fetch_wdc_classes", lambda: (_ for _ in ()).throw(RuntimeError("offline")))

    with client:
        resp = client.get("/refresh_classes", follow_redirects=False)

    assert resp.status_code == 303
    location = resp.headers.get("location") or ""
    assert "form_error=" in location
    rows = [dict(r) for r in web_main.db.list_wdc_classes()]
    assert any(r["class_name"] == "CachedOnly" for r in rows)


def test_index_discovers_local_testclass_parts(monkeypatch):
    import beam.db as beam_db
    import webapp.main as web_main

    class_dir = Path("Download") / "TestClass"
    class_dir.mkdir(parents=True, exist_ok=True)
    (class_dir / "part_0001.nq").write_text(
        "<http://example.org/testclass/entity/paris> <http://schema.org/url> \"http://www.wikidata.org/entity/Q90\" .\n",
        encoding="utf-8",
    )
    (class_dir / "part_0002.nq").write_text(
        "<http://example.org/testclass/entity/berlin> <http://schema.org/url> \"http://www.wikidata.org/entity/Q64\" .\n",
        encoding="utf-8",
    )

    importlib.reload(web_main)
    monkeypatch.setattr(web_main.db, "DB_PATH", beam_db.DB_PATH)
    monkeypatch.setattr(web_main, "fetch_wdc_classes", lambda: [])
    client = TestClient(web_main.app)
    with client:
        resp = client.get("/?test_mode=1")

    assert resp.status_code == 200
    assert "TestClass" in resp.text
    rows = [dict(r) for r in web_main.db.list_wdc_classes()]
    test_row = [r for r in rows if r["class_name"] == "TestClass"]
    assert len(test_row) == 1
    assert test_row[0]["num_parts"] == 2


def test_create_job_persists_params(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    form = {
        "matching_mode": "property",
        "class_name": "  TestClass  ",
        "parts_spec": "  all  ",
        "wdc_predicate_pattern": "  name  ",
        "wdc_pattern_search_in": "value",
        "wikidata_property": "  P31  ",
        "wkd_class": "  Q515  ",
        "ignore_chars": "  spaces;-;.  ",
        "force_align": "",
        "use_local_only": "",
        "strict_duplicate_key_filter": "on",
    }
    with client:
        resp = client.post("/jobs", data=form, follow_redirects=False)

    assert resp.status_code == 303
    jobs = web_main.db.list_jobs(limit=1)
    assert len(jobs) == 1
    params = json.loads(jobs[0]["params_json"])
    assert params["class_name"] == "TestClass"
    assert params["parts_spec"] == "all"
    assert params["matching_mode"] == "property"
    assert params["wdc_predicate_pattern"] == "name"
    assert params["wdc_pattern_search_in"] == "value"
    assert params["wikidata_property"] == "P31"
    assert params["strict_duplicate_key_filter"] is True


def test_create_job_requires_wikidata_property_when_not_url_mode(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    form = {
        "matching_mode": "property",
        "class_name": "TestClass",
        "parts_spec": "all",
        "wdc_predicate_pattern": "name",
        "wikidata_property": "",
        "wkd_class": "Q515",
        "ignore_chars": "spaces;-;.",
    }
    with client:
        resp = client.post("/jobs", data=form, follow_redirects=False)

    assert resp.status_code == 303
    assert "form_error=" in (resp.headers.get("location") or "")
    jobs = web_main.db.list_jobs(limit=10)
    assert jobs == []


def test_create_job_url_mode_clears_wikidata_property(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    form = {
        "matching_mode": "sameas",
        "class_name": "TestClass",
        "parts_spec": "all",
        "wdc_predicate_pattern": "sameAs",
        "wikidata_property": "rdfs:label",
        "wkd_class": "Q486972",
        "ignore_chars": "spaces;-;.",
    }
    with client:
        resp = client.post("/jobs", data=form, follow_redirects=False)

    assert resp.status_code == 303
    jobs = web_main.db.list_jobs(limit=1)
    assert len(jobs) == 1
    params = json.loads(jobs[0]["params_json"])
    assert params["matching_mode"] == "sameas"
    assert params["wikidata_property"] == ""
    assert params["wkd_class"] == "Q486972"


def test_create_job_sameas_pattern_does_not_auto_enable_url_mode(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    form = {
        "matching_mode": "property",
        "class_name": "TestClass",
        "parts_spec": "all",
        "wdc_predicate_pattern": "sameAs",
        "wikidata_property": "rdfs:label",
        "wkd_class": "Q486972",
        "ignore_chars": "spaces;-;.",
    }
    with client:
        resp = client.post("/jobs", data=form, follow_redirects=False)

    assert resp.status_code == 303
    jobs = web_main.db.list_jobs(limit=1)
    assert len(jobs) == 1
    params = json.loads(jobs[0]["params_json"])
    assert params["matching_mode"] == "property"
    assert params["wikidata_property"] == "rdfs:label"
    assert params["ignore_chars"] == "spaces;-;."


def test_create_job_sameas_list_pattern_does_not_auto_enable_url_mode(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    form = {
        "matching_mode": "property",
        "class_name": "TestClass",
        "parts_spec": "all",
        "wdc_predicate_pattern": "sameAs, url",
        "wikidata_property": "rdfs:label",
        "wkd_class": "Q486972",
        "ignore_chars": "spaces;-;.",
    }
    with client:
        resp = client.post("/jobs", data=form, follow_redirects=False)

    assert resp.status_code == 303
    jobs = web_main.db.list_jobs(limit=1)
    assert len(jobs) == 1
    params = json.loads(jobs[0]["params_json"])
    assert params["matching_mode"] == "property"
    assert params["wikidata_property"] == "rdfs:label"
    assert params["ignore_chars"] == "spaces;-;."
    assert params["wdc_predicate_pattern"] == "sameAs, url"


def test_create_job_url_mode_requires_wikidata_class(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    form = {
        "matching_mode": "sameas",
        "class_name": "TestClass",
        "parts_spec": "all",
        "wdc_predicate_pattern": "sameAs",
        "wikidata_property": "rdfs:label",
        "wkd_class": "",
        "ignore_chars": "spaces;-;.",
    }
    with client:
        resp = client.post("/jobs", data=form, follow_redirects=False)

    assert resp.status_code == 303
    assert "form_error=" in (resp.headers.get("location") or "")
    assert web_main.db.list_jobs(limit=10) == []


def test_create_job_sameas_or_property_requires_both_class_and_property(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    form_missing_class = {
        "matching_mode": "sameas_or_property",
        "class_name": "TestClass",
        "parts_spec": "all",
        "wdc_predicate_pattern": "sameAs",
        "wikidata_property": "rdfs:label",
        "wkd_class": "",
        "ignore_chars": "spaces;-;.",
    }
    with client:
        resp = client.post("/jobs", data=form_missing_class, follow_redirects=False)
    assert resp.status_code == 303
    assert "form_error=" in (resp.headers.get("location") or "")
    assert web_main.db.list_jobs(limit=10) == []

    form_missing_prop = {
        "matching_mode": "sameas_or_property",
        "class_name": "TestClass",
        "parts_spec": "all",
        "wdc_predicate_pattern": "sameAs",
        "wikidata_property": "",
        "wkd_class": "Q486972",
        "ignore_chars": "spaces;-;.",
    }
    with client:
        resp = client.post("/jobs", data=form_missing_prop, follow_redirects=False)
    assert resp.status_code == 303
    assert "form_error=" in (resp.headers.get("location") or "")
    assert web_main.db.list_jobs(limit=10) == []


def test_create_job_sameas_or_property_persists_mode(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    form = {
        "matching_mode": "sameas_or_property",
        "class_name": "TestClass",
        "parts_spec": "all",
        "wdc_predicate_pattern": "sameAs",
        "wikidata_property": "rdfs:label",
        "wkd_class": "Q486972",
        "ignore_chars": "spaces;-;.",
    }
    with client:
        resp = client.post("/jobs", data=form, follow_redirects=False)

    assert resp.status_code == 303
    jobs = web_main.db.list_jobs(limit=1)
    assert len(jobs) == 1
    params = json.loads(jobs[0]["params_json"])
    assert params["matching_mode"] == "sameas_or_property"
    assert params["wikidata_property"] == "rdfs:label"
    assert params["wkd_class"] == "Q486972"


def test_create_job_accepts_property_mapping_rules_without_compat_fields(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    form = {
        "matching_mode": "property",
        "class_name": "TestClass",
        "parts_spec": "all",
        "wdc_predicate_pattern": "",
        "property_mapping_rules": "name => rdfs:label\niata => P238",
        "wikidata_property": "",
        "wkd_class": "Q515",
        "ignore_chars": "spaces;-;.",
    }
    with client:
        resp = client.post("/jobs", data=form, follow_redirects=False)

    assert resp.status_code == 303
    jobs = web_main.db.list_jobs(limit=1)
    assert len(jobs) == 1
    params = json.loads(jobs[0]["params_json"])
    assert params["property_mapping_rules"] == "name => rdfs:label\niata => P238"
    assert params["wikidata_property"] == ""


def test_create_job_accepts_property_mapping_rules_with_per_pair_search_modes(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    rules = 'name,iata => rdfs:label,P238 || {"search_in":["value","predicate"]}'
    form = {
        "matching_mode": "property",
        "class_name": "TestClass",
        "parts_spec": "all",
        "wdc_predicate_pattern": "",
        "property_mapping_rules": rules,
        "wikidata_property": "",
        "wkd_class": "Q515",
        "ignore_chars": "spaces;-;.",
    }
    with client:
        resp = client.post("/jobs", data=form, follow_redirects=False)

    assert resp.status_code == 303
    jobs = web_main.db.list_jobs(limit=1)
    assert len(jobs) == 1
    params = json.loads(jobs[0]["params_json"])
    assert params["property_mapping_rules"] == rules


def test_create_job_accepts_sameas_rule_mode_without_target_property(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    rules = 'sameAs => || {"mode":"sameas"}'
    form = {
        "matching_mode": "property",
        "class_name": "TestClass",
        "parts_spec": "all",
        "wdc_predicate_pattern": "",
        "property_mapping_rules": rules,
        "wikidata_property": "",
        "wkd_class": "Q515",
        "ignore_chars": "",
    }
    with client:
        resp = client.post("/jobs", data=form, follow_redirects=False)

    assert resp.status_code == 303
    jobs = web_main.db.list_jobs(limit=1)
    assert len(jobs) == 1
    params = json.loads(jobs[0]["params_json"])
    assert params["property_mapping_rules"] == rules
    assert params["wikidata_property"] == ""
    assert params["wkd_class"] == "Q515"


def test_create_job_rejects_invalid_property_mapping_rules(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    form = {
        "matching_mode": "property",
        "class_name": "TestClass",
        "parts_spec": "all",
        "wdc_predicate_pattern": "",
        "property_mapping_rules": "name,iata => rdfs:label",
        "wikidata_property": "",
        "wkd_class": "Q515",
        "ignore_chars": "spaces;-;.",
    }
    with client:
        resp = client.post("/jobs", data=form, follow_redirects=False)

    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "form_error=" in location
    assert web_main.db.list_jobs(limit=10) == []


def test_preflight_api_reports_matches(monkeypatch, test_wdc_classes):
    client, _web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    class_dir = Path("Download") / "TestClass"
    class_dir.mkdir(parents=True, exist_ok=True)
    lines = [
        '<http://example.org/e1> <http://schema.org/name> "Paris" .\n',
        '<http://example.org/e2> <http://schema.org/name> "Berlin" .\n',
        '<http://example.org/e3> <http://schema.org/name> "Madrid" .\n',
        '<http://example.org/e4> <http://schema.org/name> "Rome" .\n',
        '<http://example.org/e5> <http://schema.org/name> "Lisbon" .\n',
        '<http://example.org/e6> <http://schema.org/name> "Vienna" .\n',
    ]
    (class_dir / "part_0001.nq").write_text("".join(lines), encoding="utf-8")

    with client:
        resp = client.get(
            "/api/preflight",
            params={
                "class_name": "TestClass",
                "parts_spec": "all",
                "matching_mode": "property",
                "wdc_predicate_pattern": "name",
                "ignore_chars": "spaces;-;.",
                "use_local_only": "true",
                "scan_limit_lines": "10000",
            },
        )

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is True
    assert payload["matched_triples"] >= 6
    assert payload["distinct_values"] >= 6
    assert payload["risk"] == "low"
    assert payload["selected_files_count"] == 1


def test_preflight_api_supports_value_pattern_scope(monkeypatch, test_wdc_classes):
    client, _web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    class_dir = Path("Download") / "TestClass"
    class_dir.mkdir(parents=True, exist_ok=True)
    (class_dir / "part_0001.nq").write_text(
        "<http://example.org/e1> <http://schema.org/sameAs> <https://ror.org/04pf8en64> .\n"
        "<http://example.org/e2> <http://schema.org/name> \"Berlin\" .\n",
        encoding="utf-8",
    )

    with client:
        resp = client.get(
            "/api/preflight",
            params={
                "class_name": "TestClass",
                "parts_spec": "all",
                "matching_mode": "property",
                "wdc_predicate_pattern": "ror.org",
                "wdc_pattern_search_in": "value",
                "ignore_chars": "spaces;-;.",
                "use_local_only": "true",
                "scan_limit_lines": "10000",
            },
        )

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is True
    assert payload["matched_triples"] == 1


def test_create_job_does_not_block_high_risk_preflight(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    class_dir = Path("Download") / "TestClass"
    class_dir.mkdir(parents=True, exist_ok=True)
    (class_dir / "part_0001.nq").write_text(
        '<http://example.org/e1> <http://schema.org/url> "https://example.org/a" .\n',
        encoding="utf-8",
    )
    form = {
        "class_name": "TestClass",
        "parts_spec": "all",
        "wdc_predicate_pattern": "name",
        "wikidata_property": "rdfs:label",
        "wkd_class": "Q515",
        "ignore_chars": "spaces;-;.",
        "use_local_only": "on",
    }
    with client:
        resp = client.post("/jobs", data=form, follow_redirects=False)

    assert resp.status_code == 303
    jobs = web_main.db.list_jobs(limit=1)
    assert len(jobs) == 1


def test_builds_render_and_download(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    build_name = "beam_20260212_120000"
    build_root = Path("data") / "TestClass" / build_name
    _make_build_tree(build_root)

    with client:
        home = client.get("/?test_mode=1")
        zipped = client.get(f"/builds/TestClass/{build_name}/download")

    assert home.status_code == 200
    assert "Entity Links" in home.text
    assert "Parts Used" in home.text
    assert "part_0001.nq" in home.text

    assert zipped.status_code == 200
    assert zipped.headers["content-type"].startswith("application/zip")


def test_download_selected_builds_groups_files_under_class_subfolders(monkeypatch, test_wdc_classes):
    client, _web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    airport_build = "beam_20260212_airport"
    book_build = "beam_20260212_book"
    _make_build_tree(Path("data") / "Airport" / airport_build, class_name="Airport")
    _make_build_tree(Path("data") / "Book" / book_build, class_name="Book")

    selected = json.dumps(
        [
            {"class_name": "Airport", "build_name": airport_build},
            {"class_name": "Book", "build_name": book_build},
        ]
    )

    with client:
        resp = client.post("/builds/download_selected", data={"selected_builds": selected})

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/zip")

    with zipfile.ZipFile(io.BytesIO(resp.content), "r") as zf:
        names = zf.namelist()
    lower_names = [name.lower() for name in names]
    assert any(name.startswith("airport/") for name in lower_names)
    assert any(name.startswith("book/") for name in lower_names)


def test_history_card_exposes_build_detail_url(monkeypatch, test_wdc_classes):
    client, _web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    build_name = "beam_20260212_120010"
    build_root = Path("data") / "TestClass" / build_name
    _make_build_tree(build_root)

    with client:
        resp = client.get("/?test_mode=1")

    assert resp.status_code == 200
    soup = BeautifulSoup(resp.text, "html.parser")
    card = soup.select_one("#build-list .build[data-class-name='TestClass']")
    assert card is not None
    assert card.get("data-build-name") == build_name
    assert card.get("data-build-detail-url") == f"/builds/TestClass/{build_name}?test_mode=1"

    open_btn = card.select_one(".js-toggle-build")
    assert open_btn is not None
    assert open_btn.get("title") == "Open details"


def test_build_detail_page_renders_existing_build(monkeypatch, test_wdc_classes):
    client, _web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    build_name = "beam_20260212_120011"
    build_root = Path("data") / "TestClass" / build_name
    _make_build_tree(build_root)

    with client:
        resp = client.get(f"/builds/TestClass/{build_name}?test_mode=1")

    assert resp.status_code == 200
    assert "<title>Build Detail</title>" in resp.text
    assert "Back to dashboard" in resp.text
    assert "/?test_mode=1" in resp.text
    assert "Variant: with_link_code" in resp.text
    assert "Parts Used" in resp.text


def test_tutorial_page_renders_and_is_linked(monkeypatch, test_wdc_classes):
    client, _web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    with client:
        home = client.get("/?test_mode=1")
        tutorial = client.get("/tutorial?test_mode=1")

    assert home.status_code == 200
    assert 'href="/tutorial?test_mode=1"' in home.text
    assert tutorial.status_code == 200
    assert "<title>BEAM Tutorial</title>" in tutorial.text
    assert "Source:" in tutorial.text


def test_tutorial_page_missing_source_shows_fallback(monkeypatch, test_wdc_classes):
    client, web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    monkeypatch.setattr(web_main, "TUTORIAL_MD_PATH", Path("docs/user/_missing_tutorial.md"))

    with client:
        tutorial = client.get("/tutorial?test_mode=1")

    assert tutorial.status_code == 200
    assert "Tutorial source not found" in tutorial.text


def test_build_detail_page_missing_build_redirects_to_index(monkeypatch, test_wdc_classes):
    client, _web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)

    with client:
        resp = client.get("/builds/TestClass/beam_missing?test_mode=1", follow_redirects=False)

    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert location.startswith("/?test_mode=1&")
    assert "form_error=Build+not+found." in location


def test_build_without_done_marker_is_hidden_and_inaccessible(monkeypatch, test_wdc_classes):
    client, _web_main = _client_with_test_classes(monkeypatch, test_wdc_classes)
    build_name = "beam_20260212_partial"
    build_root = Path("data") / "TestClass" / build_name
    build_root.mkdir(parents=True, exist_ok=True)
    (build_root / "BUILD_CONFIG.json").write_text(
        json.dumps({"class_name": "TestClass", "build_name": build_name}),
        encoding="utf-8",
    )
    _write_link_explorer_variant(build_root / "with_link_code")

    with client:
        home = client.get("/?test_mode=1")
        detail = client.get(f"/builds/TestClass/{build_name}?test_mode=1", follow_redirects=False)
        links_page = client.get(
            f"/builds/TestClass/{build_name}/links?test_mode=1&variant=with_link_code",
            follow_redirects=False,
        )
        links_api = client.get(f"/api/builds/TestClass/{build_name}/links?variant=with_link_code")

    assert home.status_code == 200
    assert build_name not in home.text
    assert detail.status_code == 303
    assert links_page.status_code == 303
    assert links_api.status_code == 404



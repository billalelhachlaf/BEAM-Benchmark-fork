

def test_generate_benchmark_wikidata_mode_fails_when_class_filter_has_no_hits(monkeypatch):
    class_name = "TestClassNoWdClassHits"
    _write_test_parts(class_name)

    monkeypatch.setattr(pipeline.align, "set_normalization", lambda enabled: None)
    monkeypatch.setattr(pipeline.align, "set_cancel_checker", lambda checker: None)
    monkeypatch.setattr(
        pipeline.align,
        "extract_unique_iris_from_files",
        lambda *args, **kwargs: (
            {
                "httpwwwwikidataorgentityq515": [
                    ("https://www.wikidata.org/wiki/Q515", "http://example.org/wdc/entity1")
                ]
            },
            1,
        ),
    )
    monkeypatch.setattr(pipeline.align, "fetch_wikidata_values", lambda *args, **kwargs: {})

    with pytest.raises(
        pipeline.PipelineError,
        match="No Wikidata entities matched class filter",
    ):
        pipeline.generate_benchmark(
            {
                "class_name": class_name,
                "parts_spec": "all",
                "wdc_predicate_pattern": "sameAs",
                "wikidata_property": None,
                "wkd_class": "Q110879422",
                "wdc_value_is_wikidata": True,
                "use_local_only": True,
                "force_align": True,
            },
            workers=1,
        )


def test_generate_benchmark_sameas_non_wikidata_uses_value_candidates(monkeypatch):
    class_name = "TestClassSameAsNonWikidataCandidates"
    _write_test_parts(class_name)

    monkeypatch.setattr(pipeline.align, "set_normalization", lambda enabled: None)
    monkeypatch.setattr(pipeline.align, "set_cancel_checker", lambda checker: None)
    monkeypatch.setattr(
        pipeline.align,
        "extract_unique_iris_from_files",
        lambda *args, **kwargs: (
            {
                "httpwwwwikidataorgentityq17146713": [
                    ("https://www.wikidata.org/wiki/Q17146713", "http://example.org/wdc/entity1")
                ]
            },
            1,
        ),
    )

    captured = {}

    def _fetch_target_values(**kwargs):
        captured.update(kwargs)
        return {}

    monkeypatch.setattr(pipeline.align, "fetch_target_values", _fetch_target_values)
    monkeypatch.setattr(
        pipeline.align,
        "fetch_wikidata_values",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not query Wikidata in non-wikidata endpoint mode")),
    )

    with pytest.raises(
        pipeline.PipelineError,
        match="No target entities matched class filter",
    ):
        pipeline.generate_benchmark(
            {
                "class_name": class_name,
                "parts_spec": "all",
                "matching_mode": "sameas",
                "wdc_predicate_pattern": "sameAs",
                "target_endpoint": "dbpedia",
                "target_class": "dbo:Museum",
                "use_local_only": True,
                "force_align": True,
            },
            workers=1,
        )

    assert captured["target_property"] == "owl:sameAs"
    assert captured["entity_iris"] is None
    vals = set(captured["value_candidates"])
    assert "http://www.wikidata.org/entity/Q17146713" in vals
    assert "https://www.wikidata.org/entity/Q17146713" in vals


def test_generate_benchmark_sameas_or_property_combines_matches(monkeypatch):
    class_name = "TestClassSameAsOrProperty"
    _write_test_parts(class_name)

    monkeypatch.setattr(pipeline.align, "set_normalization", lambda enabled: None)
    monkeypatch.setattr(pipeline.align, "set_extra_strip_chars", lambda chars: None)
    monkeypatch.setattr(pipeline.align, "parse_strip_list", lambda text: {" ", "-", "."})
    monkeypatch.setattr(pipeline.align, "set_cancel_checker", lambda checker: None)

    def _extract(*_args, **kwargs):
        if kwargs.get("wdc_value_is_wd_iri"):
            return (
                {"k_same": [("https://www.wikidata.org/wiki/Q1", "http://example.org/wdc/entity_same")]},
                1,
            )
        return (
            {"k_prop": [("ABC", "http://example.org/wdc/entity_prop")]},
            1,
        )

    monkeypatch.setattr(pipeline.align, "extract_unique_iris_from_files", _extract)

    def _fetch_wikidata_values(prop=None, *_args, **kwargs):
        if prop is None and kwargs.get("entity_iris"):
            return {"k_same": [("http://www.wikidata.org/entity/Q1", "http://www.wikidata.org/entity/Q1")]}
        if prop:
            return {"k_prop": [("ABC", "http://www.wikidata.org/entity/Q2")]}
        return {}

    monkeypatch.setattr(pipeline.align, "fetch_wikidata_values", _fetch_wikidata_values)

    def _fuzzy_link(local_wdc_map, local_wd_map, **_kwargs):
        if "k_same" in local_wdc_map and "k_same" in local_wd_map:
            return (
                [
                    {
                        "wdc_iri": "http://example.org/wdc/entity_same",
                        "wikidata_uri": "http://www.wikidata.org/entity/Q1",
                        "wdc_value": "https://www.wikidata.org/wiki/Q1",
                        "wiki_value": "http://www.wikidata.org/entity/Q1",
                        "method": "exact",
                    }
                ],
                {"https://www.wikidata.org/wiki/Q1"},
            )
        if "k_prop" in local_wdc_map and "k_prop" in local_wd_map:
            return (
                [
                    {
                        "wdc_iri": "http://example.org/wdc/entity_prop",
                        "wikidata_uri": "http://www.wikidata.org/entity/Q2",
                        "wdc_value": "ABC",
                        "wiki_value": "ABC",
                        "method": "exact",
                    }
                ],
                {"ABC"},
            )
        return ([], set())

    monkeypatch.setattr(pipeline.align, "fuzzy_link", _fuzzy_link)

    def export_results(matches, *args, **_kwargs):
        output_dir = args[3]
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        lines = ["wdc_iri\twikidata_uri\twdc_value\twiki_value\tmethod\tmin_len\n"]
        for m in matches:
            lines.append(
                f"{m['wdc_iri']}\t{m['wikidata_uri']}\t{m['wdc_value']}\t{m['wiki_value']}\t{m['method']}\t1\n"
            )
        (out / "wdc_wikidata_links.tsv").write_text("".join(lines), encoding="utf-8")

    monkeypatch.setattr(pipeline.align, "export_results", export_results)
    monkeypatch.setattr(
        pipeline.build,
        "read_links",
        lambda *args, **kwargs: (
            ["http://example.org/wdc/entity_same", "http://example.org/wdc/entity_prop"],
            ["http://www.wikidata.org/entity/Q1", "http://www.wikidata.org/entity/Q2"],
            ["https://www.wikidata.org/wiki/Q1", "ABC"],
            ["http://www.wikidata.org/entity/Q1", "ABC"],
        ),
    )

    def run_pipeline_stub(args, wdc_entities, wd_entities_raw, wdc_values, wd_values, out_dir, *rest, **kwargs):
        out = Path(out_dir)
        out.mkdir(parents=True, exist_ok=True)
        (out / "ent_links").write_text(
            "wdc_iri\twikidata_uri\n"
            "http://example.org/wdc/entity_same\thttp://www.wikidata.org/entity/Q1\n"
            "http://example.org/wdc/entity_prop\thttp://www.wikidata.org/entity/Q2\n",
            encoding="utf-8",
        )
        (out / "attr_triples_1").write_text("s\tp\to\n", encoding="utf-8")
        (out / "rel_triples_1").write_text("s\tp\to\n", encoding="utf-8")
        (out / "attr_triples_2").write_text("s\tp\to\n", encoding="utf-8")
        (out / "rel_triples_2").write_text("s\tp\to\n", encoding="utf-8")
        (out / "prop_stats_wdc.tsv").write_text("property\tcount\nname\t2\n", encoding="utf-8")
        (out / "prop_stats_wd.tsv").write_text("property\tcount\nP31\t2\n", encoding="utf-8")

    monkeypatch.setattr(pipeline.build, "run_pipeline", run_pipeline_stub)

    result = pipeline.generate_benchmark(
        {
            "class_name": class_name,
            "parts_spec": "all",
            "matching_mode": "sameas_or_property",
            "wdc_predicate_pattern": "sameAs",
            "wdc_pattern_search_in": "value",
            "target_property": "P238",
            "target_class": "Q5",
            "ignore_chars": "spaces;-;.",
            "use_local_only": True,
            "force_align": True,
        },
        workers=1,
    )

    links_tsv = Path(result["links_tsv"])
    rows = [ln for ln in links_tsv.read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert len(rows) == 3


def test_generate_benchmark_with_property_mapping_rules(monkeypatch):
    class_name = "TestClassRules"
    _write_test_parts(class_name)

    monkeypatch.setattr(pipeline.align, "set_normalization", lambda enabled: None)
    monkeypatch.setattr(pipeline.align, "set_extra_strip_chars", lambda chars: None)
    monkeypatch.setattr(pipeline.align, "parse_strip_list", lambda text: {" ", "-", "."})
    monkeypatch.setattr(pipeline.align, "set_cancel_checker", lambda checker: None)

    extract_calls = []
    fetch_calls = []

    def extract_unique_iris_from_files(files, pattern, **kwargs):
        extract_calls.append(pattern)
        if pattern == "name":
            return ({"alpha node": [("Alpha Node", "http://example.org/wdc/entity1")]}, 1)
        if pattern == "iata":
            return ({"abc": [("ABC", "http://example.org/wdc/entity1")]}, 1)
        return ({}, 0)

    monkeypatch.setattr(pipeline.align, "extract_unique_iris_from_files", extract_unique_iris_from_files)

    def fetch_wikidata_values(wikidata_property, wkd_class, wkd_prop_class):
        fetch_calls.append(wikidata_property)
        if wikidata_property == "rdfs:label":
            return {"alpha node": [("Alpha Node", "http://www.wikidata.org/entity/Q1001")]}
        if wikidata_property == "P238":
            return {"abc": [("ABC", "http://www.wikidata.org/entity/Q1001")]}
        return {}

    monkeypatch.setattr(pipeline.align, "fetch_wikidata_values", fetch_wikidata_values)

    def fuzzy_link(wdc_map, wikidata_map, **kwargs):
        if "alpha node" in wdc_map:
            return (
                [
                    {
                        "wdc_iri": "http://example.org/wdc/entity1",
                        "wikidata_uri": "http://www.wikidata.org/entity/Q1001",
                        "wdc_value": "Alpha Node",
                        "wiki_value": "Alpha Node",
                        "method": "exact",
                    }
                ],
                {"alpha node"},
            )
        if "abc" in wdc_map:
            return (
                [
                    {
                        "wdc_iri": "http://example.org/wdc/entity1",
                        "wikidata_uri": "http://www.wikidata.org/entity/Q1001",
                        "wdc_value": "ABC",
                        "wiki_value": "ABC",
                        "method": "exact",
                    }
                ],
                {"abc"},
            )
        return ([], set())

    monkeypatch.setattr(pipeline.align, "fuzzy_link", fuzzy_link)

    def export_results(matches, wdc_values_matched, wdc_map, wikidata_map, output_dir, **kwargs):
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        with (out / "wdc_wikidata_links.tsv").open("w", encoding="utf-8") as f:
            f.write("wdc_iri\twikidata_uri\twdc_value\twiki_value\tmethod\tmin_len\n")
            for m in matches:
                f.write(
                    f"{m['wdc_iri']}\t{m['wikidata_uri']}\t{m['wdc_value']}\t{m['wiki_value']}\t{m['method']}\t1\n"
                )

    monkeypatch.setattr(pipeline.align, "export_results", export_results)

    monkeypatch.setattr(
        pipeline.build,
        "read_links",
        lambda *args, **kwargs: (
            ["http://example.org/wdc/entity1"],
            ["http://www.wikidata.org/entity/Q1001"],
            ["Alpha Node"],
            ["Alpha Node"],
        ),
    )

    def run_pipeline_stub(args, wdc_entities, wd_entities_raw, wdc_values, wd_values, out_dir, *rest, **kwargs):
        out = Path(out_dir)
        out.mkdir(parents=True, exist_ok=True)
        (out / "ent_links").write_text(
            "wdc_iri\twikidata_uri\n"
            "http://example.org/wdc/entity1\thttp://www.wikidata.org/entity/Q1001\n",
            encoding="utf-8",
        )
        (out / "attr_triples_1").write_text("s\tp\to\n", encoding="utf-8")
        (out / "rel_triples_1").write_text("s\tp\to\n", encoding="utf-8")
        (out / "attr_triples_2").write_text("s\tp\to\n", encoding="utf-8")
        (out / "rel_triples_2").write_text("s\tp\to\n", encoding="utf-8")
        (out / "prop_stats_wdc.tsv").write_text("property\tcount\nname\t2\n", encoding="utf-8")
        (out / "prop_stats_wd.tsv").write_text("property\tcount\nP31\t2\n", encoding="utf-8")

    monkeypatch.setattr(pipeline.build, "run_pipeline", run_pipeline_stub)

    result = pipeline.generate_benchmark(
        {
            "class_name": class_name,
            "parts_spec": "all",
            "matching_mode": "property",
            "wdc_predicate_pattern": "",
            "property_mapping_rules": "name => rdfs:label\niata => P238",
            "wikidata_property": "",
            "wkd_class": "Q1248784",
            "ignore_chars": "spaces;-;.",
            "use_local_only": True,
            "force_align": True,
        },
        workers=1,
    )

    assert result["class_name"] == class_name
    assert extract_calls == ["name", "iata"]
    assert fetch_calls == ["rdfs:label", "P238"]

    out_dir = Path(result["out_dir"])
    cfg = json.loads((out_dir / "BUILD_CONFIG.json").read_text(encoding="utf-8"))
    assert cfg["property_mapping_rules"] == "name => rdfs:label\niata => P238"


def test_generate_benchmark_with_mixed_rule_modes(monkeypatch):
    class_name = "TestClassMixedRuleModes"
    _write_test_parts(class_name)

    monkeypatch.setattr(pipeline.align, "set_normalization", lambda enabled: None)
    monkeypatch.setattr(pipeline.align, "set_extra_strip_chars", lambda chars: None)
    monkeypatch.setattr(pipeline.align, "parse_strip_list", lambda text: {" ", "-", "."})
    monkeypatch.setattr(pipeline.align, "set_cancel_checker", lambda checker: None)

    def extract_with_cache_stub(
        work_dir,
        class_name,
        parts_spec,
        decompressed_files,
        pattern,
        search_in,
        wdc_value_is_wd_iri,
        **kwargs,
    ):
        if pattern == "sameAsId" and wdc_value_is_wd_iri:
            return (
                {"q1": [("https://www.wikidata.org/wiki/Q1", "http://example.org/wdc/entity_same")]},
                1,
                False,
            )
        if pattern == "name" and not wdc_value_is_wd_iri:
            return (
                {"alpha node": [("Alpha Node", "http://example.org/wdc/entity_prop")]},
                1,
                False,
            )
        return ({}, 0, False)

    monkeypatch.setattr(pipeline, "_extract_wdc_values_with_cache", extract_with_cache_stub)

    def fetch_wikidata_values_stub(wikidata_property=None, wkd_class=None, wkd_prop_class=None, entity_iris=None, **kwargs):
        if entity_iris:
            return {"q1": [("http://www.wikidata.org/entity/Q1", "http://www.wikidata.org/entity/Q1")]}
        if wikidata_property == "rdfs:label":
            return {"alpha node": [("Alpha Node", "http://www.wikidata.org/entity/Q2")]}
        return {}

    monkeypatch.setattr(pipeline.align, "fetch_wikidata_values", fetch_wikidata_values_stub)

    def fuzzy_link_stub(wdc_map, wikidata_map, **kwargs):
        if "q1" in wdc_map:
            return (
                [
                    {
                        "wdc_iri": "http://example.org/wdc/entity_same",
                        "wikidata_uri": "http://www.wikidata.org/entity/Q1",
                        "wdc_value": "https://www.wikidata.org/wiki/Q1",
                        "wiki_value": "http://www.wikidata.org/entity/Q1",
                        "method": "exact",
                    }
                ],
                {"q1"},
            )
        if "alpha node" in wdc_map:
            return (
                [
                    {
                        "wdc_iri": "http://example.org/wdc/entity_prop",
                        "wikidata_uri": "http://www.wikidata.org/entity/Q2",
                        "wdc_value": "Alpha Node",
                        "wiki_value": "Alpha Node",
                        "method": "exact",
                    }
                ],
                {"alpha node"},
            )
        return ([], set())

    monkeypatch.setattr(pipeline.align, "fuzzy_link", fuzzy_link_stub)

    def export_results_stub(matches, wdc_values_matched, wdc_map, wikidata_map, output_dir, **kwargs):
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        with (out / "wdc_wikidata_links.tsv").open("w", encoding="utf-8") as f:
            f.write("wdc_iri\twikidata_uri\twdc_value\twiki_value\tmethod\tmin_len\n")
            for m in matches:
                f.write(
                    f"{m['wdc_iri']}\t{m['wikidata_uri']}\t{m['wdc_value']}\t{m['wiki_value']}\t{m['method']}\t1\n"
                )

    monkeypatch.setattr(pipeline.align, "export_results", export_results_stub)

    monkeypatch.setattr(
        pipeline.build,
        "read_links",
        lambda *args, **kwargs: (
            ["http://example.org/wdc/entity_same", "http://example.org/wdc/entity_prop"],
            ["http://www.wikidata.org/entity/Q1", "http://www.wikidata.org/entity/Q2"],
            ["Q1", "Alpha Node"],
            ["Q1", "Alpha Node"],
        ),
    )

    def run_pipeline_stub(args, wdc_entities, wd_entities_raw, wdc_values, wd_values, out_dir, *rest, **kwargs):
        out = Path(out_dir)
        out.mkdir(parents=True, exist_ok=True)
        (out / "ent_links").write_text(
            "wdc_iri\twikidata_uri\n"
            "http://example.org/wdc/entity_same\thttp://www.wikidata.org/entity/Q1\n"
            "http://example.org/wdc/entity_prop\thttp://www.wikidata.org/entity/Q2\n",
            encoding="utf-8",
        )
        (out / "attr_triples_1").write_text("s\tp\to\n", encoding="utf-8")
        (out / "rel_triples_1").write_text("s\tp\to\n", encoding="utf-8")
        (out / "attr_triples_2").write_text("s\tp\to\n", encoding="utf-8")
        (out / "rel_triples_2").write_text("s\tp\to\n", encoding="utf-8")
        (out / "prop_stats_wdc.tsv").write_text("property\tcount\nname\t2\n", encoding="utf-8")
        (out / "prop_stats_wd.tsv").write_text("property\tcount\nP31\t2\n", encoding="utf-8")

    monkeypatch.setattr(pipeline.build, "run_pipeline", run_pipeline_stub)

    result = pipeline.generate_benchmark(
        {
            "class_name": class_name,
            "parts_spec": "all",
            "matching_mode": "property",
            "wdc_predicate_pattern": "",
            "property_mapping_rules": 'sameAsId => || {"mode":"sameas"}\nname => rdfs:label',
            "wikidata_property": "",
            "wkd_class": "Q5",
            "ignore_chars": "spaces;-;.",
            "use_local_only": True,
            "force_align": True,
        },
        workers=1,
    )

    links_tsv = Path(result["links_tsv"])
    rows = [ln for ln in links_tsv.read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert len(rows) == 3


def test_parse_property_mapping_rules_accepts_sameas_mode_without_target_property():
    rows = pipeline._parse_property_mapping_rules('sameAs => || {"mode":"sameas"}')
    assert len(rows) == 1
    assert rows[0]["mode"] == "sameas"
    assert rows[0]["pairs"] == [("sameAs", "")]


@pytest.mark.parametrize(
    "class_name,property_mapping_rules,extract_payloads,fetch_payloads,expected_extract_calls,expected_fetch_calls",
    [
        (
            "TestSinglePropNameRule",
            "name => rdfs:label",
            {
                "name": ({"alpha node": [("Alpha Node", "http://example.org/wdc/entity1")]}, 1),
            },
            {
                "rdfs:label": {"alpha node": [("Alpha Node", "http://www.wikidata.org/entity/Q1001")]},
            },
            ["name"],
            ["rdfs:label"],
        ),
        (
            "TestSinglePropCodeRule",
            "code => P528",
            {
                "code": ({"x-001": [("X-001", "http://example.org/wdc/entity2")]}, 1),
            },
            {
                "P528": {"x-001": [("X-001", "http://www.wikidata.org/entity/Q2002")]},
            },
            ["code"],
            ["P528"],
        ),
    ],
)
def test_generate_benchmark_property_mapping_single_prop_classes(
    monkeypatch,
    class_name,
    property_mapping_rules,
    extract_payloads,
    fetch_payloads,
    expected_extract_calls,
    expected_fetch_calls,
):
    _write_test_parts(class_name)

    monkeypatch.setattr(pipeline.align, "set_normalization", lambda enabled: None)
    monkeypatch.setattr(pipeline.align, "set_extra_strip_chars", lambda chars: None)
    monkeypatch.setattr(pipeline.align, "parse_strip_list", lambda text: {" ", "-", "."})
    monkeypatch.setattr(pipeline.align, "set_cancel_checker", lambda checker: None)

    extract_calls = []
    fetch_calls = []

    def extract_unique_iris_from_files(files, pattern, **kwargs):
        extract_calls.append(pattern)
        return extract_payloads.get(pattern, ({}, 0))

    monkeypatch.setattr(pipeline.align, "extract_unique_iris_from_files", extract_unique_iris_from_files)

    def fetch_wikidata_values(wikidata_property, wkd_class, wkd_prop_class):
        fetch_calls.append(wikidata_property)
        return fetch_payloads.get(wikidata_property, {})

    monkeypatch.setattr(pipeline.align, "fetch_wikidata_values", fetch_wikidata_values)

    def fuzzy_link(wdc_map, wikidata_map, **kwargs):
        out = []
        matched = set()
        for norm, wdc_entries in (wdc_map or {}).items():
            wd_entries = (wikidata_map or {}).get(norm) or []
            if not wd_entries:
                continue
            for wdc_val, wdc_iri in wdc_entries:
                wd_val, wd_iri = wd_entries[0]
                out.append(
                    {
                        "wdc_iri": wdc_iri,
                        "wikidata_uri": wd_iri,
                        "wdc_value": wdc_val,
                        "wiki_value": wd_val,
                        "method": "exact",
                    }
                )
                matched.add(norm)
        return (out, matched)

    monkeypatch.setattr(pipeline.align, "fuzzy_link", fuzzy_link)

    def export_results(matches, wdc_values_matched, wdc_map, wikidata_map, output_dir, **kwargs):
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        with (out / "wdc_wikidata_links.tsv").open("w", encoding="utf-8") as f:
            f.write("wdc_iri\twikidata_uri\twdc_value\twiki_value\tmethod\tmin_len\n")
            for m in matches:
                f.write(
                    f"{m['wdc_iri']}\t{m['wikidata_uri']}\t{m['wdc_value']}\t{m['wiki_value']}\t{m['method']}\t1\n"
                )

    monkeypatch.setattr(pipeline.align, "export_results", export_results)

    monkeypatch.setattr(
        pipeline.build,
        "read_links",
        lambda *args, **kwargs: (
            ["http://example.org/wdc/entity1"],
            ["http://www.wikidata.org/entity/Q1001"],
            ["Alpha Node"],
            ["Alpha Node"],
        ),
    )

    def run_pipeline_stub(args, wdc_entities, wd_entities_raw, wdc_values, wd_values, out_dir, *rest, **kwargs):
        out = Path(out_dir)
        out.mkdir(parents=True, exist_ok=True)
        (out / "ent_links").write_text(
            "wdc_iri\twikidata_uri\n"
            "http://example.org/wdc/entity1\thttp://www.wikidata.org/entity/Q1001\n",
            encoding="utf-8",
        )
        (out / "attr_triples_1").write_text("s\tp\to\n", encoding="utf-8")
        (out / "rel_triples_1").write_text("s\tp\to\n", encoding="utf-8")
        (out / "attr_triples_2").write_text("s\tp\to\n", encoding="utf-8")
        (out / "rel_triples_2").write_text("s\tp\to\n", encoding="utf-8")
        (out / "prop_stats_wdc.tsv").write_text("property\tcount\nname\t2\n", encoding="utf-8")
        (out / "prop_stats_wd.tsv").write_text("property\tcount\nP31\t2\n", encoding="utf-8")

    monkeypatch.setattr(pipeline.build, "run_pipeline", run_pipeline_stub)

    result = pipeline.generate_benchmark(
        {
            "class_name": class_name,
            "parts_spec": "all",
            "matching_mode": "property",
            "wdc_predicate_pattern": "",
            "property_mapping_rules": property_mapping_rules,
            "wikidata_property": "",
            "wkd_class": "Q1248784",
            "ignore_chars": "spaces;-;.",
            "use_local_only": True,
            "force_align": True,
        },
        workers=1,
    )

    assert result["class_name"] == class_name
    assert extract_calls == expected_extract_calls
    assert fetch_calls == expected_fetch_calls
    out_dir = Path(result["out_dir"])
    cfg = json.loads((out_dir / "BUILD_CONFIG.json").read_text(encoding="utf-8"))
    assert cfg["property_mapping_rules"] == property_mapping_rules


def test_generate_benchmark_property_mapping_multi_prop_with_per_pair_normalization(monkeypatch):
    class_name = "TestMultiPropPairNorms"
    _write_test_parts(class_name)

    normalize_enabled_calls = []
    normalize_specs = []
    extract_calls = []
    fetch_calls = []

    monkeypatch.setattr(
        pipeline.align,
        "set_normalization",
        lambda enabled: normalize_enabled_calls.append(bool(enabled)),
    )
    monkeypatch.setattr(pipeline.align, "set_extra_strip_chars", lambda chars: None)

    def parse_strip_list(text):
        spec = str(text or "")
        normalize_specs.append(spec)
        return {" "}

    monkeypatch.setattr(pipeline.align, "parse_strip_list", parse_strip_list)
    monkeypatch.setattr(pipeline.align, "set_cancel_checker", lambda checker: None)

    extract_payloads = {
        "name": ({"alpha node": [("Alpha Node", "http://example.org/wdc/entity1")]}, 1),
        "iata": ({"abc": [("ABC", "http://example.org/wdc/entity1")]}, 1),
        "telephone": ({"123456": [("123-456", "http://example.org/wdc/entity1")]}, 1),
    }
    fetch_payloads = {
        "rdfs:label": {"alpha node": [("Alpha Node", "http://www.wikidata.org/entity/Q1001")]},
        "P238": {"abc": [("ABC", "http://www.wikidata.org/entity/Q1001")]},
        "P1329": {"123456": [("123456", "http://www.wikidata.org/entity/Q1001")]},
    }

    def extract_unique_iris_from_files(files, pattern, **kwargs):
        extract_calls.append(pattern)
        return extract_payloads.get(pattern, ({}, 0))

    monkeypatch.setattr(pipeline.align, "extract_unique_iris_from_files", extract_unique_iris_from_files)

    def fetch_wikidata_values(wikidata_property, wkd_class, wkd_prop_class):
        fetch_calls.append(wikidata_property)
        return fetch_payloads.get(wikidata_property, {})

    monkeypatch.setattr(pipeline.align, "fetch_wikidata_values", fetch_wikidata_values)

    def fuzzy_link(wdc_map, wikidata_map, **kwargs):
        out = []
        matched = set()
        for norm, wdc_entries in (wdc_map or {}).items():
            wd_entries = (wikidata_map or {}).get(norm) or []
            if not wd_entries:
                continue
            for wdc_val, wdc_iri in wdc_entries:
                wd_val, wd_iri = wd_entries[0]
                out.append(
                    {
                        "wdc_iri": wdc_iri,
                        "wikidata_uri": wd_iri,
                        "wdc_value": wdc_val,
                        "wiki_value": wd_val,
                        "method": "exact",
                    }
                )
                matched.add(norm)
        return (out, matched)

    monkeypatch.setattr(pipeline.align, "fuzzy_link", fuzzy_link)

    def export_results(matches, wdc_values_matched, wdc_map, wikidata_map, output_dir, **kwargs):
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        with (out / "wdc_wikidata_links.tsv").open("w", encoding="utf-8") as f:
            f.write("wdc_iri\twikidata_uri\twdc_value\twiki_value\tmethod\tmin_len\n")
            for m in matches:
                f.write(
                    f"{m['wdc_iri']}\t{m['wikidata_uri']}\t{m['wdc_value']}\t{m['wiki_value']}\t{m['method']}\t1\n"
                )

    monkeypatch.setattr(pipeline.align, "export_results", export_results)

    monkeypatch.setattr(
        pipeline.build,
        "read_links",
        lambda *args, **kwargs: (
            ["http://example.org/wdc/entity1"],
            ["http://www.wikidata.org/entity/Q1001"],
            ["Alpha Node"],
            ["Alpha Node"],
        ),
    )

    def run_pipeline_stub(args, wdc_entities, wd_entities_raw, wdc_values, wd_values, out_dir, *rest, **kwargs):
        out = Path(out_dir)
        out.mkdir(parents=True, exist_ok=True)
        (out / "ent_links").write_text(
            "wdc_iri\twikidata_uri\n"
            "http://example.org/wdc/entity1\thttp://www.wikidata.org/entity/Q1001\n",
            encoding="utf-8",
        )
        (out / "attr_triples_1").write_text("s\tp\to\n", encoding="utf-8")
        (out / "rel_triples_1").write_text("s\tp\to\n", encoding="utf-8")
        (out / "attr_triples_2").write_text("s\tp\to\n", encoding="utf-8")
        (out / "rel_triples_2").write_text("s\tp\to\n", encoding="utf-8")
        (out / "prop_stats_wdc.tsv").write_text("property\tcount\nname\t2\n", encoding="utf-8")
        (out / "prop_stats_wd.tsv").write_text("property\tcount\nP31\t2\n", encoding="utf-8")

    monkeypatch.setattr(pipeline.build, "run_pipeline", run_pipeline_stub)

    property_mapping_rules = (
        'name,iata,telephone => rdfs:label,P238,P1329 || ["spaces;dot","hyphen","slash"]'
    )
    result = pipeline.generate_benchmark(
        {
            "class_name": class_name,
            "parts_spec": "all",
            "matching_mode": "property",
            "wdc_predicate_pattern": "",
            "property_mapping_rules": property_mapping_rules,
            "wikidata_property": "",
            "wkd_class": "Q1248784",
            "ignore_chars": "spaces;-;.",
            "use_local_only": True,
            "force_align": True,
        },
        workers=1,
    )

    assert result["class_name"] == class_name
    assert extract_calls == ["name", "iata", "telephone"]
    assert fetch_calls == ["rdfs:label", "P238", "P1329"]
    assert normalize_specs == ["spaces;-;.", "spaces;dot", "hyphen", "slash", "spaces;-;."]
    assert normalize_enabled_calls.count(True) >= 5

    out_dir = Path(result["out_dir"])
    cfg = json.loads((out_dir / "BUILD_CONFIG.json").read_text(encoding="utf-8"))
    assert cfg["property_mapping_rules"] == property_mapping_rules

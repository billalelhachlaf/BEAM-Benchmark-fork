

def generate_benchmark(
    params,
    workers=None,
    should_cancel=None,
    set_phase=None,
    should_skip_build=None,
    on_checkpoint=None,
    on_final_links_count=None,
):
    start_ts = time.time()

    class_name = params.get("class_name")
    parts_spec = params.get("parts_spec") or "all"
    matching_mode = _normalize_matching_mode(
        params.get("matching_mode"),
        fallback_wdc_value_is_wikidata=bool(params.get("wdc_value_is_wikidata")),
    )
    pattern = params.get("wdc_predicate_pattern")
    wdc_pattern_search_in = _normalize_wdc_pattern_search_in(params.get("wdc_pattern_search_in"))
    target_property = params.get("target_property")
    if target_property in {None, ""}:
        target_property = params.get("wikidata_property")
    target_class = params.get("target_class")
    if target_class in {None, ""}:
        target_class = params.get("wkd_class")
    target_endpoint = params.get("target_endpoint") or "wikidata"
    target_endpoint_url = params.get("target_endpoint_url") or None
    target_prefixes = params.get("target_prefixes") or None
    property_mapping_rules = params.get("property_mapping_rules") or ""
    wkd_prop_class = params.get("wkd_prop_class") or None
    ignore_chars = params.get("ignore_chars") or None
    includes_sameas = _mode_includes_sameas(matching_mode)
    includes_property = _mode_includes_property(matching_mode)
    wdc_value_is_wikidata = matching_mode == "sameas"
    # WDC traversal depth is fixed to "full traversal" for web builds.
    # Keep this internal and stop exposing/persisting it as a user parameter.
    max_depth = -1
    match_min_length = int(params.get("match_min_length", 1))
    force_align = bool(params.get("force_align"))
    use_local_only = bool(params.get("use_local_only"))
    strict_duplicate_key_filter = bool(params.get("strict_duplicate_key_filter", True))
    require_cached_align = bool(params.get("require_cached_align"))
    resume_build = bool(params.get("resume_build")) and require_cached_align
    resume_out_dir_raw = str(params.get("resume_out_dir") or "").strip()

    if not class_name:
        raise PipelineError("class_name is required")
    parsed_rules = _parse_property_mapping_rules(property_mapping_rules) if property_mapping_rules else []
    has_rules = len(parsed_rules) > 0
    rules_include_sameas = any(str(r.get("mode") or "property").lower() == "sameas" for r in parsed_rules)
    rules_include_property = any(str(r.get("mode") or "property").lower() != "sameas" for r in parsed_rules)
    effective_includes_sameas = includes_sameas or rules_include_sameas
    effective_includes_property = includes_property if not has_rules else rules_include_property
    if effective_includes_property and not has_rules and not pattern:
        raise PipelineError("wdc_predicate_pattern is required")
    if effective_includes_property and not has_rules and not target_property:
        if (target_endpoint or "wikidata") == "wikidata":
            raise PipelineError("wikidata_property is required")
        raise PipelineError("target_property is required")
    if effective_includes_sameas and not target_class:
        if (target_endpoint or "wikidata") == "wikidata":
            raise PipelineError("wkd_class is required when wdc_value_is_wikidata is enabled")
        raise PipelineError("target_class is required when sameAs mode is enabled")

    _set_align_normalization(ignore_chars)

    def _check_cancel():
        if should_cancel and should_cancel():
            raise PipelineError("Cancelled by user")

    def _emit_final_links_count(count, **meta):
        if not on_final_links_count:
            return
        try:
            payload = {"final_links_count": int(count)}
            if meta:
                payload.update(meta)
            on_final_links_count(payload)
        except Exception:
            pass

    align.set_cancel_checker(should_cancel)

    work_dir = Path("Download") / class_name
    work_dir.mkdir(parents=True, exist_ok=True)

    lock_path = Path("Download") / ".workers.lock"

    # Always use part_* sources. Do not fallback to *_full_graph.nq files.
    decompressed_files = []
    available_parts = None
    if use_local_only:
        decompressed_files = _select_local_part_files(str(work_dir), parts_spec)
        if not decompressed_files:
            raise PipelineError(f"No local parts matched '{parts_spec}' in Download/; download is disabled.")
    else:
        if parts_spec.lower() == "all":
            available_parts = align.discover_parts(class_name)
            if not available_parts:
                raise PipelineError("No parts available for class")
        else:
            available_parts = align.discover_parts(class_name)
            if not available_parts:
                available_parts = None
        parts_to_download = align.parse_parts_spec(parts_spec, available_parts)
        if not parts_to_download:
            raise PipelineError(f"No valid parts for '{parts_spec}'")
        decompressed_files = align.download_and_decompress(
            class_name,
            parts_to_download,
            work_dir,
            parallel_decompress=True,
            workers=workers,
            lock_path=lock_path,
        )
        if len(decompressed_files) < len(parts_to_download):
            missing = len(parts_to_download) - len(decompressed_files)
            raise PipelineError(f"Missing {missing} part(s). Download/decompress incomplete.")

    if not decompressed_files:
        raise PipelineError("No decompressed files available")

    local_parts = _count_local_parts(str(work_dir))
    if local_parts <= 0:
        raise PipelineError("No local parts found after download.")

    align_params = _align_params_from_job_params(
        {
            "matching_mode": matching_mode,
            "class_name": class_name,
            "parts_spec": parts_spec,
            "wdc_predicate_pattern": pattern,
            "wdc_pattern_search_in": wdc_pattern_search_in,
            "target_property": target_property,
            "target_class": target_class,
            "target_endpoint": target_endpoint,
            "target_endpoint_url": target_endpoint_url,
            "target_prefixes": target_prefixes,
            "property_mapping_rules": property_mapping_rules,
            "ignore_chars": ignore_chars,
        }
    )
    cache_hash = _config_hash(align_params)
    cache_dir = work_dir / "align_cache" / cache_hash
    cache_dir.mkdir(parents=True, exist_ok=True)
    full_config = _full_config_for_cache(params)
    full_config_hash = _full_config_hash(params)
    (cache_dir / "ALIGN_CONFIG.json").write_text(
        json.dumps(
            {
                "cache_hash": cache_hash,
                "align_params": align_params,
                "full_config_hash": full_config_hash,
                "full_config": full_config,
            },
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    reused_align = False
    links_tsv = cache_dir / "wdc_wikidata_links.tsv"
    align_done = cache_dir / "ALIGN_DONE"
    align_pairs = 0
    align_link_sources = []
    pair_source_map = {}
    type_filter_iris = align.default_type_filter_iris_for_class(class_name)

    cache_ready = links_tsv.exists() and align_done.exists()
    cache_config_ok = _align_cache_config_matches(cache_dir, params) if cache_ready else False

    if cache_ready and not force_align and cache_config_ok:
        reused_align = True
        align_pairs = _count_alignment_pairs(links_tsv)
    else:
        if require_cached_align:
            if cache_ready and not cache_config_ok:
                raise PipelineError("Cached align found but full config mismatch; build-only requested")
            raise PipelineError("Cached align not found; build-only requested")
        if cache_ready and (not cache_config_ok) and not force_align:
            print("[INFO] Align cache found but full config mismatch; recomputing align.")
        if set_phase:
            set_phase("align")
        _check_cancel()

        wdc_map = {}
        wikidata_map = {}
        matches = []
        wdc_values_matched = set()
        seen_pairs = set()
        component_errors = []

        def _merge_component(component_wdc_map, component_wikidata_map, component_matches, component_wdc_values):
            _merge_value_maps(wdc_map, component_wdc_map or {})
            _merge_value_maps(wikidata_map, component_wikidata_map or {})
            wdc_values_matched.update(component_wdc_values or set())
            for item in component_matches or []:
                pair = (item.get("wdc_iri"), item.get("wikidata_uri"))
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)
                matches.append(item)

        if includes_sameas and not has_rules:
            try:
                sameas_wdc_map, matched_count, _wdc_cache_reused = _extract_wdc_values_with_cache(
                    work_dir=work_dir,
                    class_name=class_name,
                    parts_spec=parts_spec,
                    decompressed_files=decompressed_files,
                    pattern=pattern,
                    search_in=wdc_pattern_search_in,
                    wdc_value_is_wd_iri=True,
                    type_filter_iris=type_filter_iris,
                    ignore_chars=ignore_chars,
                    force_refresh=force_align,
                    workers=workers,
                    lock_path=lock_path,
                    progress_every=100,
                )
                if matched_count == 0:
                    raise PipelineError("No WDC values matched the predicate pattern")

                _check_cancel()
                wd_entity_iris = set()
                for entries in sameas_wdc_map.values():
                    for value, _iri in entries:
                        wd_iri = align.extract_wd_entity_iri(value)
                        if wd_iri:
                            wd_entity_iris.add(wd_iri)
                if not wd_entity_iris:
                    if (target_endpoint or "wikidata") == "wikidata":
                        raise PipelineError("No Wikidata URLs extracted from WDC values")
                    raise PipelineError("No target entity URLs extracted from WDC values")
                if (target_endpoint or "wikidata") == "wikidata":
                    sameas_wikidata_map = align.fetch_wikidata_values(
                        wikidata_property=None,
                        wkd_class=target_class,
                        wkd_prop_class=None,
                        entity_iris=sorted(wd_entity_iris),
                    )
                else:
                    sameas_value_candidates = set(wd_entity_iris)
                    for iri in list(wd_entity_iris):
                        if iri.startswith("http://www.wikidata.org/entity/"):
                            sameas_value_candidates.add("https://www.wikidata.org/entity/" + iri.rsplit("/", 1)[-1])
                        elif iri.startswith("https://www.wikidata.org/entity/"):
                            sameas_value_candidates.add("http://www.wikidata.org/entity/" + iri.rsplit("/", 1)[-1])
                    sameas_wikidata_map = align.fetch_target_values(
                        target_property="owl:sameAs",
                        target_class=target_class,
                        target_prop_class=None,
                        entity_iris=None,
                        value_candidates=sorted(sameas_value_candidates),
                        target_endpoint=target_endpoint,
                        target_endpoint_url=target_endpoint_url,
                        target_prefixes=target_prefixes,
                    )
                if not sameas_wikidata_map:
                    if (target_endpoint or "wikidata") == "wikidata":
                        raise PipelineError(
                            "No Wikidata entities matched class filter "
                            f"({target_class}) for extracted WDC Wikidata URLs "
                            f"({len(wd_entity_iris):,} entities)"
                        )
                    raise PipelineError(
                        "No target entities matched class filter "
                        f"({target_class}) for extracted WDC target URLs "
                        f"({len(wd_entity_iris):,} entities)"
                    )
                _check_cancel()
                sameas_matches, sameas_wdc_values_matched = align.fuzzy_link(
                    sameas_wdc_map,
                    sameas_wikidata_map,
                    parallel=True,
                    workers=workers,
                    lock_path=lock_path,
                    min_length=match_min_length,
                )
                _merge_component(sameas_wdc_map, sameas_wikidata_map, sameas_matches, sameas_wdc_values_matched)
            except PipelineError as exc:
                if matching_mode == "sameas":
                    raise
                print(f"[WARN] sameAs component skipped: {exc}")
                component_errors.append(f"sameAs: {exc}")

        if includes_property or has_rules:
            try:
                if has_rules:
                    print(f"[INFO] Property mapping rules enabled: {len(parsed_rules)} rule line(s).")
                    merged_wdc_map = {}
                    merged_wikidata_map = {}
                    prop_matches = []
                    prop_wdc_values_matched = set()
                    prop_seen_pairs = set()
                    matched_total = 0
                    target_fetch_any = False
                    target_fetch_error = False
                    for rule in parsed_rules:
                        rule_mode = str(rule.get("mode") or "property").strip().lower()
                        if rule_mode == "sameas":
                            for wdc_prop, _unused_target_prop in rule["pairs"]:
                                _set_align_normalization("")
                                rule_wdc_map, rule_matched_count, _rule_cache_reused = _extract_wdc_values_with_cache(
                                    work_dir=work_dir,
                                    class_name=class_name,
                                    parts_spec=parts_spec,
                                    decompressed_files=decompressed_files,
                                    pattern=wdc_prop,
                                    search_in="predicate",
                                    wdc_value_is_wd_iri=True,
                                    type_filter_iris=type_filter_iris,
                                    ignore_chars="",
                                    force_refresh=force_align,
                                    workers=workers,
                                    lock_path=lock_path,
                                    progress_every=100,
                                )
                                matched_total += int(rule_matched_count or 0)
                                if not rule_wdc_map:
                                    continue
                                wd_entity_iris = set()
                                for entries in rule_wdc_map.values():
                                    for value, _iri in entries:
                                        wd_iri = align.extract_wd_entity_iri(value)
                                        if wd_iri:
                                            wd_entity_iris.add(wd_iri)
                                if not wd_entity_iris:
                                    continue
                                if (target_endpoint or "wikidata") == "wikidata":
                                    rule_wikidata_map = align.fetch_wikidata_values(
                                        wikidata_property=None,
                                        wkd_class=target_class,
                                        wkd_prop_class=None,
                                        entity_iris=sorted(wd_entity_iris),
                                    )
                                else:
                                    sameas_value_candidates = set(wd_entity_iris)
                                    for iri in list(wd_entity_iris):
                                        if iri.startswith("http://www.wikidata.org/entity/"):
                                            sameas_value_candidates.add(
                                                "https://www.wikidata.org/entity/" + iri.rsplit("/", 1)[-1]
                                            )
                                        elif iri.startswith("https://www.wikidata.org/entity/"):
                                            sameas_value_candidates.add(
                                                "http://www.wikidata.org/entity/" + iri.rsplit("/", 1)[-1]
                                            )
                                    rule_wikidata_map = align.fetch_target_values(
                                        target_property="owl:sameAs",
                                        target_class=target_class,
                                        target_prop_class=None,
                                        entity_iris=None,
                                        value_candidates=sorted(sameas_value_candidates),
                                        target_endpoint=target_endpoint,
                                        target_endpoint_url=target_endpoint_url,
                                        target_prefixes=target_prefixes,
                                    )
                                if rule_wikidata_map is None:
                                    target_fetch_error = True
                                    continue
                                if rule_wikidata_map:
                                    target_fetch_any = True
                                if not rule_wikidata_map:
                                    continue
                                _merge_value_maps(merged_wdc_map, rule_wdc_map)
                                _merge_value_maps(merged_wikidata_map, rule_wikidata_map)
                                _check_cancel()
                                pair_matches, pair_wdc_values = align.fuzzy_link(
                                    rule_wdc_map,
                                    rule_wikidata_map,
                                    parallel=True,
                                    workers=workers,
                                    lock_path=lock_path,
                                    min_length=match_min_length,
                                )
                                prop_wdc_values_matched.update(pair_wdc_values or set())
                                for item in pair_matches or []:
                                    pair = (item.get("wdc_iri"), item.get("wikidata_uri"))
                                    if pair in prop_seen_pairs:
                                        continue
                                    prop_seen_pairs.add(pair)
                                    tagged = dict(item)
                                    prev_method = str(tagged.get("method") or "exact")
                                    tagged["method"] = f"{prev_method}|sameAs:{wdc_prop}"
                                    prop_matches.append(tagged)
                            continue
                        rule_ignore = str(rule.get("ignore_chars") or "").strip() or ignore_chars
                        pair_ignores = list(rule.get("pair_ignore_chars") or [])
                        pair_search_modes = list(rule.get("pair_search_in") or [])
                        for pair_idx, (wdc_prop, target_prop) in enumerate(rule["pairs"]):
                            pair_ignore = ""
                            if pair_idx < len(pair_ignores):
                                pair_ignore = str(pair_ignores[pair_idx] or "").strip()
                            pair_search_in = wdc_pattern_search_in
                            if pair_idx < len(pair_search_modes):
                                pair_search_in = _normalize_wdc_pattern_search_in(pair_search_modes[pair_idx])
                            _set_align_normalization(pair_ignore or rule_ignore)
                            rule_wdc_map, rule_matched_count, _rule_cache_reused = _extract_wdc_values_with_cache(
                                work_dir=work_dir,
                                class_name=class_name,
                                parts_spec=parts_spec,
                                decompressed_files=decompressed_files,
                                pattern=wdc_prop,
                                search_in=pair_search_in,
                                wdc_value_is_wd_iri=False,
                                type_filter_iris=type_filter_iris,
                                ignore_chars=(pair_ignore or rule_ignore),
                                force_refresh=force_align,
                                workers=workers,
                                lock_path=lock_path,
                                progress_every=100,
                            )
                            matched_total += int(rule_matched_count or 0)
                            if not rule_wdc_map:
                                continue
                            for target_prop_alt in _split_target_property_alternatives(target_prop):
                                if (target_endpoint or "wikidata") == "wikidata":
                                    rule_wikidata_map = _fetch_wikidata_values_for_alignment(
                                        target_prop_alt,
                                        target_class,
                                        wkd_prop_class,
                                        wdc_map=rule_wdc_map,
                                    )
                                else:
                                    rule_wikidata_map = _fetch_target_values_for_alignment(
                                        target_prop_alt,
                                        target_class,
                                        wkd_prop_class,
                                        target_endpoint=target_endpoint,
                                        target_endpoint_url=target_endpoint_url,
                                        target_prefixes=target_prefixes,
                                        wdc_map=rule_wdc_map,
                                    )
                                if rule_wikidata_map is None:
                                    target_fetch_error = True
                                    continue
                                if rule_wikidata_map:
                                    target_fetch_any = True
                                if not rule_wikidata_map:
                                    continue
                                _merge_value_maps(merged_wdc_map, rule_wdc_map)
                                _merge_value_maps(merged_wikidata_map, rule_wikidata_map)
                                _check_cancel()
                                pair_matches, pair_wdc_values = align.fuzzy_link(
                                    rule_wdc_map,
                                    rule_wikidata_map,
                                    parallel=True,
                                    workers=workers,
                                    lock_path=lock_path,
                                    min_length=match_min_length,
                                )
                                prop_wdc_values_matched.update(pair_wdc_values or set())
                                for item in pair_matches or []:
                                    pair = (item.get("wdc_iri"), item.get("wikidata_uri"))
                                    if pair in prop_seen_pairs:
                                        continue
                                    prop_seen_pairs.add(pair)
                                    tagged = dict(item)
                                    prev_method = str(tagged.get("method") or "exact")
                                    tagged["method"] = f"{prev_method}|{wdc_prop}->{target_prop_alt}"
                                    prop_matches.append(tagged)

                    _set_align_normalization(ignore_chars)
                    if matched_total <= 0:
                        parts_hint = ""
                        spec_txt = str(parts_spec or "").strip().lower()
                        if spec_txt and spec_txt != "all" and not spec_txt.startswith("0"):
                            parts_hint = " (parts are 0-based; try part 0 or 'all')"
                        raise PipelineError(
                            "No WDC values matched the property mapping rules"
                            f"{parts_hint}. Try a broader parts_spec or a different WDC pattern."
                        )
                    if target_fetch_error and not target_fetch_any:
                        raise PipelineError("Failed to fetch target endpoint values for property mapping rules")
                    _merge_component(merged_wdc_map, merged_wikidata_map, prop_matches, prop_wdc_values_matched)
                else:
                    prop_wdc_map, matched_count, _wdc_cache_reused = _extract_wdc_values_with_cache(
                        work_dir=work_dir,
                        class_name=class_name,
                        parts_spec=parts_spec,
                        decompressed_files=decompressed_files,
                        pattern=pattern,
                        search_in=wdc_pattern_search_in,
                        wdc_value_is_wd_iri=False,
                        type_filter_iris=type_filter_iris,
                        ignore_chars=ignore_chars,
                        force_refresh=force_align,
                        workers=workers,
                        lock_path=lock_path,
                        progress_every=100,
                    )
                    if matched_count == 0:
                        raise PipelineError("No WDC values matched the predicate pattern")
                    _check_cancel()
                    target_prop_alts = _split_target_property_alternatives(target_property)
                    if not target_prop_alts:
                        raise PipelineError("target_property is required")
                    prop_wikidata_map = {}
                    prop_matches = []
                    prop_wdc_values_matched = set()
                    prop_seen_pairs = set()
                    fetched_any = False
                    fetch_error = False
                    for target_prop_alt in target_prop_alts:
                        if (target_endpoint or "wikidata") == "wikidata":
                            alt_map = _fetch_wikidata_values_for_alignment(
                                target_prop_alt,
                                target_class,
                                wkd_prop_class,
                                wdc_map=prop_wdc_map,
                            )
                        else:
                            alt_map = _fetch_target_values_for_alignment(
                                target_prop_alt,
                                target_class,
                                wkd_prop_class,
                                target_endpoint=target_endpoint,
                                target_endpoint_url=target_endpoint_url,
                                target_prefixes=target_prefixes,
                                wdc_map=prop_wdc_map,
                            )
                        if alt_map is None:
                            fetch_error = True
                            continue
                        if not alt_map:
                            continue
                        fetched_any = True
                        _merge_value_maps(prop_wikidata_map, alt_map)
                        _check_cancel()
                        pair_matches, pair_wdc_values = align.fuzzy_link(
                            prop_wdc_map,
                            alt_map,
                            parallel=True,
                            workers=workers,
                            lock_path=lock_path,
                            min_length=match_min_length,
                        )
                        prop_wdc_values_matched.update(pair_wdc_values or set())
                        for item in pair_matches or []:
                            pair = (item.get("wdc_iri"), item.get("wikidata_uri"))
                            if pair in prop_seen_pairs:
                                continue
                            prop_seen_pairs.add(pair)
                            tagged = dict(item)
                            prev_method = str(tagged.get("method") or "exact")
                            tagged["method"] = f"{prev_method}|{target_prop_alt}"
                            prop_matches.append(tagged)
                    if fetch_error and not fetched_any:
                        raise PipelineError("Failed to fetch target endpoint values")
                    _merge_component(prop_wdc_map, prop_wikidata_map, prop_matches, prop_wdc_values_matched)
            except PipelineError as exc:
                if matching_mode == "property":
                    raise
                print(f"[WARN] property component skipped: {exc}")
                component_errors.append(f"property: {exc}")

        if not matches:
            if component_errors:
                raise PipelineError("No links produced in combined mode. " + " | ".join(component_errors))
            raise PipelineError("No links produced")
        pair_source_map = _build_pair_source_map(matches, fallback_pattern=pattern)
        align_link_sources = _count_sources_for_pairs(
            [m.get("wdc_iri") for m in matches],
            [m.get("wikidata_uri") for m in matches],
            pair_source_map,
        )
        align_pairs = len(matches)

        _check_cancel()
        align.export_results(
            matches,
            wdc_values_matched,
            wdc_map,
            wikidata_map,
            cache_dir,
            key_name=pattern,
            class_name=class_name,
            parts_spec=parts_spec,
            pattern=pattern,
            wikidata_property=target_property,
            wkd_class=target_class,
            wkd_prop_class=wkd_prop_class,
            start_ts=start_ts,
        )

        if not links_tsv.exists():
            raise PipelineError("Links TSV not found after alignment")
        align_done.write_text(time.strftime("%Y-%m-%d %H:%M:%S"), encoding="utf-8")

    if reused_align:
        print("✅ Alignment stage completed (cached).")
    else:
        print("✅ Alignment stage completed.")

    if strict_duplicate_key_filter:
        print("ℹ️ Final entity links (exact) pending strict duplicate-key filtering.")
    else:
        _emit_final_links_count(
            align_pairs,
            source="align",
            exact=True,
            raw_links=align_pairs,
            links_after_strict_duplicate_key_filter=align_pairs,
        )
        print(f"✅ Final entity links (exact): {align_pairs:,}.")

    if align_pairs == 0:
        reason = "No alignments found (0); build skipped."
        print(f"[INFO] {reason}")
        _emit_final_links_count(0, source="align", exact=True, raw_links=0, links_after=0)
        return {
            "class_name": class_name,
            "links_tsv": str(links_tsv),
            "align_dir": str(cache_dir),
            "reused_align": reused_align,
            "out_dir": None,
            "build_cancelled": False,
            "build_skipped": True,
            "build_skip_reason": reason,
            "started_at": start_ts,
            "ended_at": time.time(),
        }

    data_dir = Path("data") / class_name
    data_dir.mkdir(parents=True, exist_ok=True)
    if resume_build and resume_out_dir_raw:
        out_dir = Path(resume_out_dir_raw)
    else:
        out_dir = data_dir / f"beam_{_timestamp_tag()}"
    out_dir.mkdir(parents=True, exist_ok=True)

    wdc_nq = [str(Path(p)) for p in decompressed_files]
    if not wdc_nq:
        raise PipelineError(f"No WDC files found in {work_dir}")

    parts_manifest = []
    total_parts_size = 0
    for p in wdc_nq:
        fp = Path(p)
        try:
            size_b = fp.stat().st_size
        except Exception:
            size_b = 0
        total_parts_size += size_b
        parts_manifest.append(
            {
                "name": fp.name,
                "size_bytes": size_b,
                "size_human": _fmt_size(size_b),
            }
        )
    build_config = {
        "matching_mode": matching_mode,
        "class_name": class_name,
        "parts_spec": parts_spec,
        "wdc_predicate_pattern": pattern,
        "wdc_pattern_search_in": wdc_pattern_search_in,
        "property_mapping_rules": property_mapping_rules,
        "target_property": target_property,
        "target_class": target_class,
        "target_endpoint": target_endpoint,
        "target_endpoint_url": target_endpoint_url,
        "target_prefixes": target_prefixes,
        # Backward-compatible aliases for existing tools/views.
        "wikidata_property": target_property,
        "wkd_class": target_class,
        "ignore_chars": ignore_chars,
        "force_align": force_align,
        "use_local_only": use_local_only,
        "strict_duplicate_key_filter": strict_duplicate_key_filter,
        "linked_only_entities": True,
        "build_name": out_dir.name,
        "result_path": str(out_dir),
        "parts_count": len(parts_manifest),
        "parts_total_size_bytes": total_parts_size,
        "parts_total_size_human": _fmt_size(total_parts_size),
        "parts_manifest": parts_manifest,
    }
    (out_dir / "BUILD_CONFIG.json").write_text(
        json.dumps(build_config, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    if params.get("skip_build"):
        return {
            "class_name": class_name,
            "links_tsv": str(links_tsv),
            "align_dir": str(cache_dir),
            "reused_align": reused_align,
            "out_dir": None,
            "build_cancelled": False,
            "started_at": start_ts,
            "ended_at": time.time(),
        }

    if should_skip_build and should_skip_build():
        return {
            "class_name": class_name,
            "links_tsv": str(links_tsv),
            "align_dir": str(cache_dir),
            "reused_align": reused_align,
            "out_dir": None,
            "build_cancelled": True,
            "started_at": start_ts,
            "ended_at": time.time(),
        }

    if on_checkpoint:
        try:
            on_checkpoint(
                {
                    "kind": "build_started",
                    "phase": "build",
                    "out_dir": str(out_dir),
                    "align_dir": str(cache_dir),
                    "resume": bool(resume_build),
                    "ts": time.time(),
                }
            )
        except Exception:
            pass

    if set_phase:
        set_phase("build")
    _check_cancel()
    wdc_entities, wd_entities_raw, wdc_values, wd_values = build.read_links(
        str(links_tsv),
        "\t",
        0,
        1,
        None,
        None,
    )
    if not pair_source_map:
        pair_source_map = _pair_source_map_from_links_tsv(links_tsv, fallback_pattern=pattern)
    if not align_link_sources:
        align_link_sources = _count_sources_for_pairs(wdc_entities, wd_entities_raw, pair_source_map)
    raw_links_before_filters = len(wdc_entities)
    strict_filter_report = None
    if strict_duplicate_key_filter:
        (
            wdc_entities,
            wd_entities_raw,
            wdc_values,
            wd_values,
            strict_filter_report,
            strict_filter_decisions,
        ) = _apply_strict_duplicate_key_filter(
            wdc_nq,
            wdc_entities,
            wd_entities_raw,
            wdc_values,
            wd_values,
            should_cancel=should_cancel,
        )
        try:
            (out_dir / "WDC_DUPLICATE_KEY_FILTER_REPORT.json").write_text(
                json.dumps(strict_filter_report, indent=2, ensure_ascii=False, sort_keys=True),
                encoding="utf-8",
            )
        except Exception:
            pass
        try:
            with (out_dir / "WDC_DUPLICATE_KEY_FILTER_DECISIONS.tsv").open("w", encoding="utf-8") as f:
                f.write("key\tdecision\treason\tsignature_hash\twdc_entity\twikidata_entity\n")
                for row in strict_filter_decisions:
                    f.write(
                        f"{row.get('key','')}\t{row.get('decision','')}\t{row.get('reason','')}\t"
                        f"{row.get('signature_hash','')}\t{row.get('wdc_entity','')}\t{row.get('wikidata_entity','')}\n"
                    )
        except Exception:
            pass
        if not wdc_entities or not wd_entities_raw:
            reason = "No links left after strict duplicate-key filtering; build skipped."
            print(f"[INFO] {reason}")
            _emit_final_links_count(
                0,
                source="build_prefilter",
                exact=True,
                raw_links=raw_links_before_filters,
                links_after_strict_duplicate_key_filter=0,
            )
            return {
                "class_name": class_name,
                "links_tsv": str(links_tsv),
                "align_dir": str(cache_dir),
                "reused_align": reused_align,
                "out_dir": None,
                "build_cancelled": False,
                "build_skipped": True,
                "build_skip_reason": reason,
                "started_at": start_ts,
                "ended_at": time.time(),
            }

    final_links_count = len(wdc_entities)
    links_by_source_after_filter = _count_sources_for_pairs(wdc_entities, wd_entities_raw, pair_source_map)
    _emit_final_links_count(
        final_links_count,
        source="build_prefilter",
        exact=True,
        raw_links=raw_links_before_filters,
        links_after_strict_duplicate_key_filter=final_links_count,
    )

    wdc_mask_values = set(v for v in wdc_values if v)
    wd_mask_values = set(v for v in wd_values if v)

    wdc_exclude_props = set()
    wd_exclude_props = set()
    wd_link_prop_uris = set()
    wdc_link_prop_patterns = set()
    if has_rules:
        for rule in parsed_rules:
            for wdc_prop, target_prop in rule["pairs"]:
                if wdc_prop:
                    wdc_link_prop_patterns.add(str(wdc_prop).lower())
                if (target_endpoint or "wikidata") == "wikidata" and target_prop:
                    norm_prop = build.normalize_wd_prop_id(str(target_prop))
                    if norm_prop:
                        wd_link_prop_uris.update(build.wikidata_prop_uris(norm_prop))
    elif pattern:
        wdc_link_prop_patterns.add(str(pattern).lower())
    if (not has_rules) and (target_endpoint or "wikidata") == "wikidata" and target_property:
        for target_prop_alt in _split_target_property_alternatives(target_property):
            norm_prop = build.normalize_wd_prop_id(str(target_prop_alt))
            if norm_prop:
                wd_link_prop_uris.update(build.wikidata_prop_uris(norm_prop))

    replace_map = {}
    lowercase_wd = True
    add_wd_labels = True
    endpoint_sparql_url = align.resolve_target_endpoint_url(target_endpoint, target_endpoint_url)
    if (target_endpoint or "wikidata") != "wikidata":
        add_wd_labels = False
    wd_batch_default = int(os.environ.get("BEAM_WD_BATCH_SIZE", "150"))
    wd_sleep_default = float(os.environ.get("BEAM_WD_SLEEP", "0.05"))

    args = SimpleNamespace(
        wdc_nq=wdc_nq,
        wd_nq=None,
        sep="\t",
        wdc_col=0,
        wd_col=1,
        wdc_value_col=None,
        wd_value_col=None,
        dedupe_links=False,
        keep_link_values=False,
        wdc_min_triples=0,
        wdc_exclude_prop=[],
        wd_exclude_prop=[],
        no_wd_labels=False,
        wd_prop_min_count=0,
        merge_wd_by_link_values=False,
        sparql_url=endpoint_sparql_url or "https://query.wikidata.org/sparql",
        lang="en",
        batch_size=int(params.get("wd_batch_size", wd_batch_default)),
        sleep=float(params.get("wd_sleep", wd_sleep_default)),
        timeout=int(params.get("wd_timeout", 60)),
        retries=int(params.get("wd_retries", 3)),
        backoff=float(params.get("wd_backoff", 2.0)),
        no_lowercase_wd=False,
        resume=bool(resume_build),
        state_file=None,
        max_depth=max_depth,
        progress_every=10_000_000,
        linked_only_entities=True,
    )

    out_without = str(out_dir / "without_link_code")
    out_with = str(out_dir / "with_link_code")
    shared_wd_raw_cache = str(out_dir / ".wd_raw_triples.tsv")

    build.run_pipeline(
        args,
        wdc_entities,
        wd_entities_raw,
        wdc_values,
        wd_values,
        out_without,
        wdc_mask_values,
        wd_mask_values,
        wdc_exclude_props,
        wdc_link_prop_patterns,
        wd_exclude_props | wd_link_prop_uris,
        replace_map,
        lowercase_wd,
        add_wd_labels,
        wd_raw_cache_path=shared_wd_raw_cache,
    )
    build.run_pipeline(
        args,
        wdc_entities,
        wd_entities_raw,
        wdc_values,
        wd_values,
        out_with,
        None,
        None,
        wdc_exclude_props,
        set(),
        wd_exclude_props,
        replace_map,
        lowercase_wd,
        add_wd_labels,
        wd_raw_cache_path=shared_wd_raw_cache,
    )

    with_ent_links = Path(out_with) / "ent_links"
    without_ent_links = Path(out_without) / "ent_links"
    with_links_count = _count_ent_links_rows(with_ent_links)
    without_links_count = _count_ent_links_rows(without_ent_links)
    links_after_strict_filter = int(final_links_count)
    strict_removed_groups_count = 0
    if isinstance(strict_filter_report, dict):
        summary = strict_filter_report.get("summary") or {}
        try:
            links_after_strict_filter = int(summary.get("links_after", final_links_count))
        except Exception:
            links_after_strict_filter = int(final_links_count)
        try:
            strict_removed_groups_count = int(summary.get("removed_groups_count", 0))
        except Exception:
            strict_removed_groups_count = 0
    build_stats = {
        "class_name": class_name,
        "build_name": out_dir.name,
        "target_endpoint": target_endpoint,
        "target_endpoint_url": target_endpoint_url,
        "strict_duplicate_key_filter": bool(strict_duplicate_key_filter),
        "links_before_filters": int(raw_links_before_filters),
        "links_after_strict_duplicate_key_filter": links_after_strict_filter,
        "strict_duplicate_key_removed_groups_count": strict_removed_groups_count,
        "links_by_source_align": align_link_sources,
        "links_by_source_after_filter": links_by_source_after_filter,
        "links_count_with_link_code": int(with_links_count),
        "links_count_without_link_code": int(without_links_count),
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    try:
        (Path(out_dir) / "BUILD_STATS.json").write_text(
            json.dumps(build_stats, indent=2, ensure_ascii=False, sort_keys=True),
            encoding="utf-8",
        )
    except Exception:
        pass
    variant_stats_with = {
        "variant": "with_link_code",
        "links_count": int(with_links_count),
        "build_name": out_dir.name,
        "class_name": class_name,
        "target_endpoint": target_endpoint,
        "generated_at": build_stats["generated_at"],
    }
    variant_stats_without = {
        "variant": "without_link_code",
        "links_count": int(without_links_count),
        "build_name": out_dir.name,
        "class_name": class_name,
        "target_endpoint": target_endpoint,
        "generated_at": build_stats["generated_at"],
    }
    try:
        (Path(out_with) / "BUILD_STATS.json").write_text(
            json.dumps(variant_stats_with, indent=2, ensure_ascii=False, sort_keys=True),
            encoding="utf-8",
        )
    except Exception:
        pass
    try:
        (Path(out_without) / "BUILD_STATS.json").write_text(
            json.dumps(variant_stats_without, indent=2, ensure_ascii=False, sort_keys=True),
            encoding="utf-8",
        )
    except Exception:
        pass

    # mark build done
    (Path(out_dir) / "BUILD_DONE").write_text(time.strftime("%Y-%m-%d %H:%M:%S"), encoding="utf-8")

    return {
        "class_name": class_name,
        "links_tsv": str(links_tsv),
        "align_dir": str(cache_dir),
        "reused_align": reused_align,
        "out_dir": str(out_dir),
        "build_cancelled": False,
        "started_at": start_ts,
        "ended_at": time.time(),
    }

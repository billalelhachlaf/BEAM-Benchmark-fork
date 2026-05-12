def _read_top_props(path: Path, limit: int = 5):
    rows = []
    if not path.exists() or not path.is_file():
        return rows
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        header_skipped = False
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split("\t")
            if not header_skipped:
                header_skipped = True
                first = parts[0].strip().lower() if parts else ""
                if first in {"predicate", "property"}:
                    continue
            prop = parts[0].strip() if parts else ""
            count_raw = parts[1].strip() if len(parts) > 1 else "0"
            try:
                count = int(count_raw)
            except Exception:
                count = 0
            label = parts[2].strip() if len(parts) > 2 else ""
            description = parts[3].strip() if len(parts) > 3 else ""
            rows.append(
                {
                    "property": prop,
                    "count": count,
                    "label": label,
                    "description": description,
                }
            )
            if len(rows) >= limit:
                break
    return rows


def _read_ent_links_samples(path: Path, limit: int = 5):
    rows = []
    if not path.exists() or not path.is_file():
        return rows
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 2:
                continue
            left = parts[0].strip()
            right = parts[1].strip()
            if _looks_like_ent_links_header(f"{left}\t{right}"):
                continue
            rows.append({"wdc_iri": left, "wikidata_uri": right})
            if len(rows) >= limit:
                break
    return rows


def _fetch_target_preview_values(
    target_property: str,
    target_class: str,
    target_endpoint: str,
    target_endpoint_url: str,
    target_prefixes: str,
    ignore_chars: str,
    limit: int = 1200,
):
    endpoint_key = _normalize_target_endpoint(target_endpoint)
    q_limit = max(100, min(int(limit), 5000))

    # Keep optimized dedicated query for Wikidata preview, unchanged behavior.
    if endpoint_key == "wikidata":
        prop = align_script.normalize_wikidata_property(target_property)
        if not prop:
            return []
        class_norm = align_script.normalize_wkd_class(target_class)
        class_filter = ""
        if class_norm:
            class_filter = f"""
      ?entity wdt:P31 ?type .
      ?type wdt:P279* {class_norm} .
    """
        query = f"""
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    SELECT DISTINCT ?value WHERE {{
      ?entity {prop} ?value .
      {class_filter}
    }}
    LIMIT {q_limit}
    """
        headers = {
            "Accept": "application/sparql-results+json",
            "User-Agent": "beam-preflight/1.0",
        }
        timeout_s = max(5, int(os.environ.get("PREFLIGHT_WIKIDATA_TIMEOUT", "25")))
        try:
            response = requests.post(
                align_script.WIKIDATA_ENDPOINT,
                data={"query": query, "format": "json"},
                headers=headers,
                timeout=timeout_s,
            )
            response.raise_for_status()
            loader = getattr(align_script, "_load_sparql_json_payload", None)
            if callable(loader):
                payload = loader(response.text)
            else:
                payload = json.loads(response.text)
        except Exception:
            return []

        rows = []
        seen_norm = set()
        bindings = (((payload or {}).get("results") or {}).get("bindings")) or []
        for item in bindings:
            value = str((((item or {}).get("value") or {}).get("value")) or "").strip()
            if not value:
                continue
            normalized = _normalize_preflight_value(value, ignore_chars)
            if not normalized or normalized in seen_norm:
                continue
            seen_norm.add(normalized)
            rows.append({"value": value[:180], "normalized": normalized})
            if len(rows) >= q_limit:
                break
        return rows

    fetch_target = getattr(align_script, "fetch_target_values", None)
    if not callable(fetch_target):
        return []
    target_map = fetch_target(
        target_property=target_property,
        target_class=target_class,
        target_prop_class=None,
        entity_iris=None,
        target_endpoint=endpoint_key,
        target_endpoint_url=_clean_text(target_endpoint_url),
        target_prefixes=_clean_text(target_prefixes),
    )
    if not isinstance(target_map, dict):
        return []
    rows = []
    for norm, entries in target_map.items():
        if not norm or not isinstance(entries, list) or not entries:
            continue
        first = entries[0]
        raw_value = str(first[0] if isinstance(first, (list, tuple)) and len(first) > 0 else "")
        normalized = _normalize_preflight_value(raw_value, ignore_chars) if raw_value else str(norm)
        if not normalized:
            continue
        rows.append({"value": raw_value[:180], "normalized": normalized})
        if len(rows) >= q_limit:
            break
    return rows


def _build_preflight_report(
    class_name: str,
    parts_spec: str,
    wdc_predicate_pattern: str,
    wdc_pattern_search_in: str,
    ignore_chars: str,
    matching_mode: str,
    use_local_only: bool,
    target_endpoint: str = "wikidata",
    target_endpoint_url: str = "",
    target_prefixes: str = "",
    property_mapping_rules: str = "",
    target_property: str = "",
    target_class: str = "",
    wikidata_property: str = "",
    wkd_class: str = "",
    include_wikidata_preview: bool = True,
    scan_limit_lines: int = 30000,
):
    class_name = _clean_text(class_name)
    parts_spec = _clean_text(parts_spec) or "all"
    pattern = _clean_text(wdc_predicate_pattern)
    pattern_search_in = _normalize_wdc_pattern_search_in(wdc_pattern_search_in)
    ignore_chars = _clean_text(ignore_chars)
    endpoint_key = _normalize_target_endpoint(target_endpoint)
    target_endpoint_url = _clean_text(target_endpoint_url)
    target_prefixes = _clean_text(target_prefixes)
    property_mapping_rules = _clean_text(property_mapping_rules)
    target_property = _clean_text(target_property or wikidata_property)
    target_class = _clean_text(target_class or wkd_class)
    mode_norm = _normalize_matching_mode(matching_mode)
    includes_sameas = _mode_includes_sameas(mode_norm)
    includes_property = _mode_includes_property(mode_norm)
    wdc_value_is_wikidata = mode_norm == "sameas"
    parsed_rules = []
    if property_mapping_rules:
        try:
            parsed_rules = _parse_property_mapping_rules_text(property_mapping_rules)
        except ValueError as exc:
            report = {
                "ok": False,
                "summary": str(exc),
                "risk": "high",
                "confidence": "low",
            }
            return report
    rules_include_sameas = any(_clean_text(str(r.get("mode", "property"))).lower() == "sameas" for r in parsed_rules)
    rules_include_property = any(_clean_text(str(r.get("mode", "property"))).lower() != "sameas" for r in parsed_rules)
    effective_includes_sameas = includes_sameas or rules_include_sameas
    effective_includes_property = includes_property if not parsed_rules else rules_include_property

    if effective_includes_property and parsed_rules:
        first_pair = parsed_rules[0]["pairs"][0]
        if not pattern:
            pattern = _clean_text(first_pair[0])
        if not target_property:
            target_property = _clean_text(first_pair[1])
            if "|" in target_property:
                alts = _split_target_property_alternatives(target_property)
                target_property = alts[0] if alts else target_property
    report = {
        "ok": False,
        "class_name": class_name,
        "parts_spec": parts_spec,
        "pattern": pattern,
        "pattern_search_in": pattern_search_in,
        "matching_mode": mode_norm,
        "target_endpoint": endpoint_key,
        "target_endpoint_url": target_endpoint_url,
        "target_prefixes": target_prefixes,
        "property_mapping_rules": property_mapping_rules,
        "target_property": target_property,
        "target_class": target_class,
        "wdc_value_is_wikidata": bool(wdc_value_is_wikidata or effective_includes_sameas),
        "scan_limit_lines": int(max(1000, scan_limit_lines)),
        "selected_files_count": 0,
        "selected_files": [],
        "scanned_lines": 0,
        "matched_triples": 0,
        "distinct_values": 0,
        "wikidata_url_like": 0,
        "sample_values": [],
        "top_unmatched_wdc_values": [],
        "close_wikidata_examples": [],
        "top_predicates": [],
        "invalid_wikidata_samples": [],
        "wikidata_preview_count": 0,
        "risk": "high",
        "confidence": "low",
        "warnings": [],
        "summary": "",
    }

    if not class_name:
        report["summary"] = "Class name is required."
        return report
    if not pattern:
        report["summary"] = "Considered pattern for WDC properties is required."
        return report

    selected_files, select_warnings = _select_local_part_files(class_name, parts_spec)
    report["warnings"].extend(select_warnings)
    if not selected_files:
        report["summary"] = "No local files available for preflight."
        return report

    selected_names = [fp.name for fp in selected_files]
    report["selected_files"] = selected_names[:20]
    report["selected_files_count"] = len(selected_files)
    if len(selected_names) > 20:
        report["warnings"].append(f"Preflight uses first 20 listed files out of {len(selected_names)} selected.")

    if not use_local_only:
        parts_info = _build_class_parts_info(class_name)
        missing_online = int(parts_info.get("not_downloaded_online_parts_count") or 0)
        if missing_online > 0:
            report["warnings"].append(
                "Preflight scans local files only; some online parts are not downloaded yet."
            )

    prepared_patterns = align_script.prepare_predicate_patterns(pattern)
    distinct_norm = set()
    value_counts = Counter()
    value_examples = {}
    predicate_counts = Counter()
    invalid_wikidata_samples = []
    sample_values = []
    wikidata_like_values = 0
    matched = 0
    scanned = 0
    scan_limit = int(max(1000, scan_limit_lines))

    for fp in selected_files:
        with fp.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                if scanned >= scan_limit:
                    break
                scanned += 1
                parsed = _parse_nq_or_nt(line)
                if not parsed:
                    continue
                _s, p_tok, o_tok = parsed
                predicate = p_tok.strip("<>")
                predicate_counts[predicate] += 1
                if o_tok.startswith('"'):
                    raw_value = _literal_lex(o_tok) or o_tok.strip('"')
                else:
                    raw_value = o_tok.strip("<>")
                if pattern_search_in == "value":
                    if not align_script.value_matches_prepared_patterns(raw_value, prepared_patterns):
                        continue
                else:
                    if not align_script.predicate_matches_prepared_patterns(predicate, prepared_patterns):
                        continue

                matched += 1
                if raw_value:
                    normalized = _normalize_preflight_value(raw_value, ignore_chars)
                    if normalized:
                        if normalized not in distinct_norm and len(sample_values) < 5:
                            sample_values.append(raw_value[:120])
                        value_counts[normalized] += 1
                        if normalized not in value_examples:
                            value_examples[normalized] = raw_value[:180]
                        distinct_norm.add(normalized)
                    if wdc_value_is_wikidata:
                        extractor = getattr(align_script, "extract_target_entity_iri", None)
                        if callable(extractor):
                            endpoint_iri = extractor(
                                raw_value,
                                target_endpoint=endpoint_key,
                                target_endpoint_url=target_endpoint_url,
                            )
                        else:
                            endpoint_iri = align_script.extract_wd_entity_iri(raw_value)
                        if endpoint_iri:
                            wikidata_like_values += 1
                        elif len(invalid_wikidata_samples) < 5:
                            invalid_wikidata_samples.append(raw_value[:160])
            if scanned >= scan_limit:
                break

    report["scanned_lines"] = scanned
    report["matched_triples"] = matched
    report["distinct_values"] = len(distinct_norm)
    report["wikidata_url_like"] = wikidata_like_values
    report["sample_values"] = sample_values
    report["invalid_wikidata_samples"] = invalid_wikidata_samples
    report["top_unmatched_wdc_values"] = [
        {
            "normalized": norm,
            "value": value_examples.get(norm, norm),
            "count": int(cnt),
        }
        for norm, cnt in value_counts.most_common(8)
    ]

    if scanned >= scan_limit:
        report["warnings"].append(f"Sample limit reached ({scan_limit:,} lines).")
    if matched == 0:
        report["top_predicates"] = [
            {"predicate": pred, "count": int(cnt)}
            for pred, cnt in predicate_counts.most_common(8)
        ]
        report["risk"] = "high"
        report["summary"] = "No triple matched the considered pattern for WDC properties in sampled local data."
    elif wdc_value_is_wikidata and wikidata_like_values == 0:
        report["risk"] = "high"
        report["summary"] = "Pattern matched, but no target endpoint URL-like values were found."
    elif len(distinct_norm) < 5:
        report["risk"] = "medium"
        report["summary"] = "Very few distinct values found; alignment risk is moderate."
    else:
        report["risk"] = "low"
        report["summary"] = "Signal looks good in sampled local data."

    if scanned >= 20000:
        report["confidence"] = "high"
    elif scanned >= 5000:
        report["confidence"] = "medium"
    else:
        report["confidence"] = "low"

    if (
        include_wikidata_preview
        and not wdc_value_is_wikidata
        and target_property
        and report["top_unmatched_wdc_values"]
    ):
        preview_rows = _fetch_target_preview_values(
            target_property=target_property,
            target_class=target_class,
                target_endpoint=endpoint_key,
                target_endpoint_url=target_endpoint_url,
                target_prefixes=target_prefixes,
                ignore_chars=ignore_chars,
                limit=1200,
            )
        report["wikidata_preview_count"] = len(preview_rows)
        if preview_rows:
            wd_norm_to_value = {}
            wd_norm_keys = []
            for row in preview_rows:
                norm = row.get("normalized")
                raw_value = row.get("value")
                if not norm:
                    continue
                if norm not in wd_norm_to_value:
                    wd_norm_to_value[norm] = raw_value
                    wd_norm_keys.append(norm)
            for row in report["top_unmatched_wdc_values"][:5]:
                norm = row.get("normalized")
                if not norm:
                    continue
                close_norms = difflib.get_close_matches(norm, wd_norm_keys, n=3, cutoff=0.72)
                if not close_norms:
                    continue
                report["close_wikidata_examples"].append(
                    {
                        "wdc_value": row.get("value"),
                        "wdc_count": row.get("count"),
                        "wikidata_candidates": [wd_norm_to_value[n] for n in close_norms],
                    }
                )
        else:
            report["warnings"].append("Could not fetch target endpoint preview values for preflight diagnostics.")

    report["ok"] = True
    return report


def _discover_local_class_rows(download_root: str = "Download"):
    root = Path(download_root)
    if not root.exists() or not root.is_dir():
        return []

    rows = []
    for class_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        parts = []
        full_graph = []
        try:
            for fp in class_dir.iterdir():
                if not fp.is_file():
                    continue
                name = fp.name
                if name.startswith("part_") and (name.endswith(".nq") or name.endswith(".nt") or "." not in name):
                    parts.append(fp)
                elif name.endswith("_full_graph.nq"):
                    full_graph.append(fp)
        except Exception:
            continue

        files = parts if parts else full_graph
        if not files:
            continue

        total_size = 0
        for fp in files:
            try:
                total_size += fp.stat().st_size
            except Exception:
                pass
        rows.append(
            {
                "class_name": class_dir.name,
                "num_parts": len(parts) if parts else len(full_graph),
                "size_human": _fmt_size(total_size),
            }
        )
    return rows


def _seed_wdc_classes_from_local_catalog():
    try:
        rows = load_wdc_classes_catalog()
    except Exception:
        return 0
    if not rows:
        return 0
    try:
        db.upsert_wdc_classes(rows)
    except Exception:
        return 0
    return len(rows)


def _refresh_wdc_classes_from_remote():
    rows = fetch_wdc_classes()
    if not rows:
        raise RuntimeError("WDC class refresh returned no rows")
    save_wdc_classes_catalog(rows)
    db.upsert_wdc_classes(rows)
    return len(rows)


def _part_number_from_name(name: str):
    if not name:
        return None
    m = _PART_HREF_RE.match(name) or _PART_NAME_RE.match(name)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _discover_local_part_numbers(class_name: str):
    class_dir = Path("Download") / (class_name or "")
    if not class_dir.exists() or not class_dir.is_dir():
        return []

    numbers = set()
    for fp in class_dir.iterdir():
        if not fp.is_file():
            continue
        name = fp.name
        if not name.startswith("part_"):
            continue
        if not (name.endswith(".nq") or name.endswith(".nt") or "." not in name):
            continue
        num = _part_number_from_name(name)
        if num is not None:
            numbers.add(num)
    return sorted(numbers)


@lru_cache(maxsize=256)
def _discover_online_part_numbers(class_name: str):
    if not class_name:
        return [], "class_name is empty"
    url = urljoin(WDC_PARTS_BASE_URL, f"{class_name}/")
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        numbers = set()
        for link in soup.find_all("a"):
            href = (link.get("href") or "").strip()
            num = _part_number_from_name(href)
            if num is not None:
                numbers.add(num)
        return sorted(numbers), None
    except Exception as exc:
        return [], str(exc)


def _format_part_ranges(values):
    if not values:
        return "—"
    nums = sorted(set(int(v) for v in values))
    chunks = []
    start = nums[0]
    prev = nums[0]
    for n in nums[1:]:
        if n == prev + 1:
            prev = n
            continue
        chunks.append(f"{start}-{prev}" if start != prev else str(start))
        start = prev = n
    chunks.append(f"{start}-{prev}" if start != prev else str(start))
    if len(chunks) > 28:
        return ", ".join(chunks[:28]) + f", ... (+{len(chunks)-28} ranges)"
    return ", ".join(chunks)


def _format_part_list(values, limit=60):
    if not values:
        return "—"
    nums = [int(v) for v in sorted(set(values))]
    if len(nums) <= limit:
        return ", ".join(str(v) for v in nums)
    return ", ".join(str(v) for v in nums[:limit]) + f", ... (+{len(nums)-limit})"


def _class_meta_by_name(class_name: str):
    for row in db.list_wdc_classes():
        if row["class_name"] == class_name:
            return dict(row)
    return None


def _build_class_parts_info(class_name: str):
    class_name = _clean_text(class_name)
    local_numbers = _discover_local_part_numbers(class_name)
    online_numbers, online_error = _discover_online_part_numbers(class_name)
    local_set = set(local_numbers)
    meta = _class_meta_by_name(class_name) or {}
    class_num_parts = meta.get("num_parts")
    try:
        class_num_parts = int(class_num_parts) if class_num_parts is not None else None
    except Exception:
        class_num_parts = None

    online_set = set(online_numbers)
    inferred_online_set = set(online_set)
    inferred_from_catalog = False

    if online_numbers:
        start_num = min(online_numbers)
    elif local_numbers:
        start_num = min(local_numbers)
    else:
        start_num = 0

    catalog_expected_numbers = []
    if class_num_parts and class_num_parts > 0:
        catalog_expected_numbers = list(range(start_num, start_num + class_num_parts))
        catalog_set = set(catalog_expected_numbers)
        if not inferred_online_set:
            inferred_online_set = set(catalog_set)
            inferred_from_catalog = True
        elif len(inferred_online_set) < class_num_parts:
            # Online listing can be incomplete; complete the expected contiguous range using catalog count.
            inferred_online_set |= catalog_set
            inferred_from_catalog = True

    if inferred_online_set:
        downloaded_numbers = sorted(local_set & inferred_online_set)
    else:
        downloaded_numbers = list(local_numbers)
    not_downloaded_online_numbers = sorted(inferred_online_set - local_set)
    local_only_numbers = sorted(local_set - inferred_online_set) if inferred_online_set else []

    return {
        "class_name": class_name,
        "class_num_parts": class_num_parts,
        "class_size_human": meta.get("size_human"),
        "online_error": online_error,
        "online_available_count": len(inferred_online_set),
        "online_available_numbers": sorted(inferred_online_set),
        "online_available_numbers_text": _format_part_list(sorted(inferred_online_set)),
        "online_available_ranges": _format_part_ranges(sorted(inferred_online_set)),
        "online_discovered_count": len(online_numbers),
        "online_discovered_numbers": online_numbers,
        "online_discovered_numbers_text": _format_part_list(online_numbers),
        "online_discovered_ranges": _format_part_ranges(online_numbers),
        "online_inferred_from_catalog": inferred_from_catalog,
        "catalog_expected_numbers": catalog_expected_numbers,
        "catalog_expected_ranges": _format_part_ranges(catalog_expected_numbers),
        "downloaded_parts_count": len(downloaded_numbers),
        "downloaded_part_numbers": downloaded_numbers,
        "downloaded_part_numbers_text": _format_part_list(downloaded_numbers),
        "downloaded_part_ranges": _format_part_ranges(downloaded_numbers),
        "not_downloaded_online_parts_count": len(not_downloaded_online_numbers),
        "not_downloaded_online_part_numbers": not_downloaded_online_numbers,
        "not_downloaded_online_part_numbers_text": _format_part_list(not_downloaded_online_numbers),
        "not_downloaded_online_part_ranges": _format_part_ranges(not_downloaded_online_numbers),
        "local_only_parts_count": len(local_only_numbers),
        "local_only_part_numbers": local_only_numbers,
        "local_only_part_numbers_text": _format_part_list(local_only_numbers),
    }



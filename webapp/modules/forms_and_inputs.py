def _default_form():
    return {
        "matching_mode": "property",
        "class_name": "",
        "parts_spec": "all",
        "wdc_predicate_pattern": "",
        "wdc_pattern_search_in": "predicate",
        "target_endpoint": "wikidata",
        "target_endpoint_url": "",
        "target_prefixes": "",
        "property_mapping_rules": "",
        "target_property": "",
        "target_class": "",
        "wikidata_property": "",
        "wkd_class": "",
        "ignore_chars": "spaces;-;.",
        "force_align": False,
        "use_local_only": False,
        "strict_duplicate_key_filter": True,
    }


def _clean_text(value: Optional[str]) -> str:
    return (value or "").strip()


def _slugify_heading(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", (value or "").strip().lower()).strip("-")
    return slug or "section"


def _render_markdown_basic(md_text: str):
    lines = (md_text or "").splitlines()
    out = []
    sections = []
    in_code = False
    list_mode = None

    def _close_list():
        nonlocal list_mode
        if list_mode == "ul":
            out.append("</ul>")
        elif list_mode == "ol":
            out.append("</ol>")
        list_mode = None

    for raw in lines:
        line = raw.rstrip("\n")
        stripped = line.strip()

        if stripped.startswith("```"):
            _close_list()
            if not in_code:
                out.append("<pre><code>")
                in_code = True
            else:
                out.append("</code></pre>")
                in_code = False
            continue

        if in_code:
            out.append(html.escape(line))
            continue

        if not stripped:
            _close_list()
            continue

        h = re.match(r"^(#{1,6})\s+(.+)$", stripped)
        if h:
            _close_list()
            lvl = len(h.group(1))
            title = h.group(2).strip()
            anchor = _slugify_heading(title)
            if lvl <= 3:
                sections.append({"title": title, "anchor": anchor, "level": lvl})
            out.append(f'<h{lvl} id="{anchor}">{html.escape(title)}</h{lvl}>')
            continue

        ul = re.match(r"^[-*]\s+(.+)$", stripped)
        if ul:
            if list_mode != "ul":
                _close_list()
                out.append("<ul>")
                list_mode = "ul"
            item = html.escape(ul.group(1).strip())
            item = re.sub(r"`([^`]+)`", r"<code>\1</code>", item)
            out.append(f"<li>{item}</li>")
            continue

        ol = re.match(r"^\d+\.\s+(.+)$", stripped)
        if ol:
            if list_mode != "ol":
                _close_list()
                out.append("<ol>")
                list_mode = "ol"
            item = html.escape(ol.group(1).strip())
            item = re.sub(r"`([^`]+)`", r"<code>\1</code>", item)
            out.append(f"<li>{item}</li>")
            continue

        _close_list()
        para = html.escape(stripped)
        para = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r'<a href="\2" target="_blank" rel="noopener">\1</a>', para)
        para = re.sub(r"`([^`]+)`", r"<code>\1</code>", para)
        out.append(f"<p>{para}</p>")

    _close_list()
    if in_code:
        out.append("</code></pre>")
    return "\n".join(out), sections


def _load_tutorial_page_data():
    if not TUTORIAL_MD_PATH.exists():
        return {
            "ok": False,
            "error": f"Tutorial source not found: {TUTORIAL_MD_PATH}",
            "html": "",
            "sections": [],
            "source_path": str(TUTORIAL_MD_PATH),
        }
    text = TUTORIAL_MD_PATH.read_text(encoding="utf-8", errors="replace")
    rendered, sections = _render_markdown_basic(text)
    return {
        "ok": True,
        "error": "",
        "html": rendered,
        "sections": sections,
        "source_path": str(TUTORIAL_MD_PATH),
    }


def _normalize_target_endpoint(value: Optional[str]) -> str:
    key = _clean_text(value).lower()
    if key in TARGET_ENDPOINTS:
        return key
    return "wikidata"


def _safe_filename_token(value: str, fallback: str = "value") -> str:
    text = _clean_text(value).lower()
    text = re.sub(r"[^a-z0-9._-]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("._-")
    return text or fallback


def _endpoint_filename_token(config: dict) -> str:
    endpoint = _normalize_target_endpoint(_clean_text(str((config or {}).get("target_endpoint", "wikidata"))))
    if endpoint != "custom":
        return _safe_filename_token(endpoint, fallback="wikidata")
    custom_url = _clean_text(str((config or {}).get("target_endpoint_url", "")))
    host = _clean_text(urlparse(custom_url).netloc).lower()
    if host:
        return _safe_filename_token(f"custom_{host}", fallback="custom")
    return "custom"


def _parse_property_mapping_rules_text(value: str):
    text = _clean_text(value)
    if not text:
        return []
    rows = []
    for line_no, raw_line in enumerate(text.splitlines(), 1):
        line = _clean_text(raw_line)
        if not line:
            continue
        norm = ""
        mapping_text = line
        if "||" in line:
            mapping_text, norm = line.split("||", 1)
            mapping_text = _clean_text(mapping_text)
            norm = _clean_text(norm)
        if "=>" not in mapping_text:
            raise ValueError(
                f"Invalid property mapping rule at line {line_no}: expected 'wdc_prop[,wdc_prop] => target_prop[,target_prop]'"
            )
        left_raw, right_raw = mapping_text.split("=>", 1)
        wdc_props = [_clean_text(tok) for tok in left_raw.split(",") if _clean_text(tok)]
        target_props = [_clean_text(tok) for tok in right_raw.split(",") if _clean_text(tok)]
        if not wdc_props:
            raise ValueError(
                f"Invalid property mapping rule at line {line_no}: left side must contain at least one property"
            )
        pair_ignore_chars = []
        pair_search_in = []
        row_mode = "property"
        norm_text = _clean_text(norm)
        if norm_text.startswith("["):
            try:
                decoded = json.loads(norm_text)
            except Exception:
                decoded = None
            if isinstance(decoded, list):
                pair_ignore_chars = [_clean_text(v) for v in decoded]
        elif norm_text.startswith("{"):
            try:
                decoded = json.loads(norm_text)
            except Exception:
                decoded = None
            if isinstance(decoded, dict):
                raw_ignore = decoded.get("ignore_chars")
                if isinstance(raw_ignore, list):
                    pair_ignore_chars = [_clean_text(v) for v in raw_ignore]
                raw_search = decoded.get("search_in")
                if isinstance(raw_search, list):
                    pair_search_in = [_normalize_wdc_pattern_search_in(v) for v in raw_search]
                raw_mode = _clean_text(str(decoded.get("mode", ""))).lower()
                if raw_mode in {"property", "sameas"}:
                    row_mode = raw_mode
        if row_mode == "property":
            if not target_props:
                raise ValueError(
                    f"Invalid property mapping rule at line {line_no}: right side must contain at least one property"
                )
            if len(wdc_props) != len(target_props):
                raise ValueError(
                    f"Invalid property mapping rule at line {line_no}: left/right property counts differ"
                )
        else:
            target_props = [""] * len(wdc_props)
        if pair_ignore_chars and len(pair_ignore_chars) != len(wdc_props):
            raise ValueError(
                f"Invalid property mapping rule at line {line_no}: per-pair normalization count differs from pair count"
            )
        if pair_search_in and len(pair_search_in) != len(wdc_props):
            raise ValueError(
                f"Invalid property mapping rule at line {line_no}: per-pair search mode count differs from pair count"
            )
        rows.append(
            {
                "line_no": line_no,
                "pairs": list(zip(wdc_props, target_props)),
                "raw": line,
                "ignore_chars": norm,
                "pair_ignore_chars": pair_ignore_chars,
                "pair_search_in": pair_search_in,
                "mode": row_mode,
            }
        )
    return rows


def _split_target_property_alternatives(value: str):
    raw = _clean_text(value)
    if not raw:
        return []
    parts = [_clean_text(tok) for tok in raw.split("|") if _clean_text(tok)]
    return parts or [raw]


def _load_build_config(build_dir: Path):
    cfg_path = build_dir / "BUILD_CONFIG.json"
    if not cfg_path.exists() or not cfg_path.is_file():
        return {}
    try:
        raw = json.loads(cfg_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}
    _sync_target_alias_fields(raw)
    return raw


def _extract_linking_combinations(config: dict):
    if not isinstance(config, dict):
        return []
    mode = _normalize_matching_mode(
        _clean_text(str(config.get("matching_mode", ""))),
        fallback_wdc_value_is_wikidata=_is_wikidata_url_mode(config),
    )
    if not _mode_includes_property(mode):
        return []

    combos = []
    rules_text = _clean_text(str(config.get("property_mapping_rules", "")))
    if rules_text:
        try:
            parsed = _parse_property_mapping_rules_text(rules_text)
        except Exception:
            parsed = []
        for i, row in enumerate(parsed, 1):
            row_mode = _clean_text(str(row.get("mode", "property"))).lower()
            if row_mode not in {"property", "sameas"}:
                row_mode = "property"
            pairs = [{"wdc": _clean_text(l), "target": _clean_text(r)} for l, r in (row.get("pairs") or [])]
            if row_mode == "sameas":
                pairs = [p for p in pairs if p["wdc"]]
            else:
                pairs = [p for p in pairs if p["wdc"] and p["target"]]
            if not pairs:
                continue
            combos.append(
                {
                    "id": i,
                    "label": f"OR #{i} ({'sameAs' if row_mode == 'sameas' else 'property'})",
                    "pairs": pairs,
                    "raw": _clean_text(row.get("raw", "")),
                    "mode": row_mode,
                }
            )
        return combos

    left = _clean_text(str(config.get("wdc_predicate_pattern", "")))
    right = _clean_text(str(config.get("target_property", config.get("wikidata_property", ""))))
    if left and right:
        combos.append(
            {
                "id": 1,
                "label": "Rule",
                "pairs": [{"wdc": left, "target": right}],
                "raw": f"{left} => {right}",
            }
        )
    return combos


def _extract_linking_elements(config: dict):
    if not isinstance(config, dict):
        return []
    out = []
    seen = set()
    combos = _extract_linking_combinations(config)
    for combo in list(combos or []):
        for pair in list((combo or {}).get("pairs") or []):
            left = _clean_text(str((pair or {}).get("wdc", "")))
            if not left:
                continue
            low = left.lower()
            if low in seen:
                continue
            seen.add(low)
            out.append(left)
    if out:
        return out
    fallback = _clean_text(str(config.get("wdc_predicate_pattern", "")))
    if fallback:
        return [fallback]
    return []


def _sync_target_alias_fields(params: dict):
    if not isinstance(params, dict):
        return params
    params["target_endpoint"] = _normalize_target_endpoint(params.get("target_endpoint"))
    params["target_endpoint_url"] = _clean_text(params.get("target_endpoint_url"))
    params["target_prefixes"] = _clean_text(params.get("target_prefixes"))
    params["property_mapping_rules"] = _clean_text(params.get("property_mapping_rules"))
    params["wdc_pattern_search_in"] = _normalize_wdc_pattern_search_in(params.get("wdc_pattern_search_in"))
    params["target_property"] = _clean_text(params.get("target_property") or params.get("wikidata_property"))
    params["target_class"] = _clean_text(params.get("target_class") or params.get("wkd_class"))
    # Backward-compatible aliases.
    params["wikidata_property"] = params["target_property"]
    params["wkd_class"] = params["target_class"]
    # One-to-one duplicate-key filtering is always enabled.
    params["strict_duplicate_key_filter"] = True
    if params["target_endpoint"] != "custom":
        params["target_endpoint_url"] = ""
    return params


def _normalize_matching_mode(value: Optional[str], fallback_wdc_value_is_wikidata: bool = False) -> str:
    mode = _clean_text(value).lower()
    if mode in {"property", "sameas", "sameas_or_property"}:
        return mode
    return "sameas" if bool(fallback_wdc_value_is_wikidata) else "property"


def _mode_includes_sameas(mode: Optional[str]) -> bool:
    return _normalize_matching_mode(mode) in {"sameas", "sameas_or_property"}


def _mode_includes_property(mode: Optional[str]) -> bool:
    return _normalize_matching_mode(mode) in {"property", "sameas_or_property"}


def _normalize_wdc_pattern_search_in(value: Optional[str]) -> str:
    mode = _clean_text(value).lower()
    if mode in {"value", "object"}:
        return "value"
    return "predicate"


def _is_wikidata_url_mode(params: dict) -> bool:
    return _normalize_matching_mode(
        (params or {}).get("matching_mode"),
        fallback_wdc_value_is_wikidata=bool((params or {}).get("wdc_value_is_wikidata")),
    ) == "sameas"


def _validate_and_normalize_job_params(raw_params: dict):
    params = dict(raw_params or {})
    _sync_target_alias_fields(params)
    params["matching_mode"] = _normalize_matching_mode(
        params.get("matching_mode"),
        fallback_wdc_value_is_wikidata=bool(params.get("wdc_value_is_wikidata")),
    )
    params.pop("wdc_value_is_wikidata", None)
    params["class_name"] = _clean_text(params.get("class_name"))
    params["parts_spec"] = _clean_text(params.get("parts_spec")) or "all"
    params["wdc_predicate_pattern"] = _clean_text(params.get("wdc_predicate_pattern"))
    params["wdc_pattern_search_in"] = _normalize_wdc_pattern_search_in(params.get("wdc_pattern_search_in"))
    params["target_endpoint"] = _normalize_target_endpoint(params.get("target_endpoint"))
    params["target_endpoint_url"] = _clean_text(params.get("target_endpoint_url"))
    params["target_prefixes"] = _clean_text(params.get("target_prefixes"))
    params["property_mapping_rules"] = _clean_text(params.get("property_mapping_rules"))
    params["target_property"] = _clean_text(params.get("target_property") or params.get("wikidata_property"))
    params["target_class"] = _clean_text(params.get("target_class") or params.get("wkd_class"))
    params["wikidata_property"] = params["target_property"]
    params["wkd_class"] = params["target_class"]
    params["ignore_chars"] = _clean_text(params.get("ignore_chars"))
    params["force_align"] = bool(params.get("force_align"))
    params["use_local_only"] = bool(params.get("use_local_only"))
    # One-to-one duplicate-key filtering is always enabled.
    params["strict_duplicate_key_filter"] = True

    if not params["class_name"]:
        return params, "Class name is required."
    if params["target_endpoint"] == "custom" and not params["target_endpoint_url"]:
        return params, "Custom endpoint URL is required when endpoint is set to Custom."
    if params["target_prefixes"]:
        for line in params["target_prefixes"].splitlines():
            prefix_line = _clean_text(line)
            if not prefix_line:
                continue
            if not TARGET_PREFIX_DECL_RE.match(prefix_line):
                return (
                    params,
                    "Custom prefixes must use one PREFIX declaration per line (e.g. PREFIX bd: <http://www.bigdata.com/rdf#>).",
                )

    parsed_rules = []
    if params["property_mapping_rules"]:
        try:
            parsed_rules = _parse_property_mapping_rules_text(params["property_mapping_rules"])
        except ValueError as exc:
            return params, str(exc)

    mode = _normalize_matching_mode(params.get("matching_mode"))
    includes_sameas = _mode_includes_sameas(mode)
    includes_property = _mode_includes_property(mode)
    rules_include_sameas = any(_clean_text(str(r.get("mode", "property"))).lower() == "sameas" for r in parsed_rules)
    rules_include_property = any(_clean_text(str(r.get("mode", "property"))).lower() != "sameas" for r in parsed_rules)
    effective_includes_sameas = includes_sameas or rules_include_sameas
    effective_includes_property = includes_property if not parsed_rules else rules_include_property

    if not params["target_class"]:
        return params, "Target class filter is required."

    if mode == "sameas" and not parsed_rules:
        params["target_property"] = ""
        params["wikidata_property"] = ""
        params["ignore_chars"] = ""
        params["property_mapping_rules"] = ""
    else:
        if not params["wdc_predicate_pattern"] and not parsed_rules:
            return params, "Considered pattern for WDC properties is required."
        if effective_includes_property and not params["ignore_chars"]:
            params["ignore_chars"] = "spaces;-;."
        if effective_includes_property and not params["target_property"] and not parsed_rules:
            return params, "Equivalent target property is required when WDC values are not endpoint URLs."

    params["wkd_class"] = params["target_class"]
    params["wikidata_property"] = params["target_property"]

    return params, None


def _is_test_class_name(class_name: Optional[str]) -> bool:
    name = _clean_text(class_name)
    if not name:
        return False
    lowered = name.lower()
    return lowered.startswith("testclass") or lowered.startswith("uxcheckclass")


def _is_test_preset(preset: dict) -> bool:
    if not isinstance(preset, dict):
        return False
    return _is_test_class_name(preset.get("class_name"))


def _filter_presets_by_mode(test_mode: bool):
    desired = bool(test_mode)
    return {k: v for k, v in PRESETS.items() if _is_test_preset(v) == desired}


def _get_recent_presets(limit=50, test_mode: Optional[bool] = None):
    rows = db.list_jobs(limit=limit)
    recent = []
    seen = set()
    for r in rows:
        try:
            params = json.loads(r["params_json"])
        except Exception:
            continue
        _sync_target_alias_fields(params)
        mode = _normalize_matching_mode(
            params.get("matching_mode"),
            fallback_wdc_value_is_wikidata=bool(params.get("wdc_value_is_wikidata")),
        )
        if test_mode is not None and _is_test_class_name(params.get("class_name")) != bool(test_mode):
            continue
        key = (
            mode,
            params.get("class_name", ""),
            params.get("parts_spec", ""),
            params.get("wdc_predicate_pattern", ""),
            params.get("wdc_pattern_search_in", "predicate"),
            params.get("target_endpoint", "wikidata"),
            params.get("target_endpoint_url", ""),
            params.get("target_prefixes", ""),
            params.get("property_mapping_rules", ""),
            params.get("target_property", ""),
            params.get("target_class", ""),
            params.get("ignore_chars", ""),
            params.get("strict_duplicate_key_filter", ""),
        )
        if key in seen:
            continue
        seen.add(key)
        endpoint_key = params.get("target_endpoint", "wikidata")
        endpoint_label = (TARGET_ENDPOINTS.get(endpoint_key) or {}).get("label", endpoint_key)
        target_hint = params.get("target_property", "") or ("Target URL" if _is_wikidata_url_mode(params) else "")
        label = (
            f"{params.get('class_name','')} | {params.get('parts_spec','')} | "
            f"{params.get('wdc_predicate_pattern','')} -> "
            f"{target_hint} ({endpoint_label})"
        )
        recent.append({"label": label, "params": params, "job_id": r["id"]})
    return recent


def _fmt_ts(ts):
    if not ts:
        return None
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(ts)))
    except Exception:
        return None


def _fmt_size(num_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    n = float(max(0, num_bytes))
    for unit in units:
        if n < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(n)} {unit}"
            return f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{num_bytes} B"


def _count_lines(path: Path) -> int:
    if not path.exists() or not path.is_file():
        return 0
    c = 0
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for _ in f:
            c += 1
    return c


def _looks_like_ent_links_header(line: str) -> bool:
    line = (line or "").strip()
    if not line:
        return False
    parts = line.split("\t")
    if len(parts) < 2:
        return False
    left = parts[0].strip().lower()
    right = parts[1].strip().lower()
    return left in {"wdc_iri", "wdc", "wdc_entity"} and right in {"wikidata_uri", "wikidata", "wikidata_entity"}


def _count_ent_links_rows(path: Path) -> int:
    if not path.exists() or not path.is_file():
        return 0
    total = _count_lines(path)
    if total <= 0:
        return 0
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            first = f.readline()
        if _looks_like_ent_links_header(first):
            return max(0, total - 1)
    except Exception:
        pass
    return total


def _parse_nq_or_nt(line: str):
    line = (line or "").strip()
    if not line or line.startswith("#"):
        return None
    m = _QUAD_RE.match(line)
    if m:
        s, p, o, _g = m.groups()
        return s, p, o
    m = _TRIPLE_RE.match(line)
    if m:
        s, p, o = m.groups()
        return s, p, o
    return None


def _literal_lex(value: str):
    value = value or ""
    if not value.startswith('"'):
        return None
    escape = False
    for i in range(1, len(value)):
        ch = value[i]
        if escape:
            escape = False
            continue
        if ch == "\\":
            escape = True
            continue
        if ch == '"':
            return value[1:i]
    return None


def _normalize_preflight_value(raw_value: str, ignore_chars_text: str):
    v = align_script.normalize_for_matching(raw_value or "")
    if not v:
        return ""
    try:
        extra = align_script.parse_strip_list(ignore_chars_text or "")
    except Exception:
        extra = set()
    if " " in extra:
        v = v.replace(" ", "")
    for ch in extra:
        if ch and ch != " ":
            v = v.replace(ch, "")
    return v


def _parse_parts_spec_numbers(parts_spec: str):
    spec = _clean_text(parts_spec) or "all"
    if spec.lower() == "all":
        return None, None
    wanted = set()
    try:
        if "," in spec:
            for token in spec.split(","):
                token = token.strip()
                if not token:
                    continue
                wanted.add(int(token))
        elif "-" in spec:
            left, right = spec.split("-", 1)
            start = int(left.strip())
            end = int(right.strip())
            if end < start:
                start, end = end, start
            for n in range(start, end + 1):
                wanted.add(n)
        else:
            wanted.add(int(spec.strip()))
    except Exception:
        return None, f"Invalid parts spec: '{parts_spec}'. Use all, 0-10, or 0,2,4."
    return sorted(wanted), None


def _discover_local_part_files(class_name: str):
    class_dir = Path("Download") / (class_name or "")
    if not class_dir.exists() or not class_dir.is_dir():
        return []
    files = []
    for fp in sorted(class_dir.iterdir()):
        if not fp.is_file():
            continue
        if not fp.name.startswith("part_"):
            continue
        if _part_number_from_name(fp.name) is None:
            # Ignore ad-hoc files like part_sample, part_echantillon_vickey, etc.
            continue
        if not (fp.name.endswith(".nq") or fp.name.endswith(".nt") or "." not in fp.name):
            continue
        files.append(fp)
    return files


def _discover_local_wikidata_files(class_name: str):
    class_dir = Path("Download") / (class_name or "")
    if not class_dir.exists() or not class_dir.is_dir():
        return []
    preferred = []
    fallback = []
    for fp in sorted(class_dir.iterdir()):
        if not fp.is_file():
            continue
        name = fp.name.lower()
        if not name.startswith("wikidata_"):
            continue
        if not name.endswith(".nt"):
            continue
        if name.endswith("_sakey_input.nt"):
            preferred.append(fp)
        else:
            fallback.append(fp)
    return preferred or fallback


def _select_local_part_files(class_name: str, parts_spec: str):
    spec = _clean_text(parts_spec) or "all"
    if spec.lower() == "all":
        wd_files = _discover_local_wikidata_files(class_name)
        if wd_files:
            return wd_files, []

    files = _discover_local_part_files(class_name)
    if not files:
        return [], []
    wanted_numbers, parse_error = _parse_parts_spec_numbers(parts_spec)
    if parse_error:
        return [], [parse_error]
    if wanted_numbers is None:
        warnings = []
        try:
            online_numbers, _online_err = _discover_online_part_numbers(class_name)
        except Exception:
            online_numbers = []
        if online_numbers:
            allowed = set(int(n) for n in online_numbers)
            filtered = []
            removed = []
            for fp in files:
                num = _part_number_from_name(fp.name)
                if num is None:
                    continue
                if num in allowed:
                    filtered.append(fp)
                else:
                    removed.append(num)
            if filtered:
                files = sorted(filtered, key=lambda p: p.name)
                if removed:
                    warnings.append(
                        f"Ignored non-official local parts for class {class_name}: {_format_part_ranges(sorted(set(removed)))}."
                    )
        return files, warnings

    files_by_num = {}
    for fp in files:
        num = _part_number_from_name(fp.name)
        if num is None:
            continue
        files_by_num.setdefault(num, []).append(fp)

    selected = []
    missing = []
    for num in wanted_numbers:
        if num in files_by_num:
            selected.extend(files_by_num[num])
        else:
            missing.append(num)
    selected.sort(key=lambda p: p.name)

    warnings = []
    if missing:
        warnings.append(f"Requested local parts not found: {_format_part_ranges(missing)}.")
    if not selected:
        warnings.append("No local part file matches this parts spec.")
    return selected, warnings


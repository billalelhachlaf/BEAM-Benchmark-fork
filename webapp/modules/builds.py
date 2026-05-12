def _variant_stats(base: Path, variant: str):
    p = base / variant
    if not p.exists() or not p.is_dir():
        return None
    files = {
        "ent_links": p / "ent_links",
        "attr_triples_1": p / "attr_triples_1",
        "rel_triples_1": p / "rel_triples_1",
        "attr_triples_2": p / "attr_triples_2",
        "rel_triples_2": p / "rel_triples_2",
        "prop_stats_wdc": p / "prop_stats_wdc.tsv",
        "prop_stats_wd": p / "prop_stats_wd.tsv",
    }
    size_total = 0
    for fp in files.values():
        if fp.exists() and fp.is_file():
            try:
                size_total += fp.stat().st_size
            except Exception:
                pass
    links_count = _count_ent_links_rows(files["ent_links"])
    wd_props = max(0, _count_lines(files["prop_stats_wd"]) - 1)
    wdc_props = max(0, _count_lines(files["prop_stats_wdc"]) - 1)
    top_wdc_props = _read_top_props(files["prop_stats_wdc"], limit=5)
    top_wd_props = _read_top_props(files["prop_stats_wd"], limit=5)
    sample_links = _read_ent_links_samples(files["ent_links"], limit=5)
    qa_warnings = []
    if links_count == 0:
        qa_warnings.append("No entity links generated.")
    if wdc_props == 0:
        qa_warnings.append("No WDC property stats found.")
    if wd_props == 0:
        qa_warnings.append("No target-side property stats found.")
    if links_count > 0 and not sample_links:
        qa_warnings.append("Could not read ent_links samples.")
    return {
        "name": variant,
        "path": str(p),
        "size_total_b": size_total,
        "size_total_h": _fmt_size(size_total),
        "links_count": links_count,
        "wd_props": wd_props,
        "wdc_props": wdc_props,
        "sample_links": sample_links,
        "top_wdc_props": top_wdc_props,
        "top_wd_props": top_wd_props,
        "qa_warnings": qa_warnings,
        "files": {k: str(v) for k, v in files.items() if v.exists()},
    }


def _load_build_stats(base: Path):
    stats_path = base / "BUILD_STATS.json"
    if not stats_path.exists() or not stats_path.is_file():
        return {}
    try:
        raw = json.loads(stats_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def _source_label_from_config(config: dict):
    cfg = config if isinstance(config, dict) else {}
    mode = _normalize_matching_mode(
        _clean_text(str(cfg.get("matching_mode", ""))),
        fallback_wdc_value_is_wikidata=_is_wikidata_url_mode(cfg),
    )
    if mode == "sameas":
        return "via sameas"

    combos = _extract_linking_combinations(cfg)
    if len(combos) == 1:
        combo = combos[0] if isinstance(combos[0], dict) else {}
        row_mode = _clean_text(str(combo.get("mode", ""))).lower()
        if row_mode == "sameas":
            return "via sameas"
        pairs = combo.get("pairs") or []
        if pairs and isinstance(pairs[0], dict):
            left = _clean_text(str(pairs[0].get("wdc", "")))
            if left:
                return f"via {left.lower()}"

    fallback = _clean_text(str(cfg.get("wdc_predicate_pattern", "")))
    if fallback:
        return f"via {fallback.lower()}"
    return "via unknown"


def _backfill_link_source_stats_if_missing(
    base: Path,
    build_stats: dict,
    build_config: dict,
    links_after: int,
):
    if not isinstance(build_stats, dict):
        build_stats = {}
    rows_after = build_stats.get("links_by_source_after_filter")
    rows_align = build_stats.get("links_by_source_align")
    has_after = isinstance(rows_after, list) and any(isinstance(r, dict) for r in rows_after)
    has_align = isinstance(rows_align, list) and any(isinstance(r, dict) for r in rows_align)
    if has_after and has_align:
        return build_stats

    source_label = _source_label_from_config(build_config if isinstance(build_config, dict) else {})
    links_after = max(0, int(links_after or 0))
    try:
        links_before = max(0, int(build_stats.get("links_before_filters", 0)))
    except Exception:
        links_before = 0

    changed = False
    if not has_after and links_after >= 0:
        build_stats["links_by_source_after_filter"] = [{"source": source_label, "count": links_after}]
        changed = True
    if not has_align:
        align_count = links_before if links_before > 0 else links_after
        build_stats["links_by_source_align"] = [{"source": source_label, "count": align_count}]
        changed = True
    if not changed:
        return build_stats

    try:
        stats_path = base / "BUILD_STATS.json"
        stats_path.write_text(json.dumps(build_stats, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass
    return build_stats


def _format_link_source_stats(stats: dict):
    if not isinstance(stats, dict):
        return ""
    rows = stats.get("links_by_source_after_filter")
    if not isinstance(rows, list) or not rows:
        rows = stats.get("links_by_source_align")
    if not isinstance(rows, list) or not rows:
        return ""
    parts = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        src = _clean_text(str(row.get("source", "")))
        try:
            cnt = int(row.get("count", 0))
        except Exception:
            cnt = 0
        if not src:
            continue
        parts.append(f"{cnt} {src}")
    return " | ".join(parts)


def _scan_builds(limit=30):
    builds = []
    root = Path("data")
    if not root.exists():
        return builds
    candidates = []
    for class_dir in root.iterdir():
        if not class_dir.is_dir():
            continue
        for base in class_dir.iterdir():
            if not _is_build_dir_candidate(base):
                continue
            candidates.append(base)
    for base in candidates:
        summary = _build_summary_from_dir(base)
        if summary:
            builds.append(summary)
    builds.sort(key=lambda b: float(b.get("sort_ts") or 0.0), reverse=True)
    if limit and int(limit) > 0:
        builds = builds[: int(limit)]
    for b in builds:
        b.pop("sort_ts", None)
    return builds


def _build_config_groups(cfg: dict):
    if not isinstance(cfg, dict):
        return []
    ordered = [
        ("Input", ["class_name"]),
        (
            "Matching",
            [
                "matching_mode",
                "wdc_predicate_pattern",
                "wdc_pattern_search_in",
                "target_endpoint",
                "target_endpoint_url",
                "target_prefixes",
                "property_mapping_rules",
                "target_property",
                "target_class",
                "ignore_chars",
            ],
        ),
        (
            "Build",
            [
                "force_align",
                "use_local_only",
                "strict_duplicate_key_filter",
                "build_name",
                "result_path",
            ],
        ),
    ]
    used = set()
    groups = []
    for title, keys in ordered:
        items = []
        for k in keys:
            if k in cfg:
                items.append((k, cfg[k]))
                used.add(k)
        if items:
            groups.append({"title": title, "items": items})
    ignored = {
        "parts_spec",
        "parts_count",
        "parts_total_size_human",
        "parts_total_size_bytes",
        "parts_manifest",
    }
    other = [(k, v) for k, v in cfg.items() if (k not in used and k not in ignored)]
    if other:
        groups.append({"title": "Other", "items": other})
    return groups


def _safe_unlink(path: str):
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def _resolve_build_dir(class_name: str, build_name: str):
    data_root = Path("data").resolve()
    base = (data_root / class_name / build_name).resolve()
    try:
        base.relative_to(data_root)
    except ValueError:
        return None
    if not _is_build_dir_candidate(base):
        return None
    return base


def _is_build_dir_candidate(base: Path) -> bool:
    if not base or not base.exists() or not base.is_dir():
        return False
    if not base.name.lower().startswith("beam"):
        return False
    marker = base / "BUILD_DONE"
    # History must only show completed builds.
    return marker.exists() and marker.is_file()


def _read_text_head(path: Path, max_chars: int = 12000):
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            return f.read(max_chars)
    except Exception:
        return ""


def _collect_sakey_insights(build_dir: Path):
    if not build_dir or not build_dir.exists() or not build_dir.is_dir():
        return {"available": False, "summary_lines": [], "artifacts": [], "primary": None}

    candidate_roots = []
    for rel in ("sakey", "SAKEY", "vickey", "VICKEY", "."):
        p = build_dir / rel
        if p.exists():
            candidate_roots.append(p)

    artifacts = []
    seen = set()
    allowed_suffixes = {".json", ".tsv", ".txt", ".out", ".log", ".csv"}
    for root in candidate_roots:
        if root.is_file():
            files = [root]
        else:
            try:
                files = [p for p in root.rglob("*") if p.is_file()]
            except Exception:
                files = []
        for p in files:
            try:
                rel = p.relative_to(build_dir)
            except Exception:
                continue
            rel_s = str(rel)
            if not _SAKEY_FILE_RE.search(rel_s):
                continue
            if p.suffix.lower() not in allowed_suffixes:
                continue
            if rel_s in seen:
                continue
            seen.add(rel_s)
            try:
                st = p.stat()
                mtime = float(st.st_mtime)
                size_b = int(st.st_size)
            except Exception:
                mtime = 0.0
                size_b = 0
            artifacts.append(
                {
                    "relative_path": rel_s,
                    "size_h": _fmt_size(size_b),
                    "size_b": size_b,
                    "mtime": mtime,
                    "mtime_h": _fmt_ts(mtime) if mtime > 0 else "",
                }
            )

    artifacts.sort(key=lambda a: (float(a.get("mtime") or 0.0), a.get("relative_path", "")), reverse=True)
    if not artifacts:
        return {"available": False, "summary_lines": [], "artifacts": [], "primary": None}

    primary = dict(artifacts[0])
    summary_lines = []
    primary_path = build_dir / primary["relative_path"]
    suffix = primary_path.suffix.lower()
    if suffix == ".json":
        try:
            payload = json.loads(primary_path.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
        if isinstance(payload, dict):
            ckeys = payload.get("conditional_keys")
            keys = payload.get("keys")
            if isinstance(ckeys, list):
                summary_lines.append(f"{len(ckeys)} conditional keys")
            if isinstance(keys, list):
                summary_lines.append(f"{len(keys)} keys")
            runtime = payload.get("runtime") or payload.get("runtime_seconds")
            if runtime is not None:
                summary_lines.append(f"runtime: {runtime}")
            if not summary_lines:
                summary_lines.append("JSON report detected")
        else:
            summary_lines.append("JSON report detected")
    else:
        head = _read_text_head(primary_path)
        m = _SAKEY_VICKEY_KEYS_RE.search(head)
        if m:
            summary_lines.append(f"{int(m.group(1))} conditional keys")
        m = _SAKEY_KEYS_RE.search(head)
        if m:
            summary_lines.append(f"{int(m.group(1))} keys")
        m = _SAKEY_NON_KEYS_RE.search(head)
        if m:
            summary_lines.append(f"{int(m.group(1))} non-keys found")
        if suffix == ".tsv":
            try:
                nrows = max(0, _count_lines(primary_path) - 1)
                summary_lines.append(f"{nrows} rows")
            except Exception:
                pass
        if not summary_lines:
            summary_lines.append("Text report detected")

    for idx, item in enumerate(artifacts):
        item["download_url"] = (
            f"/builds/{quote(build_dir.parent.name, safe='')}/{quote(build_dir.name, safe='')}/sakey/download/{idx}"
        )

    return {
        "available": True,
        "summary_lines": summary_lines[:5],
        "artifacts": artifacts[:25],
        "primary": primary,
    }


def _resolve_sakey_artifact(build_dir: Path, artifact_idx: int):
    info = _collect_sakey_insights(build_dir)
    artifacts = info.get("artifacts") or []
    if not artifacts:
        return None
    try:
        idx = int(artifact_idx)
    except Exception:
        return None
    if idx < 0 or idx >= len(artifacts):
        return None
    rel = str(artifacts[idx].get("relative_path", "")).strip()
    if not rel:
        return None
    path = (build_dir / rel).resolve()
    try:
        path.relative_to(build_dir.resolve())
    except Exception:
        return None
    if not path.exists() or not path.is_file():
        return None
    return path


def _build_summary_from_dir(base: Path):
    if not _is_build_dir_candidate(base):
        return None
    marker = base / "BUILD_DONE"
    done_at = ""
    sort_ts = 0.0
    if marker.exists() and marker.is_file():
        try:
            st = marker.stat()
            done_at = _fmt_ts(st.st_mtime)
            sort_ts = float(st.st_mtime)
        except Exception:
            done_at = ""
            sort_ts = 0.0

    build_config = None
    cfg_path = base / "BUILD_CONFIG.json"
    if cfg_path.exists():
        try:
            build_config = json.loads(cfg_path.read_text(encoding="utf-8"))
        except Exception:
            build_config = None

    with_link = _variant_stats(base, "with_link_code")
    without_link = _variant_stats(base, "without_link_code")
    build_stats = _load_build_stats(base)
    links_after = 0
    if with_link:
        links_after = int(with_link.get("links_count", 0) or 0)
    elif without_link:
        links_after = int(without_link.get("links_count", 0) or 0)
    build_stats = _backfill_link_source_stats_if_missing(base, build_stats, build_config or {}, links_after)
    if not marker.exists() and not build_config and not with_link and not without_link:
        return None

    if sort_ts <= 0:
        for p in (cfg_path, base / "with_link_code", base / "without_link_code", base):
            try:
                if p.exists():
                    sort_ts = max(sort_ts, float(p.stat().st_mtime))
            except Exception:
                pass
    if not done_at and sort_ts > 0:
        done_at = _fmt_ts(sort_ts)

    variants_same = False
    if with_link and without_link:
        variants_same = (
            with_link["size_total_b"] == without_link["size_total_b"]
            and with_link["links_count"] == without_link["links_count"]
            and with_link["wdc_props"] == without_link["wdc_props"]
            and with_link["wd_props"] == without_link["wd_props"]
        )

    build = {
        "class_name": base.parent.name,
        "build_name": base.name,
        "path": str(base),
        "done_at": done_at,
        "is_completed": bool(marker.exists()),
        "done_label": "Completed" if marker.exists() else "Last update",
        "with_link": with_link,
        "without_link": without_link,
        "variants_same": variants_same,
        "build_config": build_config,
        "build_stats": build_stats,
        "linking_stats_text": _format_link_source_stats(build_stats),
        "sort_ts": sort_ts,
    }

    config = build_config if isinstance(build_config, dict) else None
    if config:
        _sync_target_alias_fields(config)
        build["config"] = config
    else:
        build["config"] = {
            "class_name": build["class_name"],
            "build_name": build["build_name"],
            "result_path": build["path"],
            "config_source": "inferred",
            "wdc_pattern_search_in": "predicate",
            "target_endpoint": "wikidata",
            "target_endpoint_url": "",
            "target_prefixes": "",
            "property_mapping_rules": "",
            "target_property": "",
            "target_class": "",
        }

    parts = build["config"].get("parts_manifest")
    if not isinstance(parts, list):
        parts = []
    build["parts_manifest"] = parts
    build["parts_count"] = build["config"].get("parts_count", len(parts))
    build["parts_total_size_human"] = build["config"].get("parts_total_size_human")
    build["config_groups"] = _build_config_groups(build["config"])
    build["linking_combinations"] = _extract_linking_combinations(build["config"])
    endpoint_key = _normalize_target_endpoint(_clean_text(str(build["config"].get("target_endpoint", "wikidata"))))
    endpoint_label = _clean_text(str((TARGET_ENDPOINTS.get(endpoint_key) or {}).get("label", ""))) or "Wikidata"
    linking_elements = _extract_linking_elements(build["config"])
    build["endpoint_label"] = endpoint_label
    build["linking_elements"] = linking_elements
    build["linking_elements_text"] = ", ".join(linking_elements)
    build["sakey"] = _collect_sakey_insights(base)
    return build



import os
import json
import hashlib
import time
import errno
import re
import unicodedata
import gzip
from collections import defaultdict
from pathlib import Path
from types import SimpleNamespace

from scripts import align
from scripts import build_beam_files as build


class PipelineError(RuntimeError):
    pass


def _discover_wdc_files(download_dir):
    candidates = []
    if os.path.isdir(download_dir):
        for name in sorted(os.listdir(download_dir)):
            if name.startswith("part_") and (
                name.endswith(".nq") or name.endswith(".nt") or "." not in name
            ):
                candidates.append(os.path.join(download_dir, name))
    return candidates


def _count_local_parts(download_dir):
    if not os.path.isdir(download_dir):
        return 0
    count = 0
    for name in os.listdir(download_dir):
        if name.startswith("part_") and (name.endswith(".nq") or name.endswith(".nt") or "." not in name):
            count += 1
    return count


def _part_number_from_filename(name):
    m = re.search(r"part_0*(\d+)", str(name or ""))
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _select_local_part_files(download_dir, parts_spec):
    files = [Path(p) for p in _discover_wdc_files(download_dir)]
    if not files:
        return []
    if (parts_spec or "").lower() == "all":
        return files
    wanted_parts = {
        _part_number_from_filename(name)
        for name in align.parse_parts_spec(parts_spec, available_parts=None)
    }
    wanted_parts.discard(None)
    selected = []
    for fp in files:
        if _part_number_from_filename(fp.name) in wanted_parts:
            selected.append(fp)
    return selected


def _fmt_size(num_bytes):
    units = ["B", "KB", "MB", "GB", "TB"]
    n = float(max(0, int(num_bytes)))
    for unit in units:
        if n < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(n)} {unit}"
            return f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{int(num_bytes)} B"


def _timestamp_tag():
    return time.strftime("%Y%m%d_%H%M%S")


def _looks_like_ent_links_header(line):
    text = str(line or "").strip().lower()
    if not text:
        return False
    parts = text.split("\t")
    if len(parts) < 2:
        return False
    left = parts[0].strip()
    right = parts[1].strip()
    return (
        (left in {"wdc", "wdc_iri", "wdc_entity"} and right in {"wikidata", "wikidata_uri", "target", "target_uri"})
        or (left == "subject" and right == "object")
    )


def _count_ent_links_rows(path):
    fp = Path(path)
    if not fp.exists() or not fp.is_file():
        return 0
    count = 0
    with fp.open("r", encoding="utf-8", errors="ignore") as f:
        header_checked = False
        for raw in f:
            line = raw.rstrip("\n")
            if not line:
                continue
            if not header_checked:
                header_checked = True
                if _looks_like_ent_links_header(line):
                    continue
            parts = line.split("\t")
            if len(parts) < 2:
                continue
            count += 1
    return count


def _config_hash(align_params):
    payload = json.dumps(align_params, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


_RUNTIME_ONLY_PARAM_KEYS = {
    # Runtime/recovery controls (not user-visible benchmark config)
    "require_cached_align",
    "resume_build",
    "resume_out_dir",
    "resume_checkpoint_at",
    "resume_checkpoint_reason",
    "resume_checkpoint_step",
}


def _full_config_for_cache(params):
    data = params if isinstance(params, dict) else {}
    out = {}
    for k, v in data.items():
        if k in _RUNTIME_ONLY_PARAM_KEYS:
            continue
        out[k] = v
    return out


def _full_config_hash(params):
    payload = json.dumps(_full_config_for_cache(params), sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _normalize_matching_mode(value, fallback_wdc_value_is_wikidata=False):
    mode = str(value or "").strip().lower()
    if mode in {"property", "sameas", "sameas_or_property"}:
        return mode
    return "sameas" if bool(fallback_wdc_value_is_wikidata) else "property"


def _mode_includes_sameas(mode):
    return _normalize_matching_mode(mode) in {"sameas", "sameas_or_property"}


def _mode_includes_property(mode):
    return _normalize_matching_mode(mode) in {"property", "sameas_or_property"}


def _normalize_wdc_pattern_search_in(value):
    mode = str(value or "predicate").strip().lower()
    if mode in {"value", "object"}:
        return "value"
    return "predicate"


def _is_wikidata_url_mode(params):
    data = params if isinstance(params, dict) else {}
    return _normalize_matching_mode(
        data.get("matching_mode"),
        fallback_wdc_value_is_wikidata=bool(data.get("wdc_value_is_wikidata")),
    ) == "sameas"


def _parse_property_mapping_rules(value):
    text = str(value or "").strip()
    if not text:
        return []
    rules = []
    for line_no, raw_line in enumerate(text.splitlines(), 1):
        line = str(raw_line or "").strip()
        if not line:
            continue
        norm = ""
        mapping_text = line
        if "||" in line:
            mapping_text, norm = line.split("||", 1)
            mapping_text = str(mapping_text or "").strip()
            norm = str(norm or "").strip()
        if "=>" not in mapping_text:
            raise PipelineError(
                f"Invalid property mapping rule at line {line_no}: expected 'wdc_prop[,wdc_prop] => target_prop[,target_prop]'"
            )
        left_raw, right_raw = mapping_text.split("=>", 1)
        wdc_props = [tok.strip() for tok in left_raw.split(",") if tok.strip()]
        target_props = [tok.strip() for tok in right_raw.split(",") if tok.strip()]
        if not wdc_props:
            raise PipelineError(
                f"Invalid property mapping rule at line {line_no}: left side must contain at least one property"
            )
        pair_ignore_chars = []
        pair_search_in = []
        row_mode = "property"
        norm_text = str(norm or "").strip()
        if norm_text.startswith("["):
            try:
                decoded = json.loads(norm_text)
            except Exception:
                decoded = None
            if isinstance(decoded, list):
                pair_ignore_chars = [str(v or "").strip() for v in decoded]
        elif norm_text.startswith("{"):
            try:
                decoded = json.loads(norm_text)
            except Exception:
                decoded = None
            if isinstance(decoded, dict):
                raw_ignore = decoded.get("ignore_chars")
                if isinstance(raw_ignore, list):
                    pair_ignore_chars = [str(v or "").strip() for v in raw_ignore]
                raw_search = decoded.get("search_in")
                if isinstance(raw_search, list):
                    pair_search_in = [_normalize_wdc_pattern_search_in(v) for v in raw_search]
                raw_mode = str(decoded.get("mode") or "").strip().lower()
                if raw_mode in {"sameas", "property"}:
                    row_mode = raw_mode
        if row_mode == "property":
            if not target_props:
                raise PipelineError(
                    f"Invalid property mapping rule at line {line_no}: right side must contain at least one property"
                )
            if len(wdc_props) != len(target_props):
                raise PipelineError(
                    f"Invalid property mapping rule at line {line_no}: left/right property counts differ"
                )
        else:
            target_props = [""] * len(wdc_props)
        if pair_ignore_chars and len(pair_ignore_chars) != len(wdc_props):
            raise PipelineError(
                f"Invalid property mapping rule at line {line_no}: per-pair normalization count differs from pair count"
            )
        if pair_search_in and len(pair_search_in) != len(wdc_props):
            raise PipelineError(
                f"Invalid property mapping rule at line {line_no}: per-pair search mode count differs from pair count"
            )
        pairs = list(zip(wdc_props, target_props))
        rules.append(
            {
                "line_no": line_no,
                "pairs": pairs,
                "raw": line,
                "ignore_chars": norm,
                "pair_ignore_chars": pair_ignore_chars,
                "pair_search_in": pair_search_in,
                "mode": row_mode,
            }
        )
    return rules


def _split_target_property_alternatives(value):
    raw = str(value or "").strip()
    if not raw:
        return []
    parts = [p.strip() for p in raw.split("|") if p.strip()]
    return parts or [raw]


def _merge_value_maps(dst_map, src_map):
    if not isinstance(src_map, dict):
        return
    for norm, entries in src_map.items():
        if norm not in dst_map:
            dst_map[norm] = []
        dst_map[norm].extend(list(entries or []))


def _distinct_wdc_values(wdc_map):
    out = set()
    if not isinstance(wdc_map, dict):
        return []
    for entries in wdc_map.values():
        for pair in list(entries or []):
            if not isinstance(pair, (list, tuple)) or len(pair) < 1:
                continue
            value = str(pair[0] or "").strip()
            if value:
                out.add(value)
    return sorted(out)


def _normalize_link_source_key(raw):
    text = str(raw or "").strip()
    if not text:
        return ""
    lowered = text.lower()
    if "wikidata.org/wiki" in lowered or "wikidata.org/entity" in lowered:
        return "wikidata"
    if lowered.startswith("<") and lowered.endswith(">"):
        lowered = lowered[1:-1]
    for sep in ("#", "/"):
        if sep in lowered:
            lowered = lowered.rsplit(sep, 1)[-1]
    lowered = lowered.replace("http://", "").replace("https://", "").strip()
    return lowered or text


def _source_label_from_method(method, fallback_pattern=""):
    method_text = str(method or "").strip()
    fallback = _normalize_link_source_key(fallback_pattern)
    token = ""
    if method_text:
        token = method_text.split("|")[-1].strip()
    if token.lower().startswith("sameas:"):
        source_key = _normalize_link_source_key(token.split(":", 1)[1])
    elif "->" in token:
        source_key = _normalize_link_source_key(token.split("->", 1)[0])
    elif token and token.lower() not in {"exact", "fuzzy"}:
        source_key = _normalize_link_source_key(token)
    else:
        source_key = fallback or "unknown"
    if not source_key:
        source_key = "unknown"
    return f"via {source_key}"


def _build_pair_source_map(matches, fallback_pattern=""):
    out = {}
    for item in list(matches or []):
        wdc_iri = str((item or {}).get("wdc_iri") or "").strip()
        wd_iri = str((item or {}).get("wikidata_uri") or "").strip()
        if not wdc_iri or not wd_iri:
            continue
        key = (wdc_iri, wd_iri)
        if key in out:
            continue
        out[key] = _source_label_from_method((item or {}).get("method"), fallback_pattern=fallback_pattern)
    return out


def _count_sources_for_pairs(wdc_entities, wd_entities, pair_source_map):
    counts = {}
    total = min(len(wdc_entities or []), len(wd_entities or []))
    for i in range(total):
        pair = (str(wdc_entities[i] or "").strip(), str(wd_entities[i] or "").strip())
        if not pair[0] or not pair[1]:
            continue
        source = str((pair_source_map or {}).get(pair) or "via unknown")
        counts[source] = counts.get(source, 0) + 1
    rows = [{"source": src, "count": int(cnt)} for src, cnt in counts.items()]
    rows.sort(key=lambda x: (-x["count"], x["source"]))
    return rows


def _pair_source_map_from_links_tsv(path, fallback_pattern=""):
    out = {}
    tsv_path = Path(path)
    if not tsv_path.exists() or not tsv_path.is_file():
        return out
    try:
        with tsv_path.open("r", encoding="utf-8", errors="ignore") as f:
            header = f.readline().rstrip("\n").split("\t")
            idx_wdc = header.index("wdc_iri")
            idx_wd = header.index("wikidata_uri")
            idx_method = header.index("method") if "method" in header else -1
            for raw in f:
                parts = raw.rstrip("\n").split("\t")
                if idx_wdc >= len(parts) or idx_wd >= len(parts):
                    continue
                pair = (parts[idx_wdc].strip(), parts[idx_wd].strip())
                if not pair[0] or not pair[1] or pair in out:
                    continue
                method = parts[idx_method].strip() if idx_method >= 0 and idx_method < len(parts) else ""
                out[pair] = _source_label_from_method(method, fallback_pattern=fallback_pattern)
    except Exception:
        return {}
    return out


def _should_prefilter_wikidata_by_wdc_values(target_endpoint, target_property, target_class):
    if align.normalize_target_endpoint_key(target_endpoint) != "wikidata":
        return False
    if not str(target_class or "").strip():
        return False
    normalized_prop = align.normalize_wikidata_property(target_property)
    return str(normalized_prop or "").strip().lower() == "wdt:p297"


def _fetch_wikidata_values_for_alignment(target_property, target_class, wkd_prop_class, wdc_map=None):
    if _should_prefilter_wikidata_by_wdc_values("wikidata", target_property, target_class):
        candidate_values = _distinct_wdc_values(wdc_map)
        if candidate_values:
            print(
                "[INFO] Wikidata prefilter enabled for P297: "
                f"{len(candidate_values):,} WDC value(s) sent as VALUES batches."
            )
            try:
                return align.fetch_wikidata_values(
                    target_property,
                    target_class,
                    wkd_prop_class,
                    value_candidates=candidate_values,
                )
            except TypeError:
                # Backward compatibility for tests/stubs that do not accept the new kwarg.
                pass
    return align.fetch_wikidata_values(
        target_property,
        target_class,
        wkd_prop_class,
    )


def _fetch_target_values_for_alignment(
    target_property,
    target_class,
    target_prop_class,
    target_endpoint,
    target_endpoint_url,
    target_prefixes,
    wdc_map=None,
):
    candidate_values = _distinct_wdc_values(wdc_map)
    if candidate_values:
        try:
            return align.fetch_target_values(
                target_property,
                target_class,
                target_prop_class,
                value_candidates=candidate_values,
                target_endpoint=target_endpoint,
                target_endpoint_url=target_endpoint_url,
                target_prefixes=target_prefixes,
            )
        except TypeError:
            # Backward compatibility for tests/stubs that do not accept value_candidates.
            pass
    return align.fetch_target_values(
        target_property,
        target_class,
        target_prop_class,
        target_endpoint=target_endpoint,
        target_endpoint_url=target_endpoint_url,
        target_prefixes=target_prefixes,
    )


def _set_align_normalization(ignore_spec):
    spec = str(ignore_spec or "").strip()
    if spec:
        align.set_normalization(True)
        align.set_extra_strip_chars(align.parse_strip_list(spec))
    else:
        align.set_normalization(False)


def _align_params_from_job_params(params):
    data = params if isinstance(params, dict) else {}
    wdc_value_is_wikidata = _normalize_matching_mode(
        data.get("matching_mode"),
        fallback_wdc_value_is_wikidata=bool(data.get("wdc_value_is_wikidata")),
    ) == "sameas"
    target_property = data.get("target_property")
    if target_property in {None, ""}:
        target_property = data.get("wikidata_property")
    target_class = data.get("target_class")
    if target_class in {None, ""}:
        target_class = data.get("wkd_class")
    target_endpoint = data.get("target_endpoint") or "wikidata"
    target_endpoint_url = data.get("target_endpoint_url") or None
    target_prefixes = data.get("target_prefixes") or None
    property_mapping_rules = data.get("property_mapping_rules") or None
    out = {
        "class_name": data.get("class_name"),
        "parts_spec": data.get("parts_spec") or "all",
        "pattern": data.get("wdc_predicate_pattern"),
        "pattern_search_in": _normalize_wdc_pattern_search_in(data.get("wdc_pattern_search_in")),
        "wikidata_property": target_property or None,
        "wkd_class": target_class or None,
        "ignore_chars": data.get("ignore_chars") or None,
        "wdc_value_is_wikidata": bool(wdc_value_is_wikidata),
    }
    # Add endpoint-specific keys only when not using the historical default config.
    if (target_endpoint or "wikidata") != "wikidata" or (target_endpoint_url or ""):
        out["target_property"] = target_property or None
        out["target_class"] = target_class or None
        out["target_endpoint"] = target_endpoint or "wikidata"
        out["target_endpoint_url"] = target_endpoint_url or None
    if target_prefixes:
        out["target_prefixes"] = target_prefixes
    if property_mapping_rules:
        out["property_mapping_rules"] = property_mapping_rules
    return out


def _align_cache_dir_for_params(params):
    align_params = _align_params_from_job_params(params)
    class_name = str(align_params.get("class_name") or "").strip()
    if not class_name:
        return None, align_params
    cache_hash = _config_hash(align_params)
    cache_dir = Path("Download") / class_name / "align_cache" / cache_hash
    return cache_dir, align_params


def _wdc_extract_sources_manifest(paths):
    manifest = []
    for raw in list(paths or []):
        fp = Path(raw)
        try:
            st = fp.stat()
            size_b = int(st.st_size)
            mtime_ns = int(st.st_mtime_ns)
        except Exception:
            size_b = 0
            mtime_ns = 0
        manifest.append(
            {
                "path": str(fp),
                "name": fp.name,
                "size_bytes": size_b,
                "mtime_ns": mtime_ns,
            }
        )
    manifest.sort(key=lambda row: row.get("path") or "")
    return manifest


def _wdc_extract_cache_hash(
    class_name,
    parts_spec,
    pattern,
    search_in,
    wdc_value_is_wd_iri,
    type_filter_iris,
    ignore_chars,
    sources_manifest,
):
    payload = {
        "class_name": str(class_name or "").strip(),
        "parts_spec": str(parts_spec or "all"),
        "pattern": str(pattern or ""),
        "search_in": _normalize_wdc_pattern_search_in(search_in),
        "wdc_value_is_wd_iri": bool(wdc_value_is_wd_iri),
        "type_filter_iris": sorted({str(v or "").strip() for v in list(type_filter_iris or []) if str(v or "").strip()}),
        "ignore_chars": str(ignore_chars or "").strip(),
        "sources_manifest": list(sources_manifest or []),
    }
    return _config_hash(payload)


def _wdc_extract_cache_paths(work_dir, cache_hash):
    base = Path(work_dir) / "wdc_extract_cache" / str(cache_hash)
    return {
        "base": base,
        "meta": base / "WDC_EXTRACT_META.json",
        "data": base / "WDC_EXTRACT_MAP.jsonl.gz",
        "done": base / "WDC_EXTRACT_DONE",
    }


def _save_wdc_extract_cache(paths, meta_payload, wdc_map):
    base = paths["base"]
    base.mkdir(parents=True, exist_ok=True)
    tmp_data = paths["data"].with_suffix(paths["data"].suffix + ".tmp")
    tmp_meta = paths["meta"].with_suffix(paths["meta"].suffix + ".tmp")
    try:
        with gzip.open(tmp_data, "wt", encoding="utf-8") as f:
            for norm, entries in (wdc_map or {}).items():
                norm_text = str(norm or "")
                for pair in list(entries or []):
                    if not isinstance(pair, (list, tuple)) or len(pair) < 2:
                        continue
                    raw_value = str(pair[0] or "")
                    iri = str(pair[1] or "")
                    f.write(json.dumps([norm_text, raw_value, iri], ensure_ascii=False) + "\n")
        tmp_meta.write_text(
            json.dumps(meta_payload, indent=2, ensure_ascii=False, sort_keys=True),
            encoding="utf-8",
        )
        tmp_data.replace(paths["data"])
        tmp_meta.replace(paths["meta"])
        paths["done"].write_text(time.strftime("%Y-%m-%d %H:%M:%S"), encoding="utf-8")
        return True
    except Exception:
        try:
            if tmp_data.exists():
                tmp_data.unlink()
        except Exception:
            pass
        try:
            if tmp_meta.exists():
                tmp_meta.unlink()
        except Exception:
            pass
        return False


def _load_wdc_extract_cache(paths):
    if not (paths["done"].exists() and paths["meta"].exists() and paths["data"].exists()):
        return None
    try:
        payload = json.loads(paths["meta"].read_text(encoding="utf-8") or "{}")
    except Exception:
        return None
    out = defaultdict(list)
    try:
        with gzip.open(paths["data"], "rt", encoding="utf-8") as f:
            for raw_line in f:
                line = str(raw_line or "").strip()
                if not line:
                    continue
                row = json.loads(line)
                if not isinstance(row, list) or len(row) < 3:
                    continue
                norm, raw_value, iri = row[0], row[1], row[2]
                out[str(norm or "")].append((str(raw_value or ""), str(iri or "")))
    except Exception:
        return None
    if not out:
        # Empty extract maps are valid, but not useful for alignment cache reuse.
        # Keep strict behavior and force recomputation for this edge case.
        return None
    matched_count = int(payload.get("matched_count", 0) or 0)
    return dict(out), matched_count, payload


def _extract_wdc_values_with_cache(
    work_dir,
    class_name,
    parts_spec,
    decompressed_files,
    pattern,
    search_in,
    wdc_value_is_wd_iri,
    type_filter_iris,
    ignore_chars,
    force_refresh,
    workers,
    lock_path,
    progress_every=100,
):
    sources_manifest = _wdc_extract_sources_manifest(decompressed_files)
    cache_hash = _wdc_extract_cache_hash(
        class_name=class_name,
        parts_spec=parts_spec,
        pattern=pattern,
        search_in=search_in,
        wdc_value_is_wd_iri=wdc_value_is_wd_iri,
        type_filter_iris=type_filter_iris,
        ignore_chars=ignore_chars,
        sources_manifest=sources_manifest,
    )
    paths = _wdc_extract_cache_paths(work_dir, cache_hash)
    if not force_refresh:
        cached = _load_wdc_extract_cache(paths)
        if cached is not None:
            wdc_map, matched_count, _meta = cached
            print(
                "[WDC_CACHE] reuse "
                f"{cache_hash} | values={len(wdc_map):,} | matched={int(matched_count):,}"
            )
            return wdc_map, matched_count, True

    try:
        wdc_map, matched_count = align.extract_unique_iris_from_files(
            decompressed_files,
            pattern,
            collect_top_props=False,
            parallel=True,
            workers=workers,
            lock_path=lock_path,
            progress_every=progress_every,
            wdc_value_is_wd_iri=wdc_value_is_wd_iri,
            type_filter_iris=type_filter_iris,
            search_in=search_in,
        )
    except Exception as e:
        if not _is_too_many_open_files(e):
            raise
        print(
            "[WARN] Too many open files detected during align extraction; "
            "retrying in low-FD mode (parallel disabled)."
        )
        wdc_map, matched_count = align.extract_unique_iris_from_files(
            decompressed_files,
            pattern,
            collect_top_props=False,
            parallel=False,
            workers=1,
            lock_path=lock_path,
            progress_every=progress_every,
            wdc_value_is_wd_iri=wdc_value_is_wd_iri,
            type_filter_iris=type_filter_iris,
            search_in=search_in,
        )

    entries_count = int(sum(len(list(v or [])) for v in (wdc_map or {}).values()))
    meta_payload = {
        "cache_hash": cache_hash,
        "class_name": str(class_name or "").strip(),
        "parts_spec": str(parts_spec or "all"),
        "pattern": str(pattern or ""),
        "search_in": _normalize_wdc_pattern_search_in(search_in),
        "wdc_value_is_wd_iri": bool(wdc_value_is_wd_iri),
        "ignore_chars": str(ignore_chars or "").strip(),
        "type_filter_iris": sorted(
            {
                str(v or "").strip()
                for v in list(type_filter_iris or [])
                if str(v or "").strip()
            }
        ),
        "sources_manifest": sources_manifest,
        "matched_count": int(matched_count or 0),
        "normalized_values_count": int(len(wdc_map or {})),
        "entries_count": entries_count,
        "saved_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    if _save_wdc_extract_cache(paths, meta_payload, wdc_map or {}):
        print(
            "[WDC_CACHE] save "
            f"{cache_hash} | values={len(wdc_map or {}):,} | matched={int(matched_count or 0):,}"
        )
    return wdc_map, matched_count, False


def _align_cache_config_matches(cache_dir: Path, params: dict) -> bool:
    if not cache_dir:
        return False
    config_path = Path(cache_dir) / "ALIGN_CONFIG.json"
    if not config_path.exists():
        return False
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8") or "{}")
    except Exception:
        return False

    expected_hash = _full_config_hash(params)
    cached_hash = str(payload.get("full_config_hash") or "").strip()
    if cached_hash:
        return cached_hash == expected_hash

    cached_full = payload.get("full_config")
    if isinstance(cached_full, dict):
        return _full_config_for_cache(cached_full) == _full_config_for_cache(params)
    return False


def is_align_cache_reusable(params) -> bool:
    cache_dir, _align_params = _align_cache_dir_for_params(params)
    if not cache_dir:
        return False
    links_tsv = cache_dir / "wdc_wikidata_links.tsv"
    align_done = cache_dir / "ALIGN_DONE"
    if not (links_tsv.exists() and align_done.exists()):
        return False
    return _align_cache_config_matches(cache_dir, params)


def _is_too_many_open_files(exc: Exception) -> bool:
    if isinstance(exc, OSError) and getattr(exc, "errno", None) == errno.EMFILE:
        return True
    return "Too many open files" in str(exc)


def _count_alignment_pairs(links_tsv: Path) -> int:
    if not links_tsv.exists():
        return 0
    total = 0
    with links_tsv.open("r", encoding="utf-8", errors="ignore") as f:
        for _ in f:
            total += 1
    return max(0, total - 1)  # minus header


def _canonical_wdc_link_entity(value) -> str:
    return str(value or "").strip().strip("<>")


def _canonical_wd_link_entity(value) -> str:
    raw = str(value or "").strip()
    try:
        return build.canonical_wd_link_entity_uri(build.normalize_wd_uri(raw, True))
    except Exception:
        return raw


def _canonical_wdc_token(value) -> str:
    return str(value or "").strip().strip("<>")


def _fast_subject_key_from_nq_line(line):
    """
    Fast subject extraction without full N-Quad regex parsing.

    Returns canonical subject key:
    - IRI subject: without angle brackets
    - blank node subject: unchanged (e.g., _:b0)
    """
    if not line:
        return None
    first = line[0]
    if first.isspace():
        stripped = line.lstrip()
        if not stripped:
            return None
        line = stripped
        first = line[0]
    if first == "<":
        end = line.find(">")
        if end <= 1:
            return None
        if end + 1 >= len(line) or not line[end + 1].isspace():
            return None
        return line[1:end]
    if first == "_" and line.startswith("_:"):
        sep = line.find(" ")
        if sep == -1:
            sep = line.find("\t")
        if sep <= 2:
            return None
        return line[:sep]
    return None


_LINK_EXPLORER_VARIANTS = ("with_link_code", "without_link_code")
_LINK_EXPLORER_FAST_SCAN_BYTES = 64 * 1024 * 1024  # 64 MB
_LINK_DETAIL_CACHE_MAX = 256
_LINK_DETAIL_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="link-detail")
_LINK_DETAIL_CACHE = OrderedDict()
_LINK_DETAIL_FUTURES = {}
_LINK_DETAIL_LOCK = Lock()
_LINK_EXPLORER_PROP_ALIASES = {
    "name": "label",
    "label": "label",
    "rdfslabel": "label",
    "preflabel": "label",
    "altlabel": "label",
    "title": "label",
    "description": "description",
    "schemaorgdescription": "description",
    "telephone": "phone",
    "phone": "phone",
    "contactpoint": "phone",
    "p1329": "phone",
    "iatacode": "iata",
    "iataairportcode": "iata",
    "p238": "iata",
    "icaocode": "icao",
    "icaoairportcode": "icao",
    "p239": "icao",
    "faaairportcode": "faa",
    "p240": "faa",
    "sameas": "sameas",
    "url": "url",
    "website": "url",
    "officialwebsite": "url",
    "p856": "url",
    "identifier": "identifier",
    "code": "identifier",
    "eidr": "identifier",
    "p2704": "identifier",
}


def _link_detail_cache_key(build_dir: Path, variant_name: str, idx: int) -> str:
    try:
        build_key = str(build_dir.resolve())
    except Exception:
        build_key = str(build_dir)
    return f"{build_key}|{_clean_text(variant_name)}|{int(idx)}"


def _link_detail_cache_get(key: str):
    with _LINK_DETAIL_LOCK:
        payload = _LINK_DETAIL_CACHE.get(key)
        if payload is not None:
            _LINK_DETAIL_CACHE.move_to_end(key)
        return payload


def _link_detail_cache_set(key: str, payload: dict):
    if payload is None:
        return
    with _LINK_DETAIL_LOCK:
        _LINK_DETAIL_CACHE[key] = payload
        _LINK_DETAIL_CACHE.move_to_end(key)
        while len(_LINK_DETAIL_CACHE) > _LINK_DETAIL_CACHE_MAX:
            _LINK_DETAIL_CACHE.popitem(last=False)


def _start_link_detail_build(
    build_dir: Path,
    variant_dir: Path,
    variant_name: str,
    idx: int,
):
    key = _link_detail_cache_key(build_dir, variant_name, idx)
    cached = _link_detail_cache_get(key)
    if cached is not None:
        return key, "ready", cached, None

    with _LINK_DETAIL_LOCK:
        fut = _LINK_DETAIL_FUTURES.get(key)
        if fut is None or fut.cancelled():
            fut = _LINK_DETAIL_EXECUTOR.submit(_build_link_detail_payload, variant_dir, idx)
            _LINK_DETAIL_FUTURES[key] = fut
    return key, "pending", None, fut


def _read_link_detail_future(
    key: str,
    fut,
    wait_ms: int = 0,
):
    timeout_s = max(0.0, float(wait_ms) / 1000.0)
    if timeout_s <= 0 and not fut.done():
        return None, "pending"
    try:
        payload = fut.result(timeout=timeout_s if timeout_s > 0 else None)
    except FuturesTimeoutError:
        return None, "pending"
    except Exception:
        with _LINK_DETAIL_LOCK:
            _LINK_DETAIL_FUTURES.pop(key, None)
        raise

    with _LINK_DETAIL_LOCK:
        _LINK_DETAIL_FUTURES.pop(key, None)
    if payload is not None:
        _link_detail_cache_set(key, payload)
    return payload, "ready"


def _normalize_node_token(value: str) -> str:
    raw = _clean_text(value).strip().strip("<>").strip()
    if not raw:
        return ""
    try:
        wd_iri = align_script.extract_wd_entity_iri(raw)
    except Exception:
        wd_iri = None
    if wd_iri:
        return wd_iri
    return raw


def _short_predicate(value: str) -> str:
    text = _clean_text(value).strip().strip("<>")
    if not text:
        return ""
    if "#" in text:
        text = text.rsplit("#", 1)[-1]
    if "/" in text:
        text = text.rsplit("/", 1)[-1]
    if ":" in text and "/" not in text and "#" not in text:
        text = text.split(":", 1)[-1]
    return text


def _predicate_token(value: str) -> str:
    raw = _short_predicate(value).lower()
    return re.sub(r"[^a-z0-9]+", "", raw)


def _predicate_alias_key(value: str) -> str:
    token = _predicate_token(value)
    return _LINK_EXPLORER_PROP_ALIASES.get(token, token)


def _normalize_property_key(value: str) -> str:
    return _clean_text(value).strip().strip("<>").lower()


def _extract_wikidata_property_id(predicate: str):
    raw = _clean_text(predicate).strip().strip("<>")
    if not raw:
        return ""
    m = re.search(r"([Pp]\d+)$", raw)
    if not m:
        return ""
    return m.group(1).upper()


def _extract_wikidata_entity_id(value: str):
    raw = _clean_text(value).strip().strip("<>")
    if not raw:
        return ""
    direct = re.fullmatch(r"([QqPp]\d+)", raw)
    if direct:
        return direct.group(1).upper()
    iri_match = re.search(r"/entity/([QqPp]\d+)$", raw)
    if iri_match:
        return iri_match.group(1).upper()
    return ""


@lru_cache(maxsize=4096)
def _fetch_wikidata_entity_meta(entity_id: str, language: str = "en"):
    eid = _clean_text(entity_id).upper()
    lang = _clean_text(language) or "en"
    if not re.fullmatch(r"[QP]\d+", eid):
        return "", ""
    url = "https://www.wikidata.org/w/api.php"
    params = {
        "action": "wbgetentities",
        "ids": eid,
        "props": "labels|descriptions",
        "languages": lang,
        "format": "json",
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        entity = ((data or {}).get("entities") or {}).get(eid, {})
        labels = entity.get("labels") or {}
        descs = entity.get("descriptions") or {}
        label = _clean_text((labels.get(lang) or {}).get("value", ""))
        desc = _clean_text((descs.get(lang) or {}).get("value", ""))
        return label, desc
    except Exception:
        return "", ""


@lru_cache(maxsize=2048)
def _fetch_wikidata_property_meta(prop_id: str, language: str = "en"):
    pid = _clean_text(prop_id).upper()
    lang = _clean_text(language) or "en"
    if not re.fullmatch(r"P\d+", pid):
        return "", ""
    url = "https://www.wikidata.org/w/api.php"
    params = {
        "action": "wbgetentities",
        "ids": pid,
        "props": "labels|descriptions",
        "languages": lang,
        "format": "json",
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        entity = ((data or {}).get("entities") or {}).get(pid, {})
        labels = entity.get("labels") or {}
        descs = entity.get("descriptions") or {}
        label = _clean_text((labels.get(lang) or {}).get("value", ""))
        desc = _clean_text((descs.get(lang) or {}).get("value", ""))
        return label, desc
    except Exception:
        return "", ""


@lru_cache(maxsize=512)
def _load_property_meta_cached(path_text: str, mtime_ns: int, size_b: int):
    del mtime_ns, size_b
    out = {}
    path = Path(path_text)
    if not path.exists() or not path.is_file():
        return out
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        header_skipped = False
        for line in f:
            raw = line.rstrip("\n")
            if not raw:
                continue
            parts = raw.split("\t")
            if not header_skipped:
                header_skipped = True
                first = parts[0].strip().lower() if parts else ""
                if first in {"predicate", "property"}:
                    continue
            prop = _clean_text(parts[0] if parts else "").strip().strip("<>")
            if not prop:
                continue
            label = _clean_text(parts[2] if len(parts) > 2 else "")
            desc = _clean_text(parts[3] if len(parts) > 3 else "")
            keys = {
                _normalize_property_key(prop),
                _predicate_token(prop),
                _short_predicate(prop).lower(),
            }
            score = (1 if label else 0) + (1 if desc else 0)
            for key in keys:
                if not key:
                    continue
                existing = out.get(key)
                if existing:
                    prev_score = (1 if existing.get("label") else 0) + (1 if existing.get("description") else 0)
                    if prev_score > score:
                        continue
                out[key] = {
                    "label": label,
                    "description": desc,
                }
    return out


def _load_property_meta(path: Path):
    if not path.exists() or not path.is_file():
        return {}
    try:
        st = path.stat()
    except Exception:
        return {}
    try:
        resolved = path.resolve()
    except Exception:
        resolved = path
    return _load_property_meta_cached(str(resolved), int(st.st_mtime_ns), int(st.st_size))


def _property_meta_for(predicate: str, prop_meta: dict):
    if not prop_meta:
        prop_id = _extract_wikidata_property_id(predicate)
        if not prop_id:
            return "", ""
        return _fetch_wikidata_property_meta(prop_id)
    keys = (
        _normalize_property_key(predicate),
        _predicate_token(predicate),
        _short_predicate(predicate).lower(),
    )
    label = ""
    desc = ""
    for key in keys:
        if not key:
            continue
        data = prop_meta.get(key)
        if not data:
            continue
        label = _clean_text(data.get("label"))
        desc = _clean_text(data.get("description"))
        break

    if label and desc:
        return label, desc

    prop_id = _extract_wikidata_property_id(predicate)
    if not prop_id:
        return label, desc
    remote_label, remote_desc = _fetch_wikidata_property_meta(prop_id)
    if not label:
        label = remote_label
    if not desc:
        desc = remote_desc
    return label, desc


def _normalize_compare_text(value: str) -> str:
    raw = _clean_text(value)
    if not raw:
        return ""
    base = align_script.normalize_for_matching(raw)
    return re.sub(r"[^a-z0-9]+", "", base.lower())


def _is_informative_value_norm(value: str) -> bool:
    token = _clean_text(value).lower()
    if not token:
        return False
    # Ignore tiny numeric tokens (e.g. "6") which cause many false alignments.
    if re.fullmatch(r"\d{1,4}", token):
        return False
    # Ignore blank-node-like normalized IDs, usually not semantically informative.
    if re.fullmatch(r"n[0-9a-f]{10,}", token):
        return False
    # Keep concise Wikidata IDs.
    if re.fullmatch(r"[pq]\d+", token):
        return True
    # Very short non-ID tokens are typically noisy.
    if len(token) < 4:
        return False
    return True


def _informative_value_norms(values):
    return {v for v in (values or set()) if _is_informative_value_norm(v)}


def _object_value_info(obj: str):
    literal = _literal_lex(obj)
    if literal is not None:
        return {
            "text": literal,
            "is_node": False,
            "node": "",
            "norm": _normalize_compare_text(literal),
        }
    node = _normalize_node_token(obj)
    text = node or _clean_text(obj).strip().strip("<>")
    return {
        "text": text,
        "is_node": True,
        "node": node or text,
        "norm": _normalize_compare_text(text),
    }


def _first_literal_value(values):
    for value in values or []:
        if not isinstance(value, dict):
            continue
        if value.get("is_node"):
            continue
        text = _clean_text(value.get("text"))
        if text:
            return text
    return ""


def _build_node_summary(side: str, node: str, attr_items):
    side_norm = _clean_text(side).lower()
    node_key = _normalize_node_token(node)
    label = ""
    description = ""
    for item in attr_items or []:
        alias = _predicate_alias_key(item.get("property", ""))
        if alias == "label" and not label:
            label = _first_literal_value(item.get("values"))
        elif alias == "description" and not description:
            description = _first_literal_value(item.get("values"))
        if label and description:
            break

    if side_norm == "wd":
        entity_id = _extract_wikidata_entity_id(node_key)
        if entity_id:
            remote_label, remote_desc = _fetch_wikidata_entity_meta(entity_id)
            if not label:
                label = remote_label
            if not description:
                description = remote_desc

    return label, description


def _parse_ent_link_line(line: str):
    text = (line or "").rstrip("\n")
    if not text:
        return None
    parts = text.split("\t")
    if len(parts) < 2:
        return None
    left = _clean_text(parts[0])
    right = _clean_text(parts[1])
    if _looks_like_ent_links_header(f"{left}\t{right}"):
        return None
    wdc_iri = _normalize_node_token(left)
    wd_iri = _normalize_node_token(right)
    if not wdc_iri or not wd_iri:
        return None
    return wdc_iri, wd_iri


def _resolve_link_explorer_variant_dir(build_dir: Path, variant: Optional[str] = None):
    requested = _clean_text(variant)
    names = []
    if requested in _LINK_EXPLORER_VARIANTS:
        names.append(requested)
    for default_name in _LINK_EXPLORER_VARIANTS:
        if default_name not in names:
            names.append(default_name)

    for name in names:
        p = build_dir / name
        if p.exists() and p.is_dir() and (p / "ent_links").exists():
            return p, name
    for name in names:
        p = build_dir / name
        if p.exists() and p.is_dir():
            return p, name
    return None, None


def _scan_ent_links_page(path: Path, offset: int = 0, limit: int = 30, query: str = ""):
    if not path.exists() or not path.is_file():
        return {"rows": [], "total": 0, "has_more": False}
    offset = max(0, int(offset))
    limit = max(1, min(int(limit), 200))
    q = _clean_text(query).lower()

    # For large files without a filter, avoid a full scan to compute an exact total.
    # We only collect one page (+1 row to detect next page) for fast first render.
    try:
        file_size = path.stat().st_size
    except Exception:
        file_size = 0
    fast_mode = (not q) and file_size >= _LINK_EXPLORER_FAST_SCAN_BYTES

    rows = []
    total = 0
    has_more = False
    logical_idx = -1
    matched = 0
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            parsed = _parse_ent_link_line(line)
            if not parsed:
                continue
            logical_idx += 1
            wdc_iri, wd_iri = parsed
            if q and q not in wdc_iri.lower() and q not in wd_iri.lower():
                continue
            if matched >= offset and len(rows) < limit:
                rows.append(
                    {
                        "idx": logical_idx,
                        "wdc_iri": wdc_iri,
                        "wikidata_uri": wd_iri,
                    }
                )
            matched += 1

            if fast_mode and matched > (offset + limit):
                # We already captured page rows; first extra match means next page exists.
                if len(rows) >= limit:
                    has_more = True
                    break

    if fast_mode:
        return {"rows": rows, "total": None, "has_more": has_more}
    total = matched
    has_more = (offset + len(rows)) < total
    return {"rows": rows, "total": total, "has_more": has_more}


def _scan_ent_link_by_index(path: Path, idx: int):
    if not path.exists() or not path.is_file():
        return None
    if idx is None:
        return None
    try:
        target = int(idx)
    except Exception:
        return None
    if target < 0:
        return None

    logical_idx = -1
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            parsed = _parse_ent_link_line(line)
            if not parsed:
                continue
            logical_idx += 1
            if logical_idx != target:
                continue
            wdc_iri, wd_iri = parsed
            return {
                "idx": logical_idx,
                "wdc_iri": wdc_iri,
                "wikidata_uri": wd_iri,
            }
    return None


def _scan_subject_triples(
    path: Path,
    subject_key: str,
    max_rows: int = 4000,
    max_scan_lines: int = 0,
):
    rows = []
    if not path.exists() or not path.is_file() or not subject_key:
        return rows
    scanned = 0
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            scanned += 1
            if max_scan_lines > 0 and scanned > max_scan_lines:
                # Optional safeguard when a caller explicitly requests bounded scans.
                break
            parts = line.rstrip("\n").split("\t", 2)
            if len(parts) < 3:
                continue
            s = _clean_text(parts[0])
            p = _clean_text(parts[1]).strip().strip("<>")
            o = _clean_text(parts[2])
            if not s or not p:
                continue
            same_subject = _normalize_node_token(s) == subject_key
            if not same_subject:
                continue
            rows.append((p, o))
            if len(rows) >= max_rows:
                break
    return rows


def _aggregate_property_items(rows, relation: bool, max_values: int = 8, prop_meta: Optional[dict] = None):
    by_pred = {}
    for p, o in rows:
        pred = _clean_text(p).strip().strip("<>")
        if not pred:
            continue
        info = _object_value_info(o)
        if not info["text"]:
            continue
        item = by_pred.get(pred)
        if item is None:
            prop_label, prop_desc = _property_meta_for(pred, prop_meta or {})
            item = {
                "property": pred,
                "short_property": _short_predicate(pred),
                "property_label": prop_label,
                "property_description": prop_desc,
                "count": 0,
                "values": [],
                "value_norms": set(),
                "_seen": set(),
                "relation": relation,
            }
            by_pred[pred] = item
        item["count"] += 1
        signature = ("node" if info["is_node"] else "literal", info["node"] if info["is_node"] else info["text"])
        if signature in item["_seen"]:
            continue
        item["_seen"].add(signature)
        if info["norm"]:
            item["value_norms"].add(info["norm"])
        if len(item["values"]) < max_values:
            payload = {
                "text": info["text"],
                "is_node": info["is_node"],
            }
            if info["is_node"]:
                payload["node"] = info["node"]
            item["values"].append(payload)

    items = []
    for pred, item in by_pred.items():
        items.append(
            {
                "property": pred,
                "short_property": item["short_property"],
                "property_label": item.get("property_label", ""),
                "property_description": item.get("property_description", ""),
                "count": item["count"],
                "values": item["values"],
                "value_norms": sorted(item["value_norms"]),
                "relation": relation,
            }
        )
    items.sort(key=lambda r: (-int(r.get("count", 0)), r.get("property", "")))
    return items


def _side_files(variant_dir: Path, side: str):
    side_norm = _clean_text(side).lower()
    if side_norm in {"wd", "wikidata", "right"}:
        return {
            "side": "wd",
            "attr": variant_dir / "attr_triples_2",
            "rel": variant_dir / "rel_triples_2",
        }
    return {
        "side": "wdc",
        "attr": variant_dir / "attr_triples_1",
        "rel": variant_dir / "rel_triples_1",
    }


def _build_node_payload(variant_dir: Path, side: str, node: str):
    files = _side_files(variant_dir, side)
    node_key = _normalize_node_token(node)
    stats_path = variant_dir / ("prop_stats_wd.tsv" if files["side"] == "wd" else "prop_stats_wdc.tsv")
    prop_meta = _load_property_meta(stats_path)
    if not node_key:
        return {
            "side": files["side"],
            "node": "",
            "summary_label": "",
            "summary_description": "",
            "attr_items": [],
            "rel_items": [],
            "attr_count": 0,
            "rel_count": 0,
        }
    attr_rows = _scan_subject_triples(files["attr"], node_key)
    rel_rows = _scan_subject_triples(files["rel"], node_key)
    attr_items = _aggregate_property_items(attr_rows, relation=False, prop_meta=prop_meta)
    rel_items = _aggregate_property_items(rel_rows, relation=True, prop_meta=prop_meta)
    summary_label, summary_description = _build_node_summary(files["side"], node_key, attr_items)
    return {
        "side": files["side"],
        "node": node_key,
        "summary_label": summary_label,
        "summary_description": summary_description,
        "attr_items": attr_items,
        "rel_items": rel_items,
        "attr_count": sum(int(r.get("count", 0)) for r in attr_items),
        "rel_count": sum(int(r.get("count", 0)) for r in rel_items),
    }


def _similarity_for_properties(left_item: dict, right_item: dict):
    left_prop = left_item.get("property", "")
    right_prop = right_item.get("property", "")
    left_token = _predicate_token(left_prop)
    right_token = _predicate_token(right_prop)
    if not left_token or not right_token:
        return 0.0, 0.0, 0.0

    name_score = 0.0
    if left_token == right_token:
        name_score = 1.0
    else:
        left_alias = _predicate_alias_key(left_prop)
        right_alias = _predicate_alias_key(right_prop)
        if left_alias and left_alias == right_alias:
            name_score = 0.93
        else:
            ratio = difflib.SequenceMatcher(None, left_token, right_token).ratio()
            if left_token in right_token or right_token in left_token:
                ratio = max(ratio, 0.86)
            name_score = ratio

    left_values = _informative_value_norms(set(left_item.get("value_norms") or []))
    right_values = _informative_value_norms(set(right_item.get("value_norms") or []))
    value_score = 0.0
    if left_values and right_values:
        inter = len(left_values & right_values)
        union = len(left_values | right_values)
        jaccard = (inter / union) if union > 0 else 0.0
        smaller = min(len(left_values), len(right_values))
        coverage = (inter / smaller) if smaller > 0 else 0.0

        # Best-pair fallback when one side contains many values and only one needs to match.
        best_pair = 0.0
        for lv in left_values:
            for rv in right_values:
                if not lv or not rv:
                    continue
                if lv == rv:
                    best_pair = 1.0
                    break
                ratio = difflib.SequenceMatcher(None, lv, rv).ratio()
                if lv in rv or rv in lv:
                    ratio = max(ratio, 0.96)
                if ratio > best_pair:
                    best_pair = ratio
            if best_pair >= 1.0:
                break

        value_score = max(jaccard, coverage, best_pair)

    score = (0.65 * name_score) + (0.35 * value_score)
    if name_score >= 0.93 and score < 0.93:
        score = 0.93
    return score, name_score, value_score


def _pattern_token_set(value: str):
    raw = _clean_text(value)
    if not raw:
        return set()
    base = {
        raw.lower(),
        _short_predicate(raw).lower(),
        _predicate_token(raw),
        _predicate_alias_key(raw),
    }
    out = set()
    for t in base:
        t = _clean_text(t).lower()
        if not t:
            continue
        out.add(t)
        out.add(re.sub(r"[^a-z0-9]+", "", t))
    return {t for t in out if t}


def _property_matches_pattern(prop_value: str, pattern_value: str) -> bool:
    prop_tokens = _pattern_token_set(prop_value)
    if not prop_tokens:
        return False
    for pat in _split_target_property_alternatives(pattern_value):
        pat_tokens = _pattern_token_set(pat)
        if not pat_tokens:
            continue
        if prop_tokens & pat_tokens:
            return True
    return False


def _compute_property_matches(
    left_items,
    right_items,
    max_matches: int = 14,
    threshold: float = 0.55,
    configured_pairs: Optional[list] = None,
):
    def _candidate_row(left_item: dict, right_item: dict, cand: dict, reason: str):
        return {
            "wdc_property": left_item.get("property", ""),
            "wdc_short_property": left_item.get("short_property", ""),
            "wdc_property_label": left_item.get("property_label", ""),
            "wdc_property_description": left_item.get("property_description", ""),
            "wikidata_property": right_item.get("property", ""),
            "wikidata_short_property": right_item.get("short_property", ""),
            "wikidata_property_label": right_item.get("property_label", ""),
            "wikidata_property_description": right_item.get("property_description", ""),
            "score": round(float(cand["score"]), 3),
            "name_score": round(float(cand["name_score"]), 3),
            "value_score": round(float(cand["value_score"]), 3),
            "match_reason": reason,
            "wdc_sample": (left_item.get("values") or [{}])[0].get("text", "") if left_item.get("values") else "",
            "wikidata_sample": (right_item.get("values") or [{}])[0].get("text", "")
            if right_item.get("values")
            else "",
        }

    configured_pairs = configured_pairs or []
    used_left = set()
    used_right = set()
    rows = []

    for pair in configured_pairs:
        left_pat = _clean_text((pair or {}).get("wdc", ""))
        right_pat = _clean_text((pair or {}).get("target", ""))
        if not left_pat or not right_pat:
            continue
        best = None
        for l_idx, left_item in enumerate(left_items or []):
            if l_idx in used_left:
                continue
            if not _property_matches_pattern(left_item.get("property", ""), left_pat):
                continue
            for r_idx, right_item in enumerate(right_items or []):
                if r_idx in used_right:
                    continue
                if not _property_matches_pattern(right_item.get("property", ""), right_pat):
                    continue
                if bool(left_item.get("relation")) != bool(right_item.get("relation")):
                    continue
                score, name_score, value_score = _similarity_for_properties(left_item, right_item)
                boosted_score = max(score, 0.9 if value_score > 0 else 0.78)
                cand = {
                    "l_idx": l_idx,
                    "r_idx": r_idx,
                    "score": boosted_score,
                    "name_score": name_score,
                    "value_score": value_score,
                }
                if best is None or cand["score"] > best["score"]:
                    best = cand
        if not best:
            continue
        used_left.add(best["l_idx"])
        used_right.add(best["r_idx"])
        rows.append(
            _candidate_row(
                (left_items or [])[best["l_idx"]],
                (right_items or [])[best["r_idx"]],
                best,
                reason="configured_rule",
            )
        )
        if len(rows) >= max_matches:
            return rows[:max_matches]

    candidates = []
    for l_idx, left_item in enumerate(left_items or []):
        if l_idx in used_left:
            continue
        for r_idx, right_item in enumerate(right_items or []):
            if r_idx in used_right:
                continue
            # Keep attribute vs relation comparisons separate to avoid noisy cross-type matches.
            if bool(left_item.get("relation")) != bool(right_item.get("relation")):
                continue
            score, name_score, value_score = _similarity_for_properties(left_item, right_item)
            candidates.append(
                {
                    "l_idx": l_idx,
                    "r_idx": r_idx,
                    "score": score,
                    "name_score": name_score,
                    "value_score": value_score,
                }
            )
    candidates.sort(key=lambda row: row["score"], reverse=True)

    for cand in candidates:
        if cand["score"] < threshold:
            continue
        if cand["l_idx"] in used_left or cand["r_idx"] in used_right:
            continue
        left_item = left_items[cand["l_idx"]]
        right_item = right_items[cand["r_idx"]]
        used_left.add(cand["l_idx"])
        used_right.add(cand["r_idx"])
        rows.append(_candidate_row(left_item, right_item, cand, reason="name_or_alias"))
        if len(rows) >= max_matches:
            break

    # Fallback: for properties still unmatched, align by value similarity only.
    # This catches cases like custom WDC keys mapping to Pxxxx when names differ.
    value_fallback_threshold = 0.80
    if len(rows) < max_matches:
        fallback_candidates = [
            row
            for row in candidates
            if row["value_score"] >= value_fallback_threshold
            and row["l_idx"] not in used_left
            and row["r_idx"] not in used_right
        ]
        fallback_candidates.sort(key=lambda row: (row["value_score"], row["score"]), reverse=True)
        for cand in fallback_candidates:
            if cand["l_idx"] in used_left or cand["r_idx"] in used_right:
                continue
            left_item = left_items[cand["l_idx"]]
            right_item = right_items[cand["r_idx"]]
            used_left.add(cand["l_idx"])
            used_right.add(cand["r_idx"])
            boosted = dict(cand)
            boosted["score"] = max(float(boosted["score"]), 0.70)
            rows.append(_candidate_row(left_item, right_item, boosted, reason="value_fallback"))
            if len(rows) >= max_matches:
                break

    if len(rows) > max_matches:
        rows = rows[:max_matches]
    return rows


def _node_graph_preview(node_payload: dict, max_neighbors: int = 10):
    items = []
    if not node_payload:
        return items
    root = _clean_text(node_payload.get("node"))
    side = _clean_text(node_payload.get("side"))
    if root:
        items.append({"node": root, "side": side, "root": True})
    seen = {root}
    for rel_item in (node_payload.get("rel_items") or []):
        for value in (rel_item.get("values") or []):
            if not value.get("is_node"):
                continue
            node = _clean_text(value.get("node"))
            if not node or node in seen:
                continue
            seen.add(node)
            items.append({"node": node, "side": side, "root": False})
            if len(items) >= max_neighbors + 1:
                return items
    return items


def _build_link_detail_payload(variant_dir: Path, idx: int):
    ent_links_path = variant_dir / "ent_links"
    link_row = _scan_ent_link_by_index(ent_links_path, idx)
    if not link_row:
        return None
    wdc_node = _build_node_payload(variant_dir, "wdc", link_row["wdc_iri"])
    wd_node = _build_node_payload(variant_dir, "wd", link_row["wikidata_uri"])
    left_items = (wdc_node.get("attr_items") or []) + (wdc_node.get("rel_items") or [])
    right_items = (wd_node.get("attr_items") or []) + (wd_node.get("rel_items") or [])
    build_dir = variant_dir.parent
    build_config = _load_build_config(build_dir)
    linking_combinations = _extract_linking_combinations(build_config)
    configured_pairs = []
    seen_pairs = set()
    for combo in linking_combinations:
        for pair in (combo.get("pairs") or []):
            left_pat = _clean_text(pair.get("wdc", ""))
            right_pat = _clean_text(pair.get("target", ""))
            sig = f"{left_pat}=>{right_pat}".lower()
            if not left_pat or not right_pat or sig in seen_pairs:
                continue
            seen_pairs.add(sig)
            configured_pairs.append({"wdc": left_pat, "target": right_pat})
    matches = _compute_property_matches(left_items, right_items, configured_pairs=configured_pairs)
    return {
        "idx": link_row["idx"],
        "wdc_iri": link_row["wdc_iri"],
        "wikidata_uri": link_row["wikidata_uri"],
        "wdc_node": wdc_node,
        "wd_node": wd_node,
        "property_matches": matches,
        "wdc_graph_nodes": _node_graph_preview(wdc_node),
        "wd_graph_nodes": _node_graph_preview(wd_node),
    }


def _normalized_path_text(value: str) -> str:
    raw = _clean_text(value)
    if not raw:
        return ""
    try:
        # Normalize both relative and absolute paths to the same canonical form.
        return str(Path(raw).expanduser().resolve())
    except Exception:
        return os.path.normpath(raw)

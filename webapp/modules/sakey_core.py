def _sakey_runs_root():
    _SAKEY_RUNS_ROOT.mkdir(parents=True, exist_ok=True)
    return _SAKEY_RUNS_ROOT


def _sakey_run_dir(run_id: str):
    root = _sakey_runs_root().resolve()
    d = (root / _clean_text(run_id)).resolve()
    try:
        d.relative_to(root)
    except Exception:
        return None
    d.mkdir(parents=True, exist_ok=True)
    return d


def _sakey_meta_path(run_id: str):
    d = _sakey_run_dir(run_id)
    if not d:
        return None
    return d / "meta.json"


def _sakey_read_meta(run_id: str):
    p = _sakey_meta_path(run_id)
    if not p or not p.exists():
        return None
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _sakey_write_meta(run_id: str, payload: dict):
    p = _sakey_meta_path(run_id)
    if not p:
        return
    safe = payload if isinstance(payload, dict) else {}
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(safe, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(p)


def _sakey_update_meta(run_id: str, **fields):
    with _SAKEY_RUN_LOCK:
        meta = _sakey_read_meta(run_id) or {}
        meta.update(fields)
        _sakey_write_meta(run_id, meta)
        return meta


def _sakey_log(run_id: str, message: str):
    d = _sakey_run_dir(run_id)
    if not d:
        return
    ts = _fmt_ts(time.time())
    line = f"[{ts}] {str(message or '').rstrip()}\n"
    try:
        with (d / "run.log").open("a", encoding="utf-8", errors="ignore") as f:
            f.write(line)
    except Exception:
        pass


def _sakey_list_runs(limit: int = 30, class_name: str = ""):
    root = _sakey_runs_root()
    rows = []
    wanted = _clean_text(class_name)
    for child in root.iterdir():
        if not child.is_dir():
            continue
        meta = _sakey_read_meta(child.name)
        if not meta:
            continue
        if wanted and _clean_text(str(meta.get("class_name"))) != wanted:
            continue
        rows.append(meta)
    rows.sort(key=lambda r: float(r.get("created_at", 0.0) or 0.0), reverse=True)
    limit = max(1, min(int(limit or 30), _SAKEY_MAX_LIST))
    return rows[:limit]


def _sakey_reconcile_inflight_runs():
    global _SAKEY_RECONCILED
    if _SAKEY_RECONCILED:
        return
    with _SAKEY_RECONCILE_LOCK:
        if _SAKEY_RECONCILED:
            return
        root = _sakey_runs_root()
        now_ts = time.time()
        for child in root.iterdir():
            if not child.is_dir():
                continue
            run_id = child.name
            meta = _sakey_read_meta(run_id)
            if not meta:
                continue
            st = _clean_text(str(meta.get("status", ""))).lower()
            if st not in {"queued", "waiting", "running"}:
                continue
            created_at = float(meta.get("created_at", 0.0) or 0.0)
            # Runs that were active before this webapp process started are stale.
            if created_at and created_at < (_SAKEY_APP_BOOT_TS - 0.5):
                msg = "Run interrupted by server restart. Relaunch it if needed."
                _sakey_update_meta(
                    run_id,
                    status="error",
                    ended_at=now_ts,
                    error=msg,
                )
                _sakey_log(run_id, msg)
        _SAKEY_RECONCILED = True


def _sakey_find_active_duplicate(class_name: str, parts_spec: str, mins: int, timeout_hours: float):
    cname = _clean_text(class_name)
    pspec = _clean_text(parts_spec) or "all"
    mins_v = int(max(1, mins))
    tout_v = float(max(0.1, timeout_hours))
    root = _sakey_runs_root()
    best = None
    best_created = -1.0
    for child in root.iterdir():
        if not child.is_dir():
            continue
        meta = _sakey_read_meta(child.name)
        if not meta:
            continue
        st = _clean_text(str(meta.get("status", ""))).lower()
        if st not in {"queued", "waiting", "running"}:
            continue
        if _clean_text(str(meta.get("class_name", ""))) != cname:
            continue
        if (_clean_text(str(meta.get("parts_spec", ""))) or "all") != pspec:
            continue
        if int(meta.get("mins", 0) or 0) != mins_v:
            continue
        try:
            mt = float(meta.get("timeout_hours", 0.0) or 0.0)
        except Exception:
            mt = 0.0
        if abs(mt - tout_v) > 1e-9:
            continue
        created = float(meta.get("created_at", 0.0) or 0.0)
        if created > best_created:
            best_created = created
            best = _clean_text(str(meta.get("run_id", ""))) or child.name
    return best


def _sakey_list_legacy_runs(limit: int = 80):
    root = (Path(__file__).resolve().parents[1] / "vickey" / "runs").resolve()
    if not root.exists() or not root.is_dir():
        return []
    outs = []
    for p in root.rglob("*.out"):
        if not p.is_file():
            continue
        try:
            st = p.stat()
            ts = float(st.st_mtime)
        except Exception:
            ts = 0.0
        outs.append((ts, p))
    outs.sort(key=lambda x: x[0], reverse=True)
    out_rows = []
    for ts, p in outs[: max(1, min(int(limit or 80), 400))]:
        rel = str(p.relative_to(root))
        cls_guess = _clean_text(p.parent.name).replace("_", " ").title().replace(" ", "")
        run_id = f"legacy::{rel}"
        log_candidate = p.with_suffix(".log")
        out_rows.append(
            {
                "run_id": run_id,
                "class_name": cls_guess or "Unknown",
                "parts_spec": "legacy",
                "mins": "",
                "timeout_hours": "",
                "status": "legacy",
                "created_at": ts,
                "created_at_h": _fmt_ts(ts) if ts > 0 else "",
                "started_at": None,
                "ended_at": ts,
                "error": "",
                "keys_candidates_count": 0,
                "key_summary": {},
                "legacy": True,
                "legacy_out_path": str(p),
                "legacy_log_path": str(log_candidate) if log_candidate.exists() else "",
            }
        )
    return out_rows


def _sakey_collect_class_options(test_mode: bool = False):
    rows = [dict(r) for r in db.list_wdc_classes()]
    out = []
    for r in rows:
        name = _clean_text(str(r.get("class_name", "")))
        if not name:
            continue
        if _is_test_class_name(name) != bool(test_mode):
            continue
        out.append(name)
    out.sort()
    return out


def _sakey_convert_to_nt(part_files, out_nt: Path, run_id: str):
    converted = 0
    bad = 0
    skipped_bnode = 0
    total = 0
    with out_nt.open("w", encoding="utf-8") as fo:
        for fp in part_files:
            _sakey_log(run_id, f"Convert: {fp}")
            with Path(fp).open("r", encoding="utf-8", errors="ignore") as f:
                for raw in f:
                    total += 1
                    line = raw.rstrip("\n")
                    m3 = _TRIPLE_RE.match(line)
                    if m3:
                        s = _clean_text(m3.group(1))
                        p = _clean_text(m3.group(2))
                        o = _clean_text(m3.group(3))
                        if s.startswith("_:") or o.startswith("_:"):
                            skipped_bnode += 1
                            continue
                        fo.write(f"{s} {p} {o} .\n")
                        converted += 1
                        continue
                    m4 = _QUAD_RE.match(line)
                    if m4:
                        s = _clean_text(m4.group(1))
                        p = _clean_text(m4.group(2))
                        o = _clean_text(m4.group(3))
                        if s.startswith("_:") or o.startswith("_:"):
                            skipped_bnode += 1
                            continue
                        fo.write(f"{s} {p} {o} .\n")
                        converted += 1
                        continue
                    bad += 1
    return {
        "total_lines": total,
        "converted_lines": converted,
        "skipped_lines": bad,
        "skipped_bnode_lines": skipped_bnode,
    }


def _sakey_parse_block_keys(raw_block: str):
    out = []
    if not raw_block:
        return out
    for m in re.finditer(r"\[([^\[\]]+)\]", raw_block):
        chunk = _clean_text(m.group(1))
        if not chunk:
            continue
        props = []
        for tok in chunk.split(","):
            pn = _normalize_prop_iri(tok)
            if pn:
                props.append(pn)
        if not props:
            continue
        out.append(props)
    return out


def _safe_float(value, default=None):
    try:
        if value is None:
            return default
        return float(str(value).strip())
    except Exception:
        return default


def _safe_int(value, default=None):
    try:
        if value is None:
            return default
        return int(str(value).strip())
    except Exception:
        return default


def _normalize_prop_iri(value: str) -> str:
    p = _clean_text(value)
    if not p:
        return ""
    if p.startswith("<") and p.endswith(">") and len(p) > 2:
        p = _clean_text(p[1:-1])
        if not p:
            return ""
    low = p.lower()
    if low.startswith("http://www.wikidata.org/prop/") or low.startswith("https://www.wikidata.org/prop/"):
        return low
    return p


def _normalize_sakey_order_by(value: str):
    key = _clean_text(value).lower()
    allowed = {"coverage_desc", "support_desc", "size_asc", "type_then_coverage"}
    if key not in allowed:
        return "coverage_desc"
    return key


def _parse_sakey_filter_params(
    order_by: str = "",
    min_support: str = "",
    only_almost: Optional[str] = None,
    max_key_size: str = "",
    q: str = "",
):
    return {
        "order_by": _normalize_sakey_order_by(order_by),
        "min_support": max(0, _safe_int(min_support, 0) or 0),
        "only_almost": _bool_from_any(only_almost),
        "max_key_size": max(0, _safe_int(max_key_size, 0) or 0),
        "q": _clean_text(q).lower(),
    }


def _sakey_compute_row_metrics(
    nt_path: Path,
    rows: list,
    max_lines: int = 400000,
):
    if not nt_path or not nt_path.exists() or not nt_path.is_file() or not rows:
        return {"subjects": None, "lines": 0, "sampled": False, "max_lines": max_lines}
    all_props = set()
    for row in rows:
        props = list(row.get("props") or [])
        for p in props:
            cp = _normalize_prop_iri(p)
            if cp:
                all_props.add(cp)
    if not all_props:
        return {"subjects": None, "lines": 0, "sampled": False, "max_lines": max_lines}
    prop_index = {p: i for i, p in enumerate(sorted(all_props))}
    subject_masks = {}
    lines = 0
    sampled = False
    with nt_path.open("r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            lines += 1
            if lines > max_lines:
                sampled = True
                break
            parts = raw.strip().split(" ", 2)
            if len(parts) < 3:
                continue
            s = _clean_text(parts[0])
            p = _normalize_prop_iri(parts[1])
            if not s or not p:
                continue
            idx = prop_index.get(p)
            if idx is None:
                continue
            prev = subject_masks.get(s, 0)
            subject_masks[s] = prev | (1 << idx)
    if not subject_masks:
        return {"subjects": 0, "lines": lines, "sampled": sampled, "max_lines": max_lines}
    mask_freq = Counter(subject_masks.values())
    subject_count = len(subject_masks)
    for row in rows:
        props = []
        for p in (row.get("props") or []):
            pn = _normalize_prop_iri(p)
            if pn in prop_index:
                props.append(pn)
        if not props:
            row["support_num"] = None
            row["coverage_num"] = None
            row["support"] = row.get("support") or "n/a"
            row["coverage"] = "n/a"
            row["sampled"] = sampled
            continue
        key_mask = 0
        for p in props:
            key_mask |= 1 << prop_index[p]
        support = 0
        for m, freq in mask_freq.items():
            if (m & key_mask) == key_mask:
                support += int(freq)
        coverage = float(support) / float(subject_count) if subject_count else 0.0
        row["support_num"] = support
        row["coverage_num"] = coverage
        row["support"] = str(support)
        row["coverage"] = f"{coverage:.4f}"
        row["sampled"] = sampled
    return {"subjects": subject_count, "lines": lines, "sampled": sampled, "max_lines": max_lines}


def _sakey_apply_filters_and_sort(rows: list, opts: dict):
    items = []
    q = _clean_text(str((opts or {}).get("q", ""))).lower()
    min_support = int((opts or {}).get("min_support", 0) or 0)
    only_almost = bool((opts or {}).get("only_almost", False))
    max_key_size = int((opts or {}).get("max_key_size", 0) or 0)
    for row in list(rows or []):
        rr = dict(row or {})
        rr["key_size"] = int(len(rr.get("props") or []))
        if only_almost and _clean_text(str(rr.get("type", rr.get("condition", "")))).lower() != "almost_key":
            continue
        support_num = _safe_int(rr.get("support_num"), None)
        if min_support > 0 and (support_num is None or support_num < min_support):
            continue
        if max_key_size > 0 and rr["key_size"] > max_key_size:
            continue
        if q:
            blob = " ".join(
                [
                    _clean_text(str(rr.get("key", ""))).lower(),
                    " ".join([_clean_text(str(p)).lower() for p in (rr.get("props") or [])]),
                ]
            )
            if q not in blob:
                continue
        items.append(rr)
    order_by = _normalize_sakey_order_by((opts or {}).get("order_by", "coverage_desc"))
    if order_by == "support_desc":
        items.sort(
            key=lambda r: (
                -float(_safe_float(r.get("support_num"), -1.0) or -1.0),
                -float(_safe_float(r.get("coverage_num"), -1.0) or -1.0),
                int(r.get("key_size", 0) or 0),
                _clean_text(str(r.get("key", ""))).lower(),
            )
        )
    elif order_by == "size_asc":
        items.sort(
            key=lambda r: (
                int(r.get("key_size", 0) or 0),
                -float(_safe_float(r.get("coverage_num"), -1.0) or -1.0),
                -float(_safe_float(r.get("support_num"), -1.0) or -1.0),
                _clean_text(str(r.get("key", ""))).lower(),
            )
        )
    elif order_by == "type_then_coverage":
        items.sort(
            key=lambda r: (
                0 if _clean_text(str(r.get("type", r.get("condition", "")))).lower() == "almost_key" else 1,
                -float(_safe_float(r.get("coverage_num"), -1.0) or -1.0),
                -float(_safe_float(r.get("support_num"), -1.0) or -1.0),
                int(r.get("key_size", 0) or 0),
                _clean_text(str(r.get("key", ""))).lower(),
            )
        )
    else:
        items.sort(
            key=lambda r: (
                -float(_safe_float(r.get("coverage_num"), -1.0) or -1.0),
                -float(_safe_float(r.get("support_num"), -1.0) or -1.0),
                int(r.get("key_size", 0) or 0),
                _clean_text(str(r.get("key", ""))).lower(),
            )
        )
    return items


def _sakey_parse_keys_from_output(out_path: Path, limit: int = 500):
    lines = []
    summary = {
        "conditional_keys_count": None,
        "keys_count": None,
        "non_keys_found": None,
    }
    text = ""
    try:
        text = out_path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        text = ""
    if text:
        m = _SAKEY_VICKEY_KEYS_RE.search(text)
        if m:
            summary["conditional_keys_count"] = int(m.group(1))
        m = _SAKEY_KEYS_RE.search(text)
        if m:
            summary["keys_count"] = int(m.group(1))
        m = _SAKEY_NON_KEYS_RE.search(text)
        if m:
            summary["non_keys_found"] = int(m.group(1))
        non_key_rows = []
        almost_key_rows = []
        for bm in _SAKEY_NON_KEYS_BLOCK_RE.finditer(text):
            non_key_rows.extend(_sakey_parse_block_keys(_clean_text(bm.group(2))))
        for bm in _SAKEY_ALMOST_KEYS_BLOCK_RE.finditer(text):
            almost_key_rows.extend(_sakey_parse_block_keys(_clean_text(bm.group(2))))
        if non_key_rows:
            summary["non_keys_found"] = len(non_key_rows)
        if almost_key_rows:
            summary["keys_count"] = len(almost_key_rows)
        for props in almost_key_rows:
            if len(lines) >= limit:
                break
            lines.append(
                {
                    "key": " + ".join(props),
                    "condition": "almost_key",
                    "type": "almost_key",
                    "support": "n/a",
                    "score": "",
                    "props": props,
                }
            )
        for props in non_key_rows:
            if len(lines) >= limit:
                break
            lines.append(
                {
                    "key": " + ".join(props),
                    "condition": "non_key",
                    "type": "non_key",
                    "support": "n/a",
                    "score": "",
                    "props": props,
                }
            )
        for raw in text.splitlines():
            if len(lines) >= limit:
                break
            if "\t" not in raw:
                continue
            row = [c.strip() for c in raw.split("\t")]
            if len(row) < 3:
                continue
            left = row[0]
            if not left:
                continue
            lleft = left.lower()
            if lleft.startswith("computing") or lleft.startswith("vickey found"):
                continue
            if lleft.startswith("we found") or lleft.startswith("key discovery"):
                continue
            item = {
                "key": left,
                "condition": row[1] if len(row) > 1 else "",
                "support": row[2] if len(row) > 2 else "",
                "score": row[3] if len(row) > 3 else "",
            }
            lines.append(item)
    return summary, lines


def _sakey_write_report_files(run_id: str, summary: dict, keys_rows):
    d = _sakey_run_dir(run_id)
    if not d:
        return None, None
    rep_json = d / "SAKEY_REPORT.json"
    rep_tsv = d / "SAKEY_KEYS.tsv"
    payload = {
        "run_id": run_id,
        "summary": summary or {},
        "keys_candidates": keys_rows or [],
    }
    rep_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    with rep_tsv.open("w", encoding="utf-8") as f:
        f.write("key\tcondition\ttype\tsupport\tcoverage\tkey_size\tscore\n")
        for row in keys_rows or []:
            f.write(
                f"{str(row.get('key','')).replace(chr(9), ' ')}\t"
                f"{str(row.get('condition','')).replace(chr(9), ' ')}\t"
                f"{str(row.get('type','')).replace(chr(9), ' ')}\t"
                f"{str(row.get('support','')).replace(chr(9), ' ')}\t"
                f"{str(row.get('coverage','')).replace(chr(9), ' ')}\t"
                f"{str(row.get('key_size','')).replace(chr(9), ' ')}\t"
                f"{str(row.get('score','')).replace(chr(9), ' ')}\n"
            )
    return rep_json, rep_tsv


def _run_sakey_worker(run_id: str):
    meta = _sakey_read_meta(run_id) or {}
    class_name = _clean_text(str(meta.get("class_name", "")))
    parts_spec = _clean_text(str(meta.get("parts_spec", ""))) or "all"
    mins = int(meta.get("mins", 3) or 3)
    timeout_hours = float(meta.get("timeout_hours", 2.0) or 2.0)
    timeout_sec = max(60, int(timeout_hours * 3600))
    run_dir = _sakey_run_dir(run_id)
    if not run_dir:
        return
    out_path = run_dir / "sakey.out"
    nt_path = run_dir / "input.nt"
    _sakey_update_meta(run_id, status="waiting", started_at=None, ended_at=None, error="")
    _sakey_log(
        run_id,
        f"Waiting for worker slot class={class_name} parts={parts_spec} (capacity={_SAKEY_MAX_CONCURRENT})",
    )
    try:
        with _SAKEY_EXEC_SEMAPHORE:
            _sakey_update_meta(run_id, status="running", started_at=time.time(), ended_at=None, error="")
            _sakey_log(run_id, f"Run started class={class_name} parts={parts_spec} mins={mins} timeout_h={timeout_hours}")
            part_files, warnings = _select_local_part_files(class_name, parts_spec)
            if warnings:
                for w in warnings:
                    _sakey_log(run_id, f"Warning: {w}")
            if not part_files:
                raise RuntimeError("No local parts selected. Download parts first or adjust parts spec.")
            _sakey_update_meta(
                run_id,
                selected_parts=[str(Path(p)) for p in part_files[:200]],
                selected_parts_count=len(part_files),
            )

            conv = _sakey_convert_to_nt(part_files, nt_path, run_id)
            _sakey_update_meta(run_id, conversion=conv, nt_path=str(nt_path))
            _sakey_log(
                run_id,
                f"Conversion done lines={conv.get('total_lines',0)} converted={conv.get('converted_lines',0)} skipped={conv.get('skipped_lines',0)}",
            )
            if int(conv.get("converted_lines", 0) or 0) <= 0:
                raise RuntimeError("Conversion produced 0 valid triples for SAKEY (after filtering malformed/bnode triples).")

            sakey_root = (Path(__file__).resolve().parents[1] / "SAKEY").resolve()
            runner = sakey_root / "run_sakey.sh"
            if not runner.exists():
                raise RuntimeError("SAKEY runner not found: SAKEY/run_sakey.sh")
            _sakey_log(run_id, "Running SAKEY")
            with out_path.open("w", encoding="utf-8", errors="ignore") as fout:
                subprocess.run(
                    [
                        "bash",
                        str(runner),
                        str(nt_path),
                        str(mins),
                    ],
                    cwd=str(sakey_root),
                    check=True,
                    text=True,
                    stdout=fout,
                    stderr=subprocess.STDOUT,
                    timeout=timeout_sec,
                )
            _sakey_log(run_id, f"SAKEY done. Output: {out_path}")

            key_summary, keys_rows = _sakey_parse_keys_from_output(out_path)
            metrics = _sakey_compute_row_metrics(nt_path, keys_rows)
            key_summary = dict(key_summary or {})
            key_summary["subjects_count_sample"] = metrics.get("subjects")
            key_summary["metrics_lines_scanned"] = metrics.get("lines")
            key_summary["metrics_sampled"] = bool(metrics.get("sampled"))
            rep_json, rep_tsv = _sakey_write_report_files(run_id, key_summary, keys_rows)
            _sakey_update_meta(
                run_id,
                status="completed",
                ended_at=time.time(),
                output_path=str(out_path),
                report_json=str(rep_json) if rep_json else "",
                report_tsv=str(rep_tsv) if rep_tsv else "",
                key_summary=key_summary,
                keys_candidates_count=len(keys_rows),
            )
            _sakey_log(run_id, f"Completed. keys_candidates={len(keys_rows)}")
    except subprocess.TimeoutExpired:
        key_summary = {}
        keys_rows = []
        if out_path.exists():
            key_summary, keys_rows = _sakey_parse_keys_from_output(out_path)
        if keys_rows and nt_path.exists():
            metrics = _sakey_compute_row_metrics(nt_path, keys_rows)
            key_summary = dict(key_summary or {})
            key_summary["subjects_count_sample"] = metrics.get("subjects")
            key_summary["metrics_lines_scanned"] = metrics.get("lines")
            key_summary["metrics_sampled"] = bool(metrics.get("sampled"))
        rep_json = rep_tsv = None
        if keys_rows:
            rep_json, rep_tsv = _sakey_write_report_files(run_id, key_summary, keys_rows)
        _sakey_update_meta(
            run_id,
            status="timeout",
            ended_at=time.time(),
            output_path=str(out_path),
            report_json=str(rep_json) if rep_json else "",
            report_tsv=str(rep_tsv) if rep_tsv else "",
            key_summary=key_summary,
            keys_candidates_count=len(keys_rows),
        )
        _sakey_log(run_id, f"Timeout after {timeout_sec}s")
    except Exception as exc:
        key_summary = {}
        keys_rows = []
        if out_path.exists():
            key_summary, keys_rows = _sakey_parse_keys_from_output(out_path)
        if keys_rows and nt_path.exists():
            metrics = _sakey_compute_row_metrics(nt_path, keys_rows)
            key_summary = dict(key_summary or {})
            key_summary["subjects_count_sample"] = metrics.get("subjects")
            key_summary["metrics_lines_scanned"] = metrics.get("lines")
            key_summary["metrics_sampled"] = bool(metrics.get("sampled"))
        rep_json = rep_tsv = None
        if keys_rows:
            rep_json, rep_tsv = _sakey_write_report_files(run_id, key_summary, keys_rows)
        _sakey_update_meta(
            run_id,
            status="error",
            ended_at=time.time(),
            error=str(exc),
            output_path=str(out_path),
            report_json=str(rep_json) if rep_json else "",
            report_tsv=str(rep_tsv) if rep_tsv else "",
            key_summary=key_summary,
            keys_candidates_count=len(keys_rows),
        )
        _sakey_log(run_id, f"Error: {exc}")



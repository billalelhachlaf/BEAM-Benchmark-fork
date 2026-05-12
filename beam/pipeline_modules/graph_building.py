

def _collect_wdc_outgoing_subgraphs(wdc_nq_paths, root_subjects, should_cancel=None):
    """
    Collect outgoing triples for selected WDC subjects and their recursively referenced bnodes.

    Subjects are matched on canonical token shape (IRIs without angle brackets, bnodes as-is).
    The graph context column is ignored by parse_nq_or_nt.
    """
    roots = {_canonical_wdc_token(s) for s in (root_subjects or []) if str(s or "").strip()}
    if not roots:
        return {}, {
            "scan_passes": 0,
            "parsed_lines": 0,
            "matched_subject_lines": 0,
            "subjects_requested": 0,
            "subjects_collected": 0,
        }

    outgoing = defaultdict(list)
    seen_subjects = set()
    pending = set(roots)
    scan_passes = 0
    parsed_lines = 0
    matched_subject_lines = 0
    total_bytes_all_files = 0
    for fp in wdc_nq_paths or []:
        try:
            total_bytes_all_files += int(os.path.getsize(fp))
        except Exception:
            continue

    def _eta_text(done_bytes, total_bytes, start_ts):
        if done_bytes <= 0 or total_bytes <= 0:
            return "ETA: N/A"
        elapsed = max(0.001, time.time() - start_ts)
        rate = done_bytes / elapsed
        if rate <= 0:
            return "ETA: N/A"
        remaining = max(0.0, float(total_bytes) - float(done_bytes))
        secs = int(remaining / rate)
        m, s = divmod(secs, 60)
        h, m = divmod(m, 60)
        if h:
            return f"ETA: {h}h{m:02d}m{s:02d}s"
        if m:
            return f"ETA: {m}m{s:02d}s"
        return f"ETA: {s}s"

    while True:
        to_scan = pending - seen_subjects
        if not to_scan:
            break
        scan_passes += 1
        pass_start = time.time()
        pass_done_bytes = 0
        last_progress_log = 0.0
        print(
            "[WDC_DEDUP] "
            f"pass {scan_passes}: scanning for {len(to_scan):,} subject(s) "
            f"(roots={len(roots):,}, pending_total={len(pending):,})"
        )
        newly_discovered = set()
        for fp in wdc_nq_paths or []:
            if should_cancel and should_cancel():
                raise PipelineError("Cancelled by user")
            file_size = 0
            file_done_bytes = 0
            try:
                file_size = int(os.path.getsize(fp))
            except Exception:
                file_size = 0
            try:
                with open(fp, "r", encoding="utf-8", errors="ignore") as f:
                    progress_line_stride = 20000
                    for line_idx, line in enumerate(f, start=1):
                        if should_cancel and line_idx % 200000 == 0 and should_cancel():
                            raise PipelineError("Cancelled by user")
                        line_len = len(line)
                        pass_done_bytes += line_len
                        file_done_bytes += line_len

                        subject_key = _fast_subject_key_from_nq_line(line)
                        if not subject_key or subject_key not in to_scan:
                            if line_idx % progress_line_stride == 0:
                                now = time.time()
                                if (now - last_progress_log) >= 10.0:
                                    pct = (file_done_bytes / file_size * 100.0) if file_size > 0 else 0.0
                                    print(
                                        "[WDC_DEDUP] "
                                        f"pass {scan_passes} file={Path(fp).name} "
                                        f"{pct:5.1f}% | {_eta_text(pass_done_bytes, file_size, pass_start)} | "
                                        f"matched={matched_subject_lines:,}"
                                    )
                                    last_progress_log = now
                            continue

                        parsed = build.parse_nq_or_nt(line)
                        if not parsed:
                            if line_idx % progress_line_stride == 0:
                                now = time.time()
                                if (now - last_progress_log) >= 10.0:
                                    pct = (file_done_bytes / file_size * 100.0) if file_size > 0 else 0.0
                                    print(
                                        "[WDC_DEDUP] "
                                        f"pass {scan_passes} file={Path(fp).name} "
                                        f"{pct:5.1f}% | {_eta_text(pass_done_bytes, file_size, pass_start)} | "
                                        f"matched={matched_subject_lines:,}"
                                    )
                                    last_progress_log = now
                            continue

                        parsed_lines += 1
                        _s, p, o = parsed
                        matched_subject_lines += 1
                        outgoing[subject_key].append((p, o))
                        if isinstance(o, str) and o.startswith("_:") and o not in seen_subjects and o not in to_scan:
                            newly_discovered.add(o)

                        if line_idx % progress_line_stride == 0:
                            now = time.time()
                            if (now - last_progress_log) >= 10.0:
                                pct = (file_done_bytes / file_size * 100.0) if file_size > 0 else 0.0
                                print(
                                    "[WDC_DEDUP] "
                                    f"pass {scan_passes} file={Path(fp).name} "
                                    f"{pct:5.1f}% | {_eta_text(pass_done_bytes, file_size, pass_start)} | "
                                    f"matched={matched_subject_lines:,}"
                                )
                                last_progress_log = now
            except PipelineError:
                raise
            except FileNotFoundError:
                continue
            if file_size > 0:
                print(
                    "[WDC_DEDUP] "
                    f"pass {scan_passes} file={Path(fp).name} done "
                    f"(matched={matched_subject_lines:,})"
                )
        seen_subjects.update(to_scan)
        pending.update(newly_discovered)
        pass_elapsed = time.time() - pass_start
        print(
            "[WDC_DEDUP] "
            f"pass {scan_passes} done in {int(pass_elapsed)}s | "
            f"new_bnodes={len(newly_discovered):,} | collected_subjects={len(outgoing):,}"
        )

    return outgoing, {
        "scan_passes": int(scan_passes),
        "parsed_lines": int(parsed_lines),
        "matched_subject_lines": int(matched_subject_lines),
        "subjects_requested": int(len(roots)),
        "subjects_collected": int(len([s for s in roots if s in outgoing])),
        "total_bytes_all_files": int(total_bytes_all_files),
    }


_NOISY_PREDICATE_HINTS = (
    "datecreated",
    "datemodified",
    "datepublished",
    "crawl",
    "timestamp",
    "version",
    "lastupdated",
    "mainentityofpage",
    "sameas",
    "url",
    "image",
    "thumbnail",
)

_STRONG_PREDICATE_HINTS = (
    "name",
    "label",
    "title",
    "identifier",
    "code",
    "iata",
    "icao",
    "isrc",
    "isbn",
    "issn",
    "postalcode",
    "latitude",
    "longitude",
    "coord",
    "telephone",
    "phone",
    "email",
)


def _canonical_link_value_for_dedup(value) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        norm = align.normalize_value_for_matching(raw, phone_mode=False)
    except Exception:
        norm = raw
    norm = str(norm or "").strip().lower()
    if not norm:
        return ""
    norm = unicodedata.normalize("NFKD", norm)
    norm = "".join(ch for ch in norm if not unicodedata.combining(ch))
    norm = "".join(ch for ch in norm if not unicodedata.category(ch).startswith("C"))
    norm = re.sub(r"[\s\-\.,;:|/\\_(){}\[\]\"'`]+", " ", norm)
    return " ".join(norm.split())


def _literal_lex_token(value):
    if not isinstance(value, str) or not value.startswith('"'):
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


def _normalize_attr_literal_value(text):
    value = str(text or "").strip().lower()
    if not value:
        return ""
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = "".join(ch for ch in value if not unicodedata.category(ch).startswith("C"))
    value = re.sub(r"[\s\-\.,;:|/\\_(){}\[\]\"'`]+", " ", value)
    return " ".join(value.split())


def _is_noisy_predicate(predicate_key):
    pred = str(predicate_key or "").lower()
    return any(hint in pred for hint in _NOISY_PREDICATE_HINTS)


def _is_strong_predicate(predicate_key):
    pred = str(predicate_key or "").lower()
    return any(hint in pred for hint in _STRONG_PREDICATE_HINTS)


def _collect_attr_signature_for_subject(root_subject, outgoing):
    root = _canonical_wdc_token(root_subject)
    if root not in outgoing:
        return None
    visited_bnodes = set()
    stack = [root]
    attr_pairs = set()
    strong_pairs = set()
    while stack:
        node = stack.pop()
        if node in visited_bnodes:
            continue
        visited_bnodes.add(node)
        for p, o in list(outgoing.get(node, [])):
            pred = _canonical_wdc_token(p).lower()
            if _is_noisy_predicate(pred):
                continue
            if isinstance(o, str) and o.startswith("_:"):
                stack.append(o)
                continue
            if not (isinstance(o, str) and o.startswith('"')):
                continue
            lex = _literal_lex_token(o)
            if lex is None:
                continue
            value_norm = _normalize_attr_literal_value(lex)
            if not value_norm:
                continue
            pair = (pred, value_norm)
            attr_pairs.add(pair)
            if _is_strong_predicate(pred):
                strong_pairs.add(pair)
    signature_hash = hashlib.sha256(
        json.dumps(sorted(attr_pairs), ensure_ascii=True).encode("utf-8")
    ).hexdigest()[:16]
    return {
        "attr_pairs": attr_pairs,
        "strong_pairs": strong_pairs,
        "signature_hash": signature_hash,
    }


def _jaccard_similarity(left, right):
    set_left = set(left or set())
    set_right = set(right or set())
    if not set_left and not set_right:
        return 1.0
    union = set_left | set_right
    if not union:
        return 1.0
    return len(set_left & set_right) / float(len(union))


def _compute_key_stats_after_filter(link_keys):
    counts = {}
    for key in list(link_keys or []):
        k = str(key or "").strip()
        if not k:
            continue
        counts[k] = counts.get(k, 0) + 1
    repeated = {k: c for k, c in counts.items() if c > 1}
    histogram = {}
    for freq in repeated.values():
        histogram[str(freq)] = histogram.get(str(freq), 0) + 1
    top_repeated = sorted(repeated.items(), key=lambda x: (-x[1], x[0]))[:30]
    return {
        "unique_key_count": int(sum(1 for c in counts.values() if c == 1)),
        "repeated_key_count": int(len(repeated)),
        "repeated_total_occurrences": int(sum(repeated.values())),
        "repetition_histogram": histogram,
        "top_repeated_keys": [{"key": key, "count": int(count)} for key, count in top_repeated],
    }


def _apply_strict_duplicate_key_filter(
    wdc_nq_paths,
    wdc_entities,
    wd_entities_raw,
    wdc_values=None,
    wd_values=None,
    should_cancel=None,
):
    total = min(len(wdc_entities), len(wd_entities_raw))
    wdc_entities = list(wdc_entities[:total])
    wd_entities_raw = list(wd_entities_raw[:total])
    wdc_values = list(wdc_values or [])
    wd_values = list(wd_values or [])
    similarity_threshold = float(os.environ.get("STRICT_DUPLICATE_KEY_SIMILARITY", "0.82"))

    if total <= 0:
        empty_report = {
            "summary": {
                "enabled": True,
                "links_before": 0,
                "links_after": 0,
                "filtered_out_links": 0,
                "repeated_key_groups": 0,
                "kept_groups_count": 0,
                "removed_groups_count": 0,
                "similarity_threshold": similarity_threshold,
                "reason": "no_links",
            },
            "kept_groups": [],
            "removed_groups": [],
            "entity_decisions": [],
            "examples": [],
            "scan": {"scan_passes": 0, "parsed_lines": 0, "matched_subject_lines": 0},
            "key_stats_after_filter": _compute_key_stats_after_filter([]),
        }
        return wdc_entities, wd_entities_raw, wdc_values, wd_values, empty_report, []

    wdc_keys = [_canonical_wdc_link_entity(v) for v in wdc_entities]
    wd_keys = [_canonical_wd_link_entity(v) for v in wd_entities_raw]
    link_keys = []
    for i in range(total):
        raw_value = wdc_values[i] if i < len(wdc_values) else ""
        key = _canonical_link_value_for_dedup(raw_value)
        if not key:
            key = f"__missing__::{i}"
        link_keys.append(key)

    group_to_indices = defaultdict(list)
    for idx, key in enumerate(link_keys):
        group_to_indices[key].append(idx)

    repeated_groups = []
    for key, indices in group_to_indices.items():
        unique_wdc = sorted({_canonical_wdc_link_entity(wdc_entities[i]) for i in indices if wdc_entities[i]})
        if len(unique_wdc) > 1:
            repeated_groups.append((key, sorted(indices), unique_wdc))
    repeated_groups.sort(key=lambda item: item[0])

    if not repeated_groups:
        all_decisions = []
        tsv_rows = []
        for i in range(total):
            row = {
                "index": i,
                "key": link_keys[i],
                "wdc_entity": wdc_entities[i],
                "wikidata_entity": wd_entities_raw[i],
                "decision": "keep",
                "reason": "unique_key_or_single_entity",
                "signature_hash": "",
            }
            all_decisions.append(row)
            tsv_rows.append(row)
        report = {
            "summary": {
                "enabled": True,
                "links_before": int(total),
                "links_after": int(total),
                "filtered_out_links": 0,
                "repeated_key_groups": 0,
                "kept_groups_count": 0,
                "removed_groups_count": 0,
                "similarity_threshold": similarity_threshold,
                "reason": "no_repeated_keys",
            },
            "kept_groups": [],
            "removed_groups": [],
            "entity_decisions": all_decisions,
            "examples": [],
            "scan": {"scan_passes": 0, "parsed_lines": 0, "matched_subject_lines": 0},
            "key_stats_after_filter": _compute_key_stats_after_filter(link_keys),
        }
        return wdc_entities, wd_entities_raw, wdc_values, wd_values, report, tsv_rows

    subjects_to_profile = {
        _canonical_wdc_link_entity(wdc_entities[i])
        for _, indices, _unique_wdc in repeated_groups
        for i in indices
        if _canonical_wdc_link_entity(wdc_entities[i])
    }
    print(
        "[WDC_DUPKEY] "
        f"profiling {len(subjects_to_profile):,} WDC subjects across {len(repeated_groups):,} repeated-key groups"
    )
    outgoing, scan_report = _collect_wdc_outgoing_subgraphs(
        wdc_nq_paths,
        subjects_to_profile,
        should_cancel=should_cancel,
    )

    signatures = {}
    for subject in sorted(subjects_to_profile):
        signatures[subject] = _collect_attr_signature_for_subject(subject, outgoing)

    keep_idx = set(range(total))
    removed_groups = []
    kept_groups = []
    all_decisions = []
    tsv_rows = []
    examples = []
    decisions_by_index = {}

    for key, indices, unique_wdc_entities in repeated_groups:
        if should_cancel and should_cancel():
            raise PipelineError("Cancelled by user")
        unique_entities = [str(v) for v in unique_wdc_entities]
        richness_rows = []
        for entity in unique_entities:
            sig = signatures.get(entity) or {}
            attr_pairs = set(sig.get("attr_pairs") or set())
            strong_pairs = set(sig.get("strong_pairs") or set())
            richness_rows.append(
                {
                    "entity": entity,
                    "attr_count": int(len(attr_pairs)),
                    "strong_count": int(len(strong_pairs)),
                    "signature_hash": str(sig.get("signature_hash") or ""),
                }
            )
        richness_rows.sort(
            key=lambda row: (
                -row["attr_count"],
                -row["strong_count"],
                row["entity"],
            )
        )
        selected_entity = richness_rows[0]["entity"] if richness_rows else unique_entities[0]
        selected_idx = [idx for idx in indices if _canonical_wdc_link_entity(wdc_entities[idx]) == selected_entity]
        removed_idx = [idx for idx in indices if idx not in selected_idx]
        for idx in removed_idx:
            keep_idx.discard(idx)
        group_payload = {
            "key": key,
            "occurrences": int(len(indices)),
            "unique_wdc_entities": unique_entities,
            "selection_mode": "richest_entity",
            "selected_wdc_entity": selected_entity,
            "selected_occurrences": int(len(selected_idx)),
            "removed_occurrences": int(len(removed_idx)),
            "richness": richness_rows,
            "decision": "keep_selected_only",
            "reason": "one_to_one_keep_richest_wdc_entity",
        }
        if removed_idx:
            removed_groups.append(group_payload)
            if len(examples) < 25:
                examples.append(group_payload)
        else:
            kept_groups.append(group_payload)

        for idx in indices:
            subject = _canonical_wdc_link_entity(wdc_entities[idx])
            sig = signatures.get(subject) or {}
            is_selected = idx in selected_idx
            decision = "keep" if is_selected else "remove"
            decision_reason = (
                "selected_richest_wdc_entity"
                if is_selected
                else f"removed_non_selected_wdc_entity:{selected_entity}"
            )
            decision_row = {
                "index": int(idx),
                "key": key,
                "wdc_entity": wdc_entities[idx],
                "wikidata_entity": wd_entities_raw[idx],
                "decision": decision,
                "reason": decision_reason,
                "signature_hash": str(sig.get("signature_hash") or ""),
            }
            decisions_by_index[idx] = decision_row
            tsv_rows.append(decision_row)

    for i in range(total):
        if i in decisions_by_index:
            all_decisions.append(decisions_by_index[i])
            continue
        row = {
            "index": int(i),
            "key": link_keys[i],
            "wdc_entity": wdc_entities[i],
            "wikidata_entity": wd_entities_raw[i],
            "decision": "keep",
            "reason": "unique_key_or_single_entity",
            "signature_hash": "",
        }
        all_decisions.append(row)
        tsv_rows.append(row)

    keep_order = sorted(keep_idx)
    filtered_wdc = [wdc_entities[i] for i in keep_order]
    filtered_wd = [wd_entities_raw[i] for i in keep_order]
    filtered_wdc_values = [wdc_values[i] for i in keep_order if i < len(wdc_values)]
    filtered_wd_values = [wd_values[i] for i in keep_order if i < len(wd_values)]
    kept_keys = [link_keys[i] for i in keep_order]

    report = {
        "summary": {
            "enabled": True,
            "links_before": int(total),
            "links_after": int(len(filtered_wdc)),
            "filtered_out_links": int(total - len(filtered_wdc)),
            "repeated_key_groups": int(len(repeated_groups)),
            "kept_groups_count": int(len(kept_groups)),
            "removed_groups_count": int(len(removed_groups)),
            "similarity_threshold": similarity_threshold,
            "reason": "ok",
        },
        "kept_groups": kept_groups,
        "removed_groups": removed_groups,
        "entity_decisions": all_decisions,
        "examples": examples,
        "scan": scan_report,
        "key_stats_after_filter": _compute_key_stats_after_filter(kept_keys),
    }
    print(
        "[WDC_DUPKEY] "
        f"done: kept_groups={len(kept_groups):,}, removed_groups={len(removed_groups):,}, "
        f"links_kept={len(filtered_wdc):,}/{total:,}"
    )
    return filtered_wdc, filtered_wd, filtered_wdc_values, filtered_wd_values, report, tsv_rows



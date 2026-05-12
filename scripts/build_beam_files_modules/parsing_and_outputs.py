#!/usr/bin/env python3
import argparse
import os
import re
import json
import sys
import time
import fcntl
from pathlib import Path
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, as_completed, wait
from typing import Iterable, List
from urllib.parse import parse_qs, unquote, urlparse

import requests

_CPU_COUNT = max(1, os.cpu_count() or 1)
BUILD_CPU_SHARE = float(os.environ.get("BUILD_CPU_SHARE", "0.95"))


QUAD_RE = re.compile(
    r'^\s*(<[^>]+>|_:[^\s]+)\s+(<[^>]+>)\s+(".*?"(?:\^\^<[^>]+>|@[a-zA-Z-]+)?|<[^>]+>|_:[^\s]+)\s+(<[^>]+>)\s+\.\s*$'
)
TRIPLE_RE = re.compile(
    r'^\s*(<[^>]+>|_:[^\s]+)\s+(<[^>]+>)\s+(".*?"(?:\^\^<[^>]+>|@[a-zA-Z-]+)?|<[^>]+>|_:[^\s]+)\s+\.\s*$'
)


def parse_nq_or_nt(line):
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    m = QUAD_RE.match(line)
    if m:
        s, p, o, _g = m.groups()
        return s, p, o
    m = TRIPLE_RE.match(line)
    if m:
        s, p, o = m.groups()
        return s, p, o
    return None


def _format_eta(seconds):
    if seconds is None or seconds < 0:
        return "ETA: N/A"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h:
        return f"ETA: {h}h{m:02d}m{s:02d}s"
    if m:
        return f"ETA: {m}m{s:02d}s"
    return f"ETA: {s}s"


def _normalize_prop_token(value):
    token = value.strip("<>")
    if token.startswith("http://www.wikidata.org/"):
        return token.lower()
    return token


def _eta_update(start_ts, done_bytes, total_bytes):
    if done_bytes <= 0:
        return "ETA: N/A"
    elapsed = time.time() - start_ts
    if elapsed <= 0:
        return "ETA: N/A"
    rate = done_bytes / elapsed
    remaining = max(0, total_bytes - done_bytes)
    return _format_eta(remaining / rate if rate > 0 else None)


def _progress_line(start_ts, done_bytes, total_bytes):
    pct = 0.0 if total_bytes <= 0 else (done_bytes / total_bytes) * 100
    return f"{pct:5.1f}% | {_eta_update(start_ts, done_bytes, total_bytes)}"


def _split_worker(args):
    (
        input_path,
        targets_inner,
        lowercase_wd,
        mask_values,
        exclude_props,
        exclude_prop_patterns,
        replace_map,
        follow_iri_objects,
        linked_entities_inner,
    ) = args
    tmp_attr = input_path + f".tmp_attr_{os.getpid()}"
    tmp_rel = input_path + f".tmp_rel_{os.getpid()}"
    new_subjects = set()
    line_count = 0
    kept_attr = 0
    kept_rel = 0
    exclude_props_norm = {_normalize_prop_token(p) for p in exclude_props} if exclude_props else None
    with open(input_path, "r", encoding="utf-8") as f, \
         open(tmp_attr, "w", encoding="utf-8") as attr_out, \
         open(tmp_rel, "w", encoding="utf-8") as rel_out:
        for line in f:
            line_count += 1
            parsed = parse_nq_or_nt(line)
            if not parsed:
                continue
            s, p, o = parsed
            s_inner = s[1:-1] if s.startswith("<") and s.endswith(">") else s
            s_inner_norm = normalize_entity_token(s_inner, lowercase_wd=lowercase_wd)
            p_norm = _normalize_prop_token(p)
            if linked_entities_inner and s.startswith("<") and s_inner_norm not in linked_entities_inner:
                continue
            if s_inner_norm not in targets_inner:
                continue
            if exclude_props_norm and p_norm in exclude_props_norm:
                continue
            if exclude_prop_patterns and any(pat in p_norm.lower() for pat in exclude_prop_patterns):
                continue
            if replace_map:
                s_key = s.strip("<>")
                if s_key in replace_map:
                    s = replace_map[s_key]
            if replace_map and (not o.startswith('"')):
                o_key = o.strip("<>")
                if o_key in replace_map:
                    o = replace_map[o_key]
            s_out, p_out, o_out = transform_triple(s, p, o, lowercase_wd)
            if o.startswith('"'):
                o_out = clean_literal(o_out)
                if mask_values:
                    lex = literal_lex(o_out)
                    if lex in mask_values:
                        continue
                attr_out.write(f"{s_out}\t{p_out}\t{o_out}\n")
                kept_attr += 1
            else:
                if o.startswith("<") and linked_entities_inner:
                    o_inner = o[1:-1]
                    o_inner_norm = normalize_entity_token(o_inner, lowercase_wd=lowercase_wd)
                    if o_inner_norm not in linked_entities_inner:
                        continue
                rel_out.write(f"{s_out}\t{p_out}\t{o_out}\n")
                kept_rel += 1
                if o.startswith("_:"):
                    new_subjects.add(o)
                elif follow_iri_objects and o.startswith("<"):
                    if not linked_entities_inner:
                        new_subjects.add(normalize_entity_token(o, lowercase_wd=lowercase_wd))
                    else:
                        o_inner = o[1:-1]
                        o_inner_norm = normalize_entity_token(o_inner, lowercase_wd=lowercase_wd)
                        if o_inner_norm in linked_entities_inner:
                            new_subjects.add(o_inner_norm)
    size = os.path.getsize(input_path)
    return tmp_attr, tmp_rel, new_subjects, line_count, kept_attr, kept_rel, size


def _count_worker(args):
    path, subjects, exclude_props, exclude_prop_patterns, mask_values = args
    local = {}
    exclude_props_norm = {_normalize_prop_token(p) for p in exclude_props} if exclude_props else None
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            parsed = parse_nq_or_nt(line)
            if not parsed:
                continue
            s, p, o = parsed
            p_norm = _normalize_prop_token(p)
            if s not in subjects:
                continue
            if exclude_props_norm and p_norm in exclude_props_norm:
                continue
            if exclude_prop_patterns and any(pat in p_norm.lower() for pat in exclude_prop_patterns):
                continue
            if mask_values and o.startswith('"'):
                lex = literal_lex(o)
                if lex in mask_values:
                    continue
            local[s] = local.get(s, 0) + 1
    size = os.path.getsize(path)
    return local, size


def _labels_worker(args):
    path, target_iris, label_preds = args
    tmp = path + f".tmp_wdc_labels_{os.getpid()}"
    written = 0
    with open(path, "r", encoding="utf-8") as f, open(tmp, "w", encoding="utf-8") as out:
        for line in f:
            parsed = parse_nq_or_nt(line)
            if not parsed:
                continue
            s, p, o = parsed
            s_norm = s.strip("<>")
            p_norm = p.strip("<>")
            if s_norm not in target_iris:
                continue
            if p_norm not in label_preds:
                continue
            if o.startswith('"'):
                o = clean_literal(o)
            out.write(f"{s}\t{p}\t{o}\n")
            written += 1
    size = os.path.getsize(path)
    return tmp, written, size


def _prop_label_worker(args):
    path, targets, label_preds = args
    local_labels = {}
    local_descs = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            parsed = parse_nq_or_nt(line)
            if not parsed:
                continue
            s, p, o = parsed
            s_norm = s.strip("<>")
            p_norm = p.strip("<>")
            if s_norm not in targets:
                continue
            if p_norm not in label_preds:
                continue
            lex = literal_lex(o) or o.strip('"')
            if p_norm.endswith("#label") or p_norm.endswith("prefLabel"):
                if s not in local_labels:
                    local_labels[s] = lex
            elif p_norm.endswith("description"):
                if s not in local_descs:
                    local_descs[s] = lex
    size = os.path.getsize(path)
    return local_labels, local_descs, size


def _is_pid_alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _normalize_worker_share(value, default=0.95):
    try:
        share = float(value)
    except Exception:
        share = float(default)
    if share <= 0 or share > 1.0:
        return float(default)
    return share


def compute_shared_workers(lock_path, share=None):
    share = _normalize_worker_share(BUILD_CPU_SHARE if share is None else share)
    cpu = _CPU_COUNT
    lock_path = os.path.abspath(lock_path)
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)
    with open(lock_path, "a+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        f.seek(0)
        lines = [ln.strip() for ln in f.readlines() if ln.strip()]
        active_pids = []
        for ln in lines:
            try:
                pid_str, _ts = ln.split(",", 1)
                pid = int(pid_str)
            except Exception:
                continue
            if _is_pid_alive(pid):
                active_pids.append(pid)
        if os.getpid() not in active_pids:
            active_pids.append(os.getpid())
        f.seek(0)
        f.truncate()
        now = int(time.time())
        for pid in active_pids:
            f.write(f"{pid},{now}\n")
        f.flush()
        fcntl.flock(f, fcntl.LOCK_UN)
    runs = max(1, len(active_pids))
    workers = max(1, int((cpu * share) / runs))
    return workers, runs, cpu


def _iter_input_paths(input_paths):
    if isinstance(input_paths, (list, tuple)):
        return list(input_paths)
    return [input_paths]


def normalize_header(value):
    return value.strip().lower().replace(" ", "")


def read_links(path, sep, wdc_col, wd_col, wdc_value_col, wd_value_col):
    wdc_entities = []
    wd_entities = []
    wdc_values = []
    wd_values = []
    with open(path, "r", encoding="utf-8") as f:
        first = f.readline()
        if not first:
            return wdc_entities, wd_entities, wdc_values, wd_values
        parts = [normalize_header(p) for p in first.rstrip("\n").split(sep)]
        header = "wdc_iri" in parts and "wikidata_uri" in parts
        header_map = {name: idx for idx, name in enumerate(parts)}

        if header:
            wdc_col = header_map.get("wdc_iri", wdc_col)
            wd_col = header_map.get("wikidata_uri", wd_col)
            if wdc_value_col is None:
                wdc_value_col = header_map.get("wdc_value")
            if wd_value_col is None:
                wd_value_col = header_map.get("wiki_value")
        else:
            parts = first.rstrip("\n").split(sep)
            if len(parts) > max(wdc_col, wd_col):
                wdc_entities.append(parts[wdc_col].strip())
                wd_entities.append(parts[wd_col].strip())
                if wdc_value_col is not None and len(parts) > wdc_value_col:
                    wdc_values.append(parts[wdc_value_col].strip())
                if wd_value_col is not None and len(parts) > wd_value_col:
                    wd_values.append(parts[wd_value_col].strip())

        for line in f:
            cols = line.rstrip("\n").split(sep)
            if len(cols) <= max(wdc_col, wd_col):
                continue
            wdc_entities.append(cols[wdc_col].strip())
            wd_entities.append(cols[wd_col].strip())
            if wdc_value_col is not None and len(cols) > wdc_value_col:
                wdc_values.append(cols[wdc_value_col].strip())
            if wd_value_col is not None and len(cols) > wd_value_col:
                wd_values.append(cols[wd_value_col].strip())
    return wdc_entities, wd_entities, wdc_values, wd_values


def normalize_wd_uri(value, lowercase):
    # Normalize URI token shape first so all downstream files use a stable form.
    text = (value or "").strip()
    if text.startswith("<") and text.endswith(">"):
        text = text[1:-1]
    if "wikidata.org/" in text.lower():
        # Property URIs stay in the prop namespace.
        m = re.match(r"^https?://(?:www\.)?wikidata\.org/(prop(?:/[^/]+)*/)([Pp]\d+)$", text)
        if m:
            pid = m.group(2).lower() if lowercase else m.group(2).upper()
            return f"http://www.wikidata.org/{m.group(1)}{pid}"
        # Canonical entity URI with enforced lowercase q/p identifiers.
        ent = canonical_wd_link_entity_uri(text, lowercase=True)
        if ent != text.strip("<>"):
            return ent
    return text


def _format_iri_or_term(value):
    text = (value or "").strip()
    if not text:
        return text
    if text.startswith('"'):
        return text
    if text.startswith("_:"):
        return text
    if text.startswith("<") and text.endswith(">"):
        return text
    if text.startswith("http://") or text.startswith("https://"):
        return f"<{text}>"
    return text


def transform_triple(s, p, o, lowercase):
    s = normalize_wd_uri(s, lowercase)
    p = normalize_wd_uri(p, lowercase)
    if not o.startswith('"'):
        o = normalize_wd_uri(o, lowercase)
    s = _format_iri_or_term(s)
    p = _format_iri_or_term(p)
    o = _format_iri_or_term(o) if not o.startswith('"') else o
    return s, p, o


def write_links(path, wdc_entities, wd_entities, dedupe):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    seen = set()
    with open(path, "w", encoding="utf-8") as out:
        for wdc, wd in zip(wdc_entities, wd_entities):
            wdc = wdc.strip().strip("<>")
            wd = canonical_wd_link_entity_uri(wd, lowercase=True)
            if not wdc or not wd:
                continue
            if not is_allowed_wdc_subject(wdc):
                continue
            if dedupe:
                key = (wdc, wd)
                if key in seen:
                    continue
                seen.add(key)
            out.write(f"{wdc}\t{wd}\n")


LITERAL_RE = re.compile(r'^"((?:[^"\\]|\\.)*)"(?:(?:\^\^<[^>]+>)|@[a-zA-Z-]+)?$')


def literal_lex(value):
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


def clean_literal(value):
    if not value.startswith('"'):
        return value
    lex = literal_lex(value)
    if lex is None:
        match = LITERAL_RE.match(value)
        if not match:
            return value
        lex = match.group(1)
    return f"\"{lex}\""


def _cleanup_stale_temp_files(input_paths, stale_after_s=None):
    """
    Best-effort cleanup for orphaned worker temp files from interrupted runs.
    Uses an age threshold to avoid touching temp files from actively running workers.
    """
    if stale_after_s is None:
        try:
            stale_after_s = int(os.environ.get("BEAM_TMP_CLEANUP_STALE_S", "300"))
        except Exception:
            stale_after_s = 300
    now = time.time()
    seen = set()
    for raw in _iter_input_paths(input_paths):
        p = Path(raw)
        parent = p.parent
        stem = p.name
        for pat in (
            f"{stem}.tmp_attr_*",
            f"{stem}.tmp_rel_*",
            f"{stem}.tmp_wdc_labels_*",
        ):
            for cand in parent.glob(pat):
                key = str(cand.resolve())
                if key in seen:
                    continue
                seen.add(key)
                try:
                    age = now - cand.stat().st_mtime
                    if age < stale_after_s:
                        continue
                    cand.unlink(missing_ok=True)
                except Exception:
                    pass


def split_triples(
    input_path,
    out_attr_path,
    out_rel_path,
    seed_subjects,
    max_depth,
    lowercase_wd=False,
    mask_values=None,
    exclude_props=None,
    exclude_prop_patterns=None,
    replace_map=None,
    progress_every=0,
    follow_iri_objects=False,
    linked_entity_iris=None,
):
    _cleanup_stale_temp_files(input_path)
    os.makedirs(os.path.dirname(out_attr_path), exist_ok=True)
    os.makedirs(os.path.dirname(out_rel_path), exist_ok=True)

    keep_subjects = set(
        normalize_entity_token(s, lowercase_wd=lowercase_wd)
        for s in seed_subjects
        if s
    )
    linked_entities_inner = set()
    for value in list(linked_entity_iris or []):
        token = str(value or "").strip()
        if not token:
            continue
        token = normalize_entity_token(token, lowercase_wd=lowercase_wd)
        linked_entities_inner.add(token)
    processed_subjects = set()

    input_paths = _iter_input_paths(input_path)
    with open(out_attr_path, "w", encoding="utf-8") as attr_out, \
         open(out_rel_path, "w", encoding="utf-8") as rel_out:
        depth = 0
        while True:
            if max_depth >= 0 and depth > max_depth:
                break
            targets = keep_subjects - processed_subjects
            if not targets:
                break
            targets_inner = set()
            for value in targets:
                token = str(value or "").strip()
                if not token:
                    continue
                token = normalize_entity_token(token, lowercase_wd=lowercase_wd)
                targets_inner.add(token)
            new_subjects = set()
            line_count = 0
            kept_attr = 0
            kept_rel = 0
            lock_path = os.path.join("Download", ".workers.lock")
            n_workers, _runs, _cpu = compute_shared_workers(lock_path, share=BUILD_CPU_SHARE)
            total_bytes = sum(os.path.getsize(p) for p in input_paths)
            done_bytes = 0
            start_ts = time.time()
            if progress_every:
                prog = _progress_line(start_ts, done_bytes, total_bytes)
                print(f"[WDC] depth={depth} progress {done_bytes}/{total_bytes} bytes {prog}", file=sys.stderr)
            with ProcessPoolExecutor(max_workers=n_workers) as ex:
                futures = [
                    ex.submit(
                        _split_worker,
                        (
                            input_path,
                            targets_inner,
                            lowercase_wd,
                            mask_values,
                            exclude_props,
                            exclude_prop_patterns,
                            replace_map,
                            follow_iri_objects,
                            linked_entities_inner,
                        ),
                    )
                    for input_path in input_paths
                ]
                for fut in as_completed(futures):
                    tmp_attr, tmp_rel, new_subs, lines, ka, kr, fsize = fut.result()
                    line_count += lines
                    kept_attr += ka
                    kept_rel += kr
                    new_subjects.update(new_subs)
                    done_bytes += fsize
                    prog = _progress_line(start_ts, done_bytes, total_bytes)
                    print(f"[WDC] depth={depth} progress {done_bytes}/{total_bytes} bytes {prog}", file=sys.stderr)
                    try:
                        with open(tmp_attr, "r", encoding="utf-8") as f_attr:
                            for line in f_attr:
                                attr_out.write(line)
                        with open(tmp_rel, "r", encoding="utf-8") as f_rel:
                            for line in f_rel:
                                rel_out.write(line)
                    finally:
                        try:
                            os.remove(tmp_attr)
                        except Exception:
                            pass
                        try:
                            os.remove(tmp_rel)
                        except Exception:
                            pass
            if progress_every:
                done_bytes = total_bytes
                prog = _progress_line(start_ts, done_bytes, total_bytes)
                print(f"[WDC] depth={depth} progress {done_bytes}/{total_bytes} bytes {prog}", file=sys.stderr)
            processed_subjects.update(targets)
            keep_subjects.update(new_subjects)
            print(
                f"[WDC] depth={depth} done lines={line_count} "
                f"attr={kept_attr} rel={kept_rel} new_bnodes={len(new_subjects)}",
                file=sys.stderr,
            )
            depth += 1
    _cleanup_stale_temp_files(input_path)


def batch_iter(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def dedupe_preserve_order(items):
    seen = set()
    out = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _read_raw_wd_triples(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t", 2)
            if len(parts) != 3:
                continue
            yield parts[0], parts[1], parts[2]


def count_wdc_triples(input_path, subjects, exclude_props=None, exclude_prop_patterns=None, mask_values=None):
    counts = {s: 0 for s in subjects}
    input_paths = _iter_input_paths(input_path)

    # Parallel over parts
    lock_path = os.path.join("Download", ".workers.lock")
    n_workers, _runs, _cpu = compute_shared_workers(lock_path, share=BUILD_CPU_SHARE)
    total_bytes = sum(os.path.getsize(p) for p in input_paths)
    done_bytes = 0
    start_ts = time.time()
    prog = _progress_line(start_ts, done_bytes, total_bytes)
    print(f"[WDC] count_triples progress {done_bytes}/{total_bytes} bytes {prog}", file=sys.stderr)
    with ProcessPoolExecutor(max_workers=n_workers) as ex:
        futures = [
            ex.submit(_count_worker, (p, set(subjects), exclude_props, exclude_prop_patterns, mask_values))
            for p in input_paths
        ]
        for fut in as_completed(futures):
            local, fsize = fut.result()
            for s, c in local.items():
                counts[s] += c
            done_bytes += fsize
            prog = _progress_line(start_ts, done_bytes, total_bytes)
            print(f"[WDC] count_triples progress {done_bytes}/{total_bytes} bytes {prog}", file=sys.stderr)
    done_bytes = total_bytes
    prog = _progress_line(start_ts, done_bytes, total_bytes)
    print(f"[WDC] count_triples progress {done_bytes}/{total_bytes} bytes {prog}", file=sys.stderr)
    return counts


def filter_links_by_wdc(wdc_entities, wd_entities, wdc_values, wd_values, allowed_wdc):
    new_wdc = []
    new_wd = []
    new_wdc_vals = []
    new_wd_vals = []
    for wdc, wd, wv, wdv in zip(wdc_entities, wd_entities, wdc_values, wd_values):
        if wdc in allowed_wdc:
            new_wdc.append(wdc)
            new_wd.append(wd)
            new_wdc_vals.append(wv)
            new_wd_vals.append(wdv)
    return new_wdc, new_wd, new_wdc_vals, new_wd_vals


def build_wd_merge_map(wd_entities, wd_values):
    value_to_ents = {}
    for ent, val in zip(wd_entities, wd_values):
        if not val:
            continue
        value_to_ents.setdefault(val, set()).add(ent)
    replace_map = {}
    for ents in value_to_ents.values():
        if len(ents) <= 1:
            continue
        canonical = sorted(ents)[0]
        for ent in ents:
            if ent != canonical:
                replace_map[ent] = canonical
    return replace_map


def count_props_in_files(paths, exclude_props=None):
    counts = {}
    for path in paths:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                parts = line.rstrip("\n").split("\t")
                if len(parts) != 3:
                    continue
                p = parts[1]
                if exclude_props and p in exclude_props:
                    continue
                counts[p] = counts.get(p, 0) + 1
    return counts


def filter_triples_by_prop_count(
    in_attr,
    in_rel,
    out_attr,
    out_rel,
    min_count,
    exclude_props=None,
):
    counts = count_props_in_files([in_attr, in_rel], exclude_props=exclude_props)
    os.makedirs(os.path.dirname(out_attr), exist_ok=True)
    os.makedirs(os.path.dirname(out_rel), exist_ok=True)
    with open(in_attr, "r", encoding="utf-8") as fin, \
         open(out_attr, "w", encoding="utf-8") as fout:
        for line in fin:
            parts = line.rstrip("\n").split("\t")
            if len(parts) != 3:
                continue
            p = parts[1]
            if exclude_props and p in exclude_props:
                continue
            if counts.get(p, 0) >= min_count:
                fout.write(line)


def wikidata_prop_uris(prop_id):
    prop_id = prop_id.lower()
    return {
        f"http://www.wikidata.org/prop/direct/{prop_id}",
        f"http://www.wikidata.org/prop/direct-normalized/{prop_id}",
        f"http://www.wikidata.org/prop/{prop_id}",
        f"http://www.wikidata.org/prop/statement/{prop_id}",
        f"http://www.wikidata.org/prop/statement/value/{prop_id}",
        f"http://www.wikidata.org/prop/statement/value-normalized/{prop_id}",
        f"http://www.wikidata.org/prop/qualifier/{prop_id}",
        f"http://www.wikidata.org/prop/qualifier/value/{prop_id}",
        f"http://www.wikidata.org/prop/qualifier/value-normalized/{prop_id}",
        f"http://www.wikidata.org/prop/reference/{prop_id}",
        f"http://www.wikidata.org/prop/reference/value/{prop_id}",
        f"http://www.wikidata.org/prop/reference/value-normalized/{prop_id}",
    }


def schema_org_prop_uris(prop_name):
    return {
        f"http://schema.org/{prop_name}",
        f"https://schema.org/{prop_name}",
    }


def normalize_wd_prop_id(value):
    match = re.search(r'P\d+', value, re.IGNORECASE)
    if not match:
        return None
    return match.group(0).lower()


def prop_uri_to_entity(uri):
    uri = uri.strip("<>")
    if "wikidata.org/prop/" not in uri:
        return None
    tail = uri.rstrip("/").split("/")[-1]
    if not (tail.startswith("P") or tail.startswith("p")):
        return None
    return f"http://www.wikidata.org/entity/{tail.upper()}"


def canonical_wd_entity_uri(uri):
    uri = (uri or "").strip().strip("<>")
    if not uri:
        return uri
    match = re.match(r"^https?://www\.wikidata\.org/entity/([pqPQ]\d+)$", uri)
    if match:
        return f"http://www.wikidata.org/entity/{match.group(1).lower()}"
    return uri


def _extract_wikidata_entity_id(uri):
    text = (uri or "").strip()
    if not text:
        return None
    text = text.strip("<>")
    m = re.fullmatch(r"[PpQq](\d+)", text)
    if m:
        return text[0].upper() + m.group(1)
    m = re.fullmatch(r"wd:([PpQq]\d+)", text, flags=re.IGNORECASE)
    if m:
        return m.group(1).upper()

    try:
        parsed = urlparse(unquote(text))
    except Exception:
        return None
    if parsed.scheme not in {"http", "https"}:
        return None
    host = (parsed.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    if host.startswith("m."):
        host = host[2:]
    if host != "wikidata.org":
        return None

    parts = [p for p in (parsed.path or "").split("/") if p]
    for token in reversed(parts):
        m = re.fullmatch(r"[PpQq](\d+)", token.strip())
        if m:
            return token[0].upper() + m.group(1)

    query_map = parse_qs(parsed.query or "", keep_blank_values=False)
    for key in ("title", "entity", "id", "q"):
        for raw in query_map.get(key, []):
            m = re.fullmatch(r"[PpQq](\d+)", str(raw).strip())
            if m:
                v = str(raw).strip()
                return v[0].upper() + m.group(1)

    frag = (parsed.fragment or "").strip()
    m = re.fullmatch(r"[PpQq](\d+)", frag)
    if m:
        return frag[0].upper() + m.group(1)
    return None


def canonical_wd_link_entity_uri(uri, lowercase=False):
    uri = (uri or "").strip()
    if not uri:
        return uri
    qid = _extract_wikidata_entity_id(uri)
    if not qid:
        return uri.strip("<>")
    if lowercase:
        qid = qid.lower()
    return f"http://www.wikidata.org/entity/{qid}"


_ESCAPED_URI_RE = re.compile(r"\\\\[ux][0-9a-fA-F]{2,4}")


def _decode_escaped_uri_token(token):
    text = (token or "").strip()
    if not text:
        return text
    if not _ESCAPED_URI_RE.search(text):
        return text
    try:
        return bytes(text, "utf-8").decode("unicode_escape")
    except Exception:
        return text


def normalize_entity_token(token, lowercase_wd=False):
    text = (token or "").strip().strip("<>")
    if not text:
        return text
    text = _decode_escaped_uri_token(text)
    if lowercase_wd:
        text = normalize_wd_uri(text, lowercase=True)
    if "wikidata.org/entity/" in text.lower():
        text = canonical_wd_link_entity_uri(text, lowercase=lowercase_wd)
    return text


def is_allowed_wdc_subject(token):
    text = (token or "").strip()
    if text.startswith("<") and text.endswith(">"):
        text = text[1:-1]
    return text.startswith("_:") or text.startswith("http://") or text.startswith("https://")


def filter_triples_by_subject_membership(
    in_attr,
    in_rel,
    out_attr,
    out_rel,
    allowed_subjects,
    lowercase_wd=False,
):
    allowed = set()
    for s in list(allowed_subjects or []):
        n = normalize_entity_token(s, lowercase_wd=lowercase_wd)
        if n:
            allowed.add(n)
    os.makedirs(os.path.dirname(out_attr), exist_ok=True)
    os.makedirs(os.path.dirname(out_rel), exist_ok=True)

    def _copy_filtered(inp, outp):
        kept = 0
        with open(inp, "r", encoding="utf-8") as fin, open(outp, "w", encoding="utf-8") as fout:
            for line in fin:
                parts = line.rstrip("\n").split("\t", 1)
                if len(parts) < 2:
                    continue
                subj = normalize_entity_token(parts[0], lowercase_wd=lowercase_wd)
                if subj not in allowed:
                    continue
                fout.write(line)
                kept += 1
        return kept

    kept_attr = _copy_filtered(in_attr, out_attr)
    kept_rel = _copy_filtered(in_rel, out_rel)
    return kept_attr, kept_rel


def collect_wikidata_uris(attr_path, rel_path):
    uris = set()
    prop_uri_map = {}
    for path in (attr_path, rel_path):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                parts = line.rstrip("\n").split("\t")
                if len(parts) != 3:
                    continue
                s, p, o = parts
                s_norm = s.strip("<>")
                p_norm = p.strip("<>")
                o_norm = o.strip("<>")
                if s_norm.startswith("http://www.wikidata.org/entity/"):
                    uris.add(canonical_wd_entity_uri(s_norm))
                elif s_norm.startswith("http://www.wikidata.org/prop/"):
                    ent = prop_uri_to_entity(s_norm)
                    if ent:
                        ent = canonical_wd_entity_uri(ent)
                        prop_uri_map[s_norm] = ent
                        uris.add(ent)
                if p_norm.startswith("http://www.wikidata.org/prop/"):
                    ent = prop_uri_to_entity(p_norm)
                    if ent:
                        ent = canonical_wd_entity_uri(ent)
                        prop_uri_map[p_norm] = ent
                        uris.add(ent)
                elif p_norm.startswith("http://www.wikidata.org/entity/"):
                    uris.add(canonical_wd_entity_uri(p_norm))
                if o_norm.startswith("http://www.wikidata.org/entity/"):
                    uris.add(canonical_wd_entity_uri(o_norm))
                elif o_norm.startswith("http://www.wikidata.org/prop/"):
                    ent = prop_uri_to_entity(o_norm)
                    if ent:
                        ent = canonical_wd_entity_uri(ent)
                        prop_uri_map[o_norm] = ent
                        uris.add(ent)
    return uris, prop_uri_map


def collect_wdc_iris(attr_path, rel_path):
    uris = set()
    prop_uris = set()
    for path in (attr_path, rel_path):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                parts = line.rstrip("\n").split("\t")
                if len(parts) != 3:
                    continue
                s, p, o = parts
                s_norm = s.strip("<>")
                p_norm = p.strip("<>")
                o_norm = o.strip("<>")
                if s_norm.startswith("http://") or s_norm.startswith("https://"):
                    uris.add(s_norm)
                if p_norm.startswith("http://") or p_norm.startswith("https://"):
                    prop_uris.add(p_norm)
                if o_norm.startswith("http://") or o_norm.startswith("https://"):
                    uris.add(o_norm)
    return uris, prop_uris



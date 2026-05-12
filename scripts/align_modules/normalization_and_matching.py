#!/usr/bin/env python3
"""
WDC Entity Linker - Download, Filter & Link to Wikidata

Usage:
    python app.py MusicRecording "isrc" "all" "wdt:P1243"
    python app.py Organization "vat" "0-2" "wdt:P1648"
"""

import sys
import argparse
import os
import re
import gzip
import hashlib
import shutil
import json
import requests
import unicodedata
import random
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
import time
import fcntl
from pathlib import Path
from collections import defaultdict
from urllib.parse import parse_qs, unquote, urljoin, urlparse
from bs4 import BeautifulSoup

# Configuration
WDC_BASE_URL = "https://data.dws.informatik.uni-mannheim.de/structureddata/2024-12/quads/classspecific/"
WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
TARGET_ENDPOINTS = {
    "wikidata": {
        "label": "Wikidata",
        "sparql_url": WIKIDATA_ENDPOINT,
        "supports_qid": True,
    },
    "dbpedia": {
        "label": "DBpedia",
        "sparql_url": "https://dbpedia.org/sparql",
        "supports_qid": False,
    },
    "yago": {
        "label": "YAGO",
        "sparql_url": "https://yago-knowledge.org/sparql/query",
        "supports_qid": False,
    },
    "custom": {
        "label": "Custom",
        "sparql_url": "",
        "supports_qid": False,
    },
}

# Endpoint-aware aliases for common Wikidata/semantic properties when querying non-Wikidata targets.
NON_WIKIDATA_PROPERTY_ALIASES = {
    "dbpedia": {
        "p238": "dbp:iata",
        "iata": "dbp:iata",
        "iatacode": "dbp:iata",
        "p212": "dbo:isbn",
        "isbn": "dbo:isbn",
        "p1329": "dbp:telephone",
        "telephone": "dbp:telephone",
        "phone": "dbp:phone",
    },
    "yago": {
        "p238": "schema:iataCode",
        "iata": "schema:iataCode",
        "iatacode": "schema:iataCode",
        "p212": "schema:isbn",
        "isbn": "schema:isbn",
        "p1329": "schema:telephone",
        "telephone": "schema:telephone",
        "phone": "schema:telephone",
    },
}
_PREFIX_DECL_RE = re.compile(r"^PREFIX\s+([A-Za-z][A-Za-z0-9_-]*)\s*:\s*<([^>\s]+)>\s*$", re.IGNORECASE)
_CPU_COUNT = max(1, os.cpu_count() or 1)
MAX_PARALLEL_WORKERS = int(os.environ.get("ALIGN_MAX_WORKERS", str(_CPU_COUNT)))
ALIGN_CPU_SHARE = float(os.environ.get("ALIGN_CPU_SHARE", "0.95"))

# Colors
class Colors:
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    BLUE = '\033[0;34m'
    CYAN = '\033[0;36m'
    RESET = '\033[0m'

def print_color(text, color):
    print(f"{color}{text}{Colors.RESET}")

_EXTRA_STRIP_CHARS = set()
_NORMALIZATION_ENABLED = True
_CANCEL_CHECK = None

def set_extra_strip_chars(strip_chars):
    global _EXTRA_STRIP_CHARS
    _EXTRA_STRIP_CHARS = set(strip_chars or [])
    return _EXTRA_STRIP_CHARS

def set_normalization(enabled: bool):
    global _NORMALIZATION_ENABLED
    _NORMALIZATION_ENABLED = bool(enabled)


def set_cancel_checker(fn):
    global _CANCEL_CHECK
    _CANCEL_CHECK = fn

def parse_strip_list(spec):
    if not spec:
        return []
    parts = [p for p in spec.split(";") if p != ""]
    chars = []
    named_chars = {
        "dot": ".",
        "period": ".",
        "hyphen": "-",
        "dash": "-",
        "semicolon": ";",
        "semi": ";",
        "comma": ",",
        "slash": "/",
        "underscore": "_",
        "colon": ":",
    }
    for p in parts:
        p = p.strip()
        if not p:
            continue
        p_lower = p.lower()
        if p_lower == "spaces":
            chars.extend([" ", "\t", "\n", "\r"])
            continue
        if p_lower == "special-chars":
            # Placeholder token handled in normalize_for_matching
            chars.append("__SPECIAL_CHARS__")
            continue
        if p_lower in named_chars:
            chars.append(named_chars[p_lower])
            continue
        # unescape common sequences
        p = p.replace("\\;", ";").replace("\\/", "/").replace("\\\\", "\\")
        p = p.replace("\\t", "\t").replace("\\n", "\n").replace("\\r", "\r")
        chars.append(p)
    return chars

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

def _eta_update(start_ts, done_bytes, total_bytes):
    if done_bytes <= 0 or total_bytes <= 0:
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

def _truncate_sample(text, max_len=120):
    text = text.strip()
    return text if len(text) <= max_len else text[: max_len - 3] + "..."

def _literal_lex(value):
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

def _extract_object_token(line):
    # Extract object (literal/IRI/blank node) token from N-Quads/N-Triples
    m = re.match(r'^\s*\S+\s+<[^>]+>\s+(".*?"(?:\^\^<[^>]+>|@[a-zA-Z-]+)?|<[^>]+>|_:[^\s]+)', line)
    if not m:
        return None
    return m.group(1)

def _extract_spo_tokens(line):
    # Extract subject, predicate, object tokens from N-Quads/N-Triples
    m = re.match(r'^\s*(\S+)\s+(<[^>]+>)\s+(".*?"(?:\^\^<[^>]+>|@[a-zA-Z-]+)?|<[^>]+>|_:[^\s]+)', line)
    if not m:
        return None, None, None
    return m.group(1), m.group(2), m.group(3)


def default_type_filter_iris_for_class(class_name):
    class_name = str(class_name or "").strip()
    if not class_name:
        return []
    return [
        f"<http://schema.org/{class_name}>",
        f"<https://schema.org/{class_name}>",
    ]


def collect_allowed_subjects_by_type(files, type_filter_iris=None, progress_every=100):
    if not type_filter_iris:
        return None
    files = [Path(p) for p in files]
    type_pred = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
    type_set = set(type_filter_iris)
    allowed_subjects = set()
    total_bytes = sum(Path(p).stat().st_size for p in files)
    done_bytes = 0
    start_ts = time.time()

    print_color("\n🔎 Filtrage des sujets par rdf:type...", Colors.BLUE)
    for file_path in files:
        print(f"\n  📄 Type scan: {file_path.name}")
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                done_bytes += len(line)
                s, p, o = _extract_spo_tokens(line)
                if not s:
                    continue
                if p == type_pred and o in type_set:
                    allowed_subjects.add(s)
                if progress_every and done_bytes % (progress_every * 50) == 0:
                    prog = _progress_line(start_ts, done_bytes, total_bytes)
                    print(f"\r  Sujets retenus: {len(allowed_subjects):,} | {prog}", end="", flush=True)

    print(f"\n  ✅ Sujets retenus: {len(allowed_subjects):,}")
    return allowed_subjects

def _update_reservoir(samples_map, counts_map, key, sample, k=5):
    count = counts_map.get(key, 0) + 1
    counts_map[key] = count
    bucket = samples_map.setdefault(key, [])
    if len(bucket) < k:
        bucket.append(sample)
    else:
        # Reservoir sampling
        j = random.randint(1, count)
        if j <= k:
            bucket[j - 1] = sample

def print_top_props(predicates_found, top_n=10, title=None, output_file=None, samples_map=None, min_count=1, write_samples=False, fallback_map=None):
    if not predicates_found:
        return
    lines = []
    if title:
        lines.append(title)
    for pred, count in sorted(predicates_found.items(), key=lambda x: -x[1])[:top_n]:
        if count < min_count:
            continue
        lines.append(f"   {count:>8} × {pred}")
    for line in lines:
        print(line)
    if output_file:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
            if write_samples and samples_map:
                for pred, count in sorted(predicates_found.items(), key=lambda x: -x[1])[:top_n]:
                    if count < min_count:
                        continue
                    samples = list(samples_map.get(pred, []))
                    if fallback_map is not None and len(samples) < 5:
                        needed = 5 - len(samples)
                        samples.extend(fallback_map.get(pred, [])[:needed])
                    samples = ", ".join(_truncate_sample(s) for s in samples)
                    f.write(f"   {count:>8} × {pred}; {samples}\n")

def _count_predicates_batch(lines):
    predicates_found = defaultdict(int)
    line_count = 0
    for line in lines:
        line_count += 1
        predicates = re.findall(r'<([^>]+)>', line)
        if len(predicates) >= 1:
            predicate = predicates[0]
            predicates_found[predicate] += 1
    return predicates_found, line_count

def scan_top_props_from_files(files, top_n=1000, parallel=True, workers=None, batch_size=500000, lock_path=None, progress_every=100, output_file=None, type_filter_iris=None):
    print_color(f"\n📊 Scan top-props (sans filtrage)...", Colors.BLUE)
    predicates_found = defaultdict(int)
    samples_map = {}
    fallback_map = {}
    sample_counts = {}
    fallback_counts = {}
    iri_labels = {}
    iri_literals = {}
    iri_literals_is_id = {}
    total_lines = 0
    allowed_subjects = None
    files = [Path(p) for p in files]
    total_bytes = sum(p.stat().st_size for p in files)
    done_bytes = 0
    start_ts = time.time()

    if type_filter_iris:
        type_pred = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
        type_set = set(type_filter_iris)
        allowed_subjects = set()
        print_color(f"\n🔎 Filtrage des sujets par rdf:type...", Colors.BLUE)
        bytes_read = 0
        for file_path in files:
            print(f"\n  📄 Type scan: {file_path.name}")
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    bytes_read += len(line)
                    s, p, o = _extract_spo_tokens(line)
                    if not s:
                        continue
                    if p == type_pred and o in type_set:
                        allowed_subjects.add(s)
                    if progress_every and bytes_read % (progress_every * 50) == 0:
                        done_bytes = bytes_read
                        prog = _progress_line(start_ts, done_bytes, total_bytes)
                        print(f"\r  Sujets retenus: {len(allowed_subjects):,} | {prog}", end='', flush=True)
        print(f"\n  ✅ Sujets retenus: {len(allowed_subjects):,}")

    if parallel:
        buffer = []
        window_batches = []
        n_workers = workers or 1
        lines_since_workers_update = 0
        for file_path in files:
            file_base = done_bytes
            bytes_read = 0
            print(f"\n  📄 Scan: {file_path.name}")
            if progress_every:
                prog = _progress_line(start_ts, done_bytes, total_bytes)
                print(f"  ⏳ Progress: Lignes lues: {total_lines:,} | {prog}", flush=True)
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    bytes_read += len(line)
                    total_lines += 1
                    if allowed_subjects is not None:
                        subj, _, _ = _extract_spo_tokens(line)
                        if not subj or subj not in allowed_subjects:
                            continue
                    lines_since_workers_update += 1
                    buffer.append(line)
                    # Échantillonnage des exemples (main thread)
                    predicates = re.findall(r'<([^>]+)>', line)
                    if len(predicates) >= 1:
                        predicate = predicates[0]
                        obj_tok = _extract_object_token(line)
                        if obj_tok is not None:
                            # Cache labels/descriptions for IRIs to improve samples
                            if predicate in (
                                "http://www.w3.org/2000/01/rdf-schema#label",
                                "http://www.w3.org/2004/02/skos/core#prefLabel",
                                "http://schema.org/name",
                            ):
                                subj = line.split(None, 1)[0]
                                lex = _literal_lex(obj_tok)
                                if subj and lex:
                                    iri_labels[subj] = lex
                            if obj_tok.startswith('"'):
                                lex = _literal_lex(obj_tok)
                                if lex:
                                    # cache literal values for IRI subjects (prefer identifier-like predicates)
                                    subj = line.split(None, 1)[0]
                                    if subj and subj.startswith("<http"):
                                        pred_l = predicate.lower()
                                        is_id = any(tag in pred_l for tag in ("identifier", "isrc", "isbn", "issn", "imdb", "viaf", "gnd", "id"))
                                        if predicate != "http://schema.org/description":
                                            prev_is_id = iri_literals_is_id.get(subj, False)
                                            if is_id or not subj in iri_literals or not prev_is_id:
                                                iri_literals[subj] = lex
                                                iri_literals_is_id[subj] = is_id
                                    _update_reservoir(samples_map, sample_counts, predicate, lex)
                            else:
                                # IRI or blank node: try to resolve to label
                                if not obj_tok.startswith("_:"):
                                    resolved = None
                                    if obj_tok in iri_literals and iri_literals_is_id.get(obj_tok, False):
                                        resolved = iri_literals.get(obj_tok)
                                    elif obj_tok in iri_labels:
                                        resolved = iri_labels.get(obj_tok)
                                    elif obj_tok in iri_literals:
                                        resolved = iri_literals.get(obj_tok)
                                    if resolved:
                                        _update_reservoir(samples_map, sample_counts, predicate, resolved)
                                    else:
                                        tail = obj_tok.strip("<>").rstrip("/").split("/")[-1]
                                        _update_reservoir(fallback_map, fallback_counts, predicate, tail)
                    if len(buffer) >= batch_size:
                        window_batches.append(buffer)
                        buffer = []

                    if lines_since_workers_update >= 10000:
                        if lock_path:
                            n_workers, _runs, _cpu = get_shared_workers(
                                lock_path, share=ALIGN_CPU_SHARE, override=workers
                            )
                        else:
                            n_workers = workers or 1
                        lines_since_workers_update = 0

                    window_size = max(1, n_workers * 6)
                    if progress_every and total_lines % progress_every == 0:
                        done_bytes = file_base + bytes_read
                        prog = _progress_line(start_ts, done_bytes, total_bytes)
                        print(f"\r  Lignes lues: {total_lines:,} | {prog}", end='', flush=True)

                    if len(window_batches) >= window_size:
                        with ProcessPoolExecutor(max_workers=n_workers) as ex:
                            futures = [ex.submit(_count_predicates_batch, b) for b in window_batches]
                            for fut in as_completed(futures):
                                preds, lines = fut.result()
                                for pred, cnt in preds.items():
                                    predicates_found[pred] += cnt
                        window_batches = []
            print_top_props(
                predicates_found,
                top_n=top_n,
                title=f"\n  📋 Top {top_n} prédicats (après {file_path.name}):",
                output_file=None,
                samples_map=None,
                min_count=1000,
            )
            done_bytes = file_base + file_path.stat().st_size

        if buffer:
            window_batches.append(buffer)
        if window_batches:
            if lock_path:
                n_workers, _runs, _cpu = get_shared_workers(
                    lock_path, share=ALIGN_CPU_SHARE, override=workers
                )
            else:
                n_workers = workers or 1
            with ProcessPoolExecutor(max_workers=n_workers) as ex:
                futures = [ex.submit(_count_predicates_batch, b) for b in window_batches]
                for fut in as_completed(futures):
                    preds, lines = fut.result()
                    for pred, cnt in preds.items():
                        predicates_found[pred] += cnt
        if progress_every:
            done_bytes = total_bytes
            prog = _progress_line(start_ts, done_bytes, total_bytes)
            print(f"\r  Lignes lues: {total_lines:,} | {prog}", end='', flush=True)
    else:
        for file_path in files:
            file_base = done_bytes
            bytes_read = 0
            print(f"\n  📄 Scan: {file_path.name}")
            if progress_every:
                prog = _progress_line(start_ts, done_bytes, total_bytes)
                print(f"  ⏳ Progress: Lignes lues: {total_lines:,} | {prog}", flush=True)
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    bytes_read += len(line)
                    total_lines += 1
                    if allowed_subjects is not None:
                        subj, _, _ = _extract_spo_tokens(line)
                        if not subj or subj not in allowed_subjects:
                            continue
                    predicates = re.findall(r'<([^>]+)>', line)
                    if len(predicates) >= 1:
                        predicate = predicates[0]
                        predicates_found[predicate] += 1
                        obj_tok = _extract_object_token(line)
                        if obj_tok is not None:
                            if predicate in (
                                "http://www.w3.org/2000/01/rdf-schema#label",
                                "http://www.w3.org/2004/02/skos/core#prefLabel",
                                "http://schema.org/name",
                            ):
                                subj = line.split(None, 1)[0]
                                lex = _literal_lex(obj_tok)
                                if subj and lex:
                                    iri_labels[subj] = lex
                            if obj_tok.startswith('"'):
                                lex = _literal_lex(obj_tok)
                                if lex:
                                    subj = line.split(None, 1)[0]
                                    if subj and subj.startswith("<http"):
                                        pred_l = predicate.lower()
                                        is_id = any(tag in pred_l for tag in ("identifier", "isrc", "isbn", "issn", "imdb", "viaf", "gnd", "id"))
                                        if predicate != "http://schema.org/description":
                                            prev_is_id = iri_literals_is_id.get(subj, False)
                                            if is_id or not subj in iri_literals or not prev_is_id:
                                                iri_literals[subj] = lex
                                                iri_literals_is_id[subj] = is_id
                                    _update_reservoir(samples_map, sample_counts, predicate, lex)
                            else:
                                if not obj_tok.startswith("_:"):
                                    resolved = None
                                    if obj_tok in iri_literals and iri_literals_is_id.get(obj_tok, False):
                                        resolved = iri_literals.get(obj_tok)
                                    elif obj_tok in iri_labels:
                                        resolved = iri_labels.get(obj_tok)
                                    elif obj_tok in iri_literals:
                                        resolved = iri_literals.get(obj_tok)
                                    if resolved:
                                        _update_reservoir(samples_map, sample_counts, predicate, resolved)
                                    else:
                                        tail = obj_tok.strip("<>").rstrip("/").split("/")[-1]
                                        _update_reservoir(fallback_map, fallback_counts, predicate, tail)
                    if progress_every and total_lines % progress_every == 0:
                        done_bytes = file_base + bytes_read
                        prog = _progress_line(start_ts, done_bytes, total_bytes)
                        print(f"\r  Lignes lues: {total_lines:,} | {prog}", end='', flush=True)
            print_top_props(
                predicates_found,
                top_n=top_n,
                title=f"\n  📋 Top {top_n} prédicats (après {file_path.name}):",
                output_file=None,
                samples_map=None,
                min_count=1000,
            )
            done_bytes = file_base + file_path.stat().st_size
        if progress_every:
            done_bytes = total_bytes
            prog = _progress_line(start_ts, done_bytes, total_bytes)
            print(f"\r  Lignes lues: {total_lines:,} | {prog}", end='', flush=True)

    # Final write to file (once)
    if output_file:
        print_top_props(
            predicates_found,
            top_n=top_n,
            title=None,
            output_file=output_file,
            samples_map=samples_map,
            min_count=1000,
            write_samples=True,
            fallback_map=fallback_map,
        )
    return predicates_found

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
    share = _normalize_worker_share(ALIGN_CPU_SHARE if share is None else share)
    cpu = _CPU_COUNT
    lock_path = Path(lock_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    
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
        
        # Ajouter le pid courant s'il n'est pas déjà là
        if os.getpid() not in active_pids:
            active_pids.append(os.getpid())
        
        # Réécrire la liste nettoyée
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

def get_shared_workers(lock_path, share=None, override=None):
    if override:
        return min(max(1, int(override)), MAX_PARALLEL_WORKERS), None, None
    workers, runs, cpu = compute_shared_workers(lock_path, share=share)
    return max(1, min(workers, MAX_PARALLEL_WORKERS)), runs, cpu

def normalize_for_matching(text):
    """
    Normalisation pour matching:
    - Lowercase
    - Suppression accents/diacritiques
    - Suppression uniquement des tokens configurés via --ignore-chars
    - Si "special-chars" est demandé: garde seulement [a-z0-9]
    """
    if not text:
        return ""
    if not _NORMALIZATION_ENABLED:
        return text
    special_chars = "__SPECIAL_CHARS__" in _EXTRA_STRIP_CHARS
    if _EXTRA_STRIP_CHARS:
        # Remove only configured tokens (single chars and optional multi-char tokens).
        tokens = [tok for tok in _EXTRA_STRIP_CHARS if tok and tok != "__SPECIAL_CHARS__"]
        for tok in sorted(tokens, key=len, reverse=True):
            text = text.replace(tok, "")
    # 1) Lowercase
    text = text.lower()
    # 2) Remove accents/diacritics (NFKD)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    # Drop control/format characters that may break downstream parsing.
    text = "".join(ch for ch in text if not unicodedata.category(ch).startswith("C"))
    # 3) Optional aggressive mode
    if special_chars:
        return re.sub(r'[^a-z0-9]', '', text)
    return text

def _looks_like_phone_mode(value):
    for p in split_predicate_patterns(value):
        low = p.lower()
        if (
            "telephone" in low
            or "phone" in low
            or "phonenumber" in low
            or "p1329" in low
        ):
            return True
    return False

def normalize_for_phone_matching(text):
    """
    Phone normalization:
    - Apply configured token stripping and base normalization
    - Keep only '+' and digits
    """
    base = normalize_for_matching(text)
    if not base:
        return ""
    return "".join(ch for ch in base if ch == "+" or ch.isdigit())

def normalize_value_for_matching(text, phone_mode=False):
    if phone_mode:
        return normalize_for_phone_matching(text)
    return normalize_for_matching(text)

def prepare_predicate_pattern(pattern):
    """
    Prépare un pattern de prédicat:
    - Matching de noms de propriétés/prédicats: toujours case-insensitive via lowercase.
    - La normalisation configurable des valeurs (ignore chars, etc.) ne doit pas impacter
      le matching des prédicats.
    Retourne (pattern_normalized, use_raw) pour compatibilité; use_raw reste True.
    """
    if pattern is None:
        return "", False
    p = str(pattern).strip()
    if not p:
        return "", False
    return p.lower(), True


def split_predicate_patterns(pattern):
    """
    Split a user predicate pattern string into multiple OR-patterns.

    Supported separators:
    - comma (,)
    - semicolon (;)
    - newlines

    Example:
      "sameAs, url" -> ["sameAs", "url"]
    """
    if pattern is None:
        return []
    text = str(pattern).strip()
    if not text:
        return []
    parts = re.split(r"[\n,;]+", text)
    cleaned = []
    seen = set()
    for raw in parts:
        token = str(raw or "").strip()
        if not token:
            continue
        key = token.lower()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(token)
    return cleaned


def prepare_predicate_patterns(pattern):
    """
    Prepare multiple predicate patterns for OR matching.

    Returns a list of tuples: [(pattern_normalized, use_raw, original_token), ...]
    """
    prepared = []
    seen = set()
    for token in split_predicate_patterns(pattern):
        norm, use_raw = prepare_predicate_pattern(token)
        if not norm:
            continue
        key = (norm, bool(use_raw))
        if key in seen:
            continue
        seen.add(key)
        prepared.append((norm, bool(use_raw), token))
    return prepared


def predicate_matches_prepared_patterns(predicate, prepared_patterns):
    """
    True if predicate matches any prepared pattern (OR semantics).
    """
    if not prepared_patterns:
        return False
    predicate_raw = str(predicate or "").lower()
    for pattern_normalized, use_raw, _original in prepared_patterns:
        # Predicate/property-name matching is always case-insensitive only.
        haystack = predicate_raw
        if pattern_normalized in haystack:
            return True
    return False


def _normalize_search_in_mode(search_in):
    mode = str(search_in or "predicate").strip().lower()
    if mode in {"value", "object"}:
        return "value"
    return "predicate"


def value_matches_prepared_patterns(value, prepared_patterns):
    """
    True if an object/literal value matches any prepared pattern (OR semantics).
    Matching is case-insensitive substring on raw value text.
    """
    if not prepared_patterns:
        return False
    value_raw = str(value or "").lower()
    for pattern_normalized, _use_raw, _original in prepared_patterns:
        if pattern_normalized in value_raw:
            return True
    return False

def normalize_predicate_for_match(predicate, use_raw):
    return str(predicate or "").lower()

def normalize_country_code(isrc_normalized):
    """
    Normalise les codes pays non-standards dans les ISRC
    - GX → GB (code non-standard utilisé par certains)
    - UK → GB (UK n'est pas le code ISO, c'est GB)
    - GE → (Géorgie, garde tel quel pour l'instant)
    
    Note: Cette fonction prend un ISRC déjà normalisé (lowercase, alphanumeriques)
    """
    if not isrc_normalized or len(isrc_normalized) < 2:
        return isrc_normalized
    
    # Extraire les 2 premiers caractères (code pays)
    country_code = isrc_normalized[:2]
    rest = isrc_normalized[2:]
    
    # Mappings des codes non-standards
    country_mappings = {
        'gx': 'gb',  # GX → GB
        'uk': 'gb',  # UK → GB
        # Ajoute d'autres mappings si nécessaire
    }
    
    # Appliquer la normalisation si le code existe dans le mapping
    if country_code in country_mappings:
        return country_mappings[country_code] + rest
    
    return isrc_normalized

def normalize_wkd_class(wkd_class):
    """
    Normalise la classe Wikidata:
    - Qxxxx -> wd:Qxxxx
    - wdt:Qxxxx -> wd:Qxxxx (wdt n'est pas valide pour les items)
    - wd:Qxxxx ou IRI complet -> inchangé
    """
    if not wkd_class:
        return None
    wkd_class = wkd_class.strip()
    if wkd_class.startswith("http://") or wkd_class.startswith("https://"):
        return f"<{wkd_class}>"
    if wkd_class.startswith("wd:"):
        return wkd_class
    if wkd_class.startswith("wdt:Q"):
        return "wd:" + wkd_class.split("wdt:", 1)[1]
    if re.match(r'^[Qq]\d+$', wkd_class):
        return "wd:" + wkd_class.upper()
    return wkd_class

def normalize_wkd_prop_class(prop_class):
    """
    Normalise la classe Wikidata pour les propriétés:
    - Qxxxx -> wd:Qxxxx
    - wdt:Qxxxx -> wd:Qxxxx
    - wd:Qxxxx ou IRI complet -> inchangé
    """
    if not prop_class:
        return None
    prop_class = prop_class.strip()
    if prop_class.startswith("http://") or prop_class.startswith("https://"):
        return f"<{prop_class}>"
    if prop_class.startswith("wd:"):
        return prop_class
    if prop_class.startswith("wdt:Q"):
        return "wd:" + prop_class.split("wdt:", 1)[1]
    if re.match(r'^[Qq]\d+$', prop_class):
        return "wd:" + prop_class.upper()
    return prop_class

def normalize_wdc_type(wdc_type):
    if not wdc_type:
        return None
    wdc_type = wdc_type.strip()
    if wdc_type.startswith("<") and wdc_type.endswith(">"):
        return wdc_type
    if wdc_type.startswith("http://") or wdc_type.startswith("https://"):
        return f"<{wdc_type}>"
    if wdc_type.startswith("schema:"):
        return f"<http://schema.org/{wdc_type.split(':',1)[1]}>"
    return f"<http://schema.org/{wdc_type}>"

def normalize_wikidata_property(wikidata_property):
    """
    Normalise la propriété Wikidata pour SPARQL:
    - Pxx -> wdt:Pxx
    - IRI complet -> <IRI>
    - Prefix:suffix -> inchangé (suppose préfixe défini)
    """
    if not wikidata_property:
        return None
    wikidata_property = wikidata_property.strip()
    if wikidata_property.startswith("<") and wikidata_property.endswith(">"):
        return wikidata_property
    if wikidata_property.startswith("http://") or wikidata_property.startswith("https://"):
        return f"<{wikidata_property}>"
    if re.match(r'^[Pp]\d+$', wikidata_property):
        return "wdt:" + wikidata_property.upper()
    # Bare term -> assume wdt: prefix
    if re.match(r'^[A-Za-z_][A-Za-z0-9_-]*$', wikidata_property):
        return "wdt:" + wikidata_property
    return wikidata_property


def normalize_target_endpoint_key(target_endpoint):
    key = str(target_endpoint or "").strip().lower()
    if key in TARGET_ENDPOINTS:
        return key
    return "wikidata"


def resolve_target_endpoint_url(target_endpoint, target_endpoint_url=None):
    key = normalize_target_endpoint_key(target_endpoint)
    custom = str(target_endpoint_url or "").strip()
    if key == "custom":
        return custom
    default_url = str((TARGET_ENDPOINTS.get(key) or {}).get("sparql_url") or "").strip()
    return custom or default_url


def normalize_target_class(target_class, target_endpoint="wikidata"):
    key = normalize_target_endpoint_key(target_endpoint)
    if key == "wikidata":
        return normalize_wkd_class(target_class)
    raw = str(target_class or "").strip()
    if not raw:
        return None
    if raw.startswith("<") and raw.endswith(">"):
        return raw
    if raw.startswith("http://") or raw.startswith("https://"):
        return f"<{raw}>"
    return raw


def normalize_target_property(target_property, target_endpoint="wikidata"):
    key = normalize_target_endpoint_key(target_endpoint)
    if key == "wikidata":
        return normalize_wikidata_property(target_property)
    raw = str(target_property or "").strip()
    if not raw:
        return None
    if raw.startswith("<") and raw.endswith(">"):
        return raw
    if raw.startswith("http://") or raw.startswith("https://"):
        return f"<{raw}>"
    low = raw.lower()
    alias = (NON_WIKIDATA_PROPERTY_ALIASES.get(key) or {}).get(low)
    if alias:
        return alias
    return raw


def _target_phone_fallback_properties(target_endpoint="wikidata"):
    key = normalize_target_endpoint_key(target_endpoint)
    if key == "dbpedia":
        return [
            "dbp:telephone",
            "dbp:phone",
            "schema:telephone",
            "schema:phone",
            "foaf:phone",
        ]
    if key == "yago":
        return [
            "schema:telephone",
            "<https://schema.org/telephone>",
            "schema:phone",
            "<https://schema.org/phone>",
            "foaf:phone",
        ]
    return []



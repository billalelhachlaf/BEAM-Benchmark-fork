#!/usr/bin/env python3
import argparse
import os
import re
import json
import sys
import time
from typing import Iterable, List

import requests


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
    if lowercase and value.startswith("http://www.wikidata.org/"):
        return value.lower()
    return value


def transform_triple(s, p, o, lowercase):
    if lowercase:
        s = normalize_wd_uri(s, lowercase)
        p = normalize_wd_uri(p, lowercase)
        if not o.startswith('"'):
            o = normalize_wd_uri(o, lowercase)
    return s, p, o


def write_links(path, wdc_entities, wd_entities, dedupe):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    seen = set()
    with open(path, "w", encoding="utf-8") as out:
        for wdc, wd in zip(wdc_entities, wd_entities):
            if not wdc or not wd:
                continue
            if dedupe:
                key = (wdc, wd)
                if key in seen:
                    continue
                seen.add(key)
            out.write(f"{wdc}\t{wd}\n")


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


def split_triples(
    input_path,
    out_attr_path,
    out_rel_path,
    seed_subjects,
    max_depth,
    lowercase_wd=False,
    mask_values=None,
    progress_every=0,
):
    os.makedirs(os.path.dirname(out_attr_path), exist_ok=True)
    os.makedirs(os.path.dirname(out_rel_path), exist_ok=True)

    keep_subjects = set(s for s in seed_subjects if s)
    processed_subjects = set()

    with open(out_attr_path, "w", encoding="utf-8") as attr_out, \
         open(out_rel_path, "w", encoding="utf-8") as rel_out:
        for depth in range(max_depth + 1):
            targets = keep_subjects - processed_subjects
            if not targets:
                break
            new_subjects = set()
            with open(input_path, "r", encoding="utf-8") as f:
                line_count = 0
                kept_attr = 0
                kept_rel = 0
                for line in f:
                    line_count += 1
                    if progress_every and line_count % progress_every == 0:
                        print(
                            f"[WDC] depth={depth} lines={line_count} "
                            f"attr={kept_attr} rel={kept_rel}",
                            file=sys.stderr,
                        )
                    parsed = parse_nq_or_nt(line)
                    if not parsed:
                        continue
                    s, p, o = parsed
                    if s not in targets:
                        continue
                    s_out, p_out, o_out = transform_triple(s, p, o, lowercase_wd)
                    if o.startswith('"'):
                        if mask_values:
                            lex = literal_lex(o_out)
                            if lex in mask_values:
                                continue
                        attr_out.write(f"{s_out}\t{p_out}\t{o_out}\n")
                        kept_attr += 1
                    else:
                        rel_out.write(f"{s_out}\t{p_out}\t{o_out}\n")
                        kept_rel += 1
                        if o.startswith("_:"):
                            new_subjects.add(o)
            processed_subjects.update(targets)
            keep_subjects.update(new_subjects)
            print(
                f"[WDC] depth={depth} done lines={line_count} "
                f"attr={kept_attr} rel={kept_rel} new_bnodes={len(new_subjects)}",
                file=sys.stderr,
            )


def batch_iter(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def sparql_construct(
    endpoint,
    subjects,
    language,
    batch_size,
    sleep_s,
    timeout,
    retries,
    backoff,
    start_batch,
):
    headers = {
        "Accept": "application/n-triples",
        "User-Agent": "beam-builder/1.0",
    }
    for batch_idx, batch in enumerate(batch_iter(subjects, batch_size), start=1):
        if batch_idx < start_batch:
            continue
        values = " ".join(f"<{uri}>" for uri in batch)
        query = (
            "CONSTRUCT { ?s ?p ?o . } WHERE { "
            f"VALUES ?s {{ {values} }} "
            "?s ?p ?o . "
            "FILTER(!isLiteral(?o) || lang(?o) = \"\" "
            f"|| langMatches(lang(?o), \"{language}\")) "
            "}"
        )
        print(f"[WD] batch {batch_idx} size={len(batch)}", file=sys.stderr)
        attempt = 0
        while True:
            try:
                resp = requests.post(
                    endpoint,
                    data={"query": query},
                    headers=headers,
                    timeout=timeout,
                )
                resp.raise_for_status()
                for line in resp.text.splitlines():
                    parsed = parse_nq_or_nt(line)
                    if parsed:
                        yield batch_idx, parsed
                yield batch_idx, None
                break
            except requests.RequestException as exc:
                attempt += 1
                if attempt > retries:
                    raise
                wait_s = backoff ** attempt
                print(f"[WD] retry {attempt}/{retries} in {wait_s}s: {exc}", file=sys.stderr)
                time.sleep(wait_s)
        if sleep_s > 0:
            time.sleep(sleep_s)


def write_wikidata_from_sparql(
    endpoint,
    subjects,
    out_attr_path,
    out_rel_path,
    lowercase_wd,
    language,
    batch_size,
    sleep_s,
    timeout,
    retries,
    backoff,
    mask_values=None,
    state_path=None,
    resume=False,
):
    os.makedirs(os.path.dirname(out_attr_path), exist_ok=True)
    os.makedirs(os.path.dirname(out_rel_path), exist_ok=True)
    start_batch = 1
    state = {
        "done_batch": 0,
        "batch_size": batch_size,
        "lang": language,
        "endpoint": endpoint,
        "total_subjects": len(subjects),
    }
    if resume and state_path and os.path.exists(state_path):
        with open(state_path, "r", encoding="utf-8") as f:
            prev = json.load(f)
        start_batch = int(prev.get("done_batch", 0)) + 1
        print(f"[WD] resuming from batch {start_batch}", file=sys.stderr)

    attr_mode = "a" if resume else "w"
    rel_mode = "a" if resume else "w"
    with open(out_attr_path, attr_mode, encoding="utf-8") as attr_out, \
         open(out_rel_path, rel_mode, encoding="utf-8") as rel_out:
        kept_attr = 0
        kept_rel = 0
        for batch_idx, item in sparql_construct(
            endpoint,
            subjects,
            language,
            batch_size,
            sleep_s,
            timeout,
            retries,
            backoff,
            start_batch,
        ):
            if item is None:
                if state_path:
                    state["done_batch"] = batch_idx
                    with open(state_path, "w", encoding="utf-8") as f:
                        json.dump(state, f, indent=2)
                continue
            s, p, o = item
            s_out, p_out, o_out = transform_triple(s, p, o, lowercase_wd)
            if o.startswith('"'):
                if mask_values:
                    lex = literal_lex(o_out)
                    if lex in mask_values:
                        continue
                attr_out.write(f"{s_out}\t{p_out}\t{o_out}\n")
                kept_attr += 1
            else:
                rel_out.write(f"{s_out}\t{p_out}\t{o_out}\n")
                kept_rel += 1
        print(f"[WD] done attr={kept_attr} rel={kept_rel}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description="Generate BEAM-style files from N-Quads/N-Triples and a link TSV."
    )
    parser.add_argument("--wdc-nq", required=True, help="Path to WDC N-Quads/N-Triples file.")
    parser.add_argument("--wd-nq", help="Path to Wikidata N-Quads/N-Triples file.")
    parser.add_argument("--links-tsv", required=True, help="Path to link TSV file.")
    parser.add_argument("--out-dir", required=True, help="Output directory for attr/rel/link files.")
    parser.add_argument("--sep", default="\t", help="Column separator for links file (default: tab).")
    parser.add_argument("--wdc-col", type=int, default=0, help="0-based column index for wdc_iri.")
    parser.add_argument("--wd-col", type=int, default=1, help="0-based column index for wikidata_uri.")
    parser.add_argument("--wdc-value-col", type=int, help="0-based column index for wdc_value.")
    parser.add_argument("--wd-value-col", type=int, help="0-based column index for wiki_value.")
    parser.add_argument("--max-depth", type=int, default=1, help="Depth for following bnodes (default: 1).")
    parser.add_argument("--dedupe-links", action="store_true", help="Remove duplicate ent_links pairs.")
    parser.add_argument("--progress-every", type=int, default=0, help="Print progress every N lines (WDC scan).")
    parser.add_argument("--sparql-url", default="https://query.wikidata.org/sparql", help="Wikidata SPARQL endpoint.")
    parser.add_argument("--lang", default="en", help="Language filter for literals with language tag.")
    parser.add_argument("--batch-size", type=int, default=50, help="Wikidata SPARQL batch size.")
    parser.add_argument("--sleep", type=float, default=1.0, help="Sleep between SPARQL batches in seconds.")
    parser.add_argument("--timeout", type=int, default=60, help="SPARQL request timeout in seconds.")
    parser.add_argument("--retries", type=int, default=3, help="SPARQL retries per batch.")
    parser.add_argument("--backoff", type=float, default=2.0, help="Exponential backoff base (seconds).")
    parser.add_argument("--no-lowercase-wd", action="store_true", help="Do not lowercase Wikidata URIs.")
    parser.add_argument("--resume", action="store_true", help="Resume Wikidata SPARQL extraction.")
    parser.add_argument("--state-file", help="Path to resume state file (default: OUT_DIR/.wd_state.json).")

    args = parser.parse_args()

    wdc_entities, wd_entities_raw, wdc_values, wd_values = read_links(
        args.links_tsv,
        args.sep,
        args.wdc_col,
        args.wd_col,
        args.wdc_value_col,
        args.wd_value_col,
    )
    print(
        f"[Links] wdc={len(wdc_entities)} wd={len(wd_entities_raw)} "
        f"wdc_values={len(wdc_values)} wd_values={len(wd_values)}",
        file=sys.stderr,
    )
    lowercase_wd = not args.no_lowercase_wd
    wd_entities_out = [normalize_wd_uri(uri, lowercase_wd) for uri in wd_entities_raw]
    wdc_mask_values = set(v for v in wdc_values if v)
    wd_mask_values = set(v for v in wd_values if v)

    out_attr_1 = os.path.join(args.out_dir, "attr_triples_1")
    out_rel_1 = os.path.join(args.out_dir, "rel_triples_1")
    out_attr_2 = os.path.join(args.out_dir, "attr_triples_2")
    out_rel_2 = os.path.join(args.out_dir, "rel_triples_2")
    out_links = os.path.join(args.out_dir, "ent_links")

    write_links(out_links, wdc_entities, wd_entities_out, args.dedupe_links)

    split_triples(
        args.wdc_nq,
        out_attr_1,
        out_rel_1,
        seed_subjects=wdc_entities,
        max_depth=args.max_depth,
        mask_values=wdc_mask_values,
        progress_every=args.progress_every,
    )

    if args.wd_nq:
        split_triples(
            args.wd_nq,
            out_attr_2,
            out_rel_2,
            seed_subjects=wd_entities_raw,
            max_depth=args.max_depth,
            lowercase_wd=lowercase_wd,
            mask_values=wd_mask_values,
        )
    else:
        write_wikidata_from_sparql(
            args.sparql_url,
            wd_entities_raw,
            out_attr_2,
            out_rel_2,
            lowercase_wd=lowercase_wd,
            language=args.lang,
            batch_size=args.batch_size,
            sleep_s=args.sleep,
            timeout=args.timeout,
            retries=args.retries,
            backoff=args.backoff,
            mask_values=wd_mask_values,
            state_path=args.state_file or os.path.join(args.out_dir, ".wd_state.json"),
            resume=args.resume,
        )


if __name__ == "__main__":
    main()

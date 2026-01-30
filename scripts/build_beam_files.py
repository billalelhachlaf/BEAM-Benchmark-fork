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


def clean_literal(value):
    if not value.startswith('"'):
        return value
    lex = literal_lex(value)
    if lex is None:
        return value
    return f"\"{lex}\""


def split_triples(
    input_path,
    out_attr_path,
    out_rel_path,
    seed_subjects,
    max_depth,
    lowercase_wd=False,
    mask_values=None,
    exclude_props=None,
    replace_map=None,
    progress_every=0,
):
    os.makedirs(os.path.dirname(out_attr_path), exist_ok=True)
    os.makedirs(os.path.dirname(out_rel_path), exist_ok=True)

    keep_subjects = set(s for s in seed_subjects if s)
    processed_subjects = set()

    with open(out_attr_path, "w", encoding="utf-8") as attr_out, \
         open(out_rel_path, "w", encoding="utf-8") as rel_out:
        depth = 0
        while True:
            if max_depth >= 0 and depth > max_depth:
                break
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
                    if exclude_props and p in exclude_props:
                        continue
                    if replace_map and s in replace_map:
                        s = replace_map[s]
                    if replace_map and (not o.startswith('"')) and o in replace_map:
                        o = replace_map[o]
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
            depth += 1


def batch_iter(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def count_wdc_triples(input_path, subjects, exclude_props=None, mask_values=None):
    counts = {s: 0 for s in subjects}
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            parsed = parse_nq_or_nt(line)
            if not parsed:
                continue
            s, p, o = parsed
            if s not in counts:
                continue
            if exclude_props and p in exclude_props:
                continue
            if mask_values and o.startswith('"'):
                lex = literal_lex(o)
                if lex in mask_values:
                    continue
            counts[s] += 1
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
    prop_id = prop_id.upper()
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


def prop_uri_to_entity(uri):
    if "wikidata.org/prop/" not in uri:
        return None
    tail = uri.rstrip("/").split("/")[-1]
    if not tail.startswith("P"):
        return None
    return f"http://www.wikidata.org/entity/{tail}"


def collect_wikidata_uris(attr_path, rel_path):
    uris = set()
    for path in (attr_path, rel_path):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                parts = line.rstrip("\n").split("\t")
                if len(parts) != 3:
                    continue
                s, p, o = parts
                if s.startswith("http://www.wikidata.org/entity/"):
                    uris.add(s)
                if p.startswith("http://www.wikidata.org/prop/"):
                    ent = prop_uri_to_entity(p)
                    if ent:
                        uris.add(ent)
                if o.startswith("http://www.wikidata.org/entity/"):
                    uris.add(o)
    return uris


def fetch_wd_labels_descriptions(uris, endpoint, language, batch_size, sleep_s, timeout, retries, backoff):
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "beam-builder/1.0",
    }
    results = []
    uris = list(uris)
    for batch_idx, batch in enumerate(batch_iter(uris, batch_size), start=1):
        values = " ".join(f"<{uri}>" for uri in batch)
        query = (
            "SELECT ?s ?label ?desc WHERE { "
            f"VALUES ?s {{ {values} }} "
            "OPTIONAL { ?s rdfs:label ?label FILTER(LANG(?label) = \"" + language + "\") } "
            "OPTIONAL { ?s schema:description ?desc FILTER(LANG(?desc) = \"" + language + "\") } "
            "}"
        )
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
                data = resp.json()
                for row in data.get("results", {}).get("bindings", []):
                    s = row["s"]["value"]
                    if "label" in row:
                        results.append((s, "http://www.w3.org/2000/01/rdf-schema#label", f"\"{row['label']['value']}\""))
                    if "desc" in row:
                        results.append((s, "http://schema.org/description", f"\"{row['desc']['value']}\""))
                break
            except requests.RequestException as exc:
                attempt += 1
                if attempt > retries:
                    raise
                wait_s = backoff ** attempt
                print(f"[WD] label retry {attempt}/{retries} in {wait_s}s: {exc}", file=sys.stderr)
                time.sleep(wait_s)
        if sleep_s > 0:
            time.sleep(sleep_s)
    return results


def append_labels_descriptions(
    attr_path,
    rel_path,
    endpoint,
    language,
    batch_size,
    sleep_s,
    timeout,
    retries,
    backoff,
    lowercase_wd,
):
    uris = collect_wikidata_uris(attr_path, rel_path)
    if not uris:
        return
    triples = fetch_wd_labels_descriptions(
        uris,
        endpoint,
        language,
        batch_size,
        sleep_s,
        timeout,
        retries,
        backoff,
    )
    with open(attr_path, "a", encoding="utf-8") as out:
        for s, p, o in triples:
            s_out, p_out, o_out = transform_triple(s, p, o, lowercase_wd)
            o_out = clean_literal(o_out)
            out.write(f"{s_out}\t{p_out}\t{o_out}\n")


def run_pipeline(
    args,
    wdc_entities,
    wd_entities_raw,
    wdc_values,
    wd_values,
    out_dir,
    wdc_mask_values,
    wd_mask_values,
    wdc_exclude_props,
    wd_exclude_props,
    replace_map,
    lowercase_wd,
    add_wd_labels,
):
    out_attr_1 = os.path.join(out_dir, "attr_triples_1")
    out_rel_1 = os.path.join(out_dir, "rel_triples_1")
    out_attr_2 = os.path.join(out_dir, "attr_triples_2")
    out_rel_2 = os.path.join(out_dir, "rel_triples_2")
    out_links = os.path.join(out_dir, "ent_links")

    wd_entities_out = [normalize_wd_uri(replace_map.get(uri, uri), lowercase_wd) for uri in wd_entities_raw]
    write_links(out_links, wdc_entities, wd_entities_out, args.dedupe_links)

    split_triples(
        args.wdc_nq,
        out_attr_1,
        out_rel_1,
        seed_subjects=wdc_entities,
        max_depth=args.max_depth,
        mask_values=wdc_mask_values,
        exclude_props=wdc_exclude_props,
        progress_every=args.progress_every,
    )

    if args.wd_nq:
        wd_attr_tmp = out_attr_2
        wd_rel_tmp = out_rel_2
        if args.wd_prop_min_count > 0:
            wd_attr_tmp = out_attr_2 + ".tmp"
            wd_rel_tmp = out_rel_2 + ".tmp"
        split_triples(
            args.wd_nq,
            wd_attr_tmp,
            wd_rel_tmp,
            seed_subjects=wd_entities_raw,
            max_depth=args.max_depth,
            lowercase_wd=lowercase_wd,
            mask_values=wd_mask_values,
            exclude_props=wd_exclude_props,
            replace_map=replace_map,
        )
        if args.wd_prop_min_count > 0:
            filter_triples_by_prop_count(
                wd_attr_tmp,
                wd_rel_tmp,
                out_attr_2,
                out_rel_2,
                args.wd_prop_min_count,
                exclude_props=None,
            )
        if add_wd_labels:
            append_labels_descriptions(
                out_attr_2,
                out_rel_2,
                args.sparql_url,
                args.lang,
                args.batch_size,
                args.sleep,
                args.timeout,
                args.retries,
                args.backoff,
                lowercase_wd,
            )
    else:
        wd_attr_tmp = out_attr_2
        wd_rel_tmp = out_rel_2
        if args.wd_prop_min_count > 0:
            wd_attr_tmp = out_attr_2 + ".tmp"
            wd_rel_tmp = out_rel_2 + ".tmp"
        write_wikidata_from_sparql(
            args.sparql_url,
            wd_entities_raw,
            wd_attr_tmp,
            wd_rel_tmp,
            lowercase_wd=lowercase_wd,
            language=args.lang,
            batch_size=args.batch_size,
            sleep_s=args.sleep,
            timeout=args.timeout,
            retries=args.retries,
            backoff=args.backoff,
            mask_values=wd_mask_values,
            exclude_props=wd_exclude_props,
            replace_map=replace_map,
            state_path=args.state_file or os.path.join(out_dir, ".wd_state.json"),
            resume=args.resume,
        )
        if args.wd_prop_min_count > 0:
            filter_triples_by_prop_count(
                wd_attr_tmp,
                wd_rel_tmp,
                out_attr_2,
                out_rel_2,
                args.wd_prop_min_count,
                exclude_props=None,
            )
        if add_wd_labels:
            append_labels_descriptions(
                out_attr_2,
                out_rel_2,
                args.sparql_url,
                args.lang,
                args.batch_size,
                args.sleep,
                args.timeout,
                args.retries,
                args.backoff,
                lowercase_wd,
            )
    with open(in_rel, "r", encoding="utf-8") as fin, \
         open(out_rel, "w", encoding="utf-8") as fout:
        for line in fin:
            parts = line.rstrip("\n").split("\t")
            if len(parts) != 3:
                continue
            p = parts[1]
            if exclude_props and p in exclude_props:
                continue
            if counts.get(p, 0) >= min_count:
                fout.write(line)


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
    exclude_props=None,
    replace_map=None,
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
            if exclude_props and p in exclude_props:
                continue
            if replace_map and s in replace_map:
                s = replace_map[s]
            if replace_map and (not o.startswith('"')) and o in replace_map:
                o = replace_map[o]
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
    parser.add_argument("--max-depth", type=int, default=1, help="Depth for following bnodes (default: 1, -1 means until no new bnodes).")
    parser.add_argument("--dedupe-links", action="store_true", help="Remove duplicate ent_links pairs.")
    parser.add_argument("--progress-every", type=int, default=0, help="Print progress every N lines (WDC scan).")
    parser.add_argument("--keep-link-values", action="store_true", help="Do not mask link values in triples.")
    parser.add_argument("--wdc-min-triples", type=int, default=0, help="Minimum triples per WDC entity.")
    parser.add_argument("--wdc-exclude-prop", action="append", default=[], help="Exclude WDC predicate URI (repeatable).")
    parser.add_argument("--wd-exclude-prop", action="append", default=[], help="Exclude Wikidata predicate URI (repeatable).")
    parser.add_argument("--wd-link-prop-id", action="append", default=[], help="Wikidata property id to drop (e.g., P1243).")
    parser.add_argument("--wdc-link-prop-name", action="append", default=[], help="schema.org property name to drop (e.g., isrcCode).")
    parser.add_argument("--split-link-values", action="store_true", help="Create subfolders with and without link values.")
    parser.add_argument("--no-wd-labels", action="store_true", help="Do not add labels/descriptions for Wikidata entities/properties.")
    parser.add_argument("--wd-prop-min-count", type=int, default=0, help="Min property frequency for Wikidata.")
    parser.add_argument("--merge-wd-by-link-values", action="store_true", help="Merge Wikidata entities sharing wiki_value.")
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
    wdc_mask_values = set(v for v in wdc_values if v)
    wd_mask_values = set(v for v in wd_values if v)
    if args.keep_link_values:
        wdc_mask_values = None
        wd_mask_values = None

    wdc_exclude_props = set(p for p in args.wdc_exclude_prop if p)
    wd_exclude_props = set(p for p in args.wd_exclude_prop if p)
    wd_link_prop_uris = set()
    for prop_id in args.wd_link_prop_id:
        if prop_id:
            wd_link_prop_uris.update(wikidata_prop_uris(prop_id))
    wdc_link_prop_uris = set()
    for prop_name in args.wdc_link_prop_name:
        if prop_name:
            wdc_link_prop_uris.update(schema_org_prop_uris(prop_name))

    if args.wdc_min_triples > 0:
        counts = count_wdc_triples(
            args.wdc_nq,
            set(wdc_entities),
            exclude_props=wdc_exclude_props,
            mask_values=wdc_mask_values,
        )
        allowed_wdc = {s for s, c in counts.items() if c >= args.wdc_min_triples}
        wdc_entities, wd_entities_raw, wdc_values, wd_values = filter_links_by_wdc(
            wdc_entities, wd_entities_raw, wdc_values, wd_values, allowed_wdc
        )
        print(f"[Links] kept wdc after min_triples={len(wdc_entities)}", file=sys.stderr)

    replace_map = {}
    if args.merge_wd_by_link_values and wd_values:
        replace_map = build_wd_merge_map(wd_entities_raw, wd_values)
        if replace_map:
            print(f"[WD] merge map size={len(replace_map)}", file=sys.stderr)

    add_wd_labels = not args.no_wd_labels

    if args.split_link_values:
        out_without = os.path.join(args.out_dir, "without_link_code")
        out_with = os.path.join(args.out_dir, "with_link_code")

        run_pipeline(
            args,
            wdc_entities,
            wd_entities_raw,
            wdc_values,
            wd_values,
            out_without,
            wdc_mask_values,
            wd_mask_values,
            wdc_exclude_props | wdc_link_prop_uris,
            wd_exclude_props | wd_link_prop_uris,
            replace_map,
            lowercase_wd,
            add_wd_labels,
        )
        run_pipeline(
            args,
            wdc_entities,
            wd_entities_raw,
            wdc_values,
            wd_values,
            out_with,
            None,
            None,
            wdc_exclude_props,
            wd_exclude_props,
            replace_map,
            lowercase_wd,
            add_wd_labels,
        )
    else:
        run_pipeline(
            args,
            wdc_entities,
            wd_entities_raw,
            wdc_values,
            wd_values,
            args.out_dir,
            wdc_mask_values,
            wd_mask_values,
            wdc_exclude_props | wdc_link_prop_uris,
            wd_exclude_props | wd_link_prop_uris,
            replace_map,
            lowercase_wd,
            add_wd_labels,
        )


if __name__ == "__main__":
    main()

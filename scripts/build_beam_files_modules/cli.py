

def main():
    start_ts = time.time()
    parser = argparse.ArgumentParser(
        description="Generate BEAM-style files from N-Quads/N-Triples and a link TSV."
    )
    parser.add_argument("class_name", help="Class name to use default paths (data/<class> and Download/<class>).")
    parser.add_argument("--wd-link-prop-id", action="append", default=[], help="Wikidata property id to drop (e.g., P1243).")
    parser.add_argument("--wdc-link-prop-name", action="append", default=[], help="Pattern to drop WDC predicates (e.g., isrc).")
    parser.add_argument("--max-depth", type=int, default=-1, help="Depth for following bnodes (default: -1 until no new bnodes).")
    parser.add_argument("--progress-every", type=int, default=10000000, help="Print progress every N lines (WDC scan).")

    args = parser.parse_args()

    # Defaults from class_name
    class_name = args.class_name
    data_dir = os.path.join("data", class_name)
    download_dir = os.path.join("Download", class_name)

    # Defaults: only part_* sources (no *_full_graph fallback)
    candidates = []
    if os.path.isdir(download_dir):
        for name in sorted(os.listdir(download_dir)):
            if name.startswith("part_") and (
                name.endswith(".nq") or name.endswith(".nt") or "." not in name
            ):
                candidates.append(os.path.join(download_dir, name))

    args.wdc_nq = candidates
    args.links_tsv = os.path.join(download_dir, "wdc_wikidata_links.tsv")
    base_out_dir = os.path.join(data_dir, "beam")
    out_dir = base_out_dir
    suffix = 1
    while os.path.exists(out_dir):
        out_dir = base_out_dir + str(suffix)
        suffix += 1
    args.out_dir = out_dir

    # Fixed defaults (removed flags)
    args.wd_nq = None
    args.sep = "\t"
    args.wdc_col = 0
    args.wd_col = 1
    args.wdc_value_col = None
    args.wd_value_col = None
    args.dedupe_links = False
    args.keep_link_values = False
    args.wdc_min_triples = 0
    args.wdc_exclude_prop = []
    args.wd_exclude_prop = []
    args.no_wd_labels = False
    args.wd_prop_min_count = 0
    args.merge_wd_by_link_values = False
    args.sparql_url = "https://query.wikidata.org/sparql"
    args.lang = "en"
    args.batch_size = 50
    args.sleep = 1.0
    args.timeout = 60
    args.retries = 3
    args.backoff = 2.0
    args.no_lowercase_wd = False
    args.resume = False
    args.state_file = None

    if not args.wdc_nq:
        print(f"[ERR] No WDC files found in {download_dir}", file=sys.stderr)
        sys.exit(1)
    if not os.path.exists(args.links_tsv):
        print(f"[ERR] Missing links TSV: {args.links_tsv}", file=sys.stderr)
        sys.exit(1)

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
        norm = normalize_wd_prop_id(prop_id) if prop_id else None
        if norm:
            wd_link_prop_uris.update(wikidata_prop_uris(norm))
    wdc_link_prop_patterns = {p.lower() for p in args.wdc_link_prop_name if p}

    if args.wdc_min_triples > 0:
        counts = count_wdc_triples(
            args.wdc_nq,
            set(wdc_entities),
            exclude_props=wdc_exclude_props,
            exclude_prop_patterns=wdc_link_prop_patterns,
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

    add_wd_labels = True

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
        wdc_exclude_props,
        wdc_link_prop_patterns,
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
        set(),
        wd_exclude_props,
        replace_map,
        lowercase_wd,
        add_wd_labels,
    )
    elapsed = time.time() - start_ts
    took = _format_eta(elapsed).replace("ETA: ", "")
    print(f"[DONE] total time: {took}", file=sys.stderr)
    try:
        with open(os.path.join(out_dir, "stats.txt"), "a", encoding="utf-8") as f:
            f.write(f"build_beam took {took}\n")
    except Exception:
        pass


if __name__ == "__main__":
    main()

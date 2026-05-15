

def fetch_wd_labels_descriptions(uris, endpoint, language, batch_size, sleep_s, timeout, retries, backoff):
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "beam-builder/1.0",
    }
    results = []
    uris = dedupe_preserve_order(list(uris))
    if not uris:
        return results

    batch_size_eff = max(1, int(batch_size or 1))
    total_batches = max(1, (len(uris) + batch_size_eff - 1) // batch_size_eff)
    progress_started_at = time.time()

    def _emit_labels_progress(done_batches):
        done_i = max(0, min(int(done_batches), total_batches))
        pct = (done_i / total_batches) * 100.0 if total_batches > 0 else 100.0
        if done_i <= 0:
            eta_txt = "ETA: N/A"
        else:
            elapsed = max(0.001, time.time() - progress_started_at)
            remaining = max(0, total_batches - done_i)
            eta_txt = _format_eta((elapsed / done_i) * remaining)
        print(
            f"[WD] labels progress: batches {done_i}/{total_batches} | {pct:5.1f}% | {eta_txt}",
            file=sys.stderr,
        )

    _emit_labels_progress(0)
    session = requests.Session()
    try:
        for batch_idx, batch in enumerate(batch_iter(uris, batch_size_eff), start=1):
            values = " ".join(f"<{uri}>" for uri in batch)
            query = (
                "SELECT ?s "
                "(SAMPLE(?labelPref) AS ?label_pref) "
                "(SAMPLE(?labelAny) AS ?label_any) "
                "(SAMPLE(?descPref) AS ?desc_pref) "
                "(SAMPLE(?descAny) AS ?desc_any) "
                "WHERE { "
                f"VALUES ?s {{ {values} }} "
                "OPTIONAL { ?s rdfs:label ?labelPref FILTER(LANG(?labelPref) = \"" + language + "\" || LANG(?labelPref) = \"\") } "
                "OPTIONAL { ?s rdfs:label ?labelAny } "
                "OPTIONAL { ?s schema:description ?descPref FILTER(LANG(?descPref) = \"" + language + "\" || LANG(?descPref) = \"\") } "
                "OPTIONAL { ?s schema:description ?descAny } "
                "} GROUP BY ?s"
            )
            attempt = 0
            while True:
                try:
                    resp = session.post(
                        endpoint,
                        data={"query": query},
                        headers=headers,
                        timeout=timeout,
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    for row in data.get("results", {}).get("bindings", []):
                        s = row["s"]["value"]
                        label_val = (
                            row.get("label_pref", {}).get("value")
                            or row.get("label_any", {}).get("value")
                        )
                        desc_val = (
                            row.get("desc_pref", {}).get("value")
                            or row.get("desc_any", {}).get("value")
                        )
                        if label_val and not desc_val:
                            desc_val = label_val
                        if desc_val and not label_val:
                            label_val = desc_val
                        if label_val:
                            results.append((s, "http://www.w3.org/2000/01/rdf-schema#label", f"\"{label_val}\""))
                        if desc_val:
                            results.append((s, "http://schema.org/description", f"\"{desc_val}\""))
                    _emit_labels_progress(batch_idx)
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
        _emit_labels_progress(total_batches)
    finally:
        session.close()
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
    uris, prop_uri_map = collect_wikidata_uris(attr_path, rel_path)
    if not uris and not prop_uri_map:
        return
    ent_to_prop = {}
    for prop_uri, ent_uri in prop_uri_map.items():
        ent_to_prop.setdefault(ent_uri, []).append(prop_uri)
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
    label_pred = "http://www.w3.org/2000/01/rdf-schema#label"
    desc_pred = "http://schema.org/description"
    by_subject = {}
    for s, p, o in triples:
        row = by_subject.setdefault(s, {"label": None, "desc": None})
        if p == label_pred and row["label"] is None:
            row["label"] = o
        elif p == desc_pred and row["desc"] is None:
            row["desc"] = o

    for uri in uris:
        row = by_subject.setdefault(uri, {"label": None, "desc": None})
        fallback = f"\"{uri.rstrip('/').split('/')[-1]}\""
        if row["label"] is None:
            row["label"] = fallback
        if row["desc"] is None:
            row["desc"] = row["label"]

    with open(attr_path, "a", encoding="utf-8") as out:
        for s, row in by_subject.items():
            for p, o in ((label_pred, row["label"]), (desc_pred, row["desc"])):
                s_out, p_out, o_out = transform_triple(s, p, o, lowercase_wd)
                o_out = clean_literal(o_out)
                out.write(f"{s_out}\t{p_out}\t{o_out}\n")
                for prop_uri in ent_to_prop.get(s, []):
                    s_prop, p_prop, o_prop = transform_triple(prop_uri, p, o, lowercase_wd)
                    o_prop = clean_literal(o_prop)
                    out.write(f"{s_prop}\t{p_prop}\t{o_prop}\n")


def append_wdc_labels_descriptions(attr_path, rel_path, wdc_input_paths):
    uris, prop_uris = collect_wdc_iris(attr_path, rel_path)
    if not uris and not prop_uris:
        return
    target_iris = uris | prop_uris
    label_preds = {
        "http://www.w3.org/2000/01/rdf-schema#label",
        "http://schema.org/description",
        "http://www.w3.org/2004/02/skos/core#prefLabel",
    }
    input_paths = _iter_input_paths(wdc_input_paths)

    lock_path = os.path.join("Download", ".workers.lock")
    n_workers, _runs, _cpu = compute_shared_workers(lock_path, share=BUILD_CPU_SHARE)
    total_written = 0
    total_bytes = sum(os.path.getsize(p) for p in input_paths)
    done_bytes = 0
    start_ts = time.time()
    prog = _progress_line(start_ts, done_bytes, total_bytes)
    print(f"[WDC] labels progress {done_bytes}/{total_bytes} bytes {prog}", file=sys.stderr)
    with ProcessPoolExecutor(max_workers=n_workers) as ex:
        futures = [
            ex.submit(_labels_worker, (p, target_iris, label_preds))
            for p in input_paths
        ]
        pending = set(futures)
        total_futures = len(futures)
        heartbeat_s = 10.0
        last_heartbeat = time.time()
        with open(attr_path, "a", encoding="utf-8") as out:
            while pending:
                done, pending = wait(pending, timeout=1.0, return_when=FIRST_COMPLETED)
                if not done:
                    now = time.time()
                    if (now - last_heartbeat) >= heartbeat_s:
                        prog = _progress_line(start_ts, done_bytes, total_bytes)
                        finished = total_futures - len(pending)
                        print(
                            f"[WDC] labels progress {done_bytes}/{total_bytes} bytes {prog} | workers {finished}/{total_futures}",
                            file=sys.stderr,
                        )
                        last_heartbeat = now
                    continue
                for fut in done:
                    tmp, written, fsize = fut.result()
                    total_written += written
                    done_bytes += fsize
                    prog = _progress_line(start_ts, done_bytes, total_bytes)
                    print(f"[WDC] labels progress {done_bytes}/{total_bytes} bytes {prog}", file=sys.stderr)
                    if written > 0:
                        with open(tmp, "r", encoding="utf-8") as f:
                            for line in f:
                                out.write(line)
                    os.remove(tmp)
    done_bytes = total_bytes
    prog = _progress_line(start_ts, done_bytes, total_bytes)
    print(f"[WDC] labels progress {done_bytes}/{total_bytes} bytes {prog}", file=sys.stderr)
    if total_written == 0:
        return


def fetch_wd_label_desc_map(uris, endpoint, language, batch_size, sleep_s, timeout, retries, backoff):
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
    labels = {}
    for s, p, o in triples:
        entry = labels.setdefault(s, {"label": "", "desc": ""})
        if p.endswith("#label"):
            entry["label"] = literal_lex(o) or o.strip('"')
        elif p.endswith("description"):
            entry["desc"] = literal_lex(o) or o.strip('"')
    # Ensure map is complete even when Wikidata has sparse metadata.
    for uri in uris:
        entry = labels.setdefault(uri, {"label": "", "desc": ""})
        fallback = uri.rstrip("/").split("/")[-1]
        if not entry["label"]:
            entry["label"] = fallback
        if not entry["desc"]:
            entry["desc"] = entry["label"]
    return labels


def write_prop_stats(
    out_path,
    attr_path,
    rel_path,
    endpoint,
    language,
    batch_size,
    sleep_s,
    timeout,
    retries,
    backoff,
):
    counts = count_props_in_files([attr_path, rel_path])
    prop_entity_map = {}
    for prop in counts.keys():
        prop_norm = prop.strip("<>")
        if prop_norm.startswith("http://www.wikidata.org/prop/"):
            ent = prop_uri_to_entity(prop_norm)
            if ent:
                prop_entity_map[prop] = canonical_wd_entity_uri(ent)

    label_map = {}
    if prop_entity_map:
        label_map = fetch_wd_label_desc_map(
            set(prop_entity_map.values()),
            endpoint,
            language,
            batch_size,
            sleep_s,
            timeout,
            retries,
            backoff,
        )

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as out:
        out.write("predicate\tcount\tlabel\tdescription\n")
        for prop, count in sorted(counts.items(), key=lambda x: (-x[1], x[0])):
            label = ""
            desc = ""
            ent = prop_entity_map.get(prop)
            if ent and ent in label_map:
                label = label_map[ent].get("label", "")
                desc = label_map[ent].get("desc", "")
            out.write(f"{prop}\t{count}\t{label}\t{desc}\n")


def write_prop_stats_simple(out_path, attr_path, rel_path):
    counts = count_props_in_files([attr_path, rel_path])
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as out:
        out.write("predicate\tcount\tlabel\tdescription\n")
        for prop, count in sorted(counts.items(), key=lambda x: (-x[1], x[0])):
            label = prop.rstrip("/").split("/")[-1]
            out.write(f"{prop}\t{count}\t{label}\t\n")


def write_prop_stats_wdc(out_path, attr_path, rel_path, wdc_input_paths):
    counts = count_props_in_files([attr_path, rel_path])
    label_preds = {
        "http://www.w3.org/2000/01/rdf-schema#label",
        "http://schema.org/description",
        "http://www.w3.org/2004/02/skos/core#prefLabel",
    }
    labels = {}
    descs = {}
    targets = {p.strip("<>") for p in counts.keys()}

    lock_path = os.path.join("Download", ".workers.lock")
    n_workers, _runs, _cpu = compute_shared_workers(lock_path, share=BUILD_CPU_SHARE)
    total_bytes = sum(os.path.getsize(p) for p in _iter_input_paths(wdc_input_paths))
    done_bytes = 0
    start_ts = time.time()
    prog = _progress_line(start_ts, done_bytes, total_bytes)
    print(f"[WDC] prop_stats progress {done_bytes}/{total_bytes} bytes {prog}", file=sys.stderr)
    with ProcessPoolExecutor(max_workers=n_workers) as ex:
        futures = [
            ex.submit(_prop_label_worker, (p, targets, label_preds))
            for p in _iter_input_paths(wdc_input_paths)
        ]
        pending = set(futures)
        total_futures = len(futures)
        heartbeat_s = 10.0
        last_heartbeat = time.time()
        while pending:
            done, pending = wait(pending, timeout=1.0, return_when=FIRST_COMPLETED)
            if not done:
                now = time.time()
                if (now - last_heartbeat) >= heartbeat_s:
                    prog = _progress_line(start_ts, done_bytes, total_bytes)
                    finished = total_futures - len(pending)
                    print(
                        f"[WDC] prop_stats progress {done_bytes}/{total_bytes} bytes {prog} | workers {finished}/{total_futures}",
                        file=sys.stderr,
                    )
                    last_heartbeat = now
                continue
            for fut in done:
                local_labels, local_descs, fsize = fut.result()
                labels.update(local_labels)
                descs.update(local_descs)
                done_bytes += fsize
                prog = _progress_line(start_ts, done_bytes, total_bytes)
                print(f"[WDC] prop_stats progress {done_bytes}/{total_bytes} bytes {prog}", file=sys.stderr)
    done_bytes = total_bytes
    prog = _progress_line(start_ts, done_bytes, total_bytes)
    print(f"[WDC] prop_stats progress {done_bytes}/{total_bytes} bytes {prog}", file=sys.stderr)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as out:
        out.write("predicate\tcount\tlabel\tdescription\n")
        for prop, count in sorted(counts.items(), key=lambda x: (-x[1], x[0])):
            label = labels.get(prop, "")
            desc = descs.get(prop, "")
            out.write(f"{prop}\t{count}\t{label}\t{desc}\n")


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
    wdc_exclude_prop_patterns,
    wd_exclude_props,
    replace_map,
    lowercase_wd,
    add_wd_labels,
    wd_raw_cache_path=None,
):
    out_attr_1 = os.path.join(out_dir, "attr_triples_1")
    out_rel_1 = os.path.join(out_dir, "rel_triples_1")
    out_attr_2 = os.path.join(out_dir, "attr_triples_2")
    out_rel_2 = os.path.join(out_dir, "rel_triples_2")
    out_links = os.path.join(out_dir, "ent_links")
    out_prop_stats_wdc = os.path.join(out_dir, "prop_stats_wdc.tsv")
    out_prop_stats_wd = os.path.join(out_dir, "prop_stats_wd.tsv")

    # Keep only WDC entities with blank-node identifiers.
    filtered_wdc = []
    filtered_wd_raw = []
    filtered_wdc_values = []
    filtered_wd_values = []
    for idx, (wdc_ent, wd_ent_raw) in enumerate(zip(wdc_entities, wd_entities_raw)):
        wdc_norm = str(wdc_ent or "").strip().strip("<>")
        if not is_allowed_wdc_subject(wdc_norm):
            continue
        filtered_wdc.append(wdc_norm)
        filtered_wd_raw.append(wd_ent_raw)
        filtered_wdc_values.append(wdc_values[idx] if idx < len(wdc_values) else "")
        filtered_wd_values.append(wd_values[idx] if idx < len(wd_values) else "")
    wdc_entities = filtered_wdc
    wd_entities_raw = filtered_wd_raw
    wdc_values = filtered_wdc_values
    wd_values = filtered_wd_values

    wd_entities_out = [
        canonical_wd_link_entity_uri(
            normalize_wd_uri(replace_map.get(uri, uri), lowercase_wd)
        )
        for uri in wd_entities_raw
    ]
    linked_only_entities = bool(getattr(args, "linked_only_entities", True))
    wd_linked_entities_filter = set(wd_entities_raw) | set(wd_entities_out)
    write_links(out_links, wdc_entities, wd_entities_out, args.dedupe_links)

    split_triples(
        args.wdc_nq,
        out_attr_1,
        out_rel_1,
        seed_subjects=wdc_entities,
        max_depth=args.max_depth,
        mask_values=wdc_mask_values,
        exclude_props=wdc_exclude_props,
        exclude_prop_patterns=wdc_exclude_prop_patterns,
        progress_every=args.progress_every,
        follow_iri_objects=True,
        linked_entity_iris=wdc_entities if linked_only_entities else None,
    )
    # Add labels/descriptions for WDC IRIs and properties found in WDC triples
    append_wdc_labels_descriptions(out_attr_1, out_rel_1, args.wdc_nq)

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
            seed_subjects=wd_linked_entities_filter if linked_only_entities else wd_entities_raw,
            max_depth=args.max_depth,
            lowercase_wd=lowercase_wd,
            mask_values=wd_mask_values,
            exclude_props=wd_exclude_props,
            replace_map=replace_map,
            linked_entity_iris=wd_linked_entities_filter if linked_only_entities else None,
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
        if linked_only_entities:
            tmp_attr = out_attr_2 + ".linked"
            tmp_rel = out_rel_2 + ".linked"
            filter_triples_by_subject_membership(
                out_attr_2,
                out_rel_2,
                tmp_attr,
                tmp_rel,
                wd_linked_entities_filter,
                lowercase_wd=lowercase_wd,
            )
            os.replace(tmp_attr, out_attr_2)
            os.replace(tmp_rel, out_rel_2)
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
            raw_triples_cache_path=wd_raw_cache_path,
            linked_entity_iris=wd_linked_entities_filter if linked_only_entities else None,
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
        if linked_only_entities:
            tmp_attr = out_attr_2 + ".linked"
            tmp_rel = out_rel_2 + ".linked"
            filter_triples_by_subject_membership(
                out_attr_2,
                out_rel_2,
                tmp_attr,
                tmp_rel,
                wd_linked_entities_filter,
                lowercase_wd=lowercase_wd,
            )
            os.replace(tmp_attr, out_attr_2)
            os.replace(tmp_rel, out_rel_2)
    write_prop_stats_wdc(out_prop_stats_wdc, out_attr_1, out_rel_1, args.wdc_nq)
    write_prop_stats(
        out_prop_stats_wd,
        out_attr_2,
        out_rel_2,
        args.sparql_url,
        args.lang,
        args.batch_size,
        args.sleep,
        args.timeout,
        args.retries,
        args.backoff,
    )
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
    session=None,
):
    headers = {
        "Accept": "application/n-triples",
        "User-Agent": "beam-builder/1.0",
    }
    close_session = False
    if session is None:
        session = requests.Session()
        close_session = True
    try:
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
                    resp = session.post(
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
    finally:
        if close_session:
            session.close()


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
    raw_triples_cache_path=None,
    linked_entity_iris=None,
):
    os.makedirs(os.path.dirname(out_attr_path), exist_ok=True)
    os.makedirs(os.path.dirname(out_rel_path), exist_ok=True)
    subjects_in = [s for s in subjects if s]
    total_subjects_in = len(subjects_in)
    subjects = dedupe_preserve_order(subjects_in)
    total_subjects_unique = len(subjects)
    if not subjects:
        Path(out_attr_path).write_text("", encoding="utf-8")
        Path(out_rel_path).write_text("", encoding="utf-8")
        return

    if total_subjects_unique < total_subjects_in:
        print(
            f"[WD] dedup subjects {total_subjects_unique}/{total_subjects_in}",
            file=sys.stderr,
        )

    batch_size_eff = max(1, int(batch_size or 1))
    total_batches = max(1, (total_subjects_unique + batch_size_eff - 1) // batch_size_eff)
    progress_started_at = time.time()

    start_batch = 1
    state = {
        "done_batch": 0,
        "batch_size": batch_size,
        "lang": language,
        "endpoint": endpoint,
        "total_subjects": total_subjects_unique,
    }
    if resume and state_path and os.path.exists(state_path):
        with open(state_path, "r", encoding="utf-8") as f:
            prev = json.load(f)
        start_batch = int(prev.get("done_batch", 0)) + 1
        print(f"[WD] resuming from batch {start_batch}", file=sys.stderr)

    done_batches = max(0, start_batch - 1)

    def _emit_batch_progress(done):
        done_i = max(0, min(int(done), total_batches))
        pct = (done_i / total_batches) * 100.0 if total_batches > 0 else 100.0
        if done_i <= 0:
            eta_txt = "ETA: N/A"
        else:
            elapsed = max(0.001, time.time() - progress_started_at)
            remaining_batches = max(0, total_batches - done_i)
            eta_txt = _format_eta((elapsed / done_i) * remaining_batches)
        print(
            f"[WD] Progress: batches {done_i}/{total_batches} | {pct:5.1f}% | {eta_txt}",
            file=sys.stderr,
        )

    _emit_batch_progress(done_batches)

    attr_mode = "a" if resume else "w"
    rel_mode = "a" if resume else "w"
    exclude_props_norm = {_normalize_prop_token(p) for p in exclude_props} if exclude_props else None
    linked_entities_inner = set()
    for value in list(linked_entity_iris or []):
        token = str(value or "").strip()
        if not token:
            continue
        if token.startswith("<") and token.endswith(">"):
            token = token[1:-1]
        linked_entities_inner.add(token)
    load_from_cache = bool(raw_triples_cache_path and os.path.exists(raw_triples_cache_path))
    cache_tmp_path = None
    cache_writer = None
    cache_completed = False
    if raw_triples_cache_path and not load_from_cache:
        cache_tmp_path = f"{raw_triples_cache_path}.tmp.{os.getpid()}"
        cache_dir = os.path.dirname(raw_triples_cache_path)
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)
        cache_writer = open(cache_tmp_path, "w", encoding="utf-8")

    def _write_processed_triple(attr_out, rel_out, triple, counters):
        kept_attr, kept_rel = counters
        s, p, o = triple
        p_norm = _normalize_prop_token(p)
        if exclude_props_norm and p_norm in exclude_props_norm:
            return kept_attr, kept_rel
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
                    return kept_attr, kept_rel
            attr_out.write(f"{s_out}\t{p_out}\t{o_out}\n")
            kept_attr += 1
        else:
            if o.startswith("<") and linked_entities_inner:
                o_inner = o[1:-1]
                if o_inner not in linked_entities_inner:
                    return kept_attr, kept_rel
            rel_out.write(f"{s_out}\t{p_out}\t{o_out}\n")
            kept_rel += 1
        return kept_attr, kept_rel

    session = requests.Session()
    with open(out_attr_path, attr_mode, encoding="utf-8") as attr_out, \
         open(out_rel_path, rel_mode, encoding="utf-8") as rel_out:
        kept_attr = 0
        kept_rel = 0
        try:
            if load_from_cache:
                print(f"[WD] using cached triples: {raw_triples_cache_path}", file=sys.stderr)
                for triple in _read_raw_wd_triples(raw_triples_cache_path):
                    kept_attr, kept_rel = _write_processed_triple(
                        attr_out, rel_out, triple, (kept_attr, kept_rel)
                    )
                done_batches = total_batches
                _emit_batch_progress(done_batches)
                cache_completed = True
            else:
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
                    session=session,
                ):
                    if item is None:
                        done_batches = max(done_batches, batch_idx)
                        _emit_batch_progress(done_batches)
                        if state_path:
                            state["done_batch"] = batch_idx
                            with open(state_path, "w", encoding="utf-8") as f:
                                json.dump(state, f, indent=2)
                        continue
                    if cache_writer:
                        cache_writer.write(f"{item[0]}\t{item[1]}\t{item[2]}\n")
                    kept_attr, kept_rel = _write_processed_triple(
                        attr_out, rel_out, item, (kept_attr, kept_rel)
                    )
                done_batches = total_batches
                _emit_batch_progress(done_batches)
                cache_completed = True
        finally:
            session.close()
            if cache_writer:
                cache_writer.close()
                try:
                    if cache_completed:
                        os.replace(cache_tmp_path, raw_triples_cache_path)
                    elif cache_tmp_path and os.path.exists(cache_tmp_path):
                        os.remove(cache_tmp_path)
                except Exception:
                    pass
        print(f"[WD] done attr={kept_attr} rel={kept_rel}", file=sys.stderr)



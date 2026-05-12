
def _exact_worker(args):
    wdc_items, wikidata_map, min_length = args
    exact_matches = []
    wdc_values_matched = set()
    for wdc_norm, wdc_entries in wdc_items:
        if len(wdc_norm) < min_length:
            continue
        if wdc_norm in wikidata_map:
            for wdc_orig, wdc_iri in wdc_entries:
                for wiki_orig, wiki_uri in wikidata_map[wdc_norm]:
                    exact_matches.append({
                        'wdc_iri': wdc_iri,
                        'wikidata_uri': wiki_uri,
                        'wdc_value': wdc_orig,
                        'wiki_value': wiki_orig,
                        'method': 'exact'
                    })
                    wdc_values_matched.add(wdc_orig)
    return exact_matches, wdc_values_matched

def _fuzzy_worker(args):
    wdc_items, wikidata_map, wikidata_norms, min_length = args
    fuzzy_matches = []
    wdc_values_matched = set()
    for wdc_norm, wdc_entries in wdc_items:
        if len(wdc_norm) < min_length:
            continue
        for wiki_norm in wikidata_norms:
            if len(wiki_norm) < min_length:
                continue
            min_len = min(len(wdc_norm), len(wiki_norm))
            if wdc_norm[:min_len] == wiki_norm[:min_len]:
                for wdc_orig, wdc_iri in wdc_entries:
                    for wiki_orig, wiki_uri in wikidata_map[wiki_norm]:
                        fuzzy_matches.append({
                            'wdc_iri': wdc_iri,
                            'wikidata_uri': wiki_uri,
                            'wdc_value': wdc_orig,
                            'wiki_value': wiki_orig,
                            'min_len': min_len,
                            'method': f'fuzzy_{min_len}'
                        })
                        wdc_values_matched.add(wdc_orig)
    return fuzzy_matches, wdc_values_matched

def _process_exact_window(chunks, wikidata_map, min_length, executor):
    futures = [executor.submit(_exact_worker, (chunk, wikidata_map, min_length)) for chunk in chunks]
    for fut in as_completed(futures):
        yield fut.result()

def _process_fuzzy_window(chunks, wikidata_map, wikidata_norms, min_length, executor):
    futures = [executor.submit(_fuzzy_worker, (chunk, wikidata_map, wikidata_norms, min_length)) for chunk in chunks]
    for fut in as_completed(futures):
        yield fut.result()

def fuzzy_link(wdc_map, wikidata_map, parallel=True, workers=None, lock_path=None, min_length=1):
    """
    Lie les entités WDC et Wikidata via fuzzy matching
    Compare sur la longueur du plus court des deux
    """
    print_color(f"\n🔗 Linking WDC ↔ Wikidata...", Colors.CYAN)
    print("   Stratégie: Matching exact")
    # Fuzzy min-len removed permanently
    
    # Fuzzy phase is disabled; keep exact matching available for short identifiers too
    # (e.g. ISO-2 country codes).
    MIN_LENGTH = max(1, int(min_length))
    
    exact_matches = []
    fuzzy_matches = []
    matched_pairs = set()
    
    total_comparisons = 0
    skipped_too_short = 0
    short_value_infos = []
    wdc_values_matched = set()  # Pour compter les valeurs WDC distinctes matchées
    
    print("\n   Phase 1: Matching exact...")
    wdc_items = list(wdc_map.items())
    if parallel and len(wdc_items) > 1:
        if lock_path:
            n_workers, _runs, _cpu = get_shared_workers(
                lock_path, share=ALIGN_CPU_SHARE, override=workers
            )
        else:
            n_workers = min(max(1, int(workers or 1)), MAX_PARALLEL_WORKERS)
        chunk_size = max(1, len(wdc_items) // max(1, n_workers))
        chunks = [wdc_items[i:i+chunk_size] for i in range(0, len(wdc_items), chunk_size)]
        window_size = max(1, n_workers * 2)
        with ProcessPoolExecutor(max_workers=n_workers) as ex:
            idx = 0
            while idx < len(chunks):
                window = chunks[idx:idx+window_size]
                for matches_part, wdc_matched_part in _process_exact_window(window, wikidata_map, MIN_LENGTH, ex):
                    exact_matches.extend(matches_part)
                    wdc_values_matched.update(wdc_matched_part)
                idx += window_size
    else:
        matches_part, wdc_matched_part = _exact_worker((wdc_items, wikidata_map, MIN_LENGTH))
        exact_matches.extend(matches_part)
        wdc_values_matched.update(wdc_matched_part)
    
    # Dédoublonnage exact par paire
    exact_unique = []
    exact_pairs = set()
    for m in exact_matches:
        pair = (m['wdc_iri'], m['wikidata_uri'])
        if pair not in exact_pairs:
            exact_pairs.add(pair)
            exact_unique.append(m)
    exact_matches = exact_unique
    
    print(f"   ✅ {len(exact_matches)} paires (exact)")
    
    print("\n   Phase 2: Matching fuzzy supprimé")
    all_matches = exact_matches
    print(f"   ✅ {len(all_matches)} paires (total)")
    return all_matches, wdc_values_matched

def export_unmatched_values(wdc_values_matched, wdc_map, output_dir, key_name=None):
    output_dir = Path(output_dir)
    header = f"{key_name}_value" if key_name else "wdc_value"
    unmatched_values = sorted({
        orig
        for entries in wdc_map.values()
        for orig, _iri in entries
        if orig not in wdc_values_matched
    })
    unmatched_file = output_dir / "wdc_unmatched_values.csv"
    with open(unmatched_file, "w", encoding="utf-8") as f:
        f.write(f"{header}\n")
        for val in unmatched_values:
            f.write(f"{val}\n")
    print(f"   ✅ {unmatched_file}")


def export_results(matches, wdc_values_matched, wdc_map, wikidata_map, output_dir, key_name=None,
                   class_name=None, parts_spec=None, pattern=None, wikidata_property=None,
                   wkd_class=None, wkd_prop_class=None, start_ts=None):
    """Exporte les résultats"""
    output_dir = Path(output_dir)
    
    print_color(f"\n💾 Export des résultats...", Colors.BLUE)
    
    # TSV détaillé
    tsv_file = output_dir / "wdc_wikidata_links.tsv"
    with open(tsv_file, 'w', encoding='utf-8') as f:
        f.write("wdc_iri\twikidata_uri\twdc_value\twiki_value\tmethod\tmin_len\n")
        for m in matches:
            min_len = m.get('min_len', '')
            f.write(f"{m['wdc_iri']}\t{m['wikidata_uri']}\t{m['wdc_value']}\t{m['wiki_value']}\t{m['method']}\t{min_len}\n")
    
    print(f"   ✅ {tsv_file}")
    
    # N-Triples owl:sameAs
    nt_file = output_dir / "owl_sameas.nt"
    with open(nt_file, 'w', encoding='utf-8') as f:
        for m in matches:
            f.write(f"<{m['wdc_iri']}> <http://www.w3.org/2002/07/owl#sameAs> <{m['wikidata_uri']}> .\n")
    
    print(f"   ✅ {nt_file}")
    
    # Statistiques
    stats_file = output_dir / "stats.txt"
    with open(stats_file, 'w', encoding='utf-8') as f:
        f.write("="*60 + "\n")
        f.write("STATISTIQUES DE LINKING\n")
        f.write("="*60 + "\n\n")

        # Inputs / contexte
        f.write("Inputs:\n")
        f.write(f"  Class: {class_name or ''}\n")
        f.write(f"  Parts: {parts_spec or ''}\n")
        f.write(f"  WDC predicate pattern: {pattern or ''}\n")
        f.write(f"  WD property: {wikidata_property or ''}\n")
        f.write(f"  WD class filter: {wkd_class or ''}\n")
        f.write(f"  WD property-class filter: {wkd_prop_class or ''}\n")
        f.write(f"  Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        if start_ts:
            elapsed = time.time() - start_ts
            took = _format_eta(elapsed).replace("ETA: ", "")
            f.write(f"  Duration: {took}\n")
        f.write("\n")

        # WDC volumes
        wdc_entities = {iri for entries in wdc_map.values() for (_val, iri) in entries}
        wdc_values_raw = {val for entries in wdc_map.values() for (val, _iri) in entries}
        wdc_values_norm = set(wdc_map.keys())
        wdc_pairs_total = sum(len(v) for v in wdc_map.values())
        f.write("WDC (volumes):\n")
        f.write(f"  Entities (distinct IRIs): {len(wdc_entities)}\n")
        f.write(f"  Values (distinct, raw): {len(wdc_values_raw)}\n")
        f.write(f"  Values (distinct, normalized): {len(wdc_values_norm)}\n")
        f.write(f"  Value→Entity pairs (total): {wdc_pairs_total}\n\n")

        # Wikidata volumes
        wd_entities = {iri for entries in wikidata_map.values() for (_val, iri) in entries}
        wd_values_raw = {val for entries in wikidata_map.values() for (val, _iri) in entries}
        wd_values_norm = set(wikidata_map.keys())
        wd_pairs_total = sum(len(v) for v in wikidata_map.values())
        f.write("Wikidata (volumes):\n")
        f.write(f"  Entities (distinct IRIs): {len(wd_entities)}\n")
        f.write(f"  Values (distinct, raw): {len(wd_values_raw)}\n")
        f.write(f"  Values (distinct, normalized): {len(wd_values_norm)}\n")
        f.write(f"  Value→Entity pairs (total): {wd_pairs_total}\n\n")

        exact_count = len([m for m in matches if m['method'] == 'exact'])
        fuzzy_count = len([m for m in matches if m['method'].startswith('fuzzy')])

        matched_wdc_entities = {m["wdc_iri"] for m in matches}
        matched_wd_entities = {m["wikidata_uri"] for m in matches}
        matched_wdc_values_raw = {m["wdc_value"] for m in matches}
        matched_wd_values_raw = {m["wiki_value"] for m in matches}
        matched_wdc_values_norm = set(wdc_values_matched)
        phone_mode = _looks_like_phone_mode(pattern) or _looks_like_phone_mode(wikidata_property)
        matched_wd_values_norm = set(
            normalize_value_for_matching(v, phone_mode=phone_mode)
            for v in matched_wd_values_raw
            if v
        )

        f.write("Matches:\n")
        f.write(f"  Pairs (exact): {exact_count}\n")
        f.write(f"  Pairs (fuzzy): {fuzzy_count}\n")
        f.write(f"  Total pairs: {len(matches)}\n")
        f.write(f"  WDC entities matched (distinct): {len(matched_wdc_entities)}\n")
        f.write(f"  Wikidata entities matched (distinct): {len(matched_wd_entities)}\n")
        f.write(f"  WDC values matched (distinct, raw): {len(matched_wdc_values_raw)}\n")
        f.write(f"  Wikidata values matched (distinct, raw): {len(matched_wd_values_raw)}\n\n")

        # Coverage
        def _pct(n, d):
            return (n / d * 100) if d else 0.0
        f.write("Coverage:\n")
        f.write(f"  WDC values (normalized): {len(matched_wdc_values_norm)}/{len(wdc_values_norm)} ({_pct(len(matched_wdc_values_norm), len(wdc_values_norm)):.2f}%)\n")
        f.write(f"  WDC entities: {len(matched_wdc_entities)}/{len(wdc_entities)} ({_pct(len(matched_wdc_entities), len(wdc_entities)):.2f}%)\n")
        f.write(f"  Wikidata entities: {len(matched_wd_entities)}/{len(wd_entities)} ({_pct(len(matched_wd_entities), len(wd_entities)):.2f}%)\n")
        f.write(f"  Wikidata values (normalized): {len(matched_wd_values_norm)}/{len(wd_values_norm)} ({_pct(len(matched_wd_values_norm), len(wd_values_norm)):.2f}%)\n")
    
    print(f"   ✅ {stats_file}")
    
    # Valeurs WDC non alignées
    export_unmatched_values(wdc_values_matched, wdc_map, output_dir, key_name=key_name)
    
    print_color(f"\n✅ Alignnement done.", Colors.GREEN)

def _sum_file_sizes(paths):
    total = 0
    for p in paths:
        try:
            total += Path(p).stat().st_size
        except FileNotFoundError:
            continue
    return total

def main():
    parser = argparse.ArgumentParser(description="WDC Entity Linker")
    parser.add_argument("class_name")
    parser.add_argument("parts_spec", nargs="?")
    parser.add_argument("pattern", nargs="?")
    parser.add_argument("wikidata_property", nargs="?")
    parser.add_argument("--wkd-class", help="Wikidata class QID or IRI (e.g., Q6256 or wdt:Q6256) used to filter items")
    parser.add_argument("--wkd-prop-class", help="Wikidata property class QID/IRI to filter ?prop (e.g., Q853614 for identifiers)")
    parser.add_argument("--wdc-type", help="WDC rdf:type IRI or class name (e.g., http://schema.org/Country or Country) used to filter extracted subjects")
    parser.add_argument("--top-props", action="store_true", help="Afficher le top 100 des propriétés WDC (calculé pendant le filtrage)")
    parser.add_argument("--workers", type=int, help="Nombre de workers pour le parallélisme (défaut: 80% CPU partagé entre runs)")
    parser.add_argument("--ignore-chars", help="Liste de caractères à supprimer avant normalisation (ex: \"spaces;+;\\\\;\\\\/;\\\\\\\\\")")
    parser.add_argument("--wdc-value-is-wikidata", action="store_true", help="Interpréter les valeurs WDC comme URLs Wikidata et matcher directement les entités")
    args = parser.parse_args()
    
    start_ts = time.time()
    class_name = args.class_name
    pattern = args.pattern
    parts_spec = args.parts_spec
    wikidata_property = args.wikidata_property
    if args.wdc_type:
        type_filter_iris = [normalize_wdc_type(args.wdc_type)]
    else:
        type_filter_iris = default_type_filter_iris_for_class(class_name)
    if args.ignore_chars:
        set_extra_strip_chars(parse_strip_list(args.ignore_chars))
    if args.wdc_value_is_wikidata and not args.wkd_class:
        print_color("❌ --wdc-value-is-wikidata nécessite --wkd-class", Colors.RED)
        sys.exit(1)

    # Mode: top-props uniquement (class_name + --top-props)
    top_props_only = args.top_props and not (parts_spec and pattern and wikidata_property)
    
    print("="*60)
    print("🎯 WDC Entity Linker")
    print("="*60)
    print(f"Classe:              {class_name}")
    print(f"Parts:               {parts_spec}")
    if not top_props_only:
        print(f"Pattern:             {pattern}")
        print(f"Propriété Wikidata:  {wikidata_property or '?prop'}")
    if args.wkd_class:
        print(f"Classe Wikidata:     {normalize_wkd_class(args.wkd_class)}")
    if args.wkd_prop_class:
        print(f"Classe Prop WD:      {normalize_wkd_prop_class(args.wkd_prop_class)}")
    if args.wdc_type:
        print(f"WDC rdf:type:        {normalize_wdc_type(args.wdc_type)}")
    print("="*60)
    
    # Setup directories (always under Download/<ClassName>)
    work_dir = Path("Download") / class_name
    work_dir.mkdir(parents=True, exist_ok=True)
    data_dir = work_dir

    # Calculer le nombre de workers par run (partage 80% CPU)
    lock_path = Path("Download") / ".workers.lock"
    workers_override = args.workers
    if workers_override:
        print(f"Workers:             {workers_override} (override)")
    else:
        workers_default, runs, cpu = compute_shared_workers(lock_path, share=ALIGN_CPU_SHARE)
        print(f"Workers:             {workers_default} (80% CPU partagé / {runs} runs, CPU={cpu})")
    
    # 1. Source WDC: toujours part_* (jamais *_full_graph.nq)
    decompressed_files = []
    available_parts = None
    if parts_spec is None:
        print_color("❌ parts_spec manquant (ex: all)", Colors.RED)
        sys.exit(1)
    if parts_spec.lower() == "all":
        available_parts = discover_parts(class_name)
        if not available_parts:
            print_color("❌ Aucune part disponible", Colors.RED)
            sys.exit(1)
    else:
        available_parts = discover_parts(class_name)
        if not available_parts:
            print_color("⚠️  Impossible de récupérer la liste distante, utilisation de la spécification locale.", Colors.YELLOW)
            available_parts = None

    parts_to_download = parse_parts_spec(parts_spec, available_parts)
    if not parts_to_download:
        print_color(f"❌ Aucune part valide pour '{parts_spec}'", Colors.RED)
        sys.exit(1)

    print_color(f"\n📦 {len(parts_to_download)} parts sélectionnées", Colors.GREEN)
    decompressed_files = download_and_decompress(
        class_name,
        parts_to_download,
        data_dir,
        parallel_decompress=True,
        workers=workers_override,
        lock_path=lock_path,
    )
    if not decompressed_files:
        print_color("❌ Aucun fichier disponible", Colors.RED)
        sys.exit(1)
    
    # 2. Top-props only: pas besoin de pattern/WD
    if top_props_only:
        top_props_file = work_dir / "top-props.txt"
        if top_props_file.exists():
            top_props_file.unlink()
        scan_top_props_from_files(
            decompressed_files,
            top_n=1000,
            parallel=True,
            workers=workers_override,
            lock_path=lock_path,
            progress_every=100,
            output_file=top_props_file,
            type_filter_iris=type_filter_iris,
        )
        print_color(f"\n✅ Top-props écrit dans {top_props_file}", Colors.GREEN)
        elapsed = time.time() - start_ts
        took = _format_eta(elapsed).replace("ETA: ", "")
        print_color(f"\n⏱️  Temps total: {took}", Colors.GREEN)
        try:
            with open(work_dir / "stats.txt", "a", encoding="utf-8") as f:
                f.write(f"top-props took {took}\n")
        except Exception:
            pass
        return

    # 3. Extraire les IRIs WDC uniques (sans fichier filtré ni graphe fusionné)
    wdc_map, matched_count = extract_unique_iris_from_files(
        decompressed_files,
        pattern,
        collect_top_props=args.top_props,
        top_n=100,
        parallel=True,
        workers=workers_override,
        lock_path=lock_path,
        progress_every=100,
        wdc_value_is_wd_iri=args.wdc_value_is_wikidata,
        type_filter_iris=type_filter_iris,
    )
    if matched_count == 0:
        sys.exit(1)
    
    # 6. Récupérer les valeurs Wikidata
    if args.wdc_value_is_wikidata:
        # Extract WD entity IRIs from WDC values
        wd_entity_iris = set()
        for entries in wdc_map.values():
            for value, _iri in entries:
                wd_iri = extract_wd_entity_iri(value)
                if wd_iri:
                    wd_entity_iris.add(wd_iri)
        if not wd_entity_iris:
            print_color("❌ No Wikidata URLs extracted from WDC values.", Colors.RED)
            sys.exit(1)
        wikidata_map = fetch_wikidata_values(
            wikidata_property=None,
            wkd_class=args.wkd_class,
            wkd_prop_class=None,
            entity_iris=sorted(wd_entity_iris),
        )
    else:
        wikidata_map = fetch_wikidata_values(wikidata_property, args.wkd_class, args.wkd_prop_class)
    if not wikidata_map:
        print_color("❌ Impossible de récupérer les données Wikidata", Colors.RED)
        sys.exit(1)
    
    # 7. Linking
    matches, wdc_values_matched = fuzzy_link(
        wdc_map,
        wikidata_map,
        parallel=True,
        workers=workers_override,
        lock_path=lock_path,
    )
    
    # 8. Statistiques finales
    print("\n" + "="*60)
    print_color("📊 RÉSULTATS FINAUX", Colors.CYAN)
    print("="*60)
    print(f"\n🔢 STATISTIQUES WDC:")
    total_lines = 115955562 if len(decompressed_files) == 7 else None
    total_lines_str = f"{total_lines:,}" if total_lines is not None else "N/A"
    print(f"   Lignes totales traitées:           {total_lines_str}")
    print(f"   Lignes matchant le pattern:        {matched_count:,}")
    print(f"   Valeurs distinctes (brutes):       {len(wdc_map):,}")
    
    print(f"\n🌐 STATISTIQUES WIKIDATA:")
    print(f"   Valeurs distinctes:                {len(wikidata_map):,}")
    
    print(f"\n🔗 LINKING:")
    exact_count = len([m for m in matches if m['method'] == 'exact'])
    fuzzy_count = len([m for m in matches if m['method'].startswith('fuzzy')])
    print(f"   Paires matchées (exact):           {exact_count:,}")
    print(f"   Paires matchées (fuzzy):           {fuzzy_count:,}")
    print(f"   TOTAL paires:                      {len(matches):,}")
    
    print_color(f"\n🎯 VALEURS WDC DISTINCTES LINKÉES:   {len(wdc_values_matched):,}", Colors.GREEN)
    
    if len(wdc_map) > 0:
        coverage = (len(wdc_values_matched) / len(wdc_map)) * 100
        print_color(f"📈 COVERAGE WDC:                     {coverage:.2f}%", Colors.GREEN)
    
    print(f"\n💡 Comparaison avec SPARQL:")
    print(f"   SELECT COUNT(DISTINCT ?value) WHERE {{ ?s <...{pattern}...> ?value }}")
    print(f"   → Devrait être environ: {len(wdc_map):,} valeurs distinctes")
    print(f"\n   SELECT DISTINCT ?value WHERE {{")
    print(f"     SERVICE <wikidata> {{ ?w {wikidata_property} ?value")
    if args.wkd_class:
        wkd_class_norm = normalize_wkd_class(args.wkd_class)
        print(f"       ?w wdt:P31 ?type .")
        print(f"       ?type wdt:P279* {wkd_class_norm} .")
    print(f"     }}")
    print(f"     ?s <...{pattern}...> ?value")
    print(f"   }}")
    print(f"   → {len(wdc_values_matched):,} valeurs WDC ont un match Wikidata")
    
    # 9. Export
    export_results(
        matches,
        wdc_values_matched,
        wdc_map,
        wikidata_map,
        work_dir,
        key_name=pattern,
        class_name=class_name,
        parts_spec=parts_spec,
        pattern=pattern,
        wikidata_property=wikidata_property,
        wkd_class=args.wkd_class,
        wkd_prop_class=args.wkd_prop_class,
        start_ts=start_ts,
    )

    elapsed = time.time() - start_ts
    took = _format_eta(elapsed).replace("ETA: ", "")
    print_color(f"\n⏱️  Temps total: {took}", Colors.GREEN)
    
    print("\n" + "="*60)
    print_color("✨ TERMINÉ!", Colors.GREEN)
    print("="*60)
    print(f"\nFichiers générés dans: {work_dir}/")
    print(f"  - wdc_wikidata_links.tsv (liens détaillés)")
    print(f"  - owl_sameas.nt (triplets RDF)")
    print(f"  - stats.txt (statistiques)")
    print()

if __name__ == "__main__":
    main()

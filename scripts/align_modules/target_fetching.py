
def extract_unique_iris_from_files(
    files,
    pattern,
    collect_top_props=False,
    top_n=100,
    parallel=True,
    workers=None,
    batch_size=500000,
    lock_path=None,
    progress_every=100,
    top_props_file=None,
    wdc_value_is_wd_iri=False,
    type_filter_iris=None,
    search_in="predicate",
):
    """
    Scanne plusieurs fichiers NQuads (parts), filtre par pattern de prédicat,
    et extrait les valeurs distinctes sans générer de fichier fusionné.
    """
    print_color(f"\n📊 Extraction directe depuis les parts (sans graphe fusionné)...", Colors.BLUE)
    search_mode = _normalize_search_in_mode(search_in)
    prepared_patterns = prepare_predicate_patterns(pattern)
    if not prepared_patterns:
        return {}, 0
    phone_mode = _looks_like_phone_mode(pattern)
    if len(prepared_patterns) == 1:
        pattern_normalized, pattern_raw, _ = prepared_patterns[0]
        if pattern_raw:
            print(f"   Pattern brut: '{pattern}'")
        else:
            print(f"   Pattern normalisé: '{pattern_normalized}'")
    else:
        print(f"   Patterns (OR): {', '.join(t for _, _, t in prepared_patterns)}")
    
    value_map = defaultdict(list)
    all_raw_values = set()
    all_iris = set()
    country_code_changes = defaultdict(int)
    predicates_found = defaultdict(int) if collect_top_props else None
    total_lines = 0
    matched_lines = 0
    
    files = [Path(p) for p in files]
    allowed_subjects = collect_allowed_subjects_by_type(files, type_filter_iris, progress_every=progress_every)
    if allowed_subjects is not None and len(allowed_subjects) == 0:
        print_color("❌ Aucun sujet ne matche le rdf:type demandé", Colors.RED)
        return {}, 0
    
    total_bytes = sum(Path(p).stat().st_size for p in files)
    done_bytes = 0
    start_ts = time.time()
    if progress_every:
        prog = _progress_line(start_ts, done_bytes, total_bytes)
        print(f"  ⏳ Progress: Lignes lues: {total_lines:,} | Matches: {matched_lines:,} | Valeurs distinctes: {len(all_raw_values)} | {prog}", flush=True)
    if parallel:
        buffer = []
        window_batches = []
        if lock_path:
            n_workers, _runs, _cpu = get_shared_workers(
                lock_path, share=ALIGN_CPU_SHARE, override=workers
            )
        else:
            n_workers = min(max(1, int(workers or 1)), MAX_PARALLEL_WORKERS)
        window_size = max(1, n_workers * 6)
        with ProcessPoolExecutor(max_workers=n_workers) as ex:
            for file_path in files:
                file_base = done_bytes
                bytes_read = 0
                print(f"\n  📄 Scan: {file_path.name}")
                if progress_every:
                    prog = _progress_line(start_ts, done_bytes, total_bytes)
                    print(f"  ⏳ Progress: Lignes lues: {total_lines:,} | Matches: {matched_lines:,} | Valeurs distinctes: {len(all_raw_values)} | {prog}", flush=True)
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        bytes_read += len(line)
                        total_lines += 1
                        if allowed_subjects is not None:
                            subject_tok, _, _ = _extract_spo_tokens(line)
                            if not subject_tok or subject_tok not in allowed_subjects:
                                continue
                        buffer.append(line)
                        if len(buffer) >= batch_size:
                            window_batches.append(buffer)
                            buffer = []

                        if progress_every and total_lines % progress_every == 0:
                            done_bytes = file_base + bytes_read
                            prog = _progress_line(start_ts, done_bytes, total_bytes)
                            print(f"\r  Lignes lues: {total_lines:,} | Matches: {matched_lines:,} | Valeurs distinctes: {len(all_raw_values)} | {prog}", end='', flush=True)

                        if len(window_batches) >= window_size:
                            for vmap, raw_vals, iris, cc_changes, lines, matched, preds in _process_extract_window(
                                window_batches, prepared_patterns, collect_top_props, wdc_value_is_wd_iri, phone_mode, search_mode, ex
                            ):
                                matched_lines += matched
                                all_raw_values.update(raw_vals)
                                all_iris.update(iris)
                                for k, v in cc_changes.items():
                                    country_code_changes[k] += v
                                for norm, entries in vmap.items():
                                    value_map[norm].extend(entries)
                                if collect_top_props and predicates_found is not None:
                                    for pred, cnt in preds.items():
                                        predicates_found[pred] += cnt
                                if progress_every and total_lines % progress_every == 0:
                                    done_bytes = file_base + bytes_read
                                    prog = _progress_line(start_ts, done_bytes, total_bytes)
                                    print(f"\r  Lignes lues: {total_lines:,} | Matches: {matched_lines:,} | Valeurs distinctes: {len(all_raw_values)} | {prog}", end='', flush=True)
                            window_batches = []
                if collect_top_props and predicates_found is not None:
                    print_top_props(
                        predicates_found,
                        top_n=top_n,
                        title=f"\n  📋 Top {top_n} prédicats (après {file_path.name}):",
                        output_file=top_props_file,
                    )

            if buffer:
                window_batches.append(buffer)
            if window_batches:
                for vmap, raw_vals, iris, cc_changes, lines, matched, preds in _process_extract_window(
                    window_batches, prepared_patterns, collect_top_props, wdc_value_is_wd_iri, phone_mode, search_mode, ex
                ):
                    matched_lines += matched
                    all_raw_values.update(raw_vals)
                    all_iris.update(iris)
                    for k, v in cc_changes.items():
                        country_code_changes[k] += v
                    for norm, entries in vmap.items():
                        value_map[norm].extend(entries)
                    if collect_top_props and predicates_found is not None:
                        for pred, cnt in preds.items():
                            predicates_found[pred] += cnt
                    if progress_every and total_lines % progress_every == 0:
                        print(f"\r  Lignes lues: {total_lines:,} | Matches: {matched_lines:,} | Valeurs distinctes: {len(all_raw_values)}", end='', flush=True)
    else:
        for file_path in files:
            file_base = done_bytes
            bytes_read = 0
            print(f"\n  📄 Scan: {file_path.name}")
            if progress_every:
                prog = _progress_line(start_ts, done_bytes, total_bytes)
                print(f"  ⏳ Progress: Lignes lues: {total_lines:,} | Matches: {matched_lines:,} | Valeurs distinctes: {len(all_raw_values)} | {prog}", flush=True)
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    bytes_read += len(line)
                    total_lines += 1
                    subject, predicate_tok, obj_tok = _extract_spo_tokens(line)
                    if not (subject and predicate_tok and obj_tok):
                        continue
                    if allowed_subjects is not None and subject not in allowed_subjects:
                        continue
                    predicate = predicate_tok.strip("<>")
                    if obj_tok.startswith('"'):
                        value = _literal_lex(obj_tok)
                        if value is None:
                            continue
                    elif obj_tok.startswith("<") and obj_tok.endswith(">"):
                        value = obj_tok[1:-1]
                    else:
                        continue
                    if collect_top_props:
                        predicates_found[predicate] += 1
                    if search_mode == "value":
                        if not value_matches_prepared_patterns(value, prepared_patterns):
                            continue
                    else:
                        if not predicate_matches_prepared_patterns(predicate, prepared_patterns):
                            continue
                    matched_lines += 1
                    value_for_norm = value
                    if wdc_value_is_wd_iri:
                        wd_iri = extract_wd_entity_iri(value)
                        if not wd_iri:
                            continue
                        value_for_norm = wd_iri
                    elif search_mode != "value" and not obj_tok.startswith('"'):
                        continue
                    all_raw_values.add(value)
                    all_iris.add(subject)
                    value_normalized = normalize_value_for_matching(value_for_norm, phone_mode=phone_mode)
                    value_normalized_original = value_normalized
                    value_normalized = normalize_country_code(value_normalized)
                    if value_normalized != value_normalized_original:
                        old_code = value_normalized_original[:2]
                        new_code = value_normalized[:2]
                        country_code_changes[f"{old_code.upper()}→{new_code.upper()}"] += 1
                    if value_normalized:
                        value_map[value_normalized].append((value, subject))
                if progress_every and total_lines % progress_every == 0:
                    done_bytes = file_base + bytes_read
                    prog = _progress_line(start_ts, done_bytes, total_bytes)
                    print(f"\r  Lignes lues: {total_lines:,} | Matches: {matched_lines:,} | Valeurs distinctes: {len(all_raw_values)} | {prog}", end='', flush=True)
            if collect_top_props and predicates_found is not None:
                print_top_props(
                    predicates_found,
                    top_n=top_n,
                    title=f"\n  📋 Top {top_n} prédicats (après {file_path.name}):",
                    output_file=top_props_file,
                )
    
    print(f"\r  Lignes: {total_lines:,} | Matches: {matched_lines:,} | Valeurs distinctes: {len(all_raw_values)}")
    
    if matched_lines == 0:
        print_color("❌ Aucune ligne ne matche le pattern", Colors.RED)
        return {}, 0
    
    if country_code_changes:
        print(f"\n🌍 Normalisation des codes pays:")
        for change, count in sorted(country_code_changes.items(), key=lambda x: -x[1]):
            print(f"   {change}: {count} valeurs")
    
    print_color(f"\n📈 Statistiques (équivalent requêtes SPARQL):", Colors.CYAN)
    print(f"   Lignes totales (triplets):           {matched_lines:,}")
    print(f"   IRIs distincts (?songWdc):           {len(all_iris):,}")
    print(f"   Valeurs brutes distinctes (?value):  {len(all_raw_values):,}")
    print(f"   Valeurs normalisées:                 {len(value_map):,}")
    
    # Distribution des longueurs
    lengths = defaultdict(int)
    for norm_val in value_map:
        lengths[len(norm_val)] += 1
    print(f"\n📏 Distribution des longueurs (normalisées):")
    for length in sorted(lengths.keys())[:10]:
        print(f"   {length:>2} chars: {lengths[length]:>6} valeurs")
    
    # Exemples
    print(f"\n📋 Exemples de valeurs (5 premiers):")
    for i, (norm, entries) in enumerate(list(value_map.items())[:5]):
        orig, iri = entries[0]
        orig_display = orig if len(orig) <= 50 else orig[:47] + "..."
        print(f"   {i+1}. '{orig_display}'")
        print(f"      → '{norm}' (len={len(norm)})")
    
    if collect_top_props and predicates_found is not None:
        print(f"\n📋 Prédicats trouvés (top {top_n}):")
        for pred, count in sorted(predicates_found.items(), key=lambda x: -x[1])[:top_n]:
            print(f"   {count:>8} × {pred}")
    
    return value_map, matched_lines


def _is_rate_limited_error(exc: Exception) -> bool:
    msg = str(exc)
    return "429" in msg or "Too Many Requests" in msg


def _is_retryable_query_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    retry_tokens = (
        "incompleteread",
        "remote disconnected",
        "connection reset",
        "connection aborted",
        "temporarily unavailable",
        "timed out",
        "timeout",
        "bad gateway",
        "service unavailable",
        "gateway timeout",
        "invalid control character",
        "unterminated string",
        "jsondecodeerror",
        "expecting value",
        "extra data",
    )
    return any(tok in msg for tok in retry_tokens)


def _load_sparql_json_payload(payload_text: str):
    try:
        return json.loads(payload_text)
    except json.JSONDecodeError:
        pass

    try:
        return json.loads(payload_text, strict=False)
    except json.JSONDecodeError:
        pass

    # Some endpoint responses may contain raw control chars in string values.
    cleaned = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", payload_text)
    return json.loads(cleaned, strict=False)


def _chunk_list(values, chunk_size):
    chunk_size = max(1, int(chunk_size))
    for i in range(0, len(values), chunk_size):
        yield values[i : i + chunk_size]


def _run_sparql_query_with_retry_to_endpoint(endpoint_url, query, headers, timeout_s, max_attempts, base_delay):
    endpoint = str(endpoint_url or "").strip() or WIKIDATA_ENDPOINT
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.post(
                endpoint,
                data={"query": query, "format": "json"},
                headers=headers,
                timeout=timeout_s,
            )
            try:
                response.raise_for_status()
                return _load_sparql_json_payload(response.text)
            except requests.HTTPError as post_err:
                status = getattr(response, "status_code", None)
                # Some endpoints (e.g., DBpedia) may reject POST for specific routes.
                # Fallback to GET before considering this attempt failed.
                if status == 405:
                    get_resp = requests.get(
                        endpoint,
                        params={"query": query, "format": "json"},
                        headers=headers,
                        timeout=timeout_s,
                    )
                    get_resp.raise_for_status()
                    return _load_sparql_json_payload(get_resp.text)
                raise post_err
        except Exception as e:
            if (_is_rate_limited_error(e) or _is_retryable_query_error(e)) and attempt < max_attempts:
                delay_s = (base_delay * (2 ** (attempt - 1))) + random.uniform(0, 0.5)
                print_color(
                    f"⚠️ Wikidata query retry {attempt}/{max_attempts} in {delay_s:.1f}s ({type(e).__name__})...",
                    Colors.YELLOW,
                )
                time.sleep(delay_s)
                continue
            raise
    return None


def _run_sparql_query_with_retry(query, headers, timeout_s, max_attempts, base_delay):
    return _run_sparql_query_with_retry_to_endpoint(
        WIKIDATA_ENDPOINT,
        query=query,
        headers=headers,
        timeout_s=timeout_s,
        max_attempts=max_attempts,
        base_delay=base_delay,
    )


def _truthy_env(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _sparql_quote_literal(value):
    # JSON string escaping is compatible with SPARQL string literals.
    return json.dumps(str(value or ""), ensure_ascii=False)


def _is_absolute_iri(value):
    raw = str(value or "").strip()
    if not raw:
        return False
    if raw.startswith("<") and raw.endswith(">"):
        raw = raw[1:-1].strip()
    return raw.startswith("http://") or raw.startswith("https://")


def _wikidata_cache_path(
    prop,
    wkd_class_norm,
    wkd_prop_class_norm,
    entity_iris=None,
    value_candidates=None,
    lang_key="all",
):
    entity_iris = sorted({str(v).strip() for v in (entity_iris or []) if str(v).strip()})
    entity_hash = "none"
    if entity_iris:
        sha = hashlib.sha1()
        for iri in entity_iris:
            sha.update(iri.encode("utf-8", errors="ignore"))
            sha.update(b"\n")
        entity_hash = sha.hexdigest()
    value_candidates = sorted({str(v).strip() for v in (value_candidates or []) if str(v).strip()})
    value_hash = "none"
    if value_candidates:
        sha = hashlib.sha1()
        for value in value_candidates:
            sha.update(value.encode("utf-8", errors="ignore"))
            sha.update(b"\n")
        value_hash = sha.hexdigest()
    key_payload = {
        "v": 2,
        "prop": prop or "?prop",
        "wkd_class": wkd_class_norm or "",
        "wkd_prop_class": wkd_prop_class_norm or "",
        "lang": str(lang_key or "all"),
        "entity_count": len(entity_iris),
        "entity_hash": entity_hash,
        "value_count": len(value_candidates),
        "value_hash": value_hash,
    }
    cache_key = hashlib.sha1(
        json.dumps(key_payload, sort_keys=True, ensure_ascii=True).encode("utf-8", errors="ignore")
    ).hexdigest()
    cache_root = Path(os.environ.get("WIKIDATA_CACHE_DIR", str(Path("Download") / ".wikidata_cache")))
    cache_root.mkdir(parents=True, exist_ok=True)
    return cache_root / f"values_{cache_key}.json.gz"


def _load_wikidata_value_cache(path):
    if not path.exists() or not path.is_file():
        return None
    ttl_s_raw = os.environ.get("WIKIDATA_CACHE_TTL_S", os.environ.get("WIKIDATA_CACHE_TTL", "604800"))
    try:
        ttl_s = int(ttl_s_raw)
    except Exception:
        ttl_s = 604800
    if ttl_s > 0:
        try:
            age_s = max(0.0, time.time() - float(path.stat().st_mtime))
            if age_s > ttl_s:
                return None
        except Exception:
            return None
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        return None
    value_map_raw = (payload or {}).get("value_map")
    if not isinstance(value_map_raw, dict):
        return None
    value_map = defaultdict(list)
    for norm, entries in value_map_raw.items():
        if not isinstance(norm, str):
            continue
        if not isinstance(entries, list):
            continue
        for pair in entries:
            if not isinstance(pair, (list, tuple)) or len(pair) < 2:
                continue
            value_map[norm].append((str(pair[0]), str(pair[1])))
    return value_map


def _save_wikidata_value_cache(path, value_map):
    try:
        tmp_path = path.with_name(path.name + f".tmp.{os.getpid()}")
        serializable = {}
        for norm, entries in (value_map or {}).items():
            if not isinstance(norm, str):
                continue
            rows = []
            for pair in entries:
                if not isinstance(pair, (list, tuple)) or len(pair) < 2:
                    continue
                rows.append([str(pair[0]), str(pair[1])])
            serializable[norm] = rows
        payload = {
            "saved_at": time.time(),
            "value_map": serializable,
        }
        with gzip.open(tmp_path, "wt", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(tmp_path, path)
        return True
    except Exception:
        try:
            if "tmp_path" in locals() and tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        return False


def fetch_wikidata_values(
    wikidata_property,
    wkd_class=None,
    wkd_prop_class=None,
    entity_iris=None,
    value_candidates=None,
):
    """Récupère les valeurs depuis Wikidata pour une propriété donnée, avec filtre de classe optionnel"""
    print_color(f"\n🌐 Récupération des valeurs Wikidata ({wikidata_property})...", Colors.BLUE)
    
    prop = normalize_wikidata_property(wikidata_property)
    phone_mode = _looks_like_phone_mode(wikidata_property)
    
    class_filter = ""
    wkd_class_norm = normalize_wkd_class(wkd_class)
    if wkd_class_norm:
        class_filter = f"""
      ?entity wdt:P31 ?type .
      ?type wdt:P279* {wkd_class_norm} .
    """
    
    if not prop and entity_iris:
        property_triple = "BIND(STR(?entity) AS ?value) ."
    else:
        property_triple = "?entity ?prop ?value ." if not prop else f"?entity {prop} ?value ."
    prop_class_filter = ""
    wkd_prop_class_norm = normalize_wkd_prop_class(wkd_prop_class)
    if wkd_prop_class_norm:
        prop_class_filter = f"""
      ?prop wdt:P31 ?propType .
      ?propType wdt:P279* {wkd_prop_class_norm} .
    """
    
    entity_iris_sorted = sorted({str(v).strip() for v in (entity_iris or []) if str(v).strip()})
    value_candidates_sorted = sorted({str(v).strip() for v in (value_candidates or []) if str(v).strip()})
    if not prop and not entity_iris_sorted:
        print_color("❌ No Wikidata entity IRIs provided (empty VALUES set).", Colors.RED)
        return {}
    if value_candidates_sorted and not prop:
        print_color("❌ value_candidates requires a Wikidata property.", Colors.RED)
        return {}

    cache_disabled = _truthy_env(os.environ.get("WIKIDATA_CACHE_DISABLED", "0"))
    cache_lang = os.environ.get("WIKIDATA_CACHE_LANG", "all")
    cache_path = _wikidata_cache_path(
        prop=prop,
        wkd_class_norm=wkd_class_norm,
        wkd_prop_class_norm=wkd_prop_class_norm,
        entity_iris=entity_iris_sorted,
        value_candidates=value_candidates_sorted,
        lang_key=cache_lang,
    )
    if not cache_disabled:
        cached_map = _load_wikidata_value_cache(cache_path)
        if cached_map is not None:
            total_entities = sum(len(entries) for entries in cached_map.values())
            print_color(
                f"💾 Cache hit: {cache_path.name} ({len(cached_map)} valeurs normalisées, {total_entities} entités)",
                Colors.GREEN,
            )
            return cached_map
    query_template = """
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX p: <http://www.wikidata.org/prop/>
    PREFIX ps: <http://www.wikidata.org/prop/statement/>
    PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
    PREFIX pr: <http://www.wikidata.org/prop/reference/>
    PREFIX wds: <http://www.wikidata.org/entity/statement/>
    PREFIX wikibase: <http://wikiba.se/ontology#>
    PREFIX schema: <http://schema.org/>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX prov: <http://www.w3.org/ns/prov#>
    
    SELECT ?entity ?value WHERE {{
      {values_filter}
      {property_triple}
      {prop_class_filter}
      {class_filter}
    }}
    """
    
    print(f"   Requête SPARQL pour {prop or '?prop'}...")

    try:
        max_attempts = max(1, int(os.environ.get("WIKIDATA_QUERY_MAX_RETRIES", "4")))
        base_delay = max(0.1, float(os.environ.get("WIKIDATA_QUERY_RETRY_DELAY", "2.0")))
        timeout_s = max(1, int(os.environ.get("WIKIDATA_QUERY_TIMEOUT", "300")))
        entity_batch_size = max(1, int(os.environ.get("WIKIDATA_ENTITY_BATCH_SIZE", "500")))
        value_batch_size = max(1, int(os.environ.get("WIKIDATA_VALUE_BATCH_SIZE", "500")))
        headers = {
            "Accept": "application/sparql-results+json",
            "User-Agent": "beam-align/1.0",
        }
        bindings = []

        # Large VALUES payloads can yield massive/truncated JSON responses; batch them.
        if entity_iris_sorted and len(entity_iris_sorted) > entity_batch_size:
            batches = list(_chunk_list(entity_iris_sorted, entity_batch_size))
            print(f"   Batching entities: {len(entity_iris_sorted):,} IRIs in {len(batches)} batches (size={entity_batch_size})")
            for idx, entity_batch in enumerate(batches, 1):
                values = " ".join(f"<{uri}>" for uri in entity_batch)
                values_filter = f"VALUES ?entity {{ {values} }}\n"
                batch_query = query_template.format(
                    values_filter=values_filter,
                    property_triple=property_triple,
                    prop_class_filter=prop_class_filter,
                    class_filter=class_filter,
                )
                print(f"   [WD] batch {idx}/{len(batches)} size={len(entity_batch)}")
                results = _run_sparql_query_with_retry(
                    batch_query,
                    headers=headers,
                    timeout_s=timeout_s,
                    max_attempts=max_attempts,
                    base_delay=base_delay,
                )
                if results and isinstance(results, dict):
                    batch_bindings = (((results.get("results") or {}).get("bindings")) or [])
                    if isinstance(batch_bindings, list):
                        bindings.extend(batch_bindings)
        elif value_candidates_sorted and len(value_candidates_sorted) > value_batch_size:
            batches = list(_chunk_list(value_candidates_sorted, value_batch_size))
            print(
                f"   Batching values: {len(value_candidates_sorted):,} values in "
                f"{len(batches)} batches (size={value_batch_size})"
            )
            for idx, value_batch in enumerate(batches, 1):
                values = " ".join(_sparql_quote_literal(v) for v in value_batch)
                values_filter = f"VALUES ?value {{ {values} }}\n"
                batch_query = query_template.format(
                    values_filter=values_filter,
                    property_triple=property_triple,
                    prop_class_filter=prop_class_filter,
                    class_filter=class_filter,
                )
                print(f"   [WD] value-batch {idx}/{len(batches)} size={len(value_batch)}")
                results = _run_sparql_query_with_retry(
                    batch_query,
                    headers=headers,
                    timeout_s=timeout_s,
                    max_attempts=max_attempts,
                    base_delay=base_delay,
                )
                if results and isinstance(results, dict):
                    batch_bindings = (((results.get("results") or {}).get("bindings")) or [])
                    if isinstance(batch_bindings, list):
                        bindings.extend(batch_bindings)
        else:
            values_filter = ""
            if entity_iris_sorted:
                values = " ".join(f"<{uri}>" for uri in entity_iris_sorted)
                values_filter = f"VALUES ?entity {{ {values} }}\n"
            elif value_candidates_sorted:
                values = " ".join(_sparql_quote_literal(v) for v in value_candidates_sorted)
                values_filter = f"VALUES ?value {{ {values} }}\n"
            query = query_template.format(
                values_filter=values_filter,
                property_triple=property_triple,
                prop_class_filter=prop_class_filter,
                class_filter=class_filter,
            )
            results = _run_sparql_query_with_retry(
                query,
                headers=headers,
                timeout_s=timeout_s,
                max_attempts=max_attempts,
                base_delay=base_delay,
            )
            if results and isinstance(results, dict):
                direct_bindings = (((results.get("results") or {}).get("bindings")) or [])
                if isinstance(direct_bindings, list):
                    bindings.extend(direct_bindings)
        if not bindings:
            return {}
        
        # {value_normalized: [(original_value, wikidata_uri), ...]}
        value_map = defaultdict(list)
        all_raw_values = set()
        
        for result in bindings:
            try:
                value = result["value"]["value"]
                entity_uri = result["entity"]["value"]
            except Exception:
                continue
            
            all_raw_values.add(value)
            
            value_normalized = normalize_value_for_matching(value, phone_mode=phone_mode)
            
            # Appliquer la normalisation des codes pays
            value_normalized = normalize_country_code(value_normalized)
            
            if value_normalized:
                value_map[value_normalized].append((value, entity_uri))
        
        print_color(f"✅ {len(all_raw_values)} valeurs brutes distinctes", Colors.GREEN)
        print_color(f"✅ {len(value_map)} valeurs normalisées distinctes", Colors.GREEN)
        
        total_entities = sum(len(entries) for entries in value_map.values())
        print_color(f"✅ {total_entities} entités Wikidata", Colors.GREEN)
        if not cache_disabled:
            if _save_wikidata_value_cache(cache_path, value_map):
                print_color(f"💾 Cache saved: {cache_path.name}", Colors.BLUE)
        
        # Exemples
        print(f"\n📋 Exemples Wikidata (5 premiers):")
        for i, (norm, entries) in enumerate(list(value_map.items())[:5]):
            orig, uri = entries[0]
            print(f"   {i+1}. '{orig}' → '{norm}' (len={len(norm)})")
        
        return value_map
        
    except Exception as e:
        print_color(f"❌ Erreur: {e}", Colors.RED)
        return {}


def fetch_target_values(
    target_property,
    target_class=None,
    target_prop_class=None,
    entity_iris=None,
    value_candidates=None,
    target_endpoint="wikidata",
    target_endpoint_url=None,
    target_prefixes=None,
    _phone_fallback_attempted=False,
):
    endpoint_key = normalize_target_endpoint_key(target_endpoint)
    if endpoint_key == "wikidata":
        return fetch_wikidata_values(
            wikidata_property=target_property,
            wkd_class=target_class,
            wkd_prop_class=target_prop_class,
            entity_iris=entity_iris,
        )

    endpoint_url = resolve_target_endpoint_url(endpoint_key, target_endpoint_url)
    if not endpoint_url:
        print_color("❌ Target endpoint URL is empty.", Colors.RED)
        return {}

    print_color(
        f"\n🌐 Récupération des valeurs target ({target_property}) via {endpoint_key}...",
        Colors.BLUE,
    )

    prop = normalize_target_property(target_property, endpoint_key)
    class_norm = normalize_target_class(target_class, endpoint_key)
    entity_iris_sorted = sorted({str(v).strip() for v in (entity_iris or []) if str(v).strip()})
    value_candidates_sorted = sorted({str(v).strip() for v in (value_candidates or []) if str(v).strip()})
    if not prop and not entity_iris_sorted:
        print_color("❌ target_property is required when no entity IRIs are provided.", Colors.RED)
        return {}
    if value_candidates_sorted and not prop:
        print_color("❌ value_candidates requires target_property.", Colors.RED)
        return {}

    if not prop and entity_iris_sorted:
        property_triple = "BIND(STR(?entity) AS ?value) ."
    else:
        property_triple = f"?entity {prop} ?value ."

    class_filter = ""
    if class_norm:
        class_filter = f"""
      ?entity rdf:type ?type .
      ?type rdfs:subClassOf* {class_norm} .
    """

    query_template = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX schema: <http://schema.org/>
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX dbp: <http://dbpedia.org/property/>
    PREFIX yago: <http://yago-knowledge.org/resource/>
    {custom_prefixes}
    SELECT ?entity ?value WHERE {{
      {values_filter}
      {property_triple}
      {class_filter}
    }}
    """
    custom_prefixes = render_prefix_declarations(target_prefixes)

    try:
        max_attempts = max(1, int(os.environ.get("TARGET_QUERY_MAX_RETRIES", "3")))
        base_delay = max(0.1, float(os.environ.get("TARGET_QUERY_RETRY_DELAY", "1.5")))
        timeout_s = max(1, int(os.environ.get("TARGET_QUERY_TIMEOUT", "120")))
        entity_batch_size = max(1, int(os.environ.get("TARGET_ENTITY_BATCH_SIZE", "500")))
        headers = {
            "Accept": "application/sparql-results+json",
            "User-Agent": "beam-align/1.0",
        }
        bindings = []
        
        def _build_values_filter(entity_batch=None, value_batch=None):
            chunks = []
            if entity_batch:
                values = " ".join(f"<{uri}>" for uri in entity_batch)
                chunks.append(f"VALUES ?entity {{ {values} }}")
            if value_batch:
                rendered = []
                for raw in value_batch:
                    value = str(raw or "").strip()
                    if not value:
                        continue
                    if _is_absolute_iri(value):
                        if value.startswith("<") and value.endswith(">"):
                            rendered.append(value)
                        else:
                            rendered.append(f"<{value}>")
                    else:
                        rendered.append(_sparql_quote_literal(value))
                values = " ".join(rendered)
                chunks.append(f"VALUES ?value {{ {values} }}")
            return ("\n".join(chunks) + "\n") if chunks else ""

        if entity_iris_sorted and len(entity_iris_sorted) > entity_batch_size:
            batches = list(_chunk_list(entity_iris_sorted, entity_batch_size))
            print(
                f"   Batching entities: {len(entity_iris_sorted):,} IRIs in {len(batches)} batches (size={entity_batch_size})"
            )
            for idx, entity_batch in enumerate(batches, 1):
                values_filter = _build_values_filter(entity_batch=entity_batch, value_batch=value_candidates_sorted)
                batch_query = query_template.format(
                    custom_prefixes=custom_prefixes,
                    values_filter=values_filter,
                    property_triple=property_triple,
                    class_filter=class_filter,
                )
                print(f"   [TARGET] batch {idx}/{len(batches)} size={len(entity_batch)}")
                results = _run_sparql_query_with_retry_to_endpoint(
                    endpoint_url,
                    query=batch_query,
                    headers=headers,
                    timeout_s=timeout_s,
                    max_attempts=max_attempts,
                    base_delay=base_delay,
                )
                if results and isinstance(results, dict):
                    batch_bindings = (((results.get("results") or {}).get("bindings")) or [])
                    if isinstance(batch_bindings, list):
                        bindings.extend(batch_bindings)
        elif value_candidates_sorted and len(value_candidates_sorted) > entity_batch_size:
            batches = list(_chunk_list(value_candidates_sorted, entity_batch_size))
            print(
                f"   Batching values: {len(value_candidates_sorted):,} values in {len(batches)} batches (size={entity_batch_size})"
            )
            for idx, value_batch in enumerate(batches, 1):
                values_filter = _build_values_filter(entity_batch=entity_iris_sorted, value_batch=value_batch)
                batch_query = query_template.format(
                    custom_prefixes=custom_prefixes,
                    values_filter=values_filter,
                    property_triple=property_triple,
                    class_filter=class_filter,
                )
                print(f"   [TARGET] value-batch {idx}/{len(batches)} size={len(value_batch)}")
                results = _run_sparql_query_with_retry_to_endpoint(
                    endpoint_url,
                    query=batch_query,
                    headers=headers,
                    timeout_s=timeout_s,
                    max_attempts=max_attempts,
                    base_delay=base_delay,
                )
                if results and isinstance(results, dict):
                    batch_bindings = (((results.get("results") or {}).get("bindings")) or [])
                    if isinstance(batch_bindings, list):
                        bindings.extend(batch_bindings)
        else:
            values_filter = _build_values_filter(entity_batch=entity_iris_sorted, value_batch=value_candidates_sorted)
            query = query_template.format(
                custom_prefixes=custom_prefixes,
                values_filter=values_filter,
                property_triple=property_triple,
                class_filter=class_filter,
            )
            results = _run_sparql_query_with_retry_to_endpoint(
                endpoint_url,
                query=query,
                headers=headers,
                timeout_s=timeout_s,
                max_attempts=max_attempts,
                base_delay=base_delay,
            )
            if results and isinstance(results, dict):
                direct_bindings = (((results.get("results") or {}).get("bindings")) or [])
                if isinstance(direct_bindings, list):
                    bindings.extend(direct_bindings)
        if not bindings:
            if (
                (not _phone_fallback_attempted)
                and _looks_like_phone_mode(target_property)
                and endpoint_key in {"dbpedia", "yago"}
            ):
                fallback_props = _target_phone_fallback_properties(endpoint_key)
                merged = defaultdict(list)
                seen_pairs = set()
                any_found = False
                for alt_prop in fallback_props:
                    alt_prop = str(alt_prop or "").strip()
                    if not alt_prop:
                        continue
                    if prop and alt_prop == prop:
                        continue
                    alt_map = fetch_target_values(
                        target_property=alt_prop,
                        target_class=target_class,
                        target_prop_class=target_prop_class,
                        entity_iris=entity_iris,
                        value_candidates=value_candidates,
                        target_endpoint=target_endpoint,
                        target_endpoint_url=target_endpoint_url,
                        target_prefixes=target_prefixes,
                        _phone_fallback_attempted=True,
                    )
                    if alt_map is None:
                        continue
                    if alt_map:
                        any_found = True
                    for norm, entries in alt_map.items():
                        for raw_value, iri in list(entries or []):
                            pair = (str(raw_value or ""), str(iri or ""))
                            if pair in seen_pairs:
                                continue
                            seen_pairs.add(pair)
                            merged[str(norm or "")].append(pair)
                if any_found:
                    print_color(
                        f"ℹ️ Phone fallback matched {sum(len(v) for v in merged.values())} value→entity pairs via alternate properties.",
                        Colors.BLUE,
                    )
                    return dict(merged)
            return {}

        phone_mode = _looks_like_phone_mode(target_property)
        value_map = defaultdict(list)
        all_raw_values = set()
        for result in bindings:
            try:
                value = result["value"]["value"]
                entity_uri = result["entity"]["value"]
            except Exception:
                continue
            all_raw_values.add(value)
            value_normalized = normalize_value_for_matching(value, phone_mode=phone_mode)
            value_normalized = normalize_country_code(value_normalized)
            if value_normalized:
                value_map[value_normalized].append((value, entity_uri))

        print_color(f"✅ {len(all_raw_values)} valeurs brutes distinctes", Colors.GREEN)
        print_color(f"✅ {len(value_map)} valeurs normalisées distinctes", Colors.GREEN)
        return value_map
    except Exception as e:
        print_color(f"❌ Erreur target endpoint: {e}", Colors.RED)
        return None


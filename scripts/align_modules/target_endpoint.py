

def normalize_prefix_declarations(prefix_text):
    text = str(prefix_text or "").strip()
    if not text:
        return []
    out = []
    seen = set()
    for raw_line in text.splitlines():
        line = str(raw_line or "").strip()
        if not line:
            continue
        m = _PREFIX_DECL_RE.match(line)
        if not m:
            continue
        name = m.group(1).strip()
        iri = m.group(2).strip()
        key = f"{name.lower()}|{iri}"
        if key in seen:
            continue
        seen.add(key)
        out.append(f"PREFIX {name}: <{iri}>")
    return out


def render_prefix_declarations(prefix_text):
    rows = normalize_prefix_declarations(prefix_text)
    if not rows:
        return ""
    return "\n".join(rows)


def extract_target_entity_iri(value, target_endpoint="wikidata", target_endpoint_url=None):
    key = normalize_target_endpoint_key(target_endpoint)
    if key == "wikidata":
        return extract_wd_entity_iri(value)
    raw = str(value or "").strip()
    if raw.startswith("<") and raw.endswith(">"):
        raw = raw[1:-1].strip()
    if not raw:
        return None
    try:
        parsed = urlparse(unquote(raw))
    except Exception:
        return None
    if parsed.scheme not in {"http", "https"}:
        return None
    return raw

def extract_wd_entity_iri(value):
    if not value:
        return None
    value = str(value).strip()
    if value.startswith("<") and value.endswith(">"):
        value = value[1:-1].strip()
    if not value:
        return None

    # Already a bare QID.
    m = re.fullmatch(r"[Qq](\d+)", value)
    if m:
        return f"http://www.wikidata.org/entity/Q{m.group(1)}"

    # Prefix form (wd:Q42).
    m = re.fullmatch(r"wd:[Qq](\d+)", value, flags=re.IGNORECASE)
    if m:
        return f"http://www.wikidata.org/entity/Q{m.group(1)}"

    try:
        parsed = urlparse(unquote(value))
    except Exception:
        parsed = None
    if not parsed or parsed.scheme not in {"http", "https"}:
        return None

    host = (parsed.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    if host.startswith("m."):
        host = host[2:]
    if host != "wikidata.org":
        return None

    path_parts = [p for p in (parsed.path or "").split("/") if p]
    for token in reversed(path_parts):
        m = re.fullmatch(r"[Qq](\d+)", token.strip())
        if m:
            return f"http://www.wikidata.org/entity/Q{m.group(1)}"

    query_map = parse_qs(parsed.query or "", keep_blank_values=False)
    for key in ("title", "entity", "id", "q"):
        for raw in query_map.get(key, []):
            m = re.fullmatch(r"[Qq](\d+)", str(raw).strip())
            if m:
                return f"http://www.wikidata.org/entity/Q{m.group(1)}"

    frag = (parsed.fragment or "").strip()
    m = re.fullmatch(r"[Qq](\d+)", frag)
    if m:
        return f"http://www.wikidata.org/entity/Q{m.group(1)}"

    return None

def discover_parts(class_name):
    """Découvre les parts disponibles pour une classe"""
    url = urljoin(WDC_BASE_URL, f"{class_name}/")
    
    print_color(f"🔍 Découverte des parts disponibles pour {class_name}...", Colors.BLUE)
    print(f"   URL: {url}")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        parts = []
        
        for link in soup.find_all('a'):
            href = link.get('href', '')
            if re.match(r'part_\d+\.gz$', href):
                parts.append(href)
        
        parts.sort(key=lambda x: int(re.search(r'\d+', x).group()))
        
        print_color(f"✅ {len(parts)} parts trouvées", Colors.GREEN)
        return parts
        
    except Exception as e:
        print_color(f"❌ Erreur: {e}", Colors.RED)
        return []

def parse_parts_spec(parts_spec, available_parts=None):
    """Parse la spécification des parts (all, 0-3, 0,1,2)"""
    if parts_spec.lower() == "all":
        return available_parts or []

    available_by_number = None
    if available_parts is not None:
        available_by_number = {}
        for part in available_parts:
            m = re.search(r'part_0*(\d+)\.gz$', str(part or ''))
            if not m:
                continue
            available_by_number[int(m.group(1))] = part

    def add_part(number):
        part_num = int(str(number).strip())
        part_file = f"part_{part_num}.gz"
        if available_by_number is not None:
            matched = available_by_number.get(part_num)
            if matched is not None:
                selected.append(matched)
        elif available_parts is None or part_file in available_parts:
            selected.append(part_file)

    selected = []

    # Range: 0-3
    if '-' in parts_spec:
        start, end = map(int, parts_spec.split('-'))
        for i in range(start, end + 1):
            add_part(i)
    
    # Liste: 0,1,2
    elif ',' in parts_spec:
        for num in parts_spec.split(','):
            add_part(num)
    
    # Single: 0
    else:
        add_part(parts_spec)
    
    return selected

def download_file(url, dest_path):
    """Télécharge un fichier avec barre de progression"""
    if _CANCEL_CHECK and _CANCEL_CHECK():
        raise RuntimeError("Cancelled")
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()
    
    total_size = int(response.headers.get('content-length', 0))
    
    try:
        with open(dest_path, 'wb') as f:
            if total_size == 0:
                f.write(response.content)
            else:
                downloaded = 0
                chunk_size = 8192
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if _CANCEL_CHECK and _CANCEL_CHECK():
                        raise RuntimeError("Cancelled")
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        percent = (downloaded / total_size) * 100
                        print(f"\r  Téléchargement: {percent:.1f}% ({downloaded}/{total_size} bytes)", end='')
                print()  # Newline après progression
    except Exception:
        try:
            if Path(dest_path).exists():
                Path(dest_path).unlink()
        except Exception:
            pass
        raise
    finally:
        try:
            response.close()
        except Exception:
            pass


def download_file_with_retries(url, dest_path, attempts=5, base_sleep=2.0):
    """Télécharge avec retries bornés pour les erreurs réseau transitoires."""
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            download_file(url, dest_path)
            return
        except Exception as e:
            if "Cancelled" in str(e):
                raise
            last_error = e
            try:
                if Path(dest_path).exists():
                    Path(dest_path).unlink()
            except Exception:
                pass
            if attempt >= attempts:
                break
            wait_s = base_sleep * attempt
            print_color(
                f"  ⚠️ Échec téléchargement ({attempt}/{attempts}): {e}. Retry dans {wait_s:.1f}s...",
                Colors.YELLOW,
            )
            time.sleep(wait_s)
    raise last_error

def _decompress_worker(gz_path, nq_path):
    gz_path = Path(gz_path)
    nq_path = Path(nq_path)
    with gzip.open(gz_path, 'rb') as f_in:
        with open(nq_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    if gz_path.exists():
        try:
            gz_path.unlink()
        except Exception:
            pass
    return str(nq_path)

def download_and_decompress(class_name, parts, work_dir, parallel_decompress=True, workers=None, lock_path=None):
    """Télécharge et décompresse les parts"""
    work_dir = Path(work_dir)
    work_dir.mkdir(exist_ok=True)
    
    decompressed_files = []
    
    print_color(f"\n📦 Téléchargement/Décompression de {len(parts)} parts...", Colors.BLUE)
    executor = None
    futures = {}
    current_workers = None
    
    for i, part_file in enumerate(parts, 1):
        if _CANCEL_CHECK and _CANCEL_CHECK():
            raise RuntimeError("Cancelled")
        print(f"\n[{i}/{len(parts)}] {part_file}")
        
        gz_path = work_dir / part_file
        nq_path = work_dir / part_file.replace('.gz', '')
        
        # Skip si déjà décompressé
        if nq_path.exists():
            size = nq_path.stat().st_size / (1024**2)  # MB
            print_color(f"  ✅ Déjà disponible ({size:.1f} MB)", Colors.GREEN)
            if gz_path.exists():
                try:
                    gz_path.unlink()
                    print_color("  🧹 .gz supprimé (déjà décompressé)", Colors.GREEN)
                except Exception:
                    pass
            decompressed_files.append(nq_path)
            continue
        
        # Download si nécessaire
        if not gz_path.exists():
            url = urljoin(WDC_BASE_URL, f"{class_name}/{part_file}")
            print(f"  ⬇️  Téléchargement depuis {url}")
            try:
                download_file_with_retries(url, gz_path)
                size = gz_path.stat().st_size / (1024**2)
                print_color(f"  ✅ Téléchargé ({size:.1f} MB)", Colors.GREEN)
            except Exception as e:
                if "Cancelled" in str(e):
                    raise
                print_color(f"  ❌ Erreur: {e}", Colors.RED)
                continue
        else:
            size = gz_path.stat().st_size / (1024**2)
            print_color(f"  ✅ Déjà téléchargé ({size:.1f} MB)", Colors.GREEN)
        
        # Décompresser (parallèle si activé)
        print("  📂 Décompression...")
        try:
            if parallel_decompress:
                if lock_path:
                    desired_workers, _runs, _cpu = get_shared_workers(
                        lock_path, share=ALIGN_CPU_SHARE, override=workers
                    )
                else:
                    desired_workers = workers or 1
                
                if executor is None or (not futures and desired_workers != current_workers):
                    if executor:
                        executor.shutdown(wait=True)
                    executor = ProcessPoolExecutor(max_workers=desired_workers)
                    current_workers = desired_workers
                
                fut = executor.submit(_decompress_worker, str(gz_path), str(nq_path))
                futures[fut] = (part_file, gz_path, nq_path)
            else:
                _decompress_worker(str(gz_path), str(nq_path))
                size = nq_path.stat().st_size / (1024**2)
                print_color(f"  ✅ Décompressé ({size:.1f} MB)", Colors.GREEN)
                decompressed_files.append(nq_path)
        except Exception as e:
            if "Cancelled" in str(e):
                raise
            print_color(f"  ❌ Erreur décompression: {e}", Colors.RED)
    
    if executor:
        for fut in as_completed(futures):
            part_file, gz_path, nq_path = futures[fut]
            try:
                nq_path_str = fut.result()
                nq_path = Path(nq_path_str)
                size = nq_path.stat().st_size / (1024**2)
                print_color(f"  ✅ Décompressé ({nq_path.name}, {size:.1f} MB)", Colors.GREEN)
                decompressed_files.append(nq_path)
            except Exception as e:
                if "Cancelled" in str(e):
                    raise
                print_color(f"  ❌ Erreur décompression: {e}", Colors.RED)
                # Auto-heal once: remove broken artifacts, re-download and decompress this part.
                try:
                    if nq_path.exists():
                        nq_path.unlink()
                except Exception:
                    pass
                try:
                    if gz_path.exists():
                        gz_path.unlink()
                except Exception:
                    pass
                try:
                    url = urljoin(WDC_BASE_URL, f"{class_name}/{part_file}")
                    print(f"  🔁 Retry download: {url}")
                    download_file_with_retries(url, gz_path, attempts=3, base_sleep=2.0)
                    _decompress_worker(str(gz_path), str(nq_path))
                    size = nq_path.stat().st_size / (1024**2)
                    print_color(f"  ✅ Retry OK ({nq_path.name}, {size:.1f} MB)", Colors.GREEN)
                    decompressed_files.append(nq_path)
                except Exception as retry_e:
                    if "Cancelled" in str(retry_e):
                        raise
                    print_color(f"  ❌ Retry décompression échouée: {retry_e}", Colors.RED)
        executor.shutdown(wait=True)
    
    return decompressed_files

def _filter_file_worker(args):
    file_path, prepared_patterns, tmp_dir, collect_top_props = args
    file_path = Path(file_path)
    tmp_dir = Path(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_out = tmp_dir / f"{file_path.name}.filtered"
    
    file_lines = 0
    file_matched = 0
    predicates_found = defaultdict(int) if collect_top_props else None
    
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as in_f:
        with open(tmp_out, 'w', encoding='utf-8') as out_f:
            for line in in_f:
                file_lines += 1
                predicates = re.findall(r'<([^>]+)>', line)
                if len(predicates) >= 1:
                    predicate = predicates[0]
                    if collect_top_props:
                        predicates_found[predicate] += 1
                    if predicate_matches_prepared_patterns(predicate, prepared_patterns):
                        out_f.write(line)
                        file_matched += 1
    
    return {
        "file": str(file_path),
        "tmp": str(tmp_out),
        "lines": file_lines,
        "matched": file_matched,
        "predicates": predicates_found or {},
    }

def filter_by_pattern(files, pattern, output_file, collect_top_props=False, top_n=100, parallel=True, workers=None):
    """
    Filtre les lignes dont le PRÉDICAT contient le pattern
    Équivalent à: ?x <...pattern...> ?value
    """
    print_color(f"\n🔍 Filtrage par pattern dans les PRÉDICATS: '{pattern}'", Colors.BLUE)
    print("   Recherche: <predicate> qui contient le pattern (case-insensitive)")
    
    prepared_patterns = prepare_predicate_patterns(pattern)
    if not prepared_patterns:
        raise ValueError("Empty predicate pattern")
    if len(prepared_patterns) == 1:
        pattern_normalized, pattern_raw, _ = prepared_patterns[0]
        if pattern_raw:
            print(f"   Pattern brut: '{pattern}'")
        else:
            print(f"   Pattern normalisé: '{pattern_normalized}'")
    else:
        print(f"   Patterns (OR): {', '.join(t for _, _, t in prepared_patterns)}")
    
    total_lines = 0
    matched_lines = 0
    predicates_found = defaultdict(int) if collect_top_props else None
    
    files = [Path(p) for p in files]
    do_parallel = parallel and len(files) > 1
    
    if do_parallel:
        tmp_dir = output_file.parent / f".tmp_filter_{output_file.stem}"
        tmp_dir.mkdir(parents=True, exist_ok=True)

        print_color(f"\n⚙️  Filtrage parallèle ({len(files)} fichiers, workers={workers or 1})...", Colors.BLUE)
        tasks = [(str(p), prepared_patterns, str(tmp_dir), collect_top_props) for p in files]
        results = {}
        with ProcessPoolExecutor(max_workers=workers) as ex:
            future_map = {ex.submit(_filter_file_worker, t): t[0] for t in tasks}
            for fut in as_completed(future_map):
                res = fut.result()
                results[res["file"]] = res
                print(f"  ✅ {Path(res['file']).name}: {res['matched']:,} matches")
        
        # Concaténer dans l'ordre des fichiers d'entrée
        with open(output_file, 'w', encoding='utf-8') as out_f:
            for file_path in files:
                res = results[str(file_path)]
                total_lines += res["lines"]
                matched_lines += res["matched"]
                if collect_top_props and predicates_found is not None:
                    for pred, cnt in res["predicates"].items():
                        predicates_found[pred] += cnt
                with open(res["tmp"], 'r', encoding='utf-8', errors='ignore') as in_f:
                    shutil.copyfileobj(in_f, out_f)
        
        shutil.rmtree(tmp_dir, ignore_errors=True)
    else:
        with open(output_file, 'w', encoding='utf-8') as out_f:
            for file_path in files:
                print(f"\n  📄 Traitement: {file_path.name}")
                file_lines = 0
                file_matched = 0
                
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as in_f:
                    for line in in_f:
                        file_lines += 1
                        total_lines += 1
                        
                        # Le prédicat est TOUJOURS le premier <...> dans NQuads
                        # Format: (sujet_ou_blanknode) <predicate> (objet) <graph> .
                        predicates = re.findall(r'<([^>]+)>', line)
                        
                        if len(predicates) >= 1:
                            predicate = predicates[0]  # Premier <...> = prédicat
                            if collect_top_props:
                                predicates_found[predicate] += 1
                            # Match si un pattern match le prédicat (OR)
                            if predicate_matches_prepared_patterns(predicate, prepared_patterns):
                                out_f.write(line)
                                matched_lines += 1
                                file_matched += 1
                        
                        if file_lines % 100000 == 0:
                            print(f"\r    Lignes: {file_lines:,} | Matches: {file_matched:,}", end='')
                
                print(f"\r    Lignes: {file_lines:,} | Matches: {file_matched:,}")
                percent = (file_matched / file_lines * 100) if file_lines > 0 else 0
                print(f"    Taux: {percent:.2f}%")
    
    print_color(f"\n✅ Filtrage terminé", Colors.GREEN)
    print(f"   Total lignes traitées: {total_lines:,}")
    print(f"   Lignes matchées: {matched_lines:,}")
    if total_lines > 0:
        print(f"   Taux global: {(matched_lines/total_lines*100):.2f}%")
    
    # Afficher les prédicats trouvés (top N)
    if collect_top_props and predicates_found is not None:
        print(f"\n📋 Prédicats trouvés (top {top_n}):")
        for pred, count in sorted(predicates_found.items(), key=lambda x: -x[1])[:top_n]:
            print(f"   {count:>8} × {pred}")
    
    return matched_lines


def _extract_batch(lines):
    value_map = defaultdict(list)
    all_raw_values = set()
    all_iris = set()
    country_code_changes = defaultdict(int)
    line_count = 0
    for line in lines:
        line_count += 1
        match = re.match(r'(\S+)\s+<([^>]+)>\s+"([^"]+)"', line)
        if match:
            subject = match.group(1)
            value = match.group(3)
            all_raw_values.add(value)
            all_iris.add(subject)
            value_normalized = normalize_for_matching(value)
            value_normalized_original = value_normalized
            value_normalized = normalize_country_code(value_normalized)
            if value_normalized != value_normalized_original:
                old_code = value_normalized_original[:2]
                new_code = value_normalized[:2]
                country_code_changes[f"{old_code.upper()}→{new_code.upper()}"] += 1
            if value_normalized:
                value_map[value_normalized].append((value, subject))
    return value_map, all_raw_values, all_iris, country_code_changes, line_count

def _extract_batch_with_pattern(args):
    lines, prepared_patterns, collect_top_props, wdc_value_is_wd_iri, phone_mode, search_in = args
    search_mode = _normalize_search_in_mode(search_in)
    value_map = defaultdict(list)
    all_raw_values = set()
    all_iris = set()
    country_code_changes = defaultdict(int)
    predicates_found = defaultdict(int) if collect_top_props else None
    line_count = 0
    matched_count = 0
    for line in lines:
        line_count += 1
        subject, predicate_tok, obj_tok = _extract_spo_tokens(line)
        if not (subject and predicate_tok and obj_tok):
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

        matched_count += 1

        value_for_norm = value
        if wdc_value_is_wd_iri:
            wd_iri = extract_wd_entity_iri(value)
            if not wd_iri:
                continue
            value_for_norm = wd_iri
        elif search_mode != "value" and not obj_tok.startswith('"'):
            # In non-Wikidata mode, only literal values should be aligned.
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
    return value_map, all_raw_values, all_iris, country_code_changes, line_count, matched_count, (predicates_found or {})

def extract_unique_iris(filtered_file, parallel=True, workers=None, batch_size=200000):
    """
    Extrait les valeurs distinctes (comme COUNT(DISTINCT ?value))
    Returns: {value_normalized: [(original_value, wdc_iri), ...]}
    """
    print_color(f"\n📊 Extraction des valeurs distinctes (équivalent SPARQL)...", Colors.BLUE)
    
    # {value_normalized: [(original_value, wdc_iri), ...]}
    value_map = defaultdict(list)
    all_raw_values = set()
    all_iris = set()
    country_code_changes = defaultdict(int)
    
    line_count = 0
    if parallel:
        n_workers = workers or 1
        futures = []
        buffer = []
        with ProcessPoolExecutor(max_workers=n_workers) as ex:
            with open(filtered_file, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    buffer.append(line)
                    if len(buffer) >= batch_size:
                        futures.append(ex.submit(_extract_batch, buffer))
                        buffer = []
                if buffer:
                    futures.append(ex.submit(_extract_batch, buffer))
            for fut in as_completed(futures):
                vmap, raw_vals, iris, cc_changes, lines = fut.result()
                line_count += lines
                all_raw_values.update(raw_vals)
                all_iris.update(iris)
                for k, v in cc_changes.items():
                    country_code_changes[k] += v
                for norm, entries in vmap.items():
                    value_map[norm].extend(entries)
                if line_count % 10000 == 0:
                    print(f"\r  Lignes: {line_count:,} | Valeurs distinctes: {len(all_raw_values)} | IRIs: {len(all_iris)}", end='')
    else:
        with open(filtered_file, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                line_count += 1
                
                # Parse NQuads: <subj> <pred> "value" <graph>
                # ou: _:blanknode <pred> "value" <graph>
                match = re.match(r'(\S+)\s+<([^>]+)>\s+"([^"]+)"', line)
                
                if match:
                    subject = match.group(1)
                    predicate = match.group(2)
                    value = match.group(3)
                    
                    all_raw_values.add(value)
                    all_iris.add(subject)
                    
                    # Normaliser la valeur
                    value_normalized = normalize_for_matching(value)
                    
                    # Appliquer la normalisation des codes pays
                    value_normalized_original = value_normalized
                    value_normalized = normalize_country_code(value_normalized)
                    
                    # Tracker les changements
                    if value_normalized != value_normalized_original:
                        old_code = value_normalized_original[:2]
                        new_code = value_normalized[:2]
                        country_code_changes[f"{old_code.upper()}→{new_code.upper()}"] += 1
                    
                    if value_normalized:
                        value_map[value_normalized].append((value, subject))
                
                if line_count % 10000 == 0:
                    print(f"\r  Lignes: {line_count:,} | Valeurs distinctes: {len(all_raw_values)} | IRIs: {len(all_iris)}", end='')
    
    print(f"\r  Lignes: {line_count:,} | Valeurs distinctes: {len(all_raw_values)} | IRIs: {len(all_iris)}")
    
    # Afficher les changements de codes pays
    if country_code_changes:
        print(f"\n🌍 Normalisation des codes pays:")
        for change, count in sorted(country_code_changes.items(), key=lambda x: -x[1]):
            print(f"   {change}: {count} valeurs")
    
    # Statistiques comme SPARQL
    print_color(f"\n📈 Statistiques (équivalent requêtes SPARQL):", Colors.CYAN)
    print(f"   Lignes totales (triplets):           {line_count:,}")
    print(f"   IRIs distincts (?songWdc):           {len(all_iris):,}")
    print(f"   Valeurs brutes distinctes (?value):  {len(all_raw_values):,}")
    print(f"   Valeurs normalisées:                 {len(value_map):,}")
    
    # Distribution des longueurs
    lengths = defaultdict(int)
    for norm_val in value_map:
        lengths[len(norm_val)] += 1
    
    print(f"\n📏 Distribution des longueurs (normalisées):")
    for length in sorted(lengths.keys())[:10]:  # Top 10
        print(f"   {length:>2} chars: {lengths[length]:>6} valeurs")
    
    # Exemples
    print(f"\n📋 Exemples de valeurs (5 premiers):")
    for i, (norm, entries) in enumerate(list(value_map.items())[:5]):
        orig, iri = entries[0]
        # Tronquer les valeurs trop longues
        orig_display = orig if len(orig) <= 50 else orig[:47] + "..."
        print(f"   {i+1}. '{orig_display}'")
        print(f"      → '{norm}' (len={len(norm)})")
    
    return value_map

def _process_extract_window(
    window_batches,
    prepared_patterns,
    collect_top_props,
    wdc_value_is_wd_iri,
    phone_mode,
    search_in,
    executor,
):
    futures = [
        executor.submit(
            _extract_batch_with_pattern,
            (b, prepared_patterns, collect_top_props, wdc_value_is_wd_iri, phone_mode, search_in),
        )
        for b in window_batches
    ]
    for fut in as_completed(futures):
        vmap, raw_vals, iris, cc_changes, lines, matched, preds = fut.result()
        yield vmap, raw_vals, iris, cc_changes, lines, matched, preds

def extract_unique_iris_from_graph(
    graph_file,
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
    Scanne un fichier NQuads complet, filtre par pattern de prédicat,
    et extrait les valeurs distinctes sans générer de fichier filtré.
    """
    print_color(f"\n📊 Extraction directe depuis le graphe (sans fichier filtré)...", Colors.BLUE)
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
    allowed_subjects = collect_allowed_subjects_by_type([graph_file], type_filter_iris, progress_every=progress_every)
    if allowed_subjects is not None and len(allowed_subjects) == 0:
        print_color("❌ Aucun sujet ne matche le rdf:type demandé", Colors.RED)
        return {}, 0
    
    total_bytes = Path(graph_file).stat().st_size
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
        bytes_read = 0
        with ProcessPoolExecutor(max_workers=n_workers) as ex:
            with open(graph_file, 'r', encoding='utf-8', errors='ignore') as f:
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
                        done_bytes = bytes_read
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
                                done_bytes = bytes_read
                                prog = _progress_line(start_ts, done_bytes, total_bytes)
                                print(f"\r  Lignes lues: {total_lines:,} | Matches: {matched_lines:,} | Valeurs distinctes: {len(all_raw_values)} | {prog}", end='', flush=True)
                        window_batches = []

                # Reste
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
                            done_bytes = bytes_read
                            prog = _progress_line(start_ts, done_bytes, total_bytes)
                            print(f"\r  Lignes lues: {total_lines:,} | Matches: {matched_lines:,} | Valeurs distinctes: {len(all_raw_values)} | {prog}", end='', flush=True)
    else:
        bytes_read = 0
        with open(graph_file, 'r', encoding='utf-8', errors='ignore') as f:
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
                    done_bytes = bytes_read
                    prog = _progress_line(start_ts, done_bytes, total_bytes)
                    print(f"\r  Lignes lues: {total_lines:,} | Matches: {matched_lines:,} | Valeurs distinctes: {len(all_raw_values)} | {prog}", end='', flush=True)
    
    if progress_every:
        done_bytes = total_bytes
        prog = _progress_line(start_ts, done_bytes, total_bytes)
        print(f"\r  Lignes lues: {total_lines:,} | Matches: {matched_lines:,} | Valeurs distinctes: {len(all_raw_values)} | {prog}", end='', flush=True)
    print(f"\r  Lignes: {total_lines:,} | Matches: {matched_lines:,} | Valeurs distinctes: {len(all_raw_values)}")
    
    if progress_every:
        done_bytes = total_bytes
        prog = _progress_line(start_ts, done_bytes, total_bytes)
        print(f"\r  Lignes lues: {total_lines:,} | Matches: {matched_lines:,} | Valeurs distinctes: {len(all_raw_values)} | {prog}", end='', flush=True)
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
        print_top_props(
            predicates_found,
            top_n=top_n,
            title=f"\n📋 Prédicats trouvés (top {top_n}):",
            output_file=top_props_file,
        )
    
    return value_map, matched_lines

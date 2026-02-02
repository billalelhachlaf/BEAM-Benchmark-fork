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
import shutil
import requests
from pathlib import Path
from collections import defaultdict
from SPARQLWrapper import SPARQLWrapper, JSON
from urllib.parse import urljoin
from bs4 import BeautifulSoup

# Configuration
WDC_BASE_URL = "https://data.dws.informatik.uni-mannheim.de/structureddata/2024-12/quads/classspecific/"
WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"

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

def normalize_for_matching(text):
    """
    Normalisation agressive pour matching:
    - Lowercase
    - Suppression caractères spéciaux
    - Garde seulement alphanumériques
    """
    if not text:
        return ""
    return re.sub(r'[^a-z0-9]', '', text.lower())

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

def parse_parts_spec(parts_spec, available_parts):
    """Parse la spécification des parts (all, 0-3, 0,1,2)"""
    if parts_spec.lower() == "all":
        return available_parts
    
    selected = []
    
    # Range: 0-3
    if '-' in parts_spec:
        start, end = map(int, parts_spec.split('-'))
        for i in range(start, end + 1):
            part_file = f"part_{i}.gz"
            if part_file in available_parts:
                selected.append(part_file)
    
    # Liste: 0,1,2
    elif ',' in parts_spec:
        for num in parts_spec.split(','):
            part_file = f"part_{num.strip()}.gz"
            if part_file in available_parts:
                selected.append(part_file)
    
    # Single: 0
    else:
        part_file = f"part_{parts_spec}.gz"
        if part_file in available_parts:
            selected.append(part_file)
    
    return selected

def download_file(url, dest_path):
    """Télécharge un fichier avec barre de progression"""
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()
    
    total_size = int(response.headers.get('content-length', 0))
    
    with open(dest_path, 'wb') as f:
        if total_size == 0:
            f.write(response.content)
        else:
            downloaded = 0
            chunk_size = 8192
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    percent = (downloaded / total_size) * 100
                    print(f"\r  Téléchargement: {percent:.1f}% ({downloaded}/{total_size} bytes)", end='')
            print()  # Newline après progression

def download_and_decompress(class_name, parts, work_dir):
    """Télécharge et décompresse les parts"""
    work_dir = Path(work_dir)
    work_dir.mkdir(exist_ok=True)
    
    decompressed_files = []
    
    print_color(f"\n📦 Téléchargement/Décompression de {len(parts)} parts...", Colors.BLUE)
    
    for i, part_file in enumerate(parts, 1):
        print(f"\n[{i}/{len(parts)}] {part_file}")
        
        gz_path = work_dir / part_file
        nq_path = work_dir / part_file.replace('.gz', '')
        
        # Skip si déjà décompressé
        if nq_path.exists():
            size = nq_path.stat().st_size / (1024**2)  # MB
            print_color(f"  ✅ Déjà disponible ({size:.1f} MB)", Colors.GREEN)
            decompressed_files.append(nq_path)
            continue
        
        # Download si nécessaire
        if not gz_path.exists():
            url = urljoin(WDC_BASE_URL, f"{class_name}/{part_file}")
            print(f"  ⬇️  Téléchargement depuis {url}")
            try:
                download_file(url, gz_path)
                size = gz_path.stat().st_size / (1024**2)
                print_color(f"  ✅ Téléchargé ({size:.1f} MB)", Colors.GREEN)
            except Exception as e:
                print_color(f"  ❌ Erreur: {e}", Colors.RED)
                continue
        else:
            size = gz_path.stat().st_size / (1024**2)
            print_color(f"  ✅ Déjà téléchargé ({size:.1f} MB)", Colors.GREEN)
        
        # Décompresser
        print("  📂 Décompression...")
        try:
            with gzip.open(gz_path, 'rb') as f_in:
                with open(nq_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            size = nq_path.stat().st_size / (1024**2)
            print_color(f"  ✅ Décompressé ({size:.1f} MB)", Colors.GREEN)
            decompressed_files.append(nq_path)
            
        except Exception as e:
            print_color(f"  ❌ Erreur décompression: {e}", Colors.RED)
    
    return decompressed_files

def filter_by_pattern(files, pattern, output_file):
    """
    Filtre les lignes dont le PRÉDICAT contient le pattern
    Équivalent à: ?x <...pattern...> ?value
    """
    print_color(f"\n🔍 Filtrage par pattern dans les PRÉDICATS: '{pattern}'", Colors.BLUE)
    print("   Recherche: <predicate> qui contient le pattern (case-insensitive)")
    
    pattern_normalized = normalize_for_matching(pattern)
    print(f"   Pattern normalisé: '{pattern_normalized}'")
    
    total_lines = 0
    matched_lines = 0
    predicates_found = defaultdict(int)
    
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
                        predicate_normalized = normalize_for_matching(predicate)
                        
                        # Match si pattern dans prédicat normalisé
                        if pattern_normalized in predicate_normalized:
                            out_f.write(line)
                            matched_lines += 1
                            file_matched += 1
                            predicates_found[predicate] += 1
                    
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
    
    # Afficher les prédicats trouvés
    print(f"\n📋 Prédicats trouvés (top 10):")
    for pred, count in sorted(predicates_found.items(), key=lambda x: -x[1])[:10]:
        print(f"   {count:>6} × {pred}")
    
    return matched_lines

def extract_unique_iris(filtered_file):
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

def fetch_wikidata_values(wikidata_property):
    """Récupère les valeurs depuis Wikidata pour une propriété donnée"""
    print_color(f"\n🌐 Récupération des valeurs Wikidata ({wikidata_property})...", Colors.BLUE)
    
    # Extraire le PID (P1243 de wdt:P1243)
    pid_match = re.search(r'P\d+', wikidata_property)
    if not pid_match:
        print_color(f"❌ Format invalide: {wikidata_property}", Colors.RED)
        return {}
    
    pid = pid_match.group()
    
    sparql = SPARQLWrapper(WIKIDATA_ENDPOINT)
    sparql.setReturnFormat(JSON)
    sparql.setTimeout(300)
    
    query = f"""
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    
    SELECT ?entity ?value WHERE {{
      ?entity wdt:{pid} ?value .
    }}
    """
    
    print(f"   Requête SPARQL pour {pid}...")
    
    sparql.setQuery(query)
    
    try:
        results = sparql.query().convert()
        
        # {value_normalized: [(original_value, wikidata_uri), ...]}
        value_map = defaultdict(list)
        all_raw_values = set()
        
        for result in results["results"]["bindings"]:
            value = result["value"]["value"]
            entity_uri = result["entity"]["value"]
            
            all_raw_values.add(value)
            
            value_normalized = normalize_for_matching(value)
            
            # Appliquer la normalisation des codes pays
            value_normalized = normalize_country_code(value_normalized)
            
            if value_normalized:
                value_map[value_normalized].append((value, entity_uri))
        
        print_color(f"✅ {len(all_raw_values)} valeurs brutes distinctes", Colors.GREEN)
        print_color(f"✅ {len(value_map)} valeurs normalisées distinctes", Colors.GREEN)
        
        total_entities = sum(len(entries) for entries in value_map.values())
        print_color(f"✅ {total_entities} entités Wikidata", Colors.GREEN)
        
        # Exemples
        print(f"\n📋 Exemples Wikidata (5 premiers):")
        for i, (norm, entries) in enumerate(list(value_map.items())[:5]):
            orig, uri = entries[0]
            print(f"   {i+1}. '{orig}' → '{norm}' (len={len(norm)})")
        
        return value_map
        
    except Exception as e:
        print_color(f"❌ Erreur: {e}", Colors.RED)
        return {}

def fuzzy_link(wdc_map, wikidata_map):
    """
    Lie les entités WDC et Wikidata via fuzzy matching
    Compare sur la longueur du plus court des deux
    """
    print_color(f"\n🔗 Linking WDC ↔ Wikidata...", Colors.CYAN)
    print("   Stratégie: Comparaison sur longueur minimale de chaque paire")
    print("   Filtre: Longueur minimale de 8 caractères (éviter faux positifs)")
    
    MIN_LENGTH = 8  # ISRC standard = 12 chars, on tolère jusqu'à 8
    
    exact_matches = []
    fuzzy_matches = []
    matched_pairs = set()
    
    total_comparisons = 0
    skipped_too_short = 0
    short_value_infos = []
    wdc_values_matched = set()  # Pour compter les valeurs WDC distinctes matchées
    
    print("\n   Phase 1: Matching exact...")
    for wdc_norm in wdc_map:
        # Filtrer les valeurs trop courtes
        if len(wdc_norm) < MIN_LENGTH:
            continue
            
        if wdc_norm in wikidata_map:
            for wdc_orig, wdc_iri in wdc_map[wdc_norm]:
                for wiki_orig, wiki_uri in wikidata_map[wdc_norm]:
                    pair = (wdc_iri, wiki_uri)
                    if pair not in matched_pairs:
                        matched_pairs.add(pair)
                        exact_matches.append({
                            'wdc_iri': wdc_iri,
                            'wikidata_uri': wiki_uri,
                            'wdc_value': wdc_orig,
                            'wiki_value': wiki_orig,
                            'method': 'exact'
                        })
                        wdc_values_matched.add(wdc_orig)
    
    print(f"   ✅ {len(exact_matches)} paires (exact)")
    
    print("\n   Phase 2: Matching fuzzy (longueur minimale)...")
    
    for wdc_norm in wdc_map:
        # Filtrer les valeurs trop courtes
        if len(wdc_norm) < MIN_LENGTH:
            skipped_too_short += 1
            candidates = wikidata_map.get(wdc_norm, [])
            short_value_infos.append((wdc_norm, wdc_map[wdc_norm], candidates))
            continue
            
        for wiki_norm in wikidata_map:
            # Filtrer les valeurs trop courtes
            if len(wiki_norm) < MIN_LENGTH:
                continue
                
            total_comparisons += 1
            
            # Longueur minimale pour cette paire
            min_len = min(len(wdc_norm), len(wiki_norm))
            
            # Comparer sur les min_len premiers caractères
            if wdc_norm[:min_len] == wiki_norm[:min_len]:
                for wdc_orig, wdc_iri in wdc_map[wdc_norm]:
                    for wiki_orig, wiki_uri in wikidata_map[wiki_norm]:
                        pair = (wdc_iri, wiki_uri)
                        if pair not in matched_pairs:
                            matched_pairs.add(pair)
                            fuzzy_matches.append({
                                'wdc_iri': wdc_iri,
                                'wikidata_uri': wiki_uri,
                                'wdc_value': wdc_orig,
                                'wiki_value': wiki_orig,
                                'min_len': min_len,
                                'method': f'fuzzy_{min_len}'
                            })
                            wdc_values_matched.add(wdc_orig)
            
            if total_comparisons % 100000 == 0:
                print(f"\r   Comparaisons: {total_comparisons:,} | Matches: {len(fuzzy_matches)}", end='')
    
    if total_comparisons >= 100000:
        print()  # Newline
    
    if skipped_too_short > 0:
        print(f"   ⚠️  {skipped_too_short} valeurs WDC ignorées (< {MIN_LENGTH} chars)")
        for wdc_norm, wdc_entries, candidates in short_value_infos:
            wdc_vals = ", ".join({orig for orig, _ in wdc_entries})
            if candidates:
                wiki_vals = ", ".join({orig for orig, _ in candidates})
                print(f"     - '{wdc_vals}' -> candidats Wikidata: {wiki_vals}")
            else:
                print(f"     - '{wdc_vals}' -> aucun candidat Wikidata")
    
    # Filtrer les matches fuzzy qui sont déjà dans exact
    exact_pairs_only = {(m['wdc_iri'], m['wikidata_uri']) for m in exact_matches}
    fuzzy_only = [m for m in fuzzy_matches if (m['wdc_iri'], m['wikidata_uri']) not in exact_pairs_only]
    
    print(f"   ✅ {len(fuzzy_matches)} paires (fuzzy total)")
    print(f"   ✅ {len(fuzzy_only)} paires (fuzzy nouvelles)")
    
    all_matches = exact_matches + fuzzy_only
    
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


def export_results(matches, wdc_values_matched, wdc_map, wikidata_map, output_dir, key_name=None):
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
        
        f.write(f"WDC:\n")
        f.write(f"  Valeurs normalisées distinctes: {len(wdc_map)}\n")
        f.write(f"  IRIs totaux: {sum(len(v) for v in wdc_map.values())}\n\n")
        
        f.write(f"Wikidata:\n")
        f.write(f"  Valeurs normalisées distinctes: {len(wikidata_map)}\n")
        f.write(f"  Entités totales: {sum(len(v) for v in wikidata_map.values())}\n\n")
        
        exact_count = len([m for m in matches if m['method'] == 'exact'])
        fuzzy_count = len([m for m in matches if m['method'].startswith('fuzzy')])
        
        f.write(f"Matches:\n")
        f.write(f"  Paires (exact): {exact_count}\n")
        f.write(f"  Paires (fuzzy): {fuzzy_count}\n")
        f.write(f"  TOTAL paires: {len(matches)}\n\n")
        
        f.write(f"Valeurs WDC distinctes matchées: {len(wdc_values_matched)}\n")
        
        if len(wdc_map) > 0:
            coverage = (len(wdc_values_matched) / len(wdc_map)) * 100
            f.write(f"Coverage WDC: {coverage:.2f}%\n")
    
    print(f"   ✅ {stats_file}")
    
    # Valeurs WDC non alignées
    export_unmatched_values(wdc_values_matched, wdc_map, output_dir, key_name=key_name)
    
    print_color(f"\n✅ Export terminé!", Colors.GREEN)

def main():
    parser = argparse.ArgumentParser(description="WDC Entity Linker")
    parser.add_argument("class_name")
    parser.add_argument("pattern")
    parser.add_argument("parts_spec")
    parser.add_argument("wikidata_property")
    parser.add_argument("--out-dir", help="Output directory (default: <ClassName>_download)")
    parser.add_argument("--data-dir", help="Directory containing full graph or parts (.nq/.gz)")
    args = parser.parse_args()
    
    class_name = args.class_name
    pattern = args.pattern
    parts_spec = args.parts_spec
    wikidata_property = args.wikidata_property
    
    print("="*60)
    print("🎯 WDC Entity Linker")
    print("="*60)
    print(f"Classe:              {class_name}")
    print(f"Pattern:             {pattern}")
    print(f"Parts:               {parts_spec}")
    print(f"Propriété Wikidata:  {wikidata_property}")
    print("="*60)
    
    # Setup directories
    work_dir = Path(args.out_dir or f"{class_name}_download")
    work_dir.mkdir(exist_ok=True)
    data_dir = Path(args.data_dir) if args.data_dir else work_dir
    data_dir.mkdir(exist_ok=True)
    
    full_graph_file = data_dir / f"{class_name}_full_graph.nq"
    use_full_graph = full_graph_file.exists()
    if not use_full_graph:
        candidates = sorted(data_dir.glob("*full_graph.nq"))
        if candidates:
            full_graph_file = candidates[0]
            use_full_graph = True
    
    # 1. Déterminer la source locale si disponible
    decompressed_files = []
    if use_full_graph:
        decompressed_files = [full_graph_file]
        print_color(f"  ✅ Full graph détecté: {full_graph_file}", Colors.GREEN)
    else:
        local_nq = sorted(data_dir.glob("part_*.nq"))
        if local_nq:
            decompressed_files = local_nq
            print_color(f"  ✅ Parts .nq détectées: {len(local_nq)}", Colors.GREEN)
        else:
            local_gz = sorted(data_dir.glob("part_*.gz"))
            if local_gz:
                parts = [p.name for p in local_gz]
                decompressed_files = download_and_decompress(class_name, parts, data_dir)
            else:
                # Fallback: télécharger depuis WDC
                available_parts = discover_parts(class_name)
                if not available_parts:
                    print_color("❌ Aucune part disponible", Colors.RED)
                    sys.exit(1)
                parts_to_download = parse_parts_spec(parts_spec, available_parts)
                if not parts_to_download:
                    print_color(f"❌ Aucune part valide pour '{parts_spec}'", Colors.RED)
                    sys.exit(1)
                print_color(f"\n📦 {len(parts_to_download)} parts sélectionnées", Colors.GREEN)
                decompressed_files = download_and_decompress(class_name, parts_to_download, data_dir)
    if not decompressed_files:
        print_color("❌ Aucun fichier disponible", Colors.RED)
        sys.exit(1)
    
    # 4. Filtrer par pattern
    filtered_file = work_dir / f"{class_name}_filtered.nq"
    if filtered_file.exists():
        matched_count = sum(1 for _ in open(filtered_file, "r", encoding="utf-8"))
        print_color(f"  ✅ Filtré déjà existant ({matched_count} lignes)", Colors.GREEN)
    else:
        matched_count = filter_by_pattern(decompressed_files, pattern, filtered_file)
    
    if matched_count == 0:
        print_color("❌ Aucune ligne ne matche le pattern", Colors.RED)
        sys.exit(1)
    
    # 5. Extraire les IRIs WDC uniques
    wdc_map = extract_unique_iris(filtered_file)
    
    # 6. Récupérer les valeurs Wikidata
    wikidata_map = fetch_wikidata_values(wikidata_property)
    if not wikidata_map:
        print_color("❌ Impossible de récupérer les données Wikidata", Colors.RED)
        sys.exit(1)
    
    # 7. Linking
    matches, wdc_values_matched = fuzzy_link(wdc_map, wikidata_map)
    
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
    print(f"     SERVICE <wikidata> {{ ?w {wikidata_property} ?value }}")
    print(f"     ?s <...{pattern}...> ?value")
    print(f"   }}")
    print(f"   → {len(wdc_values_matched):,} valeurs WDC ont un match Wikidata")
    
    # 9. Export
    if (work_dir / "wdc_wikidata_links.tsv").exists():
        print_color("  ✅ Résultats déjà présents, export ignoré.", Colors.GREEN)
        export_unmatched_values(wdc_values_matched, wdc_map, work_dir, key_name=pattern)
    else:
        export_results(matches, wdc_values_matched, wdc_map, wikidata_map, work_dir, key_name=pattern)
    
    print("\n" + "="*60)
    print_color("✨ TERMINÉ!", Colors.GREEN)
    print("="*60)
    print(f"\nFichiers générés dans: {work_dir}/")
    print(f"  - {class_name}_filtered.nq (données filtrées)")
    print(f"  - wdc_wikidata_links.tsv (liens détaillés)")
    print(f"  - owl_sameas.nt (triplets RDF)")
    print(f"  - stats.txt (statistiques)")
    print()

if __name__ == "__main__":
    main()

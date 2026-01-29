#!/usr/bin/env python3
"""
BEAM Format Generator - Generate BEAM-style benchmark files from WDC N-Quads and Wikidata

This script produces the 5 files required by BEAM format:
  - attr_triples_1: WDC attribute triples (literals)
  - rel_triples_1: WDC relational triples (URIs/bnodes)
  - attr_triples_2: Wikidata attribute triples (literals)
  - rel_triples_2: Wikidata relational triples (URIs)
  - ent_links: Entity alignments (wdc_iri \t wikidata_uri)

Usage:
    python generate_beam_format.py \\
        --wdc-nquads Musics_full_graph.nq \\
        --linking-tsv wdc_wikidata_links.tsv \\
        --output-dir data/music \\
        --max-depth 1 \\
        --lang en \\
        --lowercase-wikidata

Author: Generated for BEAM benchmark
"""

import argparse
import re
import sys
import time
import json
from pathlib import Path
from collections import defaultdict
from typing import Set, Dict, List, Tuple
from urllib.parse import quote
import requests
from SPARQLWrapper import SPARQLWrapper, JSON as SPARQL_JSON
from SPARQLWrapper.SPARQLExceptions import EndPointInternalError, QueryBadFormed

# ============================================================================
# CONFIGURATION
# ============================================================================

WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
BATCH_SIZE = 50  # Number of entities per SPARQL query
RETRY_MAX = 5
RETRY_DELAY = 2.0
TIMEOUT = 300  # 5 minutes

# Progress display frequency
PROGRESS_LINES = 10000

# ============================================================================
# UTILITIES
# ============================================================================

class Colors:
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    BLUE = '\033[0;34m'
    CYAN = '\033[0;36m'
    RESET = '\033[0m'

def print_color(text, color):
    print(f"{color}{text}{Colors.RESET}")

def is_literal(value: str) -> bool:
    """Check if value is a literal (quoted or with datatype/lang tag)"""
    return value.startswith('"')

def is_blank_node(value: str) -> bool:
    """Check if value is a blank node"""
    return value.startswith('_:')

def is_uri(value: str) -> bool:
    """Check if value is a URI (enclosed in < >)"""
    return value.startswith('<') and value.endswith('>')

def strip_uri_brackets(uri: str) -> str:
    """Remove < > from URI"""
    if uri.startswith('<') and uri.endswith('>'):
        return uri[1:-1]
    return uri

def extract_literal_value(literal: str) -> str:
    """Extract the actual value from a literal (removes quotes, lang tags, datatypes)"""
    # Remove language tags and datatypes
    if '@' in literal:
        literal = literal.split('@')[0]
    if '^^' in literal:
        literal = literal.split('^^')[0]
    
    # Remove quotes
    if literal.startswith('"') and literal.endswith('"'):
        return literal[1:-1]
    
    return literal

def parse_nquad_line(line: str) -> Tuple[str, str, str, str]:
    """
    Parse an N-Quad line into (subject, predicate, object, graph)
    
    Format: <subj> <pred> "obj" <graph> .
    or: _:bnode <pred> <uri> <graph> .
    """
    line = line.strip()
    if not line or line.startswith('#'):
        return None, None, None, None
    
    # Remove trailing dot
    if line.endswith(' .'):
        line = line[:-2].strip()
    
    parts = []
    i = 0
    in_literal = False
    current = []
    
    while i < len(line):
        char = line[i]
        
        if char == '"' and (i == 0 or line[i-1] != '\\'):
            in_literal = not in_literal
            current.append(char)
        elif char in (' ', '\t') and not in_literal:
            if current:
                parts.append(''.join(current))
                current = []
        else:
            current.append(char)
        
        i += 1
    
    if current:
        parts.append(''.join(current))
    
    if len(parts) >= 3:
        subject = parts[0]
        predicate = parts[1]
        obj = parts[2]
        graph = parts[3] if len(parts) >= 4 else ""
        return subject, predicate, obj, graph
    
    return None, None, None, None

# ============================================================================
# WDC PROCESSING
# ============================================================================

def load_linking_file(tsv_path: Path) -> Tuple[Dict[str, str], Set[str], Set[str]]:
    """
    Load linking TSV file
    
    Returns:
        - entity_links: {wdc_iri: wikidata_uri}
        - wdc_values_to_mask: set of wdc_value literals to remove
        - wiki_values_to_mask: set of wiki_value literals to remove
    """
    print_color(f"\n📂 Loading linking file: {tsv_path}", Colors.BLUE)
    
    entity_links = {}
    wdc_values_to_mask = set()
    wiki_values_to_mask = set()
    
    with open(tsv_path, 'r', encoding='utf-8') as f:
        header = f.readline().strip().split('\t')
        
        # Find column indices
        wdc_iri_idx = header.index('wdc_iri')
        wiki_uri_idx = header.index('wikidata_uri')
        wdc_value_idx = header.index('wdc_value')
        wiki_value_idx = header.index('wiki_value')
        
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            parts = line.split('\t')
            if len(parts) <= max(wdc_iri_idx, wiki_uri_idx, wdc_value_idx, wiki_value_idx):
                continue
            
            wdc_iri = parts[wdc_iri_idx]
            wiki_uri = parts[wiki_uri_idx]
            wdc_value = parts[wdc_value_idx]
            wiki_value = parts[wiki_value_idx]
            
            entity_links[wdc_iri] = wiki_uri
            
            # Store values to mask (normalize for comparison)
            if wdc_value:
                wdc_values_to_mask.add(wdc_value.strip())
            if wiki_value:
                wiki_values_to_mask.add(wiki_value.strip())
    
    print_color(f"✅ Loaded {len(entity_links)} entity links", Colors.GREEN)
    print_color(f"✅ WDC values to mask: {len(wdc_values_to_mask)}", Colors.GREEN)
    print_color(f"✅ Wikidata values to mask: {len(wiki_values_to_mask)}", Colors.GREEN)
    
    return entity_links, wdc_values_to_mask, wiki_values_to_mask

def extract_wdc_triples(
    nquads_path: Path,
    entity_links: Dict[str, str],
    wdc_values_to_mask: Set[str],
    max_depth: int = 1
) -> Tuple[List[Tuple[str, str, str]], List[Tuple[str, str, str]]]:
    """
    Extract WDC triples from N-Quads file (memory-efficient streaming)
    
    Returns:
        - attr_triples: [(subject, predicate, literal), ...]
        - rel_triples: [(subject, predicate, uri_or_bnode), ...]
    """
    print_color(f"\n📖 Extracting WDC triples from: {nquads_path}", Colors.BLUE)
    print(f"   Max depth for blank node traversal: {max_depth}")
    
    # Track which entities to include
    entities_to_include = set(entity_links.keys())
    
    # Build bnode expansion by streaming through file multiple times
    print("\n   Pass 1: Identifying blank nodes to explore...")
    
    bnodes_at_depth = {0: set()}  # depth -> set of bnodes
    
    # Find bnodes directly connected to linked entities
    line_count = 0
    with open(nquads_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line_num, line in enumerate(f, 1):
            line_count = line_num
            
            subj, pred, obj, graph = parse_nquad_line(line)
            
            if not subj or not pred or not obj:
                continue
            
            if subj in entities_to_include and is_blank_node(obj):
                bnodes_at_depth[0].add(obj)
            
            if line_num % PROGRESS_LINES == 0:
                print(f"\r      Lines: {line_num:,} | Depth 0 bnodes: {len(bnodes_at_depth[0]):,}", end='')
    
    print(f"\r      Lines: {line_count:,} | Depth 0 bnodes: {len(bnodes_at_depth[0]):,}")
    
    # Expand bnodes to max_depth
    for depth in range(1, max_depth + 1):
        if not bnodes_at_depth.get(depth - 1):
            break
        
        print(f"\n   Pass {depth + 1}: Expanding bnodes at depth {depth}...")
        
        bnodes_at_depth[depth] = set()
        previous_bnodes = bnodes_at_depth[depth - 1]
        
        with open(nquads_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line_num, line in enumerate(f, 1):
                subj, pred, obj, graph = parse_nquad_line(line)
                
                if not subj or not pred or not obj:
                    continue
                
                if subj in previous_bnodes and is_blank_node(obj):
                    bnodes_at_depth[depth].add(obj)
                
                if line_num % PROGRESS_LINES == 0:
                    print(f"\r      Lines: {line_num:,} | Depth {depth} bnodes: {len(bnodes_at_depth[depth]):,}", end='')
        
        print(f"\r      Lines: {line_count:,} | Depth {depth} bnodes: {len(bnodes_at_depth[depth]):,}")
    
    # Merge all bnodes into entities_to_include
    all_bnodes = set()
    for depth_bnodes in bnodes_at_depth.values():
        all_bnodes.update(depth_bnodes)
    
    entities_to_include.update(all_bnodes)
    
    print(f"\n   ✅ Total entities to extract: {len(entities_to_include):,}")
    print(f"      - Linked entities: {len(entity_links)}")
    print(f"      - Blank nodes: {len(all_bnodes)}")
    
    # Final pass: Extract triples (streaming)
    print(f"\n   Pass {max_depth + 2}: Extracting and separating triples...")
    
    attr_triples = []
    rel_triples = []
    extracted_count = 0
    
    with open(nquads_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line_num, line in enumerate(f, 1):
            subj, pred, obj, graph = parse_nquad_line(line)
            
            if not subj or not pred or not obj:
                continue
            
            # Skip if subject not in our set
            if subj not in entities_to_include:
                continue
            
            extracted_count += 1
            
            # Check if object is a masked value
            if is_literal(obj):
                literal_value = extract_literal_value(obj)
                if literal_value in wdc_values_to_mask:
                    continue  # Skip this triple (it's a linking value)
                
                # Attribute triple
                attr_triples.append((subj, pred, obj))
            else:
                # Relational triple
                rel_triples.append((subj, pred, obj))
            
            if line_num % PROGRESS_LINES == 0:
                print(f"\r      Lines: {line_num:,} | Extracted: {extracted_count:,} | Attr: {len(attr_triples):,} | Rel: {len(rel_triples):,}", end='')
    
    print(f"\r      Lines: {line_count:,} | Extracted: {extracted_count:,} | Attr: {len(attr_triples):,} | Rel: {len(rel_triples):,}")
    
    print_color(f"\n✅ Extracted {len(attr_triples)} attribute triples", Colors.GREEN)
    print_color(f"✅ Extracted {len(rel_triples)} relational triples", Colors.GREEN)
    
    return attr_triples, rel_triples

# ============================================================================
# WIKIDATA PROCESSING
# ============================================================================

def query_wikidata_with_retry(query: str, retries: int = RETRY_MAX) -> dict:
    """Execute SPARQL query with retry and exponential backoff"""
    sparql = SPARQLWrapper(WIKIDATA_ENDPOINT)
    sparql.setQuery(query)
    sparql.setReturnFormat(SPARQL_JSON)
    sparql.setTimeout(TIMEOUT)
    
    for attempt in range(retries):
        try:
            return sparql.query().convert()
        except Exception as e:
            if attempt < retries - 1:
                wait_time = RETRY_DELAY * (2 ** attempt)
                print(f"\n      ⚠️  Query failed (attempt {attempt + 1}/{retries}): {e}")
                print(f"      Retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)
            else:
                print(f"\n      ❌ Query failed after {retries} attempts: {e}")
                raise

def extract_wikidata_triples(
    entity_links: Dict[str, str],
    wiki_values_to_mask: Set[str],
    lang: str = 'en',
    lowercase: bool = True,
    resume_state_file: Path = None
) -> Tuple[List[Tuple[str, str, str]], List[Tuple[str, str, str]]]:
    """
    Extract Wikidata triples via SPARQL
    
    Returns:
        - attr_triples: [(subject, predicate, literal), ...]
        - rel_triples: [(subject, predicate, uri), ...]
    """
    print_color(f"\n🌐 Extracting Wikidata triples via SPARQL", Colors.BLUE)
    print(f"   Endpoint: {WIKIDATA_ENDPOINT}")
    print(f"   Language filter: {lang}")
    print(f"   Lowercase URIs: {lowercase}")
    print(f"   Batch size: {BATCH_SIZE}")
    
    wikidata_uris = list(set(entity_links.values()))
    total = len(wikidata_uris)
    
    print(f"   Total Wikidata entities: {total}")
    
    # Load resume state if exists
    completed_batches = set()
    if resume_state_file and resume_state_file.exists():
        print_color(f"\n📂 Resuming from state file: {resume_state_file}", Colors.YELLOW)
        with open(resume_state_file, 'r') as f:
            state = json.load(f)
            completed_batches = set(state.get('completed_batches', []))
        print(f"   Already completed: {len(completed_batches)} batches")
    
    attr_triples = []
    rel_triples = []
    
    # Process in batches
    for batch_idx in range(0, total, BATCH_SIZE):
        batch_num = batch_idx // BATCH_SIZE
        
        if batch_num in completed_batches:
            print(f"\r   Batch {batch_num + 1}/{(total + BATCH_SIZE - 1) // BATCH_SIZE}: SKIPPED (already done)", end='')
            continue
        
        batch = wikidata_uris[batch_idx:batch_idx + BATCH_SIZE]
        
        print(f"\r   Batch {batch_num + 1}/{(total + BATCH_SIZE - 1) // BATCH_SIZE}: Processing {len(batch)} entities...", end='')
        
        # Build CONSTRUCT query
        values_clause = ' '.join([f"<{uri}>" for uri in batch])
        
        query = f"""
        CONSTRUCT {{
          ?s ?p ?o .
        }}
        WHERE {{
          VALUES ?s {{ {values_clause} }}
          ?s ?p ?o .
          FILTER(
            !isLiteral(?o) || 
            !lang(?o) || 
            lang(?o) = "" || 
            lang(?o) = "{lang}"
          )
        }}
        """
        
        try:
            results = query_wikidata_with_retry(query)
            
            # Parse results (CONSTRUCT returns triples)
            if '@graph' in results:
                for triple in results['@graph']:
                    subj = triple.get('@id', '')
                    
                    for key, value in triple.items():
                        if key.startswith('@'):
                            continue
                        
                        pred = key
                        
                        # Handle different value types
                        if isinstance(value, list):
                            for v in value:
                                _process_wikidata_triple(
                                    subj, pred, v, 
                                    wiki_values_to_mask, 
                                    lowercase, 
                                    attr_triples, 
                                    rel_triples
                                )
                        else:
                            _process_wikidata_triple(
                                subj, pred, value, 
                                wiki_values_to_mask, 
                                lowercase, 
                                attr_triples, 
                                rel_triples
                            )
            
            # Mark batch as completed
            completed_batches.add(batch_num)
            
            # Save state
            if resume_state_file:
                with open(resume_state_file, 'w') as f:
                    json.dump({'completed_batches': list(completed_batches)}, f)
            
            # Small delay to be nice to the endpoint
            time.sleep(0.5)
            
        except Exception as e:
            print(f"\n   ❌ Failed to process batch {batch_num + 1}: {e}")
            print(f"   Skipping this batch...")
            continue
    
    print()  # Newline after progress
    
    print_color(f"\n✅ Extracted {len(attr_triples)} Wikidata attribute triples", Colors.GREEN)
    print_color(f"✅ Extracted {len(rel_triples)} Wikidata relational triples", Colors.GREEN)
    
    # Clean up state file
    if resume_state_file and resume_state_file.exists():
        resume_state_file.unlink()
        print_color(f"✅ Removed state file (extraction complete)", Colors.GREEN)
    
    return attr_triples, rel_triples

def _process_wikidata_triple(
    subj: str,
    pred: str,
    obj,
    wiki_values_to_mask: Set[str],
    lowercase: bool,
    attr_triples: List,
    rel_triples: List
):
    """Helper to process a single Wikidata triple"""
    # Apply lowercase if needed
    if lowercase:
        subj = subj.lower()
        pred = pred.lower()
    
    if isinstance(obj, dict):
        if '@value' in obj:
            # Literal value
            value = obj['@value']
            
            # Check if it's a masked value
            if str(value) in wiki_values_to_mask:
                return
            
            # Format as proper literal
            if '@language' in obj:
                literal = f'"{value}"@{obj["@language"]}'
            elif '@type' in obj:
                literal = f'"{value}"^^<{obj["@type"]}>'
            else:
                literal = f'"{value}"'
            
            attr_triples.append((subj, pred, literal))
        
        elif '@id' in obj:
            # URI object
            obj_uri = obj['@id']
            if lowercase:
                obj_uri = obj_uri.lower()
            
            rel_triples.append((subj, pred, obj_uri))
    
    elif isinstance(obj, str):
        # Simple URI
        obj_uri = obj
        if lowercase:
            obj_uri = obj_uri.lower()
        
        rel_triples.append((subj, pred, obj_uri))

# ============================================================================
# OUTPUT
# ============================================================================

def write_triples(triples: List[Tuple[str, str, str]], output_path: Path):
    """Write triples to file in tab-separated format"""
    with open(output_path, 'w', encoding='utf-8') as f:
        for subj, pred, obj in triples:
            f.write(f"{subj}\t{pred}\t{obj}\n")

def write_entity_links(entity_links: Dict[str, str], output_path: Path, lowercase: bool = True):
    """Write entity links to file"""
    with open(output_path, 'w', encoding='utf-8') as f:
        for wdc_iri, wiki_uri in entity_links.items():
            if lowercase:
                wiki_uri = wiki_uri.lower()
            f.write(f"{wdc_iri}\t{wiki_uri}\n")

# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Generate BEAM-format benchmark files from WDC and Wikidata',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  python generate_beam_format.py \\
      --wdc-nquads Musics_full_graph.nq \\
      --linking-tsv wdc_wikidata_links.tsv \\
      --output-dir data/music

  # With resume support and custom settings
  python generate_beam_format.py \\
      --wdc-nquads Musics_full_graph.nq \\
      --linking-tsv wdc_wikidata_links.tsv \\
      --output-dir data/music \\
      --max-depth 2 \\
      --lang en \\
      --lowercase-wikidata \\
      --resume

  # Skip Wikidata extraction (use existing files)
  python generate_beam_format.py \\
      --wdc-nquads Musics_full_graph.nq \\
      --linking-tsv wdc_wikidata_links.tsv \\
      --output-dir data/music \\
      --skip-wikidata
        """
    )
    
    # Required arguments
    parser.add_argument(
        '--wdc-nquads',
        type=Path,
        required=True,
        help='Path to WDC N-Quads file (e.g., Musics_full_graph.nq)'
    )
    
    parser.add_argument(
        '--linking-tsv',
        type=Path,
        required=True,
        help='Path to linking TSV file (columns: wdc_iri, wikidata_uri, wdc_value, wiki_value, method, min_len)'
    )
    
    parser.add_argument(
        '--output-dir',
        type=Path,
        required=True,
        help='Output directory for BEAM files (e.g., data/music)'
    )
    
    # Optional arguments
    parser.add_argument(
        '--max-depth',
        type=int,
        default=1,
        help='Maximum depth for blank node traversal (default: 1)'
    )
    
    parser.add_argument(
        '--lang',
        type=str,
        default='en',
        help='Language filter for Wikidata literals (default: en)'
    )
    
    parser.add_argument(
        '--lowercase-wikidata',
        action='store_true',
        help='Lowercase Wikidata URIs (as in BEAM airport/books)'
    )
    
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Enable resume mode for Wikidata extraction (creates state file)'
    )
    
    parser.add_argument(
        '--skip-wikidata',
        action='store_true',
        help='Skip Wikidata extraction (only process WDC)'
    )
    
    args = parser.parse_args()
    
    # Validate inputs
    if not args.wdc_nquads.exists():
        print_color(f"❌ WDC N-Quads file not found: {args.wdc_nquads}", Colors.RED)
        sys.exit(1)
    
    if not args.linking_tsv.exists():
        print_color(f"❌ Linking TSV file not found: {args.linking_tsv}", Colors.RED)
        sys.exit(1)
    
    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 60)
    print_color("🎯 BEAM Format Generator", Colors.CYAN)
    print("=" * 60)
    print(f"WDC N-Quads:     {args.wdc_nquads}")
    print(f"Linking TSV:     {args.linking_tsv}")
    print(f"Output dir:      {args.output_dir}")
    print(f"Max depth:       {args.max_depth}")
    print(f"Language:        {args.lang}")
    print(f"Lowercase Wiki:  {args.lowercase_wikidata}")
    print(f"Resume mode:     {args.resume}")
    print("=" * 60)
    
    # Load linking file
    entity_links, wdc_values_to_mask, wiki_values_to_mask = load_linking_file(args.linking_tsv)
    
    # Extract WDC triples
    wdc_attr, wdc_rel = extract_wdc_triples(
        args.wdc_nquads,
        entity_links,
        wdc_values_to_mask,
        args.max_depth
    )
    
    # Extract Wikidata triples
    if not args.skip_wikidata:
        resume_file = args.output_dir / '.wikidata_state.json' if args.resume else None
        
        wiki_attr, wiki_rel = extract_wikidata_triples(
            entity_links,
            wiki_values_to_mask,
            args.lang,
            args.lowercase_wikidata,
            resume_file
        )
    else:
        print_color("\n⏭️  Skipping Wikidata extraction (--skip-wikidata)", Colors.YELLOW)
        wiki_attr, wiki_rel = [], []
    
    # Write output files
    print_color(f"\n💾 Writing output files to {args.output_dir}", Colors.BLUE)
    
    attr_triples_1 = args.output_dir / 'attr_triples_1'
    rel_triples_1 = args.output_dir / 'rel_triples_1'
    attr_triples_2 = args.output_dir / 'attr_triples_2'
    rel_triples_2 = args.output_dir / 'rel_triples_2'
    ent_links_file = args.output_dir / 'ent_links'
    
    write_triples(wdc_attr, attr_triples_1)
    write_triples(wdc_rel, rel_triples_1)
    
    if not args.skip_wikidata:
        write_triples(wiki_attr, attr_triples_2)
        write_triples(wiki_rel, rel_triples_2)
    
    write_entity_links(entity_links, ent_links_file, args.lowercase_wikidata)
    
    print(f"   ✅ {attr_triples_1}")
    print(f"   ✅ {rel_triples_1}")
    
    if not args.skip_wikidata:
        print(f"   ✅ {attr_triples_2}")
        print(f"   ✅ {rel_triples_2}")
    
    print(f"   ✅ {ent_links_file}")
    
    # Final summary
    print("\n" + "=" * 60)
    print_color("✨ COMPLETED!", Colors.GREEN)
    print("=" * 60)
    print(f"WDC Attribute Triples:    {len(wdc_attr):,}")
    print(f"WDC Relational Triples:   {len(wdc_rel):,}")
    
    if not args.skip_wikidata:
        print(f"Wikidata Attribute Triples: {len(wiki_attr):,}")
        print(f"Wikidata Relational Triples: {len(wiki_rel):,}")
    
    print(f"Entity Links:             {len(entity_links):,}")
    print("=" * 60)
    print()
    print(f"📂 Output files in: {args.output_dir}/")
    print()
    print("Next steps:")
    print(f"  1. Review the generated files")
    print(f"  2. Run create_folds.sh to generate train/test/valid splits")
    print(f"  3. Use the files with BEAM/OpenEA models")
    print()

if __name__ == '__main__':
    main()

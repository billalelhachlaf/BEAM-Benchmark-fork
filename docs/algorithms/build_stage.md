# Build Stage Details

## Purpose

Transform aligned entities into BEAM-compatible files for model training/evaluation.

## Entry Point

- `scripts/build_beam_files.py`
- Main orchestrator: `run_pipeline(...)`.

## Processing Steps

1. Read alignment links (`read_links`).
2. Normalize URIs and literals.
3. Split WDC graph into:
   - attributes (`attr_triples_1`)
   - relations (`rel_triples_1`)
4. Generate Wikidata side:
   - local NQ path or SPARQL construction
   - output `attr_triples_2` / `rel_triples_2`
5. Optional enrichment:
   - labels/descriptions append
6. Stats generation:
   - property distributions
7. Finalization:
   - `BUILD_CONFIG.json`, `BUILD_DONE`

## Normalization Rules (Current)

- Wikidata entities use canonical host `http://www.wikidata.org/...`.
- Lowercase behavior is applied consistently when selected.
- WDC subject filtering preserves valid IRIs and bnodes.

## Key Output Guarantees

- Deterministic file names and structure.
- Dedupe behavior controllable via parameters.
- Linked-only filtering available for stricter datasets.

## Example CLI Invocation (Standalone)

```bash
python scripts/build_beam_files.py \
  --links-tsv data/example/links.tsv \
  --wdc-nq data/example/wdc.nq \
  --out-dir data/example/beam_out
```

## Common Pitfalls

- Empty `ent_links` -> downstream empty outputs.
- Misconfigured linked-only mode can over-filter relations.
- Inconsistent URI case normalization across mixed data sources.

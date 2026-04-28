# Results Interpretation

## Build-Level Questions

Ask these first:
- Did the job finish (`done`) without fallback errors?
- Is links volume plausible for class size?
- Are there clear property signals in stats files?

## File Semantics

- `ent_links`: canonical aligned entity pairs (WDC -> Wikidata).
- `attr_triples_1` / `rel_triples_1`: WDC-side attributes/relations.
- `attr_triples_2` / `rel_triples_2`: Wikidata-side attributes/relations.
- `prop_stats_wdc.tsv`: source property frequencies.
- `prop_stats_wd.tsv`: target property frequencies.

## Quality Heuristics

- Very low links: likely mapping mismatch or overly strict filters.
- Excessively high links: possible noisy key/predicate selection.
- Sparse relation files: over-filtering or weak graph connectivity.

## Iteration Strategy

1. Adjust one variable at a time.
2. Re-run on subset parts.
3. Compare links volume and property distributions.
4. Promote best config to full-scale run.

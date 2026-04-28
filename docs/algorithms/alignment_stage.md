# Alignment Stage Details

## Purpose

Map WDC entities to Wikidata entities with configurable matching strategies.

## Inputs

- WDC class name and selected parts.
- Matching rules:
  - WDC predicate pattern(s)
  - Wikidata property or direct Wikidata URL mode
- Optional class filter (Wikidata QID).

## Core Logic

Code references:
- `scripts/align.py`
- `beam/pipeline.py`

Processing steps:
1. Read WDC triples for target parts.
2. Select WDC values from matching predicates.
3. Build candidate query batches to endpoint.
4. Match and score candidate links.
5. Deduplicate and persist links.

## Caching

- Align cache key is based on class + parts + matching configuration.
- Cache path: `Download/<ClassName>/align_cache/<hash>/`.
- Files:
  - `wdc_wikidata_links.tsv`
  - `ALIGN_DONE`

## Failure Modes

- Endpoint timeout / throttling.
- Too strict property constraints.
- No discriminative values in WDC source.

## Tuning Levers

- `ALIGN_MAX_WORKERS`
- `WIKIDATA_QUERY_MAX_RETRIES`
- `WIKIDATA_QUERY_RETRY_DELAY`
- `WIKIDATA_QUERY_TIMEOUT`

## Debug Checklist

```bash
tail -n 200 logs/worker.log
sqlite3 jobs.db "select id,status,error from jobs order by created_at desc limit 10;"
```

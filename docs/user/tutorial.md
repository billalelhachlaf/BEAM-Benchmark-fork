# User Tutorial (Current UI)

Updated: 2026-05-11  
Audience: benchmark users (no code changes required)

## 1. Open the App

- Open `http://<host>:8501`.
- From the dashboard top bar, open `/tutorial` if you want side-by-side guidance.

## 2. Before You Run (30-second checklist)

- Choose a valid `Class name`.
- Start with a small `Parts to process` value (`0-2` or `0-5`).
- Set `Matching mode` and matching fields consistently.
- Keep `Target endpoint` = `Wikidata` for first validation.

## 3. Fill Job Configuration

### Matching mode
- `property`: align by value matching between WDC and endpoint properties.
- `sameas`: align through explicit resource links.
- `sameas_or_property`: union of both strategies.

### Class scope
- `Class name`: WDC class to process (for example `Movie`, `City`, `Language`).
- `Target class filter`: optional semantic constraint (QID for Wikidata, class URI/prefix for other endpoints).

### Property mapping
- `Pattern search scope`: where WDC pattern tokens are searched (`predicate` or `value`).
- `Property mapping rules`: one rule per row, format:
  - `wdc_prop => target_prop`
  - target alternatives allowed (example: `P212|P957`)
- Per-row normalization can be configured from the mapping UI controls.

### Endpoint and execution controls
- `Target endpoint`: `wikidata`, `dbpedia`, `yago`, or `custom`.
- `Custom endpoint URL`: required only when endpoint is `custom`.
- `Custom prefixes`: optional SPARQL prefixes.
- `Parts to process`: `all`, `0,2,4`, or `0-10`.
- `Use local parts only`: never download missing WDC parts.
- `Ignore align cache`: force a fresh align run.

## 4. Launch and Monitor

- Click `Generate benchmark`.
- Expected job flow: `queued -> running -> done`.
- Track subjobs and logs from history/build details.

## 5. Validate Outputs Quickly

In completed build artifacts, verify:
- `ent_links` exists and is non-empty,
- `attr_triples_*` and `rel_triples_*` exist,
- `stats.json` and `BUILD_CONFIG.json` are present.

## 6. Recovery Patterns

If job stays `queued`:

```bash
docker compose logs --tail=300 worker
```

If align/download fails:
- rerun once (transient endpoint/network failures are common),
- reduce `Parts to process`,
- retry with same config.

If job ends with `0 links`:
- verify `Property mapping rules` syntax,
- try the other `Pattern search scope`,
- relax or remove `Target class filter`,
- rerun with `Ignore align cache`.

## 7. Next Steps

- Optimization recipes: [recipes.md](recipes.md)
- Result interpretation: [results_interpretation.md](results_interpretation.md)
- FAQ: [faq.md](faq.md)

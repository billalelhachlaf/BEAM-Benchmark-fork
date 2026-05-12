# User Tutorial (SaaS UI)

Updated: 2026-05-12  
Audience: benchmark users (no code changes required)

## 1. Open the App

- Open `http://<host>:8501`.
- Use the top navigation bar available on all main pages.
- Core pages:
  - `/app/create` for configuration and launch
  - `/app/jobs` for live monitoring
  - `/app/history` for completed builds
  - `/tutorial` for guidance

## 2. Before You Run (30-second checklist)

- Choose a valid `Class name`.
- Start with a small `Parts to process` value (`0-2` or `0-5`).
- Set `Matching mode` and matching fields consistently.
- Keep `Target endpoint` = `Wikidata` for first validation.

## 3. Create Run Wizard

The Create page is step-based:
- `1. Scope`
- `2. Mapping`
- `3. Prefixes`
- `4. Parts`
- `5. Validation`

Use `Next`/`Previous`. The wizard blocks progression when required fields are missing.

The layout is simple:
- Create page: configuration wizard,
- Jobs page: running and queued work,
- History page: completed builds,
- Tutorial page: this guide.

### 1) Scope
- `property`: align by value matching between WDC and endpoint properties.
- `sameas`: align through explicit resource links.
- `sameas_or_property`: union of both strategies.
- `Target endpoint`: `wikidata`, `dbpedia`, `yago`, or `custom`.
- `Custom endpoint URL`: required only when endpoint is `custom`.
- `Class name`: WDC class to process (for example `Movie`, `City`, `Language`).
- `Target class filter`: optional semantic constraint (QID for Wikidata, class URI/prefix for other endpoints).

### 2) Mapping
- `Pattern search scope`: where WDC pattern tokens are searched (`predicate` or `value`).
- `Property mapping rules`: one rule per row, format:
  - `wdc_prop => target_prop`
  - target alternatives allowed (example: `P212|P957`)
- Per-row normalization can be configured from the mapping UI controls.

### 3) Prefixes
- `Custom prefixes`: optional SPARQL prefixes for target endpoint queries.
- Leave empty unless the target endpoint requires extra `PREFIX` declarations.

### 4) Parts and execution controls
- `Parts to process`: `all`, `0,2,4`, or `0-10`.
- `Use local parts only`: never download missing WDC parts.
- `Ignore align cache`: force a fresh align run.

### 5) Validation and launch
- Check readiness warnings in the validation step.
- Run preflight before launch when possible.
- Click `Generate benchmark`.

## 4. Launch and Monitor

- Expected job flow: `queued -> running -> done`.
- Use `/app/jobs` to track status, subjobs, and logs.
- Use action buttons (`cancel`, `rerun`, `rerun align/build`) when needed.

## 5. Validate Outputs Quickly

In `/app/history`, open a completed build and verify:
- `ent_links` exists and is non-empty,
- `attr_triples_*` and `rel_triples_*` exist,
- `stats.json` and `BUILD_CONFIG.json` are present.

Use the build detail page for the full file summary. Use the link explorer when you need to inspect individual WDC-to-target links and their property evidence.

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

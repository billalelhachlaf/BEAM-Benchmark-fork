# BEAM-App Tutorial

This guide walks through a first benchmark run from the web interface.

## 1. Open the App

- Go to `http://<host>`.
- Confirm the dashboard loads.
- Use the top navigation bar to move between the main pages.
- Open `/app/create` to configure and launch a run.
- Open `/app/jobs` to monitor active jobs.
- Open `/app/history` to review completed builds.
- Open `/tutorial` to return to this guide.

## 2. Prepare a Small First Run

- Choose a valid `Class name`.
- Start with a small `Parts to process` value, such as `0-2` or `0-5`.
- Set `Matching mode` and the matching fields consistently.
- Keep `Target endpoint` set to `Wikidata` for the first validation.

## 3. Configure the Run

The Create page is split into five steps:
- `1. Scope`
- `2. Mapping`
- `3. Endpoint`
- `4. Parts`
- `5. Validation`

Use `Next` and `Previous` to move through the form. The wizard blocks launch
when required fields are missing.

### Scope

- `property`: align by value matching between WDC and endpoint properties.
- `sameas`: align through explicit resource links.
- `sameas_or_property`: combine both strategies.
- `Class name`: WDC class to process, for example `Movie`, `City`, or `Language`.
- `Target class filter`: optional semantic constraint, such as a Wikidata QID.

### Mapping

- `Pattern search scope`: where WDC pattern tokens are searched, either `predicate` or `value`.
- `Property mapping rules`: one rule per row, using `wdc_prop => target_prop`.
- Target alternatives are allowed, for example `P212|P957`.
- Per-row normalization can be configured from the mapping controls.

### Endpoint

- `Target endpoint`: `wikidata`, `dbpedia`, `yago`, or `custom`.
- `Custom endpoint URL`: required only when endpoint is `custom`.
- `Custom prefixes`: optional SPARQL prefixes.

### Parts and Execution

- `Parts to process`: `all`, `0,2,4`, or `0-10`.
- `Use local parts only`: do not download missing WDC parts.
- `Ignore align cache`: force a fresh align run.

### Validation and Launch

- Review readiness warnings in the validation step.
- Run preflight before launch when possible.
- Click `Generate benchmark`.

## 4. Monitor Progress

- Expected job flow: `queued -> running -> done`.
- Use `/app/jobs` to track status, subjobs, and logs.
- Use action buttons such as `cancel`, `rerun`, `rerun align`, or `rerun build` when needed.

## 5. Check the Outputs

In `/app/history`, open a completed build and verify:
- `ent_links` exists and is non-empty.
- `attr_triples_*` and `rel_triples_*` exist.
- `stats.json` and `BUILD_CONFIG.json` are present.

## 6. Recover From Common Issues

If a job stays `queued`:

```bash
docker compose logs --tail=300 worker
```

If align or download fails:
- rerun once, because endpoint and network failures can be transient.
- reduce `Parts to process`.
- retry with the same config.

If a job ends with `0 links`:
- verify `Property mapping rules` syntax.
- try the other `Pattern search scope`.
- relax or remove `Target class filter`.
- rerun with `Ignore align cache`.

## 7. Next Steps

- Optimization recipes: [recipes.md](recipes.md)
- Result interpretation: [results_interpretation.md](results_interpretation.md)
- FAQ: [faq.md](faq.md)

# Pipeline End-to-End (All Steps)

This page explains every stage executed by BEAM-App.

## Stage 0: Job Creation (UI/API)

Code: `webapp/main.py` loads the app; job routes live in `webapp/modules/routes_jobs_downloads_ws.py`.

- User submits a form on `/`.
- Backend validates required fields (`class_name`, matching config, endpoint/mode constraints).
- A row is inserted into `jobs.db` with status `queued`.
- Subjobs are prepared: `align` then `build`.

## Stage 1: Job Pickup (Worker)

Code: `worker/run.py` loads the worker; execution logic lives in `worker/run_modules/`.

- Worker polls `jobs.db` using `JOB_POLL_INTERVAL`.
- It acquires a queued job if capacity allows (`MAX_CONCURRENT_JOBS`).
- Job status transitions to `running` and subjob `align` starts.

## Stage 2: Alignment (WDC -> Wikidata)

Code: `scripts/align.py`, `scripts/align_modules/`, `beam/pipeline.py`, and `beam/pipeline_modules/`.

- Loads WDC class parts (`Download/<ClassName>/`).
- Extracts candidate values from selected WDC predicates.
- Depending on mode:
  - property mode: compares WDC values against Wikidata property candidates.
  - direct link mode (`sameAs`/URL): maps values directly to Wikidata entity URLs.
- Writes alignment artifacts (notably links TSV) to align cache.

Output contract:
- if `links_count > 0`: continue to build.
- if `links_count == 0`: job goes `error` with explicit message.

## Stage 3: Build Generation (BEAM format)

Code: `scripts/build_beam_files.py` and `scripts/build_beam_files_modules/`.

Main high-level flow:
1. Read `ent_links` inputs (`read_links`).
2. Normalize entities and predicates.
3. Split WDC triples into `attr_triples_1` and `rel_triples_1` (`split_triples`).
4. Build Wikidata side (`attr_triples_2`, `rel_triples_2`) from local NQ or SPARQL (`write_wikidata_from_sparql`).
5. Optional label/description enrichment.
6. Compute property statistics.
7. Write completion markers and build metadata.

## Stage 4: Persistence + Exposure

Code: `webapp/modules/` endpoints.

- Build appears in history/dashboard.
- User can inspect link explorer/build detail pages.
- User can download build zip from `/builds/{class}/{build}/download`.

## Status Model

Job statuses:
- `queued` -> `running` -> `done`
- or terminal: `error`, `cancelled`

Subjob statuses:
- align status tracked independently from build.

## Important Files Produced

Build root:
- `data/<ClassName>/beam_<timestamp>/BUILD_CONFIG.json`
- `data/<ClassName>/beam_<timestamp>/BUILD_DONE`

Per variant (`with_link_code` / `without_link_code`):
- `ent_links`
- `attr_triples_1`, `rel_triples_1`
- `attr_triples_2`, `rel_triples_2`
- `prop_stats_wdc.tsv`, `prop_stats_wd.tsv`

## Minimal Reproducible Demo

```bash
python scripts/create_testclass_data.py
docker compose up -d --build
# Submit a TestClass preset from the UI
```

# User Tutorial (Detailed)

## Step 1: Open the App

- Go to `http://<host>`.
- Confirm dashboard loads with jobs/history blocks.

## Step 2: Fill Job Configuration

Main fields:
- `Class name`: WDC class (e.g. `Movie`, `City`, `Language`).
- `Parts spec`: `all`, `1,2,3`, or `1-5`.
- `WDC predicate pattern`: source property hint (`name`, `sameAs`, `eidr`, ...).
- `Wikidata property`: target mapping (`Pxxxx`, `wdt:Pxxxx`, or `rdfs:label`).
- `Wikidata class (QID)`: optional semantic filter.
- `Max depth`: traversal depth through blank nodes.
- `WDC values are Wikidata URLs`: direct-link mode toggle.
- `Ignore align cache`: force fresh alignment.
- `Use local parts only`: disable downloads for missing parts.

Rule reminders:
- if URL mode is OFF, Wikidata property is required.
- if URL mode is ON, Wikidata class is required.

## Step 3: Launch Job

- Click `Generate benchmark`.
- Job appears with status `queued`.
- Worker transitions it to `running`.

## Step 4: Monitor Execution

- Watch subjobs (`align`, `build`).
- Open live logs for details.
- If needed, cancel and rerun with adjusted parameters.

## Step 5: Inspect Completed Build

Open build detail and verify:
- links count,
- build metadata,
- output directories (`with_link_code`, `without_link_code`).

## Step 6: Download Output

Use download action from build page.

Typical files:
- `ent_links`
- `attr_triples_1`, `rel_triples_1`
- `attr_triples_2`, `rel_triples_2`
- property stats files

## Step 7: Iterate for Better Quality

- Start with small `parts_spec`.
- Validate matching behavior quickly.
- Scale to `all` only after good signal.

## Optional: SAKEY Explorer

- Open SAKEY page from dashboard.
- Explore key candidates for property strategy refinement.
- Treat as assistive analysis, not automatic pipeline mutation.

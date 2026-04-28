# Screenshots Plan (With Proposals)

Purpose: support training, onboarding, and presentation decks.

## Folder Convention

Store screenshots in:
- `docs/assets/screenshots/`

Naming:
- `01-dashboard-home.png`
- `02-job-form.png`
- `03-job-running-logs.png`
- `04-job-done-history.png`
- `05-build-detail.png`
- `06-link-explorer.png`
- `07-help-page.png`
- `08-sakey-explorer.png`
- `09-admin-health-check-terminal.png`
- `10-download-build.png`

## Proposed Captures

1. Dashboard home (empty state + existing jobs).
2. Full job form filled with a realistic config.
3. Running job with live logs (WebSocket).
4. Completed job with `done` status.
5. Build detail page with key metrics.
6. Link explorer page with one selected entity.
7. In-app `/help` page.
8. SAKEY explorer with filters and ranking.
9. Terminal running `check_health.sh` and passing.
10. Download action and resulting artifact list.

## Quality Rules

- Include browser URL bar in at least 2 captures.
- Keep consistent resolution (e.g. 1440x900 desktop).
- Blur sensitive hostnames/credentials if any.
- Use the same test class for coherent storytelling.

## Suggested Storyline For Presentation

1. Problem statement and architecture.
2. User run flow (submit -> monitor -> download).
3. Processing internals and generated files.
4. Admin operations and reliability controls.
5. Limits and roadmap.

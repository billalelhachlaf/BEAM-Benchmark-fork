# Documentation Style Guide

Updated: 2026-05-11

## Purpose
Keep BEAM documentation clear, concise, and operational.

## Writing Rules
- Use direct language and short sentences.
- Keep one idea per bullet.
- Prefer concrete examples over abstract explanations.
- Avoid duplicate content across pages; link instead.
- Mark examples as examples when values are illustrative.

## Required Header (major docs)
Add this at the top of user/admin/tutorial pages:
- `Updated: YYYY-MM-DD`
- `Audience: ...`
- optional `Prerequisites: ...`

## Terminology
Use current UI terms exactly:
- `Matching mode`
- `Target endpoint`
- `Target class filter`
- `Property mapping rules`
- `Pattern search scope`
- `Parts to process`
- `Use local parts only`
- `Ignore align cache`

Avoid deprecated terms in new docs:
- `Max depth`
- `WDC values are Wikidata URLs`
- `Ignore cache` (without align context)

## PR Checklist (docs impact)
If UI fields or behavior changed:
1. Update `docs/user/tutorial.md`.
2. Update `README.md` when onboarding steps change.
3. Run `bash scripts/docs_check.sh`.

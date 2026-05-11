#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

python - <<'PY'
import re
from pathlib import Path

root = Path('docs')
pat = re.compile(r'\[[^\]]+\]\(([^)]+)\)')
errors = []
deprecated_terms = [
    "Max depth",
    "WDC values are Wikidata URLs",
]

for md in sorted(root.rglob('*.md')):
    text = md.read_text(encoding='utf-8', errors='ignore')
    for m in pat.finditer(text):
        raw = (m.group(1) or '').strip()
        if not raw:
            continue
        target = raw.split('#', 1)[0].split('?', 1)[0].strip()
        if not target:
            continue
        if target.startswith(('http://', 'https://', 'mailto:', '/')):
            continue
        if target.startswith('#'):
            continue
        path = (md.parent / target).resolve()
        if not path.exists():
            errors.append(f"[ERR] broken doc link: {md} -> {raw}")
    if md.name != "DOC_STYLE.md":
        for term in deprecated_terms:
            if term in text:
                errors.append(f"[ERR] deprecated term in docs: {md} -> '{term}'")

if errors:
    for e in errors:
        print(e)
    raise SystemExit(1)

print('[OK] docs_check passed')
PY

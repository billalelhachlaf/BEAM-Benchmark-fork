import json
import os
import shutil
import tempfile
import time
import html
import zipfile
import asyncio
import difflib
import re
import subprocess
from collections import Counter, OrderedDict
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from pathlib import Path
from functools import lru_cache
from threading import Lock, Thread, Semaphore
from typing import Optional
from urllib.parse import urljoin, quote_plus, quote
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, Form, HTTPException, Response
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.background import BackgroundTask

from beam import db, ownership
from beam.wdc_classes import fetch_wdc_classes, load_wdc_classes_catalog, save_wdc_classes_catalog
from scripts import align as align_script

app = FastAPI()
app.mount("/static", StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
TUTORIAL_MD_PATH = Path(__file__).resolve().parents[1] / "docs" / "user" / "tutorial.md"

WDC_PARTS_BASE_URL = "https://data.dws.informatik.uni-mannheim.de/structureddata/2024-12/quads/classspecific/"
_PART_HREF_RE = re.compile(r"^part_(\d+)\.gz$", re.IGNORECASE)
_PART_NAME_RE = re.compile(r"^part_(\d+)(?:\.[A-Za-z0-9]+)?$", re.IGNORECASE)
_QUAD_RE = re.compile(
    r'^\s*(<[^>]+>|_:[^\s]+)\s+(<[^>]+>)\s+(".*?"(?:\^\^<[^>]+>|@[a-zA-Z-]+)?|<[^>]+>|_:[^\s]+)\s+(<[^>]+>)\s+\.\s*$'
)
_TRIPLE_RE = re.compile(
    r'^\s*(<[^>]+>|_:[^\s]+)\s+(<[^>]+>)\s+(".*?"(?:\^\^<[^>]+>|@[a-zA-Z-]+)?|<[^>]+>|_:[^\s]+)\s+\.\s*$'
)
_SAKEY_FILE_RE = re.compile(r"(sakey|vickey)", re.IGNORECASE)
_SAKEY_VICKEY_KEYS_RE = re.compile(r"VICKEY found\s+(\d+)\s+unique conditional keys", re.IGNORECASE)
_SAKEY_KEYS_RE = re.compile(r"We found\s+(\d+)\s+key\(s\)", re.IGNORECASE)
_SAKEY_NON_KEYS_RE = re.compile(r"(\d+)\s+non-keys?\s+found", re.IGNORECASE)
_SAKEY_NON_KEYS_BLOCK_RE = re.compile(r"(\d+)-\s*non keys?\s*:\s*(\[\[.*?\]\])", re.IGNORECASE | re.DOTALL)
_SAKEY_ALMOST_KEYS_BLOCK_RE = re.compile(r"(\d+)-\s*almost keys?\s*:\s*(\[\[.*?\]\])", re.IGNORECASE | re.DOTALL)
_SAKEY_RUNS_ROOT = Path("data") / ".sakey_runs"
_SAKEY_MAX_LIST = 100
_SAKEY_RUN_LOCK = Lock()
_SAKEY_MAX_CONCURRENT = max(1, int(os.getenv("SAKEY_MAX_CONCURRENT", "1") or "1"))
_SAKEY_EXEC_SEMAPHORE = Semaphore(_SAKEY_MAX_CONCURRENT)
_SAKEY_RECONCILE_LOCK = Lock()
_SAKEY_RECONCILED = False
_SAKEY_APP_BOOT_TS = time.time()
_BUILD_OWNER_FILENAME = ".beam_owner"


PRESETS = {
    "testclass_large_benchmark": {
        "label": "TestClassLarge - via property (label)",
        "matching_mode": "property",
        "class_name": "TestClassLarge",
        "parts_spec": "all",
        "wdc_predicate_pattern": "name",
        "wikidata_property": "rdfs:label",
        "wkd_class": "Q34770",
        "ignore_chars": "spaces;-;.",
        "force_align": False,
        "use_local_only": False,
    },
    "testclass_quick": {
        "label": "TestClass - via property (label)",
        "matching_mode": "property",
        "class_name": "TestClass",
        "parts_spec": "all",
        "wdc_predicate_pattern": "name",
        "wikidata_property": "rdfs:label",
        "wkd_class": "Q34770",
        "ignore_chars": "spaces;-;.",
        "force_align": False,
        "use_local_only": False,
    },
    "testclass_label": {
        "label": "TestClassLabel - via property (label)",
        "matching_mode": "property",
        "class_name": "TestClassLabel",
        "parts_spec": "all",
        "wdc_predicate_pattern": "name",
        "wikidata_property": "rdfs:label",
        "wkd_class": "Q34770",
        "ignore_chars": "spaces;-;.",
        "force_align": False,
        "use_local_only": False,
    },
    "testclass_identifier": {
        "label": "TestClassIdentifier - via property (code)",
        "matching_mode": "property",
        "class_name": "TestClassIdentifier",
        "parts_spec": "all",
        "wdc_predicate_pattern": "eidr",
        "wikidata_property": "wdt:P2704",
        "wkd_class": "Q11424",
        "ignore_chars": "spaces;-;.",
        "force_align": False,
        "use_local_only": False,
    },
    "testclass_wikidata_url": {
        "label": "TestClassWikidataUrl - via sameAs",
        "matching_mode": "sameas",
        "class_name": "TestClassWikidataUrl",
        "parts_spec": "all",
        "wdc_predicate_pattern": "url",
        "wikidata_property": "wdt:P31",
        "wkd_class": "Q515",
        "ignore_chars": "spaces;-;.",
        "force_align": False,
        "use_local_only": False,
    },
    "testclass_wikidata_sameas": {
        "label": "TestClassWikidataSameAs - via sameAs",
        "matching_mode": "sameas",
        "class_name": "TestClassWikidataSameAs",
        "parts_spec": "all",
        "wdc_predicate_pattern": "sameas",
        "wikidata_property": "wdt:P31",
        "wkd_class": "Q6256",
        "ignore_chars": "spaces;-;.",
        "force_align": False,
        "use_local_only": False,
    },
    "code_movie": {
        "label": "Movie - via property (code/EIDR)",
        "matching_mode": "property",
        "class_name": "Movie",
        "parts_spec": "all",
        "wdc_predicate_pattern": "eidr",
        "wikidata_property": "wdt:P2704",
        "wkd_class": "Q11424",
        "ignore_chars": "spaces;-;.",
        "force_align": False,
        "use_local_only": False,
    },
    "label_language": {
        "label": "Language - via property (label)",
        "matching_mode": "property",
        "class_name": "Language",
        "parts_spec": "all",
        "wdc_predicate_pattern": "name",
        "wikidata_property": "rdfs:label",
        "wkd_class": "Q33742",
        "ignore_chars": "spaces;-;.",
        "force_align": False,
        "use_local_only": False,
    },
    "property_college_or_university_telephone": {
        "label": "CollegeOrUniversity - via property (telephone)",
        "matching_mode": "property",
        "class_name": "CollegeOrUniversity",
        "parts_spec": "all",
        "wdc_predicate_pattern": "telephone",
        "wikidata_property": "P1329",
        "wkd_class": "Q38723",
        "ignore_chars": "spaces;-;.",
        "force_align": False,
        "use_local_only": False,
    },
    "wikidata_link_city": {
        "label": "City - via sameAs",
        "matching_mode": "sameas",
        "class_name": "City",
        "parts_spec": "all",
        "wdc_predicate_pattern": "sameAs",
        "wikidata_property": "",
        "wkd_class": "Q486972",
        "ignore_chars": "spaces;-;.",
        "force_align": False,
        "use_local_only": False,
    },
}

TARGET_ENDPOINTS = {
    "wikidata": {
        "label": "Wikidata",
        "default_url": "https://query.wikidata.org/sparql",
        "supports_qid": True,
    },
    "dbpedia": {
        "label": "DBpedia",
        "default_url": "https://dbpedia.org/sparql",
        "supports_qid": False,
    },
    "yago": {
        "label": "YAGO",
        "default_url": "https://yago-knowledge.org/sparql/query",
        "supports_qid": False,
    },
    "custom": {
        "label": "Custom endpoint",
        "default_url": "",
        "supports_qid": False,
    },
}
TARGET_PREFIX_DECL_RE = re.compile(
    r"^PREFIX\s+[A-Za-z][A-Za-z0-9_-]*\s*:\s*<[^>\s]+>\s*$",
    re.IGNORECASE,
)


def _default_form():
    return {
        "matching_mode": "property",
        "class_name": "",
        "parts_spec": "all",
        "wdc_predicate_pattern": "",
        "wdc_pattern_search_in": "predicate",
        "target_endpoint": "wikidata",
        "target_endpoint_url": "",
        "target_prefixes": "",
        "property_mapping_rules": "",
        "target_property": "",
        "target_class": "",
        "wikidata_property": "",
        "wkd_class": "",
        "ignore_chars": "spaces;-;.",
        "force_align": False,
        "use_local_only": False,
        "strict_duplicate_key_filter": True,
    }


def _clean_text(value: Optional[str]) -> str:
    return (value or "").strip()


def _slugify_heading(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", (value or "").strip().lower()).strip("-")
    return slug or "section"


def _render_markdown_basic(md_text: str):
    lines = (md_text or "").splitlines()
    out = []
    sections = []
    in_code = False
    list_mode = None

    def _close_list():
        nonlocal list_mode
        if list_mode == "ul":
            out.append("</ul>")
        elif list_mode == "ol":
            out.append("</ol>")
        list_mode = None

    for raw in lines:
        line = raw.rstrip("\n")
        stripped = line.strip()

        if stripped.startswith("```"):
            _close_list()
            if not in_code:
                out.append("<pre><code>")
                in_code = True
            else:
                out.append("</code></pre>")
                in_code = False
            continue

        if in_code:
            out.append(html.escape(line))
            continue

        if not stripped:
            _close_list()
            continue

        h = re.match(r"^(#{1,6})\s+(.+)$", stripped)
        if h:
            _close_list()
            lvl = len(h.group(1))
            title = h.group(2).strip()
            anchor = _slugify_heading(title)
            if lvl <= 3:
                sections.append({"title": title, "anchor": anchor, "level": lvl})
            out.append(f'<h{lvl} id="{anchor}">{html.escape(title)}</h{lvl}>')
            continue

        ul = re.match(r"^[-*]\s+(.+)$", stripped)
        if ul:
            if list_mode != "ul":
                _close_list()
                out.append("<ul>")
                list_mode = "ul"
            item = html.escape(ul.group(1).strip())
            item = re.sub(r"`([^`]+)`", r"<code>\1</code>", item)
            out.append(f"<li>{item}</li>")
            continue

        ol = re.match(r"^\d+\.\s+(.+)$", stripped)
        if ol:
            if list_mode != "ol":
                _close_list()
                out.append("<ol>")
                list_mode = "ol"
            item = html.escape(ol.group(1).strip())
            item = re.sub(r"`([^`]+)`", r"<code>\1</code>", item)
            out.append(f"<li>{item}</li>")
            continue

        _close_list()
        para = html.escape(stripped)
        para = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r'<a href="\2" target="_blank" rel="noopener">\1</a>', para)
        para = re.sub(r"`([^`]+)`", r"<code>\1</code>", para)
        out.append(f"<p>{para}</p>")

    _close_list()
    if in_code:
        out.append("</code></pre>")
    return "\n".join(out), sections


def _load_tutorial_page_data():
    if not TUTORIAL_MD_PATH.exists():
        return {
            "ok": False,
            "error": f"Tutorial source not found: {TUTORIAL_MD_PATH}",
            "html": "",
            "sections": [],
            "source_path": str(TUTORIAL_MD_PATH),
        }
    text = TUTORIAL_MD_PATH.read_text(encoding="utf-8", errors="replace")
    rendered, sections = _render_markdown_basic(text)
    return {
        "ok": True,
        "error": "",
        "html": rendered,
        "sections": sections,
        "source_path": str(TUTORIAL_MD_PATH),
    }


def _normalize_target_endpoint(value: Optional[str]) -> str:
    key = _clean_text(value).lower()
    if key in TARGET_ENDPOINTS:
        return key
    return "wikidata"


def _safe_filename_token(value: str, fallback: str = "value") -> str:
    text = _clean_text(value).lower()
    text = re.sub(r"[^a-z0-9._-]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("._-")
    return text or fallback


def _endpoint_filename_token(config: dict) -> str:
    endpoint = _normalize_target_endpoint(_clean_text(str((config or {}).get("target_endpoint", "wikidata"))))
    if endpoint != "custom":
        return _safe_filename_token(endpoint, fallback="wikidata")
    custom_url = _clean_text(str((config or {}).get("target_endpoint_url", "")))
    host = _clean_text(urlparse(custom_url).netloc).lower()
    if host:
        return _safe_filename_token(f"custom_{host}", fallback="custom")
    return "custom"


def _parse_property_mapping_rules_text(value: str):
    text = _clean_text(value)
    if not text:
        return []
    rows = []
    for line_no, raw_line in enumerate(text.splitlines(), 1):
        line = _clean_text(raw_line)
        if not line:
            continue
        norm = ""
        mapping_text = line
        if "||" in line:
            mapping_text, norm = line.split("||", 1)
            mapping_text = _clean_text(mapping_text)
            norm = _clean_text(norm)
        if "=>" not in mapping_text:
            raise ValueError(
                f"Invalid property mapping rule at line {line_no}: expected 'wdc_prop[,wdc_prop] => target_prop[,target_prop]'"
            )
        left_raw, right_raw = mapping_text.split("=>", 1)
        wdc_props = [_clean_text(tok) for tok in left_raw.split(",") if _clean_text(tok)]
        target_props = [_clean_text(tok) for tok in right_raw.split(",") if _clean_text(tok)]
        if not wdc_props:
            raise ValueError(
                f"Invalid property mapping rule at line {line_no}: left side must contain at least one property"
            )
        pair_ignore_chars = []
        pair_search_in = []
        row_mode = "property"
        norm_text = _clean_text(norm)
        if norm_text.startswith("["):
            try:
                decoded = json.loads(norm_text)
            except Exception:
                decoded = None
            if isinstance(decoded, list):
                pair_ignore_chars = [_clean_text(v) for v in decoded]
        elif norm_text.startswith("{"):
            try:
                decoded = json.loads(norm_text)
            except Exception:
                decoded = None
            if isinstance(decoded, dict):
                raw_ignore = decoded.get("ignore_chars")
                if isinstance(raw_ignore, list):
                    pair_ignore_chars = [_clean_text(v) for v in raw_ignore]
                raw_search = decoded.get("search_in")
                if isinstance(raw_search, list):
                    pair_search_in = [_normalize_wdc_pattern_search_in(v) for v in raw_search]
                raw_mode = _clean_text(str(decoded.get("mode", ""))).lower()
                if raw_mode in {"property", "sameas"}:
                    row_mode = raw_mode
        if row_mode == "property":
            if not target_props:
                raise ValueError(
                    f"Invalid property mapping rule at line {line_no}: right side must contain at least one property"
                )
            if len(wdc_props) != len(target_props):
                raise ValueError(
                    f"Invalid property mapping rule at line {line_no}: left/right property counts differ"
                )
        else:
            target_props = [""] * len(wdc_props)
        if pair_ignore_chars and len(pair_ignore_chars) != len(wdc_props):
            raise ValueError(
                f"Invalid property mapping rule at line {line_no}: per-pair normalization count differs from pair count"
            )
        if pair_search_in and len(pair_search_in) != len(wdc_props):
            raise ValueError(
                f"Invalid property mapping rule at line {line_no}: per-pair search mode count differs from pair count"
            )
        rows.append(
            {
                "line_no": line_no,
                "pairs": list(zip(wdc_props, target_props)),
                "raw": line,
                "ignore_chars": norm,
                "pair_ignore_chars": pair_ignore_chars,
                "pair_search_in": pair_search_in,
                "mode": row_mode,
            }
        )
    return rows


def _split_target_property_alternatives(value: str):
    raw = _clean_text(value)
    if not raw:
        return []
    parts = [_clean_text(tok) for tok in raw.split("|") if _clean_text(tok)]
    return parts or [raw]


def _load_build_config(build_dir: Path):
    cfg_path = build_dir / "BUILD_CONFIG.json"
    if not cfg_path.exists() or not cfg_path.is_file():
        return {}
    try:
        raw = json.loads(cfg_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}
    _sync_target_alias_fields(raw)
    return raw


def _extract_linking_combinations(config: dict):
    if not isinstance(config, dict):
        return []
    mode = _normalize_matching_mode(
        _clean_text(str(config.get("matching_mode", ""))),
        fallback_wdc_value_is_wikidata=_is_wikidata_url_mode(config),
    )
    if not _mode_includes_property(mode):
        return []

    combos = []
    rules_text = _clean_text(str(config.get("property_mapping_rules", "")))
    if rules_text:
        try:
            parsed = _parse_property_mapping_rules_text(rules_text)
        except Exception:
            parsed = []
        for i, row in enumerate(parsed, 1):
            row_mode = _clean_text(str(row.get("mode", "property"))).lower()
            if row_mode not in {"property", "sameas"}:
                row_mode = "property"
            pairs = [{"wdc": _clean_text(l), "target": _clean_text(r)} for l, r in (row.get("pairs") or [])]
            if row_mode == "sameas":
                pairs = [p for p in pairs if p["wdc"]]
            else:
                pairs = [p for p in pairs if p["wdc"] and p["target"]]
            if not pairs:
                continue
            combos.append(
                {
                    "id": i,
                    "label": f"OR #{i} ({'sameAs' if row_mode == 'sameas' else 'property'})",
                    "pairs": pairs,
                    "raw": _clean_text(row.get("raw", "")),
                    "mode": row_mode,
                }
            )
        return combos

    left = _clean_text(str(config.get("wdc_predicate_pattern", "")))
    right = _clean_text(str(config.get("target_property", config.get("wikidata_property", ""))))
    if left and right:
        combos.append(
            {
                "id": 1,
                "label": "Rule",
                "pairs": [{"wdc": left, "target": right}],
                "raw": f"{left} => {right}",
            }
        )
    return combos


def _extract_linking_elements(config: dict):
    if not isinstance(config, dict):
        return []
    out = []
    seen = set()
    combos = _extract_linking_combinations(config)
    for combo in list(combos or []):
        for pair in list((combo or {}).get("pairs") or []):
            left = _clean_text(str((pair or {}).get("wdc", "")))
            if not left:
                continue
            low = left.lower()
            if low in seen:
                continue
            seen.add(low)
            out.append(left)
    if out:
        return out
    fallback = _clean_text(str(config.get("wdc_predicate_pattern", "")))
    if fallback:
        return [fallback]
    return []


def _sync_target_alias_fields(params: dict):
    if not isinstance(params, dict):
        return params
    params["target_endpoint"] = _normalize_target_endpoint(params.get("target_endpoint"))
    params["target_endpoint_url"] = _clean_text(params.get("target_endpoint_url"))
    params["target_prefixes"] = _clean_text(params.get("target_prefixes"))
    params["property_mapping_rules"] = _clean_text(params.get("property_mapping_rules"))
    params["wdc_pattern_search_in"] = _normalize_wdc_pattern_search_in(params.get("wdc_pattern_search_in"))
    params["target_property"] = _clean_text(params.get("target_property") or params.get("wikidata_property"))
    params["target_class"] = _clean_text(params.get("target_class") or params.get("wkd_class"))
    # Backward-compatible aliases.
    params["wikidata_property"] = params["target_property"]
    params["wkd_class"] = params["target_class"]
    # One-to-one duplicate-key filtering is always enabled.
    params["strict_duplicate_key_filter"] = True
    if params["target_endpoint"] != "custom":
        params["target_endpoint_url"] = ""
    return params


def _normalize_matching_mode(value: Optional[str], fallback_wdc_value_is_wikidata: bool = False) -> str:
    mode = _clean_text(value).lower()
    if mode in {"property", "sameas", "sameas_or_property"}:
        return mode
    return "sameas" if bool(fallback_wdc_value_is_wikidata) else "property"


def _mode_includes_sameas(mode: Optional[str]) -> bool:
    return _normalize_matching_mode(mode) in {"sameas", "sameas_or_property"}


def _mode_includes_property(mode: Optional[str]) -> bool:
    return _normalize_matching_mode(mode) in {"property", "sameas_or_property"}


def _normalize_wdc_pattern_search_in(value: Optional[str]) -> str:
    mode = _clean_text(value).lower()
    if mode in {"value", "object"}:
        return "value"
    return "predicate"


def _is_wikidata_url_mode(params: dict) -> bool:
    return _normalize_matching_mode(
        (params or {}).get("matching_mode"),
        fallback_wdc_value_is_wikidata=bool((params or {}).get("wdc_value_is_wikidata")),
    ) == "sameas"


def _validate_and_normalize_job_params(raw_params: dict):
    params = dict(raw_params or {})
    _sync_target_alias_fields(params)
    params["matching_mode"] = _normalize_matching_mode(
        params.get("matching_mode"),
        fallback_wdc_value_is_wikidata=bool(params.get("wdc_value_is_wikidata")),
    )
    params.pop("wdc_value_is_wikidata", None)
    params["class_name"] = _clean_text(params.get("class_name"))
    params["parts_spec"] = _clean_text(params.get("parts_spec")) or "all"
    params["wdc_predicate_pattern"] = _clean_text(params.get("wdc_predicate_pattern"))
    params["wdc_pattern_search_in"] = _normalize_wdc_pattern_search_in(params.get("wdc_pattern_search_in"))
    params["target_endpoint"] = _normalize_target_endpoint(params.get("target_endpoint"))
    params["target_endpoint_url"] = _clean_text(params.get("target_endpoint_url"))
    params["target_prefixes"] = _clean_text(params.get("target_prefixes"))
    params["property_mapping_rules"] = _clean_text(params.get("property_mapping_rules"))
    params["target_property"] = _clean_text(params.get("target_property") or params.get("wikidata_property"))
    params["target_class"] = _clean_text(params.get("target_class") or params.get("wkd_class"))
    params["wikidata_property"] = params["target_property"]
    params["wkd_class"] = params["target_class"]
    params["ignore_chars"] = _clean_text(params.get("ignore_chars"))
    params["force_align"] = bool(params.get("force_align"))
    params["use_local_only"] = bool(params.get("use_local_only"))
    # One-to-one duplicate-key filtering is always enabled.
    params["strict_duplicate_key_filter"] = True

    if not params["class_name"]:
        return params, "Class name is required."
    if params["target_endpoint"] == "custom" and not params["target_endpoint_url"]:
        return params, "Custom endpoint URL is required when endpoint is set to Custom."
    if params["target_prefixes"]:
        for line in params["target_prefixes"].splitlines():
            prefix_line = _clean_text(line)
            if not prefix_line:
                continue
            if not TARGET_PREFIX_DECL_RE.match(prefix_line):
                return (
                    params,
                    "Custom prefixes must use one PREFIX declaration per line (e.g. PREFIX bd: <http://www.bigdata.com/rdf#>).",
                )

    parsed_rules = []
    if params["property_mapping_rules"]:
        try:
            parsed_rules = _parse_property_mapping_rules_text(params["property_mapping_rules"])
        except ValueError as exc:
            return params, str(exc)

    mode = _normalize_matching_mode(params.get("matching_mode"))
    includes_sameas = _mode_includes_sameas(mode)
    includes_property = _mode_includes_property(mode)
    rules_include_sameas = any(_clean_text(str(r.get("mode", "property"))).lower() == "sameas" for r in parsed_rules)
    rules_include_property = any(_clean_text(str(r.get("mode", "property"))).lower() != "sameas" for r in parsed_rules)
    effective_includes_sameas = includes_sameas or rules_include_sameas
    effective_includes_property = includes_property if not parsed_rules else rules_include_property

    if not params["target_class"]:
        return params, "Target class filter is required."

    if mode == "sameas" and not parsed_rules:
        params["target_property"] = ""
        params["wikidata_property"] = ""
        params["ignore_chars"] = ""
        params["property_mapping_rules"] = ""
    else:
        if not params["wdc_predicate_pattern"] and not parsed_rules:
            return params, "Considered pattern for WDC properties is required."
        if effective_includes_property and not params["ignore_chars"]:
            params["ignore_chars"] = "spaces;-;."
        if effective_includes_property and not params["target_property"] and not parsed_rules:
            return params, "Equivalent target property is required when WDC values are not endpoint URLs."

    params["wkd_class"] = params["target_class"]
    params["wikidata_property"] = params["target_property"]

    return params, None


def _is_test_class_name(class_name: Optional[str]) -> bool:
    name = _clean_text(class_name)
    if not name:
        return False
    lowered = name.lower()
    return lowered.startswith("testclass") or lowered.startswith("uxcheckclass")


def _is_test_preset(preset: dict) -> bool:
    if not isinstance(preset, dict):
        return False
    return _is_test_class_name(preset.get("class_name"))


def _filter_presets_by_mode(test_mode: bool):
    desired = bool(test_mode)
    return {k: v for k, v in PRESETS.items() if _is_test_preset(v) == desired}


def _get_or_create_owner_key(request):
    owner_key, created = ownership.get_or_create_owner_key(request)
    try:
        db.claim_unowned_jobs(owner_key)
    except Exception:
        pass
    return owner_key, created


def _set_owner_cookie_if_needed(response, request, owner_key):
    if not ownership.get_request_owner_key(request):
        ownership.set_owner_cookie(response, owner_key)
    return response


def _redirect_with_owner(request, url="/", status_code=303):
    owner_key, _ = _get_or_create_owner_key(request)
    response = RedirectResponse(url=url, status_code=status_code)
    return _set_owner_cookie_if_needed(response, request, owner_key)


def _insert_job_for_owner(params, owner_key):
    try:
        return db.insert_job(params, owner_key=owner_key)
    except TypeError as exc:
        if "owner_key" not in str(exc):
            raise
        return db.insert_job(params)


def _get_recent_presets(limit=50, test_mode: Optional[bool] = None, owner_key: Optional[str] = None):
    rows = db.list_jobs(limit=limit, owner_key=owner_key)
    recent = []
    seen = set()
    for r in rows:
        try:
            params = json.loads(r["params_json"])
        except Exception:
            continue
        _sync_target_alias_fields(params)
        mode = _normalize_matching_mode(
            params.get("matching_mode"),
            fallback_wdc_value_is_wikidata=bool(params.get("wdc_value_is_wikidata")),
        )
        if test_mode is not None and _is_test_class_name(params.get("class_name")) != bool(test_mode):
            continue
        key = (
            mode,
            params.get("class_name", ""),
            params.get("parts_spec", ""),
            params.get("wdc_predicate_pattern", ""),
            params.get("wdc_pattern_search_in", "predicate"),
            params.get("target_endpoint", "wikidata"),
            params.get("target_endpoint_url", ""),
            params.get("target_prefixes", ""),
            params.get("property_mapping_rules", ""),
            params.get("target_property", ""),
            params.get("target_class", ""),
            params.get("ignore_chars", ""),
            params.get("strict_duplicate_key_filter", ""),
        )
        if key in seen:
            continue
        seen.add(key)
        endpoint_key = params.get("target_endpoint", "wikidata")
        endpoint_label = (TARGET_ENDPOINTS.get(endpoint_key) or {}).get("label", endpoint_key)
        target_hint = params.get("target_property", "") or ("Target URL" if _is_wikidata_url_mode(params) else "")
        label = (
            f"{params.get('class_name','')} | {params.get('parts_spec','')} | "
            f"{params.get('wdc_predicate_pattern','')} -> "
            f"{target_hint} ({endpoint_label})"
        )
        recent.append({"label": label, "params": params, "job_id": r["id"]})
    return recent


def _fmt_ts(ts):
    if not ts:
        return None
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(ts)))
    except Exception:
        return None


def _fmt_size(num_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    n = float(max(0, num_bytes))
    for unit in units:
        if n < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(n)} {unit}"
            return f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{num_bytes} B"


def _count_lines(path: Path) -> int:
    if not path.exists() or not path.is_file():
        return 0
    c = 0
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for _ in f:
            c += 1
    return c


def _looks_like_ent_links_header(line: str) -> bool:
    line = (line or "").strip()
    if not line:
        return False
    parts = line.split("\t")
    if len(parts) < 2:
        return False
    left = parts[0].strip().lower()
    right = parts[1].strip().lower()
    return left in {"wdc_iri", "wdc", "wdc_entity"} and right in {"wikidata_uri", "wikidata", "wikidata_entity"}


def _count_ent_links_rows(path: Path) -> int:
    if not path.exists() or not path.is_file():
        return 0
    total = _count_lines(path)
    if total <= 0:
        return 0
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            first = f.readline()
        if _looks_like_ent_links_header(first):
            return max(0, total - 1)
    except Exception:
        pass
    return total


def _parse_nq_or_nt(line: str):
    line = (line or "").strip()
    if not line or line.startswith("#"):
        return None
    m = _QUAD_RE.match(line)
    if m:
        s, p, o, _g = m.groups()
        return s, p, o
    m = _TRIPLE_RE.match(line)
    if m:
        s, p, o = m.groups()
        return s, p, o
    return None


def _literal_lex(value: str):
    value = value or ""
    if not value.startswith('"'):
        return None
    escape = False
    for i in range(1, len(value)):
        ch = value[i]
        if escape:
            escape = False
            continue
        if ch == "\\":
            escape = True
            continue
        if ch == '"':
            return value[1:i]
    return None


def _normalize_preflight_value(raw_value: str, ignore_chars_text: str):
    v = align_script.normalize_for_matching(raw_value or "")
    if not v:
        return ""
    try:
        extra = align_script.parse_strip_list(ignore_chars_text or "")
    except Exception:
        extra = set()
    if " " in extra:
        v = v.replace(" ", "")
    for ch in extra:
        if ch and ch != " ":
            v = v.replace(ch, "")
    return v


def _parse_parts_spec_numbers(parts_spec: str):
    spec = _clean_text(parts_spec) or "all"
    if spec.lower() == "all":
        return None, None
    wanted = set()
    try:
        if "," in spec:
            for token in spec.split(","):
                token = token.strip()
                if not token:
                    continue
                wanted.add(int(token))
        elif "-" in spec:
            left, right = spec.split("-", 1)
            start = int(left.strip())
            end = int(right.strip())
            if end < start:
                start, end = end, start
            for n in range(start, end + 1):
                wanted.add(n)
        else:
            wanted.add(int(spec.strip()))
    except Exception:
        return None, f"Invalid parts spec: '{parts_spec}'. Use all, 0-10, or 0,2,4."
    return sorted(wanted), None


def _discover_local_part_files(class_name: str):
    class_dir = Path("Download") / (class_name or "")
    if not class_dir.exists() or not class_dir.is_dir():
        return []
    files = []
    for fp in sorted(class_dir.iterdir()):
        if not fp.is_file():
            continue
        if not fp.name.startswith("part_"):
            continue
        if _part_number_from_name(fp.name) is None:
            # Ignore ad-hoc files like part_sample, part_echantillon_vickey, etc.
            continue
        if not (fp.name.endswith(".nq") or fp.name.endswith(".nt") or "." not in fp.name):
            continue
        files.append(fp)
    return files


def _discover_local_wikidata_files(class_name: str):
    class_dir = Path("Download") / (class_name or "")
    if not class_dir.exists() or not class_dir.is_dir():
        return []
    preferred = []
    fallback = []
    for fp in sorted(class_dir.iterdir()):
        if not fp.is_file():
            continue
        name = fp.name.lower()
        if not name.startswith("wikidata_"):
            continue
        if not name.endswith(".nt"):
            continue
        if name.endswith("_sakey_input.nt"):
            preferred.append(fp)
        else:
            fallback.append(fp)
    return preferred or fallback


def _select_local_part_files(class_name: str, parts_spec: str):
    spec = _clean_text(parts_spec) or "all"
    if spec.lower() == "all":
        wd_files = _discover_local_wikidata_files(class_name)
        if wd_files:
            return wd_files, []

    files = _discover_local_part_files(class_name)
    if not files:
        return [], []
    wanted_numbers, parse_error = _parse_parts_spec_numbers(parts_spec)
    if parse_error:
        return [], [parse_error]
    if wanted_numbers is None:
        warnings = []
        try:
            online_numbers, _online_err = _discover_online_part_numbers(class_name)
        except Exception:
            online_numbers = []
        if online_numbers:
            allowed = set(int(n) for n in online_numbers)
            filtered = []
            removed = []
            for fp in files:
                num = _part_number_from_name(fp.name)
                if num is None:
                    continue
                if num in allowed:
                    filtered.append(fp)
                else:
                    removed.append(num)
            if filtered:
                files = sorted(filtered, key=lambda p: p.name)
                if removed:
                    warnings.append(
                        f"Ignored non-official local parts for class {class_name}: {_format_part_ranges(sorted(set(removed)))}."
                    )
        return files, warnings

    files_by_num = {}
    for fp in files:
        num = _part_number_from_name(fp.name)
        if num is None:
            continue
        files_by_num.setdefault(num, []).append(fp)

    selected = []
    missing = []
    for num in wanted_numbers:
        if num in files_by_num:
            selected.extend(files_by_num[num])
        else:
            missing.append(num)
    selected.sort(key=lambda p: p.name)

    warnings = []
    if missing:
        warnings.append(f"Requested local parts not found: {_format_part_ranges(missing)}.")
    if not selected:
        warnings.append("No local part file matches this parts spec.")
    return selected, warnings


def _sakey_runs_root():
    _SAKEY_RUNS_ROOT.mkdir(parents=True, exist_ok=True)
    return _SAKEY_RUNS_ROOT


def _sakey_run_dir(run_id: str):
    root = _sakey_runs_root().resolve()
    d = (root / _clean_text(run_id)).resolve()
    try:
        d.relative_to(root)
    except Exception:
        return None
    d.mkdir(parents=True, exist_ok=True)
    return d


def _sakey_meta_path(run_id: str):
    d = _sakey_run_dir(run_id)
    if not d:
        return None
    return d / "meta.json"


def _sakey_read_meta(run_id: str):
    p = _sakey_meta_path(run_id)
    if not p or not p.exists():
        return None
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _sakey_write_meta(run_id: str, payload: dict):
    p = _sakey_meta_path(run_id)
    if not p:
        return
    safe = payload if isinstance(payload, dict) else {}
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(safe, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(p)


def _sakey_update_meta(run_id: str, **fields):
    with _SAKEY_RUN_LOCK:
        meta = _sakey_read_meta(run_id) or {}
        meta.update(fields)
        _sakey_write_meta(run_id, meta)
        return meta


def _sakey_log(run_id: str, message: str):
    d = _sakey_run_dir(run_id)
    if not d:
        return
    ts = _fmt_ts(time.time())
    line = f"[{ts}] {str(message or '').rstrip()}\n"
    try:
        with (d / "run.log").open("a", encoding="utf-8", errors="ignore") as f:
            f.write(line)
    except Exception:
        pass


def _sakey_list_runs(limit: int = 30, class_name: str = ""):
    root = _sakey_runs_root()
    rows = []
    wanted = _clean_text(class_name)
    for child in root.iterdir():
        if not child.is_dir():
            continue
        meta = _sakey_read_meta(child.name)
        if not meta:
            continue
        if wanted and _clean_text(str(meta.get("class_name"))) != wanted:
            continue
        rows.append(meta)
    rows.sort(key=lambda r: float(r.get("created_at", 0.0) or 0.0), reverse=True)
    limit = max(1, min(int(limit or 30), _SAKEY_MAX_LIST))
    return rows[:limit]


def _sakey_reconcile_inflight_runs():
    global _SAKEY_RECONCILED
    if _SAKEY_RECONCILED:
        return
    with _SAKEY_RECONCILE_LOCK:
        if _SAKEY_RECONCILED:
            return
        root = _sakey_runs_root()
        now_ts = time.time()
        for child in root.iterdir():
            if not child.is_dir():
                continue
            run_id = child.name
            meta = _sakey_read_meta(run_id)
            if not meta:
                continue
            st = _clean_text(str(meta.get("status", ""))).lower()
            if st not in {"queued", "waiting", "running"}:
                continue
            created_at = float(meta.get("created_at", 0.0) or 0.0)
            # Runs that were active before this webapp process started are stale.
            if created_at and created_at < (_SAKEY_APP_BOOT_TS - 0.5):
                msg = "Run interrupted by server restart. Relaunch it if needed."
                _sakey_update_meta(
                    run_id,
                    status="error",
                    ended_at=now_ts,
                    error=msg,
                )
                _sakey_log(run_id, msg)
        _SAKEY_RECONCILED = True


def _sakey_find_active_duplicate(class_name: str, parts_spec: str, mins: int, timeout_hours: float):
    cname = _clean_text(class_name)
    pspec = _clean_text(parts_spec) or "all"
    mins_v = int(max(1, mins))
    tout_v = float(max(0.1, timeout_hours))
    root = _sakey_runs_root()
    best = None
    best_created = -1.0
    for child in root.iterdir():
        if not child.is_dir():
            continue
        meta = _sakey_read_meta(child.name)
        if not meta:
            continue
        st = _clean_text(str(meta.get("status", ""))).lower()
        if st not in {"queued", "waiting", "running"}:
            continue
        if _clean_text(str(meta.get("class_name", ""))) != cname:
            continue
        if (_clean_text(str(meta.get("parts_spec", ""))) or "all") != pspec:
            continue
        if int(meta.get("mins", 0) or 0) != mins_v:
            continue
        try:
            mt = float(meta.get("timeout_hours", 0.0) or 0.0)
        except Exception:
            mt = 0.0
        if abs(mt - tout_v) > 1e-9:
            continue
        created = float(meta.get("created_at", 0.0) or 0.0)
        if created > best_created:
            best_created = created
            best = _clean_text(str(meta.get("run_id", ""))) or child.name
    return best


def _sakey_list_legacy_runs(limit: int = 80):
    root = (Path(__file__).resolve().parents[1] / "vickey" / "runs").resolve()
    if not root.exists() or not root.is_dir():
        return []
    outs = []
    for p in root.rglob("*.out"):
        if not p.is_file():
            continue
        try:
            st = p.stat()
            ts = float(st.st_mtime)
        except Exception:
            ts = 0.0
        outs.append((ts, p))
    outs.sort(key=lambda x: x[0], reverse=True)
    out_rows = []
    for ts, p in outs[: max(1, min(int(limit or 80), 400))]:
        rel = str(p.relative_to(root))
        cls_guess = _clean_text(p.parent.name).replace("_", " ").title().replace(" ", "")
        run_id = f"legacy::{rel}"
        log_candidate = p.with_suffix(".log")
        out_rows.append(
            {
                "run_id": run_id,
                "class_name": cls_guess or "Unknown",
                "parts_spec": "legacy",
                "mins": "",
                "timeout_hours": "",
                "status": "legacy",
                "created_at": ts,
                "created_at_h": _fmt_ts(ts) if ts > 0 else "",
                "started_at": None,
                "ended_at": ts,
                "error": "",
                "keys_candidates_count": 0,
                "key_summary": {},
                "legacy": True,
                "legacy_out_path": str(p),
                "legacy_log_path": str(log_candidate) if log_candidate.exists() else "",
            }
        )
    return out_rows


def _sakey_collect_class_options(test_mode: bool = False):
    rows = [dict(r) for r in db.list_wdc_classes()]
    out = []
    for r in rows:
        name = _clean_text(str(r.get("class_name", "")))
        if not name:
            continue
        if _is_test_class_name(name) != bool(test_mode):
            continue
        out.append(name)
    out.sort()
    return out


def _sakey_convert_to_nt(part_files, out_nt: Path, run_id: str):
    converted = 0
    bad = 0
    skipped_bnode = 0
    total = 0
    with out_nt.open("w", encoding="utf-8") as fo:
        for fp in part_files:
            _sakey_log(run_id, f"Convert: {fp}")
            with Path(fp).open("r", encoding="utf-8", errors="ignore") as f:
                for raw in f:
                    total += 1
                    line = raw.rstrip("\n")
                    m3 = _TRIPLE_RE.match(line)
                    if m3:
                        s = _clean_text(m3.group(1))
                        p = _clean_text(m3.group(2))
                        o = _clean_text(m3.group(3))
                        if s.startswith("_:") or o.startswith("_:"):
                            skipped_bnode += 1
                            continue
                        fo.write(f"{s} {p} {o} .\n")
                        converted += 1
                        continue
                    m4 = _QUAD_RE.match(line)
                    if m4:
                        s = _clean_text(m4.group(1))
                        p = _clean_text(m4.group(2))
                        o = _clean_text(m4.group(3))
                        if s.startswith("_:") or o.startswith("_:"):
                            skipped_bnode += 1
                            continue
                        fo.write(f"{s} {p} {o} .\n")
                        converted += 1
                        continue
                    bad += 1
    return {
        "total_lines": total,
        "converted_lines": converted,
        "skipped_lines": bad,
        "skipped_bnode_lines": skipped_bnode,
    }


def _sakey_parse_block_keys(raw_block: str):
    out = []
    if not raw_block:
        return out
    for m in re.finditer(r"\[([^\[\]]+)\]", raw_block):
        chunk = _clean_text(m.group(1))
        if not chunk:
            continue
        props = []
        for tok in chunk.split(","):
            pn = _normalize_prop_iri(tok)
            if pn:
                props.append(pn)
        if not props:
            continue
        out.append(props)
    return out


def _safe_float(value, default=None):
    try:
        if value is None:
            return default
        return float(str(value).strip())
    except Exception:
        return default


def _safe_int(value, default=None):
    try:
        if value is None:
            return default
        return int(str(value).strip())
    except Exception:
        return default


def _normalize_prop_iri(value: str) -> str:
    p = _clean_text(value)
    if not p:
        return ""
    if p.startswith("<") and p.endswith(">") and len(p) > 2:
        p = _clean_text(p[1:-1])
        if not p:
            return ""
    low = p.lower()
    if low.startswith("http://www.wikidata.org/prop/") or low.startswith("https://www.wikidata.org/prop/"):
        return low
    return p


def _normalize_sakey_order_by(value: str):
    key = _clean_text(value).lower()
    allowed = {"coverage_desc", "support_desc", "size_asc", "type_then_coverage"}
    if key not in allowed:
        return "coverage_desc"
    return key


def _parse_sakey_filter_params(
    order_by: str = "",
    min_support: str = "",
    only_almost: Optional[str] = None,
    max_key_size: str = "",
    q: str = "",
):
    return {
        "order_by": _normalize_sakey_order_by(order_by),
        "min_support": max(0, _safe_int(min_support, 0) or 0),
        "only_almost": _bool_from_any(only_almost),
        "max_key_size": max(0, _safe_int(max_key_size, 0) or 0),
        "q": _clean_text(q).lower(),
    }


def _sakey_compute_row_metrics(
    nt_path: Path,
    rows: list,
    max_lines: int = 400000,
):
    if not nt_path or not nt_path.exists() or not nt_path.is_file() or not rows:
        return {"subjects": None, "lines": 0, "sampled": False, "max_lines": max_lines}
    all_props = set()
    for row in rows:
        props = list(row.get("props") or [])
        for p in props:
            cp = _normalize_prop_iri(p)
            if cp:
                all_props.add(cp)
    if not all_props:
        return {"subjects": None, "lines": 0, "sampled": False, "max_lines": max_lines}
    prop_index = {p: i for i, p in enumerate(sorted(all_props))}
    subject_masks = {}
    lines = 0
    sampled = False
    with nt_path.open("r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            lines += 1
            if lines > max_lines:
                sampled = True
                break
            parts = raw.strip().split(" ", 2)
            if len(parts) < 3:
                continue
            s = _clean_text(parts[0])
            p = _normalize_prop_iri(parts[1])
            if not s or not p:
                continue
            idx = prop_index.get(p)
            if idx is None:
                continue
            prev = subject_masks.get(s, 0)
            subject_masks[s] = prev | (1 << idx)
    if not subject_masks:
        return {"subjects": 0, "lines": lines, "sampled": sampled, "max_lines": max_lines}
    mask_freq = Counter(subject_masks.values())
    subject_count = len(subject_masks)
    for row in rows:
        props = []
        for p in (row.get("props") or []):
            pn = _normalize_prop_iri(p)
            if pn in prop_index:
                props.append(pn)
        if not props:
            row["support_num"] = None
            row["coverage_num"] = None
            row["support"] = row.get("support") or "n/a"
            row["coverage"] = "n/a"
            row["sampled"] = sampled
            continue
        key_mask = 0
        for p in props:
            key_mask |= 1 << prop_index[p]
        support = 0
        for m, freq in mask_freq.items():
            if (m & key_mask) == key_mask:
                support += int(freq)
        coverage = float(support) / float(subject_count) if subject_count else 0.0
        row["support_num"] = support
        row["coverage_num"] = coverage
        row["support"] = str(support)
        row["coverage"] = f"{coverage:.4f}"
        row["sampled"] = sampled
    return {"subjects": subject_count, "lines": lines, "sampled": sampled, "max_lines": max_lines}


def _sakey_apply_filters_and_sort(rows: list, opts: dict):
    items = []
    q = _clean_text(str((opts or {}).get("q", ""))).lower()
    min_support = int((opts or {}).get("min_support", 0) or 0)
    only_almost = bool((opts or {}).get("only_almost", False))
    max_key_size = int((opts or {}).get("max_key_size", 0) or 0)
    for row in list(rows or []):
        rr = dict(row or {})
        rr["key_size"] = int(len(rr.get("props") or []))
        if only_almost and _clean_text(str(rr.get("type", rr.get("condition", "")))).lower() != "almost_key":
            continue
        support_num = _safe_int(rr.get("support_num"), None)
        if min_support > 0 and (support_num is None or support_num < min_support):
            continue
        if max_key_size > 0 and rr["key_size"] > max_key_size:
            continue
        if q:
            blob = " ".join(
                [
                    _clean_text(str(rr.get("key", ""))).lower(),
                    " ".join([_clean_text(str(p)).lower() for p in (rr.get("props") or [])]),
                ]
            )
            if q not in blob:
                continue
        items.append(rr)
    order_by = _normalize_sakey_order_by((opts or {}).get("order_by", "coverage_desc"))
    if order_by == "support_desc":
        items.sort(
            key=lambda r: (
                -float(_safe_float(r.get("support_num"), -1.0) or -1.0),
                -float(_safe_float(r.get("coverage_num"), -1.0) or -1.0),
                int(r.get("key_size", 0) or 0),
                _clean_text(str(r.get("key", ""))).lower(),
            )
        )
    elif order_by == "size_asc":
        items.sort(
            key=lambda r: (
                int(r.get("key_size", 0) or 0),
                -float(_safe_float(r.get("coverage_num"), -1.0) or -1.0),
                -float(_safe_float(r.get("support_num"), -1.0) or -1.0),
                _clean_text(str(r.get("key", ""))).lower(),
            )
        )
    elif order_by == "type_then_coverage":
        items.sort(
            key=lambda r: (
                0 if _clean_text(str(r.get("type", r.get("condition", "")))).lower() == "almost_key" else 1,
                -float(_safe_float(r.get("coverage_num"), -1.0) or -1.0),
                -float(_safe_float(r.get("support_num"), -1.0) or -1.0),
                int(r.get("key_size", 0) or 0),
                _clean_text(str(r.get("key", ""))).lower(),
            )
        )
    else:
        items.sort(
            key=lambda r: (
                -float(_safe_float(r.get("coverage_num"), -1.0) or -1.0),
                -float(_safe_float(r.get("support_num"), -1.0) or -1.0),
                int(r.get("key_size", 0) or 0),
                _clean_text(str(r.get("key", ""))).lower(),
            )
        )
    return items


def _sakey_parse_keys_from_output(out_path: Path, limit: int = 500):
    lines = []
    summary = {
        "conditional_keys_count": None,
        "keys_count": None,
        "non_keys_found": None,
    }
    text = ""
    try:
        text = out_path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        text = ""
    if text:
        m = _SAKEY_VICKEY_KEYS_RE.search(text)
        if m:
            summary["conditional_keys_count"] = int(m.group(1))
        m = _SAKEY_KEYS_RE.search(text)
        if m:
            summary["keys_count"] = int(m.group(1))
        m = _SAKEY_NON_KEYS_RE.search(text)
        if m:
            summary["non_keys_found"] = int(m.group(1))
        non_key_rows = []
        almost_key_rows = []
        for bm in _SAKEY_NON_KEYS_BLOCK_RE.finditer(text):
            non_key_rows.extend(_sakey_parse_block_keys(_clean_text(bm.group(2))))
        for bm in _SAKEY_ALMOST_KEYS_BLOCK_RE.finditer(text):
            almost_key_rows.extend(_sakey_parse_block_keys(_clean_text(bm.group(2))))
        if non_key_rows:
            summary["non_keys_found"] = len(non_key_rows)
        if almost_key_rows:
            summary["keys_count"] = len(almost_key_rows)
        for props in almost_key_rows:
            if len(lines) >= limit:
                break
            lines.append(
                {
                    "key": " + ".join(props),
                    "condition": "almost_key",
                    "type": "almost_key",
                    "support": "n/a",
                    "score": "",
                    "props": props,
                }
            )
        for props in non_key_rows:
            if len(lines) >= limit:
                break
            lines.append(
                {
                    "key": " + ".join(props),
                    "condition": "non_key",
                    "type": "non_key",
                    "support": "n/a",
                    "score": "",
                    "props": props,
                }
            )
        for raw in text.splitlines():
            if len(lines) >= limit:
                break
            if "\t" not in raw:
                continue
            row = [c.strip() for c in raw.split("\t")]
            if len(row) < 3:
                continue
            left = row[0]
            if not left:
                continue
            lleft = left.lower()
            if lleft.startswith("computing") or lleft.startswith("vickey found"):
                continue
            if lleft.startswith("we found") or lleft.startswith("key discovery"):
                continue
            item = {
                "key": left,
                "condition": row[1] if len(row) > 1 else "",
                "support": row[2] if len(row) > 2 else "",
                "score": row[3] if len(row) > 3 else "",
            }
            lines.append(item)
    return summary, lines


def _sakey_write_report_files(run_id: str, summary: dict, keys_rows):
    d = _sakey_run_dir(run_id)
    if not d:
        return None, None
    rep_json = d / "SAKEY_REPORT.json"
    rep_tsv = d / "SAKEY_KEYS.tsv"
    payload = {
        "run_id": run_id,
        "summary": summary or {},
        "keys_candidates": keys_rows or [],
    }
    rep_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    with rep_tsv.open("w", encoding="utf-8") as f:
        f.write("key\tcondition\ttype\tsupport\tcoverage\tkey_size\tscore\n")
        for row in keys_rows or []:
            f.write(
                f"{str(row.get('key','')).replace(chr(9), ' ')}\t"
                f"{str(row.get('condition','')).replace(chr(9), ' ')}\t"
                f"{str(row.get('type','')).replace(chr(9), ' ')}\t"
                f"{str(row.get('support','')).replace(chr(9), ' ')}\t"
                f"{str(row.get('coverage','')).replace(chr(9), ' ')}\t"
                f"{str(row.get('key_size','')).replace(chr(9), ' ')}\t"
                f"{str(row.get('score','')).replace(chr(9), ' ')}\n"
            )
    return rep_json, rep_tsv


def _sakey_fallback_discover_keys(nt_path: Path, out_path: Path):
    subjects = {}
    with nt_path.open("r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            line = _clean_text(raw)
            if not line:
                continue
            parts = line.split(" ", 2)
            if len(parts) < 3:
                continue
            subj = _clean_text(parts[0])
            pred = _normalize_prop_iri(parts[1])
            obj = _clean_text(parts[2])
            if not subj or not pred or not obj:
                continue
            subjects.setdefault(subj, {}).setdefault(pred, []).append(obj)

    subject_count = len(subjects)
    prop_support = Counter()
    prop_value_sets = {}
    for prop_map in subjects.values():
        for pred, values in prop_map.items():
            prop_support[pred] += 1
            joined = " | ".join(sorted(set(values)))
            prop_value_sets.setdefault(pred, []).append(joined)

    keys_rows = []
    for pred, value_rows in prop_value_sets.items():
        support = int(prop_support.get(pred, 0) or 0)
        if support <= 0:
            continue
        if len(set(value_rows)) != support:
            continue
        coverage = (float(support) / float(subject_count)) if subject_count else 0.0
        keys_rows.append(
            {
                "key": pred,
                "condition": "almost_key",
                "type": "almost_key",
                "support": str(support),
                "support_num": support,
                "coverage": f"{coverage:.4f}",
                "coverage_num": coverage,
                "key_size": 1,
                "score": f"{coverage:.4f}",
                "props": [pred],
            }
        )

    keys_rows.sort(
        key=lambda row: (
            -float(row.get("coverage_num", 0.0) or 0.0),
            -float(row.get("support_num", 0.0) or 0.0),
            _clean_text(str(row.get("key", ""))).lower(),
        )
    )
    summary = {
        "conditional_keys_count": 0,
        "keys_count": len(keys_rows),
        "non_keys_found": 0,
    }
    with out_path.open("w", encoding="utf-8", errors="ignore") as f:
        f.write("Fallback SAKEY runner used (external runner not found)\n")
        f.write(f"We found {len(keys_rows)} key(s)\n")
        f.write("key\tcondition\tsupport\tscore\n")
        for row in keys_rows:
            f.write(f"{row['key']}\t{row['condition']}\t{row['support']}\t{row['score']}\n")
    return summary, keys_rows


def _run_sakey_worker(run_id: str):
    meta = _sakey_read_meta(run_id) or {}
    class_name = _clean_text(str(meta.get("class_name", "")))
    parts_spec = _clean_text(str(meta.get("parts_spec", ""))) or "all"
    mins = int(meta.get("mins", 3) or 3)
    timeout_hours = float(meta.get("timeout_hours", 2.0) or 2.0)
    timeout_sec = max(60, int(timeout_hours * 3600))
    run_dir = _sakey_run_dir(run_id)
    if not run_dir:
        return
    out_path = run_dir / "sakey.out"
    nt_path = run_dir / "input.nt"
    _sakey_update_meta(run_id, status="waiting", started_at=None, ended_at=None, error="")
    _sakey_log(
        run_id,
        f"Waiting for worker slot class={class_name} parts={parts_spec} (capacity={_SAKEY_MAX_CONCURRENT})",
    )
    try:
        with _SAKEY_EXEC_SEMAPHORE:
            _sakey_update_meta(run_id, status="running", started_at=time.time(), ended_at=None, error="")
            _sakey_log(run_id, f"Run started class={class_name} parts={parts_spec} mins={mins} timeout_h={timeout_hours}")
            part_files, warnings = _select_local_part_files(class_name, parts_spec)
            if warnings:
                for w in warnings:
                    _sakey_log(run_id, f"Warning: {w}")
            if not part_files:
                raise RuntimeError("No local parts selected. Download parts first or adjust parts spec.")
            _sakey_update_meta(
                run_id,
                selected_parts=[str(Path(p)) for p in part_files[:200]],
                selected_parts_count=len(part_files),
            )

            conv = _sakey_convert_to_nt(part_files, nt_path, run_id)
            _sakey_update_meta(run_id, conversion=conv, nt_path=str(nt_path))
            _sakey_log(
                run_id,
                f"Conversion done lines={conv.get('total_lines',0)} converted={conv.get('converted_lines',0)} skipped={conv.get('skipped_lines',0)}",
            )
            if int(conv.get("converted_lines", 0) or 0) <= 0:
                raise RuntimeError("Conversion produced 0 valid triples for SAKEY (after filtering malformed/bnode triples).")

            sakey_root = (Path(__file__).resolve().parents[1] / "SAKEY").resolve()
            runner = sakey_root / "run_sakey.sh"
            if runner.exists():
                _sakey_log(run_id, "Running SAKEY")
                with out_path.open("w", encoding="utf-8", errors="ignore") as fout:
                    subprocess.run(
                        [
                            "bash",
                            str(runner),
                            str(nt_path),
                            str(mins),
                        ],
                        cwd=str(sakey_root),
                        check=True,
                        text=True,
                        stdout=fout,
                        stderr=subprocess.STDOUT,
                        timeout=timeout_sec,
                    )
                _sakey_log(run_id, f"SAKEY done. Output: {out_path}")
                key_summary, keys_rows = _sakey_parse_keys_from_output(out_path)
            else:
                _sakey_log(run_id, "SAKEY runner not found; using built-in fallback.")
                key_summary, keys_rows = _sakey_fallback_discover_keys(nt_path, out_path)
                _sakey_log(run_id, f"Fallback SAKEY done. Output: {out_path}")

            metrics = _sakey_compute_row_metrics(nt_path, keys_rows)
            key_summary = dict(key_summary or {})
            key_summary["subjects_count_sample"] = metrics.get("subjects")
            key_summary["metrics_lines_scanned"] = metrics.get("lines")
            key_summary["metrics_sampled"] = bool(metrics.get("sampled"))
            rep_json, rep_tsv = _sakey_write_report_files(run_id, key_summary, keys_rows)
            _sakey_update_meta(
                run_id,
                status="completed",
                ended_at=time.time(),
                output_path=str(out_path),
                report_json=str(rep_json) if rep_json else "",
                report_tsv=str(rep_tsv) if rep_tsv else "",
                key_summary=key_summary,
                keys_candidates_count=len(keys_rows),
            )
            _sakey_log(run_id, f"Completed. keys_candidates={len(keys_rows)}")
    except subprocess.TimeoutExpired:
        key_summary = {}
        keys_rows = []
        if out_path.exists():
            key_summary, keys_rows = _sakey_parse_keys_from_output(out_path)
        if keys_rows and nt_path.exists():
            metrics = _sakey_compute_row_metrics(nt_path, keys_rows)
            key_summary = dict(key_summary or {})
            key_summary["subjects_count_sample"] = metrics.get("subjects")
            key_summary["metrics_lines_scanned"] = metrics.get("lines")
            key_summary["metrics_sampled"] = bool(metrics.get("sampled"))
        rep_json = rep_tsv = None
        if keys_rows:
            rep_json, rep_tsv = _sakey_write_report_files(run_id, key_summary, keys_rows)
        _sakey_update_meta(
            run_id,
            status="timeout",
            ended_at=time.time(),
            output_path=str(out_path),
            report_json=str(rep_json) if rep_json else "",
            report_tsv=str(rep_tsv) if rep_tsv else "",
            key_summary=key_summary,
            keys_candidates_count=len(keys_rows),
        )
        _sakey_log(run_id, f"Timeout after {timeout_sec}s")
    except Exception as exc:
        key_summary = {}
        keys_rows = []
        if out_path.exists():
            key_summary, keys_rows = _sakey_parse_keys_from_output(out_path)
        if keys_rows and nt_path.exists():
            metrics = _sakey_compute_row_metrics(nt_path, keys_rows)
            key_summary = dict(key_summary or {})
            key_summary["subjects_count_sample"] = metrics.get("subjects")
            key_summary["metrics_lines_scanned"] = metrics.get("lines")
            key_summary["metrics_sampled"] = bool(metrics.get("sampled"))
        rep_json = rep_tsv = None
        if keys_rows:
            rep_json, rep_tsv = _sakey_write_report_files(run_id, key_summary, keys_rows)
        _sakey_update_meta(
            run_id,
            status="error",
            ended_at=time.time(),
            error=str(exc),
            output_path=str(out_path),
            report_json=str(rep_json) if rep_json else "",
            report_tsv=str(rep_tsv) if rep_tsv else "",
            key_summary=key_summary,
            keys_candidates_count=len(keys_rows),
        )
        _sakey_log(run_id, f"Error: {exc}")


def _enqueue_sakey_run(class_name: str, parts_spec: str, mins: int, timeout_hours: float):
    _sakey_reconcile_inflight_runs()
    duplicate = _sakey_find_active_duplicate(class_name, parts_spec, mins, timeout_hours)
    if duplicate:
        _sakey_log(duplicate, "Duplicate launch ignored (same class/parts/mins/timeout).")
        return duplicate

    run_id = f"sakey_{time.strftime('%Y%m%d_%H%M%S')}_{int(time.time() * 1000) % 1000:03d}"
    created_at = time.time()
    meta = {
        "run_id": run_id,
        "class_name": _clean_text(class_name),
        "parts_spec": _clean_text(parts_spec) or "all",
        "mins": int(max(1, mins)),
        "timeout_hours": float(max(0.1, timeout_hours)),
        "status": "queued",
        "created_at": created_at,
        "created_at_h": _fmt_ts(created_at),
        "started_at": None,
        "ended_at": None,
        "error": "",
        "keys_candidates_count": 0,
        "key_summary": {},
    }
    with _SAKEY_RUN_LOCK:
        _sakey_write_meta(run_id, meta)
    _sakey_log(run_id, "Queued")
    Thread(target=_run_sakey_worker, args=(run_id,), daemon=True).start()
    return run_id


def _sakey_resolve_artifact(run_id: str, name: str):
    d = _sakey_run_dir(run_id)
    if not d:
        return None
    safe = _clean_text(name)
    if not safe or "/" in safe or "\\" in safe or safe.startswith("."):
        return None
    p = (d / safe).resolve()
    try:
        p.relative_to(d.resolve())
    except Exception:
        return None
    if not p.exists() or not p.is_file():
        return None
    return p


def _sakey_tail_log(run_id: str, max_lines: int = 120):
    d = _sakey_run_dir(run_id)
    if not d:
        return []
    p = d / "run.log"
    if not p.exists() or not p.is_file():
        return []
    try:
        lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception:
        return []
    max_lines = max(10, min(int(max_lines or 120), 500))
    return lines[-max_lines:]


def _sakey_tail_file(path: Path, max_lines: int = 120):
    if not path or not path.exists() or not path.is_file():
        return []
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception:
        return []
    max_lines = max(10, min(int(max_lines or 120), 500))
    return lines[-max_lines:]


def _sakey_load_keys_candidates(run_id: str, limit: int = 500):
    d = _sakey_run_dir(run_id)
    if not d:
        return []
    report = d / "SAKEY_REPORT.json"
    if not report.exists() or not report.is_file():
        return []
    try:
        payload = json.loads(report.read_text(encoding="utf-8"))
    except Exception:
        return []
    rows = payload.get("keys_candidates")
    if not isinstance(rows, list):
        return []
    out = []
    for row in rows[: max(1, min(int(limit or 500), 2000))]:
        if not isinstance(row, dict):
            continue
        props = row.get("props")
        if not isinstance(props, list):
            props = []
            key_text = _clean_text(str(row.get("key", "")))
            if key_text and " + " in key_text:
                props = []
                for tok in key_text.split(" + "):
                    pn = _normalize_prop_iri(tok)
                    if pn:
                        props.append(pn)
        else:
            norm_props = []
            for tok in props:
                pn = _normalize_prop_iri(tok)
                if pn:
                    norm_props.append(pn)
            props = norm_props
        out.append(
            {
                "key": _clean_text(str(row.get("key", ""))),
                "condition": _clean_text(str(row.get("condition", ""))),
                "type": _clean_text(str(row.get("type", row.get("condition", "")))),
                "support": _clean_text(str(row.get("support", ""))),
                "support_num": _safe_int(row.get("support_num"), None),
                "coverage": _clean_text(str(row.get("coverage", ""))),
                "coverage_num": _safe_float(row.get("coverage_num"), None),
                "key_size": _safe_int(row.get("key_size"), len(props) if props else 0) or 0,
                "score": _clean_text(str(row.get("score", ""))),
                "props": props,
            }
        )
    return out


def _sakey_list_artifacts(run_id: str):
    d = _sakey_run_dir(run_id)
    if not d:
        return []
    out = []
    for name in ("run.log", "input.nt", "sakey.out", "input.tsv", "vickey.out", "SAKEY_REPORT.json", "SAKEY_KEYS.tsv"):
        p = d / name
        if not p.exists() or not p.is_file():
            continue
        try:
            st = p.stat()
            size_h = _fmt_size(int(st.st_size))
        except Exception:
            size_h = ""
        out.append(
            {
                "name": name,
                "size_h": size_h,
                "url": f"/sakey/runs/{quote(run_id, safe='')}/artifact/{quote(name, safe='')}",
            }
        )
    return out


def _sakey_page_payload(
    class_name: str = "",
    run_id: str = "",
    test_mode: bool = False,
    key_order_by: str = "",
    key_min_support: str = "",
    key_only_almost: Optional[str] = None,
    key_max_size: str = "",
    key_q: str = "",
):
    _sakey_reconcile_inflight_runs()
    key_filters = _parse_sakey_filter_params(
        order_by=key_order_by,
        min_support=key_min_support,
        only_almost=key_only_almost,
        max_key_size=key_max_size,
        q=key_q,
    )
    class_options = _sakey_collect_class_options(test_mode=bool(test_mode))
    selected_class = _clean_text(class_name)
    if selected_class and selected_class not in class_options:
        selected_class = ""

    runs = _sakey_list_runs(limit=80, class_name=selected_class or "")
    legacy_runs = _sakey_list_legacy_runs(limit=120)
    if selected_class:
        legacy_runs = [r for r in legacy_runs if _clean_text(str(r.get("class_name", ""))) == selected_class]
    runs.extend(legacy_runs)
    runs.sort(key=lambda r: float(r.get("created_at", 0.0) or 0.0), reverse=True)
    runs = runs[:50]
    active_states = {"queued", "waiting", "running"}
    active_jobs = []
    history_runs = []
    for r in runs:
        st = _clean_text(str(r.get("status", ""))).lower()
        if st in active_states:
            active_jobs.append(r)
        else:
            history_runs.append(r)
    selected_run = None
    wanted_run_id = _clean_text(run_id)
    if wanted_run_id:
        for r in runs:
            if _clean_text(str(r.get("run_id", ""))) == wanted_run_id:
                selected_run = r
                break
    if not selected_run and runs and selected_class:
        for r in runs:
            if _clean_text(str(r.get("class_name", ""))) == selected_class:
                selected_run = r
                break
    if not selected_run and runs and not selected_class:
        selected_run = runs[0]

    keys_candidates = []
    log_tail = []
    artifacts = []
    form_parts_spec = "all"
    form_mins = 3
    form_timeout_hours = 48.0
    if selected_run:
        sid = _clean_text(str(selected_run.get("run_id", "")))
        if not bool(selected_run.get("legacy")):
            form_parts_spec = _clean_text(str(selected_run.get("parts_spec", ""))) or "all"
            try:
                form_mins = max(1, int(selected_run.get("mins", 3) or 3))
            except Exception:
                form_mins = 3
            try:
                form_timeout_hours = max(0.1, float(selected_run.get("timeout_hours", 48.0) or 48.0))
            except Exception:
                form_timeout_hours = 48.0
        if bool(selected_run.get("legacy")):
            out_p = Path(str(selected_run.get("legacy_out_path", "")))
            _summary, keys_candidates = _sakey_parse_keys_from_output(out_p, limit=800)
            legacy_meta_path = out_p.with_suffix(".meta")
            if legacy_meta_path.exists() and keys_candidates:
                try:
                    meta_lines = legacy_meta_path.read_text(encoding="utf-8", errors="ignore").splitlines()
                    dataset_path = ""
                    for ln in meta_lines:
                        if ln.startswith("dataset="):
                            dataset_path = _clean_text(ln.split("=", 1)[1])
                            break
                    if dataset_path:
                        ds = (Path(__file__).resolve().parents[1] / "SAKEY" / dataset_path).resolve()
                        if ds.exists():
                            metrics = _sakey_compute_row_metrics(ds, keys_candidates)
                            ks = dict((selected_run or {}).get("key_summary") or {})
                            ks["subjects_count_sample"] = metrics.get("subjects")
                            ks["metrics_lines_scanned"] = metrics.get("lines")
                            ks["metrics_sampled"] = bool(metrics.get("sampled"))
                            if isinstance(selected_run, dict):
                                selected_run["key_summary"] = ks
                except Exception:
                    pass
            log_tail = _sakey_tail_file(out_p, max_lines=100)
            log_p = Path(str(selected_run.get("legacy_log_path", "")))
            if log_p.exists():
                log_tail = _sakey_tail_file(log_p, max_lines=100)
            artifacts = []
        else:
            keys_candidates = _sakey_load_keys_candidates(sid, limit=800)
            needs_metrics = bool(keys_candidates) and all(row.get("support_num") is None for row in keys_candidates)
            if needs_metrics:
                nt_path = Path(str((selected_run or {}).get("nt_path", "")))
                if nt_path.exists():
                    metrics = _sakey_compute_row_metrics(nt_path, keys_candidates)
                    key_summary = dict((selected_run or {}).get("key_summary") or {})
                    key_summary["subjects_count_sample"] = metrics.get("subjects")
                    key_summary["metrics_lines_scanned"] = metrics.get("lines")
                    key_summary["metrics_sampled"] = bool(metrics.get("sampled"))
                    rep_json, rep_tsv = _sakey_write_report_files(sid, key_summary, keys_candidates)
                    _sakey_update_meta(
                        sid,
                        key_summary=key_summary,
                        keys_candidates_count=len(keys_candidates),
                        report_json=str(rep_json) if rep_json else "",
                        report_tsv=str(rep_tsv) if rep_tsv else "",
                    )
                    if isinstance(selected_run, dict):
                        selected_run["key_summary"] = key_summary
            log_tail = _sakey_tail_log(sid, max_lines=100)
            artifacts = _sakey_list_artifacts(sid)

    keys_candidates = _sakey_apply_filters_and_sort(keys_candidates, key_filters)

    return {
        "class_options": class_options,
        "selected_class": selected_class,
        "sakey_max_concurrent": _SAKEY_MAX_CONCURRENT,
        "runs": runs,
        "active_jobs": active_jobs,
        "history_runs": history_runs,
        "selected_run": selected_run,
        "selected_run_id": _clean_text(str((selected_run or {}).get("run_id", ""))),
        "form_parts_spec": form_parts_spec,
        "form_mins": form_mins,
        "form_timeout_hours": form_timeout_hours,
        "key_filters": key_filters,
        "keys_candidates": keys_candidates,
        "log_tail": log_tail,
        "artifacts": artifacts,
    }


def _read_top_props(path: Path, limit: int = 5):
    rows = []
    if not path.exists() or not path.is_file():
        return rows
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        header_skipped = False
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split("\t")
            if not header_skipped:
                header_skipped = True
                first = parts[0].strip().lower() if parts else ""
                if first in {"predicate", "property"}:
                    continue
            prop = parts[0].strip() if parts else ""
            count_raw = parts[1].strip() if len(parts) > 1 else "0"
            try:
                count = int(count_raw)
            except Exception:
                count = 0
            label = parts[2].strip() if len(parts) > 2 else ""
            description = parts[3].strip() if len(parts) > 3 else ""
            rows.append(
                {
                    "property": prop,
                    "count": count,
                    "label": label,
                    "description": description,
                }
            )
            if len(rows) >= limit:
                break
    return rows


def _read_ent_links_samples(path: Path, limit: int = 5):
    rows = []
    if not path.exists() or not path.is_file():
        return rows
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 2:
                continue
            left = parts[0].strip()
            right = parts[1].strip()
            if _looks_like_ent_links_header(f"{left}\t{right}"):
                continue
            rows.append({"wdc_iri": left, "wikidata_uri": right})
            if len(rows) >= limit:
                break
    return rows


def _fetch_target_preview_values(
    target_property: str,
    target_class: str,
    target_endpoint: str,
    target_endpoint_url: str,
    target_prefixes: str,
    ignore_chars: str,
    limit: int = 1200,
):
    endpoint_key = _normalize_target_endpoint(target_endpoint)
    q_limit = max(100, min(int(limit), 5000))

    # Keep optimized dedicated query for Wikidata preview, unchanged behavior.
    if endpoint_key == "wikidata":
        prop = align_script.normalize_wikidata_property(target_property)
        if not prop:
            return []
        class_norm = align_script.normalize_wkd_class(target_class)
        class_filter = ""
        if class_norm:
            class_filter = f"""
      ?entity wdt:P31 ?type .
      ?type wdt:P279* {class_norm} .
    """
        query = f"""
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    SELECT DISTINCT ?value WHERE {{
      ?entity {prop} ?value .
      {class_filter}
    }}
    LIMIT {q_limit}
    """
        headers = {
            "Accept": "application/sparql-results+json",
            "User-Agent": "beam-preflight/1.0",
        }
        timeout_s = max(5, int(os.environ.get("PREFLIGHT_WIKIDATA_TIMEOUT", "25")))
        try:
            response = requests.post(
                align_script.WIKIDATA_ENDPOINT,
                data={"query": query, "format": "json"},
                headers=headers,
                timeout=timeout_s,
            )
            response.raise_for_status()
            loader = getattr(align_script, "_load_sparql_json_payload", None)
            if callable(loader):
                payload = loader(response.text)
            else:
                payload = json.loads(response.text)
        except Exception:
            return []

        rows = []
        seen_norm = set()
        bindings = (((payload or {}).get("results") or {}).get("bindings")) or []
        for item in bindings:
            value = str((((item or {}).get("value") or {}).get("value")) or "").strip()
            if not value:
                continue
            normalized = _normalize_preflight_value(value, ignore_chars)
            if not normalized or normalized in seen_norm:
                continue
            seen_norm.add(normalized)
            rows.append({"value": value[:180], "normalized": normalized})
            if len(rows) >= q_limit:
                break
        return rows

    fetch_target = getattr(align_script, "fetch_target_values", None)
    if not callable(fetch_target):
        return []
    target_map = fetch_target(
        target_property=target_property,
        target_class=target_class,
        target_prop_class=None,
        entity_iris=None,
        target_endpoint=endpoint_key,
        target_endpoint_url=_clean_text(target_endpoint_url),
        target_prefixes=_clean_text(target_prefixes),
    )
    if not isinstance(target_map, dict):
        return []
    rows = []
    for norm, entries in target_map.items():
        if not norm or not isinstance(entries, list) or not entries:
            continue
        first = entries[0]
        raw_value = str(first[0] if isinstance(first, (list, tuple)) and len(first) > 0 else "")
        normalized = _normalize_preflight_value(raw_value, ignore_chars) if raw_value else str(norm)
        if not normalized:
            continue
        rows.append({"value": raw_value[:180], "normalized": normalized})
        if len(rows) >= q_limit:
            break
    return rows


def _build_preflight_report(
    class_name: str,
    parts_spec: str,
    wdc_predicate_pattern: str,
    wdc_pattern_search_in: str,
    ignore_chars: str,
    matching_mode: str,
    use_local_only: bool,
    target_endpoint: str = "wikidata",
    target_endpoint_url: str = "",
    target_prefixes: str = "",
    property_mapping_rules: str = "",
    target_property: str = "",
    target_class: str = "",
    wikidata_property: str = "",
    wkd_class: str = "",
    include_wikidata_preview: bool = True,
    scan_limit_lines: int = 30000,
):
    class_name = _clean_text(class_name)
    parts_spec = _clean_text(parts_spec) or "all"
    pattern = _clean_text(wdc_predicate_pattern)
    pattern_search_in = _normalize_wdc_pattern_search_in(wdc_pattern_search_in)
    ignore_chars = _clean_text(ignore_chars)
    endpoint_key = _normalize_target_endpoint(target_endpoint)
    target_endpoint_url = _clean_text(target_endpoint_url)
    target_prefixes = _clean_text(target_prefixes)
    property_mapping_rules = _clean_text(property_mapping_rules)
    target_property = _clean_text(target_property or wikidata_property)
    target_class = _clean_text(target_class or wkd_class)
    mode_norm = _normalize_matching_mode(matching_mode)
    includes_sameas = _mode_includes_sameas(mode_norm)
    includes_property = _mode_includes_property(mode_norm)
    wdc_value_is_wikidata = mode_norm == "sameas"
    parsed_rules = []
    if property_mapping_rules:
        try:
            parsed_rules = _parse_property_mapping_rules_text(property_mapping_rules)
        except ValueError as exc:
            report = {
                "ok": False,
                "summary": str(exc),
                "risk": "high",
                "confidence": "low",
            }
            return report
    rules_include_sameas = any(_clean_text(str(r.get("mode", "property"))).lower() == "sameas" for r in parsed_rules)
    rules_include_property = any(_clean_text(str(r.get("mode", "property"))).lower() != "sameas" for r in parsed_rules)
    effective_includes_sameas = includes_sameas or rules_include_sameas
    effective_includes_property = includes_property if not parsed_rules else rules_include_property

    if effective_includes_property and parsed_rules:
        first_pair = parsed_rules[0]["pairs"][0]
        if not pattern:
            pattern = _clean_text(first_pair[0])
        if not target_property:
            target_property = _clean_text(first_pair[1])
            if "|" in target_property:
                alts = _split_target_property_alternatives(target_property)
                target_property = alts[0] if alts else target_property
    report = {
        "ok": False,
        "class_name": class_name,
        "parts_spec": parts_spec,
        "pattern": pattern,
        "pattern_search_in": pattern_search_in,
        "matching_mode": mode_norm,
        "target_endpoint": endpoint_key,
        "target_endpoint_url": target_endpoint_url,
        "target_prefixes": target_prefixes,
        "property_mapping_rules": property_mapping_rules,
        "target_property": target_property,
        "target_class": target_class,
        "wdc_value_is_wikidata": bool(wdc_value_is_wikidata or effective_includes_sameas),
        "scan_limit_lines": int(max(1000, scan_limit_lines)),
        "selected_files_count": 0,
        "selected_files": [],
        "scanned_lines": 0,
        "matched_triples": 0,
        "distinct_values": 0,
        "wikidata_url_like": 0,
        "sample_values": [],
        "top_unmatched_wdc_values": [],
        "close_wikidata_examples": [],
        "top_predicates": [],
        "invalid_wikidata_samples": [],
        "wikidata_preview_count": 0,
        "risk": "high",
        "confidence": "low",
        "warnings": [],
        "summary": "",
    }

    if not class_name:
        report["summary"] = "Class name is required."
        return report
    if not pattern:
        report["summary"] = "Considered pattern for WDC properties is required."
        return report

    selected_files, select_warnings = _select_local_part_files(class_name, parts_spec)
    report["warnings"].extend(select_warnings)
    if not selected_files:
        report["summary"] = "No local files available for preflight."
        return report

    selected_names = [fp.name for fp in selected_files]
    report["selected_files"] = selected_names[:20]
    report["selected_files_count"] = len(selected_files)
    if len(selected_names) > 20:
        report["warnings"].append(f"Preflight uses first 20 listed files out of {len(selected_names)} selected.")

    if not use_local_only:
        parts_info = _build_class_parts_info(class_name)
        missing_online = int(parts_info.get("not_downloaded_online_parts_count") or 0)
        if missing_online > 0:
            report["warnings"].append(
                "Preflight scans local files only; some online parts are not downloaded yet."
            )

    prepared_patterns = align_script.prepare_predicate_patterns(pattern)
    distinct_norm = set()
    value_counts = Counter()
    value_examples = {}
    predicate_counts = Counter()
    invalid_wikidata_samples = []
    sample_values = []
    wikidata_like_values = 0
    matched = 0
    scanned = 0
    scan_limit = int(max(1000, scan_limit_lines))

    for fp in selected_files:
        with fp.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                if scanned >= scan_limit:
                    break
                scanned += 1
                parsed = _parse_nq_or_nt(line)
                if not parsed:
                    continue
                _s, p_tok, o_tok = parsed
                predicate = p_tok.strip("<>")
                predicate_counts[predicate] += 1
                if o_tok.startswith('"'):
                    raw_value = _literal_lex(o_tok) or o_tok.strip('"')
                else:
                    raw_value = o_tok.strip("<>")
                if pattern_search_in == "value":
                    if not align_script.value_matches_prepared_patterns(raw_value, prepared_patterns):
                        continue
                else:
                    if not align_script.predicate_matches_prepared_patterns(predicate, prepared_patterns):
                        continue

                matched += 1
                if raw_value:
                    normalized = _normalize_preflight_value(raw_value, ignore_chars)
                    if normalized:
                        if normalized not in distinct_norm and len(sample_values) < 5:
                            sample_values.append(raw_value[:120])
                        value_counts[normalized] += 1
                        if normalized not in value_examples:
                            value_examples[normalized] = raw_value[:180]
                        distinct_norm.add(normalized)
                    if wdc_value_is_wikidata:
                        extractor = getattr(align_script, "extract_target_entity_iri", None)
                        if callable(extractor):
                            endpoint_iri = extractor(
                                raw_value,
                                target_endpoint=endpoint_key,
                                target_endpoint_url=target_endpoint_url,
                            )
                        else:
                            endpoint_iri = align_script.extract_wd_entity_iri(raw_value)
                        if endpoint_iri:
                            wikidata_like_values += 1
                        elif len(invalid_wikidata_samples) < 5:
                            invalid_wikidata_samples.append(raw_value[:160])
            if scanned >= scan_limit:
                break

    report["scanned_lines"] = scanned
    report["matched_triples"] = matched
    report["distinct_values"] = len(distinct_norm)
    report["wikidata_url_like"] = wikidata_like_values
    report["sample_values"] = sample_values
    report["invalid_wikidata_samples"] = invalid_wikidata_samples
    report["top_unmatched_wdc_values"] = [
        {
            "normalized": norm,
            "value": value_examples.get(norm, norm),
            "count": int(cnt),
        }
        for norm, cnt in value_counts.most_common(8)
    ]

    if scanned >= scan_limit:
        report["warnings"].append(f"Sample limit reached ({scan_limit:,} lines).")
    if matched == 0:
        report["top_predicates"] = [
            {"predicate": pred, "count": int(cnt)}
            for pred, cnt in predicate_counts.most_common(8)
        ]
        report["risk"] = "high"
        report["summary"] = "No triple matched the considered pattern for WDC properties in sampled local data."
    elif wdc_value_is_wikidata and wikidata_like_values == 0:
        report["risk"] = "high"
        report["summary"] = "Pattern matched, but no target endpoint URL-like values were found."
    elif len(distinct_norm) < 5:
        report["risk"] = "medium"
        report["summary"] = "Very few distinct values found; alignment risk is moderate."
    else:
        report["risk"] = "low"
        report["summary"] = "Signal looks good in sampled local data."

    if scanned >= 20000:
        report["confidence"] = "high"
    elif scanned >= 5000:
        report["confidence"] = "medium"
    else:
        report["confidence"] = "low"

    if (
        include_wikidata_preview
        and not wdc_value_is_wikidata
        and target_property
        and report["top_unmatched_wdc_values"]
    ):
        preview_rows = _fetch_target_preview_values(
            target_property=target_property,
            target_class=target_class,
                target_endpoint=endpoint_key,
                target_endpoint_url=target_endpoint_url,
                target_prefixes=target_prefixes,
                ignore_chars=ignore_chars,
                limit=1200,
            )
        report["wikidata_preview_count"] = len(preview_rows)
        if preview_rows:
            wd_norm_to_value = {}
            wd_norm_keys = []
            for row in preview_rows:
                norm = row.get("normalized")
                raw_value = row.get("value")
                if not norm:
                    continue
                if norm not in wd_norm_to_value:
                    wd_norm_to_value[norm] = raw_value
                    wd_norm_keys.append(norm)
            for row in report["top_unmatched_wdc_values"][:5]:
                norm = row.get("normalized")
                if not norm:
                    continue
                close_norms = difflib.get_close_matches(norm, wd_norm_keys, n=3, cutoff=0.72)
                if not close_norms:
                    continue
                report["close_wikidata_examples"].append(
                    {
                        "wdc_value": row.get("value"),
                        "wdc_count": row.get("count"),
                        "wikidata_candidates": [wd_norm_to_value[n] for n in close_norms],
                    }
                )
        else:
            report["warnings"].append("Could not fetch target endpoint preview values for preflight diagnostics.")

    report["ok"] = True
    return report


def _discover_local_class_rows(download_root: str = "Download"):
    root = Path(download_root)
    if not root.exists() or not root.is_dir():
        return []

    rows = []
    for class_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        parts = []
        full_graph = []
        try:
            for fp in class_dir.iterdir():
                if not fp.is_file():
                    continue
                name = fp.name
                if name.startswith("part_") and (name.endswith(".nq") or name.endswith(".nt") or "." not in name):
                    parts.append(fp)
                elif name.endswith("_full_graph.nq"):
                    full_graph.append(fp)
        except Exception:
            continue

        files = parts if parts else full_graph
        if not files:
            continue

        total_size = 0
        for fp in files:
            try:
                total_size += fp.stat().st_size
            except Exception:
                pass
        rows.append(
            {
                "class_name": class_dir.name,
                "num_parts": len(parts) if parts else len(full_graph),
                "size_human": _fmt_size(total_size),
            }
        )
    return rows


def _seed_wdc_classes_from_local_catalog():
    try:
        rows = load_wdc_classes_catalog()
    except Exception:
        return 0
    if not rows:
        return 0
    try:
        db.upsert_wdc_classes(rows)
    except Exception:
        return 0
    return len(rows)


def _refresh_wdc_classes_from_remote():
    rows = fetch_wdc_classes()
    if not rows:
        raise RuntimeError("WDC class refresh returned no rows")
    save_wdc_classes_catalog(rows)
    db.upsert_wdc_classes(rows)
    return len(rows)


def _part_number_from_name(name: str):
    if not name:
        return None
    m = _PART_HREF_RE.match(name) or _PART_NAME_RE.match(name)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _discover_local_part_numbers(class_name: str):
    class_dir = Path("Download") / (class_name or "")
    if not class_dir.exists() or not class_dir.is_dir():
        return []

    numbers = set()
    for fp in class_dir.iterdir():
        if not fp.is_file():
            continue
        name = fp.name
        if not name.startswith("part_"):
            continue
        if not (name.endswith(".nq") or name.endswith(".nt") or "." not in name):
            continue
        num = _part_number_from_name(name)
        if num is not None:
            numbers.add(num)
    return sorted(numbers)


@lru_cache(maxsize=256)
def _discover_online_part_numbers(class_name: str):
    if not class_name:
        return [], "class_name is empty"
    url = urljoin(WDC_PARTS_BASE_URL, f"{class_name}/")
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        numbers = set()
        for link in soup.find_all("a"):
            href = (link.get("href") or "").strip()
            num = _part_number_from_name(href)
            if num is not None:
                numbers.add(num)
        return sorted(numbers), None
    except Exception as exc:
        return [], str(exc)


def _format_part_ranges(values):
    if not values:
        return "—"
    nums = sorted(set(int(v) for v in values))
    chunks = []
    start = nums[0]
    prev = nums[0]
    for n in nums[1:]:
        if n == prev + 1:
            prev = n
            continue
        chunks.append(f"{start}-{prev}" if start != prev else str(start))
        start = prev = n
    chunks.append(f"{start}-{prev}" if start != prev else str(start))
    if len(chunks) > 28:
        return ", ".join(chunks[:28]) + f", ... (+{len(chunks)-28} ranges)"
    return ", ".join(chunks)


def _format_part_list(values, limit=60):
    if not values:
        return "—"
    nums = [int(v) for v in sorted(set(values))]
    if len(nums) <= limit:
        return ", ".join(str(v) for v in nums)
    return ", ".join(str(v) for v in nums[:limit]) + f", ... (+{len(nums)-limit})"


def _class_meta_by_name(class_name: str):
    for row in db.list_wdc_classes():
        if row["class_name"] == class_name:
            return dict(row)
    return None


def _build_class_parts_info(class_name: str):
    class_name = _clean_text(class_name)
    local_numbers = _discover_local_part_numbers(class_name)
    online_numbers, online_error = _discover_online_part_numbers(class_name)
    local_set = set(local_numbers)
    meta = _class_meta_by_name(class_name) or {}
    class_num_parts = meta.get("num_parts")
    try:
        class_num_parts = int(class_num_parts) if class_num_parts is not None else None
    except Exception:
        class_num_parts = None

    online_set = set(online_numbers)
    inferred_online_set = set(online_set)
    inferred_from_catalog = False

    if online_numbers:
        start_num = min(online_numbers)
    elif local_numbers:
        start_num = min(local_numbers)
    else:
        start_num = 0

    catalog_expected_numbers = []
    if class_num_parts and class_num_parts > 0:
        catalog_expected_numbers = list(range(start_num, start_num + class_num_parts))
        catalog_set = set(catalog_expected_numbers)
        if not inferred_online_set:
            inferred_online_set = set(catalog_set)
            inferred_from_catalog = True
        elif len(inferred_online_set) < class_num_parts:
            # Online listing can be incomplete; complete the expected contiguous range using catalog count.
            inferred_online_set |= catalog_set
            inferred_from_catalog = True

    if inferred_online_set:
        downloaded_numbers = sorted(local_set & inferred_online_set)
    else:
        downloaded_numbers = list(local_numbers)
    not_downloaded_online_numbers = sorted(inferred_online_set - local_set)
    local_only_numbers = sorted(local_set - inferred_online_set) if inferred_online_set else []

    return {
        "class_name": class_name,
        "class_num_parts": class_num_parts,
        "class_size_human": meta.get("size_human"),
        "online_error": online_error,
        "online_available_count": len(inferred_online_set),
        "online_available_numbers": sorted(inferred_online_set),
        "online_available_numbers_text": _format_part_list(sorted(inferred_online_set)),
        "online_available_ranges": _format_part_ranges(sorted(inferred_online_set)),
        "online_discovered_count": len(online_numbers),
        "online_discovered_numbers": online_numbers,
        "online_discovered_numbers_text": _format_part_list(online_numbers),
        "online_discovered_ranges": _format_part_ranges(online_numbers),
        "online_inferred_from_catalog": inferred_from_catalog,
        "catalog_expected_numbers": catalog_expected_numbers,
        "catalog_expected_ranges": _format_part_ranges(catalog_expected_numbers),
        "downloaded_parts_count": len(downloaded_numbers),
        "downloaded_part_numbers": downloaded_numbers,
        "downloaded_part_numbers_text": _format_part_list(downloaded_numbers),
        "downloaded_part_ranges": _format_part_ranges(downloaded_numbers),
        "not_downloaded_online_parts_count": len(not_downloaded_online_numbers),
        "not_downloaded_online_part_numbers": not_downloaded_online_numbers,
        "not_downloaded_online_part_numbers_text": _format_part_list(not_downloaded_online_numbers),
        "not_downloaded_online_part_ranges": _format_part_ranges(not_downloaded_online_numbers),
        "local_only_parts_count": len(local_only_numbers),
        "local_only_part_numbers": local_only_numbers,
        "local_only_part_numbers_text": _format_part_list(local_only_numbers),
    }


def _variant_stats(base: Path, variant: str):
    p = base / variant
    if not p.exists() or not p.is_dir():
        return None
    files = {
        "ent_links": p / "ent_links",
        "attr_triples_1": p / "attr_triples_1",
        "rel_triples_1": p / "rel_triples_1",
        "attr_triples_2": p / "attr_triples_2",
        "rel_triples_2": p / "rel_triples_2",
        "prop_stats_wdc": p / "prop_stats_wdc.tsv",
        "prop_stats_wd": p / "prop_stats_wd.tsv",
    }
    size_total = 0
    for fp in files.values():
        if fp.exists() and fp.is_file():
            try:
                size_total += fp.stat().st_size
            except Exception:
                pass
    links_count = _count_ent_links_rows(files["ent_links"])
    wd_props = max(0, _count_lines(files["prop_stats_wd"]) - 1)
    wdc_props = max(0, _count_lines(files["prop_stats_wdc"]) - 1)
    top_wdc_props = _read_top_props(files["prop_stats_wdc"], limit=5)
    top_wd_props = _read_top_props(files["prop_stats_wd"], limit=5)
    sample_links = _read_ent_links_samples(files["ent_links"], limit=5)
    qa_warnings = []
    if links_count == 0:
        qa_warnings.append("No entity links generated.")
    if wdc_props == 0:
        qa_warnings.append("No WDC property stats found.")
    if wd_props == 0:
        qa_warnings.append("No target-side property stats found.")
    if links_count > 0 and not sample_links:
        qa_warnings.append("Could not read ent_links samples.")
    return {
        "name": variant,
        "path": str(p),
        "size_total_b": size_total,
        "size_total_h": _fmt_size(size_total),
        "links_count": links_count,
        "wd_props": wd_props,
        "wdc_props": wdc_props,
        "sample_links": sample_links,
        "top_wdc_props": top_wdc_props,
        "top_wd_props": top_wd_props,
        "qa_warnings": qa_warnings,
        "files": {k: str(v) for k, v in files.items() if v.exists()},
    }


def _load_build_stats(base: Path):
    stats_path = base / "BUILD_STATS.json"
    if not stats_path.exists() or not stats_path.is_file():
        return {}
    try:
        raw = json.loads(stats_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def _source_label_from_config(config: dict):
    cfg = config if isinstance(config, dict) else {}
    mode = _normalize_matching_mode(
        _clean_text(str(cfg.get("matching_mode", ""))),
        fallback_wdc_value_is_wikidata=_is_wikidata_url_mode(cfg),
    )
    if mode == "sameas":
        return "via sameas"

    combos = _extract_linking_combinations(cfg)
    if len(combos) == 1:
        combo = combos[0] if isinstance(combos[0], dict) else {}
        row_mode = _clean_text(str(combo.get("mode", ""))).lower()
        if row_mode == "sameas":
            return "via sameas"
        pairs = combo.get("pairs") or []
        if pairs and isinstance(pairs[0], dict):
            left = _clean_text(str(pairs[0].get("wdc", "")))
            if left:
                return f"via {left.lower()}"

    fallback = _clean_text(str(cfg.get("wdc_predicate_pattern", "")))
    if fallback:
        return f"via {fallback.lower()}"
    return "via unknown"


def _backfill_link_source_stats_if_missing(
    base: Path,
    build_stats: dict,
    build_config: dict,
    links_after: int,
):
    if not isinstance(build_stats, dict):
        build_stats = {}
    rows_after = build_stats.get("links_by_source_after_filter")
    rows_align = build_stats.get("links_by_source_align")
    has_after = isinstance(rows_after, list) and any(isinstance(r, dict) for r in rows_after)
    has_align = isinstance(rows_align, list) and any(isinstance(r, dict) for r in rows_align)
    if has_after and has_align:
        return build_stats

    source_label = _source_label_from_config(build_config if isinstance(build_config, dict) else {})
    links_after = max(0, int(links_after or 0))
    try:
        links_before = max(0, int(build_stats.get("links_before_filters", 0)))
    except Exception:
        links_before = 0

    changed = False
    if not has_after and links_after >= 0:
        build_stats["links_by_source_after_filter"] = [{"source": source_label, "count": links_after}]
        changed = True
    if not has_align:
        align_count = links_before if links_before > 0 else links_after
        build_stats["links_by_source_align"] = [{"source": source_label, "count": align_count}]
        changed = True
    if not changed:
        return build_stats

    try:
        stats_path = base / "BUILD_STATS.json"
        stats_path.write_text(json.dumps(build_stats, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass
    return build_stats


def _format_link_source_stats(stats: dict):
    if not isinstance(stats, dict):
        return ""
    rows = stats.get("links_by_source_after_filter")
    if not isinstance(rows, list) or not rows:
        rows = stats.get("links_by_source_align")
    if not isinstance(rows, list) or not rows:
        return ""
    parts = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        src = _clean_text(str(row.get("source", "")))
        try:
            cnt = int(row.get("count", 0))
        except Exception:
            cnt = 0
        if not src:
            continue
        parts.append(f"{cnt} {src}")
    return " | ".join(parts)


def _scan_builds(limit=30):
    builds = []
    root = Path("data")
    if not root.exists():
        return builds
    candidates = []
    for class_dir in root.iterdir():
        if not class_dir.is_dir():
            continue
        for base in class_dir.iterdir():
            if not _is_build_dir_candidate(base):
                continue
            candidates.append(base)
    for base in candidates:
        summary = _build_summary_from_dir(base)
        if summary:
            builds.append(summary)
    builds.sort(key=lambda b: float(b.get("sort_ts") or 0.0), reverse=True)
    if limit and int(limit) > 0:
        builds = builds[: int(limit)]
    for b in builds:
        b.pop("sort_ts", None)
    return builds


def _build_config_groups(cfg: dict):
    if not isinstance(cfg, dict):
        return []
    ordered = [
        ("Input", ["class_name"]),
        (
            "Matching",
            [
                "matching_mode",
                "wdc_predicate_pattern",
                "wdc_pattern_search_in",
                "target_endpoint",
                "target_endpoint_url",
                "target_prefixes",
                "property_mapping_rules",
                "target_property",
                "target_class",
                "ignore_chars",
            ],
        ),
        (
            "Build",
            [
                "force_align",
                "use_local_only",
                "strict_duplicate_key_filter",
                "build_name",
                "result_path",
            ],
        ),
    ]
    used = set()
    groups = []
    for title, keys in ordered:
        items = []
        for k in keys:
            if k in cfg:
                items.append((k, cfg[k]))
                used.add(k)
        if items:
            groups.append({"title": title, "items": items})
    ignored = {
        "parts_spec",
        "parts_count",
        "parts_total_size_human",
        "parts_total_size_bytes",
        "parts_manifest",
    }
    other = [(k, v) for k, v in cfg.items() if (k not in used and k not in ignored)]
    if other:
        groups.append({"title": "Other", "items": other})
    return groups


def _safe_unlink(path: str):
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def _resolve_build_dir(class_name: str, build_name: str):
    data_root = Path("data").resolve()
    base = (data_root / class_name / build_name).resolve()
    try:
        base.relative_to(data_root)
    except ValueError:
        return None
    if not _is_build_dir_candidate(base):
        return None
    return base


def _is_build_dir_candidate(base: Path) -> bool:
    if not base or not base.exists() or not base.is_dir():
        return False
    if not base.name.lower().startswith("beam"):
        return False
    marker = base / "BUILD_DONE"
    # History must only show completed builds.
    return marker.exists() and marker.is_file()


def _read_text_head(path: Path, max_chars: int = 12000):
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            return f.read(max_chars)
    except Exception:
        return ""


def _collect_sakey_insights(build_dir: Path):
    if not build_dir or not build_dir.exists() or not build_dir.is_dir():
        return {"available": False, "summary_lines": [], "artifacts": [], "primary": None}

    candidate_roots = []
    for rel in ("sakey", "SAKEY", "vickey", "VICKEY", "."):
        p = build_dir / rel
        if p.exists():
            candidate_roots.append(p)

    artifacts = []
    seen = set()
    allowed_suffixes = {".json", ".tsv", ".txt", ".out", ".log", ".csv"}
    for root in candidate_roots:
        if root.is_file():
            files = [root]
        else:
            try:
                files = [p for p in root.rglob("*") if p.is_file()]
            except Exception:
                files = []
        for p in files:
            try:
                rel = p.relative_to(build_dir)
            except Exception:
                continue
            rel_s = str(rel)
            if not _SAKEY_FILE_RE.search(rel_s):
                continue
            if p.suffix.lower() not in allowed_suffixes:
                continue
            if rel_s in seen:
                continue
            seen.add(rel_s)
            try:
                st = p.stat()
                mtime = float(st.st_mtime)
                size_b = int(st.st_size)
            except Exception:
                mtime = 0.0
                size_b = 0
            artifacts.append(
                {
                    "relative_path": rel_s,
                    "size_h": _fmt_size(size_b),
                    "size_b": size_b,
                    "mtime": mtime,
                    "mtime_h": _fmt_ts(mtime) if mtime > 0 else "",
                }
            )

    artifacts.sort(key=lambda a: (float(a.get("mtime") or 0.0), a.get("relative_path", "")), reverse=True)
    if not artifacts:
        return {"available": False, "summary_lines": [], "artifacts": [], "primary": None}

    primary = dict(artifacts[0])
    summary_lines = []
    primary_path = build_dir / primary["relative_path"]
    suffix = primary_path.suffix.lower()
    if suffix == ".json":
        try:
            payload = json.loads(primary_path.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
        if isinstance(payload, dict):
            ckeys = payload.get("conditional_keys")
            keys = payload.get("keys")
            if isinstance(ckeys, list):
                summary_lines.append(f"{len(ckeys)} conditional keys")
            if isinstance(keys, list):
                summary_lines.append(f"{len(keys)} keys")
            runtime = payload.get("runtime") or payload.get("runtime_seconds")
            if runtime is not None:
                summary_lines.append(f"runtime: {runtime}")
            if not summary_lines:
                summary_lines.append("JSON report detected")
        else:
            summary_lines.append("JSON report detected")
    else:
        head = _read_text_head(primary_path)
        m = _SAKEY_VICKEY_KEYS_RE.search(head)
        if m:
            summary_lines.append(f"{int(m.group(1))} conditional keys")
        m = _SAKEY_KEYS_RE.search(head)
        if m:
            summary_lines.append(f"{int(m.group(1))} keys")
        m = _SAKEY_NON_KEYS_RE.search(head)
        if m:
            summary_lines.append(f"{int(m.group(1))} non-keys found")
        if suffix == ".tsv":
            try:
                nrows = max(0, _count_lines(primary_path) - 1)
                summary_lines.append(f"{nrows} rows")
            except Exception:
                pass
        if not summary_lines:
            summary_lines.append("Text report detected")

    for idx, item in enumerate(artifacts):
        item["download_url"] = (
            f"/builds/{quote(build_dir.parent.name, safe='')}/{quote(build_dir.name, safe='')}/sakey/download/{idx}"
        )

    return {
        "available": True,
        "summary_lines": summary_lines[:5],
        "artifacts": artifacts[:25],
        "primary": primary,
    }


def _resolve_sakey_artifact(build_dir: Path, artifact_idx: int):
    info = _collect_sakey_insights(build_dir)
    artifacts = info.get("artifacts") or []
    if not artifacts:
        return None
    try:
        idx = int(artifact_idx)
    except Exception:
        return None
    if idx < 0 or idx >= len(artifacts):
        return None
    rel = str(artifacts[idx].get("relative_path", "")).strip()
    if not rel:
        return None
    path = (build_dir / rel).resolve()
    try:
        path.relative_to(build_dir.resolve())
    except Exception:
        return None
    if not path.exists() or not path.is_file():
        return None
    return path


def _build_summary_from_dir(base: Path):
    if not _is_build_dir_candidate(base):
        return None
    marker = base / "BUILD_DONE"
    done_at = ""
    sort_ts = 0.0
    if marker.exists() and marker.is_file():
        try:
            st = marker.stat()
            done_at = _fmt_ts(st.st_mtime)
            sort_ts = float(st.st_mtime)
        except Exception:
            done_at = ""
            sort_ts = 0.0

    build_config = None
    cfg_path = base / "BUILD_CONFIG.json"
    if cfg_path.exists():
        try:
            build_config = json.loads(cfg_path.read_text(encoding="utf-8"))
        except Exception:
            build_config = None

    with_link = _variant_stats(base, "with_link_code")
    without_link = _variant_stats(base, "without_link_code")
    build_stats = _load_build_stats(base)
    links_after = 0
    if with_link:
        links_after = int(with_link.get("links_count", 0) or 0)
    elif without_link:
        links_after = int(without_link.get("links_count", 0) or 0)
    build_stats = _backfill_link_source_stats_if_missing(base, build_stats, build_config or {}, links_after)
    if not marker.exists() and not build_config and not with_link and not without_link:
        return None

    if sort_ts <= 0:
        for p in (cfg_path, base / "with_link_code", base / "without_link_code", base):
            try:
                if p.exists():
                    sort_ts = max(sort_ts, float(p.stat().st_mtime))
            except Exception:
                pass
    if not done_at and sort_ts > 0:
        done_at = _fmt_ts(sort_ts)

    variants_same = False
    if with_link and without_link:
        variants_same = (
            with_link["size_total_b"] == without_link["size_total_b"]
            and with_link["links_count"] == without_link["links_count"]
            and with_link["wdc_props"] == without_link["wdc_props"]
            and with_link["wd_props"] == without_link["wd_props"]
        )

    build = {
        "class_name": base.parent.name,
        "build_name": base.name,
        "path": str(base),
        "done_at": done_at,
        "is_completed": bool(marker.exists()),
        "done_label": "Completed" if marker.exists() else "Last update",
        "with_link": with_link,
        "without_link": without_link,
        "variants_same": variants_same,
        "build_config": build_config,
        "build_stats": build_stats,
        "linking_stats_text": _format_link_source_stats(build_stats),
        "sort_ts": sort_ts,
    }

    config = build_config if isinstance(build_config, dict) else None
    if config:
        _sync_target_alias_fields(config)
        build["config"] = config
    else:
        build["config"] = {
            "class_name": build["class_name"],
            "build_name": build["build_name"],
            "result_path": build["path"],
            "config_source": "inferred",
            "wdc_pattern_search_in": "predicate",
            "target_endpoint": "wikidata",
            "target_endpoint_url": "",
            "target_prefixes": "",
            "property_mapping_rules": "",
            "target_property": "",
            "target_class": "",
        }

    parts = build["config"].get("parts_manifest")
    if not isinstance(parts, list):
        parts = []
    build["parts_manifest"] = parts
    build["parts_count"] = build["config"].get("parts_count", len(parts))
    build["parts_total_size_human"] = build["config"].get("parts_total_size_human")
    build["config_groups"] = _build_config_groups(build["config"])
    build["linking_combinations"] = _extract_linking_combinations(build["config"])
    endpoint_key = _normalize_target_endpoint(_clean_text(str(build["config"].get("target_endpoint", "wikidata"))))
    endpoint_label = _clean_text(str((TARGET_ENDPOINTS.get(endpoint_key) or {}).get("label", ""))) or "Wikidata"
    linking_elements = _extract_linking_elements(build["config"])
    build["endpoint_label"] = endpoint_label
    build["linking_elements"] = linking_elements
    build["linking_elements_text"] = ", ".join(linking_elements)
    build["sakey"] = _collect_sakey_insights(base)
    return build


_LINK_EXPLORER_VARIANTS = ("with_link_code", "without_link_code")
_LINK_EXPLORER_FAST_SCAN_BYTES = 64 * 1024 * 1024  # 64 MB
_LINK_DETAIL_CACHE_MAX = 256
_LINK_DETAIL_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="link-detail")
_LINK_DETAIL_CACHE = OrderedDict()
_LINK_DETAIL_FUTURES = {}
_LINK_DETAIL_LOCK = Lock()
_LINK_EXPLORER_PROP_ALIASES = {
    "name": "label",
    "label": "label",
    "rdfslabel": "label",
    "preflabel": "label",
    "altlabel": "label",
    "title": "label",
    "description": "description",
    "schemaorgdescription": "description",
    "telephone": "phone",
    "phone": "phone",
    "contactpoint": "phone",
    "p1329": "phone",
    "iatacode": "iata",
    "iataairportcode": "iata",
    "p238": "iata",
    "icaocode": "icao",
    "icaoairportcode": "icao",
    "p239": "icao",
    "faaairportcode": "faa",
    "p240": "faa",
    "sameas": "sameas",
    "url": "url",
    "website": "url",
    "officialwebsite": "url",
    "p856": "url",
    "identifier": "identifier",
    "code": "identifier",
    "eidr": "identifier",
    "p2704": "identifier",
}


def _link_detail_cache_key(build_dir: Path, variant_name: str, idx: int) -> str:
    try:
        build_key = str(build_dir.resolve())
    except Exception:
        build_key = str(build_dir)
    return f"{build_key}|{_clean_text(variant_name)}|{int(idx)}"


def _link_detail_cache_get(key: str):
    with _LINK_DETAIL_LOCK:
        payload = _LINK_DETAIL_CACHE.get(key)
        if payload is not None:
            _LINK_DETAIL_CACHE.move_to_end(key)
        return payload


def _link_detail_cache_set(key: str, payload: dict):
    if payload is None:
        return
    with _LINK_DETAIL_LOCK:
        _LINK_DETAIL_CACHE[key] = payload
        _LINK_DETAIL_CACHE.move_to_end(key)
        while len(_LINK_DETAIL_CACHE) > _LINK_DETAIL_CACHE_MAX:
            _LINK_DETAIL_CACHE.popitem(last=False)


def _start_link_detail_build(
    build_dir: Path,
    variant_dir: Path,
    variant_name: str,
    idx: int,
):
    key = _link_detail_cache_key(build_dir, variant_name, idx)
    cached = _link_detail_cache_get(key)
    if cached is not None:
        return key, "ready", cached, None

    with _LINK_DETAIL_LOCK:
        fut = _LINK_DETAIL_FUTURES.get(key)
        if fut is None or fut.cancelled():
            fut = _LINK_DETAIL_EXECUTOR.submit(_build_link_detail_payload, variant_dir, idx)
            _LINK_DETAIL_FUTURES[key] = fut
    return key, "pending", None, fut


def _read_link_detail_future(
    key: str,
    fut,
    wait_ms: int = 0,
):
    timeout_s = max(0.0, float(wait_ms) / 1000.0)
    if timeout_s <= 0 and not fut.done():
        return None, "pending"
    try:
        payload = fut.result(timeout=timeout_s if timeout_s > 0 else None)
    except FuturesTimeoutError:
        return None, "pending"
    except Exception:
        with _LINK_DETAIL_LOCK:
            _LINK_DETAIL_FUTURES.pop(key, None)
        raise

    with _LINK_DETAIL_LOCK:
        _LINK_DETAIL_FUTURES.pop(key, None)
    if payload is not None:
        _link_detail_cache_set(key, payload)
    return payload, "ready"


def _normalize_node_token(value: str) -> str:
    raw = _clean_text(value).strip().strip("<>").strip()
    if not raw:
        return ""
    try:
        wd_iri = align_script.extract_wd_entity_iri(raw)
    except Exception:
        wd_iri = None
    if wd_iri:
        return wd_iri
    return raw


def _short_predicate(value: str) -> str:
    text = _clean_text(value).strip().strip("<>")
    if not text:
        return ""
    if "#" in text:
        text = text.rsplit("#", 1)[-1]
    if "/" in text:
        text = text.rsplit("/", 1)[-1]
    if ":" in text and "/" not in text and "#" not in text:
        text = text.split(":", 1)[-1]
    return text


def _predicate_token(value: str) -> str:
    raw = _short_predicate(value).lower()
    return re.sub(r"[^a-z0-9]+", "", raw)


def _predicate_alias_key(value: str) -> str:
    token = _predicate_token(value)
    return _LINK_EXPLORER_PROP_ALIASES.get(token, token)


def _normalize_property_key(value: str) -> str:
    return _clean_text(value).strip().strip("<>").lower()


def _extract_wikidata_property_id(predicate: str):
    raw = _clean_text(predicate).strip().strip("<>")
    if not raw:
        return ""
    m = re.search(r"([Pp]\d+)$", raw)
    if not m:
        return ""
    return m.group(1).upper()


def _extract_wikidata_entity_id(value: str):
    raw = _clean_text(value).strip().strip("<>")
    if not raw:
        return ""
    direct = re.fullmatch(r"([QqPp]\d+)", raw)
    if direct:
        return direct.group(1).upper()
    iri_match = re.search(r"/entity/([QqPp]\d+)$", raw)
    if iri_match:
        return iri_match.group(1).upper()
    return ""


@lru_cache(maxsize=4096)
def _fetch_wikidata_entity_meta(entity_id: str, language: str = "en"):
    eid = _clean_text(entity_id).upper()
    lang = _clean_text(language) or "en"
    if not re.fullmatch(r"[QP]\d+", eid):
        return "", ""
    url = "https://www.wikidata.org/w/api.php"
    params = {
        "action": "wbgetentities",
        "ids": eid,
        "props": "labels|descriptions",
        "languages": lang,
        "format": "json",
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        entity = ((data or {}).get("entities") or {}).get(eid, {})
        labels = entity.get("labels") or {}
        descs = entity.get("descriptions") or {}
        label = _clean_text((labels.get(lang) or {}).get("value", ""))
        desc = _clean_text((descs.get(lang) or {}).get("value", ""))
        return label, desc
    except Exception:
        return "", ""


@lru_cache(maxsize=2048)
def _fetch_wikidata_property_meta(prop_id: str, language: str = "en"):
    pid = _clean_text(prop_id).upper()
    lang = _clean_text(language) or "en"
    if not re.fullmatch(r"P\d+", pid):
        return "", ""
    url = "https://www.wikidata.org/w/api.php"
    params = {
        "action": "wbgetentities",
        "ids": pid,
        "props": "labels|descriptions",
        "languages": lang,
        "format": "json",
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        entity = ((data or {}).get("entities") or {}).get(pid, {})
        labels = entity.get("labels") or {}
        descs = entity.get("descriptions") or {}
        label = _clean_text((labels.get(lang) or {}).get("value", ""))
        desc = _clean_text((descs.get(lang) or {}).get("value", ""))
        return label, desc
    except Exception:
        return "", ""


@lru_cache(maxsize=512)
def _load_property_meta_cached(path_text: str, mtime_ns: int, size_b: int):
    del mtime_ns, size_b
    out = {}
    path = Path(path_text)
    if not path.exists() or not path.is_file():
        return out
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        header_skipped = False
        for line in f:
            raw = line.rstrip("\n")
            if not raw:
                continue
            parts = raw.split("\t")
            if not header_skipped:
                header_skipped = True
                first = parts[0].strip().lower() if parts else ""
                if first in {"predicate", "property"}:
                    continue
            prop = _clean_text(parts[0] if parts else "").strip().strip("<>")
            if not prop:
                continue
            label = _clean_text(parts[2] if len(parts) > 2 else "")
            desc = _clean_text(parts[3] if len(parts) > 3 else "")
            keys = {
                _normalize_property_key(prop),
                _predicate_token(prop),
                _short_predicate(prop).lower(),
            }
            score = (1 if label else 0) + (1 if desc else 0)
            for key in keys:
                if not key:
                    continue
                existing = out.get(key)
                if existing:
                    prev_score = (1 if existing.get("label") else 0) + (1 if existing.get("description") else 0)
                    if prev_score > score:
                        continue
                out[key] = {
                    "label": label,
                    "description": desc,
                }
    return out


def _load_property_meta(path: Path):
    if not path.exists() or not path.is_file():
        return {}
    try:
        st = path.stat()
    except Exception:
        return {}
    try:
        resolved = path.resolve()
    except Exception:
        resolved = path
    return _load_property_meta_cached(str(resolved), int(st.st_mtime_ns), int(st.st_size))


def _property_meta_for(predicate: str, prop_meta: dict):
    if not prop_meta:
        prop_id = _extract_wikidata_property_id(predicate)
        if not prop_id:
            return "", ""
        return _fetch_wikidata_property_meta(prop_id)
    keys = (
        _normalize_property_key(predicate),
        _predicate_token(predicate),
        _short_predicate(predicate).lower(),
    )
    label = ""
    desc = ""
    for key in keys:
        if not key:
            continue
        data = prop_meta.get(key)
        if not data:
            continue
        label = _clean_text(data.get("label"))
        desc = _clean_text(data.get("description"))
        break

    if label and desc:
        return label, desc

    prop_id = _extract_wikidata_property_id(predicate)
    if not prop_id:
        return label, desc
    remote_label, remote_desc = _fetch_wikidata_property_meta(prop_id)
    if not label:
        label = remote_label
    if not desc:
        desc = remote_desc
    return label, desc


def _normalize_compare_text(value: str) -> str:
    raw = _clean_text(value)
    if not raw:
        return ""
    base = align_script.normalize_for_matching(raw)
    return re.sub(r"[^a-z0-9]+", "", base.lower())


def _is_informative_value_norm(value: str) -> bool:
    token = _clean_text(value).lower()
    if not token:
        return False
    # Ignore tiny numeric tokens (e.g. "6") which cause many false alignments.
    if re.fullmatch(r"\d{1,4}", token):
        return False
    # Ignore blank-node-like normalized IDs, usually not semantically informative.
    if re.fullmatch(r"n[0-9a-f]{10,}", token):
        return False
    # Keep concise Wikidata IDs.
    if re.fullmatch(r"[pq]\d+", token):
        return True
    # Very short non-ID tokens are typically noisy.
    if len(token) < 4:
        return False
    return True


def _informative_value_norms(values):
    return {v for v in (values or set()) if _is_informative_value_norm(v)}


def _object_value_info(obj: str):
    literal = _literal_lex(obj)
    if literal is not None:
        return {
            "text": literal,
            "is_node": False,
            "node": "",
            "norm": _normalize_compare_text(literal),
        }
    node = _normalize_node_token(obj)
    text = node or _clean_text(obj).strip().strip("<>")
    return {
        "text": text,
        "is_node": True,
        "node": node or text,
        "norm": _normalize_compare_text(text),
    }


def _first_literal_value(values):
    for value in values or []:
        if not isinstance(value, dict):
            continue
        if value.get("is_node"):
            continue
        text = _clean_text(value.get("text"))
        if text:
            return text
    return ""


def _build_node_summary(side: str, node: str, attr_items):
    side_norm = _clean_text(side).lower()
    node_key = _normalize_node_token(node)
    label = ""
    description = ""
    for item in attr_items or []:
        alias = _predicate_alias_key(item.get("property", ""))
        if alias == "label" and not label:
            label = _first_literal_value(item.get("values"))
        elif alias == "description" and not description:
            description = _first_literal_value(item.get("values"))
        if label and description:
            break

    if side_norm == "wd":
        entity_id = _extract_wikidata_entity_id(node_key)
        if entity_id:
            remote_label, remote_desc = _fetch_wikidata_entity_meta(entity_id)
            if not label:
                label = remote_label
            if not description:
                description = remote_desc

    return label, description


def _parse_ent_link_line(line: str):
    text = (line or "").rstrip("\n")
    if not text:
        return None
    parts = text.split("\t")
    if len(parts) < 2:
        return None
    left = _clean_text(parts[0])
    right = _clean_text(parts[1])
    if _looks_like_ent_links_header(f"{left}\t{right}"):
        return None
    wdc_iri = _normalize_node_token(left)
    wd_iri = _normalize_node_token(right)
    if not wdc_iri or not wd_iri:
        return None
    return wdc_iri, wd_iri


def _resolve_link_explorer_variant_dir(build_dir: Path, variant: Optional[str] = None):
    requested = _clean_text(variant)
    names = []
    if requested in _LINK_EXPLORER_VARIANTS:
        names.append(requested)
    for default_name in _LINK_EXPLORER_VARIANTS:
        if default_name not in names:
            names.append(default_name)

    for name in names:
        p = build_dir / name
        if p.exists() and p.is_dir() and (p / "ent_links").exists():
            return p, name
    for name in names:
        p = build_dir / name
        if p.exists() and p.is_dir():
            return p, name
    return None, None


def _scan_ent_links_page(path: Path, offset: int = 0, limit: int = 30, query: str = ""):
    if not path.exists() or not path.is_file():
        return {"rows": [], "total": 0, "has_more": False}
    offset = max(0, int(offset))
    limit = max(1, min(int(limit), 200))
    q = _clean_text(query).lower()

    # For large files without a filter, avoid a full scan to compute an exact total.
    # We only collect one page (+1 row to detect next page) for fast first render.
    try:
        file_size = path.stat().st_size
    except Exception:
        file_size = 0
    fast_mode = (not q) and file_size >= _LINK_EXPLORER_FAST_SCAN_BYTES

    rows = []
    total = 0
    has_more = False
    logical_idx = -1
    matched = 0
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            parsed = _parse_ent_link_line(line)
            if not parsed:
                continue
            logical_idx += 1
            wdc_iri, wd_iri = parsed
            if q and q not in wdc_iri.lower() and q not in wd_iri.lower():
                continue
            if matched >= offset and len(rows) < limit:
                rows.append(
                    {
                        "idx": logical_idx,
                        "wdc_iri": wdc_iri,
                        "wikidata_uri": wd_iri,
                    }
                )
            matched += 1

            if fast_mode and matched > (offset + limit):
                # We already captured page rows; first extra match means next page exists.
                if len(rows) >= limit:
                    has_more = True
                    break

    if fast_mode:
        return {"rows": rows, "total": None, "has_more": has_more}
    total = matched
    has_more = (offset + len(rows)) < total
    return {"rows": rows, "total": total, "has_more": has_more}


def _scan_ent_link_by_index(path: Path, idx: int):
    if not path.exists() or not path.is_file():
        return None
    if idx is None:
        return None
    try:
        target = int(idx)
    except Exception:
        return None
    if target < 0:
        return None

    logical_idx = -1
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            parsed = _parse_ent_link_line(line)
            if not parsed:
                continue
            logical_idx += 1
            if logical_idx != target:
                continue
            wdc_iri, wd_iri = parsed
            return {
                "idx": logical_idx,
                "wdc_iri": wdc_iri,
                "wikidata_uri": wd_iri,
            }
    return None


def _scan_subject_triples(
    path: Path,
    subject_key: str,
    max_rows: int = 4000,
    max_scan_lines: int = 0,
):
    rows = []
    if not path.exists() or not path.is_file() or not subject_key:
        return rows
    scanned = 0
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            scanned += 1
            if max_scan_lines > 0 and scanned > max_scan_lines:
                # Optional safeguard when a caller explicitly requests bounded scans.
                break
            parts = line.rstrip("\n").split("\t", 2)
            if len(parts) < 3:
                continue
            s = _clean_text(parts[0])
            p = _clean_text(parts[1]).strip().strip("<>")
            o = _clean_text(parts[2])
            if not s or not p:
                continue
            same_subject = _normalize_node_token(s) == subject_key
            if not same_subject:
                continue
            rows.append((p, o))
            if len(rows) >= max_rows:
                break
    return rows


def _aggregate_property_items(rows, relation: bool, max_values: int = 8, prop_meta: Optional[dict] = None):
    by_pred = {}
    for p, o in rows:
        pred = _clean_text(p).strip().strip("<>")
        if not pred:
            continue
        info = _object_value_info(o)
        if not info["text"]:
            continue
        item = by_pred.get(pred)
        if item is None:
            prop_label, prop_desc = _property_meta_for(pred, prop_meta or {})
            item = {
                "property": pred,
                "short_property": _short_predicate(pred),
                "property_label": prop_label,
                "property_description": prop_desc,
                "count": 0,
                "values": [],
                "value_norms": set(),
                "_seen": set(),
                "relation": relation,
            }
            by_pred[pred] = item
        item["count"] += 1
        signature = ("node" if info["is_node"] else "literal", info["node"] if info["is_node"] else info["text"])
        if signature in item["_seen"]:
            continue
        item["_seen"].add(signature)
        if info["norm"]:
            item["value_norms"].add(info["norm"])
        if len(item["values"]) < max_values:
            payload = {
                "text": info["text"],
                "is_node": info["is_node"],
            }
            if info["is_node"]:
                payload["node"] = info["node"]
            item["values"].append(payload)

    items = []
    for pred, item in by_pred.items():
        items.append(
            {
                "property": pred,
                "short_property": item["short_property"],
                "property_label": item.get("property_label", ""),
                "property_description": item.get("property_description", ""),
                "count": item["count"],
                "values": item["values"],
                "value_norms": sorted(item["value_norms"]),
                "relation": relation,
            }
        )
    items.sort(key=lambda r: (-int(r.get("count", 0)), r.get("property", "")))
    return items


def _side_files(variant_dir: Path, side: str):
    side_norm = _clean_text(side).lower()
    if side_norm in {"wd", "wikidata", "right"}:
        return {
            "side": "wd",
            "attr": variant_dir / "attr_triples_2",
            "rel": variant_dir / "rel_triples_2",
        }
    return {
        "side": "wdc",
        "attr": variant_dir / "attr_triples_1",
        "rel": variant_dir / "rel_triples_1",
    }


def _build_node_payload(variant_dir: Path, side: str, node: str):
    files = _side_files(variant_dir, side)
    node_key = _normalize_node_token(node)
    stats_path = variant_dir / ("prop_stats_wd.tsv" if files["side"] == "wd" else "prop_stats_wdc.tsv")
    prop_meta = _load_property_meta(stats_path)
    if not node_key:
        return {
            "side": files["side"],
            "node": "",
            "summary_label": "",
            "summary_description": "",
            "attr_items": [],
            "rel_items": [],
            "attr_count": 0,
            "rel_count": 0,
        }
    attr_rows = _scan_subject_triples(files["attr"], node_key)
    rel_rows = _scan_subject_triples(files["rel"], node_key)
    attr_items = _aggregate_property_items(attr_rows, relation=False, prop_meta=prop_meta)
    rel_items = _aggregate_property_items(rel_rows, relation=True, prop_meta=prop_meta)
    summary_label, summary_description = _build_node_summary(files["side"], node_key, attr_items)
    return {
        "side": files["side"],
        "node": node_key,
        "summary_label": summary_label,
        "summary_description": summary_description,
        "attr_items": attr_items,
        "rel_items": rel_items,
        "attr_count": sum(int(r.get("count", 0)) for r in attr_items),
        "rel_count": sum(int(r.get("count", 0)) for r in rel_items),
    }


def _similarity_for_properties(left_item: dict, right_item: dict):
    left_prop = left_item.get("property", "")
    right_prop = right_item.get("property", "")
    left_token = _predicate_token(left_prop)
    right_token = _predicate_token(right_prop)
    if not left_token or not right_token:
        return 0.0, 0.0, 0.0

    name_score = 0.0
    if left_token == right_token:
        name_score = 1.0
    else:
        left_alias = _predicate_alias_key(left_prop)
        right_alias = _predicate_alias_key(right_prop)
        if left_alias and left_alias == right_alias:
            name_score = 0.93
        else:
            ratio = difflib.SequenceMatcher(None, left_token, right_token).ratio()
            if left_token in right_token or right_token in left_token:
                ratio = max(ratio, 0.86)
            name_score = ratio

    left_values = _informative_value_norms(set(left_item.get("value_norms") or []))
    right_values = _informative_value_norms(set(right_item.get("value_norms") or []))
    value_score = 0.0
    if left_values and right_values:
        inter = len(left_values & right_values)
        union = len(left_values | right_values)
        jaccard = (inter / union) if union > 0 else 0.0
        smaller = min(len(left_values), len(right_values))
        coverage = (inter / smaller) if smaller > 0 else 0.0

        # Best-pair fallback when one side contains many values and only one needs to match.
        best_pair = 0.0
        for lv in left_values:
            for rv in right_values:
                if not lv or not rv:
                    continue
                if lv == rv:
                    best_pair = 1.0
                    break
                ratio = difflib.SequenceMatcher(None, lv, rv).ratio()
                if lv in rv or rv in lv:
                    ratio = max(ratio, 0.96)
                if ratio > best_pair:
                    best_pair = ratio
            if best_pair >= 1.0:
                break

        value_score = max(jaccard, coverage, best_pair)

    score = (0.65 * name_score) + (0.35 * value_score)
    if name_score >= 0.93 and score < 0.93:
        score = 0.93
    return score, name_score, value_score


def _pattern_token_set(value: str):
    raw = _clean_text(value)
    if not raw:
        return set()
    base = {
        raw.lower(),
        _short_predicate(raw).lower(),
        _predicate_token(raw),
        _predicate_alias_key(raw),
    }
    out = set()
    for t in base:
        t = _clean_text(t).lower()
        if not t:
            continue
        out.add(t)
        out.add(re.sub(r"[^a-z0-9]+", "", t))
    return {t for t in out if t}


def _property_matches_pattern(prop_value: str, pattern_value: str) -> bool:
    prop_tokens = _pattern_token_set(prop_value)
    if not prop_tokens:
        return False
    for pat in _split_target_property_alternatives(pattern_value):
        pat_tokens = _pattern_token_set(pat)
        if not pat_tokens:
            continue
        if prop_tokens & pat_tokens:
            return True
    return False


def _compute_property_matches(
    left_items,
    right_items,
    max_matches: int = 14,
    threshold: float = 0.55,
    configured_pairs: Optional[list] = None,
):
    def _candidate_row(left_item: dict, right_item: dict, cand: dict, reason: str):
        return {
            "wdc_property": left_item.get("property", ""),
            "wdc_short_property": left_item.get("short_property", ""),
            "wdc_property_label": left_item.get("property_label", ""),
            "wdc_property_description": left_item.get("property_description", ""),
            "wikidata_property": right_item.get("property", ""),
            "wikidata_short_property": right_item.get("short_property", ""),
            "wikidata_property_label": right_item.get("property_label", ""),
            "wikidata_property_description": right_item.get("property_description", ""),
            "score": round(float(cand["score"]), 3),
            "name_score": round(float(cand["name_score"]), 3),
            "value_score": round(float(cand["value_score"]), 3),
            "match_reason": reason,
            "wdc_sample": (left_item.get("values") or [{}])[0].get("text", "") if left_item.get("values") else "",
            "wikidata_sample": (right_item.get("values") or [{}])[0].get("text", "")
            if right_item.get("values")
            else "",
        }

    configured_pairs = configured_pairs or []
    used_left = set()
    used_right = set()
    rows = []

    for pair in configured_pairs:
        left_pat = _clean_text((pair or {}).get("wdc", ""))
        right_pat = _clean_text((pair or {}).get("target", ""))
        if not left_pat or not right_pat:
            continue
        best = None
        for l_idx, left_item in enumerate(left_items or []):
            if l_idx in used_left:
                continue
            if not _property_matches_pattern(left_item.get("property", ""), left_pat):
                continue
            for r_idx, right_item in enumerate(right_items or []):
                if r_idx in used_right:
                    continue
                if not _property_matches_pattern(right_item.get("property", ""), right_pat):
                    continue
                if bool(left_item.get("relation")) != bool(right_item.get("relation")):
                    continue
                score, name_score, value_score = _similarity_for_properties(left_item, right_item)
                boosted_score = max(score, 0.9 if value_score > 0 else 0.78)
                cand = {
                    "l_idx": l_idx,
                    "r_idx": r_idx,
                    "score": boosted_score,
                    "name_score": name_score,
                    "value_score": value_score,
                }
                if best is None or cand["score"] > best["score"]:
                    best = cand
        if not best:
            continue
        used_left.add(best["l_idx"])
        used_right.add(best["r_idx"])
        rows.append(
            _candidate_row(
                (left_items or [])[best["l_idx"]],
                (right_items or [])[best["r_idx"]],
                best,
                reason="configured_rule",
            )
        )
        if len(rows) >= max_matches:
            return rows[:max_matches]

    candidates = []
    for l_idx, left_item in enumerate(left_items or []):
        if l_idx in used_left:
            continue
        for r_idx, right_item in enumerate(right_items or []):
            if r_idx in used_right:
                continue
            # Keep attribute vs relation comparisons separate to avoid noisy cross-type matches.
            if bool(left_item.get("relation")) != bool(right_item.get("relation")):
                continue
            score, name_score, value_score = _similarity_for_properties(left_item, right_item)
            candidates.append(
                {
                    "l_idx": l_idx,
                    "r_idx": r_idx,
                    "score": score,
                    "name_score": name_score,
                    "value_score": value_score,
                }
            )
    candidates.sort(key=lambda row: row["score"], reverse=True)

    for cand in candidates:
        if cand["score"] < threshold:
            continue
        if cand["l_idx"] in used_left or cand["r_idx"] in used_right:
            continue
        left_item = left_items[cand["l_idx"]]
        right_item = right_items[cand["r_idx"]]
        used_left.add(cand["l_idx"])
        used_right.add(cand["r_idx"])
        rows.append(_candidate_row(left_item, right_item, cand, reason="name_or_alias"))
        if len(rows) >= max_matches:
            break

    # Fallback: for properties still unmatched, align by value similarity only.
    # This catches cases like custom WDC keys mapping to Pxxxx when names differ.
    value_fallback_threshold = 0.80
    if len(rows) < max_matches:
        fallback_candidates = [
            row
            for row in candidates
            if row["value_score"] >= value_fallback_threshold
            and row["l_idx"] not in used_left
            and row["r_idx"] not in used_right
        ]
        fallback_candidates.sort(key=lambda row: (row["value_score"], row["score"]), reverse=True)
        for cand in fallback_candidates:
            if cand["l_idx"] in used_left or cand["r_idx"] in used_right:
                continue
            left_item = left_items[cand["l_idx"]]
            right_item = right_items[cand["r_idx"]]
            used_left.add(cand["l_idx"])
            used_right.add(cand["r_idx"])
            boosted = dict(cand)
            boosted["score"] = max(float(boosted["score"]), 0.70)
            rows.append(_candidate_row(left_item, right_item, boosted, reason="value_fallback"))
            if len(rows) >= max_matches:
                break

    if len(rows) > max_matches:
        rows = rows[:max_matches]
    return rows


def _node_graph_preview(node_payload: dict, max_neighbors: int = 10):
    items = []
    if not node_payload:
        return items
    root = _clean_text(node_payload.get("node"))
    side = _clean_text(node_payload.get("side"))
    if root:
        items.append({"node": root, "side": side, "root": True})
    seen = {root}
    for rel_item in (node_payload.get("rel_items") or []):
        for value in (rel_item.get("values") or []):
            if not value.get("is_node"):
                continue
            node = _clean_text(value.get("node"))
            if not node or node in seen:
                continue
            seen.add(node)
            items.append({"node": node, "side": side, "root": False})
            if len(items) >= max_neighbors + 1:
                return items
    return items


def _build_link_detail_payload(variant_dir: Path, idx: int):
    ent_links_path = variant_dir / "ent_links"
    link_row = _scan_ent_link_by_index(ent_links_path, idx)
    if not link_row:
        return None
    wdc_node = _build_node_payload(variant_dir, "wdc", link_row["wdc_iri"])
    wd_node = _build_node_payload(variant_dir, "wd", link_row["wikidata_uri"])
    left_items = (wdc_node.get("attr_items") or []) + (wdc_node.get("rel_items") or [])
    right_items = (wd_node.get("attr_items") or []) + (wd_node.get("rel_items") or [])
    build_dir = variant_dir.parent
    build_config = _load_build_config(build_dir)
    linking_combinations = _extract_linking_combinations(build_config)
    configured_pairs = []
    seen_pairs = set()
    for combo in linking_combinations:
        for pair in (combo.get("pairs") or []):
            left_pat = _clean_text(pair.get("wdc", ""))
            right_pat = _clean_text(pair.get("target", ""))
            sig = f"{left_pat}=>{right_pat}".lower()
            if not left_pat or not right_pat or sig in seen_pairs:
                continue
            seen_pairs.add(sig)
            configured_pairs.append({"wdc": left_pat, "target": right_pat})
    matches = _compute_property_matches(left_items, right_items, configured_pairs=configured_pairs)
    return {
        "idx": link_row["idx"],
        "wdc_iri": link_row["wdc_iri"],
        "wikidata_uri": link_row["wikidata_uri"],
        "wdc_node": wdc_node,
        "wd_node": wd_node,
        "property_matches": matches,
        "wdc_graph_nodes": _node_graph_preview(wdc_node),
        "wd_graph_nodes": _node_graph_preview(wd_node),
    }


def _normalized_path_text(value: str) -> str:
    raw = _clean_text(value)
    if not raw:
        return ""
    try:
        # Normalize both relative and absolute paths to the same canonical form.
        return str(Path(raw).expanduser().resolve())
    except Exception:
        return os.path.normpath(raw)


def _build_result_path_aliases(build_dir: Path):
    aliases = set()
    try:
        resolved = build_dir.resolve()
    except Exception:
        resolved = build_dir

    for candidate in (build_dir, resolved):
        txt = _clean_text(str(candidate))
        if not txt:
            continue
        aliases.add(txt)
        aliases.add(os.path.normpath(txt))

    try:
        cwd_resolved = Path.cwd().resolve()
        rel = resolved.relative_to(cwd_resolved)
        rel_txt = str(rel)
        aliases.add(rel_txt)
        aliases.add(os.path.normpath(rel_txt))
        aliases.add(f"./{rel_txt}")
    except Exception:
        pass

    normalized = {_clean_text(a.rstrip("/\\")) for a in aliases if _clean_text(a)}
    return {a for a in normalized if a}


def _build_dir_path_keys(build_dir: Path):
    aliases = _build_result_path_aliases(build_dir)
    keys = set(aliases)
    for alias in aliases:
        keys.add(os.path.normpath(alias))
        keys.add(_normalized_path_text(alias))
    keys.add(_normalized_path_text(str(build_dir)))
    return {k for k in keys if _clean_text(k)}


def _owned_build_path_keys(owner_key: Optional[str], scan_limit: int = 50000):
    keys = set()
    if not owner_key:
        return keys
    for row in db.list_jobs(limit=scan_limit, owner_key=owner_key):
        try:
            result_path = _clean_text(row["result_path"])
        except Exception:
            result_path = ""
        if not result_path:
            continue
        keys.add(result_path)
        keys.add(os.path.normpath(result_path))
        keys.add(_normalized_path_text(result_path))
    return {k for k in keys if _clean_text(k)}


def _build_owner_file(build_dir: Path) -> Path:
    return build_dir / _BUILD_OWNER_FILENAME


def _read_build_owner_key(build_dir: Path):
    try:
        return ownership.normalize_owner_key(_build_owner_file(build_dir).read_text(encoding="utf-8"))
    except Exception:
        return None


def _write_build_owner_key(build_dir: Path, owner_key: Optional[str]):
    owner_key = ownership.normalize_owner_key(owner_key)
    if not owner_key:
        return
    try:
        _build_owner_file(build_dir).write_text(owner_key, encoding="utf-8")
    except Exception:
        pass


def _build_has_any_job_reference(build_dir: Path, scan_limit: int = 50000) -> bool:
    path_keys = _build_dir_path_keys(build_dir)
    if not path_keys:
        return False
    for row in db.list_jobs(limit=scan_limit):
        try:
            result_path = _clean_text(row["result_path"])
        except Exception:
            result_path = ""
        if not result_path:
            continue
        row_keys = {result_path, os.path.normpath(result_path), _normalized_path_text(result_path)}
        if path_keys & row_keys:
            return True
    return False


def _build_is_owned_by_keys(
    build_dir: Path,
    owned_keys: set,
    owner_key: Optional[str] = None,
    claim_unowned: bool = False,
) -> bool:
    build_owner = _read_build_owner_key(build_dir)
    owner_key = ownership.normalize_owner_key(owner_key)
    if build_owner:
        return bool(owner_key and build_owner == owner_key)
    if not owned_keys:
        if claim_unowned and owner_key and not _build_has_any_job_reference(build_dir):
            _write_build_owner_key(build_dir, owner_key)
            return True
        return False
    if _build_dir_path_keys(build_dir) & owned_keys:
        _write_build_owner_key(build_dir, owner_key)
        return True
    if claim_unowned and owner_key and not _build_has_any_job_reference(build_dir):
        _write_build_owner_key(build_dir, owner_key)
        return True
    return False


def _owner_can_access_build(owner_key: Optional[str], build_dir: Path, claim_unowned: bool = True) -> bool:
    return _build_is_owned_by_keys(
        build_dir,
        _owned_build_path_keys(owner_key),
        owner_key=owner_key,
        claim_unowned=claim_unowned,
    )


def _delete_jobs_for_build_dir(build_dir: Path, scan_limit: int = 50000, owner_key: Optional[str] = None) -> int:
    aliases = _build_result_path_aliases(build_dir)
    target_norm = _normalized_path_text(str(build_dir))
    to_delete_ids = set()

    # Delete exact-path variants without relying on recency limits.
    for alias in aliases:
        try:
            db.delete_jobs_by_result_path(alias, owner_key=owner_key)
        except Exception:
            continue

    # Fallback for unusual historical path spellings that still point to the same directory.
    for row in db.list_jobs(limit=scan_limit, owner_key=owner_key):
        try:
            rp = _clean_text(row["result_path"])
        except Exception:
            rp = ""
        if not rp:
            continue
        if rp in aliases or os.path.normpath(rp) in aliases or _normalized_path_text(rp) == target_norm:
            try:
                to_delete_ids.add(int(row["id"]))
            except Exception:
                continue

    for jid in to_delete_ids:
        try:
            db.delete_job(jid)
        except Exception:
            continue
    return len(to_delete_ids)


def _bool_from_any(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "on"}


def _find_job_params_by_result_path(result_path: str, limit: int = 4000, owner_key: Optional[str] = None):
    target = str(result_path or "").strip()
    if not target:
        return None
    for row in db.list_jobs(limit=limit, owner_key=owner_key):
        rp = str(row["result_path"] or "").strip()
        if rp != target:
            continue
        params = _safe_json_loads(row["params_json"])
        if isinstance(params, dict) and params:
            return params
    return None


def _rerun_params_from_build_config(build_dir: Path, class_name: str, owner_key: Optional[str] = None):
    cfg_path = build_dir / "BUILD_CONFIG.json"
    cfg = {}
    if cfg_path.exists() and cfg_path.is_file():
        cfg = _safe_json_loads(cfg_path.read_text(encoding="utf-8"))
    cfg = cfg if isinstance(cfg, dict) else {}
    fallback = _find_job_params_by_result_path(str(build_dir), owner_key=owner_key)
    fallback = fallback if isinstance(fallback, dict) else {}

    def _pick(key, default=""):
        v = cfg.get(key, None)
        if v is None and fallback:
            v = fallback.get(key, None)
        if v is None:
            v = default
        return v

    raw_params = {
        "matching_mode": _normalize_matching_mode(
            _clean_text(str(_pick("matching_mode", ""))),
            fallback_wdc_value_is_wikidata=_bool_from_any(_pick("wdc_value_is_wikidata", False)),
        ),
        "class_name": _clean_text(str(_pick("class_name", class_name))),
        "parts_spec": _clean_text(str(_pick("parts_spec", "all"))),
        "wdc_predicate_pattern": _clean_text(str(_pick("wdc_predicate_pattern", ""))),
        "wdc_pattern_search_in": _clean_text(str(_pick("wdc_pattern_search_in", "predicate"))),
        "target_endpoint": _clean_text(str(_pick("target_endpoint", "wikidata"))),
        "target_endpoint_url": _clean_text(str(_pick("target_endpoint_url", ""))),
        "target_prefixes": _clean_text(str(_pick("target_prefixes", ""))),
        "property_mapping_rules": _clean_text(str(_pick("property_mapping_rules", ""))),
        "target_property": _clean_text(str(_pick("target_property", _pick("wikidata_property", "")))),
        "target_class": _clean_text(str(_pick("target_class", _pick("wkd_class", "")))),
        "wikidata_property": _clean_text(str(_pick("wikidata_property", ""))),
        "wkd_class": _clean_text(str(_pick("wkd_class", ""))),
        "ignore_chars": _clean_text(str(_pick("ignore_chars", "spaces;-;."))),
        "force_align": _bool_from_any(_pick("force_align", False)),
        "use_local_only": _bool_from_any(_pick("use_local_only", False)),
        "strict_duplicate_key_filter": True,
    }
    return _validate_and_normalize_job_params(raw_params)


def _job_outputs(job):
    out = {"build_done": False, "build_out_with": None, "build_out_without": None, "build_done_file": None}
    result_path = job["result_path"]
    if result_path:
        base = Path(result_path)
        out["build_done_file"] = str(base / "BUILD_DONE")
        if (base / "BUILD_DONE").exists():
            out["build_done"] = True
        if (base / "with_link_code").exists():
            out["build_out_with"] = str(base / "with_link_code")
        if (base / "without_link_code").exists():
            out["build_out_without"] = str(base / "without_link_code")
    return out


def _safe_json_loads(raw: Optional[str]):
    try:
        return json.loads(raw or "{}")
    except Exception:
        return {}


def _looks_like_skipped_build_reason(text: Optional[str]) -> bool:
    msg = str(text or "").strip().lower()
    if not msg:
        return False
    return ("build skipped" in msg) or ("no alignments found" in msg)


def _build_dashboard_state(
    job_limit: int = 50,
    build_limit: int = 200,
    test_mode: Optional[bool] = None,
    owner_key: Optional[str] = None,
):
    all_jobs = [dict(j) for j in db.list_jobs(limit=job_limit, owner_key=owner_key)]
    jobs_by_id = {j["id"]: j for j in all_jobs}
    # Always include truly active jobs even if they are outside the recency window.
    for st in ("running", "queued"):
        for row in db.list_jobs_by_status(st, owner_key=owner_key):
            jid = row["id"]
            if jid not in jobs_by_id:
                jobs_by_id[jid] = dict(row)
    all_jobs = sorted(jobs_by_id.values(), key=lambda r: int(r.get("id") or 0), reverse=True)
    all_jobs_params = {j["id"]: _safe_json_loads(j.get("params_json")) for j in all_jobs}
    if test_mode is not None:
        desired = bool(test_mode)
        all_jobs = [
            j for j in all_jobs
            if _is_test_class_name(all_jobs_params.get(j["id"], {}).get("class_name")) == desired
        ]
    active_jobs = [j for j in all_jobs if j["status"] in {"running", "queued"}]
    owned_build_keys = _owned_build_path_keys(owner_key)
    builds = [
        b for b in _scan_builds(limit=build_limit)
        if _build_is_owned_by_keys(
            Path(b.get("path") or ""),
            owned_build_keys,
            owner_key=owner_key,
            claim_unowned=True,
        )
    ]
    if test_mode is not None:
        desired = bool(test_mode)
        builds = [b for b in builds if _is_test_class_name(b.get("class_name")) == desired]

    build_params = {}
    for j in all_jobs:
        rp = j.get("result_path")
        if not rp or rp in build_params:
            continue
        params = _safe_json_loads(j.get("params_json"))
        if params:
            build_params[rp] = params

    for b in builds:
        params = b.get("build_config") or build_params.get(b["path"])
        if params:
            b["config"] = params
        else:
            b["config"] = {
                "class_name": b["class_name"],
                "build_name": b["build_name"],
                "result_path": b["path"],
                "config_source": "inferred",
            }
        parts = b["config"].get("parts_manifest")
        if not isinstance(parts, list):
            parts = []
        b["parts_manifest"] = parts
        b["parts_count"] = b["config"].get("parts_count", len(parts))
        b["parts_total_size_human"] = b["config"].get("parts_total_size_human")
        b["config_groups"] = _build_config_groups(b["config"])

    jobs_outputs = {}
    jobs_times = {}
    jobs_params = {}
    jobs_subjobs = {}
    for j in all_jobs:
        jid = j["id"]
        jobs_outputs[jid] = _job_outputs(j)
        jobs_times[jid] = {
            "created": _fmt_ts(j.get("created_at")),
            "started": _fmt_ts(j.get("started_at")),
            "ended": _fmt_ts(j.get("ended_at")),
        }
        jobs_params[jid] = all_jobs_params.get(jid, {})
        jobs_subjobs[jid] = [dict(s) for s in db.list_subjobs(jid)]

    # Safety: normalize inconsistent rows persisted as "done" when build was skipped.
    for j in all_jobs:
        if j.get("status") != "done":
            continue
        jid = j["id"]
        if jobs_outputs.get(jid, {}).get("build_done"):
            continue
        build_row = next((s for s in jobs_subjobs.get(jid, []) if s.get("type") == "build"), None)
        build_step = str((build_row or {}).get("current_step") or "").strip().lower()
        build_msg = str((build_row or {}).get("progress_text") or "").strip()
        job_msg = str(j.get("progress_text") or "").strip()
        err_msg = str(j.get("error_message") or "").strip()
        skipped = (
            build_step == "skipped"
            or _looks_like_skipped_build_reason(build_msg)
            or _looks_like_skipped_build_reason(job_msg)
            or _looks_like_skipped_build_reason(err_msg)
        )
        if not skipped:
            continue
        reason = build_msg or job_msg or err_msg or "No alignments found (0); build skipped."
        j["status"] = "error"
        j["phase"] = j.get("phase") or "build"
        j["error_message"] = reason

    # Keep done jobs visible when there is no downloadable build output,
    # except dangling rows where result_path points to a deleted/non-existent build dir.
    jobs_for_panel = []
    for j in all_jobs:
        if j["status"] != "done":
            jobs_for_panel.append(j)
            continue
        out = jobs_outputs.get(j["id"], {})
        if out.get("build_done"):
            continue
        result_path = _clean_text(j.get("result_path"))
        if result_path:
            try:
                if not Path(result_path).exists():
                    continue
            except Exception:
                pass
        jobs_for_panel.append(j)

    return {
        "all_jobs": all_jobs,
        "active_jobs": active_jobs,
        "jobs_for_panel": jobs_for_panel,
        "builds": builds,
        "jobs_outputs": jobs_outputs,
        "jobs_times": jobs_times,
        "jobs_params": jobs_params,
        "jobs_subjobs": jobs_subjobs,
    }


@app.on_event("startup")
def _init_db():
    db.init_db()


def _render_index_page(
    request: Request,
    app_view: str = "create",
    preset: Optional[str] = None,
    recent: Optional[int] = None,
    form_error: Optional[str] = None,
    test_mode: Optional[str] = None,
):
    owner_key, _ = _get_or_create_owner_key(request)
    is_test_mode = _bool_from_any(test_mode)
    # Seed from local catalog first. Do not auto-scrape remote stats on startup.
    if not db.list_wdc_classes():
        _seed_wdc_classes_from_local_catalog()
    local_rows = _discover_local_class_rows("Download")
    if local_rows:
        try:
            db.upsert_wdc_classes(local_rows)
        except Exception:
            pass

    form = _default_form()
    visible_presets = _filter_presets_by_mode(is_test_mode)
    selected_preset = ""
    if preset:
        if preset in visible_presets:
            form.update(visible_presets[preset])
            selected_preset = preset

    if recent:
        job = db.get_job(recent, owner_key=owner_key)
        if job:
            try:
                params = json.loads(job["params_json"])
                if _is_test_class_name(params.get("class_name")) == is_test_mode:
                    form.update(params)
            except Exception:
                pass

    _sync_target_alias_fields(form)
    form["matching_mode"] = _normalize_matching_mode(
        form.get("matching_mode"),
        fallback_wdc_value_is_wikidata=bool(form.get("wdc_value_is_wikidata")),
    )

    wdc_classes = [dict(r) for r in db.list_wdc_classes()]
    wdc_classes = [r for r in wdc_classes if _is_test_class_name(r.get("class_name")) == is_test_mode]
    class_meta = {r["class_name"]: r for r in wdc_classes}

    class_parts_info = None
    if form.get("class_name") and form.get("class_name") in class_meta:
        class_parts_info = _build_class_parts_info(form["class_name"])

    recent_presets = _get_recent_presets(test_mode=is_test_mode, owner_key=owner_key)
    dashboard = _build_dashboard_state(job_limit=50, build_limit=200, test_mode=is_test_mode, owner_key=owner_key)
    jobs = dashboard["jobs_for_panel"]
    builds = dashboard["builds"]
    jobs_outputs = {j["id"]: dashboard["jobs_outputs"][j["id"]] for j in jobs}
    jobs_times = {j["id"]: dashboard["jobs_times"][j["id"]] for j in jobs}
    jobs_params = {j["id"]: dashboard["jobs_params"][j["id"]] for j in jobs}
    jobs_subjobs = {j["id"]: dashboard["jobs_subjobs"][j["id"]] for j in jobs}

    response = templates.TemplateResponse(
        request,
        "index.html",
        {
            "app_view": app_view if app_view in {"create", "jobs", "history"} else "create",
            "form": form,
            "presets": visible_presets,
            "selected_preset": selected_preset,
            "recent_presets": recent_presets,
            "jobs": jobs,
            "jobs_outputs": jobs_outputs,
            "jobs_times": jobs_times,
            "jobs_params": jobs_params,
            "jobs_subjobs": jobs_subjobs,
            "builds": builds,
            "class_meta": class_meta,
            "class_parts_info": class_parts_info,
            "form_error": _clean_text(form_error),
            "is_test_mode": is_test_mode,
            "target_endpoints": [
                {"key": k, "label": v.get("label", k), "default_url": v.get("default_url", "")}
                for k, v in TARGET_ENDPOINTS.items()
            ],
        },
    )
    return _set_owner_cookie_if_needed(response, request, owner_key)


@app.get("/", response_class=HTMLResponse)
def index(
    request: Request,
    preset: Optional[str] = None,
    recent: Optional[int] = None,
    form_error: Optional[str] = None,
    test_mode: Optional[str] = None,
):
    return _render_index_page(
        request=request,
        app_view="create",
        preset=preset,
        recent=recent,
        form_error=form_error,
        test_mode=test_mode,
    )


@app.get("/app/create", response_class=HTMLResponse)
def app_create(
    request: Request,
    preset: Optional[str] = None,
    recent: Optional[int] = None,
    form_error: Optional[str] = None,
    test_mode: Optional[str] = None,
):
    return _render_index_page(
        request=request,
        app_view="create",
        preset=preset,
        recent=recent,
        form_error=form_error,
        test_mode=test_mode,
    )


@app.get("/app/jobs", response_class=HTMLResponse)
def app_jobs(
    request: Request,
    test_mode: Optional[str] = None,
):
    return _render_index_page(
        request=request,
        app_view="jobs",
        test_mode=test_mode,
    )


@app.get("/app/history", response_class=HTMLResponse)
def app_history(
    request: Request,
    test_mode: Optional[str] = None,
):
    return _render_index_page(
        request=request,
        app_view="history",
        test_mode=test_mode,
    )


@app.get("/tutorial", response_class=HTMLResponse)
def tutorial_page(
    request: Request,
    test_mode: Optional[str] = None,
):
    is_test_mode = _bool_from_any(test_mode)
    payload = _load_tutorial_page_data()
    return templates.TemplateResponse(
        request,
        "tutorial.html",
        {
            "is_test_mode": is_test_mode,
            "tutorial_ok": payload["ok"],
            "tutorial_error": payload["error"],
            "tutorial_html": payload["html"],
            "tutorial_sections": payload["sections"],
            "tutorial_source_path": payload["source_path"],
        },
    )


@app.get("/sakey", response_class=HTMLResponse)
def sakey_page(
    request: Request,
    class_name: str = "",
    run_id: str = "",
    key_order_by: str = "",
    key_min_support: str = "",
    key_only_almost: Optional[str] = None,
    key_max_size: str = "",
    key_q: str = "",
    test_mode: Optional[str] = None,
    form_error: Optional[str] = None,
):
    is_test_mode = _bool_from_any(test_mode)
    if not db.list_wdc_classes():
        _seed_wdc_classes_from_local_catalog()
    local_rows = _discover_local_class_rows("Download")
    if local_rows:
        try:
            db.upsert_wdc_classes(local_rows)
        except Exception:
            pass
    payload = _sakey_page_payload(
        class_name=class_name,
        run_id=run_id,
        test_mode=is_test_mode,
        key_order_by=key_order_by,
        key_min_support=key_min_support,
        key_only_almost=key_only_almost,
        key_max_size=key_max_size,
        key_q=key_q,
    )

    return templates.TemplateResponse(
        request,
        "sakey.html",
        {
            "is_test_mode": is_test_mode,
            "form_error": _clean_text(form_error),
            **payload,
        },
    )


@app.post("/sakey/run")
def sakey_run(
    class_name: str = Form(""),
    parts_spec: str = Form("all"),
    mins: int = Form(3),
    timeout_hours: float = Form(48.0),
    test_mode: Optional[str] = Form(None),
):
    is_test_mode = _bool_from_any(test_mode)
    if not db.list_wdc_classes():
        _seed_wdc_classes_from_local_catalog()
    cname = _clean_text(class_name)
    if not cname:
        query = "test_mode=1&" if is_test_mode else ""
        query += f"form_error={quote_plus('Class name is required.')}"
        return RedirectResponse(url=f"/sakey?{query}", status_code=303)
    run_id = _enqueue_sakey_run(
        class_name=cname,
        parts_spec=_clean_text(parts_spec) or "all",
        mins=max(1, int(mins or 3)),
        timeout_hours=max(0.1, float(timeout_hours or 48.0)),
    )
    query = []
    if is_test_mode:
        query.append("test_mode=1")
    query.append(f"class_name={quote_plus(cname)}")
    query.append(f"run_id={quote_plus(run_id)}")
    return RedirectResponse(url=f"/sakey?{'&'.join(query)}", status_code=303)


@app.get("/api/sakey/status")
def sakey_status_api(
    class_name: str = "",
    run_id: str = "",
    key_order_by: str = "",
    key_min_support: str = "",
    key_only_almost: Optional[str] = None,
    key_max_size: str = "",
    key_q: str = "",
    test_mode: Optional[str] = None,
):
    is_test_mode = _bool_from_any(test_mode)
    payload = _sakey_page_payload(
        class_name=class_name,
        run_id=run_id,
        test_mode=is_test_mode,
        key_order_by=key_order_by,
        key_min_support=key_min_support,
        key_only_almost=key_only_almost,
        key_max_size=key_max_size,
        key_q=key_q,
    )
    return {"ok": True, **payload}


@app.get("/sakey/runs/{run_id}/artifact/{name}")
def sakey_download_artifact(run_id: str, name: str):
    p = _sakey_resolve_artifact(run_id, name)
    if not p:
        raise HTTPException(status_code=404, detail="Artifact not found.")
    return FileResponse(
        str(p),
        media_type="application/octet-stream",
        filename=p.name,
    )


@app.get("/builds/{class_name}/{build_name}", response_class=HTMLResponse)
def build_detail_page(
    request: Request,
    class_name: str,
    build_name: str,
    test_mode: Optional[str] = None,
):
    owner_key, _ = _get_or_create_owner_key(request)
    is_test_mode = _bool_from_any(test_mode)
    build_dir = _resolve_build_dir(class_name, build_name)
    if not build_dir or not _owner_can_access_build(owner_key, build_dir):
        query = "test_mode=1&" if is_test_mode else ""
        query += f"form_error={quote_plus('Build not found.')}"
        return _redirect_with_owner(request, url=f"/?{query}", status_code=303)

    build = _build_summary_from_dir(build_dir)
    if not build:
        query = "test_mode=1&" if is_test_mode else ""
        query += f"form_error={quote_plus('Build not found.')}"
        return _redirect_with_owner(request, url=f"/?{query}", status_code=303)

    response = templates.TemplateResponse(
        request,
        "build_detail.html",
        {
            "build": build,
            "is_test_mode": is_test_mode,
        },
    )
    return _set_owner_cookie_if_needed(response, request, owner_key)


@app.get("/builds/{class_name}/{build_name}/links", response_class=HTMLResponse)
def build_links_page(
    request: Request,
    class_name: str,
    build_name: str,
    variant: Optional[str] = None,
    q: str = "",
    offset: int = 0,
    limit: int = 30,
    test_mode: Optional[str] = None,
):
    owner_key, _ = _get_or_create_owner_key(request)
    is_test_mode = _bool_from_any(test_mode)
    build_dir = _resolve_build_dir(class_name, build_name)
    if not build_dir or not _owner_can_access_build(owner_key, build_dir):
        query = "test_mode=1&" if is_test_mode else ""
        query += f"form_error={quote_plus('Build not found.')}"
        return _redirect_with_owner(request, url=f"/?{query}", status_code=303)

    build = {
        "class_name": class_name,
        "build_name": build_name,
    }
    build_config = _load_build_config(build_dir)
    build["linking_combinations"] = _extract_linking_combinations(build_config)

    variant_dir, variant_name = _resolve_link_explorer_variant_dir(build_dir, variant=variant)
    if not variant_dir or not variant_name:
        query = "test_mode=1&" if is_test_mode else ""
        query += f"form_error={quote_plus('No link files available for this build.')}"
        return _redirect_with_owner(request, url=f"/?{query}", status_code=303)

    ent_links_path = variant_dir / "ent_links"
    page = _scan_ent_links_page(ent_links_path, offset=offset, limit=limit, query=q)
    rows = page["rows"]
    total = page["total"]

    available_variants = []
    for name in _LINK_EXPLORER_VARIANTS:
        p = build_dir / name
        if not p.exists() or not p.is_dir():
            continue
        available_variants.append(
            {
                "name": name,
                "has_ent_links": (p / "ent_links").exists(),
            }
        )
    if not available_variants:
        available_variants = [{"name": variant_name, "has_ent_links": ent_links_path.exists()}]

    response = templates.TemplateResponse(
        request,
        "link_explorer.html",
        {
            "build": build,
            "is_test_mode": is_test_mode,
            "selected_variant": variant_name,
            "available_variants": available_variants,
            "initial_query": _clean_text(q),
            "initial_offset": max(0, int(offset)),
            "initial_limit": max(1, min(int(limit), 200)),
            "initial_total": total,
            "initial_has_more": bool(page.get("has_more", False)),
            "initial_rows": rows,
            "initial_detail": None,
            "linking_combinations": build.get("linking_combinations", []),
        },
    )
    return _set_owner_cookie_if_needed(response, request, owner_key)


@app.get("/api/builds/{class_name}/{build_name}/links")
def build_links_api(
    request: Request,
    response: Response,
    class_name: str,
    build_name: str,
    variant: Optional[str] = None,
    q: str = "",
    offset: int = 0,
    limit: int = 30,
):
    owner_key, _ = _get_or_create_owner_key(request)
    _set_owner_cookie_if_needed(response, request, owner_key)
    build_dir = _resolve_build_dir(class_name, build_name)
    if not build_dir or not _owner_can_access_build(owner_key, build_dir):
        raise HTTPException(status_code=404, detail="Build not found.")
    variant_dir, variant_name = _resolve_link_explorer_variant_dir(build_dir, variant=variant)
    if not variant_dir or not variant_name:
        raise HTTPException(status_code=404, detail="No link files available for this build.")

    ent_links_path = variant_dir / "ent_links"
    page = _scan_ent_links_page(ent_links_path, offset=offset, limit=limit, query=q)
    return {
        "ok": True,
        "class_name": class_name,
        "build_name": build_name,
        "variant": variant_name,
        "q": _clean_text(q),
        "offset": max(0, int(offset)),
        "limit": max(1, min(int(limit), 200)),
        "total": page["total"],
        "has_more": bool(page.get("has_more", False)),
        "rows": page["rows"],
    }


@app.get("/api/builds/{class_name}/{build_name}/link")
def build_link_detail_api(
    request: Request,
    response: Response,
    class_name: str,
    build_name: str,
    idx: int,
    variant: Optional[str] = None,
    wait_ms: int = 250,
):
    owner_key, _ = _get_or_create_owner_key(request)
    _set_owner_cookie_if_needed(response, request, owner_key)
    build_dir = _resolve_build_dir(class_name, build_name)
    if not build_dir or not _owner_can_access_build(owner_key, build_dir):
        raise HTTPException(status_code=404, detail="Build not found.")
    variant_dir, variant_name = _resolve_link_explorer_variant_dir(build_dir, variant=variant)
    if not variant_dir or not variant_name:
        raise HTTPException(status_code=404, detail="No link files available for this build.")

    wait_ms = max(0, min(int(wait_ms), 5000))
    key, status, payload, fut = _start_link_detail_build(build_dir, variant_dir, variant_name, idx)
    if status != "ready":
        payload, status = _read_link_detail_future(key, fut, wait_ms=wait_ms)

    if status != "ready":
        return {
            "ok": True,
            "class_name": class_name,
            "build_name": build_name,
            "variant": variant_name,
            "idx": int(idx),
            "pending": True,
            "cache_key": key,
        }
    if not payload:
        raise HTTPException(status_code=404, detail="Link not found at this index.")
    return {
        "ok": True,
        "class_name": class_name,
        "build_name": build_name,
        "variant": variant_name,
        "pending": False,
        "cache_key": key,
        "detail": payload,
    }


@app.get("/api/builds/{class_name}/{build_name}/node")
def build_link_node_api(
    request: Request,
    response: Response,
    class_name: str,
    build_name: str,
    node: str,
    side: str = "wdc",
    variant: Optional[str] = None,
):
    node_value = _clean_text(node)
    if not node_value:
        raise HTTPException(status_code=400, detail="node is required.")
    owner_key, _ = _get_or_create_owner_key(request)
    _set_owner_cookie_if_needed(response, request, owner_key)
    build_dir = _resolve_build_dir(class_name, build_name)
    if not build_dir or not _owner_can_access_build(owner_key, build_dir):
        raise HTTPException(status_code=404, detail="Build not found.")
    variant_dir, variant_name = _resolve_link_explorer_variant_dir(build_dir, variant=variant)
    if not variant_dir or not variant_name:
        raise HTTPException(status_code=404, detail="No link files available for this build.")

    payload = _build_node_payload(variant_dir, side, node_value)
    return {
        "ok": True,
        "class_name": class_name,
        "build_name": build_name,
        "variant": variant_name,
        "node": payload,
    }


@app.get("/api/dashboard")
def dashboard_api(
    request: Request,
    response: Response,
    job_limit: int = 80,
    build_limit: int = 200,
    test_mode: Optional[bool] = None,
):
    owner_key, _ = _get_or_create_owner_key(request)
    _set_owner_cookie_if_needed(response, request, owner_key)
    job_limit = max(1, min(int(job_limit), 200))
    build_limit = max(1, min(int(build_limit), 200))
    dashboard = _build_dashboard_state(
        job_limit=job_limit,
        build_limit=build_limit,
        test_mode=test_mode,
        owner_key=owner_key,
    )

    jobs = []
    for j in dashboard["all_jobs"]:
        jid = j["id"]
        jobs.append(
            {
                **j,
                "times": dashboard["jobs_times"].get(jid, {}),
                "params": dashboard["jobs_params"].get(jid, {}),
                "outputs": dashboard["jobs_outputs"].get(jid, {}),
                "subjobs": dashboard["jobs_subjobs"].get(jid, []),
            }
        )

    builds = []
    for b in dashboard["builds"]:
        builds.append(
            {
                "class_name": b.get("class_name"),
                "build_name": b.get("build_name"),
                "path": b.get("path"),
                "done_at": b.get("done_at"),
                "is_completed": bool(b.get("is_completed")),
                "done_label": b.get("done_label") or "Last update",
                "with_link": b.get("with_link"),
                "without_link": b.get("without_link"),
                "variants_same": b.get("variants_same"),
                "config_groups": b.get("config_groups") or [],
                "endpoint_label": b.get("endpoint_label") or "Wikidata",
                "linking_elements_text": b.get("linking_elements_text") or "",
                "linking_stats_text": b.get("linking_stats_text") or "",
            }
        )

    response = {
        "server_ts": time.time(),
        "job_count": len(jobs),
        "active_job_count": len(dashboard["active_jobs"]),
        "visible_job_count": len(dashboard["jobs_for_panel"]),
        "build_count": len(builds),
        "active_job_ids": [j["id"] for j in dashboard["active_jobs"]],
        "visible_job_ids": [j["id"] for j in dashboard["jobs_for_panel"]],
        "jobs": jobs,
        "builds": builds,
    }
    return response


@app.get("/api/class_parts/{class_name}")
def class_parts_api(class_name: str):
    return _build_class_parts_info(class_name)


@app.get("/api/preflight")
def preflight_api(
    class_name: str,
    parts_spec: str = "all",
    matching_mode: str = "property",
    wdc_predicate_pattern: str = "",
    wdc_pattern_search_in: str = "predicate",
    target_endpoint: str = "wikidata",
    target_endpoint_url: str = "",
    target_prefixes: str = "",
    property_mapping_rules: str = "",
    target_property: str = "",
    target_class: str = "",
    wikidata_property: str = "",
    wkd_class: str = "",
    ignore_chars: str = "",
    use_local_only: bool = False,
    include_wikidata_preview: bool = True,
    scan_limit_lines: int = 30000,
):
    return _build_preflight_report(
        class_name=class_name,
        parts_spec=parts_spec,
        matching_mode=matching_mode,
        wdc_predicate_pattern=wdc_predicate_pattern,
        wdc_pattern_search_in=wdc_pattern_search_in,
        target_endpoint=target_endpoint,
        target_endpoint_url=target_endpoint_url,
        target_prefixes=target_prefixes,
        property_mapping_rules=property_mapping_rules,
        target_property=target_property,
        target_class=target_class,
        wikidata_property=wikidata_property,
        wkd_class=wkd_class,
        ignore_chars=ignore_chars,
        use_local_only=bool(use_local_only),
        include_wikidata_preview=bool(include_wikidata_preview),
        scan_limit_lines=int(scan_limit_lines),
    )


@app.post("/jobs/{job_id}/cancel")
def cancel_job(request: Request, job_id: int):
    owner_key, _ = _get_or_create_owner_key(request)
    job = db.get_job(job_id, owner_key=owner_key)
    if not job:
        return _redirect_with_owner(request)
    if job["status"] not in {"running", "queued"}:
        return _redirect_with_owner(request)
    db.request_cancel(job_id)
    db.request_cancel_subjob(job_id, "align")
    db.request_cancel_subjob(job_id, "build")
    if job["status"] == "queued":
        db.update_subjob_by_type(job_id, "align", status="cancelled", ended_at=time.time())
        db.update_subjob_by_type(job_id, "build", status="cancelled", ended_at=time.time())
    db.insert_event(job_id, "system", "Cancel requested (job)")
    return _redirect_with_owner(request)


@app.post("/jobs/{job_id}/cancel_subjob/{subjob_type}")
def cancel_subjob(request: Request, job_id: int, subjob_type: str):
    owner_key, _ = _get_or_create_owner_key(request)
    if subjob_type not in {"align", "build"}:
        return _redirect_with_owner(request)
    job = db.get_job(job_id, owner_key=owner_key)
    if not job:
        return _redirect_with_owner(request)
    if job["status"] not in {"running", "queued"}:
        return _redirect_with_owner(request)
    sj = db.get_subjob(job_id, subjob_type)
    if not sj or sj["status"] not in {"running", "queued"}:
        return _redirect_with_owner(request)

    db.request_cancel_subjob(job_id, subjob_type)
    if subjob_type == "align":
        # Align cancel implies full job cancel and build cancel.
        db.request_cancel(job_id)
        db.request_cancel_subjob(job_id, "build")
        db.insert_event(job_id, "system", "Cancel requested (align; build will be cancelled too)")
    else:
        # Build cancel does not interrupt align. If already in build, stop current process.
        if job["phase"] == "build":
            db.request_cancel(job_id)
        db.insert_event(job_id, "system", "Cancel requested (build only)")

    if job["status"] == "queued":
        if subjob_type == "align":
            db.update_subjob_by_type(job_id, "align", status="cancelled", ended_at=time.time())
            db.update_subjob_by_type(job_id, "build", status="cancelled", ended_at=time.time())
        else:
            db.update_subjob_by_type(job_id, "build", status="cancelled", ended_at=time.time())
    return _redirect_with_owner(request)


@app.post("/jobs/{job_id}/rerun")
def rerun_job(request: Request, job_id: int):
    owner_key, _ = _get_or_create_owner_key(request)
    job = db.get_job(job_id, owner_key=owner_key)
    if not job:
        return _redirect_with_owner(request)
    try:
        params = json.loads(job["params_json"])
    except Exception:
        return _redirect_with_owner(request)
    # Always enforce one-to-one behavior in reruns.
    params["strict_duplicate_key_filter"] = True
    _insert_job_for_owner(params, owner_key)
    return _redirect_with_owner(request)


@app.post("/jobs/{job_id}/rerun_nocache")
def rerun_job_nocache(request: Request, job_id: int):
    owner_key, _ = _get_or_create_owner_key(request)
    job = db.get_job(job_id, owner_key=owner_key)
    if not job:
        return _redirect_with_owner(request)
    try:
        params = json.loads(job["params_json"])
    except Exception:
        return _redirect_with_owner(request)
    # Always enforce one-to-one behavior in reruns.
    params["strict_duplicate_key_filter"] = True
    params["force_align"] = True
    params["skip_build"] = False
    params.pop("require_cached_align", None)
    _insert_job_for_owner(params, owner_key)
    return _redirect_with_owner(request)


@app.post("/jobs/{job_id}/rerun_align")
def rerun_align(request: Request, job_id: int):
    owner_key, _ = _get_or_create_owner_key(request)
    job = db.get_job(job_id, owner_key=owner_key)
    if not job:
        return _redirect_with_owner(request)
    try:
        params = json.loads(job["params_json"])
    except Exception:
        return _redirect_with_owner(request)
    # Always enforce one-to-one behavior in reruns.
    params["strict_duplicate_key_filter"] = True
    params["skip_build"] = True
    _insert_job_for_owner(params, owner_key)
    return _redirect_with_owner(request)


@app.post("/jobs/{job_id}/rerun_build")
def rerun_build(request: Request, job_id: int):
    owner_key, _ = _get_or_create_owner_key(request)
    job = db.get_job(job_id, owner_key=owner_key)
    if not job:
        return _redirect_with_owner(request)
    try:
        params = json.loads(job["params_json"])
    except Exception:
        return _redirect_with_owner(request)
    # Always enforce one-to-one behavior in reruns.
    params["strict_duplicate_key_filter"] = True
    params["require_cached_align"] = True
    params["skip_build"] = False
    _insert_job_for_owner(params, owner_key)
    return _redirect_with_owner(request)


@app.post("/jobs/{job_id}/delete")
def delete_job(request: Request, job_id: int):
    owner_key, _ = _get_or_create_owner_key(request)
    job = db.get_job(job_id, owner_key=owner_key)
    if not job:
        return _redirect_with_owner(request)
    # Never delete active jobs to avoid orphaned worker processes.
    if job["status"] in {"running", "queued"}:
        return _redirect_with_owner(request)
    db.delete_job(job_id)
    return _redirect_with_owner(request)


@app.post("/jobs/delete_stopped")
def delete_stopped_jobs(request: Request):
    owner_key, _ = _get_or_create_owner_key(request)
    # Remove only non-active jobs; keep running/queued jobs intact.
    for row in db.list_jobs(limit=50000, owner_key=owner_key):
        status = str(row["status"] or "").strip().lower()
        if status in {"running", "queued"}:
            continue
        try:
            db.delete_job(int(row["id"]))
        except Exception:
            continue
    return _redirect_with_owner(request)


@app.post("/jobs")
def create_job(
    request: Request,
    matching_mode: str = Form("property"),
    class_name: str = Form(...),
    parts_spec: str = Form(""),
    wdc_predicate_pattern: str = Form(""),
    wdc_pattern_search_in: str = Form("predicate"),
    target_endpoint: str = Form("wikidata"),
    target_endpoint_url: str = Form(""),
    target_prefixes: str = Form(""),
    property_mapping_rules: str = Form(""),
    target_property: str = Form(""),
    target_class: str = Form(""),
    wikidata_property: str = Form(""),
    wkd_class: str = Form(""),
    ignore_chars: str = Form(""),
    force_align: Optional[str] = Form(None),
    use_local_only: Optional[str] = Form(None),
):
    owner_key, _ = _get_or_create_owner_key(request)
    raw_params = {
        "matching_mode": _clean_text(matching_mode),
        "class_name": _clean_text(class_name),
        "parts_spec": _clean_text(parts_spec),
        "wdc_predicate_pattern": _clean_text(wdc_predicate_pattern),
        "wdc_pattern_search_in": _clean_text(wdc_pattern_search_in),
        "target_endpoint": _clean_text(target_endpoint),
        "target_endpoint_url": _clean_text(target_endpoint_url),
        "target_prefixes": _clean_text(target_prefixes),
        "property_mapping_rules": _clean_text(property_mapping_rules),
        "target_property": _clean_text(target_property),
        "target_class": _clean_text(target_class),
        "wikidata_property": _clean_text(wikidata_property),
        "wkd_class": _clean_text(wkd_class),
        "ignore_chars": _clean_text(ignore_chars),
        "force_align": bool(force_align),
        "use_local_only": bool(use_local_only),
        "strict_duplicate_key_filter": True,
    }
    params, validation_error = _validate_and_normalize_job_params(raw_params)
    if validation_error:
        return _redirect_with_owner(request, url=f"/?form_error={quote_plus(validation_error)}", status_code=303)
    _insert_job_for_owner(params, owner_key)
    return _redirect_with_owner(request)


@app.get("/refresh_classes")
def refresh_classes():
    try:
        _refresh_wdc_classes_from_remote()
    except Exception as exc:
        msg = f"Class refresh failed; local cache/catalog kept unchanged. ({exc})"
        return RedirectResponse(url=f"/?form_error={quote_plus(msg)}", status_code=303)
    return RedirectResponse(url="/", status_code=303)


@app.get("/builds/{class_name}/{build_name}/download")
def download_build(request: Request, class_name: str, build_name: str):
    owner_key, _ = _get_or_create_owner_key(request)
    build_dir = _resolve_build_dir(class_name, build_name)
    if not build_dir or not _owner_can_access_build(owner_key, build_dir):
        return _redirect_with_owner(request)
    data_root = Path("data").resolve()
    build_config = _load_build_config(build_dir)
    endpoint_token = _endpoint_filename_token(build_config)
    class_token = _safe_filename_token(class_name, fallback="class")
    build_token = _safe_filename_token(build_name, fallback="build")
    fd, zip_path = tempfile.mkstemp(prefix=f"beam_{class_name}_{build_name}_", suffix=".zip")
    os.close(fd)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for fp in build_dir.rglob("*"):
            if fp.is_file():
                zf.write(fp, arcname=str(fp.resolve().relative_to(data_root)))
    filename = f"{class_token}_{build_token}_{endpoint_token}.zip"
    return FileResponse(
        zip_path,
        media_type="application/zip",
        filename=filename,
        background=BackgroundTask(_safe_unlink, zip_path),
    )


@app.get("/builds/{class_name}/{build_name}/sakey/download/{artifact_idx}")
def download_sakey_artifact(request: Request, class_name: str, build_name: str, artifact_idx: int):
    owner_key, _ = _get_or_create_owner_key(request)
    build_dir = _resolve_build_dir(class_name, build_name)
    if not build_dir or not _owner_can_access_build(owner_key, build_dir):
        return _redirect_with_owner(request)
    path = _resolve_sakey_artifact(build_dir, artifact_idx)
    if not path:
        raise HTTPException(status_code=404, detail="SAKEY artifact not found.")
    return FileResponse(
        str(path),
        media_type="application/octet-stream",
        filename=path.name,
    )


@app.get("/builds/download_selected")
@app.get("/builds/download_selected/")
def download_selected_builds_get():
    return RedirectResponse(url="/?form_error=Select+one+or+more+builds+before+downloading.", status_code=303)


@app.post("/builds/download_selected")
@app.post("/builds/download_selected/")
def download_selected_builds(request: Request, selected_builds: str = Form("[]")):
    owner_key, _ = _get_or_create_owner_key(request)
    try:
        parsed = json.loads(_clean_text(selected_builds) or "[]")
    except Exception:
        parsed = []
    refs = []
    if isinstance(parsed, list):
        refs = parsed

    unique_keys = set()
    selected_dirs = []
    for item in refs[:300]:
        class_name = ""
        build_name = ""
        if isinstance(item, dict):
            class_name = _clean_text(str(item.get("class_name", "")))
            build_name = _clean_text(str(item.get("build_name", "")))
        elif isinstance(item, str):
            if "::" in item:
                left, right = item.split("::", 1)
                class_name = _clean_text(left)
                build_name = _clean_text(right)
        if not class_name or not build_name:
            continue
        key = f"{class_name}::{build_name}"
        if key in unique_keys:
            continue
        unique_keys.add(key)
        build_dir = _resolve_build_dir(class_name, build_name)
        if not build_dir or not _owner_can_access_build(owner_key, build_dir):
            continue
        build_config = _load_build_config(build_dir)
        endpoint_token = _endpoint_filename_token(build_config)
        class_token = _safe_filename_token(class_name, fallback="class")
        build_token = _safe_filename_token(build_name, fallback="build")
        folder_prefix = f"{class_token}_{build_token}_{endpoint_token}"
        selected_dirs.append((class_name, build_name, build_dir, folder_prefix))

    if not selected_dirs:
        return _redirect_with_owner(request, url="/?form_error=No+valid+build+selected+for+download.", status_code=303)

    data_root = Path("data").resolve()
    fd, zip_path = tempfile.mkstemp(prefix="beam_selected_builds_", suffix=".zip")
    os.close(fd)

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for class_name, _, build_dir, folder_prefix in selected_dirs:
            class_token = _safe_filename_token(class_name, fallback="class")
            for fp in build_dir.rglob("*"):
                if not fp.is_file():
                    continue
                try:
                    rel = fp.resolve().relative_to(build_dir.resolve())
                    arcname = str(Path(class_token) / folder_prefix / rel)
                except Exception:
                    arcname = str(Path(class_token) / folder_prefix / fp.name)
                zf.write(fp, arcname=arcname)

    ts = time.strftime("%Y%m%d_%H%M%S")
    filename = f"selected_builds_{len(selected_dirs)}_{ts}.zip"
    return FileResponse(
        zip_path,
        media_type="application/zip",
        filename=filename,
        background=BackgroundTask(_safe_unlink, zip_path),
    )


@app.post("/builds/{class_name}/{build_name}/delete")
def delete_build(request: Request, class_name: str, build_name: str):
    owner_key, _ = _get_or_create_owner_key(request)
    build_dir = _resolve_build_dir(class_name, build_name)
    if not build_dir or not _owner_can_access_build(owner_key, build_dir):
        return _redirect_with_owner(request)
    try:
        _delete_jobs_for_build_dir(build_dir, owner_key=owner_key)
    except Exception:
        pass
    shutil.rmtree(build_dir, ignore_errors=True)
    return _redirect_with_owner(request)


@app.post("/builds/purge_low_links")
def purge_low_link_builds(request: Request, max_links: int = Form(10)):
    owner_key, _ = _get_or_create_owner_key(request)
    try:
        threshold = int(max_links)
    except Exception:
        threshold = 10
    threshold = max(0, threshold)

    purged = 0
    # Use a high scan limit so this action can clean the full history.
    owned_build_keys = _owned_build_path_keys(owner_key)
    for build in _scan_builds(limit=100000):
        build_path = Path(build.get("path") or "")
        if not _build_is_owned_by_keys(build_path, owned_build_keys, owner_key=owner_key, claim_unowned=True):
            continue
        class_name = str(build.get("class_name") or "").strip()
        build_name = str(build.get("build_name") or "").strip()
        if not class_name or not build_name:
            continue
        variant = build.get("with_link") or build.get("without_link")
        if not isinstance(variant, dict):
            continue
        try:
            links_count = int(variant.get("links_count") or 0)
        except Exception:
            links_count = 0
        if links_count >= threshold:
            continue
        build_dir = _resolve_build_dir(class_name, build_name)
        if not build_dir or not _owner_can_access_build(owner_key, build_dir):
            continue
        try:
            _delete_jobs_for_build_dir(build_dir, owner_key=owner_key)
        except Exception:
            pass
        shutil.rmtree(build_dir, ignore_errors=True)
        purged += 1
    return _redirect_with_owner(request, url=f"/?purged={purged}", status_code=303)


@app.post("/builds/{class_name}/{build_name}/rerun")
def rerun_build_from_build_card(request: Request, class_name: str, build_name: str):
    owner_key, _ = _get_or_create_owner_key(request)
    build_dir = _resolve_build_dir(class_name, build_name)
    if not build_dir or not _owner_can_access_build(owner_key, build_dir):
        return _redirect_with_owner(request)
    try:
        params, validation_error = _rerun_params_from_build_config(build_dir, class_name, owner_key=owner_key)
        if validation_error:
            msg = f"Cannot rerun build: {validation_error}"
            return _redirect_with_owner(request, url=f"/?form_error={quote_plus(msg)}", status_code=303)
        _insert_job_for_owner(params, owner_key)
    except Exception as exc:
        msg = f"Cannot rerun build: {exc}"
        return _redirect_with_owner(request, url=f"/?form_error={quote_plus(msg)}", status_code=303)
    return _redirect_with_owner(request)


@app.websocket("/ws/logs/{job_id}")
async def ws_logs(websocket: WebSocket, job_id: int):
    await websocket.accept()
    try:
        owner_key = ownership.get_request_owner_key(websocket)
        job = db.get_job(job_id, owner_key=owner_key)
        if not job:
            await websocket.send_text("Job not found")
            await websocket.close()
            return
        last_id = 0
        def _event_payload(row):
            meta = None
            try:
                if row["meta_json"]:
                    meta = json.loads(row["meta_json"])
            except Exception:
                meta = None
            return {
                "type": "event",
                "id": row["id"],
                "ts": row["ts"],
                "level": row["level"],
                "message": row["message"],
                "phase": row["phase"],
                "kind": row["kind"],
                "step": row["step"],
                "worker": row["worker"],
                "progress_pct": row["progress_pct"],
                "meta": meta,
            }
        # send recent history
        rows = db.list_events(job_id, since_id=None, limit=200)
        for r in rows:
            await websocket.send_text(json.dumps(_event_payload(r)))
            last_id = r["id"]
        while True:
            # Push updates at a fixed cadence even if client pings stall.
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=0.5)
            except asyncio.TimeoutError:
                pass
            job = db.get_job(job_id, owner_key=owner_key)
            if job:
                payload = {
                    "type": "progress",
                    "status": job["status"],
                    "cancel_requested": job["cancel_requested"],
                    "phase": job["phase"],
                    "progress_text": job["progress_text"],
                    "progress_pct": job["progress_pct"],
                    "current_step": job["current_step"],
                    "current_file": job["current_file"],
                    "result_path": job["result_path"],
                    "align_dir": job["align_dir"],
                    "reused_align": bool(job["reused_align"]),
                    "error_message": job["error_message"],
                    "final_links_count": job["final_links_count"],
                    "outputs": _job_outputs(job),
                    "subjobs": [dict(s) for s in db.list_subjobs(job_id)],
                }
                await websocket.send_text(json.dumps(payload))
            rows = db.list_events(job_id, since_id=last_id, limit=200)
            if rows:
                for r in rows:
                    await websocket.send_text(json.dumps(_event_payload(r)))
                    last_id = r["id"]
    except WebSocketDisconnect:
        return

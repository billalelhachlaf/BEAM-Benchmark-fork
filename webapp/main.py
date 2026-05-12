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
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, Form, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.background import BackgroundTask

from beam import db
from beam.wdc_classes import fetch_wdc_classes, load_wdc_classes_catalog, save_wdc_classes_catalog
from scripts import align as align_script

app = FastAPI()
app.mount("/static", StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
TUTORIAL_MD_PATH = Path("docs") / "user" / "tutorial.md"

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

_MODULES = (
    "forms_and_inputs.py",
    "sakey_core.py",
    "sakey_artifacts.py",
    "preflight.py",
    "builds.py",
    "link_explorer.py",
    "jobs_dashboard.py",
    "routes_pages_sakey_builds.py",
    "routes_jobs_downloads_ws.py",
)


def _load_modules() -> None:
    module_dir = Path(__file__).parent / "modules"
    namespace = globals()
    for module_name in _MODULES:
        module_path = module_dir / module_name
        code = module_path.read_text()
        exec(compile(code, str(module_path), "exec"), namespace)


_load_modules()

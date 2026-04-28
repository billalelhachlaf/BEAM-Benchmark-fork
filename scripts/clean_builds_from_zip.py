#!/usr/bin/env python3
import argparse
import json
import re
import time
import zipfile
from pathlib import Path


def ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    print(f"[{ts()}] {msg}", flush=True)


def normalize_wd_entity(token: str) -> str:
    t = (token or "").strip()
    if not t:
        return t
    m = re.match(r"^https?://(?:www\.)?wikidata\.org/entity/([PpQq]\d+)$", t)
    if m:
        return f"http://www.wikidata.org/entity/{m.group(1).upper()}"
    return t


def is_allowed_wdc_subject(token: str) -> bool:
    t = (token or "").strip()
    if t.startswith("<") and t.endswith(">"):
        t = t[1:-1]
    return t.startswith("_:")


def scan_seen_subjects(path: Path, target_subjects: set[str], normalize_wd: bool) -> tuple[int, set[str]]:
    total = 0
    seen: set[str] = set()
    if not path.exists():
        return total, seen
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            total += 1
            subj = line.split("\t", 1)[0].strip()
            if normalize_wd:
                subj = normalize_wd_entity(subj)
            if subj in target_subjects:
                seen.add(subj)
    return total, seen


def parse_ent_links(path: Path) -> list[tuple[str, str, str]]:
    rows: list[tuple[str, str, str]] = []
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            raw = line.rstrip("\n")
            if not raw:
                continue
            parts = raw.split("\t")
            if len(parts) < 2:
                continue
            w1 = parts[0].strip()
            w2 = parts[1].strip()
            rows.append((w1, w2, raw))
    return rows


def filter_triples_by_subject(path: Path, keep_subjects: set[str], normalize_wd: bool) -> tuple[int, int]:
    if not path.exists():
        return 0, 0
    tmp = path.with_suffix(path.suffix + ".tmp")
    total = 0
    kept = 0
    with path.open("r", encoding="utf-8", errors="ignore") as src, tmp.open("w", encoding="utf-8") as dst:
        for line in src:
            total += 1
            subj = line.split("\t", 1)[0].strip()
            if normalize_wd:
                subj = normalize_wd_entity(subj)
            if subj in keep_subjects:
                dst.write(line)
                kept += 1
    tmp.replace(path)
    return total, kept


def rewrite_ent_links(path: Path, rows: list[tuple[str, str, str]]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for _, _, raw in rows:
            f.write(raw + "\n")


def audit_variant(variant_dir: Path) -> dict:
    ent = variant_dir / "ent_links"
    a1 = variant_dir / "attr_triples_1"
    r1 = variant_dir / "rel_triples_1"
    a2 = variant_dir / "attr_triples_2"
    r2 = variant_dir / "rel_triples_2"
    if not all(p.exists() for p in [ent, a1, r1, a2, r2]):
        return {"status": "missing_files"}

    rows = parse_ent_links(ent)
    w1 = {x[0] for x in rows if is_allowed_wdc_subject(x[0])}
    w2 = {normalize_wd_entity(x[1]) for x in rows}
    _, seen_a1 = scan_seen_subjects(a1, w1, normalize_wd=False)
    _, seen_r1 = scan_seen_subjects(r1, w1, normalize_wd=False)
    seen_w1 = seen_a1 | seen_r1
    _, seen_a2 = scan_seen_subjects(a2, w2, normalize_wd=True)
    _, seen_r2 = scan_seen_subjects(r2, w2, normalize_wd=True)
    seen_w2 = seen_a2 | seen_r2
    miss_w1 = w1 - seen_w1
    miss_w2 = w2 - seen_w2
    status = "ok" if (not miss_w1 and not miss_w2) else "ko"
    return {
        "status": status,
        "ent_links_rows": len(rows),
        "wdc_linked_count": len(w1),
        "wd_linked_count": len(w2),
        "graph1_subjects_count": None,
        "graph2_subjects_count": None,
        "miss_wdc_in_g1": len(miss_w1),
        "miss_wd_in_g2": len(miss_w2),
        "extra_g2_not_in_links": None,
    }


def clean_variant(variant_dir: Path) -> dict:
    ent = variant_dir / "ent_links"
    a1 = variant_dir / "attr_triples_1"
    r1 = variant_dir / "rel_triples_1"
    a2 = variant_dir / "attr_triples_2"
    r2 = variant_dir / "rel_triples_2"
    if not all(p.exists() for p in [ent, a1, r1, a2, r2]):
        return {"status": "skipped_missing_files"}

    rows_all = parse_ent_links(ent)
    rows = [row for row in rows_all if is_allowed_wdc_subject(row[0])]
    # Pass 1: keep triples only for subjects present in ent_links.
    initial_w1 = {x[0] for x in rows}
    initial_w2 = {normalize_wd_entity(x[1]) for x in rows}
    a1_total, a1_kept = filter_triples_by_subject(a1, initial_w1, normalize_wd=False)
    r1_total, r1_kept = filter_triples_by_subject(r1, initial_w1, normalize_wd=False)
    a2_total, a2_kept = filter_triples_by_subject(a2, initial_w2, normalize_wd=True)
    r2_total, r2_kept = filter_triples_by_subject(r2, initial_w2, normalize_wd=True)

    # Collect which linked subjects truly exist in filtered graph files.
    _, seen_a1 = scan_seen_subjects(a1, initial_w1, normalize_wd=False)
    _, seen_r1 = scan_seen_subjects(r1, initial_w1, normalize_wd=False)
    seen_w1 = seen_a1 | seen_r1
    _, seen_a2 = scan_seen_subjects(a2, initial_w2, normalize_wd=True)
    _, seen_r2 = scan_seen_subjects(r2, initial_w2, normalize_wd=True)
    seen_w2 = seen_a2 | seen_r2

    # Prune ent_links to only links with both endpoints present in graph1/graph2.
    filtered_rows = []
    for w1, w2, raw in rows:
        if w1 in seen_w1 and normalize_wd_entity(w2) in seen_w2:
            filtered_rows.append((w1, w2, raw))

    final_w1 = {x[0] for x in filtered_rows}
    final_w2 = {normalize_wd_entity(x[1]) for x in filtered_rows}
    rewrite_ent_links(ent, filtered_rows)

    # Pass 2: enforce final strict closure to ent_links endpoints.
    filter_triples_by_subject(a1, final_w1, normalize_wd=False)
    filter_triples_by_subject(r1, final_w1, normalize_wd=False)
    filter_triples_by_subject(a2, final_w2, normalize_wd=True)
    filter_triples_by_subject(r2, final_w2, normalize_wd=True)

    return {
        "status": "cleaned",
        "ent_links_before": len(rows_all),
        "ent_links_after": len(filtered_rows),
        "attr1_before": a1_total,
        "attr1_after": a1_kept,
        "rel1_before": r1_total,
        "rel1_after": r1_kept,
        "attr2_before": a2_total,
        "attr2_after": a2_kept,
        "rel2_before": r2_total,
        "rel2_after": r2_kept,
    }


def zip_dir(src_dir: Path, out_zip: Path) -> None:
    with zipfile.ZipFile(out_zip, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as z:
        for p in src_dir.rglob("*"):
            if p.is_file():
                z.write(p, p.relative_to(src_dir))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--zip", required=True, help="Path to source zip containing builds.")
    ap.add_argument("--input-dir", default="", help="If set, use an already extracted directory and skip zip extraction.")
    ap.add_argument("--work-root", default="data/restore_from_zip", help="Work directory.")
    ap.add_argument("--out-zip", default="", help="Output cleaned zip path.")
    ap.add_argument(
        "--include-classes",
        default="",
        help="Comma-separated lowercased class folder names to include (e.g. airport,book,hotel,museum).",
    )
    ap.add_argument(
        "--endpoint-priority",
        default="wikidata,yago,dbpedia",
        help="Comma-separated endpoint priority order inferred from build dir suffix.",
    )
    ap.add_argument(
        "--endpoint-include",
        default="",
        help="Optional comma-separated endpoint whitelist (e.g. wikidata).",
    )
    args = ap.parse_args()

    src_zip = Path(args.zip).resolve()
    if args.input_dir:
        extracted_dir = Path(args.input_dir).resolve()
        if not extracted_dir.exists():
            raise FileNotFoundError(extracted_dir)
    else:
        if not src_zip.exists():
            raise FileNotFoundError(src_zip)
    work_root = Path(args.work_root).resolve()
    work_root.mkdir(parents=True, exist_ok=True)

    stamp = time.strftime("%Y%m%d_%H%M%S")
    src_name = src_zip.stem
    report_json = Path("reports") / f"{src_name}_clean_report_{stamp}.json"
    report_tsv = Path("reports") / f"{src_name}_clean_report_{stamp}.tsv"
    out_zip = Path(args.out_zip).resolve() if args.out_zip else (work_root / f"{src_name}_cleaned_{stamp}.zip")

    if args.input_dir:
        log(f"using pre-extracted dir: {extracted_dir}")
    else:
        extracted_dir = work_root / f"{src_name}_extracted_{stamp}"
        extracted_dir.mkdir(parents=True, exist_ok=False)
        log(f"extracting zip: {src_zip}")
        with zipfile.ZipFile(src_zip) as z:
            z.extractall(extracted_dir)
        log(f"extracted into: {extracted_dir}")

    configs = sorted(extracted_dir.rglob("BUILD_CONFIG.json"))
    log(f"found {len(configs)} builds")

    include_classes = {x.strip().lower() for x in (args.include_classes or "").split(",") if x.strip()}
    endpoint_priority = [x.strip().lower() for x in (args.endpoint_priority or "").split(",") if x.strip()]
    endpoint_rank = {ep: i for i, ep in enumerate(endpoint_priority)}
    endpoint_include = {x.strip().lower() for x in (args.endpoint_include or "").split(",") if x.strip()}

    filtered_configs = []
    for cfg in configs:
        build_dir = cfg.parent
        parts = build_dir.relative_to(extracted_dir).parts
        class_folder = (parts[0] if parts else "").lower()
        build_name = build_dir.name.lower()
        endpoint = build_name.rsplit("_", 1)[-1] if "_" in build_name else ""
        if include_classes and class_folder not in include_classes:
            continue
        if endpoint_include and endpoint not in endpoint_include:
            continue
        filtered_configs.append((cfg, class_folder, endpoint))

    filtered_configs.sort(
        key=lambda x: (
            endpoint_rank.get(x[2], 10_000),
            x[1],
            str(x[0]),
        )
    )
    log(
        f"selected {len(filtered_configs)} builds "
        f"(classes={sorted(include_classes) if include_classes else 'all'}, "
        f"endpoint_include={sorted(endpoint_include) if endpoint_include else 'all'})"
    )

    report = []
    for cfg, _, _ in filtered_configs:
        build_dir = cfg.parent
        rel_build = str(build_dir.relative_to(extracted_dir))
        log(f"processing {rel_build}")
        item = {
            "build_path": rel_build,
            "before": {},
            "clean": {},
            "after": {},
        }
        for variant in ("without_link_code", "with_link_code"):
            vdir = build_dir / variant
            item["before"][variant] = audit_variant(vdir)
            item["clean"][variant] = clean_variant(vdir)
            item["after"][variant] = audit_variant(vdir)
        report.append(item)

    Path("reports").mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    with report_tsv.open("w", encoding="utf-8") as f:
        f.write("build\tvariant\tbefore_status\tafter_status\tbefore_miss_wdc_in_g1\tafter_miss_wdc_in_g1\tbefore_miss_wd_in_g2\tafter_miss_wd_in_g2\tbefore_extra_g2_not_in_links\tafter_extra_g2_not_in_links\tent_links_before\tent_links_after\n")
        for item in report:
            for variant in ("without_link_code", "with_link_code"):
                b = item["before"].get(variant, {})
                a = item["after"].get(variant, {})
                c = item["clean"].get(variant, {})
                f.write(
                    f"{item['build_path']}\t{variant}\t{b.get('status','-')}\t{a.get('status','-')}\t"
                    f"{b.get('miss_wdc_in_g1','-')}\t{a.get('miss_wdc_in_g1','-')}\t"
                    f"{b.get('miss_wd_in_g2','-')}\t{a.get('miss_wd_in_g2','-')}\t"
                    f"{b.get('extra_g2_not_in_links','-')}\t{a.get('extra_g2_not_in_links','-')}\t"
                    f"{c.get('ent_links_before','-')}\t{c.get('ent_links_after','-')}\n"
                )
    log(f"report json: {report_json}")
    log(f"report tsv: {report_tsv}")

    log(f"creating cleaned zip: {out_zip}")
    zip_dir(extracted_dir, out_zip)
    log(f"done cleaned zip: {out_zip}")


if __name__ == "__main__":
    main()

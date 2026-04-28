#!/usr/bin/env python3
import json
import time
from pathlib import Path

from beam import db

REPORT_DIR = Path("reports")
REPORT_DIR.mkdir(parents=True, exist_ok=True)

TARGET = [
    (79, "Book", "beam_20260317_013056"),
    (80, "Museum", "beam_20260316_161205"),
    (81, "Museum", "beam_20260318_004913"),
    (82, "Museum", "beam_20260318_004700"),
    (83, "Book", "beam_20260317_134420"),
    (84, "Book", "beam_20260317_124532"),
    (85, "Airport", "beam_20260316_233819"),
    (86, "Airport", "beam_20260317_012036"),
    (87, "Airport", "beam_20260317_004908"),
    (88, "CollegeOrUniversity", "beam_20260317_023600"),
    (89, "CollegeOrUniversity", "beam_20260317_023232"),
    (90, "CollegeOrUniversity", "beam_20260317_001234"),
    (91, "Hotel", "beam_20260318_124207"),
]


def ts():
    return time.strftime("%Y-%m-%d %H:%M:%S")


def log(msg):
    print(f"[{ts()}] {msg}", flush=True)


def norm_token(x):
    t = (x or "").strip()
    if not t:
        return t
    low = t.lower()
    if low.startswith("http://www.wikidata.org/entity/") or low.startswith("https://www.wikidata.org/entity/"):
        return low
    return t


def read_subjects(path, norm=False):
    s = set()
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for ln in f:
            ln = ln.rstrip("\n")
            if not ln:
                continue
            p = ln.split("\t", 1)
            if not p:
                continue
            subj = p[0].strip()
            s.add(norm_token(subj) if norm else subj)
    return s


def read_ent_links(path):
    w1 = set()
    w2 = set()
    rows = 0
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for ln in f:
            ln = ln.rstrip("\n")
            if not ln:
                continue
            p = ln.split("\t")
            if len(p) < 2:
                continue
            rows += 1
            w1.add(p[0].strip())
            w2.add(norm_token(p[1].strip()))
    return rows, w1, w2


def audit_build(build_dir):
    out = {}
    for variant in ("without_link_code", "with_link_code"):
        vd = build_dir / variant
        ent = vd / "ent_links"
        a1 = vd / "attr_triples_1"
        r1 = vd / "rel_triples_1"
        a2 = vd / "attr_triples_2"
        r2 = vd / "rel_triples_2"
        if not all(p.exists() for p in [ent, a1, r1, a2, r2]):
            out[variant] = {"status": "KO", "error": "missing_files"}
            continue
        rows, w1, w2 = read_ent_links(ent)
        g1 = read_subjects(a1, norm=False) | read_subjects(r1, norm=False)
        g2 = read_subjects(a2, norm=True) | read_subjects(r2, norm=True)
        miss_w1 = sorted(w1 - g1)
        miss_w2 = sorted(w2 - g2)
        extra_g2 = sorted(g2 - w2)
        status = "OK" if (not miss_w1 and not miss_w2 and not extra_g2) else "KO"
        out[variant] = {
            "status": status,
            "ent_links_rows": rows,
            "wdc_linked": len(w1),
            "wd_linked": len(w2),
            "graph1_subjects": len(g1),
            "graph2_subjects": len(g2),
            "miss_wdc_in_g1": len(miss_w1),
            "miss_wd_in_g2": len(miss_w2),
            "extra_g2_not_in_links": len(extra_g2),
            "samples": {
                "miss_wdc_in_g1": miss_w1[:10],
                "miss_wd_in_g2": miss_w2[:10],
                "extra_g2_not_in_links": extra_g2[:10],
            },
        }
    return out


def main():
    db.init_db()
    done_states = {"done", "error", "cancelled"}
    ids = [jid for jid, _, _ in TARGET]
    while True:
        states = {}
        all_done = True
        for jid in ids:
            row = db.get_job(jid)
            st = (row["status"] if row else "missing")
            states[jid] = st
            if st not in done_states:
                all_done = False
        by = {}
        for st in states.values():
            by[st] = by.get(st, 0) + 1
        log("job states: " + ", ".join(f"{k}={v}" for k, v in sorted(by.items())))
        if all_done:
            break
        time.sleep(15)

    results = []
    for jid, class_name, old_name in TARGET:
        row = db.get_job(jid)
        item = {
            "job_id": jid,
            "class_name": class_name,
            "old_build_name": old_name,
            "status": row["status"] if row else "missing",
            "error_message": row["error_message"] if row else "missing_job",
            "result_path": row["result_path"] if row else "",
            "audit": {},
        }
        if row and row["status"] == "done" and row["result_path"]:
            p = Path(row["result_path"])
            if p.exists():
                item["audit"] = audit_build(p)
        results.append(item)

    stamp = time.strftime("%Y%m%d_%H%M%S")
    out_json = REPORT_DIR / f"rebuild_starred_13_results_{stamp}.json"
    out_json.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    out_tsv = REPORT_DIR / f"rebuild_starred_13_results_{stamp}.tsv"
    with out_tsv.open("w", encoding="utf-8") as f:
        f.write("job_id\tclass_name\told_build_name\tstatus\tresult_path\tvariant\taudit_status\tmiss_wdc_in_g1\tmiss_wd_in_g2\textra_g2_not_in_links\n")
        for r in results:
            if not r.get("audit"):
                f.write(f"{r['job_id']}\t{r['class_name']}\t{r['old_build_name']}\t{r['status']}\t{r.get('result_path', '')}\t-\t-\t-\t-\t-\n")
                continue
            for variant, ar in r["audit"].items():
                f.write(
                    f"{r['job_id']}\t{r['class_name']}\t{r['old_build_name']}\t{r['status']}\t{r.get('result_path', '')}\t{variant}\t{ar.get('status', '-')}\t{ar.get('miss_wdc_in_g1', '-')}\t{ar.get('miss_wd_in_g2', '-')}\t{ar.get('extra_g2_not_in_links', '-')}\n"
                )
    log(f"wrote {out_json}")
    log(f"wrote {out_tsv}")


if __name__ == "__main__":
    main()

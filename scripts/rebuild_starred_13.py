#!/usr/bin/env python3
import json
import shutil
import time
from pathlib import Path

from beam import db

TARGET_BUILDS = [
    ("Book", "beam_20260317_013056"),
    ("Museum", "beam_20260316_161205"),
    ("Museum", "beam_20260318_004913"),
    ("Museum", "beam_20260318_004700"),
    ("Book", "beam_20260317_134420"),
    ("Book", "beam_20260317_124532"),
    ("Airport", "beam_20260316_233819"),
    ("Airport", "beam_20260317_012036"),
    ("Airport", "beam_20260317_004908"),
    ("CollegeOrUniversity", "beam_20260317_023600"),
    ("CollegeOrUniversity", "beam_20260317_023232"),
    ("CollegeOrUniversity", "beam_20260317_001234"),
    ("Hotel", "beam_20260318_124207"),
]

REPORT_DIR = Path("reports")
REPORT_DIR.mkdir(parents=True, exist_ok=True)


def ts():
    return time.strftime("%Y-%m-%d %H:%M:%S")


def log(msg):
    print(f"[{ts()}] {msg}", flush=True)


def load_params(class_name, build_name):
    cfg = Path("data") / class_name / build_name / "BUILD_CONFIG.json"
    if not cfg.exists():
        raise FileNotFoundError(str(cfg))
    data = json.loads(cfg.read_text(encoding="utf-8"))
    # Force a fresh new build path/name, keep matching params identical otherwise.
    data.pop("build_name", None)
    data.pop("result_path", None)
    data.pop("created_at", None)
    data.pop("ended_at", None)
    return data


def cleanup_old_build_dirs(targets):
    removed = []
    missing = []
    for class_name, build_name in targets:
        p = Path("data") / class_name / build_name
        if p.exists() and p.is_dir():
            shutil.rmtree(p)
            removed.append(str(p))
        else:
            missing.append(str(p))
    return removed, missing


def enqueue_jobs(job_specs):
    queued = []
    for spec in job_specs:
        jid = db.insert_job(spec["params"])
        queued.append({"job_id": jid, "class_name": spec["class_name"], "old_build_name": spec["old_build_name"]})
        log(f"queued job_id={jid} class={spec['class_name']} old_build={spec['old_build_name']}")
    return queued


def wait_jobs(job_ids, poll_s=10):
    done_states = {"done", "error", "cancelled"}
    while True:
        states = {}
        all_done = True
        for jid in job_ids:
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
            return states
        time.sleep(poll_s)


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
    w1 = set(); w2 = set(); rows = 0
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
    run_stamp = time.strftime("%Y%m%d_%H%M%S")
    prep = []
    for class_name, old_build_name in TARGET_BUILDS:
        params = load_params(class_name, old_build_name)
        prep.append({"class_name": class_name, "old_build_name": old_build_name, "params": params})
    # Save the exact replay params.
    params_path = REPORT_DIR / f"rebuild_starred_13_params_{run_stamp}.json"
    params_path.write_text(json.dumps(prep, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"saved params: {params_path}")

    removed, missing = cleanup_old_build_dirs(TARGET_BUILDS)
    log(f"cleanup done removed={len(removed)} missing={len(missing)}")

    queued = enqueue_jobs(prep)
    qpath = REPORT_DIR / f"rebuild_starred_13_jobs_{run_stamp}.json"
    qpath.write_text(json.dumps(queued, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"queued jobs saved: {qpath}")

    states = wait_jobs([q["job_id"] for q in queued], poll_s=10)
    log("all jobs reached final states")

    results = []
    for q in queued:
        jid = q["job_id"]
        row = db.get_job(jid)
        item = {
            "job_id": jid,
            "class_name": q["class_name"],
            "old_build_name": q["old_build_name"],
            "status": states.get(jid),
            "error_message": row["error_message"] if row else "missing_job",
            "result_path": row["result_path"] if row else "",
            "audit": {},
        }
        if row and row["status"] == "done" and row["result_path"]:
            bdir = Path(row["result_path"])
            if bdir.exists():
                item["audit"] = audit_build(bdir)
        results.append(item)

    out_json = REPORT_DIR / f"rebuild_starred_13_results_{run_stamp}.json"
    out_json.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"results written: {out_json}")

    out_tsv = REPORT_DIR / f"rebuild_starred_13_results_{run_stamp}.tsv"
    with out_tsv.open("w", encoding="utf-8") as f:
        f.write("job_id\tclass_name\told_build_name\tstatus\tresult_path\tvariant\taudit_status\tmiss_wdc_in_g1\tmiss_wd_in_g2\textra_g2_not_in_links\n")
        for r in results:
            if not r.get("audit"):
                f.write(f"{r['job_id']}\t{r['class_name']}\t{r['old_build_name']}\t{r['status']}\t{r.get('result_path','')}\t-\t-\t-\t-\t-\n")
                continue
            for variant, ar in r["audit"].items():
                f.write(
                    f"{r['job_id']}\t{r['class_name']}\t{r['old_build_name']}\t{r['status']}\t{r.get('result_path','')}\t{variant}\t{ar.get('status','-')}\t{ar.get('miss_wdc_in_g1','-')}\t{ar.get('miss_wd_in_g2','-')}\t{ar.get('extra_g2_not_in_links','-')}\n"
                )
    log(f"summary tsv written: {out_tsv}")


if __name__ == "__main__":
    main()

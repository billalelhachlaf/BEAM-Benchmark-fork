import sqlite3
import time

from beam import db


def test_jobs_can_be_filtered_by_owner_key():
    owned_a = db.insert_job({"class_name": "A"}, owner_key="client-a")
    owned_b = db.insert_job({"class_name": "B"}, owner_key="client-b")
    public = db.insert_job({"class_name": "Public"})

    assert [row["id"] for row in db.list_jobs(owner_key="client-a")] == [owned_a]
    assert [row["id"] for row in db.list_jobs(owner_key="client-b")] == [owned_b]
    assert db.get_job(owned_a, owner_key="client-a")["id"] == owned_a
    assert db.get_job(owned_a, owner_key="client-b") is None

    all_ids = {row["id"] for row in db.list_jobs(limit=10)}
    assert {owned_a, owned_b, public}.issubset(all_ids)


def test_job_status_filter_keeps_owner_isolation():
    queued_a = db.insert_job({"class_name": "A"}, owner_key="client-a")
    queued_b = db.insert_job({"class_name": "B"}, owner_key="client-b")
    done_a = db.insert_job({"class_name": "Done"}, owner_key="client-a")
    db.update_job(done_a, status="done")

    assert [row["id"] for row in db.list_jobs_by_status("queued", owner_key="client-a")] == [queued_a]
    assert [row["id"] for row in db.list_jobs_by_status("queued", owner_key="client-b")] == [queued_b]


def test_claim_unowned_jobs_assigns_legacy_rows_once():
    legacy = db.insert_job({"class_name": "Legacy"})
    owned = db.insert_job({"class_name": "Owned"}, owner_key="client-b")

    assert db.claim_unowned_jobs("client-a") == 1

    assert db.get_job(legacy, owner_key="client-a") is not None
    assert db.get_job(owned, owner_key="client-a") is None
    assert db.get_job(owned, owner_key="client-b") is not None
    assert db.claim_unowned_jobs("client-c") == 0


def test_delete_jobs_by_result_path_can_be_limited_to_owner():
    build_path = "data/TestClass/beam_shared"
    owned_a = db.insert_job({"class_name": "A"}, owner_key="client-a")
    owned_b = db.insert_job({"class_name": "B"}, owner_key="client-b")
    db.update_job(owned_a, result_path=build_path)
    db.update_job(owned_b, result_path=build_path)

    db.delete_jobs_by_result_path(build_path, owner_key="client-a")

    assert db.get_job(owned_a) is None
    assert db.get_job(owned_b) is not None


def test_init_db_migrates_owner_key_column(tmp_path, monkeypatch):
    db_path = tmp_path / "legacy_jobs.db"
    monkeypatch.setattr(db, "DB_PATH", db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                status TEXT NOT NULL,
                params_json TEXT NOT NULL,
                created_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            "INSERT INTO jobs (status, params_json, created_at) VALUES (?, ?, ?)",
            ("queued", "{}", time.time()),
        )

    db.init_db()

    with sqlite3.connect(db_path) as conn:
        cols = [row[1] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()]
    assert "owner_key" in cols

"""Original-file archive: dedup, integrity, cleanup, and interruption tests."""

import hashlib
import os
import sqlite3
import threading
import time
from pathlib import Path

import pytest

from app import db, service
from app.originals import OriginalStore
from test_api import _seed_batch, csv_bytes, drain, geojson, make_client


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def upload_map(client):
    return client.post(
        "/maps",
        files={"file": ("m.geojson", geojson({"A": (0, 0, 1, 1)}), "application/geo+json")},
        follow_redirects=False,
    )


def upload_csv(client, payload: bytes, name: str = "h.csv"):
    return client.post(
        "/batches",
        files={"file": (name, payload, "text/csv")},
        follow_redirects=False,
    )


def final_files(originals_dir: Path) -> list[Path]:
    if not originals_dir.exists():
        return []
    return [p for p in originals_dir.iterdir() if p.is_file()]


# ----------------------------------------------------------------------
# Archiving on upload
# ----------------------------------------------------------------------


def test_upload_archives_original_and_download_roundtrip(tmp_path):
    client, app = make_client(tmp_path)
    payload = csv_bytes(
        [("H1", 0.5, 0.5, "2026-01-01T00:00:00Z"), ("H2", 9, 9, "2026-01-01T00:01:00Z")]
    )
    digest = sha256(payload)
    with client:
        upload_map(client)
        assert upload_csv(client, payload, name="热点-7.csv").status_code == 303
        drain(app)

        final = tmp_path / "originals" / digest
        assert final.exists(), "original bytes must be on the shared volume"
        assert final.read_bytes() == payload
        # Immutable: no write bits for anyone.
        assert final.stat().st_mode & 0o222 == 0

        with db.connection(app.state.settings["db_path"]) as c:
            row = c.execute("SELECT * FROM originals WHERE sha256 = ?", (digest,)).fetchone()
            assert row is not None and row["byte_length"] == len(payload)
            batch = service.get_batch(c, 1)
            assert batch["original_sha256"] == digest

        response = client.get("/batches/1/original.csv")
        assert response.status_code == 200
        assert response.content == payload
        assert "attachment" in response.headers["content-disposition"]
        assert response.headers["x-content-sha256"] == digest

        # Batch page advertises the archived original.
        page = client.get("/batches/1")
        assert "已归档" in page.text and digest[:16] in page.text

        # JSON status exposes the registration too.
        info = client.get("/api/batches/1").json()["original"]
        assert info == {"sha256": digest, "byte_length": len(payload)}


def test_same_content_two_instances_keep_single_immutable_original(tmp_path):
    client_a, app_a = make_client(tmp_path)
    client_b, app_b = make_client(tmp_path)
    payload = csv_bytes([(f"H{i}", 0.5, 0.5, "2026-01-01T00:00:00Z") for i in range(10)])
    digest = sha256(payload)
    with client_a, client_b:
        upload_map(client_a)
        assert upload_csv(client_a, payload, name="a.csv").status_code == 303
        assert upload_csv(client_b, payload, name="b.csv").status_code == 303

        # One physical file, one registry row, two batches referencing it.
        assert [p.name for p in final_files(tmp_path / "originals")] == [digest]
        with db.connection(app_a.state.settings["db_path"]) as c:
            assert c.execute("SELECT COUNT(*) AS n FROM originals").fetchone()["n"] == 1
            digests = {
                row["original_sha256"]
                for row in c.execute("SELECT original_sha256 FROM batches")
            }
            assert digests == {digest}

        assert client_a.get("/batches/1/original.csv").content == payload
        assert client_b.get("/batches/2/original.csv").content == payload


def test_invalid_csv_archives_nothing(tmp_path):
    client, app = make_client(tmp_path)
    with client:
        upload_map(client)
        response = upload_csv(client, b"hotspot_no,lon,lat\nH1,0,0\n")  # missing column
        assert "err=" in response.headers["location"]
        assert final_files(tmp_path / "originals") == []
        with db.connection(app.state.settings["db_path"]) as c:
            assert c.execute("SELECT COUNT(*) AS n FROM originals").fetchone()["n"] == 0


# ----------------------------------------------------------------------
# Download integrity
# ----------------------------------------------------------------------


def _archived_client(tmp_path, payload: bytes):
    client, app = make_client(tmp_path)
    with client:
        upload_map(client)
        upload_csv(client, payload)
        drain(app)
    return client, app


def test_corrupted_original_is_refused(tmp_path):
    payload = csv_bytes([("H1", 0.5, 0.5, "2026-01-01T00:00:00Z")])
    client, app = _archived_client(tmp_path, payload)
    final = tmp_path / "originals" / sha256(payload)
    with client:
        # Content corruption (same length).
        final.chmod(0o644)
        final.write_bytes(b"X" * len(payload))
        response = client.get("/batches/1/original.csv")
        assert response.status_code == 409
        assert "损坏" in response.text
        # Attribution detail is unaffected.
        assert client.get("/batches/1/download.csv").status_code == 200
        assert client.get("/batches/1").status_code == 200

        # Truncation (length mismatch) is refused too.
        final.write_bytes(payload[:5])
        assert client.get("/batches/1/original.csv").status_code == 409

        # Disappearance is refused as well.
        final.unlink()
        assert client.get("/batches/1/original.csv").status_code == 409


def test_legacy_batch_without_original_shows_unavailable(tmp_path):
    """Batches from before the archive existed: 不可取, details keep working."""

    db_path = str(tmp_path / "forest.db")
    _seed_batch(db_path)  # inserts batch #1 without original_sha256
    client, app = make_client(tmp_path)
    with client:
        with db.connection(db_path) as c:
            with db.write_tx(c):
                service._insert_run(c, 1, 1)
        drain(app)

        page = client.get("/batches/1")
        assert page.status_code == 200
        assert "原件不可取" in page.text
        response = client.get("/batches/1/original.csv")
        assert response.status_code == 409
        assert "不可取" in response.text
        # Existing attribution detail is served as before.
        assert client.get("/batches/1/download.csv").status_code == 200
        assert client.get("/api/batches/1").json()["original"] is None


# ----------------------------------------------------------------------
# Interruption and retry
# ----------------------------------------------------------------------


def test_retry_after_crash_between_deposit_and_registration(tmp_path):
    """Bytes on disk, no registry row (process died): retry must converge."""

    payload = csv_bytes([("H1", 0.5, 0.5, "2026-01-01T00:00:00Z")])
    digest = sha256(payload)
    store = OriginalStore(tmp_path / "originals")
    store.deposit(payload, digest)  # completed disk write, then "crash"

    client, app = make_client(tmp_path)
    with client:
        upload_map(client)
        assert upload_csv(client, payload).status_code == 303
        assert [p.name for p in final_files(tmp_path / "originals")] == [digest]
        with db.connection(app.state.settings["db_path"]) as c:
            assert c.execute("SELECT COUNT(*) AS n FROM originals").fetchone()["n"] == 1
        assert client.get("/batches/1/original.csv").content == payload


def test_retry_after_registration_failure(tmp_path, monkeypatch):
    """Registration tx interrupted: no batch is accepted; retry succeeds."""

    db_path = str(tmp_path / "forest.db")
    db.init_db(db_path)
    originals_dir = str(tmp_path / "originals")
    payload = csv_bytes([("H1", 0.5, 0.5, "2026-01-01T00:00:00Z")])
    digest = sha256(payload)

    original_ensure = OriginalStore.ensure
    calls = {"n": 0}

    def flaky_ensure(self, data, dg):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("模拟登记中断")
        return original_ensure(self, data, dg)

    monkeypatch.setattr(OriginalStore, "ensure", flaky_ensure)
    with pytest.raises(RuntimeError, match="模拟登记中断"):
        service.create_batch(db_path, payload, "h.csv", originals_dir=originals_dir)
    with db.connection(db_path) as c:
        assert c.execute("SELECT COUNT(*) AS n FROM batches").fetchone()["n"] == 0
        assert c.execute("SELECT COUNT(*) AS n FROM originals").fetchone()["n"] == 0

    batch_id, _ = service.create_batch(db_path, payload, "h.csv", originals_dir=originals_dir)
    assert batch_id == 1
    assert (tmp_path / "originals" / digest).read_bytes() == payload
    with db.connection(db_path) as c:
        assert service.get_batch(c, batch_id)["original_sha256"] == digest


def test_interrupted_disk_write_leaves_no_trace_after_retry(tmp_path):
    """A half-written temp file from a crashed attempt does not break a retry."""

    payload = csv_bytes([("H1", 0.5, 0.5, "2026-01-01T00:00:00Z")])
    store = OriginalStore(tmp_path / "originals")
    stale_tmp = store.tmp_dir / "deadbeef.part"
    stale_tmp.write_bytes(payload[:5])  # crashed mid-write

    client, app = make_client(tmp_path)
    with client:
        upload_map(client)
        assert upload_csv(client, payload).status_code == 303
        assert (tmp_path / "originals" / sha256(payload)).read_bytes() == payload
        # The stale temp file is still there until cleanup sweeps it.
        assert stale_tmp.exists()
        removed_tmp, removed_orphans = service.cleanup_originals(
            app.state.settings["db_path"],
            app.state.settings["originals_dir"],
            stale_seconds=0,
        )
        assert removed_tmp == 1 and removed_orphans == 0
        assert not stale_tmp.exists()
        assert client.get("/batches/1/original.csv").status_code == 200


def test_ensure_recovers_file_lost_between_deposit_and_registration(tmp_path, monkeypatch):
    """If the deposit is lost (e.g. cleanup won the race), the in-transaction
    safety net rewrites the file before the batch commits."""

    db_path = str(tmp_path / "forest.db")
    db.init_db(db_path)
    originals_dir = str(tmp_path / "originals")
    payload = csv_bytes([("H1", 0.5, 0.5, "2026-01-01T00:00:00Z")])

    monkeypatch.setattr(OriginalStore, "deposit", lambda self, data, digest: None)
    batch_id, _ = service.create_batch(db_path, payload, "h.csv", originals_dir=originals_dir)
    final = tmp_path / "originals" / sha256(payload)
    assert final.read_bytes() == payload
    with db.connection(db_path) as c:
        assert service.get_batch(c, batch_id)["original_sha256"] == sha256(payload)


# ----------------------------------------------------------------------
# Restart cleanup
# ----------------------------------------------------------------------


def test_restart_cleanup_removes_only_ownerless_remnants(tmp_path):
    db_path = str(tmp_path / "forest.db")
    originals_dir = tmp_path / "originals"
    payload = csv_bytes([("H1", 0.5, 0.5, "2026-01-01T00:00:00Z")])
    old = time.time() - 7200

    client, app = make_client(tmp_path)
    with client:
        upload_map(client)
        upload_csv(client, payload)
    store = OriginalStore(originals_dir)

    registered = originals_dir / sha256(payload)
    os.utime(registered, (old, old))  # registered + old: must survive

    orphan_old = store.deposit(b"orphan-old", sha256(b"orphan-old"))
    os.utime(orphan_old, (old, old))  # unregistered + old: swept

    orphan_fresh = store.deposit(b"orphan-fresh", sha256(b"orphan-fresh"))
    # unregistered but fresh: another instance may be mid-registration; keep

    tmp_old = store.tmp_dir / "old.part"
    tmp_old.write_bytes(b"x")
    os.utime(tmp_old, (old, old))  # stale temp: swept

    tmp_fresh = store.tmp_dir / "fresh.part"
    tmp_fresh.write_bytes(b"x")  # upload in flight elsewhere: keep

    removed_tmp, removed_orphans = service.cleanup_originals(
        db_path, str(originals_dir), stale_seconds=3600
    )
    assert (removed_tmp, removed_orphans) == (1, 1)
    assert registered.exists()
    assert not orphan_old.exists()
    assert orphan_fresh.exists()
    assert not tmp_old.exists()
    assert tmp_fresh.exists()


def test_concurrent_uploads_of_same_content_collapse_to_one_file(tmp_path):
    """Instances racing the same content: one file, one row, every batch OK."""

    db_path = str(tmp_path / "forest.db")
    db.init_db(db_path)
    originals_dir = str(tmp_path / "originals")
    payload = csv_bytes([(f"H{i}", 0.5, 0.5, "2026-01-01T00:00:00Z") for i in range(20)])
    errors: list[BaseException] = []

    def upload():
        try:
            service.create_batch(db_path, payload, "h.csv", originals_dir=originals_dir)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=upload) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors

    assert [p.name for p in final_files(tmp_path / "originals")] == [sha256(payload)]
    with db.connection(db_path) as c:
        assert c.execute("SELECT COUNT(*) AS n FROM originals").fetchone()["n"] == 1
        assert c.execute("SELECT COUNT(*) AS n FROM batches").fetchone()["n"] == 4


# ----------------------------------------------------------------------
# Schema migration
# ----------------------------------------------------------------------


def test_existing_database_is_migrated(tmp_path):
    db_path = str(tmp_path / "forest.db")
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL, filename TEXT NOT NULL, sha256 TEXT NOT NULL,
            row_count INTEGER NOT NULL, created_at TEXT NOT NULL, published_run_id INTEGER
        );
        INSERT INTO batches (name, filename, sha256, row_count, created_at)
        VALUES ('old', 'old.csv', 'abc', 1, 'then');
        """
    )
    conn.close()

    db.init_db(db_path)
    with db.connection(db_path) as c:
        columns = {row["name"] for row in c.execute("PRAGMA table_info(batches)")}
        assert "original_sha256" in columns
        row = c.execute("SELECT original_sha256 FROM batches WHERE id = 1").fetchone()
        assert row["original_sha256"] is None
        assert c.execute("SELECT COUNT(*) AS n FROM originals").fetchone()["n"] == 0

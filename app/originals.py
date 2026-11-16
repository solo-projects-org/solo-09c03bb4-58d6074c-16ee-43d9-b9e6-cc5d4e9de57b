"""Immutable content-addressed archive for original upload bytes.

Layout on the shared volume (next to the SQLite file by default)::

    <originals_dir>/<sha256>          final, immutable, one per distinct content
    <originals_dir>/tmp/<uuid>.part   in-flight upload, unique per attempt

Guarantees
----------
* **One copy per content.**  The final file name is the SHA-256 of the bytes,
  so two instances uploading identical content converge on a single file.
* **Never partially written.**  Bytes go to a unique temp file, are fsynced,
  and appear at the final name through an atomic ``os.replace``.  Readers and
  racing instances only ever see a complete file.
* **Immutable.**  Final files are chmodded read-only and never written in
  place; a rewrite (only ever of identical content, or to heal corruption)
  replaces the file atomically.
* **Registered before use.**  ``service.create_batch`` deposits the bytes
  first, then registers digest + length and creates the batch inside one
  ``BEGIN IMMEDIATE`` transaction, so a batch never exists without its
  original on disk and registered.
* **Safe cleanup.**  Startup/periodic cleanup removes stale temp files and
  unregistered orphan finals, but only past an age threshold — a file another
  instance is still uploading (fresh temp) or has just renamed and not yet
  registered (fresh final) is never touched, and registered originals are
  never touched at all.  The orphan check runs under the SQLite write lock so
  it cannot interleave with a registration commit.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import time
import uuid
from pathlib import Path

log = logging.getLogger("forest.originals")

_DIGEST_RE = re.compile(r"[0-9a-f]{64}\Z")
_CHUNK = 1024 * 1024


class OriginalStore:
    """Content-addressed store for original upload files on a shared volume."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.tmp_dir = self.root / "tmp"
        self.tmp_dir.mkdir(parents=True, exist_ok=True)

    @classmethod
    def for_db(cls, db_path: str) -> "OriginalStore":
        """Default location: ``originals/`` next to the SQLite database file."""

        return cls(Path(db_path).resolve().parent / "originals")

    def final_path(self, digest: str) -> Path:
        if not _DIGEST_RE.match(digest):
            raise ValueError(f"invalid digest {digest!r}")
        return self.root / digest

    # ------------------------------------------------------------------
    # Writing
    # ------------------------------------------------------------------

    def deposit(self, data: bytes, digest: str) -> Path:
        """Idempotently place ``data`` at its content-addressed final path."""

        final = self.final_path(digest)
        if final.exists():
            return final
        self._write_final(data, final)
        return final

    def ensure(self, data: bytes, digest: str) -> None:
        """Re-assert the file is on disk; called under the DB write lock.

        Cleanup may remove an unregistered file between :meth:`deposit` and
        the registration transaction.  Re-checking here (still before the
        batch row commits) keeps the invariant "registered => bytes on disk".
        A size match is enough: the file only ever appears via atomic rename
        of fully-written bytes, and the digest itself is verified on download.
        """

        final = self.final_path(digest)
        try:
            if final.stat().st_size == len(data):
                return
        except OSError:
            pass
        self._write_final(data, final)

    def _write_final(self, data: bytes, final: Path) -> None:
        tmp = self.tmp_dir / f"{uuid.uuid4().hex}.part"
        try:
            with open(tmp, "wb") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            # Atomic within the shared volume; a racing instance writes
            # identical bytes (same digest), so last-writer-wins is safe.
            os.replace(tmp, final)
            try:
                os.chmod(final, 0o444)  # originals are immutable
            except OSError:
                pass
            self._fsync_dir()
        finally:
            try:
                tmp.unlink()
            except OSError:
                pass

    def _fsync_dir(self) -> None:
        try:
            fd = os.open(self.root, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except OSError:
            pass

    # ------------------------------------------------------------------
    # Verification
    # ------------------------------------------------------------------

    def verify(self, digest: str, byte_length: int) -> tuple[bool, int, str | None]:
        """Re-hash the stored file.

        Returns ``(ok, actual_length, actual_sha256)``; ``ok`` requires both
        the registered length and the content-addressed digest to match.
        """

        final = self.final_path(digest)
        hasher = hashlib.sha256()
        total = 0
        try:
            with open(final, "rb") as fh:
                while True:
                    chunk = fh.read(_CHUNK)
                    if not chunk:
                        break
                    hasher.update(chunk)
                    total += len(chunk)
        except OSError:
            return False, 0, None
        actual = hasher.hexdigest()
        return (total == byte_length and actual == digest), total, actual

    # ------------------------------------------------------------------
    # Cleanup of ownerless remnants
    # ------------------------------------------------------------------

    def clean_tmp(self, stale_seconds: float, now: float | None = None) -> int:
        """Remove abandoned temp files older than ``stale_seconds``.

        A temp file being actively written has a fresh mtime, so uploads in
        progress on any instance are never touched.
        """

        now = time.time() if now is None else now
        removed = 0
        try:
            candidates = list(self.tmp_dir.glob("*.part"))
        except OSError:
            return 0
        for path in candidates:
            try:
                if now - path.stat().st_mtime < stale_seconds:
                    continue  # fresh: another instance may still be uploading
                path.unlink()
                removed += 1
            except OSError:
                continue
        return removed

    def clean_unregistered(
        self, registered: set[str], stale_seconds: float, now: float | None = None
    ) -> int:
        """Remove final files not present in the ``originals`` table.

        Must be called under the SQLite write lock so a concurrent
        registration cannot commit between the check and the delete.  Files
        younger than ``stale_seconds`` are skipped regardless: a fresh
        unregistered final is an upload whose registration transaction is
        still in flight on some instance.
        """

        now = time.time() if now is None else now
        removed = 0
        try:
            candidates = list(self.root.iterdir())
        except OSError:
            return 0
        for path in candidates:
            try:
                if not path.is_file() or not _DIGEST_RE.match(path.name):
                    continue
                if path.name in registered:
                    continue  # referenced by batches: never delete
                if now - path.stat().st_mtime < stale_seconds:
                    continue  # fresh: registration may be in flight elsewhere
                path.unlink()
                removed += 1
            except OSError:
                continue
        return removed

"""FastAPI application: uploads, run status, conflict review, CSV download."""

from __future__ import annotations

import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from urllib.parse import quote

from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import __version__, db, service
from .originals import OriginalStore
from .parsing import InputError
from .worker import Worker

BASE_DIR = Path(__file__).resolve().parent
log = logging.getLogger("forest.web")


def _settings() -> dict[str, Any]:
    db_path = os.environ.get("FOREST_DB_PATH", str(BASE_DIR.parent / "data" / "forest.db"))
    return {
        "db_path": db_path,
        "originals_dir": os.environ.get("FOREST_ORIGINALS_DIR")
        or str(Path(db_path).resolve().parent / "originals"),
        "original_stale_seconds": int(os.environ.get("FOREST_ORIGINAL_STALE_SECONDS", "3600")),
        "upload_max_mb": int(os.environ.get("FOREST_UPLOAD_MAX_MB", "32")),
        "run_worker": os.environ.get("FOREST_RUN_WORKER", "1") not in {"0", "false", "no"},
        "stale_seconds": int(os.environ.get("FOREST_STALE_RUN_SECONDS", "120")),
        "per_page": int(os.environ.get("FOREST_PREVIEW_ROWS", "200")),
    }


def create_app(db_path: str | None = None, start_worker: bool | None = None) -> FastAPI:
    settings = _settings()
    settings["db_path"] = db_path or settings["db_path"]
    if not os.environ.get("FOREST_ORIGINALS_DIR"):
        # Keep the archive next to the (possibly overridden) database file.
        settings["originals_dir"] = str(
            Path(settings["db_path"]).resolve().parent / "originals"
        )
    if start_worker is not None:
        settings["run_worker"] = start_worker

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        db.init_db(app.state.settings["db_path"])
        try:
            removed_tmp, removed_orphans = service.cleanup_originals(
                app.state.settings["db_path"],
                app.state.settings["originals_dir"],
                app.state.settings["original_stale_seconds"],
            )
            if removed_tmp or removed_orphans:
                log.info(
                    "startup cleanup removed %s temp, %s orphan original(s)",
                    removed_tmp,
                    removed_orphans,
                )
        except Exception:  # noqa: BLE001 - cleanup failure must not block boot
            log.exception("startup original cleanup failed")
        worker = None
        if app.state.settings["run_worker"]:
            worker = Worker(
                app.state.settings["db_path"],
                stale_seconds=app.state.settings["stale_seconds"],
                originals_dir=app.state.settings["originals_dir"],
                original_stale_seconds=app.state.settings["original_stale_seconds"],
            )
            worker.start()
        app.state.worker = worker
        try:
            yield
        finally:
            if worker is not None:
                worker.stop()

    app = FastAPI(title="林火卫星热点林班归属", version=__version__, lifespan=lifespan)
    app.state.settings = settings
    app.state.worker = None
    app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
    templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
    templates.env.filters["status_label"] = lambda value: service.STATUS_LABELS.get(value, value)
    templates.env.filters["reason_label"] = lambda value: service.REASON_LABELS.get(value, value)
    templates.env.filters["run_status_label"] = lambda value: service.RUN_STATUS_LABELS.get(value, value)

    def conn() -> sqlite3.Connection:
        return db.connect(app.state.settings["db_path"])

    def kick_worker() -> None:
        worker = app.state.worker
        if worker is not None:
            worker.kick()

    def render(request: Request, template: str, context: dict[str, Any], status_code: int = 200):
        return templates.TemplateResponse(
            request=request, name=template, context=context, status_code=status_code
        )

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request, msg: str | None = None, err: str | None = None):
        with conn() as c:
            active_map = service.get_active_map(c)
            batches = service.list_batches(c)
            maps = service.list_maps(c, limit=10)
        return render(
            request,
            "index.html",
            {
                "active_map": active_map,
                "maps": maps,
                "batches": batches,
                "msg": msg,
                "err": err,
                "max_mb": app.state.settings["upload_max_mb"],
            },
        )

    @app.post("/maps")
    def upload_map(request: Request, file: UploadFile = File(...)):
        try:
            data = _read_upload(file, app.state.settings["upload_max_mb"], "林班 GeoJSON")
            info, stale_batches = service.publish_map(
                app.state.settings["db_path"], data, _safe_filename(file.filename, "compartments.geojson")
            )
        except InputError as exc:
            return _redirect("/", err=str(exc))
        note = f"林班底图已发布为 v{info.id}（{info.feature_count} 个林班）"
        if info.repaired_count:
            note += f"，其中 {info.repaired_count} 个无效几何已自动修复"
        if stale_batches:
            note += f"。有 {len(stale_batches)} 个批次的已发布结果基于旧底图，请重算"
        return _redirect("/", msg=note)

    @app.post("/recompute-all")
    def recompute_all(request: Request):
        with conn() as c:
            active = service.get_active_map(c)
            if active is None:
                return _redirect("/", err="尚未上传林班底图")
            batches = [row["id"] for row in c.execute("SELECT id FROM batches ORDER BY id").fetchall()]
        queued = 0
        errors = []
        for batch_id in batches:
            try:
                service.enqueue_recompute(app.state.settings["db_path"], int(batch_id))
                queued += 1
            except service.ServiceError as exc:
                errors.append(str(exc))
        kick_worker()
        note = f"已为 {queued} 个批次按 v{active.id} 重新排队计算"
        if errors:
            return _redirect("/", msg=note, err="；".join(errors))
        return _redirect("/", msg=note)

    @app.post("/batches")
    def upload_batch(request: Request, file: UploadFile = File(...)):
        try:
            data = _read_upload(file, app.state.settings["upload_max_mb"], "热点 CSV")
            batch_id, run_id = service.create_batch(
                app.state.settings["db_path"],
                data,
                _safe_filename(file.filename, "hotspots.csv"),
                originals_dir=app.state.settings["originals_dir"],
            )
        except InputError as exc:
            return _redirect("/", err=str(exc))
        if run_id is None:
            return _redirect("/", msg=f"热点批次 #{batch_id} 已保存；上传林班底图后即可计算")
        kick_worker()
        return _redirect(f"/batches/{batch_id}", msg=f"热点批次 #{batch_id} 已上传，计算已排队")

    @app.post("/batches/{batch_id}/recompute")
    def recompute(request: Request, batch_id: int):
        try:
            run_id = service.enqueue_recompute(app.state.settings["db_path"], batch_id)
        except service.ServiceError as exc:
            return _redirect(f"/batches/{batch_id}", err=str(exc))
        kick_worker()
        return _redirect(
            f"/batches/{batch_id}", msg=f"已按当前底图排队重算（计算记录 #{run_id}）"
        )

    @app.get("/batches/{batch_id}", response_class=HTMLResponse)
    def batch_detail(
        request: Request,
        batch_id: int,
        msg: str | None = None,
        err: str | None = None,
        only: str | None = None,
    ):
        with conn() as c:
            batch = service.get_batch(c, batch_id)
            if batch is None:
                raise HTTPException(status_code=404, detail="热点批次不存在")
            active_map = service.get_active_map(c)
            published = service.get_published_run(c, batch)
            original = service.get_original_info(c, batch)
            runs = service.list_runs(c, batch_id)
            counts = service.result_counts(c, published["id"]) if published else []
            preview = (
                service.preview_results(
                    c,
                    published["id"],
                    limit=app.state.settings["per_page"],
                    only_problems=only == "problems",
                )
                if published
                else []
            )
            pending = service.is_run_active(c, batch_id)
        return render(
            request,
            "batch.html",
            {
                "batch": batch,
                "active_map": active_map,
                "published": published,
                "original": original,
                "runs": runs,
                "counts": counts,
                "preview": preview,
                "only_problems": only == "problems",
                "pending": pending,
                "msg": msg,
                "err": err,
                "per_page": app.state.settings["per_page"],
            },
        )

    @app.get("/batches/{batch_id}/download.csv")
    def download(batch_id: int):
        with conn() as c:
            batch = service.get_batch(c, batch_id)
            if batch is None:
                raise HTTPException(status_code=404, detail="热点批次不存在")
            published = service.get_published_run(c, batch)
        if published is None:
            raise HTTPException(status_code=409, detail="该批次还没有可下载的计算结果")
        name = f"hotspot_attribution_batch{batch_id}_run{published['id']}.csv"
        headers = _download_headers(name)
        return StreamingResponse(
            service.iter_result_csv(app.state.settings["db_path"], batch_id),
            media_type="text/csv; charset=utf-8",
            headers=headers,
        )

    @app.get("/batches/{batch_id}/original.csv")
    def download_original(batch_id: int):
        """Serve the archived original CSV, but only after an integrity check.

        Length and SHA-256 are re-verified against the registry on every
        request; a missing or corrupted original is refused rather than
        silently served.
        """

        with conn() as c:
            batch = service.get_batch(c, batch_id)
            if batch is None:
                raise HTTPException(status_code=404, detail="热点批次不存在")
            info = service.get_original_info(c, batch)
        if info is None:
            raise HTTPException(
                status_code=409,
                detail="该批次上传于原件归档启用之前，源文件未保留，原件不可取",
            )
        store = OriginalStore(app.state.settings["originals_dir"])
        ok, actual_length, actual_sha = store.verify(info["sha256"], info["byte_length"])
        if not ok:
            log.error(
                "original %s for batch %s failed integrity check (length=%s sha256=%s)",
                info["sha256"],
                batch_id,
                actual_length,
                actual_sha,
            )
            raise HTTPException(
                status_code=409,
                detail="归档原件校验失败（长度或 SHA-256 与登记不符），原件已损坏，拒绝提供",
            )
        name = batch["filename"] or f"hotspots_batch{batch_id}.csv"
        headers = {
            **_download_headers(name),
            "X-Content-SHA256": info["sha256"],
        }
        return FileResponse(
            store.final_path(info["sha256"]),
            media_type="text/csv; charset=utf-8",
            headers=headers,
        )

    @app.get("/api/batches/{batch_id}")
    def batch_status(batch_id: int):
        """Small JSON view, useful for polling and for smoke tests."""

        with conn() as c:
            batch = service.get_batch(c, batch_id)
            if batch is None:
                raise HTTPException(status_code=404, detail="热点批次不存在")
            published = service.get_published_run(c, batch)
            original = service.get_original_info(c, batch)
            runs = service.list_runs(c, batch_id, limit=10)
        return {
            "batch_id": batch_id,
            "published_run_id": batch["published_run_id"],
            "published_map_version": published["map_version_id"] if published else None,
            "published_total": published["total_count"] if published else None,
            "matched": published["matched_count"] if published else None,
            "conflict": published["conflict_count"] if published else None,
            "unmatched": published["unmatched_count"] if published else None,
            "original": (
                {"sha256": original["sha256"], "byte_length": original["byte_length"]}
                if original
                else None
            ),
            "pending": any(
                run["status"] in (service.STATUS_QUEUED, service.STATUS_RUNNING) for run in runs
            ),
            "runs": [
                {
                    "id": run["id"],
                    "status": run["status"],
                    "map_version_id": run["map_version_id"],
                    "matched": run["matched_count"],
                    "conflict": run["conflict_count"],
                    "unmatched": run["unmatched_count"],
                    "error": run["error"],
                }
                for run in runs
            ],
        }

    @app.get("/healthz")
    def healthz():
        with conn() as c:
            active = service.get_active_map(c)
        return {"status": "ok", "version": __version__, "active_map_version": active.id if active else None}

    return app


def _read_upload(file: UploadFile, max_mb: int, what: str) -> bytes:
    data = file.file.read()
    if not data:
        raise InputError(f"{what} 为空")
    if len(data) > max_mb * 1024 * 1024:
        raise InputError(f"{what} 超过 {max_mb} MB 限制")
    return data


def _safe_filename(name: str | None, fallback: str) -> str:
    text = Path(name or "").name.strip()
    return text or fallback


def _download_headers(filename: str) -> dict[str, str]:
    """Content-Disposition with an ASCII fallback plus RFC 5987 UTF-8 name."""

    ascii_fallback = "".join(
        ch if ord(ch) < 128 and ch not in {'"', "\\"} else "_" for ch in filename
    )
    return {
        "Content-Disposition": (
            f"attachment; filename=\"{ascii_fallback}\"; filename*=UTF-8''{quote(filename)}"
        ),
        "Cache-Control": "no-store",
    }


def _redirect(path: str, msg: str | None = None, err: str | None = None) -> RedirectResponse:
    from urllib.parse import urlencode

    params = {key: value for key, value in (("msg", msg), ("err", err)) if value}
    url = path + (("?" + urlencode(params)) if params else "")
    return RedirectResponse(url, status_code=303)


app = create_app()

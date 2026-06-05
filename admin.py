import logging
from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException, Query
import httpx

logger = logging.getLogger(__name__)

def create_admin_router(app_state) -> APIRouter:
    router = APIRouter()

    @router.get("/jobs")
    async def list_jobs(
        status: str | None = Query(None),
        source_type: str | None = Query(None),
        page: int = Query(1, ge=1),
        size: int = Query(20, ge=1, le=100),
    ):
        jobs, total = await app_state.store.list_jobs(page=page, size=size, status=status, source_type=source_type)
        return {"jobs": [j.model_dump() for j in jobs], "total": total, "page": page, "size": size}

    @router.get("/jobs/{job_id}")
    async def get_job(job_id: str):
        job = await app_state.store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        files = await app_state.store.list_files_for_job(job_id)
        return {**job.model_dump(), "files": [f.model_dump() for f in files]}

    @router.get("/jobs/{job_id}/files")
    async def get_job_files(job_id: str):
        job = await app_state.store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        files = await app_state.store.list_files_for_job(job_id)
        return {"files": [f.model_dump() for f in files]}

    @router.get("/files/{file_id}")
    async def get_file(file_id: str):
        f = await app_state.store.get_file(file_id)
        if f is None:
            raise HTTPException(status_code=404, detail="File not found")
        return f.model_dump()

    @router.post("/files/{file_id}/reject")
    async def reject_file(file_id: str):
        f = await app_state.store.get_file(file_id)
        if f is None:
            raise HTTPException(status_code=404, detail="File not found")
        await app_state.store.update_file(file_id, review_status="rejected")
        return {"rejected": True}

    @router.post("/files/{file_id}/re-ingest")
    async def re_ingest_file(file_id: str):
        f = await app_state.store.get_file(file_id)
        if f is None:
            raise HTTPException(status_code=404, detail="File not found")
        lightrag = getattr(app_state, "lightrag", None)
        forge = getattr(app_state, "forge", None)
        if lightrag is None:
            raise HTTPException(status_code=503, detail="LightRAG client not available")
        if forge is None or not f.external_job_id:
            raise HTTPException(status_code=400, detail="external_job_id not available for re-ingest")
        forge_result = await forge.get_job(f.external_job_id)
        result_text = (forge_result.get("result") or {}).get("text", "")
        await app_state.store.update_file(file_id, rag_status="ingesting")
        try:
            await lightrag.ingest_text(
                content=result_text,
                metadata={"file_id": f.file_id, "job_id": f.job_id, "file_path": f.file_path},
            )
            await app_state.store.update_file(file_id, rag_status="ingested", completed_at=datetime.now(timezone.utc))
        except Exception as e:
            await app_state.store.update_file(file_id, rag_status="failed", error=str(e))
            raise HTTPException(status_code=502, detail=f"LightRAG ingest failed: {e}")
        return {"re_ingested": True}

    @router.post("/jobs/{job_id}/retry")
    async def retry_job(job_id: str):
        job = await app_state.store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        files = await app_state.store.list_files_for_job(job_id)
        failed = [f for f in files if f.external_status == "failed" or f.rag_status == "failed"]
        for f in failed:
            await app_state.store.update_file(f.file_id, external_status="queued", rag_status="pending", error=None)
        return {"retried": len(failed)}

    @router.get("/stats")
    async def stats():
        return await app_state.store.get_stats()

    @router.get("/health/all")
    async def health_all():
        results = {"ingestion_router": "ok"}
        config = getattr(app_state, "config", None)
        if config:
            for name, url in [
                ("forge", config.forge_url),
                ("lightrag", config.lightrag_url),
                ("robotics", config.robotics_url),
                ("nexus", config.nexus_url),
            ]:
                try:
                    async with httpx.AsyncClient(timeout=5.0) as c:
                        r = await c.get(f"{url}/health")
                    results[name] = "ok" if r.status_code == 200 else "error"
                except Exception:
                    results[name] = "unreachable"
        return results

    return router

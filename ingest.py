import logging
from datetime import datetime, timezone
import httpx

logger = logging.getLogger(__name__)

PLSQL_EXTS = {".pkb", ".pks", ".sql", ".prc", ".fnc"}
DOCUMENT_EXTS = {".pdf", ".docx", ".pptx", ".xlsx", ".hwpx"}  # Forge 변환 필요 포맷
TEXT_EXTS = {".md", ".txt"}                                     # 이미 텍스트 → LightRAG 직접
CODE_EXTS = {".java", ".js", ".ts", ".jsx", ".tsx", ".py"}

def classify_file(file_path: str) -> str:
    if "." not in file_path:
        return "skip"
    ext = "." + file_path.rsplit(".", 1)[-1].lower()
    if ext in PLSQL_EXTS:
        return "plsql"
    if ext in DOCUMENT_EXTS:
        return "document"
    if ext in TEXT_EXTS:
        return "text_doc"
    if ext in CODE_EXTS:
        return "code"
    return "skip"

def is_body_file(file_bytes: bytes) -> bool:
    header = file_bytes[:500].decode("utf-8", errors="replace").lower()
    return "package body" in header


class ForgeClient:
    def __init__(self, base_url: str, api_key: str):
        self._client = httpx.AsyncClient(
            base_url=base_url,
            headers={"X-Forge-Key": api_key},
            timeout=30.0,
        )

    async def convert(self, file_bytes: bytes, file_name: str, callback_url: str) -> dict:
        resp = await self._client.post(
            "/convert",
            files={"file": (file_name, file_bytes, "application/octet-stream")},
            params={"callback_url": callback_url, "requested_by": "ingestion-router"},
        )
        resp.raise_for_status()
        return resp.json()

    async def get_job(self, job_id: str) -> dict:
        resp = await self._client.get(f"/result/{job_id}")
        resp.raise_for_status()
        return resp.json()

    async def close(self):
        await self._client.aclose()


class RoboticsClient:
    def __init__(self, base_url: str, api_key: str):
        headers = {}
        if api_key:
            headers["X-API-Key"] = api_key
        self._client = httpx.AsyncClient(
            base_url=base_url,
            headers=headers,
            timeout=30.0,
        )

    async def submit(self, file_bytes: bytes, file_name: str, callback_url: str) -> dict:
        resp = await self._client.post(
            "/jobs",
            files={"file": (file_name, file_bytes, "text/plain")},
            data={
                "asset_type": "plsql",
                "callback_url": callback_url,
                "requested_by": "ingestion-router",
            },
        )
        resp.raise_for_status()
        return resp.json()

    async def close(self):
        await self._client.aclose()


class NexusClient:
    def __init__(self, base_url: str, api_key: str):
        headers = {}
        if api_key:
            headers["X-Api-Key"] = api_key
        self._client = httpx.AsyncClient(
            base_url=base_url,
            headers=headers,
            timeout=120.0,
        )

    async def rebuild(self) -> dict:
        resp = await self._client.post("/rebuild/")
        resp.raise_for_status()
        return resp.json()

    async def upload(self, file_bytes: bytes, file_name: str, project_id: str = "default") -> dict:
        resp = await self._client.post(
            "/rebuild/upload",
            files={"file": (file_name, file_bytes, "application/octet-stream")},
            params={"project_id": project_id},
        )
        resp.raise_for_status()
        return resp.json()

    async def close(self):
        await self._client.aclose()


class LightRAGClient:
    def __init__(self, base_url: str, api_key: str):
        headers = {}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        self._client = httpx.AsyncClient(
            base_url=base_url,
            headers=headers,
            timeout=60.0,
        )

    async def ingest_text(self, content: str, metadata: dict) -> dict:
        safe_content = content.encode("utf-8", errors="replace").decode("utf-8")
        file_source = metadata.get("file_path", "")
        logger.info(
            "[LightRAG] ingest_text → file_path=%s text_len=%d file_id=%s",
            file_source, len(safe_content), metadata.get("file_id", "-"),
        )
        resp = await self._client.post(
            "/documents/text",
            json={"text": safe_content, "file_source": file_source},
        )
        logger.info(
            "[LightRAG] ingest_text ← status=%d body=%s",
            resp.status_code, resp.text[:300],
        )
        resp.raise_for_status()
        return resp.json()

    async def close(self):
        await self._client.aclose()


async def advance_pipeline(external_job_id: str, callback_body: dict, store, lightrag: LightRAGClient):
    files = await store.get_files_by_external_job_id(external_job_id)
    if not files:
        logger.warning("Unknown external_job_id in callback: %s", external_job_id)
        return

    cb_status = callback_body.get("status")
    result_text = (callback_body.get("result") or {}).get("text", "")
    logger.info(
        "[Pipeline] callback external_job_id=%s status=%s text_len=%d",
        external_job_id, cb_status, len(result_text),
    )

    affected_jobs = set()
    for f in files:
        if f.external_status not in ("queued", "processing"):
            continue

        if cb_status != "completed":
            error_msg = callback_body.get("error", "processing failed")
            logger.warning("[Pipeline] upstream failed file=%s error=%s", f.file_path, error_msg)
            await store.update_file(f.file_id, external_status="failed", error=error_msg)
            affected_jobs.add(f.job_id)
            continue

        await store.update_file(f.file_id, external_status="done", rag_status="ingesting")

        if not result_text:
            logger.error("[Pipeline] empty result_text file=%s external_job_id=%s", f.file_path, external_job_id)
            await store.update_file(f.file_id, rag_status="failed", error="empty result from upstream")
            affected_jobs.add(f.job_id)
            continue

        logger.info("[Pipeline] → LightRAG ingest file=%s text_len=%d", f.file_path, len(result_text))
        try:
            await lightrag.ingest_text(
                content=result_text,
                metadata={
                    "file_id": f.file_id,
                    "job_id": f.job_id,
                    "file_path": f.file_path,
                    "external_job_id": external_job_id,
                },
            )
            logger.info("[Pipeline] ✓ LightRAG ingested file=%s", f.file_path)
            await store.update_file(f.file_id, rag_status="ingested", completed_at=datetime.now(timezone.utc))
        except Exception as e:
            logger.error("[Pipeline] ✗ LightRAG ingest failed file=%s: %s", f.file_path, e)
            await store.update_file(f.file_id, rag_status="failed", error=str(e))

        affected_jobs.add(f.job_id)

    for job_id in affected_jobs:
        await _maybe_close_job(job_id, store)


async def dispatch_code_file(file_id: str, job_id: str, file_bytes: bytes, file_name: str, store, nexus: NexusClient, project_id: str = "default"):
    logger.info("[Nexus] upload file=%s size=%d project=%s", file_name, len(file_bytes), project_id)
    try:
        result = await nexus.upload(file_bytes, file_name, project_id)
        logger.info("[Nexus] ✓ upload done file=%s result=%s", file_name, str(result)[:200])
        await store.update_file(file_id, external_status="done", rag_status="skipped",
                                completed_at=datetime.now(timezone.utc))
    except Exception as e:
        logger.error("[Nexus] ✗ upload failed file=%s: %s", file_name, e)
        await store.update_file(file_id, external_status="failed", error=str(e))
    await _maybe_close_job(job_id, store)


async def dispatch_text_doc(file_id: str, job_id: str, file_bytes: bytes, file_name: str, store, lightrag: LightRAGClient):
    content = file_bytes.decode("utf-8", errors="replace")
    logger.info("[TextDoc] → LightRAG file=%s text_len=%d", file_name, len(content))
    try:
        await lightrag.ingest_text(
            content=content,
            metadata={"file_id": file_id, "job_id": job_id, "file_path": file_name},
        )
        logger.info("[TextDoc] ✓ LightRAG ingested file=%s", file_name)
        await store.update_file(file_id, external_status="done", rag_status="ingested",
                                completed_at=datetime.now(timezone.utc))
    except Exception as e:
        logger.error("[TextDoc] ✗ LightRAG ingest failed file=%s: %s", file_name, e)
        await store.update_file(file_id, external_status="failed", rag_status="failed", error=str(e))
    await _maybe_close_job(job_id, store)


async def dispatch_plsql_direct(
    file_id: str,
    job_id: str,
    file_bytes: bytes,
    file_name: str,
    store,
    lightrag: LightRAGClient,
    update_status: bool = True,
):
    content = file_bytes.decode("utf-8", errors="replace")
    logger.info("[PlsqlDirect] → LightRAG file=%s text_len=%d update_status=%s", file_name, len(content), update_status)
    try:
        await lightrag.ingest_text(
            content=content,
            metadata={"file_id": file_id, "job_id": job_id, "file_path": file_name},
        )
        logger.info("[PlsqlDirect] ✓ LightRAG ingested file=%s", file_name)
        if update_status:
            await store.update_file(file_id, external_status="done", rag_status="ingested",
                                    completed_at=datetime.now(timezone.utc))
    except Exception as e:
        logger.error("[PlsqlDirect] ✗ LightRAG ingest failed file=%s: %s", file_name, e)
        if update_status:
            await store.update_file(file_id, external_status="failed", rag_status="failed", error=str(e))
    if update_status:
        await _maybe_close_job(job_id, store)


async def _maybe_close_job(job_id: str, store):
    files = await store.list_files_for_job(job_id)
    if not files:
        return
    external_terminal = {"done", "skipped", "failed"}
    if not all(f.external_status in external_terminal for f in files):
        return
    rag_terminal = {"ingested", "failed", "pending", "skipped"}
    if not all(f.rag_status in rag_terminal for f in files):
        return

    success = sum(1 for f in files if f.rag_status == "ingested" or f.file_type in ("skip", "code"))
    total = len(files)
    if success == total:
        status = "completed"
    elif success > 0:
        status = "partial"
    else:
        status = "failed"

    await store.update_job(job_id, status=status, completed_at=datetime.now(timezone.utc))

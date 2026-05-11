import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, File, HTTPException, Request, UploadFile

from config import Config
from ingest import CitadelClient, ForgeClient, LightRAGClient, NexusClient, classify_file, advance_pipeline, _maybe_close_job, dispatch_code_file
from job_store import InMemoryJobStore
from webhook import verify_hmac, parse_bitbucket_payload
from admin import create_admin_router

logger = logging.getLogger(__name__)


async def _safe_process(coro):
    try:
        await coro
    except Exception:
        logger.exception("Unhandled pipeline error")


def create_app(store=None, config: Config = None) -> FastAPI:
    config = config or Config()
    store = store or InMemoryJobStore()

    @asynccontextmanager
    async def lifespan(a):
        a.state.config = config
        a.state.forge = ForgeClient(base_url=config.forge_url, api_key=config.forge_api_key)
        a.state.citadel = CitadelClient(base_url=config.citadel_url, api_key=config.citadel_api_key)
        a.state.nexus = NexusClient(base_url=config.nexus_url, api_key=config.nexus_api_key)
        a.state.lightrag = LightRAGClient(base_url=config.lightrag_url, api_key=config.lightrag_api_key)

        if config.database_url:
            import asyncpg
            from job_store import PostgresJobStore
            pool = await asyncpg.create_pool(config.database_url)
            a.state.pool = pool
            schema_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema.sql")
            if os.path.isfile(schema_path):
                async with pool.acquire() as conn:
                    await conn.execute(open(schema_path).read())
            a.state.store = PostgresJobStore(pool)
        else:
            a.state.store = store

        yield

        await a.state.forge.close()
        await a.state.citadel.close()
        await a.state.nexus.close()
        await a.state.lightrag.close()
        if hasattr(a.state, "pool"):
            await a.state.pool.close()

    app = FastAPI(title="Pylon", version="0.2.0", lifespan=lifespan)
    app.state.store = store
    app.state.config = config
    app.state.forge = None
    app.state.citadel = None
    app.state.nexus = None
    app.state.lightrag = None

    app.include_router(create_admin_router(app.state))

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.post("/webhook/bitbucket", status_code=202)
    async def webhook_bitbucket(request: Request):
        body = await request.body()
        sig = request.headers.get("X-Hub-Signature", "")
        secret = request.app.state.config.bitbucket_webhook_secret
        if secret and not verify_hmac(body, sig, secret):
            raise HTTPException(status_code=401, detail="Invalid HMAC signature")

        payload = json.loads(body)
        parsed = parse_bitbucket_payload(payload)
        current_store = request.app.state.store

        job = await current_store.create_job(
            source_type="webhook",
            repo=parsed["repo"],
            pr_number=parsed["pr_number"],
            commit_hash=parsed["commit_hash"],
        )
        await current_store.update_job(job.job_id, status="processing", file_count=len(parsed["files"]))

        for file_path in parsed["files"]:
            file_type = classify_file(file_path)
            f = await current_store.create_file(job_id=job.job_id, file_path=file_path, file_type=file_type)
            if file_type in ("skip", "code"):
                rag_st = "skipped" if file_type == "code" else "pending"
                await current_store.update_file(f.file_id, external_status="skipped", rag_status=rag_st)
            else:
                await current_store.update_file(f.file_id, external_status="queued")

        asyncio.create_task(_safe_process(_maybe_close_job(job.job_id, current_store)))

        return {"job_id": job.job_id, "status": "processing", "file_count": len(parsed["files"])}

    @app.post("/callback/forge")
    async def forge_callback(request: Request):
        body = await request.json()
        external_job_id = body.get("forge_job_id") or body.get("job_id", "")
        normalized = {
            "status": body.get("forge_status") or body.get("status"),
            "result": {"text": body.get("content", "")},
            "error": body.get("forge_error") or body.get("error"),
        }
        asyncio.create_task(_safe_process(
            advance_pipeline(
                external_job_id=external_job_id,
                callback_body=normalized,
                store=request.app.state.store,
                lightrag=request.app.state.lightrag,
            )
        ))
        return {"received": True}

    @app.post("/callback/citadel")
    async def citadel_callback(request: Request):
        body = await request.json()
        rdoc_job_id = body.get("rdoc_job_id", "")
        normalized = {
            "status": body.get("status"),
            "result": {"text": body.get("content", "")},
            "error": body.get("error"),
        }
        asyncio.create_task(_safe_process(
            advance_pipeline(
                external_job_id=rdoc_job_id,
                callback_body=normalized,
                store=request.app.state.store,
                lightrag=request.app.state.lightrag,
            )
        ))
        return {"received": True}

    @app.post("/ingest/bulk")
    async def ingest_bulk(request: Request):
        body = await request.json()
        files = body.get("files", [])
        current_store = request.app.state.store
        job = await current_store.create_job(source_type="bulk", repo=body.get("repo", ""))
        await current_store.update_job(job.job_id, status="processing", file_count=len(files))
        for file_path in files:
            file_type = classify_file(file_path)
            f = await current_store.create_file(job_id=job.job_id, file_path=file_path, file_type=file_type)
            if file_type in ("skip", "code"):
                rag_st = "skipped" if file_type == "code" else "pending"
                await current_store.update_file(f.file_id, external_status="skipped", rag_status=rag_st)
            else:
                await current_store.update_file(f.file_id, external_status="queued")
        asyncio.create_task(_safe_process(_maybe_close_job(job.job_id, current_store)))
        return {"job_id": job.job_id, "status": "processing", "file_count": len(files)}

    @app.post("/ingest/upload", status_code=202)
    async def ingest_upload(request: Request, files: list[UploadFile] = File(...)):
        store = request.app.state.store
        nexus = request.app.state.nexus
        forge = request.app.state.forge
        citadel = request.app.state.citadel
        config = request.app.state.config

        job = await store.create_job(source_type="upload")
        await store.update_job(job.job_id, status="processing", file_count=len(files))

        for uf in files:
            file_bytes = await uf.read()
            file_name = uf.filename or ""
            file_type = classify_file(file_name)
            f = await store.create_file(job_id=job.job_id, file_path=file_name, file_type=file_type)

            if file_type == "skip":
                await store.update_file(f.file_id, external_status="skipped", rag_status="pending")
            elif file_type == "code":
                asyncio.create_task(_safe_process(
                    dispatch_code_file(f.file_id, job.job_id, file_bytes, file_name, store, nexus)
                ))
            elif file_type == "plsql":
                callback_url = f"{config.self_url}/callback/citadel"
                try:
                    result = await citadel.submit(file_bytes, file_name, callback_url)
                    ext_id = result.get("rdoc_job_id") or result.get("job_id", "")
                    await store.update_file(f.file_id, external_job_id=ext_id, external_status="processing")
                except Exception as e:
                    logger.error("Citadel submit failed for %s: %s", file_name, e)
                    await store.update_file(f.file_id, external_status="failed", error=str(e))
            elif file_type == "document":
                callback_url = f"{config.self_url}/callback/forge"
                try:
                    result = await forge.convert(file_bytes, file_name, callback_url)
                    ext_id = result.get("job_id", "")
                    await store.update_file(f.file_id, external_job_id=ext_id, external_status="processing")
                except Exception as e:
                    logger.error("Forge convert failed for %s: %s", file_name, e)
                    await store.update_file(f.file_id, external_status="failed", error=str(e))

        asyncio.create_task(_safe_process(_maybe_close_job(job.job_id, store)))
        return {"job_id": job.job_id, "status": "processing", "file_count": len(files)}

    @app.post("/ingest/graphify-rebuild")
    async def graphify_rebuild(request: Request):
        nexus = request.app.state.nexus
        try:
            result = await nexus.rebuild()
            return result
        except Exception as e:
            logger.error("Nexus rebuild failed: %s", e)
            raise HTTPException(status_code=502, detail=f"Nexus rebuild failed: {e}")

    @app.get("/openapi-all.json", include_in_schema=False)
    async def openapi_all(request: Request):
        import httpx as _httpx
        cfg = request.app.state.config
        merged = app.openapi()

        for name, url in [("forge", cfg.forge_url), ("lightrag", cfg.lightrag_url)]:
            try:
                async with _httpx.AsyncClient(timeout=5.0) as c:
                    r = await c.get(f"{url}/openapi.json")
                if r.status_code == 200:
                    spec = r.json()
                    for path, item in spec.get("paths", {}).items():
                        prefixed = f"/{name}{path}"
                        merged.setdefault("paths", {})[prefixed] = item
            except Exception as e:
                logger.warning("Failed to fetch openapi from %s: %s", name, e)

        return merged

    return app


app = create_app()

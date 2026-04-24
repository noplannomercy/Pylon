import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request

from config import Config
from ingest import ForgeClient, LightRAGClient, classify_file, advance_pipeline
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
        await a.state.lightrag.close()
        if hasattr(a.state, "pool"):
            await a.state.pool.close()

    app = FastAPI(title="HCS Ingestion Router", version="0.1.0", lifespan=lifespan)
    app.state.store = store
    app.state.config = config
    app.state.forge = None
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
            if file_type == "skip":
                await current_store.update_file(f.file_id, forge_status="skipped")
                continue
            await current_store.update_file(f.file_id, forge_status="queued")

        return {"job_id": job.job_id, "status": "processing", "file_count": len(parsed["files"])}

    @app.post("/callback/forge")
    async def forge_callback(file_id: str, request: Request):
        body = await request.json()
        forge_job_id = body.get("job_id", "")
        asyncio.create_task(_safe_process(
            advance_pipeline(
                forge_job_id=forge_job_id,
                callback_body=body,
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
            if file_type == "skip":
                await current_store.update_file(f.file_id, forge_status="skipped")
        return {"job_id": job.job_id, "status": "processing", "file_count": len(files)}

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

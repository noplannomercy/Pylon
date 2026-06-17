import pytest
from httpx import AsyncClient, ASGITransport
from fastapi import FastAPI
from job_store import InMemoryJobStore
from admin import create_admin_router

@pytest.fixture
def test_app():
    store = InMemoryJobStore()
    a = FastAPI()
    a.state.store = store
    a.state.forge = None
    a.state.lightrag = None
    a.state.config = None
    a.include_router(create_admin_router(a.state))
    return a, store

@pytest.mark.asyncio
async def test_list_jobs_empty(test_app):
    a, _ = test_app
    async with AsyncClient(transport=ASGITransport(app=a), base_url="http://test") as client:
        resp = await client.get("/jobs")
    assert resp.status_code == 200
    assert resp.json()["total"] == 0

@pytest.mark.asyncio
async def test_get_job_detail(test_app):
    a, store = test_app
    job = await store.create_job(source_type="webhook", repo="GCore", pr_number=10)
    async with AsyncClient(transport=ASGITransport(app=a), base_url="http://test") as client:
        resp = await client.get(f"/jobs/{job.job_id}")
    assert resp.status_code == 200
    assert resp.json()["pr_number"] == 10

@pytest.mark.asyncio
async def test_get_job_not_found(test_app):
    a, _ = test_app
    async with AsyncClient(transport=ASGITransport(app=a), base_url="http://test") as client:
        resp = await client.get("/jobs/nonexistent")
    assert resp.status_code == 404

@pytest.mark.asyncio
async def test_reject_file(test_app):
    a, store = test_app
    job = await store.create_job(source_type="webhook", repo="GCore")
    f = await store.create_file(job_id=job.job_id, file_path="PKG.pkb", file_type="plsql")
    async with AsyncClient(transport=ASGITransport(app=a), base_url="http://test") as client:
        resp = await client.post(f"/files/{f.file_id}/reject")
    assert resp.status_code == 200
    updated = await store.get_file(f.file_id)
    assert updated.review_status == "rejected"

@pytest.mark.asyncio
async def test_stats(test_app):
    a, store = test_app
    await store.create_job(source_type="webhook", repo="GCore")
    async with AsyncClient(transport=ASGITransport(app=a), base_url="http://test") as client:
        resp = await client.get("/stats")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"]["jobs"] == 1

@pytest.mark.asyncio
async def test_retry_document_rag_failed_redispatches(test_app):
    """document가 forge done인데 LightRAG ingest만 실패 → retry가 forge 재취득+재적재 → ingested (#6)."""
    from unittest.mock import AsyncMock
    a, store = test_app
    a.state.forge = AsyncMock()
    a.state.forge.get_job = AsyncMock(return_value={"result": {"text": "converted text"}})
    a.state.lightrag = AsyncMock()
    a.state.lightrag.ingest_text = AsyncMock(return_value={"status": "ok"})
    job = await store.create_job(source_type="upload")
    f = await store.create_file(job_id=job.job_id, file_path="spec.pdf", file_type="document")
    await store.update_file(f.file_id, external_job_id="forge-1", external_status="done", rag_status="failed", error="ingest boom")

    async with AsyncClient(transport=ASGITransport(app=a), base_url="http://test") as client:
        resp = await client.post(f"/jobs/{job.job_id}/retry")
    assert resp.status_code == 200
    assert resp.json()["retried"] == 1
    updated = await store.get_file(f.file_id)
    assert updated.rag_status == "ingested"
    a.state.forge.get_job.assert_awaited_once_with("forge-1")
    a.state.lightrag.ingest_text.assert_awaited_once()

@pytest.mark.asyncio
async def test_retry_plsql_not_retryable_preserves_error(test_app):
    """plsql 실패 → bytes 없어 재시도 불가. terminal·error 보존 + not_retryable 보고, queued 림보 리셋 안 함 (#6)."""
    a, store = test_app
    job = await store.create_job(source_type="upload")
    f = await store.create_file(job_id=job.job_id, file_path="PKG_body.sql", file_type="plsql")
    await store.update_file(f.file_id, external_status="failed", error="robotics 500")

    async with AsyncClient(transport=ASGITransport(app=a), base_url="http://test") as client:
        resp = await client.post(f"/jobs/{job.job_id}/retry")
    assert resp.status_code == 200
    assert resp.json()["retried"] == 0
    assert f.file_id in resp.json()["not_retryable"]
    updated = await store.get_file(f.file_id)
    assert updated.external_status == "failed"
    assert updated.error == "robotics 500"

@pytest.mark.asyncio
async def test_health_all_includes_database(test_app):
    """Postgres pool 있으면 /health/all에 database readiness 포함 (#11)."""
    a, store = test_app

    class FakeConn:
        async def fetchval(self, q):
            return 1

    class FakeAcquire:
        async def __aenter__(self):
            return FakeConn()
        async def __aexit__(self, *args):
            return False

    class FakePool:
        def acquire(self):
            return FakeAcquire()

    a.state.pool = FakePool()
    async with AsyncClient(transport=ASGITransport(app=a), base_url="http://test") as client:
        resp = await client.get("/health/all")
    assert resp.status_code == 200
    assert resp.json()["database"] == "ok"

@pytest.mark.asyncio
async def test_re_ingest_plsql_returns_400(test_app):
    """plsql 파일 re-ingest 시도 → 400 반환 (원본 bytes 없음)."""
    a, store = test_app
    job = await store.create_job(source_type="upload")
    f = await store.create_file(job_id=job.job_id, file_path="PKG_body.sql", file_type="plsql")
    await store.update_file(f.file_id, external_job_id="robotics-123", external_status="done", rag_status="failed")

    async with AsyncClient(transport=ASGITransport(app=a), base_url="http://test") as client:
        resp = await client.post(f"/files/{f.file_id}/re-ingest")
    assert resp.status_code == 400
    assert "plsql" in resp.json()["detail"].lower()

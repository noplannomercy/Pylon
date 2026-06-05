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

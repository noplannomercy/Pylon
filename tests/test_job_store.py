import pytest
from job_store import InMemoryJobStore

@pytest.mark.asyncio
async def test_create_and_get_job():
    store = InMemoryJobStore()
    job = await store.create_job(source_type="webhook", repo="GCore", pr_number=42, commit_hash="abc123")
    assert job.source_type == "webhook"
    assert job.pr_number == 42
    assert job.status == "created"
    fetched = await store.get_job(job.job_id)
    assert fetched.job_id == job.job_id

@pytest.mark.asyncio
async def test_create_and_update_file():
    store = InMemoryJobStore()
    job = await store.create_job(source_type="webhook", repo="GCore")
    f = await store.create_file(job_id=job.job_id, file_path="src/PKG.pkb", file_type="plsql")
    assert f.external_status == "queued"
    await store.update_file(f.file_id, external_status="done", external_job_id="citadel-xyz")
    updated = await store.get_file(f.file_id)
    assert updated.external_status == "done"
    assert updated.external_job_id == "citadel-xyz"

@pytest.mark.asyncio
async def test_get_file_by_external_job_id():
    store = InMemoryJobStore()
    job = await store.create_job(source_type="webhook", repo="GCore")
    f = await store.create_file(job_id=job.job_id, file_path="src/PKG.pkb", file_type="plsql")
    await store.update_file(f.file_id, external_job_id="citadel-xyz")
    result = await store.get_file_by_external_job_id("citadel-xyz")
    assert result is not None
    assert result.file_id == f.file_id

@pytest.mark.asyncio
async def test_list_jobs_with_filter():
    store = InMemoryJobStore()
    await store.create_job(source_type="webhook", repo="GCore")
    j2 = await store.create_job(source_type="bulk", repo="GCore")
    await store.update_job(j2.job_id, status="completed")
    jobs, total = await store.list_jobs(status="completed")
    assert total == 1
    assert jobs[0].job_id == j2.job_id

@pytest.mark.asyncio
async def test_get_stats():
    store = InMemoryJobStore()
    job = await store.create_job(source_type="webhook", repo="GCore")
    await store.create_file(job_id=job.job_id, file_path="A.pkb", file_type="plsql")
    stats = await store.get_stats()
    assert "today" in stats
    assert "total" in stats
    assert stats["total"]["jobs"] == 1

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
    await store.update_file(f.file_id, external_status="done", external_job_id="robotics-xyz")
    updated = await store.get_file(f.file_id)
    assert updated.external_status == "done"
    assert updated.external_job_id == "robotics-xyz"

@pytest.mark.asyncio
async def test_get_file_by_external_job_id():
    store = InMemoryJobStore()
    job = await store.create_job(source_type="webhook", repo="GCore")
    f = await store.create_file(job_id=job.job_id, file_path="src/PKG.pkb", file_type="plsql")
    await store.update_file(f.file_id, external_job_id="robotics-xyz")
    result = await store.get_file_by_external_job_id("robotics-xyz")
    assert result is not None
    assert result.file_id == f.file_id

@pytest.mark.asyncio
async def test_create_job_idempotent_on_repo_pr_commit():
    """동일 (repo, pr_number, commit_hash) 재전송 → 같은 job 재사용 (#7 멱등)."""
    store = InMemoryJobStore()
    j1 = await store.create_job(source_type="webhook", repo="GCore", pr_number=7, commit_hash="abc")
    j2 = await store.create_job(source_type="webhook", repo="GCore", pr_number=7, commit_hash="abc")
    assert j1.job_id == j2.job_id

@pytest.mark.asyncio
async def test_create_job_no_dedup_without_full_key():
    """키(repo/pr/commit) 없는 upload job은 매번 새 job — NULL끼리 충돌 금지 (#7)."""
    store = InMemoryJobStore()
    j1 = await store.create_job(source_type="upload")
    j2 = await store.create_job(source_type="upload")
    assert j1.job_id != j2.job_id

@pytest.mark.asyncio
async def test_create_job_distinct_commit_not_deduped():
    """같은 PR이라도 commit이 다르면 별개 job (#7)."""
    store = InMemoryJobStore()
    j1 = await store.create_job(source_type="webhook", repo="GCore", pr_number=7, commit_hash="abc")
    j2 = await store.create_job(source_type="webhook", repo="GCore", pr_number=7, commit_hash="def")
    assert j1.job_id != j2.job_id

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
async def test_claim_file_only_first_wins():
    """조건부 claim — 첫 호출만 성공, 둘째는 None (#8 이중 ingest 차단)."""
    store = InMemoryJobStore()
    job = await store.create_job(source_type="webhook")
    f = await store.create_file(job_id=job.job_id, file_path="x.pkb", file_type="plsql")
    await store.update_file(f.file_id, external_status="processing")
    first = await store.claim_file(f.file_id, ("queued", "processing"), external_status="done", rag_status="ingesting")
    second = await store.claim_file(f.file_id, ("queued", "processing"), external_status="done", rag_status="ingesting")
    assert first is not None
    assert first.external_status == "done"
    assert second is None

@pytest.mark.asyncio
async def test_claim_file_rejects_terminal_status():
    """이미 terminal(done)인 file은 claim 불가 (#8)."""
    store = InMemoryJobStore()
    job = await store.create_job(source_type="webhook")
    f = await store.create_file(job_id=job.job_id, file_path="x.pkb", file_type="plsql")
    await store.update_file(f.file_id, external_status="done")
    claimed = await store.claim_file(f.file_id, ("queued", "processing"), external_status="done")
    assert claimed is None

@pytest.mark.asyncio
async def test_get_stats():
    store = InMemoryJobStore()
    job = await store.create_job(source_type="webhook", repo="GCore")
    await store.create_file(job_id=job.job_id, file_path="A.pkb", file_type="plsql")
    stats = await store.get_stats()
    assert "today" in stats
    assert "total" in stats
    assert stats["total"]["jobs"] == 1

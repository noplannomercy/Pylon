import pytest
from unittest.mock import AsyncMock
from job_store import InMemoryJobStore
from ingest import advance_pipeline

@pytest.mark.asyncio
async def test_advance_on_forge_success():
    store = InMemoryJobStore()
    job = await store.create_job(source_type="webhook", repo="GCore")
    f = await store.create_file(job_id=job.job_id, file_path="PKG.pkb", file_type="plsql")
    await store.update_file(f.file_id, external_job_id="forge-123", external_status="processing")

    lightrag = AsyncMock()
    lightrag.ingest_text = AsyncMock(return_value={"status": "ok"})

    await advance_pipeline(
        external_job_id="forge-123",
        callback_body={"status": "completed", "result": {"text": "# PKG\n내용"}},
        store=store,
        lightrag=lightrag,
    )

    updated = await store.get_file(f.file_id)
    assert updated.external_status == "done"
    assert updated.rag_status == "ingested"
    lightrag.ingest_text.assert_called_once()

@pytest.mark.asyncio
async def test_advance_on_forge_failure():
    store = InMemoryJobStore()
    job = await store.create_job(source_type="webhook", repo="GCore")
    f = await store.create_file(job_id=job.job_id, file_path="PKG.pkb", file_type="plsql")
    await store.update_file(f.file_id, external_job_id="forge-999", external_status="processing")

    lightrag = AsyncMock()

    await advance_pipeline(
        external_job_id="forge-999",
        callback_body={"status": "failed", "error": "VLM timeout"},
        store=store,
        lightrag=lightrag,
    )

    updated = await store.get_file(f.file_id)
    assert updated.external_status == "failed"
    assert updated.error == "VLM timeout"
    lightrag.ingest_text.assert_not_called()

@pytest.mark.asyncio
async def test_job_completed_when_all_done():
    store = InMemoryJobStore()
    job = await store.create_job(source_type="webhook", repo="GCore")
    f1 = await store.create_file(job_id=job.job_id, file_path="A.pkb", file_type="plsql")
    f2 = await store.create_file(job_id=job.job_id, file_path="B.pkb", file_type="plsql")
    await store.update_file(f1.file_id, external_job_id="forge-1", external_status="processing")
    await store.update_file(f2.file_id, external_status="done", rag_status="ingested")

    lightrag = AsyncMock()
    lightrag.ingest_text = AsyncMock(return_value={"status": "ok"})

    await advance_pipeline(
        external_job_id="forge-1",
        callback_body={"status": "completed", "result": {"text": "content"}},
        store=store,
        lightrag=lightrag,
    )

    updated_job = await store.get_job(job.job_id)
    assert updated_job.status == "completed"

@pytest.mark.asyncio
async def test_job_completed_with_code_files():
    """code 파일이 포함된 job에서 _maybe_close_job()이 정상 완료되는지 검증 (CRITICAL regression)."""
    store = InMemoryJobStore()
    job = await store.create_job(source_type="webhook", repo="GCore")
    f_plsql = await store.create_file(job_id=job.job_id, file_path="A.pkb", file_type="plsql")
    f_code = await store.create_file(job_id=job.job_id, file_path="Main.java", file_type="code")
    await store.update_file(f_plsql.file_id, external_job_id="citadel-1", external_status="processing")
    await store.update_file(f_code.file_id, external_status="skipped", rag_status="skipped")

    lightrag = AsyncMock()
    lightrag.ingest_text = AsyncMock(return_value={"status": "ok"})

    await advance_pipeline(
        external_job_id="citadel-1",
        callback_body={"status": "completed", "result": {"text": "역문서화 결과"}},
        store=store,
        lightrag=lightrag,
    )

    updated_job = await store.get_job(job.job_id)
    assert updated_job.status == "completed"

@pytest.mark.asyncio
async def test_job_completed_when_all_skip():
    """모든 파일이 skip인 job이 자동으로 완료되는지 검증 (all-skip 버그 회귀)."""
    from ingest import _maybe_close_job
    store = InMemoryJobStore()
    job = await store.create_job(source_type="webhook", repo="GCore")
    f1 = await store.create_file(job_id=job.job_id, file_path="image.png", file_type="skip")
    f2 = await store.create_file(job_id=job.job_id, file_path="Main.java", file_type="code")
    await store.update_file(f1.file_id, external_status="skipped", rag_status="pending")
    await store.update_file(f2.file_id, external_status="skipped", rag_status="skipped")

    await _maybe_close_job(job.job_id, store)

    updated_job = await store.get_job(job.job_id)
    assert updated_job.status == "completed"

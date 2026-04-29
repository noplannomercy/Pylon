# Ingestion Router v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 아키텍처 v0.4 반영 — Citadel(:8004) PL/SQL 연동, 코드 파일 분기 추가, 필드명 정규화(`forge_job_id`→`external_job_id`, `forge_status`→`external_status`), all-skip 버그 수정.

**Architecture:** Ingestion Router는 파일 유형별로 Citadel(PL/SQL), Forge(문서), skip(코드/기타)로 분기한다. Citadel은 비동기 콜백(`POST /callback/citadel`)으로 완료를 통보한다. Graphify는 외부 수동 배치로 실행되며, `POST /ingest/graphify-rebuild`로 Nexus rebuild를 트리거한다.

**Tech Stack:** Python 3.11+, FastAPI, httpx (async), pydantic-settings, asyncpg, pytest-asyncio, respx

---

### Task 1: config.py — Citadel/Nexus URL 추가

**Files:**
- Modify: `config.py`

- [ ] **Step 1: config.py에 4개 필드 추가**

```python
# config.py 전체 교체
from pydantic_settings import BaseSettings

class Config(BaseSettings):
    forge_url: str = "http://localhost:8003"
    lightrag_url: str = "http://localhost:9621"
    citadel_url: str = "http://localhost:8004"
    nexus_url: str = "http://localhost:8005"
    database_url: str = ""
    bitbucket_webhook_secret: str = ""
    forge_api_key: str = ""
    lightrag_api_key: str = ""
    citadel_api_key: str = ""
    nexus_api_key: str = ""
    port: int = 8001
    self_url: str = "http://localhost:8001"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}
```

- [ ] **Step 2: 테스트 실행 — 기존 28개 통과 확인**

```bash
python -m pytest tests/ -v
```
Expected: `28 passed`

- [ ] **Step 3: .env에 신규 항목 추가**

```env
# .env (기존 항목 유지, 아래만 추가)
CITADEL_URL=http://localhost:8004
NEXUS_URL=http://localhost:8005
CITADEL_API_KEY=
NEXUS_API_KEY=
```

- [ ] **Step 4: Commit**

```bash
git add config.py .env
git commit -m "feat: config — CITADEL_URL, NEXUS_URL, API key 필드 추가"
```

---

### Task 2: 필드명 정규화 — models, schema, job_store, 기존 테스트

이 태스크는 `forge_job_id`→`external_job_id`, `forge_status`→`external_status` 전체 rename이다. 한 번에 처리하지 않으면 중간 상태에서 테스트가 전부 깨진다.

**Files:**
- Modify: `models.py`
- Modify: `schema.sql`
- Modify: `job_store.py`
- Modify: `tests/test_job_store.py`
- Modify: `tests/test_pipeline.py`
- Modify: `tests/test_app.py`

- [ ] **Step 1: models.py 필드명 변경**

```python
# models.py 전체 교체
from datetime import datetime
from typing import Optional
from pydantic import BaseModel

class IngestionJob(BaseModel):
    job_id: str
    source_type: str            # 'webhook' | 'bulk'
    repo: Optional[str] = None
    pr_number: Optional[int] = None
    commit_hash: Optional[str] = None
    status: str = "created"     # created | processing | completed | partial | failed
    file_count: int = 0
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

class IngestionFile(BaseModel):
    file_id: str
    job_id: str
    file_path: str
    file_type: str              # 'plsql' | 'document' | 'code' | 'skip'
    external_job_id: Optional[str] = None   # Forge or Citadel job ID
    external_status: str = "queued"         # queued | processing | done | skipped | failed
    rag_status: str = "pending"             # pending | ingesting | ingested | skipped | failed
    review_status: str = "auto_approved"    # auto_approved | rejected
    error: Optional[str] = None
    created_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
```

- [ ] **Step 2: schema.sql 컬럼/인덱스명 변경**

```sql
-- schema.sql 전체 교체
-- HCS Ingestion Router schema (idempotent)

CREATE TABLE IF NOT EXISTS ingestion_job (
    job_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_type  TEXT NOT NULL,
    repo         TEXT,
    pr_number    INT,
    commit_hash  TEXT,
    status       TEXT NOT NULL DEFAULT 'created',
    file_count   INT DEFAULT 0,
    created_at   TIMESTAMPTZ DEFAULT now(),
    started_at   TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS ingestion_file (
    file_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id          UUID NOT NULL REFERENCES ingestion_job(job_id),
    file_path       TEXT NOT NULL,
    file_type       TEXT NOT NULL,
    external_job_id TEXT,
    external_status TEXT NOT NULL DEFAULT 'queued',
    rag_status      TEXT NOT NULL DEFAULT 'pending',
    review_status   TEXT NOT NULL DEFAULT 'auto_approved',
    error           TEXT,
    created_at      TIMESTAMPTZ DEFAULT now(),
    completed_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_ingestion_file_job_id ON ingestion_file(job_id);
CREATE INDEX IF NOT EXISTS idx_ingestion_file_external_job_id ON ingestion_file(external_job_id);
```

- [ ] **Step 3: job_store.py 필드명 변경**

```python
# job_store.py 전체 교체
import uuid
from datetime import datetime, timezone, date
from typing import Optional
from models import IngestionJob, IngestionFile

def _now():
    return datetime.now(timezone.utc)

class InMemoryJobStore:
    def __init__(self):
        self._jobs: dict[str, IngestionJob] = {}
        self._files: dict[str, IngestionFile] = {}

    async def create_job(self, source_type: str, repo: str = None, pr_number: int = None, commit_hash: str = None) -> IngestionJob:
        job = IngestionJob(
            job_id=str(uuid.uuid4()),
            source_type=source_type,
            repo=repo,
            pr_number=pr_number,
            commit_hash=commit_hash,
            status="created",
            created_at=_now(),
        )
        self._jobs[job.job_id] = job
        return job

    async def get_job(self, job_id: str) -> Optional[IngestionJob]:
        return self._jobs.get(job_id)

    async def update_job(self, job_id: str, **kwargs) -> Optional[IngestionJob]:
        job = self._jobs.get(job_id)
        if job is None:
            return None
        self._jobs[job_id] = job.model_copy(update=kwargs)
        return self._jobs[job_id]

    async def create_file(self, job_id: str, file_path: str, file_type: str) -> IngestionFile:
        f = IngestionFile(
            file_id=str(uuid.uuid4()),
            job_id=job_id,
            file_path=file_path,
            file_type=file_type,
            created_at=_now(),
        )
        self._files[f.file_id] = f
        return f

    async def get_file(self, file_id: str) -> Optional[IngestionFile]:
        return self._files.get(file_id)

    async def get_file_by_external_job_id(self, external_job_id: str) -> Optional[IngestionFile]:
        for f in self._files.values():
            if f.external_job_id == external_job_id:
                return f
        return None

    async def update_file(self, file_id: str, **kwargs) -> Optional[IngestionFile]:
        f = self._files.get(file_id)
        if f is None:
            return None
        self._files[file_id] = f.model_copy(update=kwargs)
        return self._files[file_id]

    async def list_files_for_job(self, job_id: str) -> list[IngestionFile]:
        return [f for f in self._files.values() if f.job_id == job_id]

    async def list_jobs(self, page: int = 1, size: int = 20, status: str = None, source_type: str = None) -> tuple[list[IngestionJob], int]:
        jobs = list(self._jobs.values())
        if status:
            jobs = [j for j in jobs if j.status == status]
        if source_type:
            jobs = [j for j in jobs if j.source_type == source_type]
        total = len(jobs)
        jobs.sort(key=lambda j: j.created_at or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        start = (page - 1) * size
        return jobs[start:start + size], total

    async def get_stats(self) -> dict:
        today_str = date.today().isoformat()
        jobs = list(self._jobs.values())
        files = list(self._files.values())
        today_jobs = [j for j in jobs if j.created_at and j.created_at.date().isoformat() == today_str]
        failed_files = [f for f in files if f.external_status == "failed" or f.rag_status == "failed"]
        return {
            "today": {
                "jobs": len(today_jobs),
                "completed": sum(1 for j in today_jobs if j.status == "completed"),
                "failed": sum(1 for j in today_jobs if j.status == "failed"),
                "partial": sum(1 for j in today_jobs if j.status == "partial"),
            },
            "total": {"jobs": len(jobs), "files": len(files)},
            "recent_failures": [
                {"file_id": f.file_id, "file_path": f.file_path, "job_id": f.job_id,
                 "external_status": f.external_status, "rag_status": f.rag_status, "error": f.error}
                for f in failed_files[-10:]
            ],
        }


class PostgresJobStore:
    def __init__(self, pool):
        self._pool = pool

    async def create_job(self, source_type: str, repo: str = None, pr_number: int = None, commit_hash: str = None) -> IngestionJob:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "INSERT INTO ingestion_job (source_type, repo, pr_number, commit_hash) VALUES ($1,$2,$3,$4) RETURNING *",
                source_type, repo, pr_number, commit_hash,
            )
        return IngestionJob(**dict(row))

    async def get_job(self, job_id: str) -> Optional[IngestionJob]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM ingestion_job WHERE job_id = $1", job_id)
        return IngestionJob(**dict(row)) if row else None

    async def update_job(self, job_id: str, **kwargs) -> Optional[IngestionJob]:
        if not kwargs:
            return await self.get_job(job_id)
        keys = list(kwargs.keys())
        vals = list(kwargs.values())
        sets = ", ".join(f"{k} = ${i+2}" for i, k in enumerate(keys))
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"UPDATE ingestion_job SET {sets} WHERE job_id = $1 RETURNING *",
                job_id, *vals,
            )
        return IngestionJob(**dict(row)) if row else None

    async def create_file(self, job_id: str, file_path: str, file_type: str) -> IngestionFile:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "INSERT INTO ingestion_file (job_id, file_path, file_type) VALUES ($1,$2,$3) RETURNING *",
                job_id, file_path, file_type,
            )
        return IngestionFile(**dict(row))

    async def get_file(self, file_id: str) -> Optional[IngestionFile]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM ingestion_file WHERE file_id = $1", file_id)
        return IngestionFile(**dict(row)) if row else None

    async def get_file_by_external_job_id(self, external_job_id: str) -> Optional[IngestionFile]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM ingestion_file WHERE external_job_id = $1", external_job_id)
        return IngestionFile(**dict(row)) if row else None

    async def update_file(self, file_id: str, **kwargs) -> Optional[IngestionFile]:
        if not kwargs:
            return await self.get_file(file_id)
        keys = list(kwargs.keys())
        vals = list(kwargs.values())
        sets = ", ".join(f"{k} = ${i+2}" for i, k in enumerate(keys))
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"UPDATE ingestion_file SET {sets} WHERE file_id = $1 RETURNING *",
                file_id, *vals,
            )
        return IngestionFile(**dict(row)) if row else None

    async def list_files_for_job(self, job_id: str) -> list[IngestionFile]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM ingestion_file WHERE job_id = $1", job_id)
        return [IngestionFile(**dict(r)) for r in rows]

    async def list_jobs(self, page: int = 1, size: int = 20, status: str = None, source_type: str = None) -> tuple[list[IngestionJob], int]:
        conditions, vals = [], []
        if status:
            vals.append(status)
            conditions.append(f"status = ${len(vals)}")
        if source_type:
            vals.append(source_type)
            conditions.append(f"source_type = ${len(vals)}")
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        offset = (page - 1) * size
        async with self._pool.acquire() as conn:
            total = await conn.fetchval(f"SELECT COUNT(*) FROM ingestion_job {where}", *vals)
            rows = await conn.fetch(
                f"SELECT * FROM ingestion_job {where} ORDER BY created_at DESC LIMIT ${len(vals)+1} OFFSET ${len(vals)+2}",
                *vals, size, offset,
            )
        return [IngestionJob(**dict(r)) for r in rows], total

    async def get_stats(self) -> dict:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT COUNT(*) as total,
                    SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) as completed,
                    SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) as failed,
                    SUM(CASE WHEN status='partial' THEN 1 ELSE 0 END) as partial
                   FROM ingestion_job WHERE created_at::date = CURRENT_DATE"""
            )
            total_jobs = await conn.fetchval("SELECT COUNT(*) FROM ingestion_job")
            total_files = await conn.fetchval("SELECT COUNT(*) FROM ingestion_file")
            failures = await conn.fetch(
                """SELECT file_id, file_path, job_id, external_status, rag_status, error
                   FROM ingestion_file WHERE external_status='failed' OR rag_status='failed'
                   ORDER BY created_at DESC LIMIT 10"""
            )
        return {
            "today": {"jobs": row["total"], "completed": row["completed"], "failed": row["failed"], "partial": row["partial"]},
            "total": {"jobs": total_jobs, "files": total_files},
            "recent_failures": [dict(r) for r in failures],
        }
```

- [ ] **Step 4: tests/test_job_store.py — 필드명 업데이트**

```python
# tests/test_job_store.py 전체 교체
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
```

- [ ] **Step 5: tests/test_pipeline.py — 필드명 업데이트**

```python
# tests/test_pipeline.py 전체 교체
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
```

- [ ] **Step 6: tests/test_app.py — /callback/forge file_id 파라미터 제거 반영**

`test_callback_forge_unknown_job`에서 `params={"file_id": "nonexistent"}` 제거:

```python
@pytest.mark.asyncio
async def test_callback_forge_unknown_job(app_instance):
    async with AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://test") as client:
        resp = await client.post(
            "/callback/forge",
            json={"job_id": "unknown-forge-id", "status": "completed", "result": {"text": ""}},
        )
    assert resp.status_code == 200
    assert resp.json()["received"] is True
```

- [ ] **Step 7: 테스트 실행 — 모두 실패 확인 (아직 ingest.py 미수정)**

```bash
python -m pytest tests/ -v 2>&1 | head -30
```
Expected: 여러 테스트 FAIL (forge_job_id/forge_status 참조 오류)

- [ ] **Step 8: ingest.py — advance_pipeline 파라미터명 변경**

`ingest.py`에서 `forge_job_id` → `external_job_id`, `forge_status` → `external_status` 변경:

```python
# ingest.py advance_pipeline 함수 교체 (CitadelClient/NexusClient는 Task 4에서 추가)
async def advance_pipeline(external_job_id: str, callback_body: dict, store, lightrag: LightRAGClient):
    f = await store.get_file_by_external_job_id(external_job_id)
    if f is None:
        logger.warning("Unknown external_job_id in callback: %s", external_job_id)
        return

    if callback_body.get("status") != "completed":
        error_msg = callback_body.get("error", "processing failed")
        await store.update_file(f.file_id, external_status="failed", error=error_msg)
        await _maybe_close_job(f.job_id, store)
        return

    await store.update_file(f.file_id, external_status="done", rag_status="ingesting")

    result_text = (callback_body.get("result") or {}).get("text", "")
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
        await store.update_file(f.file_id, rag_status="ingested", completed_at=datetime.now(timezone.utc))
    except Exception as e:
        logger.error("LightRAG ingest failed for file %s: %s", f.file_id, e)
        await store.update_file(f.file_id, rag_status="failed", error=str(e))

    await _maybe_close_job(f.job_id, store)
```

- [ ] **Step 9: ingest.py — _maybe_close_job() rag_terminal + success count 수정**

```python
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
```

- [ ] **Step 10: app.py — forge_callback에서 dead file_id 파라미터 제거**

`app.py`의 `/callback/forge` 핸들러에서 `file_id: str` 파라미터 제거 및 `forge_job_id` → `external_job_id` 변경:

```python
@app.post("/callback/forge")
async def forge_callback(request: Request):
    body = await request.json()
    external_job_id = body.get("job_id", "")
    asyncio.create_task(_safe_process(
        advance_pipeline(
            external_job_id=external_job_id,
            callback_body=body,
            store=request.app.state.store,
            lightrag=request.app.state.lightrag,
        )
    ))
    return {"received": True}
```

- [ ] **Step 11: 테스트 실행 — 28개 통과 확인**

```bash
python -m pytest tests/ -v
```
Expected: `28 passed`

- [ ] **Step 12: Commit**

```bash
git add models.py schema.sql job_store.py ingest.py tests/test_job_store.py tests/test_pipeline.py tests/test_app.py
git commit -m "refactor: forge_job_id→external_job_id, forge_status→external_status 전체 정규화"
```

---

### Task 3: classify_file() — code 타입 추가

**Files:**
- Modify: `ingest.py`
- Modify: `tests/test_ingest.py`

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/test_ingest.py`에 추가:

```python
def test_classify_code():
    assert classify_file("Main.java") == "code"
    assert classify_file("index.ts") == "code"
    assert classify_file("App.tsx") == "code"
    assert classify_file("app.js") == "code"
    assert classify_file("App.jsx") == "code"
    assert classify_file("utils.py") == "code"
```

- [ ] **Step 2: 테스트 실행 — FAIL 확인**

```bash
python -m pytest tests/test_ingest.py::test_classify_code -v
```
Expected: `FAILED` — `assert 'skip' == 'code'`

- [ ] **Step 3: ingest.py — CODE_EXTS 추가 및 classify_file 수정**

`ingest.py` 상단의 상수 부분:

```python
PLSQL_EXTS = {".pkb", ".pks", ".sql", ".prc", ".fnc"}
DOCUMENT_EXTS = {".pdf", ".docx", ".pptx", ".xlsx", ".md", ".txt", ".hwpx"}
CODE_EXTS = {".java", ".js", ".ts", ".jsx", ".tsx", ".py"}

def classify_file(file_path: str) -> str:
    if "." not in file_path:
        return "skip"
    ext = "." + file_path.rsplit(".", 1)[-1].lower()
    if ext in PLSQL_EXTS:
        return "plsql"
    if ext in DOCUMENT_EXTS:
        return "document"
    if ext in CODE_EXTS:
        return "code"
    return "skip"
```

- [ ] **Step 4: 테스트 실행 — PASS 확인**

```bash
python -m pytest tests/test_ingest.py -v
```
Expected: `5 passed`

- [ ] **Step 5: 전체 테스트 실행**

```bash
python -m pytest tests/ -v
```
Expected: `29 passed`

- [ ] **Step 6: Commit**

```bash
git add ingest.py tests/test_ingest.py
git commit -m "feat: classify_file() — code 타입 추가 (.java/.ts/.js/.tsx/.jsx/.py)"
```

---

### Task 4: CitadelClient 추가 + ForgeClient plsql 분기 제거

**Files:**
- Modify: `ingest.py`
- Modify: `tests/test_ingest.py`

- [ ] **Step 1: CitadelClient 실패 테스트 작성**

`tests/test_ingest.py`에 추가:

```python
@pytest.mark.asyncio
async def test_citadel_submit_success():
    from ingest import CitadelClient
    client = CitadelClient(base_url="http://citadel:8004", api_key="test-key")
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json = MagicMock(return_value={"job_id": "citadel-abc", "status": "queued"})
    with patch.object(client._client, "post", new=AsyncMock(return_value=mock_resp)) as mock_post:
        result = await client.submit(
            file_bytes=b"CREATE OR REPLACE PROCEDURE SP_TEST AS BEGIN NULL; END;",
            file_name="SP_TEST.pks",
            callback_url="http://router:8001/callback/citadel",
        )
        call_args = mock_post.call_args
        assert call_args[0][0] == "/jobs"
        assert call_args[1]["data"]["asset_type"] == "plsql"
        assert call_args[1]["data"]["callback_url"] == "http://router:8001/callback/citadel"
    assert result["job_id"] == "citadel-abc"
    await client.close()

@pytest.mark.asyncio
async def test_citadel_submit_http_error():
    from ingest import CitadelClient
    import httpx
    client = CitadelClient(base_url="http://citadel:8004", api_key="test-key")
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock(side_effect=httpx.HTTPStatusError("500", request=MagicMock(), response=MagicMock()))
    with patch.object(client._client, "post", new=AsyncMock(return_value=mock_resp)):
        with pytest.raises(httpx.HTTPStatusError):
            await client.submit(b"code", "file.pks", "http://callback")
    await client.close()
```

- [ ] **Step 2: 테스트 실행 — FAIL 확인**

```bash
python -m pytest tests/test_ingest.py::test_citadel_submit_success -v
```
Expected: `FAILED` — `ImportError: cannot import name 'CitadelClient'`

- [ ] **Step 3: ingest.py — CitadelClient 추가**

`ForgeClient` 클래스 다음에 추가:

```python
class CitadelClient:
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
```

- [ ] **Step 4: ForgeClient.convert() — plsql 분기 제거**

`ForgeClient.convert()`를 document 전용으로 단순화:

```python
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
```

- [ ] **Step 5: tests/test_ingest.py — test_forge_convert_plsql를 document 테스트로 교체**

```python
@pytest.mark.asyncio
async def test_forge_convert_document():
    client = ForgeClient(base_url="http://forge:8003", api_key="test-key")
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json = MagicMock(return_value={"job_id": "forge-123", "status": "queued"})
    with patch.object(client._client, "post", new=AsyncMock(return_value=mock_resp)) as mock_post:
        result = await client.convert(
            file_bytes=b"%PDF-1.4",
            file_name="spec.pdf",
            callback_url="http://router:8001/callback/forge",
        )
        assert mock_post.call_args[0][0] == "/convert"
    assert result["job_id"] == "forge-123"
    await client.close()
```

- [ ] **Step 6: 테스트 실행 — 전체 통과 확인**

```bash
python -m pytest tests/test_ingest.py -v
```
Expected: `7 passed`

- [ ] **Step 7: 전체 테스트**

```bash
python -m pytest tests/ -v
```
Expected: `31 passed`

- [ ] **Step 8: Commit**

```bash
git add ingest.py tests/test_ingest.py
git commit -m "feat: CitadelClient 추가, ForgeClient plsql 분기 제거"
```

---

### Task 5: NexusClient 추가

**Files:**
- Modify: `ingest.py`
- Modify: `tests/test_ingest.py`

- [ ] **Step 1: 실패 테스트 작성**

`tests/test_ingest.py`에 추가:

```python
@pytest.mark.asyncio
async def test_nexus_rebuild_success():
    from ingest import NexusClient
    client = NexusClient(base_url="http://nexus:8005", api_key="test-key")
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json = MagicMock(return_value={"status": "ok", "message": "Rebuild started"})
    with patch.object(client._client, "post", new=AsyncMock(return_value=mock_resp)) as mock_post:
        result = await client.rebuild()
        assert mock_post.call_args[0][0] == "/rebuild/"
    assert result["status"] == "ok"
    await client.close()
```

- [ ] **Step 2: 테스트 실행 — FAIL 확인**

```bash
python -m pytest tests/test_ingest.py::test_nexus_rebuild_success -v
```
Expected: `FAILED` — `ImportError: cannot import name 'NexusClient'`

- [ ] **Step 3: ingest.py — NexusClient 추가**

`CitadelClient` 다음에 추가:

```python
class NexusClient:
    def __init__(self, base_url: str, api_key: str):
        headers = {}
        if api_key:
            headers["X-Api-Key"] = api_key
        self._client = httpx.AsyncClient(
            base_url=base_url,
            headers=headers,
            timeout=30.0,
        )

    async def rebuild(self) -> dict:
        resp = await self._client.post("/rebuild/")
        resp.raise_for_status()
        return resp.json()

    async def close(self):
        await self._client.aclose()
```

- [ ] **Step 4: 테스트 실행 — 통과 확인**

```bash
python -m pytest tests/test_ingest.py -v
```
Expected: `8 passed`

- [ ] **Step 5: Commit**

```bash
git add ingest.py tests/test_ingest.py
git commit -m "feat: NexusClient 추가 — POST /rebuild/ 트리거"
```

---

### Task 6: app.py — 새 엔드포인트 + all-skip 버그 수정

**Files:**
- Modify: `app.py`
- Modify: `tests/test_app.py`

- [ ] **Step 1: 실패 테스트 작성**

`tests/test_app.py`에 추가:

```python
@pytest.fixture
def test_config():
    return Config(
        forge_url="http://forge:8003",
        lightrag_url="http://lightrag:9621",
        citadel_url="http://citadel:8004",
        nexus_url="http://nexus:8005",
        database_url="",
        bitbucket_webhook_secret="test-secret",
        forge_api_key="key",
        lightrag_api_key="key",
        citadel_api_key="key",
        nexus_api_key="key",
        self_url="http://localhost:8001",
    )

@pytest.mark.asyncio
async def test_callback_citadel_completed(app_instance):
    """Citadel 완료 콜백 — LightRAG ingest 트리거."""
    store = app_instance.state.store
    job = await store.create_job(source_type="webhook", repo="GCore")
    f = await store.create_file(job_id=job.job_id, file_path="PKG.pkb", file_type="plsql")
    await store.update_file(f.file_id, external_job_id="citadel-999", external_status="processing")

    async with AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://test") as client:
        resp = await client.post(
            "/callback/citadel",
            json={
                "rdoc_job_id": "citadel-999",
                "file_name": "PKG.pkb",
                "content": "# PKG 역문서화 결과",
                "status": "completed",
                "error": None,
            },
        )
    assert resp.status_code == 200
    assert resp.json()["received"] is True

@pytest.mark.asyncio
async def test_callback_citadel_failed(app_instance):
    """Citadel 실패 콜백 — external_status='failed' 기록."""
    store = app_instance.state.store
    job = await store.create_job(source_type="webhook", repo="GCore")
    f = await store.create_file(job_id=job.job_id, file_path="PKG.pkb", file_type="plsql")
    await store.update_file(f.file_id, external_job_id="citadel-fail", external_status="processing")

    async with AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://test") as client:
        resp = await client.post(
            "/callback/citadel",
            json={
                "rdoc_job_id": "citadel-fail",
                "file_name": "PKG.pkb",
                "content": "",
                "status": "failed",
                "error": "LLM timeout",
            },
        )
    assert resp.status_code == 200

    # 비동기 처리가 완료될 때까지 잠시 대기
    import asyncio
    await asyncio.sleep(0.05)
    updated = await store.get_file(f.file_id)
    assert updated.external_status == "failed"
    assert updated.error == "LLM timeout"

@pytest.mark.asyncio
async def test_callback_citadel_unknown_job(app_instance):
    """알 수 없는 rdoc_job_id — 200 반환 (fire-and-forget 패턴 유지)."""
    async with AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://test") as client:
        resp = await client.post(
            "/callback/citadel",
            json={
                "rdoc_job_id": "unknown-id",
                "file_name": "X.pkb",
                "content": "",
                "status": "completed",
                "error": None,
            },
        )
    assert resp.status_code == 200

@pytest.mark.asyncio
async def test_graphify_rebuild(app_instance):
    """POST /ingest/graphify-rebuild — Nexus rebuild 트리거."""
    import respx
    import httpx
    async with respx.mock:
        respx.post("http://nexus:8005/rebuild/").mock(
            return_value=httpx.Response(200, json={"status": "ok", "message": "Rebuild started"})
        )
        async with AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://test") as client:
            resp = await client.post("/ingest/graphify-rebuild")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"

@pytest.mark.asyncio
async def test_webhook_all_skip_completes_job(app_instance):
    """모든 파일이 skip/code인 webhook job이 자동으로 completed 처리되는지 검증."""
    payload = json.dumps({
        "pullrequest": {"id": 5, "source": {"commit": {"hash": "def"}}},
        "repository": {"full_name": "hcs/GCore"},
        "changes": [
            {"path": {"toString": "image.png"}, "type": "added"},
            {"path": {"toString": "Main.java"}, "type": "added"},
        ],
    }).encode()
    sig = "sha256=" + hmac.new(b"test-secret", payload, hashlib.sha256).hexdigest()

    async with AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://test") as client:
        resp = await client.post(
            "/webhook/bitbucket",
            content=payload,
            headers={"X-Hub-Signature": sig, "Content-Type": "application/json"},
        )
    assert resp.status_code == 202

    import asyncio
    await asyncio.sleep(0.05)

    job_id = resp.json()["job_id"]
    job = await app_instance.state.store.get_job(job_id)
    assert job.status == "completed"
```

- [ ] **Step 2: 테스트 실행 — FAIL 확인**

```bash
python -m pytest tests/test_app.py::test_callback_citadel_completed -v
```
Expected: `FAILED` — 404 또는 엔드포인트 미존재

- [ ] **Step 3: app.py 전체 교체**

```python
# app.py
import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Request

from config import Config
from ingest import CitadelClient, ForgeClient, LightRAGClient, NexusClient, classify_file, advance_pipeline, _maybe_close_job
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

    app = FastAPI(title="HCS Ingestion Router", version="0.2.0", lifespan=lifespan)
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

        # all-skip 버그 수정: 처리할 파일이 없으면 즉시 job 완료
        asyncio.create_task(_safe_process(_maybe_close_job(job.job_id, current_store)))

        return {"job_id": job.job_id, "status": "processing", "file_count": len(parsed["files"])}

    @app.post("/callback/forge")
    async def forge_callback(request: Request):
        body = await request.json()
        external_job_id = body.get("job_id", "")
        asyncio.create_task(_safe_process(
            advance_pipeline(
                external_job_id=external_job_id,
                callback_body=body,
                store=request.app.state.store,
                lightrag=request.app.state.lightrag,
            )
        ))
        return {"received": True}

    @app.post("/callback/citadel")
    async def citadel_callback(request: Request):
        body = await request.json()
        rdoc_job_id = body.get("rdoc_job_id", "")
        # Citadel 콜백 페이로드를 advance_pipeline 형식으로 정규화
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
        # all-skip 버그 수정
        asyncio.create_task(_safe_process(_maybe_close_job(job.job_id, current_store)))
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
```

- [ ] **Step 4: 테스트 실행 — 신규 테스트 통과 확인**

```bash
python -m pytest tests/test_app.py -v
```
Expected: `10 passed`

- [ ] **Step 5: 전체 테스트 실행**

```bash
python -m pytest tests/ -v
```
Expected: `37 passed`

- [ ] **Step 6: Commit**

```bash
git add app.py tests/test_app.py
git commit -m "feat: /callback/citadel, /ingest/graphify-rebuild 추가; all-skip 버그 수정"
```

---

### Task 7: admin.py — 필드명 업데이트

**Files:**
- Modify: `admin.py`

- [ ] **Step 1: admin.py 필드명 참조 수정**

`admin.py`에서 `forge_job_id` → `external_job_id`, `forge_status` → `external_status` 변경:

```python
# admin.py — re_ingest_file 함수 내 수정 부분
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

# retry_job 함수 내 수정 부분
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
```

- [ ] **Step 2: 전체 테스트 실행 — 통과 확인**

```bash
python -m pytest tests/ -v
```
Expected: `37 passed`

- [ ] **Step 3: Commit**

```bash
git add admin.py
git commit -m "refactor: admin.py — forge_job_id→external_job_id, forge_status→external_status 반영"
```

---

### Task 8: CLAUDE.md 현행화

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: CLAUDE.md 수정 — C4 제약 조건 업데이트**

`CLAUDE.md`의 제약 사항 테이블에서 C4 수정:

```markdown
| C4 | Citadel/Forge 콜백은 `external_job_id` 기준으로만 파일 역조회 | file_id는 내부 식별자, external_job_id가 외부 연결 키. |
```

- [ ] **Step 2: CLAUDE.md — 구조 테이블 업데이트**

`models.py` 설명 줄 수정:
```markdown
| `models.py` | IngestionJob, IngestionFile Pydantic 모델 (external_job_id, external_status) |
```

파이프라인 흐름 `[미구현]` 항목 중 일부 반영:
```markdown
  → POST /callback/citadel (역문서화 완료 수신)
  → POST /callback/forge (Forge 문서 변환 완료 수신)
  → advance_pipeline() → LightRAGClient.ingest_text()
```

- [ ] **Step 3: 미완성 작업 테이블 업데이트**

CLAUDE.md의 미완성 작업에서 `🔴 1 (all-skip 버그)` 완료 표시:
```markdown
| ~~🔴 1~~ | ~~all-skip job 자동 완료 버그~~ | ✅ 완료 |
```

- [ ] **Step 4: 최종 전체 테스트 실행**

```bash
python -m pytest tests/ -v
```
Expected: `37 passed`

- [ ] **Step 5: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: CLAUDE.md 현행화 — v2 변경사항 반영 (external_job_id, Citadel, all-skip 완료)"
```

---

## 완료 조건

```bash
# 전체 테스트 통과
python -m pytest tests/ -v
# Expected: 37 passed

# 서버 기동
uvicorn app:app --port 8001
curl http://localhost:8001/health
# Expected: {"status":"ok"}
```

## 구현 순서 요약

| Task | 핵심 변경 | 완료 후 테스트 수 |
|------|-----------|-----------------|
| 1 | config.py 필드 추가 | 28 passed |
| 2 | 필드명 정규화 전체 | 28 passed |
| 3 | classify_file code 타입 | 29 passed |
| 4 | CitadelClient + ForgeClient 정리 | 31 passed |
| 5 | NexusClient | 32 passed |
| 6 | app.py 새 엔드포인트 + 버그 수정 | 37 passed |
| 7 | admin.py 필드명 반영 | 37 passed |
| 8 | CLAUDE.md 현행화 | 37 passed |

# HCS Ingestion Router Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bitbucket PR Merge 이벤트를 수신해 파일별로 Forge → LightRAG 파이프라인을 구동하고 전 단계 상태를 DB에 추적하는 FastAPI 오케스트레이터 서비스 구축.

**Architecture:** 비동기 FastAPI 서비스(포트 8001). Forge는 `callback_url` 패턴으로 비동기 연결. Forge 콜백 수신 시 LightRAG ingest 자동 트리거. 모든 상태 전환은 `ingestion_job` + `ingestion_file` 테이블에 기록. InMemoryJobStore(개발/테스트) ↔ PostgresJobStore(운영) 교체 가능.

**Tech Stack:** Python 3.11+, FastAPI, asyncpg, httpx(async), pydantic-settings, pytest + pytest-asyncio

---

## 파일 책임

| 파일 | 책임 |
|------|------|
| `config.py` | 환경변수 로드 (pydantic-settings) |
| `models.py` | IngestionJob, IngestionFile Pydantic 모델 |
| `job_store.py` | InMemoryJobStore + PostgresJobStore (DB 접근 추상화) |
| `ingest.py` | ForgeClient, LightRAGClient, classify_file, advance_pipeline |
| `webhook.py` | HMAC 검증 + Bitbucket payload 파싱 (교체 가능 모듈) |
| `admin.py` | 모니터링 API 라우터 (GET jobs/files/stats, POST reject/re-ingest/retry) |
| `app.py` | FastAPI 앱 + lifespan + webhook/callback/bulk 라우트 |
| `schema.sql` | DDL (IF NOT EXISTS, idempotent) |

---

### Task 1: Config + Models

**Files:**
- Modify: `config.py`
- Modify: `models.py`
- Create: `tests/__init__.py`
- Create: `tests/test_models.py`
- Create: `pytest.ini`

- [ ] **Step 1: tests 디렉토리 생성 + pytest 설정**

```bash
cd /c/workspace/hcs-ingestion-router
mkdir -p tests
touch tests/__init__.py
```

```ini
# pytest.ini
[pytest]
asyncio_mode = auto
```

requirements.txt 끝에 추가:
```
pytest>=8.0.0
pytest-asyncio>=0.23.0
```

```bash
pip install -r requirements.txt
```

- [ ] **Step 2: 모델 테스트 작성**

```python
# tests/test_models.py
from models import IngestionJob, IngestionFile

def test_ingestion_job_defaults():
    job = IngestionJob(job_id="abc", source_type="webhook", status="created")
    assert job.status == "created"
    assert job.file_count == 0
    assert job.pr_number is None

def test_ingestion_file_defaults():
    f = IngestionFile(file_id="xyz", job_id="abc", file_path="src/PKG_LOAN.pkb", file_type="plsql")
    assert f.forge_status == "queued"
    assert f.rag_status == "pending"
    assert f.review_status == "auto_approved"
```

- [ ] **Step 3: 테스트 실패 확인**

```bash
pytest tests/test_models.py -v
```
Expected: `ImportError` — models.py 미구현

- [ ] **Step 4: config.py 구현**

```python
# config.py
from pydantic_settings import BaseSettings

class Config(BaseSettings):
    forge_url: str = "http://localhost:8003"
    lightrag_url: str = "http://localhost:9621"
    database_url: str = ""
    bitbucket_webhook_secret: str = ""
    forge_api_key: str = ""
    lightrag_api_key: str = ""
    port: int = 8001
    self_url: str = "http://localhost:8001"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}
```

- [ ] **Step 5: models.py 구현**

```python
# models.py
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
    file_type: str              # 'plsql' | 'document' | 'skip'
    forge_job_id: Optional[str] = None
    forge_status: str = "queued"       # queued | forging | done | skipped | failed
    rag_status: str = "pending"        # pending | ingesting | ingested | failed
    review_status: str = "auto_approved"  # auto_approved | rejected
    error: Optional[str] = None
    created_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
```

- [ ] **Step 6: 테스트 통과 확인**

```bash
pytest tests/test_models.py -v
```
Expected: 2 passed

- [ ] **Step 7: 커밋**

```bash
git add config.py models.py tests/ pytest.ini requirements.txt
git commit -m "feat: config + pydantic models"
```

---

### Task 2: InMemoryJobStore

**Files:**
- Modify: `job_store.py`
- Create: `tests/test_job_store.py`

- [ ] **Step 1: JobStore 테스트 작성**

```python
# tests/test_job_store.py
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
    assert f.forge_status == "queued"
    await store.update_file(f.file_id, forge_status="done", forge_job_id="forge-xyz")
    updated = await store.get_file(f.file_id)
    assert updated.forge_status == "done"
    assert updated.forge_job_id == "forge-xyz"

@pytest.mark.asyncio
async def test_get_file_by_forge_job_id():
    store = InMemoryJobStore()
    job = await store.create_job(source_type="webhook", repo="GCore")
    f = await store.create_file(job_id=job.job_id, file_path="src/PKG.pkb", file_type="plsql")
    await store.update_file(f.file_id, forge_job_id="forge-xyz")
    result = await store.get_file_by_forge_job_id("forge-xyz")
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

- [ ] **Step 2: 테스트 실패 확인**

```bash
pytest tests/test_job_store.py -v
```
Expected: FAIL

- [ ] **Step 3: job_store.py 구현**

```python
# job_store.py
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

    async def get_file_by_forge_job_id(self, forge_job_id: str) -> Optional[IngestionFile]:
        for f in self._files.values():
            if f.forge_job_id == forge_job_id:
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
        failed_files = [f for f in files if f.forge_status == "failed" or f.rag_status == "failed"]
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
                 "forge_status": f.forge_status, "rag_status": f.rag_status, "error": f.error}
                for f in failed_files[-10:]
            ],
        }
```

- [ ] **Step 4: 테스트 통과 확인**

```bash
pytest tests/test_job_store.py -v
```
Expected: 5 passed

- [ ] **Step 5: 커밋**

```bash
git add job_store.py tests/test_job_store.py pytest.ini
git commit -m "feat: InMemoryJobStore + tests"
```

---

### Task 3: ForgeClient + LightRAGClient + 파일 분류

**Files:**
- Modify: `ingest.py`
- Create: `tests/test_ingest.py`

- [ ] **Step 1: 클라이언트 테스트 작성**

```python
# tests/test_ingest.py
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from ingest import classify_file, ForgeClient, LightRAGClient

def test_classify_plsql():
    assert classify_file("PKG_LOAN_CALC.pkb") == "plsql"
    assert classify_file("SP_GET_LOAN.pks") == "plsql"
    assert classify_file("proc.sql") == "plsql"
    assert classify_file("func.fnc") == "plsql"
    assert classify_file("proc.prc") == "plsql"

def test_classify_document():
    assert classify_file("design.pdf") == "document"
    assert classify_file("spec.docx") == "document"
    assert classify_file("slides.pptx") == "document"
    assert classify_file("data.xlsx") == "document"
    assert classify_file("readme.md") == "document"

def test_classify_skip():
    assert classify_file("image.png") == "skip"
    assert classify_file("data.json") == "skip"
    assert classify_file("config.yaml") == "skip"
    assert classify_file("noextension") == "skip"

@pytest.mark.asyncio
async def test_forge_convert_plsql():
    client = ForgeClient(base_url="http://forge:8003", api_key="test-key")
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json = MagicMock(return_value={"job_id": "forge-123", "status": "queued"})
    with patch.object(client._client, "post", new=AsyncMock(return_value=mock_resp)) as mock_post:
        result = await client.convert(
            file_bytes=b"BEGIN NULL; END;",
            file_name="PKG.pkb",
            file_type="plsql",
            callback_url="http://router:8001/callback/forge?file_id=abc",
        )
        assert mock_post.call_args[0][0] == "/reverse-doc"
    assert result["job_id"] == "forge-123"
    await client.close()
```

- [ ] **Step 2: 테스트 실패 확인**

```bash
pytest tests/test_ingest.py -v
```
Expected: FAIL

- [ ] **Step 3: ingest.py 구현**

```python
# ingest.py
import logging
from datetime import datetime, timezone
from typing import Optional
import httpx

logger = logging.getLogger(__name__)

PLSQL_EXTS = {".pkb", ".pks", ".sql", ".prc", ".fnc"}
DOCUMENT_EXTS = {".pdf", ".docx", ".pptx", ".xlsx", ".md", ".txt", ".hwpx"}

def classify_file(file_path: str) -> str:
    if "." not in file_path:
        return "skip"
    ext = "." + file_path.rsplit(".", 1)[-1].lower()
    if ext in PLSQL_EXTS:
        return "plsql"
    if ext in DOCUMENT_EXTS:
        return "document"
    return "skip"

class ForgeClient:
    def __init__(self, base_url: str, api_key: str):
        self._client = httpx.AsyncClient(
            base_url=base_url,
            headers={"X-Forge-Key": api_key},
            timeout=30.0,
        )

    async def convert(self, file_bytes: bytes, file_name: str, file_type: str, callback_url: str) -> dict:
        if file_type == "plsql":
            resp = await self._client.post(
                "/reverse-doc",
                files={"file": (file_name, file_bytes, "text/plain")},
                data={"callback_url": callback_url, "requested_by": "ingestion-router"},
            )
        else:
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

class LightRAGClient:
    def __init__(self, base_url: str, api_key: str):
        self._client = httpx.AsyncClient(
            base_url=base_url,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=60.0,
        )

    async def ingest_text(self, content: str, metadata: dict) -> dict:
        resp = await self._client.post(
            "/documents/text",
            json={"content": content, "metadata": metadata},
        )
        resp.raise_for_status()
        return resp.json()

    async def close(self):
        await self._client.aclose()

async def advance_pipeline(forge_job_id: str, callback_body: dict, store, lightrag: LightRAGClient):
    """Forge callback 수신 시 파이프라인 다음 단계 진행."""
    f = await store.get_file_by_forge_job_id(forge_job_id)
    if f is None:
        logger.warning("Unknown forge_job_id in callback: %s", forge_job_id)
        return

    if callback_body.get("status") != "completed":
        error_msg = callback_body.get("error", "forge failed")
        await store.update_file(f.file_id, forge_status="failed", error=error_msg)
        await _maybe_close_job(f.job_id, store)
        return

    await store.update_file(f.file_id, forge_status="done", rag_status="ingesting")

    result_text = (callback_body.get("result") or {}).get("text", "")
    try:
        await lightrag.ingest_text(
            content=result_text,
            metadata={
                "file_id": f.file_id,
                "job_id": f.job_id,
                "file_path": f.file_path,
                "forge_job_id": forge_job_id,
            },
        )
        await store.update_file(f.file_id, rag_status="ingested", completed_at=datetime.now(timezone.utc))
    except Exception as e:
        logger.error("LightRAG ingest failed for file %s: %s", f.file_id, e)
        await store.update_file(f.file_id, rag_status="failed", error=str(e))

    await _maybe_close_job(f.job_id, store)

async def _maybe_close_job(job_id: str, store):
    files = await store.list_files_for_job(job_id)
    if not files:
        return
    forge_terminal = {"done", "skipped", "failed"}
    if not all(f.forge_status in forge_terminal for f in files):
        return
    rag_terminal = {"ingested", "failed", "pending"}
    if not all(f.rag_status in rag_terminal for f in files):
        return

    success = sum(1 for f in files if f.rag_status == "ingested" or f.file_type == "skip")
    total = len(files)
    if success == total:
        status = "completed"
    elif success > 0:
        status = "partial"
    else:
        status = "failed"

    await store.update_job(job_id, status=status, completed_at=datetime.now(timezone.utc))
```

- [ ] **Step 4: 테스트 통과 확인**

```bash
pytest tests/test_ingest.py -v
```
Expected: 4 passed

- [ ] **Step 5: pipeline advancement 테스트 추가**

```python
# tests/test_pipeline.py
import pytest
from unittest.mock import AsyncMock
from job_store import InMemoryJobStore
from ingest import advance_pipeline

@pytest.mark.asyncio
async def test_advance_on_forge_success():
    store = InMemoryJobStore()
    job = await store.create_job(source_type="webhook", repo="GCore")
    f = await store.create_file(job_id=job.job_id, file_path="PKG.pkb", file_type="plsql")
    await store.update_file(f.file_id, forge_job_id="forge-123", forge_status="forging")

    lightrag = AsyncMock()
    lightrag.ingest_text = AsyncMock(return_value={"status": "ok"})

    await advance_pipeline(
        forge_job_id="forge-123",
        callback_body={"job_id": "forge-123", "status": "completed", "result": {"text": "# PKG\n내용"}},
        store=store,
        lightrag=lightrag,
    )

    updated = await store.get_file(f.file_id)
    assert updated.forge_status == "done"
    assert updated.rag_status == "ingested"
    lightrag.ingest_text.assert_called_once()

@pytest.mark.asyncio
async def test_advance_on_forge_failure():
    store = InMemoryJobStore()
    job = await store.create_job(source_type="webhook", repo="GCore")
    f = await store.create_file(job_id=job.job_id, file_path="PKG.pkb", file_type="plsql")
    await store.update_file(f.file_id, forge_job_id="forge-999", forge_status="forging")

    lightrag = AsyncMock()

    await advance_pipeline(
        forge_job_id="forge-999",
        callback_body={"job_id": "forge-999", "status": "failed", "error": "VLM timeout"},
        store=store,
        lightrag=lightrag,
    )

    updated = await store.get_file(f.file_id)
    assert updated.forge_status == "failed"
    assert updated.error == "VLM timeout"
    lightrag.ingest_text.assert_not_called()

@pytest.mark.asyncio
async def test_job_completed_when_all_done():
    store = InMemoryJobStore()
    job = await store.create_job(source_type="webhook", repo="GCore")
    f1 = await store.create_file(job_id=job.job_id, file_path="A.pkb", file_type="plsql")
    f2 = await store.create_file(job_id=job.job_id, file_path="B.pkb", file_type="plsql")
    await store.update_file(f1.file_id, forge_job_id="forge-1", forge_status="forging")
    await store.update_file(f2.file_id, forge_status="done", rag_status="ingested")

    lightrag = AsyncMock()
    lightrag.ingest_text = AsyncMock(return_value={"status": "ok"})

    await advance_pipeline(
        forge_job_id="forge-1",
        callback_body={"job_id": "forge-1", "status": "completed", "result": {"text": "content"}},
        store=store,
        lightrag=lightrag,
    )

    updated_job = await store.get_job(job.job_id)
    assert updated_job.status == "completed"
```

- [ ] **Step 6: 테스트 통과 확인**

```bash
pytest tests/test_ingest.py tests/test_pipeline.py -v
```
Expected: 7 passed

- [ ] **Step 7: 커밋**

```bash
git add ingest.py tests/test_ingest.py tests/test_pipeline.py
git commit -m "feat: ForgeClient + LightRAGClient + advance_pipeline"
```

---

### Task 4: Webhook HMAC + Bitbucket Payload Parser

**Files:**
- Modify: `webhook.py`
- Create: `tests/test_webhook.py`

- [ ] **Step 1: webhook 테스트 작성**

```python
# tests/test_webhook.py
import hashlib
import hmac
import pytest
from webhook import verify_hmac, parse_bitbucket_payload

def test_hmac_valid():
    secret = "test-secret"
    payload = b'{"key": "value"}'
    sig = "sha256=" + hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()
    assert verify_hmac(payload, sig, secret) is True

def test_hmac_invalid():
    assert verify_hmac(b"payload", "sha256=badsig", "secret") is False

def test_hmac_missing_prefix():
    secret = "s"
    payload = b"p"
    raw_hex = hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()
    assert verify_hmac(payload, raw_hex, secret) is False

def test_parse_payload_extracts_files():
    payload = {
        "pullrequest": {
            "id": 42,
            "source": {"commit": {"hash": "abc123"}},
        },
        "repository": {"full_name": "hcs/GCore"},
        "changes": [
            {"path": {"toString": "src/PKG_LOAN.pkb"}, "type": "modified"},
            {"path": {"toString": "docs/design.docx"}, "type": "added"},
        ],
    }
    result = parse_bitbucket_payload(payload)
    assert result["repo"] == "hcs/GCore"
    assert result["pr_number"] == 42
    assert result["commit_hash"] == "abc123"
    assert "src/PKG_LOAN.pkb" in result["files"]
    assert "docs/design.docx" in result["files"]
```

- [ ] **Step 2: 테스트 실패 확인**

```bash
pytest tests/test_webhook.py -v
```
Expected: FAIL

- [ ] **Step 3: webhook.py 구현**

```python
# webhook.py
import hashlib
import hmac as hmac_lib
import logging

logger = logging.getLogger(__name__)

def verify_hmac(payload: bytes, signature: str, secret: str) -> bool:
    if not signature.startswith("sha256="):
        return False
    expected = "sha256=" + hmac_lib.new(secret.encode(), payload, hashlib.sha256).hexdigest()
    return hmac_lib.compare_digest(expected, signature)

def parse_bitbucket_payload(payload: dict) -> dict:
    """
    Bitbucket PR Merged webhook payload 파싱.
    실제 HCS Bitbucket 환경 payload 구조 확인 후 이 함수만 수정.
    현재 구현: Bitbucket Cloud PR fulfilled 이벤트 구조 기준.
    """
    pr = payload.get("pullrequest", {})
    repo = payload.get("repository", {}).get("full_name", "unknown")
    pr_number = pr.get("id")
    commit_hash = pr.get("source", {}).get("commit", {}).get("hash", "")
    changes = payload.get("changes", [])
    files = [c["path"]["toString"] for c in changes if "path" in c]
    return {
        "repo": repo,
        "pr_number": pr_number,
        "commit_hash": commit_hash,
        "files": files,
    }
```

- [ ] **Step 4: 테스트 통과 확인**

```bash
pytest tests/test_webhook.py -v
```
Expected: 4 passed

- [ ] **Step 5: 커밋**

```bash
git add webhook.py tests/test_webhook.py
git commit -m "feat: HMAC verification + Bitbucket payload parser"
```

---

### Task 5: Admin 모니터링 API

**Files:**
- Modify: `admin.py`
- Create: `tests/test_admin.py`

- [ ] **Step 1: admin 테스트 작성**

```python
# tests/test_admin.py
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
```

- [ ] **Step 2: 테스트 실패 확인**

```bash
pytest tests/test_admin.py -v
```
Expected: FAIL

- [ ] **Step 3: admin.py 구현**

```python
# admin.py
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
        if forge is None or not f.forge_job_id:
            raise HTTPException(status_code=400, detail="Forge job_id not available for re-ingest")
        forge_result = await forge.get_job(f.forge_job_id)
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
        failed = [f for f in files if f.forge_status == "failed" or f.rag_status == "failed"]
        for f in failed:
            await app_state.store.update_file(f.file_id, forge_status="queued", rag_status="pending", error=None)
        return {"retried": len(failed)}

    @router.get("/stats")
    async def stats():
        return await app_state.store.get_stats()

    @router.get("/health/all")
    async def health_all():
        results = {"ingestion_router": "ok"}
        config = getattr(app_state, "config", None)
        if config:
            for name, url in [("forge", config.forge_url), ("lightrag", config.lightrag_url)]:
                try:
                    async with httpx.AsyncClient(timeout=5.0) as c:
                        r = await c.get(f"{url}/health")
                    results[name] = "ok" if r.status_code == 200 else "error"
                except Exception:
                    results[name] = "unreachable"
        return results

    return router
```

- [ ] **Step 4: 테스트 통과 확인**

```bash
pytest tests/test_admin.py -v
```
Expected: 5 passed

- [ ] **Step 5: 커밋**

```bash
git add admin.py tests/test_admin.py
git commit -m "feat: admin monitoring API (jobs/files/stats/health)"
```

---

### Task 6: App 엔트리포인트 + 전체 라우트

**Files:**
- Modify: `app.py`
- Create: `tests/test_app.py`

- [ ] **Step 1: app 통합 테스트 작성**

```python
# tests/test_app.py
import hashlib
import hmac
import json
import pytest
from httpx import AsyncClient, ASGITransport
from app import create_app
from config import Config
from job_store import InMemoryJobStore

@pytest.fixture
def test_config():
    return Config(
        forge_url="http://forge:8003",
        lightrag_url="http://lightrag:9621",
        database_url="",
        bitbucket_webhook_secret="test-secret",
        forge_api_key="key",
        lightrag_api_key="key",
        self_url="http://localhost:8001",
    )

@pytest.fixture
def app_instance(test_config):
    return create_app(store=InMemoryJobStore(), config=test_config)

@pytest.mark.asyncio
async def test_health(app_instance):
    async with AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://test") as client:
        resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}

@pytest.mark.asyncio
async def test_webhook_hmac_failure(app_instance):
    async with AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://test") as client:
        resp = await client.post(
            "/webhook/bitbucket",
            content=b'{"key":"val"}',
            headers={"X-Hub-Signature": "sha256=badsig", "Content-Type": "application/json"},
        )
    assert resp.status_code == 401

@pytest.mark.asyncio
async def test_webhook_valid_creates_job(app_instance):
    payload = json.dumps({
        "pullrequest": {"id": 1, "source": {"commit": {"hash": "abc"}}},
        "repository": {"full_name": "hcs/GCore"},
        "changes": [{"path": {"toString": "src/PKG.pkb"}, "type": "modified"}],
    }).encode()
    sig = "sha256=" + hmac.new(b"test-secret", payload, hashlib.sha256).hexdigest()
    async with AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://test") as client:
        resp = await client.post(
            "/webhook/bitbucket",
            content=payload,
            headers={"X-Hub-Signature": sig, "Content-Type": "application/json"},
        )
    assert resp.status_code == 202
    data = resp.json()
    assert "job_id" in data
    assert data["file_count"] == 1

@pytest.mark.asyncio
async def test_callback_forge_unknown_job(app_instance):
    async with AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://test") as client:
        resp = await client.post(
            "/callback/forge",
            params={"file_id": "nonexistent"},
            json={"job_id": "unknown-forge-id", "status": "completed", "result": {"text": ""}},
        )
    assert resp.status_code == 200
    assert resp.json()["received"] is True

@pytest.mark.asyncio
async def test_bulk_ingest(app_instance):
    async with AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://test") as client:
        resp = await client.post(
            "/ingest/bulk",
            json={"repo": "GCore", "files": ["src/PKG.pkb", "docs/spec.docx", "config.xml"]},
        )
    assert resp.status_code == 200
    data = resp.json()
    assert "job_id" in data
    assert data["file_count"] == 3
```

- [ ] **Step 2: 테스트 실패 확인**

```bash
pytest tests/test_app.py -v
```
Expected: FAIL

- [ ] **Step 3: app.py 구현**

```python
# app.py
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
            # Bitbucket 파일 내용 fetch는 HCS 환경 확인 후 구현
            # parse_bitbucket_payload에서 파일 bytes를 함께 반환하도록 확장 필요
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

    return app

app = create_app()
```

- [ ] **Step 4: 테스트 통과 확인**

```bash
pytest tests/test_app.py -v
```
Expected: 5 passed

- [ ] **Step 5: 전체 테스트 통과 확인**

```bash
pytest tests/ -v
```
Expected: 전체 통과

```bash
uvicorn app:app --port 8001
curl http://localhost:8001/health
```
Expected: `{"status":"ok"}`

- [ ] **Step 6: 커밋**

```bash
git add app.py tests/test_app.py
git commit -m "feat: FastAPI app entrypoint + webhook + callback + bulk routes"
```

---

### Task 7: PostgresJobStore

**Files:**
- Modify: `job_store.py` (PostgresJobStore 추가)

Note: PostgresJobStore는 실제 DB 연결이 필요. 단위 테스트는 InMemoryJobStore로 커버됨. 운영 환경에서 DATABASE_URL 설정 시 자동 전환.

- [ ] **Step 1: PostgresJobStore를 job_store.py에 추가**

job_store.py 하단에 추가:

```python
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

    async def get_file_by_forge_job_id(self, forge_job_id: str) -> Optional[IngestionFile]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM ingestion_file WHERE forge_job_id = $1", forge_job_id)
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
                """SELECT file_id, file_path, job_id, forge_status, rag_status, error
                   FROM ingestion_file WHERE forge_status='failed' OR rag_status='failed'
                   ORDER BY created_at DESC LIMIT 10"""
            )
        return {
            "today": {"jobs": row["total"], "completed": row["completed"], "failed": row["failed"], "partial": row["partial"]},
            "total": {"jobs": total_jobs, "files": total_files},
            "recent_failures": [dict(r) for r in failures],
        }
```

- [ ] **Step 2: 전체 테스트 통과 재확인**

```bash
pytest tests/ -v
```
Expected: 전체 통과 (PostgresJobStore는 DB 없이 InMemory로 동작)

- [ ] **Step 3: 커밋**

```bash
git add job_store.py
git commit -m "feat: PostgresJobStore — production DB store"
```

---

### Task 8: 통합 openapi.json 서빙

**Files:**
- Modify: `app.py` (openapi.json 병합 엔드포인트 추가)

Note: Forge + LightRAG의 `/openapi.json`을 런타임에 fetch해 병합. Claude 크롬 익스텐션이 전체 API 구조를 파악하는 데 사용.

- [ ] **Step 1: app.py에 통합 openapi.json 엔드포인트 추가**

`create_app` 함수의 라우트 정의 부분에 추가:

```python
@app.get("/openapi-all.json", include_in_schema=False)
async def openapi_all(request: Request):
    import httpx
    cfg = request.app.state.config
    merged = app.openapi()  # 자기 자신 스펙

    for name, url in [("forge", cfg.forge_url), ("lightrag", cfg.lightrag_url)]:
        try:
            async with httpx.AsyncClient(timeout=5.0) as c:
                r = await c.get(f"{url}/openapi.json")
            if r.status_code == 200:
                spec = r.json()
                for path, item in spec.get("paths", {}).items():
                    prefixed = f"/{name}{path}"
                    merged.setdefault("paths", {})[prefixed] = item
        except Exception as e:
            logger.warning("Failed to fetch openapi from %s: %s", name, e)

    return merged
```

- [ ] **Step 2: 서버 기동 후 수동 확인**

```bash
uvicorn app:app --port 8001
curl http://localhost:8001/openapi-all.json | python -m json.tool | head -30
```
Expected: paths에 `/forge/...` `/lightrag/...` 경로 포함

- [ ] **Step 3: 커밋**

```bash
git add app.py
git commit -m "feat: /openapi-all.json — merged Forge + LightRAG spec"
```

---

## 셀프 리뷰

**스펙 커버리지 확인:**
- ✅ POST /webhook/bitbucket + HMAC — Task 4, 6
- ✅ POST /ingest/bulk — Task 6
- ✅ POST /callback/forge — Task 6
- ✅ GET /jobs, /jobs/{id}, /jobs/{id}/files, /files/{id} — Task 5
- ✅ GET /stats, /health/all — Task 5
- ✅ POST /files/{id}/reject, /re-ingest, /jobs/{id}/retry — Task 5
- ✅ GET /health — Task 6
- ✅ GET /openapi-all.json — Task 8
- ✅ InMemoryJobStore — Task 2
- ✅ PostgresJobStore — Task 7
- ✅ advance_pipeline — Task 3
- ✅ classify_file — Task 3
- ✅ Bitbucket payload parser 분리 (C4) — Task 4
- ✅ _safe_process 래퍼 (C2) — Task 6
- ⚠️ Bitbucket 파일 내용 fetch: HCS 환경 확인 후 `parse_bitbucket_payload` 확장 필요. `webhook.py`만 수정하면 됨.

# Pylon 정상화 묶음 A 구현 플랜

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Citadel → Robotics 네이밍 교체 + HCA plsql body/header 분기 파이프라인 완성 + admin/callback 버그 수정

**Architecture:** `ingest.py`에 `is_body_file()` + `dispatch_plsql_direct()` 추가. `app.py` plsql dispatch 로직에서 body는 LightRAG 직접(fire-and-forget) + Robotics 제출, header/일반은 LightRAG 직접만. 네이밍은 전 파일 일괄 교체.

**Tech Stack:** Python 3.11, FastAPI, httpx, pytest, respx

---

## 파일 변경 요약

| 파일 | 변경 |
|------|------|
| `config.py` | `citadel_url` / `citadel_api_key` → `robotics_url` / `robotics_api_key` |
| `ingest.py` | `CitadelClient` → `RoboticsClient`, `is_body_file()` 추가, `dispatch_plsql_direct()` 추가, plsql 분기 로직 없음 (app.py에서 제어) |
| `app.py` | citadel → robotics 전체, `/callback/citadel` → `/callback/robotics`, plsql upload 분기 수정, callback 404 수정 |
| `admin.py` | re-ingest plsql → 400 반환 |
| `tests/test_ingest.py` | CitadelClient → RoboticsClient |
| `tests/test_app.py` | citadel → robotics 전체, 신규 테스트 추가 |
| `tests/test_admin.py` | re-ingest plsql 400 테스트 추가 |
| `.env.example` | `CITADEL_*` → `ROBOTICS_*` |
| `CLAUDE.md` | 네이밍 + 파이프라인 흐름 업데이트 |

---

## Task 1: config.py + ingest.py 네이밍 교체

**Files:**
- Modify: `config.py`
- Modify: `ingest.py`

- [ ] **Step 1: config.py — citadel 필드명 교체**

`config.py` 전체를 아래로 교체:

```python
from pydantic_settings import BaseSettings

class Config(BaseSettings):
    forge_url: str = "http://localhost:8003"
    lightrag_url: str = "http://localhost:9621"
    robotics_url: str = "http://localhost:8004"
    nexus_url: str = "http://localhost:8005"
    database_url: str = ""
    bitbucket_webhook_secret: str = ""
    forge_api_key: str = ""
    lightrag_api_key: str = ""
    robotics_api_key: str = ""
    nexus_api_key: str = ""
    port: int = 8001
    self_url: str = "http://localhost:8001"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}
```

- [ ] **Step 2: ingest.py — CitadelClient → RoboticsClient**

`ingest.py`에서 `CitadelClient` 클래스명과 내부 참조만 변경 (로직 무변경):

```python
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
```

`ingest.py` import 줄도 교체:
```python
# 기존
from ingest import CitadelClient, ForgeClient, LightRAGClient, NexusClient, classify_file, advance_pipeline, _maybe_close_job, dispatch_text_doc, dispatch_code_file
# 변경 (app.py에서 사용하는 import — Task 2에서 처리)
```

- [ ] **Step 3: app.py — citadel → robotics 전체 교체**

`app.py`에서 다음 항목 전부 교체:

```python
# import 줄
from ingest import RoboticsClient, ForgeClient, LightRAGClient, NexusClient, classify_file, advance_pipeline, _maybe_close_job, dispatch_text_doc, dispatch_code_file

# lifespan 내
a.state.robotics = RoboticsClient(base_url=config.robotics_url, api_key=config.robotics_api_key)

# lifespan yield 후 close
await a.state.robotics.close()

# app.state 초기값
app.state.robotics = None

# /callback/citadel → /callback/robotics 엔드포인트명 + 경로
@app.post("/callback/robotics")
async def robotics_callback(request: Request):
    body = await request.json()
    rdoc_job_id = body.get("rdoc_job_id", "")
    normalized = {
        "status": body.get("status"),
        "result": {"text": body.get("text") or body.get("content", "")},
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

# ingest_upload 내 plsql 처리
robotics = request.app.state.robotics
# ...
elif file_type == "plsql":
    callback_url = f"{config.self_url}/callback/robotics"
    try:
        result = await robotics.submit(file_bytes, file_name, callback_url)
        ext_id = result.get("rdoc_job_id") or result.get("job_id", "")
        await store.update_file(f.file_id, external_job_id=ext_id, external_status="processing")
    except Exception as e:
        logger.error("Robotics submit failed for %s: %s", file_name, e)
        await store.update_file(f.file_id, external_status="failed", error=str(e))
```

- [ ] **Step 4: 커밋**

```bash
git add config.py ingest.py app.py
git commit -m "refactor: citadel → robotics 네이밍 전체 교체 (config/ingest/app)"
```

---

## Task 2: 테스트 네이밍 교체 + 전체 통과 확인

**Files:**
- Modify: `tests/test_ingest.py`
- Modify: `tests/test_app.py`

- [ ] **Step 1: test_ingest.py — CitadelClient → RoboticsClient**

`tests/test_ingest.py`에서 `CitadelClient` 참조를 모두 `RoboticsClient`로 교체:

```python
@pytest.mark.asyncio
async def test_robotics_submit_success():
    from ingest import RoboticsClient
    client = RoboticsClient(base_url="http://robotics:8004", api_key="test-key")
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json = MagicMock(return_value={"job_id": "robotics-abc", "status": "queued"})
    with patch.object(client._client, "post", new=AsyncMock(return_value=mock_resp)) as mock_post:
        result = await client.submit(
            file_bytes=b"CREATE OR REPLACE PROCEDURE SP_TEST AS BEGIN NULL; END;",
            file_name="SP_TEST.pks",
            callback_url="http://router:8001/callback/robotics",
        )
        call_args = mock_post.call_args
        assert call_args[0][0] == "/jobs"
        assert call_args[1]["data"]["asset_type"] == "plsql"
        assert call_args[1]["data"]["callback_url"] == "http://router:8001/callback/robotics"
    assert result["job_id"] == "robotics-abc"
    await client.close()

@pytest.mark.asyncio
async def test_robotics_submit_http_error():
    from ingest import RoboticsClient
    import httpx
    client = RoboticsClient(base_url="http://robotics:8004", api_key="test-key")
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock(side_effect=httpx.HTTPStatusError("500", request=MagicMock(), response=MagicMock()))
    with patch.object(client._client, "post", new=AsyncMock(return_value=mock_resp)):
        with pytest.raises(httpx.HTTPStatusError):
            await client.submit(b"code", "file.pks", "http://callback")
    await client.close()
```

구 `test_citadel_submit_success`, `test_citadel_submit_http_error` 함수는 삭제.

- [ ] **Step 2: test_app.py — citadel → robotics 전체 교체**

`tests/test_app.py`에서:

1. `test_config` fixture 교체:
```python
@pytest.fixture
def test_config():
    return Config(
        forge_url="http://forge:8003",
        lightrag_url="http://lightrag:9621",
        robotics_url="http://robotics:8004",
        nexus_url="http://nexus:8005",
        database_url="",
        bitbucket_webhook_secret="test-secret",
        forge_api_key="key",
        lightrag_api_key="key",
        robotics_api_key="key",
        nexus_api_key="key",
        self_url="http://localhost:8001",
    )
```

2. 테스트 함수명 및 경로 교체:
- `test_callback_citadel_completed` → `test_callback_robotics_completed` (경로: `/callback/robotics`)
- `test_callback_citadel_failed` → `test_callback_robotics_failed`
- `test_callback_citadel_unknown_job` → `test_callback_robotics_unknown_job` (경로: `/callback/robotics`)

각 테스트 내 `/callback/citadel` → `/callback/robotics` 교체.

- [ ] **Step 3: 전체 테스트 통과 확인**

```bash
cd C:\workspace\Pylon
python -m pytest tests/ -v
```

Expected: 모든 테스트 PASS (42개 내외). 실패 시 에러 메시지 보고 수정.

- [ ] **Step 4: 커밋**

```bash
git add tests/test_ingest.py tests/test_app.py
git commit -m "refactor: 테스트 citadel → robotics 네이밍 교체"
```

---

## Task 3: is_body_file() + dispatch_plsql_direct() 추가

**Files:**
- Modify: `ingest.py`
- Modify: `tests/test_ingest.py`

- [ ] **Step 1: 실패 테스트 작성**

`tests/test_ingest.py`에 추가:

```python
def test_is_body_file_true():
    from ingest import is_body_file
    assert is_body_file("PG_BATCHEOTBALLLEAACC_body.sql") is True
    assert is_body_file("PKG_LOAN_BODY.pkb") is True
    assert is_body_file("SP_PROC_BODY.pks") is True
    assert is_body_file("SOME_PACKAGE_BODY.sql") is True

def test_is_body_file_false():
    from ingest import is_body_file
    assert is_body_file("PG_BATCHEOTBALLLEAACC_header.sql") is False
    assert is_body_file("PKG_LOAN.pkb") is False
    assert is_body_file("SP_PROC.sql") is False
    assert is_body_file("PG_BATCH_HEADER.pks") is False

@pytest.mark.asyncio
async def test_dispatch_plsql_direct_success():
    from ingest import dispatch_plsql_direct
    from job_store import InMemoryJobStore
    store = InMemoryJobStore()
    job = await store.create_job(source_type="upload")
    f = await store.create_file(job_id=job.job_id, file_path="PKG_header.sql", file_type="plsql")

    lightrag = AsyncMock()
    lightrag.ingest_text = AsyncMock(return_value={"status": "ok"})

    await dispatch_plsql_direct(f.file_id, job.job_id, b"CREATE OR REPLACE PACKAGE PKG AS END;", "PKG_header.sql", store, lightrag)

    lightrag.ingest_text.assert_called_once()
    updated = await store.get_file(f.file_id)
    assert updated.external_status == "done"
    assert updated.rag_status == "ingested"

@pytest.mark.asyncio
async def test_dispatch_plsql_direct_no_status_update():
    from ingest import dispatch_plsql_direct
    from job_store import InMemoryJobStore
    store = InMemoryJobStore()
    job = await store.create_job(source_type="upload")
    f = await store.create_file(job_id=job.job_id, file_path="PKG_body.sql", file_type="plsql")

    lightrag = AsyncMock()
    lightrag.ingest_text = AsyncMock(return_value={"status": "ok"})

    await dispatch_plsql_direct(f.file_id, job.job_id, b"CREATE OR REPLACE PACKAGE BODY PKG AS END;", "PKG_body.sql", store, lightrag, update_status=False)

    lightrag.ingest_text.assert_called_once()
    updated = await store.get_file(f.file_id)
    # 상태 변경 없음
    assert updated.external_status == "queued"
    assert updated.rag_status == "pending"
```

- [ ] **Step 2: 테스트 실패 확인**

```bash
python -m pytest tests/test_ingest.py::test_is_body_file_true tests/test_ingest.py::test_is_body_file_false tests/test_ingest.py::test_dispatch_plsql_direct_success tests/test_ingest.py::test_dispatch_plsql_direct_no_status_update -v
```

Expected: FAIL with `ImportError: cannot import name 'is_body_file'`

- [ ] **Step 3: ingest.py에 is_body_file() + dispatch_plsql_direct() 구현**

`ingest.py`에서 `classify_file()` 함수 바로 아래에 추가:

```python
def is_body_file(filename: str) -> bool:
    return "_body" in filename.lower()
```

`dispatch_text_doc()` 아래에 추가:

```python
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
```

- [ ] **Step 4: 테스트 통과 확인**

```bash
python -m pytest tests/test_ingest.py -v
```

Expected: 전체 PASS

- [ ] **Step 5: 커밋**

```bash
git add ingest.py tests/test_ingest.py
git commit -m "feat: is_body_file() + dispatch_plsql_direct() 추가"
```

---

## Task 4: plsql upload 분기 수정

**Files:**
- Modify: `app.py`
- Modify: `tests/test_app.py`

- [ ] **Step 1: 실패 테스트 작성**

`tests/test_app.py`에 추가:

```python
@pytest.mark.asyncio
async def test_ingest_upload_plsql_body_dispatches_to_robotics_and_lightrag(app_instance, test_config):
    """body 파일 → Robotics 제출 + LightRAG 직접 ingest 둘 다 호출."""
    import asyncio
    import respx
    import httpx
    from ingest import RoboticsClient, LightRAGClient

    app_instance.state.robotics = RoboticsClient(base_url=test_config.robotics_url, api_key=test_config.robotics_api_key)
    app_instance.state.lightrag = LightRAGClient(base_url=test_config.lightrag_url, api_key=test_config.lightrag_api_key)

    async with respx.mock:
        respx.post("http://robotics:8004/jobs").mock(
            return_value=httpx.Response(200, json={"rdoc_job_id": "robotics-body-1", "status": "queued"})
        )
        respx.post("http://lightrag:9621/documents/text").mock(
            return_value=httpx.Response(200, json={"status": "ok"})
        )
        async with AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://test") as client:
            resp = await client.post(
                "/ingest/upload",
                files=[("files", ("PKG_body.sql", b"CREATE OR REPLACE PACKAGE BODY PKG AS END;", "text/plain"))],
            )
        assert resp.status_code == 202
        await asyncio.sleep(0.1)

    store = app_instance.state.store
    data = resp.json()
    files = await store.list_files_for_job(data["job_id"])
    assert len(files) == 1
    # Robotics 제출로 external_status='processing'
    assert files[0].external_status == "processing"
    assert files[0].external_job_id == "robotics-body-1"

    await app_instance.state.robotics.close()
    await app_instance.state.lightrag.close()


@pytest.mark.asyncio
async def test_ingest_upload_plsql_header_dispatches_to_lightrag_only(app_instance, test_config):
    """header 파일 → LightRAG 직접 ingest만, Robotics 호출 없음."""
    import asyncio
    import respx
    import httpx
    from ingest import LightRAGClient

    app_instance.state.lightrag = LightRAGClient(base_url=test_config.lightrag_url, api_key=test_config.lightrag_api_key)

    async with respx.mock:
        lightrag_mock = respx.post("http://lightrag:9621/documents/text").mock(
            return_value=httpx.Response(200, json={"status": "ok"})
        )
        async with AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://test") as client:
            resp = await client.post(
                "/ingest/upload",
                files=[("files", ("PKG_header.sql", b"CREATE OR REPLACE PACKAGE PKG AS END;", "text/plain"))],
            )
        assert resp.status_code == 202
        await asyncio.sleep(0.1)

    assert lightrag_mock.called
    store = app_instance.state.store
    data = resp.json()
    files = await store.list_files_for_job(data["job_id"])
    assert files[0].rag_status == "ingested"

    await app_instance.state.lightrag.close()
```

- [ ] **Step 2: 테스트 실패 확인**

```bash
python -m pytest tests/test_app.py::test_ingest_upload_plsql_body_dispatches_to_robotics_and_lightrag tests/test_app.py::test_ingest_upload_plsql_header_dispatches_to_lightrag_only -v
```

Expected: FAIL (현재 분기 로직 없음)

- [ ] **Step 3: app.py plsql 분기 수정**

`app.py`에서 `ingest_upload` 함수 상단 import 줄 교체:

```python
from ingest import RoboticsClient, ForgeClient, LightRAGClient, NexusClient, classify_file, is_body_file, advance_pipeline, _maybe_close_job, dispatch_text_doc, dispatch_code_file, dispatch_plsql_direct
```

`ingest_upload` 내 plsql 처리 블록 교체:

```python
elif file_type == "plsql":
    if is_body_file(file_name):
        # 원문 → LightRAG 직접 (fire-and-forget, 상태 미업데이트)
        asyncio.create_task(_safe_process(
            dispatch_plsql_direct(f.file_id, job.job_id, file_bytes, file_name, store, request.app.state.lightrag, update_status=False)
        ))
        # REVDOC → Robotics 제출 (상태 추적 primary)
        callback_url = f"{config.self_url}/callback/robotics"
        try:
            result = await robotics.submit(file_bytes, file_name, callback_url)
            ext_id = result.get("rdoc_job_id") or result.get("job_id", "")
            await store.update_file(f.file_id, external_job_id=ext_id, external_status="processing")
        except Exception as e:
            logger.error("Robotics submit failed for %s: %s", file_name, e)
            await store.update_file(f.file_id, external_status="failed", error=str(e))
    else:
        # header / 패턴 없는 sql → LightRAG 직접 ingest (상태 업데이트 포함)
        asyncio.create_task(_safe_process(
            dispatch_plsql_direct(f.file_id, job.job_id, file_bytes, file_name, store, request.app.state.lightrag)
        ))
```

- [ ] **Step 4: 테스트 통과 확인**

```bash
python -m pytest tests/test_app.py -v
```

Expected: 전체 PASS

- [ ] **Step 5: 커밋**

```bash
git add app.py tests/test_app.py
git commit -m "feat: plsql body/header 분기 파이프라인 구현"
```

---

## Task 5: admin re-ingest plsql → 400 수정

**Files:**
- Modify: `admin.py`
- Modify: `tests/test_admin.py`

- [ ] **Step 1: 실패 테스트 작성**

`tests/test_admin.py`에 추가:

```python
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
```

- [ ] **Step 2: 테스트 실패 확인**

```bash
python -m pytest tests/test_admin.py::test_re_ingest_plsql_returns_400 -v
```

Expected: FAIL (현재 400이 아닌 다른 에러 또는 정상 흐름)

- [ ] **Step 3: admin.py re-ingest 수정**

`admin.py`의 `re_ingest_file` 함수에서 `forge` 체크 전에 `file_type` 분기 추가:

```python
@router.post("/files/{file_id}/re-ingest")
async def re_ingest_file(file_id: str):
    f = await app_state.store.get_file(file_id)
    if f is None:
        raise HTTPException(status_code=404, detail="File not found")

    if f.file_type == "plsql":
        raise HTTPException(
            status_code=400,
            detail="plsql 파일은 원본 bytes가 없어 re-ingest 불가. 파일을 다시 업로드하세요.",
        )

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
```

- [ ] **Step 4: 테스트 통과 확인**

```bash
python -m pytest tests/test_admin.py -v
```

Expected: 전체 PASS

- [ ] **Step 5: 커밋**

```bash
git add admin.py tests/test_admin.py
git commit -m "fix: admin re-ingest plsql 파일 400 반환 (원본 bytes 없음)"
```

---

## Task 6: /callback/robotics 알 수 없는 ID → 404

**Files:**
- Modify: `app.py`
- Modify: `tests/test_app.py`

- [ ] **Step 1: 실패 테스트 작성**

`tests/test_app.py`에서 `test_callback_robotics_unknown_job` 테스트 수정 (Task 2에서 이름만 바꾼 버전을 404로 변경):

```python
@pytest.mark.asyncio
async def test_callback_robotics_unknown_job(app_instance):
    """알 수 없는 rdoc_job_id → 404 반환."""
    async with AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://test") as client:
        resp = await client.post(
            "/callback/robotics",
            json={
                "rdoc_job_id": "unknown-id",
                "file_name": "X.pkb",
                "content": "",
                "status": "completed",
                "error": None,
            },
        )
    assert resp.status_code == 404
```

- [ ] **Step 2: 테스트 실패 확인**

```bash
python -m pytest tests/test_app.py::test_callback_robotics_unknown_job -v
```

Expected: FAIL (현재 200 반환)

- [ ] **Step 3: app.py /callback/robotics 수정**

`app.py`의 `robotics_callback`에서 `asyncio.create_task` 전에 파일 존재 확인 추가:

```python
@app.post("/callback/robotics")
async def robotics_callback(request: Request):
    body = await request.json()
    rdoc_job_id = body.get("rdoc_job_id", "")
    normalized = {
        "status": body.get("status"),
        "result": {"text": body.get("text") or body.get("content", "")},
        "error": body.get("error"),
    }
    files = await request.app.state.store.get_files_by_external_job_id(rdoc_job_id)
    if not files:
        raise HTTPException(status_code=404, detail=f"Unknown rdoc_job_id: {rdoc_job_id}")
    asyncio.create_task(_safe_process(
        advance_pipeline(
            external_job_id=rdoc_job_id,
            callback_body=normalized,
            store=request.app.state.store,
            lightrag=request.app.state.lightrag,
        )
    ))
    return {"received": True}
```

- [ ] **Step 4: 전체 테스트 통과 확인**

```bash
python -m pytest tests/ -v
```

Expected: 전체 PASS

- [ ] **Step 5: 커밋**

```bash
git add app.py tests/test_app.py
git commit -m "fix: /callback/robotics 알 수 없는 rdoc_job_id → 404 반환"
```

---

## Task 7: .env.example + CLAUDE.md 업데이트

**Files:**
- Modify: `.env.example`
- Modify: `CLAUDE.md`

- [ ] **Step 1: .env.example 업데이트**

`.env.example` 전체 교체:

```
LIGHTRAG_URL=http://193.168.195.222:9621
FORGE_URL=http://193.168.195.222:8003
ROBOTICS_URL=http://193.168.195.222:8004
NEXUS_URL=http://193.168.195.222:8005
SELF_URL=http://193.168.195.222:8001
DATABASE_URL=
BITBUCKET_WEBHOOK_SECRET=
FORGE_API_KEY=
LIGHTRAG_API_KEY=
ROBOTICS_API_KEY=
NEXUS_API_KEY=
PORT=8001
```

- [ ] **Step 2: CLAUDE.md 파일 유형 분기 테이블 업데이트**

`CLAUDE.md`의 `# 파일 유형 분기` 섹션을 아래로 교체:

```markdown
# 파일 유형 분기

```
확장자                           file_type   라우팅
.pkb .pks .sql .prc .fnc (_body 포함)  → plsql   Robotics POST /jobs + LightRAG 직접 (fire-and-forget)
.pkb .pks .sql .prc .fnc (그 외)       → plsql   LightRAG 직접 ingest
.pdf .docx .pptx .xlsx .hwpx        → document Forge POST /convert
.md .txt                            → text_doc LightRAG 직접 ingest
.java .js .ts .jsx .tsx .py         → code     /ingest/upload → Nexus POST /rebuild/upload
그 외                               → skip     skipped 기록만
```
```

`# 파이프라인 흐름` 섹션 업데이트:

```markdown
# 파이프라인 흐름

```
[직접 파일 업로드] POST /ingest/upload (multipart files)
  → 파일별 classify_file() + is_body_file()
  → plsql (_body 포함) → dispatch_plsql_direct(update_status=False) fire-and-forget → LightRAG 원문
                       → RoboticsClient.submit() → POST /callback/robotics → advance_pipeline() → LightRAG REVDOC
  → plsql (그 외)     → dispatch_plsql_direct() → LightRAG 원문
  → document          → ForgeClient.convert() → POST /callback/forge → advance_pipeline() → LightRAG
  → text_doc          → dispatch_text_doc() → LightRAG
  → code              → dispatch_code_file() → Nexus
  → skip              → skipped 기록만

[Bitbucket Webhook] POST /webhook/bitbucket
  → Bitbucket API로 파일 bytes fetch  ← [미구현 — 묶음 B]
```
```

`# 제약 사항` 테이블의 `CITADEL_URL` → `ROBOTICS_URL` 교체.

- [ ] **Step 3: 전체 테스트 최종 확인**

```bash
python -m pytest tests/ -v
```

Expected: 전체 PASS

- [ ] **Step 4: 최종 커밋**

```bash
git add .env.example CLAUDE.md
git commit -m "docs: .env.example ROBOTICS_* 교체 + CLAUDE.md 파이프라인 흐름 업데이트"
```

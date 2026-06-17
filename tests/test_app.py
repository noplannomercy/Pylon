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
        robotics_url="http://robotics:8004",
        nexus_url="http://nexus:8005",
        database_url="",
        bitbucket_webhook_secret="test-secret",
        callback_secret="cb-secret",
        forge_api_key="key",
        lightrag_api_key="key",
        robotics_api_key="key",
        nexus_api_key="key",
        self_url="http://localhost:8001",
        max_upload_files=5,
        max_upload_bytes=1000,
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
async def test_webhook_rejects_when_secret_not_configured():
    """시크릿 미설정 시 검증 불가 → fail-closed 거부 (#4). 예전엔 검증 스킵하고 통과."""
    config = Config(
        forge_url="http://forge:8003", lightrag_url="http://lightrag:9621",
        robotics_url="http://robotics:8004", nexus_url="http://nexus:8005",
        database_url="", bitbucket_webhook_secret="",
        self_url="http://localhost:8001",
    )
    app = create_app(store=InMemoryJobStore(), config=config)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post(
            "/webhook/bitbucket",
            content=b'{"key":"val"}',
            headers={"X-Hub-Signature": "sha256=anything", "Content-Type": "application/json"},
        )
    assert resp.status_code == 503

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
async def test_webhook_redelivery_idempotent(app_instance):
    """같은 webhook 재전송 시 동일 job 재사용 + 파일 중복 생성 안 함 (#7)."""
    payload = json.dumps({
        "pullrequest": {"id": 9, "source": {"commit": {"hash": "def9"}}},
        "repository": {"full_name": "hcs/GCore"},
        "changes": [{"path": {"toString": "src/PKG.pkb"}, "type": "modified"}],
    }).encode()
    sig = "sha256=" + hmac.new(b"test-secret", payload, hashlib.sha256).hexdigest()
    headers = {"X-Hub-Signature": sig, "Content-Type": "application/json"}
    async with AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://test") as client:
        r1 = await client.post("/webhook/bitbucket", content=payload, headers=headers)
        r2 = await client.post("/webhook/bitbucket", content=payload, headers=headers)
    assert r1.json()["job_id"] == r2.json()["job_id"]
    files = await app_instance.state.store.list_files_for_job(r1.json()["job_id"])
    assert len(files) == 1  # 재전송에도 파일 1개 (중복 생성 안 함)

@pytest.mark.asyncio
async def test_callback_forge_unknown_job(app_instance):
    async with AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://test") as client:
        resp = await client.post(
            "/callback/forge?token=cb-secret",
            json={"job_id": "unknown-forge-id", "status": "completed", "result": {"text": ""}},
        )
    assert resp.status_code == 200
    assert resp.json()["received"] is True

@pytest.mark.asyncio
async def test_callback_robotics_rejects_without_token(app_instance):
    """콜백에 유효 토큰 없으면 401 — 무인증 RAG 주입 차단 (#3)."""
    store = app_instance.state.store
    job = await store.create_job(source_type="webhook", repo="GCore")
    f = await store.create_file(job_id=job.job_id, file_path="PKG.pkb", file_type="plsql")
    await store.update_file(f.file_id, external_job_id="rdoc-1", external_status="processing")
    async with AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://test") as client:
        resp = await client.post(
            "/callback/robotics",
            json={"rdoc_job_id": "rdoc-1", "status": "completed", "content": "inject"},
        )
    assert resp.status_code == 401

@pytest.mark.asyncio
async def test_callback_forge_rejects_without_token(app_instance):
    """forge 콜백도 토큰 없으면 401 (#3)."""
    async with AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://test") as client:
        resp = await client.post(
            "/callback/forge",
            json={"job_id": "x", "status": "completed", "text": "inject"},
        )
    assert resp.status_code == 401

@pytest.mark.asyncio
async def test_callback_rejects_when_secret_not_configured():
    """callback_secret 미설정 시 fail-closed 503 (#3, #4와 일관)."""
    config = Config(
        forge_url="http://forge:8003", lightrag_url="http://lightrag:9621",
        robotics_url="http://robotics:8004", nexus_url="http://nexus:8005",
        database_url="", bitbucket_webhook_secret="test-secret", callback_secret="",
        self_url="http://localhost:8001",
    )
    app = create_app(store=InMemoryJobStore(), config=config)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post("/callback/robotics?token=anything", json={"rdoc_job_id": "x", "status": "completed"})
    assert resp.status_code == 503

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

@pytest.mark.asyncio
async def test_callback_robotics_completed(app_instance):
    """Robotics 완료 콜백 — LightRAG ingest 트리거."""
    store = app_instance.state.store
    job = await store.create_job(source_type="webhook", repo="GCore")
    f = await store.create_file(job_id=job.job_id, file_path="PKG.pkb", file_type="plsql")
    await store.update_file(f.file_id, external_job_id="robotics-999", external_status="processing")

    async with AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://test") as client:
        resp = await client.post(
            "/callback/robotics?token=cb-secret",
            json={
                "rdoc_job_id": "robotics-999",
                "file_name": "PKG.pkb",
                "content": "# PKG 역문서화 결과",
                "status": "completed",
                "error": None,
            },
        )
    assert resp.status_code == 200
    assert resp.json()["received"] is True

@pytest.mark.asyncio
async def test_callback_robotics_failed(app_instance):
    """Robotics 실패 콜백 — external_status='failed' 기록."""
    store = app_instance.state.store
    job = await store.create_job(source_type="webhook", repo="GCore")
    f = await store.create_file(job_id=job.job_id, file_path="PKG.pkb", file_type="plsql")
    await store.update_file(f.file_id, external_job_id="robotics-fail", external_status="processing")

    async with AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://test") as client:
        resp = await client.post(
            "/callback/robotics?token=cb-secret",
            json={
                "rdoc_job_id": "robotics-fail",
                "file_name": "PKG.pkb",
                "content": "",
                "status": "failed",
                "error": "LLM timeout",
            },
        )
    assert resp.status_code == 200

    import asyncio
    await asyncio.sleep(0.05)
    updated = await store.get_file(f.file_id)
    assert updated.external_status == "failed"
    assert updated.error == "LLM timeout"

@pytest.mark.asyncio
async def test_advance_pipeline_duplicate_callback_ingests_once(app_instance):
    """동일 file에 완료 콜백 2회 → claim이 둘째를 막아 LightRAG ingest 정확히 1회 (#8)."""
    from unittest.mock import AsyncMock
    from ingest import advance_pipeline
    store = app_instance.state.store
    job = await store.create_job(source_type="webhook", repo="GCore")
    f = await store.create_file(job_id=job.job_id, file_path="PKG.pkb", file_type="plsql")
    await store.update_file(f.file_id, external_job_id="rdoc-dup", external_status="processing")

    lightrag = AsyncMock()
    lightrag.ingest_text = AsyncMock(return_value={"status": "ok"})
    body = {"status": "completed", "result": {"text": "역문서 결과"}}

    await advance_pipeline("rdoc-dup", body, store, lightrag)
    await advance_pipeline("rdoc-dup", body, store, lightrag)  # 재시도/중복 콜백

    assert lightrag.ingest_text.call_count == 1
    updated = await store.get_file(f.file_id)
    assert updated.rag_status == "ingested"

@pytest.mark.asyncio
async def test_callback_robotics_unknown_job(app_instance):
    """알 수 없는 rdoc_job_id → 404 반환."""
    async with AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://test") as client:
        resp = await client.post(
            "/callback/robotics?token=cb-secret",
            json={
                "rdoc_job_id": "unknown-id",
                "file_name": "X.pkb",
                "content": "",
                "status": "completed",
                "error": None,
            },
        )
    assert resp.status_code == 404

@pytest.mark.asyncio
async def test_graphify_rebuild(app_instance, test_config):
    """POST /ingest/graphify-rebuild — Nexus rebuild 트리거."""
    import respx
    import httpx
    from ingest import NexusClient
    app_instance.state.nexus = NexusClient(base_url=test_config.nexus_url, api_key=test_config.nexus_api_key)
    async with respx.mock:
        respx.post("http://nexus:8005/rebuild/").mock(
            return_value=httpx.Response(200, json={"status": "ok", "message": "Rebuild started"})
        )
        async with AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://test") as client:
            resp = await client.post("/ingest/graphify-rebuild")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
    await app_instance.state.nexus.close()

@pytest.mark.asyncio
async def test_ingest_upload_code_dispatches_to_nexus(app_instance, test_config):
    """코드 파일 업로드 → Nexus /rebuild/upload 호출 → job completed."""
    import asyncio
    import respx
    import httpx
    from ingest import NexusClient
    app_instance.state.nexus = NexusClient(base_url=test_config.nexus_url, api_key=test_config.nexus_api_key)

    async with respx.mock:
        respx.post("http://nexus:8005/rebuild/upload").mock(
            return_value=httpx.Response(200, json={"status": "ok", "nodes": 100, "edges": 200})
        )
        async with AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://test") as client:
            resp = await client.post(
                "/ingest/upload",
                files=[("files", ("Main.java", b"public class Main {}", "text/plain"))],
            )
        assert resp.status_code == 202
        data = resp.json()
        assert "job_id" in data
        assert data["file_count"] == 1

        await asyncio.sleep(0.1)

    job = await app_instance.state.store.get_job(data["job_id"])
    assert job.status == "completed"
    await app_instance.state.nexus.close()

@pytest.mark.asyncio
async def test_upload_rejects_too_many_files(app_instance):
    """업로드 파일 개수 상한 초과 → 413 (#5 가드레일)."""
    files = [("files", (f"f{i}.png", b"x", "image/png")) for i in range(6)]  # 상한 5
    async with AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://test") as client:
        resp = await client.post("/ingest/upload", files=files)
    assert resp.status_code == 413

@pytest.mark.asyncio
async def test_upload_oversized_file_marked_failed(app_instance):
    """크기 상한 초과 파일 → failed 기록, 디스패치 안 함 (#5 가드레일)."""
    import asyncio
    big = b"x" * 2000  # 상한 1000 초과
    async with AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://test") as client:
        resp = await client.post("/ingest/upload", files=[("files", ("big.png", big, "image/png"))])
    assert resp.status_code == 202
    await asyncio.sleep(0.05)
    data = resp.json()
    files = await app_instance.state.store.list_files_for_job(data["job_id"])
    assert files[0].external_status == "failed"
    assert "size" in (files[0].error or "").lower()

@pytest.mark.asyncio
async def test_ingest_upload_skip_file(app_instance):
    """skip 파일 업로드 → job 즉시 completed."""
    import asyncio
    async with AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://test") as client:
        resp = await client.post(
            "/ingest/upload",
            files=[("files", ("image.png", b"\x89PNG", "image/png"))],
        )
    assert resp.status_code == 202
    await asyncio.sleep(0.05)
    data = resp.json()
    job = await app_instance.state.store.get_job(data["job_id"])
    assert job.status == "completed"

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
    assert files[0].external_status == "processing"
    assert files[0].external_job_id == "robotics-body-1"

    await app_instance.state.robotics.close()
    await app_instance.state.lightrag.close()


@pytest.mark.asyncio
async def test_ingest_upload_robotics_empty_job_id_marks_failed(app_instance, test_config):
    """Robotics가 빈 job id 반환 시 processing이 아니라 failed로 기록 (#9 fan-out 차단)."""
    import asyncio
    import respx
    import httpx
    from ingest import RoboticsClient, LightRAGClient

    app_instance.state.robotics = RoboticsClient(base_url=test_config.robotics_url, api_key=test_config.robotics_api_key)
    app_instance.state.lightrag = LightRAGClient(base_url=test_config.lightrag_url, api_key=test_config.lightrag_api_key)

    async with respx.mock:
        respx.post("http://robotics:8004/jobs").mock(
            return_value=httpx.Response(200, json={"status": "queued"})  # rdoc_job_id 누락
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
    assert files[0].external_status == "failed"
    assert files[0].external_job_id in (None, "")

    await app_instance.state.robotics.close()
    await app_instance.state.lightrag.close()

@pytest.mark.asyncio
async def test_ingest_upload_document_empty_job_id_marks_failed(app_instance, test_config):
    """Forge가 빈 job id 반환 시 processing이 아니라 failed로 기록 (#9 fan-out 차단)."""
    import asyncio
    import respx
    import httpx
    from ingest import ForgeClient

    app_instance.state.forge = ForgeClient(base_url=test_config.forge_url, api_key=test_config.forge_api_key)

    async with respx.mock:
        respx.post("http://forge:8003/convert").mock(
            return_value=httpx.Response(200, json={"status": "queued"})  # job_id 누락
        )
        async with AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://test") as client:
            resp = await client.post(
                "/ingest/upload",
                files=[("files", ("spec.pdf", b"%PDF-1.4 dummy", "application/pdf"))],
            )
        assert resp.status_code == 202
        await asyncio.sleep(0.1)

    store = app_instance.state.store
    data = resp.json()
    files = await store.list_files_for_job(data["job_id"])
    assert files[0].external_status == "failed"

    await app_instance.state.forge.close()

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

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
        forge_api_key="key",
        lightrag_api_key="key",
        robotics_api_key="key",
        nexus_api_key="key",
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

@pytest.mark.asyncio
async def test_callback_robotics_completed(app_instance):
    """Robotics 완료 콜백 — LightRAG ingest 트리거."""
    store = app_instance.state.store
    job = await store.create_job(source_type="webhook", repo="GCore")
    f = await store.create_file(job_id=job.job_id, file_path="PKG.pkb", file_type="plsql")
    await store.update_file(f.file_id, external_job_id="robotics-999", external_status="processing")

    async with AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://test") as client:
        resp = await client.post(
            "/callback/robotics",
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
            "/callback/robotics",
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

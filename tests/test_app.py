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

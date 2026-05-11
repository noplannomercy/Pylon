import pytest
from unittest.mock import AsyncMock, MagicMock, patch
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

def test_classify_code():
    assert classify_file("Main.java") == "code"
    assert classify_file("index.ts") == "code"
    assert classify_file("App.tsx") == "code"
    assert classify_file("app.js") == "code"
    assert classify_file("App.jsx") == "code"
    assert classify_file("utils.py") == "code"

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

@pytest.mark.asyncio
async def test_nexus_upload_success():
    from ingest import NexusClient
    client = NexusClient(base_url="http://nexus:8005", api_key="test-key")
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json = MagicMock(return_value={"status": "ok", "nodes": 100, "edges": 200})
    with patch.object(client._client, "post", new=AsyncMock(return_value=mock_resp)) as mock_post:
        result = await client.upload(b"public class Main {}", "Main.java")
        assert mock_post.call_args[0][0] == "/rebuild/upload"
    assert result["status"] == "ok"
    assert result["nodes"] == 100
    await client.close()

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

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

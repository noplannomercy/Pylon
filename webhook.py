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

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

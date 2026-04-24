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

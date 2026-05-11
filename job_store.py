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

    async def get_file_by_external_job_id(self, external_job_id: str) -> Optional[IngestionFile]:
        for f in self._files.values():
            if f.external_job_id == external_job_id:
                return f
        return None

    async def get_files_by_external_job_id(self, external_job_id: str) -> list[IngestionFile]:
        return [f for f in self._files.values() if f.external_job_id == external_job_id]

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
        failed_files = [f for f in files if f.external_status == "failed" or f.rag_status == "failed"]
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
                 "external_status": f.external_status, "rag_status": f.rag_status, "error": f.error}
                for f in failed_files[-10:]
            ],
        }


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

    async def get_file_by_external_job_id(self, external_job_id: str) -> Optional[IngestionFile]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM ingestion_file WHERE external_job_id = $1", external_job_id)
        return IngestionFile(**dict(row)) if row else None

    async def get_files_by_external_job_id(self, external_job_id: str) -> list[IngestionFile]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM ingestion_file WHERE external_job_id = $1", external_job_id)
        return [IngestionFile(**dict(r)) for r in rows]

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
                """SELECT file_id, file_path, job_id, external_status, rag_status, error
                   FROM ingestion_file WHERE external_status='failed' OR rag_status='failed'
                   ORDER BY created_at DESC LIMIT 10"""
            )
        return {
            "today": {"jobs": row["total"], "completed": row["completed"], "failed": row["failed"], "partial": row["partial"]},
            "total": {"jobs": total_jobs, "files": total_files},
            "recent_failures": [dict(r) for r in failures],
        }

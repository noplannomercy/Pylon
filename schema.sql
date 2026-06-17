-- Pylon schema (idempotent)

CREATE TABLE IF NOT EXISTS ingestion_job (
    job_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_type  TEXT NOT NULL,
    repo         TEXT,
    pr_number    INT,
    commit_hash  TEXT,
    status       TEXT NOT NULL DEFAULT 'created',
    file_count   INT DEFAULT 0,
    created_at   TIMESTAMPTZ DEFAULT now(),
    started_at   TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS ingestion_file (
    file_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id          UUID NOT NULL REFERENCES ingestion_job(job_id),
    file_path       TEXT NOT NULL,
    file_type       TEXT NOT NULL,
    external_job_id TEXT,
    external_status TEXT NOT NULL DEFAULT 'queued',
    rag_status      TEXT NOT NULL DEFAULT 'pending',
    review_status   TEXT NOT NULL DEFAULT 'auto_approved',
    error           TEXT,
    created_at      TIMESTAMPTZ DEFAULT now(),
    completed_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_ingestion_file_job_id ON ingestion_file(job_id);
CREATE INDEX IF NOT EXISTS idx_ingestion_file_external_job_id ON ingestion_file(external_job_id);

-- #7 멱등: webhook (repo, pr_number, commit_hash) 중복 job 방지. upload/bulk(NULL)은 제외 → NULL끼리 충돌 안 함
CREATE UNIQUE INDEX IF NOT EXISTS uq_ingestion_job_pr_commit
    ON ingestion_job(repo, pr_number, commit_hash)
    WHERE repo IS NOT NULL AND pr_number IS NOT NULL AND commit_hash IS NOT NULL;

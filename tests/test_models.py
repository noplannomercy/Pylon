from models import IngestionJob, IngestionFile

def test_ingestion_job_defaults():
    job = IngestionJob(job_id="abc", source_type="webhook", status="created")
    assert job.status == "created"
    assert job.file_count == 0
    assert job.pr_number is None

def test_ingestion_file_defaults():
    f = IngestionFile(file_id="xyz", job_id="abc", file_path="src/PKG_LOAN.pkb", file_type="plsql")
    assert f.external_status == "queued"
    assert f.rag_status == "pending"
    assert f.review_status == "auto_approved"

# 작업 시작 전

- 이 파일을 끝까지 읽은 뒤 작업을 시작할 것
- `python -m pytest tests/ -v`로 현재 테스트 상태 확인 후 작업 시작
- `.env`에 `LIGHTRAG_URL`, `FORGE_URL` 설정 확인

---

# 개요

HCS Ingestion Router는 Bitbucket PR Merge 이벤트를 수신해 파일별로 Forge → LightRAG 파이프라인을 구동하고 전 단계 상태를 추적하는 FastAPI 오케스트레이터 서비스다. Forge(:8003), LightRAG(:9621)와 독립, 포트 8001.

---

# 제약 사항

| # | 규칙 | 이유 |
|---|------|------|
| C1 | asyncio.create_task 호출 시 반드시 `_safe_process` 래퍼 사용 | fire-and-forget에서 예외가 삼켜지는 문제. |
| C2 | JobStore 인터페이스를 우회하여 dict에 직접 접근 금지 | PostgresJobStore 전환 시 코드 변경 최소화. |
| C3 | API 키, 시크릿 하드코딩 금지 | `.env` 또는 환경변수. config.py의 pydantic-settings로 관리. |
| C4 | Forge 콜백은 `forge_job_id` 기준으로만 파일 역조회 | file_id는 내부 식별자, forge_job_id가 외부 연결 키. |

---

# 스택

| 기술 | 용도 |
|------|------|
| Python 3.11+ | 런타임 |
| FastAPI + uvicorn | REST API (포트 8001) |
| httpx (async) | Forge / LightRAG / Bitbucket API 호출 |
| pydantic-settings | 환경변수 관리 |
| asyncpg | PostgreSQL 비동기 드라이버 |

---

# 구조

| 경로 | 역할 |
|------|------|
| `app.py` | FastAPI 엔트리포인트 + lifespan + webhook/callback/bulk/openapi-all 라우트 |
| `admin.py` | 모니터링 API (GET /jobs, /files, /stats, /health/all, POST /reject, /re-ingest, /retry) |
| `ingest.py` | ForgeClient, LightRAGClient, classify_file, advance_pipeline, _maybe_close_job |
| `webhook.py` | HMAC 검증 + Bitbucket payload 파싱 (HCS 환경 확인 후 이 파일만 수정) |
| `job_store.py` | InMemoryJobStore + PostgresJobStore (DATABASE_URL 있으면 자동 전환) |
| `config.py` | 환경변수 로드 |
| `models.py` | IngestionJob, IngestionFile Pydantic 모델 |
| `schema.sql` | PostgreSQL DDL (ingestion_job, ingestion_file 테이블) |
| `Dockerfile` | Python 3.11-slim, 포트 8001 |

---

# 파이프라인 흐름

```
Bitbucket PR Merge
  → POST /webhook/bitbucket (HMAC 검증 → payload 파싱 → job/file 생성)
  → Bitbucket API로 파일 bytes fetch  ← [미구현]
  → ForgeClient.convert(file_bytes, callback_url)  ← [미구현]
  → Forge가 변환 완료 후 POST /callback/forge
  → advance_pipeline() → LightRAGClient.ingest_text()
  → _maybe_close_job() → job status 완료 처리
```

---

# 미완성 작업 (우선순위 순)

| # | 항목 | 비고 |
|---|------|------|
| 🔴 1 | **all-skip job 자동 완료 버그** | webhook/bulk에서 skip 처리 후 `_maybe_close_job` 미호출 |
| 🔴 2 | **Bitbucket 파일 bytes fetch** | Bitbucket API 클라이언트 미구현 |
| 🔴 3 | **Forge 실제 호출** | `ForgeClient.convert()` webhook/bulk에서 미호출, forge_job_id 저장 없음 |
| 🟡 4 | **PostgreSQL 연결** | `.env`에 `DATABASE_URL` 추가만 하면 됨 (코드 준비됨) |
| 🟡 5 | **Admin 인증** | 현재 모든 admin 엔드포인트 인증 없음 |
| 🟡 6 | **docker-compose.yml** | Dockerfile은 있음 |
| 🟢 7 | **Bitbucket webhook payload 구조 확인** | HCS 환경 Server/Cloud 여부 확인 후 webhook.py 조정 |
| 🟢 8 | **Forge callback 타임아웃 처리** | 응답 없으면 job이 processing으로 영원히 남음 |

---

# 완료 조건

```bash
# 테스트 전체 통과
python -m pytest tests/ -v
# 예상: 28 passed

# 서버 기동 확인
uvicorn app:app --port 8001
curl http://localhost:8001/health
# 예상: {"status":"ok"}

curl http://localhost:8001/health/all
# 예상: forge, lightrag 모두 "ok"
```

---

# 환경변수 (.env)

```
LIGHTRAG_URL=http://193.168.195.222:9621
FORGE_URL=http://localhost:8003
SELF_URL=http://localhost:8001
DATABASE_URL=                        # 비워두면 InMemory, 설정 시 Postgres 자동 전환
BITBUCKET_WEBHOOK_SECRET=            # HMAC 검증용
FORGE_API_KEY=
LIGHTRAG_API_KEY=
```

---

# 참조 문서

| 문서 | 설명 |
|------|------|
| `docs/superpowers/specs/2026-04-24-hcs-ingestion-router-design.md` | 설계 스펙 |
| `docs/superpowers/plans/2026-04-24-hcs-ingestion-router.md` | 구현 플랜 (8 Task) |

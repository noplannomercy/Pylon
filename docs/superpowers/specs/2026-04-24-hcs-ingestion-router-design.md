# HCS Ingestion Router — 설계 스펙

> **작성일**: 2026-04-24
> **범위**: Bitbucket PR 기반 파이프라인 오케스트레이터 (Forge + LightRAG 연결)
> **프로젝트 위치**: `/c/workspace/hcs-ingestion-router/`

---

## 1. 개요

Bitbucket PR Merge 이벤트를 수신해 파일별로 Forge(전처리)와 LightRAG(지식화)를 순차 호출하고, 전체 파이프라인 상태를 DB에 추적하는 오케스트레이터 서비스다.

**이 서비스가 하는 것:**
- Bitbucket webhook 수신 + HMAC 검증
- 파일 유형 분기 (PL/SQL → `/reverse-doc`, 문서 → `/convert`)
- Forge 비동기 호출 + callback 수신
- LightRAG ingest 트리거
- 파이프라인 전 단계 상태 DB 기록
- UI 대시보드용 모니터링 API 노출

**이 서비스가 하지 않는 것:**
- 파일 직접 파싱/변환 (Forge 담당)
- 지식화/임베딩 (LightRAG 담당)
- S3 스냅샷 (미확정, 추후 별도 스펙)
- 큐/워커 분리 (트래픽 증가 시 Stage 1 이후 재검토)

---

## 2. 스택

| 항목 | 선택 |
|------|------|
| 런타임 | Python 3.11+ |
| 프레임워크 | FastAPI + uvicorn |
| HTTP 클라이언트 | httpx (async) |
| DB 드라이버 | asyncpg |
| 설정 관리 | pydantic-settings |
| 포트 | 8001 |
| DB | PostgreSQL (Forge와 동일 인스턴스, `ingestion_*` 테이블 추가) |

---

## 3. 파일 구조

```
hcs-ingestion-router/
├── app.py          — FastAPI 엔트리포인트 + lifespan
├── config.py       — 환경변수 (pydantic-settings)
├── models.py       — Pydantic 모델
├── job_store.py    — DB 접근 (ingestion_job, ingestion_file)
├── webhook.py      — Bitbucket webhook 수신 + HMAC 검증
├── ingest.py       — Forge 호출 + LightRAG 호출 로직
├── admin.py        — 모니터링 API (UI용)
├── schema.sql      — DDL (IF NOT EXISTS, idempotent)
├── Dockerfile
└── .env.example
```

---

## 4. DB 스키마

### `ingestion_job`
PR 이벤트 또는 벌크 요청 단위.

```sql
CREATE TABLE ingestion_job (
    job_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_type  TEXT NOT NULL,        -- 'webhook' | 'bulk'
    repo         TEXT,
    pr_number    INT,                  -- webhook 시만, bulk는 NULL
    commit_hash  TEXT,
    status       TEXT NOT NULL DEFAULT 'created',
                                       -- created | processing | completed | partial | failed
    file_count   INT DEFAULT 0,
    created_at   TIMESTAMPTZ DEFAULT now(),
    started_at   TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);
```

### `ingestion_file`
파일별 처리 상태.

```sql
CREATE TABLE ingestion_file (
    file_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id         UUID NOT NULL REFERENCES ingestion_job(job_id),
    file_path      TEXT NOT NULL,      -- 레포 내 경로
    file_type      TEXT NOT NULL,      -- 'plsql' | 'document' | 'skip'
    forge_job_id   TEXT,               -- Forge job_id (callback 연결용)
    forge_status   TEXT NOT NULL DEFAULT 'queued',
                                       -- queued | forging | done | skipped | failed
    rag_status     TEXT NOT NULL DEFAULT 'pending',
                                       -- pending | ingesting | ingested | failed
    review_status  TEXT NOT NULL DEFAULT 'auto_approved',
                                       -- auto_approved | rejected (사후 개입 시)
    error          TEXT,
    created_at     TIMESTAMPTZ DEFAULT now(),
    completed_at   TIMESTAMPTZ
);

CREATE INDEX idx_ingestion_file_job_id ON ingestion_file(job_id);
CREATE INDEX idx_ingestion_file_forge_job_id ON ingestion_file(forge_job_id);
```

### 상태 흐름

```
ingestion_job:
  created → processing → completed
                       → partial   (일부 파일 실패)
                       → failed    (전체 실패)

ingestion_file:
  queued → forging → done → ingesting → ingested
                   → failed (forge)
                              → failed (rag)
```

`audit_log` — MVP 이후 추가.

---

## 5. API 엔드포인트

### 인입

| 메서드 | 경로 | 설명 |
|--------|------|------|
| POST | `/webhook/bitbucket` | PR Merge 이벤트 수신, HMAC 검증 |
| POST | `/ingest/bulk` | 파일 경로 리스트 직접 투입 (초기 적재 + 수동 트리거) |

### Forge 콜백 (내부)

| 메서드 | 경로 | 설명 |
|--------|------|------|
| POST | `/callback/forge` | Forge 변환 완료 수신, LightRAG ingest 트리거 |

### 모니터링 (UI용)

| 메서드 | 경로 | 설명 |
|--------|------|------|
| GET | `/jobs` | job 목록 (page, size, status, source_type 필터) |
| GET | `/jobs/{job_id}` | job 상세 + 포함 파일 목록 |
| GET | `/jobs/{job_id}/files` | 파일별 처리 상태 |
| GET | `/files/{file_id}` | 파일 단건 상세 (forge_job_id 포함) |
| GET | `/stats` | 오늘 job/file 수, 상태별 breakdown, 실패 목록 |
| GET | `/health/all` | Forge + LightRAG + 자기 자신 health 통합 반환 |

### 사후 개입 (UI용)

| 메서드 | 경로 | 설명 |
|--------|------|------|
| POST | `/files/{file_id}/reject` | review_status → rejected |
| POST | `/files/{file_id}/re-ingest` | rag_status 초기화 후 LightRAG 재투입 |
| POST | `/jobs/{job_id}/retry` | job 내 실패 파일 전체 재시도 |

### 시스템

| 메서드 | 경로 | 설명 |
|--------|------|------|
| GET | `/health` | 자체 헬스체크 |
| GET | `/openapi.json` | Forge + LightRAG API 머지 포함 (크롬 익스텐션용) |

---

## 6. 파이프라인 흐름

### 파일 유형 분기

```
.pkb .pks .sql .prc .fnc  →  Forge POST /reverse-doc
.pdf .docx .pptx .xlsx .md →  Forge POST /convert
그 외                      →  file_type='skip', forge_status='skipped', rag_status='pending' (무시)
```

### Forge 호출

```python
callback_url = f"http://ingestion-router:8001/callback/forge?file_id={file_id}"

# PL/SQL
POST /reverse-doc
  file=<소스코드>
  callback_url=callback_url
  requested_by="ingestion-router"

# 문서
POST /convert
  file=<파일>
  callback_url=callback_url
  requested_by="ingestion-router"
```

응답으로 받은 `forge_job_id`를 `ingestion_file.forge_job_id`에 저장.

### Forge 콜백 → LightRAG 트리거

```
POST /callback/forge?file_id=xxx  (Forge가 완료 시 호출)
  ↓
forge_status → done  (실패면 forge_failed, 이후 처리 중단)
  ↓
LightRAG POST /documents/text
  {
    content: <markdown>,
    metadata: { file_id, job_id, file_path, forge_job_id, repo, pr_number }
  }
  ↓
rag_status → ingested  (실패면 ingest_failed)
  ↓
모든 파일 처리 완료 시 ingestion_job.status 갱신
  - 전체 성공  → completed
  - 일부 실패  → partial
  - 전체 실패  → failed
```

---

## 7. 환경변수

```env
FORGE_URL=http://forge:8003
LIGHTRAG_URL=http://lightrag:포트
DATABASE_URL=postgresql://user:pass@host:5432/dbname
BITBUCKET_WEBHOOK_SECRET=...
FORGE_API_KEY=...
LIGHTRAG_API_KEY=...
PORT=8001
```

---

## 8. 제약 사항

| # | 규칙 | 이유 |
|---|------|------|
| C1 | Forge/LightRAG 코드 수정 금지 | 독립 서비스 원칙 |
| C2 | asyncio.create_task 시 반드시 _safe_process 래퍼 사용 | fire-and-forget 예외 삼킴 방지 (Forge 패턴 동일) |
| C3 | HMAC 검증 실패 시 즉시 401 반환 | 위변조 webhook 차단 |
| C4 | Bitbucket webhook payload 파싱은 별도 모듈로 분리 | payload 구조 확정 전 교체 용이성 |

---

## 9. 미확정 사항

| 항목 | 상태 | 비고 |
|------|------|------|
| Bitbucket webhook payload 구조 | 미확정 | HCS 환경 확인 후 파싱 로직 확정 |
| LightRAG ingest API 경로/포트 | 확인 필요 | 현재 가동 중인 LightRAG 버전 기준 |
| S3 스냅샷 | 별도 스펙 | 버킷/디렉토리 구조 미결정 |
| 인증 방식 | 미결정 | HCS SSO 연동 or API 키 |

---

## 10. 추후 확장 포인트

- **Stage 1**: 트래픽 증가 시 Worker 분리 + SQS/Redis Queue
- **S3 스냅샷**: 원본 파일 보존 (별도 스펙으로 추가)
- **감사 로그**: `audit_log` 테이블 추가
- **Review UI**: 사후 개입 → 사전 승인 게이트로 전환 가능

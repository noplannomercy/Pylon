# Ingestion Router v2 — 설계 스펙

> **작성일**: 2026-04-29
> **범위**: 아키텍처 v0.4 반영 — Citadel 연동, 코드 파일 분기, 필드명 정규화
> **관련 문서**: `HCS_Code2Rule_PoC2_Architecture_v0.4.md`

---

## 1. 개요

기존 ingestion-router는 PL/SQL을 Forge(:8003)로 라우팅하도록 설계되었으나, 아키텍처 v0.4에서 역문서화 전담 서비스 Citadel(:8004)이 독립 분리되었다. 또한 코드 파일(.java/.ts 등) 처리 경로 및 Graphify rebuild 트리거 엔드포인트가 추가된다.

**변경 범위:**
- PL/SQL 라우팅: Forge → Citadel
- 코드 파일: skip 기록 + graphify-rebuild 엔드포인트
- 필드명 정규화: `forge_job_id` → `external_job_id`, `forge_status` → `external_status`
- Citadel 콜백 엔드포인트 추가
- Admin API 필드명 반영

---

## 2. 파일 유형 분기

```
확장자                           file_type   라우팅
.pkb .pks .sql .prc .fnc     →  plsql      Citadel POST /jobs
.pdf .docx .pptx .md .txt    →  document   Forge POST /convert
.java .js .ts .jsx .tsx .py  →  code       skip (DB 기록만)
그 외                         →  skip       skip
```

---

## 3. 파이프라인 흐름

### 3.1 PL/SQL 경로 (Citadel)

```
1. classify_file() → file_type='plsql'
2. CitadelClient.submit(file_bytes, callback_url)
   POST /jobs (multipart/form-data)
     file:         <소스코드 바이트>
     asset_type:   "plsql"
     callback_url: "http://ingestion-router:8001/callback/citadel"
     requested_by: "ingestion-router"
   ← 응답: {job_id, status}
3. external_job_id = job_id 저장, external_status = 'processing'
4. Citadel 처리 완료 후 POST /callback/citadel 수신
   {rdoc_job_id, file_name, content, status, error}
5. status == 'completed' → LightRAG POST /documents/text (content)
   status == 'failed'    → external_status='failed', error 기록
6. _maybe_close_job()
```

### 3.2 문서 경로 (Forge — 기존 유지)

```
1. classify_file() → file_type='document'
2. ForgeClient.convert(file_bytes, callback_url)
3. POST /callback/forge 수신 (기존 유지)
4. LightRAG ingest → _maybe_close_job()
```

### 3.3 코드 파일 경로 (신규)

```
1. classify_file() → file_type='code'
2. external_status='skipped', rag_status='skipped'
3. _maybe_close_job() 호출
```

> Graphify는 시스템 외부에서 수동 배치로 실행. ingestion-router는 skip 기록만 담당.

### 3.4 all-skip 버그 수정

webhook/bulk에서 모든 파일이 skip인 경우 `_maybe_close_job()`이 호출되지 않던 버그를 코드 파일 경로 추가와 함께 수정한다.

---

## 4. 신규 엔드포인트

### `POST /callback/citadel`

Citadel이 역문서화 완료 후 호출하는 콜백.

**Request body (JSON):**
```json
{
  "rdoc_job_id": "string",
  "file_name": "string",
  "content": "string",
  "status": "completed | failed",
  "error": "string | null"
}
```

**처리 흐름:**
1. `rdoc_job_id` 기준으로 `external_job_id` 역조회 → `IngestionFile` 조회
2. `status == 'completed'` → LightRAG ingest → `external_status='done'`, `rag_status='ingested'`
3. `status == 'failed'` → `external_status='failed'`, error 기록
4. `_maybe_close_job()` 호출

### `POST /ingest/graphify-rebuild`

외부 graphify 실행 후 수동으로 Nexus rebuild를 트리거하는 엔드포인트.

**처리 흐름:**
1. Nexus `POST /rebuild/` 호출
2. 응답 그대로 반환

**Response:**
```json
{"status": "ok", "message": "Rebuild started"}
```

---

## 5. 필드명 정규화

| 기존 | 변경 | 영향 범위 |
|------|------|-----------|
| `forge_job_id` | `external_job_id` | `models.py`, `job_store.py`, `schema.sql`, `admin.py`, `ingest.py`, `tests/` |
| `forge_status` | `external_status` | 동일 |

`external_job_id`는 Forge job ID 또는 Citadel job ID를 저장하는 단일 컬럼. 파일은 항상 Forge 또는 Citadel 중 하나로만 라우팅되므로 두 컬럼 분리 불필요.

---

## 6. 설정 추가 (config.py)

```env
CITADEL_URL=http://localhost:8004    # Citadel 엔드포인트
NEXUS_URL=http://localhost:8005      # Nexus REST API
CITADEL_API_KEY=                     # 선택
NEXUS_API_KEY=                       # 선택
```

---

## 7. 컴포넌트 변경 요약

| 파일 | 변경 내용 |
|------|-----------|
| `config.py` | `CITADEL_URL`, `NEXUS_URL`, `CITADEL_API_KEY`, `NEXUS_API_KEY` 추가 |
| `ingest.py` | `classify_file()` code 타입 추가, `CitadelClient` 추가, `advance_pipeline()` Citadel/code 분기 |
| `app.py` | `POST /callback/citadel`, `POST /ingest/graphify-rebuild` 추가 |
| `job_store.py` | `forge_job_id` → `external_job_id`, `forge_status` → `external_status`, `get_file_by_forge_job_id()` → `get_file_by_external_job_id()` |
| `schema.sql` | 컬럼명 변경, 인덱스명 변경 |
| `models.py` | `IngestionFile` 필드명 변경 |
| `admin.py` | 필드명 반영 |
| `tests/` | 필드명 업데이트 + 신규 엔드포인트 테스트 추가 |

---

## 8. 에러 처리

| 상황 | 처리 |
|------|------|
| Citadel 제출 실패 | `external_status='failed'`, error 기록, `_maybe_close_job()` |
| Citadel 콜백 `status=failed` | `external_status='failed'`, LightRAG ingest 생략 |
| Nexus rebuild 실패 | HTTP 에러 그대로 반환 (재시도는 호출자 책임) |
| 알 수 없는 `rdoc_job_id` | 404 반환 |

---

## 9. 테스트 범위

- `classify_file()` — code 타입 분기 확인
- `POST /callback/citadel` — completed/failed 양 케이스
- `POST /ingest/graphify-rebuild` — Nexus 호출 확인
- all-skip job 자동 완료 버그 회귀 테스트
- 필드명 변경 반영 전체 테스트 통과

---

## 10. 미포함 범위

- Bitbucket 파일 bytes fetch (별도 태스크)
- Forge 실제 호출 연결 (별도 태스크)
- Admin 인증 (별도 태스크)
- PostgreSQL 연결 (`.env` 설정만으로 활성화 가능, 코드 준비됨)

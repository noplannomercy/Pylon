# 작업 시작 전

- 이 파일을 끝까지 읽은 뒤 작업을 시작할 것
- `python -m pytest tests/ -v`로 현재 테스트 상태 확인 후 작업 시작
- `.env`에 `LIGHTRAG_URL`, `FORGE_URL`, `ROBOTICS_URL`, `NEXUS_URL`, `SELF_URL` 설정 확인 — 모두 서버 IP 기반이어야 함 (localhost 금지)

---

# 개요

Pylon은 Bitbucket PR Merge 이벤트를 수신해 파일별로 Robotics(PL/SQL) / Forge(문서) / skip(코드·기타) 파이프라인을 구동하고 전 단계 상태를 추적하는 FastAPI 오케스트레이터 서비스다. 포트 8001.

---

# 제약 사항

| # | 규칙 | 이유 |
|---|------|------|
| C1 | asyncio.create_task 호출 시 반드시 `_safe_process` 래퍼 사용 | fire-and-forget에서 예외가 삼켜지는 문제. |
| C2 | JobStore 인터페이스를 우회하여 dict에 직접 접근 금지 | PostgresJobStore 전환 시 코드 변경 최소화. |
| C3 | API 키, 시크릿 하드코딩 금지 | `.env` 또는 환경변수. config.py의 pydantic-settings로 관리. |
| C4 | Robotics/Forge 콜백은 `external_job_id` 기준으로만 파일 역조회 | file_id는 내부 식별자, external_job_id가 외부 연결 키. |
| C5 | Docker 컨테이너 내 `localhost`는 컨테이너 자신 | .env의 NEXUS_URL/ROBOTICS_URL/SELF_URL을 서버 IP로 설정해야 라우팅됨. SELF_URL이 localhost이면 Forge/Robotics callback이 컨테이너 자신으로 향해 실패. `docker restart`는 .env 미반영 — `docker compose up -d --force-recreate` 사용. |

---

# 스택

| 기술 | 용도 |
|------|------|
| Python 3.11+ | 런타임 |
| FastAPI + uvicorn | REST API (포트 8001) |
| httpx (async) | Robotics / Forge / LightRAG / Nexus API 호출 |
| pydantic-settings | 환경변수 관리 |
| asyncpg | PostgreSQL 비동기 드라이버 |

---

# 구조

| 경로 | 역할 |
|------|------|
| `app.py` | FastAPI 엔트리포인트 + lifespan + webhook/callback/bulk/upload/graphify-rebuild/openapi-all 라우트 |
| `admin.py` | 모니터링 API (GET /jobs, /files, /stats, /health/all, POST /reject, /re-ingest, /retry) |
| `ingest.py` | ForgeClient, RoboticsClient, NexusClient, LightRAGClient, classify_file, advance_pipeline, dispatch_code_file, _maybe_close_job |
| `webhook.py` | HMAC 검증 + Bitbucket payload 파싱 (HCS 환경 확인 후 이 파일만 수정) |
| `job_store.py` | InMemoryJobStore + PostgresJobStore (DATABASE_URL 있으면 자동 전환) |
| `config.py` | 환경변수 로드 |
| `models.py` | IngestionJob, IngestionFile Pydantic 모델 (external_job_id, external_status) |
| `schema.sql` | PostgreSQL DDL (ingestion_job, ingestion_file 테이블) |
| `Dockerfile` | Python 3.11-slim, 포트 8001 |

---

# 파일 유형 분기

```
확장자                                          file_type   라우팅
.pkb .pks .sql .prc .fnc  (내용에 'package body')  →  plsql   LightRAG 직접(fire-and-forget) + Robotics POST /jobs
.pkb .pks .sql .prc .fnc  (그 외)                  →  plsql   LightRAG 직접 ingest
.pdf .docx .pptx .xlsx .hwpx             →  document   Forge POST /convert
.md .txt                                 →  text_doc   LightRAG 직접
.java .js .ts .jsx .tsx .py              →  code       /ingest/upload → Nexus POST /rebuild/upload
                                                        /webhook/bulk → skipped (파일 bytes 없음)
그 외                                     →  skip       skipped 기록만
```

> body/header 판별(`is_body_file`)은 **파일명이 아니라 파일 내용**(`package body` 포함 여부)이 단일 진실. 전체 내용 스캔(절단 없음).

---

# 파이프라인 흐름

```
[직접 파일 업로드] POST /ingest/upload (multipart files)
  → 파일별 classify_file() + is_body_file()
  → plsql (_body 포함) → dispatch_plsql_direct(update_status=False) fire-and-forget → LightRAG 원문
                       → RoboticsClient.submit() → POST /callback/robotics → advance_pipeline() → LightRAG REVDOC
  → plsql (그 외)     → dispatch_plsql_direct() → LightRAG 원문
  → document          → ForgeClient.convert() → POST /callback/forge → advance_pipeline() → LightRAG
  → text_doc          → dispatch_text_doc() → LightRAG
  → code              → dispatch_code_file() → Nexus
  → skip              → skipped 기록만

[Bitbucket Webhook] POST /webhook/bitbucket (HMAC 검증 → payload 파싱 → 파일 경로만 수신)
  → 멱등: (repo, pr_number, commit_hash) 동일 재전송 시 기존 job 반환 (파일 중복 생성 안 함)
  → Bitbucket API로 파일 bytes fetch  ← [미구현]
  → plsql/document: submit() 미호출   ← [미구현]
  → code/skip: skipped 기록만 (bytes 없음)
  → _maybe_close_job()
```

---

# 미완성 작업 (우선순위 순)

| # | 항목 | 비고 |
|---|------|------|
| 🔴 1 | **Bitbucket 파일 bytes fetch** | Bitbucket API 클라이언트 미구현 |
| 🔴 2 | **Robotics/Forge 실제 호출 (webhook/bulk)** | /ingest/upload는 완료. webhook/bulk는 Bitbucket bytes fetch 미구현으로 여전히 skipped 처리 |
| 🟡 3 | **PostgreSQL 연결** | `.env`에 `DATABASE_URL` 추가만 하면 됨 (코드 준비됨) |
| 🟡 4 | **Admin 인증** | 현재 모든 admin 엔드포인트 인증 없음 |
| 🟡 5 | **docker-compose.yml** | Dockerfile은 있음 |
| 🟢 6 | **Bitbucket webhook payload 구조 확인** | HCS 환경 Server/Cloud 여부 확인 후 webhook.py 조정 |
| 🟢 7 | **callback 타임아웃 처리** | 응답 없으면 job이 processing으로 영원히 남음 |

---

# 완료 조건

```bash
# 테스트 전체 통과
python -m pytest tests/ -v
# 예상: 69 passed

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
FORGE_URL=http://193.168.195.222:8003
ROBOTICS_URL=http://localhost:8004        # Robotics가 같은 호스트에 있으면 host network 또는 서버 IP 사용
NEXUS_URL=http://193.168.195.222:8005   # Docker 컨테이너 내에서 localhost는 컨테이너 자신 → 서버 IP 필요
SELF_URL=http://localhost:8001
DATABASE_URL=                        # 비워두면 InMemory, 설정 시 Postgres 자동 전환
BITBUCKET_WEBHOOK_SECRET=            # HMAC 검증용 (미설정 시 /webhook/bitbucket은 503 거부 — fail-closed)
CALLBACK_SECRET=                     # Robotics/Forge 콜백 인증 토큰. Pylon이 callback_url에 ?token= 으로 발급, 수신 시 검증. 미설정 시 콜백 503 (fail-closed). Robotics/Forge 측 수정 불필요 (받은 URL 그대로 POST)
FORGE_API_KEY=
LIGHTRAG_API_KEY=
ROBOTICS_API_KEY=
NEXUS_API_KEY=
```

---

# 참조 문서

| 문서 | 설명 |
|------|------|
| `docs/superpowers/specs/2026-04-29-ingestion-router-v2-design.md` | v2 설계 스펙 (Robotics, code 파일, external_job_id) |
| `docs/superpowers/plans/2026-04-29-ingestion-router-v2.md` | v2 구현 플랜 (8 Task) |
| `docs/superpowers/specs/2026-04-24-hcs-ingestion-router-design.md` | v1 설계 스펙 (참조용) |

## Skill routing

When the user's request matches an available skill, invoke it via the Skill tool. When in doubt, invoke the skill.

Key routing rules:
- Product ideas/brainstorming → invoke /office-hours
- Strategy/scope → invoke /plan-ceo-review
- Architecture → invoke /plan-eng-review
- Design system/plan review → invoke /design-consultation or /plan-design-review
- Full review pipeline → invoke /autoplan
- Bugs/errors → invoke /investigate
- QA/testing site behavior → invoke /qa or /qa-only
- Code review/diff check → invoke /review
- Visual polish → invoke /design-review
- Ship/deploy/PR → invoke /ship or /land-and-deploy
- Save progress → invoke /context-save
- Resume context → invoke /context-restore

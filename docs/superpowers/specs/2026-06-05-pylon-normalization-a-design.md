# Pylon 정상화 — 묶음 A 설계

> 작성일: 2026-06-05  
> 범위: 네이밍 정리 + HCA plsql 파이프라인 완성 + 버그 수정  
> 전제: 묶음 B(Bitbucket bytes fetch)는 별도 스펙

---

## 배경

Pylon은 파일 ingest 오케스트레이터다. 현재 코드에 세 가지 문제가 있다.

1. **네이밍 불일치** — 코드 내 `citadel`은 실제 서비스 `Robotics`를 가리키지만, 코드 전체에 `citadel`로 박혀있어 혼란을 유발한다.
2. **HCA plsql 파이프라인 미완성** — `_body` / `_header` 구분 없이 모든 plsql을 Robotics로 보낸다. 설계 의도는 body만 Robotics(역문서), 나머지는 LightRAG 직접 ingest다.
3. **admin re-ingest 버그** — plsql 파일에도 Forge API를 호출해 404가 발생한다.

---

## 변경 범위

### 1. Citadel → Robotics 네이밍 전체 교체

| 변경 전 | 변경 후 | 위치 |
|---------|---------|------|
| `CitadelClient` | `RoboticsClient` | `ingest.py` |
| `a.state.citadel` | `a.state.robotics` | `app.py` |
| `/callback/citadel` | `/callback/robotics` | `app.py` |
| `rdoc_job_id` 콜백 필드 | 동일 유지 (Robotics API 반환값) | `app.py` |
| `config.citadel_url` / `citadel_api_key` | `config.robotics_url` / `robotics_api_key` | `config.py` |
| 환경변수 `CITADEL_URL` / `CITADEL_API_KEY` | `ROBOTICS_URL` / `ROBOTICS_API_KEY` | `.env.example`, `CLAUDE.md` |
| 테스트 내 `citadel` 참조 전부 | `robotics` | `tests/` |

---

### 2. HCA plsql 파이프라인 완성

#### 판단 함수 추가 (`ingest.py`)

```python
def is_body_file(filename: str) -> bool:
    return "_body" in filename.lower()
```

#### plsql 분기 (`ingest/upload`)

```
plsql 파일 수신
  ├─ is_body_file() == True
  │   ├─ dispatch_plsql_direct()   → LightRAG 원문 ingest (fire-and-forget)
  │   └─ RoboticsClient.submit()   → 콜백 → REVDOC → LightRAG ingest (상태 추적)
  │
  └─ is_body_file() == False (header / 패턴 없는 sql)
      └─ dispatch_plsql_direct()   → LightRAG 원문 ingest
```

#### `dispatch_plsql_direct()` 신설 (`ingest.py`)

`dispatch_text_doc()`과 동일 패턴. 파일 bytes를 UTF-8 디코딩 후 `LightRAGClient.ingest_text()` 호출. 완료 시 `external_status='done'`, `rag_status='ingested'` 설정.

#### 상태 추적 전략

- **body 파일**: Robotics 콜백 흐름이 `external_status` / `rag_status` primary 관리. 원문 ingest는 `update_status=False` 옵션으로 fire-and-forget 호출 — 상태 업데이트 없이 LightRAG 전송만. 콜백과 충돌 방지.
- **header/일반 파일**: `dispatch_plsql_direct()` 완료 후 `external_status='done'`, `rag_status='ingested'` 업데이트.

`file_type`은 `plsql` 그대로 유지 — 모델/스키마 변경 없음.

---

### 3. admin re-ingest 버그 수정 (`admin.py`)

**현재 버그**: plsql 파일의 `external_job_id`로 `forge.get_job()` 호출 → Forge는 Robotics job ID를 모르므로 404.

**수정 방향**: `file_type`으로 분기.

- `file_type == "plsql"` → re-ingest 불가 처리 (`400` 반환)
  - 이유: 원본 파일 bytes가 Pylon에 없어 Robotics 재제출 불가. rag_status만 재시도하려 해도 result_text가 없음.
  - 에러 메시지: `"plsql 파일은 원본 bytes가 없어 re-ingest 불가. 파일을 다시 업로드하세요."`
- `file_type == "document"` → 기존 Forge.get_job() 흐름 유지

---

### 4. `/callback/robotics` 응답 코드 수정 (`app.py`)

알 수 없는 `rdoc_job_id` 수신 시 현재 200 OK → `404 HTTPException`으로 변경.

이유: Robotics가 콜백 성공/실패를 인식해야 재시도 여부를 판단할 수 있음.

---

### 5. `.env.example` / `CLAUDE.md` 업데이트

- `CITADEL_*` → `ROBOTICS_*` 교체
- `SELF_URL` 누락 항목 추가
- `CLAUDE.md` 파일 유형 분기 테이블 업데이트 (body/header 구분 반영)

---

## 파일별 변경 요약

| 파일 | 변경 내용 |
|------|----------|
| `ingest.py` | CitadelClient → RoboticsClient, is_body_file() 추가, dispatch_plsql_direct() 추가, plsql 분기 수정 |
| `app.py` | citadel → robotics 전체, /callback/citadel → /callback/robotics, 404 수정 |
| `config.py` | citadel_url/api_key → robotics_url/api_key |
| `admin.py` | re-ingest plsql 분기 수정 |
| `models.py` | 변경 없음 |
| `schema.sql` | 변경 없음 |
| `job_store.py` | 변경 없음 |
| `.env.example` | CITADEL_* → ROBOTICS_*, SELF_URL 추가 |
| `CLAUDE.md` | 네이밍 + 파이프라인 흐름 업데이트 |
| `tests/` | citadel → robotics 전체 |

---

## 완료 조건

```bash
# 테스트 전체 통과
python -m pytest tests/ -v

# upload → body 파일: LightRAG 원문 + Robotics 제출 모두 호출
# upload → header 파일: LightRAG 원문만 호출
# /callback/robotics 알 수 없는 ID: 404 반환
# /files/{id}/re-ingest plsql 파일: 400 반환
```

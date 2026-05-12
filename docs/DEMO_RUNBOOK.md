# Pylon 파이프라인 시연 런북

**서버**: `http://193.168.195.222`  
**샘플 파일**: `docs/samples/`

---

## 서비스 URL

| 서비스 | URL | 역할 |
|--------|-----|------|
| Pylon | http://193.168.195.222:8001 | 수신 오케스트레이터 |
| Robotics | http://193.168.195.222:8004 | PL/SQL 역문서 |
| Forge | http://193.168.195.222:8003 | 문서 변환 (.docx/.pdf) |
| LightRAG | http://193.168.195.222:9621 | RAG 지식베이스 |
| OWU | http://193.168.195.222:3000 | 질의 UI |
| Nexus | http://193.168.195.222:8005 | 코드 지식 그래프 |

---

## Step 1. 상태 확인

```bash
curl http://193.168.195.222:8001/health
# → {"status":"ok"}
```

---

## Step 2. 파일 업로드 (3종)

### 2-1. PL/SQL → Robotics → LightRAG

```bash
curl -X POST http://193.168.195.222:8001/ingest/upload \
  -F "files=@docs/samples/LOAN_EVAL_PKG.pkb"
```

**응답 예시:**
```json
{"job_id": "xxxx", "status": "processing", "file_count": 1}
```

**내부 흐름:** Pylon → Robotics (LLM 역문서 생성) → callback → LightRAG 적재

---

### 2-2. 정책 문서 (.md) → LightRAG 직접

```bash
curl -X POST http://193.168.195.222:8001/ingest/upload \
  -F "files=@docs/samples/HCA_여신심사_정책.md"
```

**내부 흐름:** Pylon → LightRAG 직접 적재 (변환 불필요)

---

### 2-3. 사양서 (.docx) → Forge → LightRAG

```bash
curl -X POST http://193.168.195.222:8001/ingest/upload \
  -F "files=@docs/samples/HCA_여신심사_사양서.docx"
```

**내부 흐름:** Pylon → Forge (텍스트 추출) → callback → LightRAG 적재

---

### 2-4. 코드 파일 (.py) → Nexus (코드 지식 그래프)

```bash
curl -X POST "http://193.168.195.222:8001/ingest/upload?project_id=hca" \
  -F "files=@docs/samples/hca_loan_eval.py"
```

**응답 예시:**
```json
{"job_id": "xxxx", "status": "processing", "file_count": 1}
```

**내부 흐름:** Pylon → Nexus `/rebuild/upload?project_id=hca` → graphify update → graph.json 갱신

**완료 확인:**
```bash
curl http://193.168.195.222:8005/graph/stats?project_id=hca
# → {"nodes": N, "edges": M}
```

**시각화:**
브라우저: `http://193.168.195.222:8005/graph/html?project_id=hca`

---

## Step 3. 처리 상태 확인

```bash
# JOB_ID는 Step 2 응답에서 복사
curl http://193.168.195.222:8001/jobs/{JOB_ID}
```

**완료 상태:**
```json
{
  "status": "completed",
  "files": [
    {
      "file_path": "LOAN_EVAL_PKG.pkb",
      "external_status": "done",
      "rag_status": "ingested"
    }
  ]
}
```

**파일 타입별 처리 시간:**
| 타입 | 예상 소요 |
|------|----------|
| `.md` / `.txt` | 즉시 (~1초) |
| `.docx` / `.pdf` | ~5초 (Forge 변환) |
| `.pkb` / `.sql` | ~30~60초 (Robotics LLM) |
| `.py` / `.java` | ~5~10초 (graphify update) |

---

## Step 4. OWU에서 질의

브라우저: **http://193.168.195.222:3000**

### 추천 질문 3개

**① 심사 시뮬레이션**
```
신용점수 680점, 연소득 5천만원, 대출금 2억원 신청 시 심사 결과는?
단계별 판단 근거를 설명해줘.
```
> 예상 답변: 4등급(CONDITIONAL) + DTI 0.48 > 0.4 → REJECT

**② 정책 확인**
```
심사역 공동 승인이 필요한 조건과 절차는 무엇인가?
```
> 예상 답변: 신용 4등급 → CONDITIONAL → 심사역 2인 공동 승인

**③ 경계값 질문**
```
연소득 정확히 7천만원인 고객의 DTI 한도는 40%인가 50%인가?
```
> 예상 답변: 40% (7천만원 초과가 아닌 정확히 7천만원은 일반 기준 적용)

---

## Step 5. (선택) Nexus 그래프 확인

### 5-1. 브라우저 시각화

브라우저 (Chrome 권장): **http://193.168.195.222:8005/graph/html**

HCA 코드 지식 그래프 인터랙티브 뷰. 노드 클릭으로 관계 탐색.

### 5-2. API 조회

```bash
# HCA 코드 지식 그래프 통계
curl http://193.168.195.222:8005/graph/stats

# 핵심 노드 조회
curl http://193.168.195.222:8005/graph/god-nodes
```

---

## 전체 파이프라인 요약

```
업로드
  .pkb/.sql  ──→ Robotics (LLM 역문서) ──→ callback ──→ LightRAG
  .md/.txt   ──────────────────────────────────────→ LightRAG
  .docx/.pdf ──→ Forge (텍스트 추출)   ──→ callback ──→ LightRAG
  .py/.java  ──→ Nexus (graphify update, project_id별 격리)

조회
  LightRAG ──→ OWU (자연어 질의)
  Nexus    ──→ graph API / MCP (코드 구조 탐색)
```

---

## 통합테스트 결과 (2026-05-12 확인)

| 파이프라인 | 테스트 파일 | 결과 | 비고 |
|-----------|------------|------|------|
| Pylon → Forge → LightRAG | HCA_여신심사_정책_v2.docx | ✓ rag=ingested | Forge callback 정상, 마크다운 변환 후 적재 |
| Pylon → Robotics → LightRAG | LOAN_EVAL_PKG.pkb | ✓ rag=ingested | LLM 역문서 생성 후 callback → LightRAG |
| Pylon → LightRAG (text_doc) | HCA_여신심사_정책.md | ✓ rag=ingested | 변환 없이 직접 적재, ~1초 완료 |
| Pylon → Nexus (code) | hca_loan_eval.py | ✓ nodes=16, edges=22 | graphify 그래프 갱신 확인 |
| Forge → LightRAG (직접) | HCA_여신심사_정책.docx | ✓ callback 정상 | callback_url Query param으로 전달 |
| Robotics → LightRAG (직접) | LOAN_EVAL_PKG.pkb | ✓ LightRAG 200 OK | entities=12, relations=9 생성 |
| OWU 질의 | — | ✓ 확인 | 중간중간 병행 검증 완료 |

**로깅 확인:**
```bash
# Pylon 파이프라인 이력
docker logs pylon --tail=50 | grep -E '\[Pipeline\]|\[TextDoc\]|\[Nexus\]|\[LightRAG\]'

# Forge callback 이력
docker logs forge --tail=50 | grep -E 'Callback|worker'

# Robotics callback 이력
docker logs robotics --tail=50 | grep -E 'Callback|callback'
```

---

## 트러블슈팅

| 증상 | 확인 |
|------|------|
| `rag_status: failed` | `error` 필드 메시지 확인 |
| `.pkb` 60초 후에도 processing | Robotics 로그: `docker logs robotics --tail=20` |
| `.docx` callback 안 옴 | Forge 로그: `docker logs forge --tail=20` |
| OWU 답변 없음 | LightRAG 문서 목록: `GET /documents` |

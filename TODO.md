# Pylon TODO

## 🔴 우선순위 높음

### Bitbucket 파일 bytes fetch
- webhook/bulk 경로는 파일 경로만 수신 — 실제 bytes fetch 미구현
- Bitbucket API 클라이언트 작성 필요
- **선행 조건**: HCS 환경 Server/Cloud 여부 확인 후 webhook.py 조정

---

## 🟡 우선순위 중간

### Admin 인증
- 현재 `/admin` 엔드포인트 전부 인증 없이 열려있음
- API Key 기반 인증 추가 (Forge 방식 참고: `X-Forge-Key`)

### Nexus 볼륨 마운트
- 현재 업로드 파일/graph.json이 컨테이너 내부에만 저장 → 재시작 시 소멸
- **선행 조건**: 스토리지 방향 확정 (S3 vs 별도 컨테이너) 후 구현

---

## 🟢 HOLD (조건 충족 후)

### Robotics 프롬프트 픽스
- `plsql`/`dictionary` 프롬프트에 산문 서술체 강제 + 마크다운 표/불릿 금지
- `prompts.py` 수정 → `PUT /admin/prompts/{asset_type}` 업데이트
- **타이밍**: 시연 이후

### LightRAG MCP 서버
- 공식 MCP 서버 없음 → LightRAG REST API 래퍼 직접 제작
- **타이밍**: 운영 안정화 후

### HCS 통합 운영 스킬
- 4개 서비스(Forge/Robotics/Pylon/Nexus) 아키텍처 컨텍스트 + LLMOps 운영 가이드
- **타이밍**: Hostinger 운영 패턴 잡힌 후

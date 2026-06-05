# Pylon 설치 매뉴얼

> 포트: 8001 | 역할: Bitbucket PR Merge 이벤트 수신 → 파일별 파이프라인 오케스트레이션

---

## 1. 사전 조건

- Docker, Docker Compose 설치
- 외부 PostgreSQL 접근 가능 (Hostinger DB)
- Forge(:8003), Robotics(:8004), LightRAG, Nexus(:8005) 기동 상태

---

## 2. DB 생성 (최초 1회)

Pylon은 전용 DB를 사용한다. Hostinger PostgreSQL에 접속해서 생성:

```bash
# postgres 컨테이너 접속
docker exec -it <postgres_container_name> psql -U postgres

# psql 프롬프트에서
CREATE USER pylon WITH PASSWORD 'your_password';
CREATE DATABASE pylon_dev OWNER pylon;
\q
```

DATABASE_URL 형식:
```
postgresql://pylon:your_password@<hostinger_ip>:<port>/pylon_dev
```

---

## 3. 설치

```bash
# 1. 클론
git clone https://github.com/noplannomercy/Pylon.git
cd Pylon

# 2. 환경변수 설정
cp .env.example .env
vi .env
```

### .env 필수 항목

| 변수 | 설명 | 예시 |
|------|------|------|
| `DATABASE_URL` | Hostinger PostgreSQL DSN | `postgresql://user:pass@host:5432/dbname` |
| `BITBUCKET_WEBHOOK_SECRET` | Bitbucket Webhook HMAC 시크릿 | |
| `FORGE_URL` | Forge 서비스 URL | `http://forge:8003` |
| `FORGE_API_KEY` | Forge API 키 | |
| `ROBOTICS_URL` | Robotics 서비스 URL | `http://robotics:8004` |
| `ROBOTICS_API_KEY` | Robotics ADMIN_API_KEY | |
| `LIGHTRAG_URL` | LightRAG URL | `http://lightrag:9621` |
| `LIGHTRAG_API_KEY` | LightRAG API 키 | |
| `NEXUS_URL` | Nexus API URL | `http://nexus-api:8005` |
| `NEXUS_API_KEY` | Nexus API 키 | |
| `SELF_URL` | 콜백 수신용 자기 URL | `http://pylon:8001` |

> `ROBOTICS_URL`/`ROBOTICS_API_KEY`는 Robotics 역문서 서비스를 가리킴.

```bash
# 3. 빌드 + 기동
docker compose up -d --build

# 4. 헬스체크
curl http://localhost:8001/health
# 예상: {"status":"ok"}
```

> DB 스키마(`ingestion_job`, `ingestion_file`)는 기동 시 자동 생성됨.

---

## 4. Bitbucket Webhook 설정

Bitbucket 레포 → Repository settings → Webhooks:
- URL: `http://<서버IP>:8001/webhook/bitbucket`
- Events: `Pull Request: Merged`
- Secret: `.env`의 `BITBUCKET_WEBHOOK_SECRET` 값과 동일하게 설정

---

## 5. 주요 명령어

```bash
# 로그 확인
docker compose logs -f pylon

# 재시작
docker compose restart pylon

# 재빌드 후 교체
docker compose up -d --build

# 정지
docker compose down
```

---

## 6. 트러블슈팅

| 증상 | 원인 | 조치 |
|------|------|------|
| Webhook 401 | HMAC 시크릿 불일치 | `BITBUCKET_WEBHOOK_SECRET` 확인 |
| Forge 호출 실패 | URL/키 오류 | `FORGE_URL`, `FORGE_API_KEY` 확인 |
| Robotics 호출 실패 | URL/키 오류 | `ROBOTICS_URL`, `ROBOTICS_API_KEY` 확인 |
| DB 연결 실패 | DSN 오류 | `DATABASE_URL` 확인 |

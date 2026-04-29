# Pylon

Bitbucket PR Merge 이벤트를 수신해 파일별로 파이프라인을 구동하고 전 단계 상태를 추적하는 FastAPI 오케스트레이터 서비스. 포트 8001.

## 파이프라인 흐름

```
Bitbucket PR Merge
       │
       ▼
    Pylon (:8001)
       │
       ├── PL/SQL 파일 → Robotics (:8004)
       ├── 문서 파일   → Forge (:8003)
       └── 코드/기타  → skip
```

## 설치

자세한 설치 방법은 [`docs/INSTALL.md`](docs/INSTALL.md) 참조.

```bash
git clone https://github.com/noplannomercy/Pylon.git
cd Pylon
cp .env.example .env
vi .env
docker compose up -d --build
curl http://localhost:8001/health
```

## 스택

- Python 3.11+ / FastAPI / uvicorn
- PostgreSQL (Hostinger 원격)
- pydantic-settings

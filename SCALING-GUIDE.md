# Airflow Worker 스케일 아웃 가이드

## 목차
- [아키텍처 개요](#아키텍처-개요)
- [로컬 테스트 (Docker Compose)](#로컬-테스트-docker-compose)
- [프로덕션 배포 (별도 EC2)](#프로덕션-배포-별도-ec2)
- [모니터링](#모니터링)
- [트러블슈팅](#트러블슈팅)

## 아키텍처 개요

### 스케일 아웃 구조

```
┌─────────────────────────────────────┐
│  Core Server                        │
│  ┌───────────────────────────────┐  │
│  │ Airflow Webserver (UI:8080)  │  │
│  │ Airflow Scheduler             │  │
│  │ Airflow DAG Processor         │  │
│  └───────────────────────────────┘  │
│  ┌───────────────────────────────┐  │
│  │ PostgreSQL (5432)             │  │
│  │ Redis (6379)                  │  │
│  └───────────────────────────────┘  │
└──────────────┬──────────────────────┘
               │
        Redis + PostgreSQL
               │
    ┌──────────┼──────────┬──────────┐
    │          │          │          │
┌───▼────┐ ┌──▼─────┐ ┌──▼─────┐ ┌──▼─────┐
│Worker 1│ │Worker 2│ │Worker 3│ │Worker N│
│(Celery)│ │(Celery)│ │(Celery)│ │(Celery)│
└────────┘ └────────┘ └────────┘ └────────┘
```

### 컴포넌트 역할

| 컴포넌트 | 역할 | 스케일링 |
|---------|------|---------|
| **Webserver** | UI 제공, API 서버 | 1개 (Core) |
| **Scheduler** | DAG 스케줄링, Task 분배 | 1개 (Core) |
| **DAG Processor** | DAG 파일 파싱 | 1개 (Core) |
| **PostgreSQL** | 메타데이터 저장소 | 1개 (Core) |
| **Redis** | Celery 메시지 브로커 | 1개 (Core) |
| **Worker** | Task 실행 | **N개 (확장 가능)** |

## 로컬 테스트 (Docker Compose)

### 1. 기본 설정 파일

`docker-compose-scalable.yaml` 파일이 생성되어 있습니다.

### 2. 전체 시스템 시작 (Core + Worker 3개)

```bash
# 1단계: Docker 이미지 빌드
docker build -t team5-airflow:custom .

# 2단계: DB 초기화
docker compose -f docker-compose-scalable.yaml up airflow-init

# 3단계: 전체 시스템 시작 (Worker 3개)
docker compose -f docker-compose-scalable.yaml up -d --scale airflow-worker=3
```

### 3. Core만 시작하고 Worker 나중에 추가

```bash
# Core 서비스만 시작
docker compose -f docker-compose-scalable.yaml up -d \
  postgres redis airflow-webserver airflow-scheduler airflow-dag-processor

# Worker 1개 추가
docker compose -f docker-compose-scalable.yaml up -d airflow-worker

# Worker 2개 더 추가 (총 3개)
docker compose -f docker-compose-scalable.yaml up -d --scale airflow-worker=3
```

### 4. Worker 동적 스케일링

```bash
# Worker 5개로 증설
docker compose -f docker-compose-scalable.yaml up -d --scale airflow-worker=5

# Worker 2개로 축소
docker compose -f docker-compose-scalable.yaml up -d --scale airflow-worker=2

# Worker 상태 확인
docker compose -f docker-compose-scalable.yaml ps
```

### 5. 컨테이너 상태 확인

```bash
# 실행 중인 컨테이너 목록
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Worker 로그 확인
docker compose -f docker-compose-scalable.yaml logs -f airflow-worker

# 특정 Worker 로그 확인
docker logs catchdata-airflow-airflow-worker-1 -f
```

### 6. Flower로 Worker 모니터링

```bash
# Flower 시작 (이미 docker-compose에 포함)
docker compose -f docker-compose-scalable.yaml up -d flower

# 브라우저에서 접속
# http://localhost:5555
```

**Flower에서 확인 가능한 정보:**
- 실행 중인 Worker 수
- Worker별 처리 중인 Task
- Task 성공/실패 통계
- Worker별 CPU/메모리 사용량

### 7. 시스템 종료

```bash
# 전체 중지
docker compose -f docker-compose-scalable.yaml down

# 볼륨까지 삭제 (데이터 초기화)
docker compose -f docker-compose-scalable.yaml down -v
```

## 프로덕션 배포 (별도 EC2)

### 아키텍처

```
┌─────────────────────────────────────┐
│  Core EC2 (Private Subnet)          │
│  Private IP: 10.0.1.100             │
│  - Webserver, Scheduler, DB, Redis  │
└──────────────┬──────────────────────┘
               │ Security Group 설정
               │ Allow: 5432, 6379
    ┌──────────┼──────────┬──────────┐
    │          │          │          │
┌───▼──────────────┐ ┌───▼──────────────┐
│ Worker EC2-1     │ │ Worker EC2-2     │
│ Private Subnet   │ │ Private Subnet   │
│ 10.0.2.101       │ │ 10.0.2.102       │
└──────────────────┘ └──────────────────┘
```

### 1. Core 서버 설정 (EC2-1)

#### docker-compose-core.yaml 생성

```yaml
version: '3.8'

x-airflow-common:
  &airflow-common
  image: team5-airflow:custom
  env_file:
    - .env
  environment:
    &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: CeleryExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
    AIRFLOW__CELERY__BROKER_URL: redis://:@redis:6379/0
    AIRFLOW__CORE__FERNET_KEY: ''
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW_CONFIG: '/opt/airflow/config/airflow.cfg'
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./config:/opt/airflow/config
    - ./plugins:/opt/airflow/plugins
    - ./dbt:/opt/airflow/dbt

services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    ports:
      - "0.0.0.0:5432:5432"  # 모든 인터페이스에서 접근 허용
    restart: always

  redis:
    image: redis:7.2-bookworm
    ports:
      - "0.0.0.0:6379:6379"  # 모든 인터페이스에서 접근 허용
    restart: always

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8080:8080"
    restart: always
    depends_on:
      - postgres
      - redis

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    restart: always
    depends_on:
      - postgres
      - redis

  airflow-dag-processor:
    <<: *airflow-common
    command: dag-processor
    restart: always
    depends_on:
      - postgres
      - redis

volumes:
  postgres-db-volume:
```

#### Core 서버 시작

```bash
# Core 서버 (EC2-1)
cd /home/ubuntu/CatchData-Airflow/catchdata-airflow

# 이미지 빌드
docker build -t team5-airflow:custom .

# DB 초기화
docker compose -f docker-compose-core.yaml up airflow-init

# Core 서비스 시작
docker compose -f docker-compose-core.yaml up -d
```

### 2. Worker 서버 설정 (EC2-2, EC2-3)

#### docker-compose-worker.yaml 생성

```yaml
version: '3.8'

services:
  airflow-worker:
    image: team5-airflow:custom
    command: celery worker
    environment:
      AIRFLOW__CORE__EXECUTOR: CeleryExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@10.0.1.100:5432/airflow
      AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@10.0.1.100:5432/airflow
      AIRFLOW__CELERY__BROKER_URL: redis://:@10.0.1.100:6379/0
      DUMB_INIT_SETSID: "0"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
      - ./dbt:/opt/airflow/dbt
    restart: always
    deploy:
      resources:
        limits:
          cpus: '2.0'
          memory: 4G
```

**중요:** `10.0.1.100`을 Core 서버의 실제 Private IP로 변경하세요!

#### Worker 서버 시작

```bash
# Worker 서버 (EC2-2)
cd /home/ubuntu/CatchData-Airflow/catchdata-airflow

# Docker 이미지 빌드 (Core와 동일한 이미지)
docker build -t team5-airflow:custom .

# Worker 1개 시작
docker compose -f docker-compose-worker.yaml up -d

# Worker 3개 시작
docker compose -f docker-compose-worker.yaml up -d --scale airflow-worker=3
```

### 3. AWS 보안 그룹 설정

#### Core 서버 보안 그룹 (Inbound)

| 타입 | 프로토콜 | 포트 | 소스 | 설명 |
|------|---------|-----|------|------|
| Custom TCP | TCP | 5432 | Worker SG | PostgreSQL |
| Custom TCP | TCP | 6379 | Worker SG | Redis |
| Custom TCP | TCP | 8080 | Bastion SG | Webserver UI |
| SSH | TCP | 22 | Bastion SG | SSH 접근 |

#### Worker 서버 보안 그룹 (Inbound)

| 타입 | 프로토콜 | 포트 | 소스 | 설명 |
|------|---------|-----|------|------|
| SSH | TCP | 22 | Bastion SG | SSH 접근 |

**Outbound 규칙:**
- Core/Worker 모두: All traffic 허용 (0.0.0.0/0)

### 4. 환경변수 동기화

모든 서버(Core, Worker)에서 동일한 환경변수를 사용해야 합니다.

**.env 파일 (모든 서버에 동일하게 배포):**
```env
# AWS Credentials
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_DEFAULT_REGION=ap-northeast-2

# Airflow Variables (Core 서버에서 UI로 설정)
# KAKAO_REST_API_KEY, SLACK_WEBHOOK_URL, S3_BUCKET_NAME
```

**중요:** Worker는 `.env` 파일과 DAG 파일이 필요합니다!

### 5. DAG 파일 동기화

Worker 서버에도 DAG 파일이 필요합니다. 다음 방법 중 하나 선택:

#### 방법 1: Git Pull (추천)
```bash
# 모든 서버에서 동일하게 실행
cd /home/ubuntu/CatchData-Airflow
git pull origin main
```

#### 방법 2: rsync
```bash
# Core 서버에서 Worker 서버로 동기화
rsync -avz --delete \
  /home/ubuntu/CatchData-Airflow/catchdata-airflow/dags/ \
  worker-server:/home/ubuntu/CatchData-Airflow/catchdata-airflow/dags/
```

#### 방법 3: NFS/EFS (프로덕션)
AWS EFS를 사용하여 DAG 파일을 공유하는 방법이 가장 권장됩니다.

## 모니터링

### 1. Flower Web UI

```bash
# Core 서버에서 Flower 시작
docker run -d \
  --name airflow-flower \
  --network host \
  -e AIRFLOW__CELERY__BROKER_URL=redis://:@localhost:6379/0 \
  team5-airflow:custom \
  celery --app airflow.providers.celery.executors.celery_executor.app flower

# 브라우저 접속
# http://core-server:5555
```

### 2. Worker 상태 확인

```bash
# Airflow CLI로 확인
docker exec airflow-scheduler airflow celery workers

# 출력 예시:
# celery@worker-1: OK
# celery@worker-2: OK
# celery@worker-3: OK
```

### 3. Task Queue 확인

```bash
# Redis CLI로 큐 확인
docker exec -it airflow-redis redis-cli

# Redis CLI에서:
> LLEN default  # 대기 중인 Task 수
> KEYS celery-task-meta-*  # 완료된 Task
```

### 4. Airflow UI에서 확인

**Admin → Celery 메뉴:**
- Workers: 연결된 Worker 수와 상태
- Tasks: 실행 중/대기 중 Task
- Queues: Task 큐 상태

## 트러블슈팅

### 문제 1: Worker가 Core 서버에 연결되지 않음

**증상:**
```
[ERROR] Failed to connect to redis://10.0.1.100:6379/0
```

**해결:**
1. 보안 그룹 확인
   ```bash
   # Core 서버에서
   sudo netstat -tlnp | grep 6379
   sudo netstat -tlnp | grep 5432
   ```

2. 네트워크 연결 테스트
   ```bash
   # Worker 서버에서
   telnet 10.0.1.100 6379
   telnet 10.0.1.100 5432
   ```

3. Docker 네트워크 설정
   ```bash
   # Core 서버에서 Redis/PostgreSQL 0.0.0.0 바인딩 확인
   docker compose -f docker-compose-core.yaml config
   ```

### 문제 2: DAG 파일을 찾을 수 없음

**증상:**
```
[WARNING] DAG file does not exist: /opt/airflow/dags/ver2_01_kakao_crawl_all_in_one.py
```

**해결:**
```bash
# Worker 서버에서 DAG 파일 확인
ls -la /home/ubuntu/CatchData-Airflow/catchdata-airflow/dags/

# 볼륨 마운트 확인
docker inspect airflow-worker | grep -A 10 Mounts
```

### 문제 3: Task가 Worker에 할당되지 않음

**증상:**
- Scheduler는 정상이지만 Task가 실행되지 않음

**해결:**
```bash
# 1. Worker 로그 확인
docker logs airflow-worker

# 2. Celery Worker 재시작
docker restart airflow-worker

# 3. Redis 큐 초기화 (주의!)
docker exec -it airflow-redis redis-cli FLUSHALL
```

### 문제 4: Worker 메모리 부족

**증상:**
```
[ERROR] Out of memory
```

**해결:**
```bash
# Worker 리소스 제한 조정 (docker-compose-worker.yaml)
deploy:
  resources:
    limits:
      cpus: '4.0'      # CPU 증가
      memory: 8G       # 메모리 증가
```

### 문제 5: 환경변수 동기화 문제

**증상:**
- Core에서는 정상이지만 Worker에서 실패

**해결:**
```bash
# Worker 서버에 .env 파일 복사
scp core-server:/home/ubuntu/CatchData-Airflow/catchdata-airflow/.env \
    worker-server:/home/ubuntu/CatchData-Airflow/catchdata-airflow/

# Worker 재시작
docker compose -f docker-compose-worker.yaml restart
```

## 성능 튜닝

### Worker 동시 실행 Task 수 조정

```bash
# docker-compose-worker.yaml에서 설정
command: celery worker --concurrency=4  # 동시 4개 Task 실행
```

### Celery Worker 옵션

```yaml
command: celery worker \
  --concurrency=4 \
  --max-tasks-per-child=100 \  # 100개 Task 후 Worker 재시작 (메모리 누수 방지)
  --time-limit=3600 \           # Task 타임아웃 1시간
  --soft-time-limit=3300        # Soft 타임아웃 55분
```

### Auto-scaling (Kubernetes)

프로덕션 환경에서는 Kubernetes HPA 사용 권장:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: airflow-worker-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: airflow-worker
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
```

## 참고 자료

- [Airflow CeleryExecutor 공식 문서](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/celery.html)
- [Celery Worker 옵션](https://docs.celeryq.dev/en/stable/userguide/workers.html)
- [Docker Compose Scale](https://docs.docker.com/compose/reference/scale/)

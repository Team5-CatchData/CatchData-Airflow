# CatchData-Airflow - 데이터 파이프라인

## 목차

- [프로젝트 소개](#-프로젝트-소개)
- [주요 기능](#-주요-기능)
- [기술 스택](#-기술-스택)
- [시스템 아키텍처](#-시스템-아키텍처)
- [설치 및 실행](#-설치-및-실행)
- [프로젝트 구조](#-프로젝트-구조)
- [DAG 구성](#-dag-구성)

## 프로젝트 소개

CatchData-Airflow는 **스마트 맛집 추천 플랫폼**을 위한 데이터 파이프라인입니다. Apache Airflow를 사용하여 카카오맵 크롤링부터 데이터 웨어하우스 구축, 실시간 추천 스코어 계산까지 전체 데이터 흐름을 자동화합니다.

### 핵심 가치

- **자동화된 데이터 수집**: 카카오맵 API와 Selenium을 활용한 일일 맛집 데이터 크롤링
- **확장 가능한 데이터 웨어하우스**: AWS Redshift 기반 분석 플랫폼
- **실시간 추천 시스템**: 시간대별 방문 패턴과 대기 정보를 반영한 동적 추천 스코어
- **안정적인 운영**: Slack 알림 기반 DAG 모니터링 및 장애 감지

## 주요 기능

### 1. 카카오맵 크롤링 (ver2_01)
- **카카오맵 REST API**를 통한 맛집 기본 정보 수집
- **Selenium 기반 웹 크롤링**으로 실시간 대기 정보 획득
- OCR(Tesseract)을 활용한 대기 인원 이미지 파싱
- 멀티프로세싱으로 크롤링 성능 최적화
- **AWS S3** 자동 업로드 (CSV 형식)

**수집 데이터:**
- 식당명, 주소, 전화번호, 카테고리, 평점
- 실시간 대기 팀 수 및 방문자 수
- 좌표 (위도/경도)

### 2. S3 → Redshift 데이터 적재
- S3에서 Redshift로 일일 크롤링 데이터 자동 로드
- `COPY` 명령 사용으로 대용량 데이터 효율적 처리
- 중복 제거 및 데이터 정합성 검증

### 3. 정적 피처 엔지니어링
- **시간대별 방문 패턴 분석** (time0~time23)
- **품질 점수(quality_score)** 계산
  - 평점, 리뷰 수, 카테고리 가중치 반영
- **클러스터링 기반 유사 맛집 그룹핑**
- 정규화된 base population 계산

**생성 테이블:**
```sql
analytics.derived_features_base (
    id, base_population, quality_score,
    rating, time0~time23, cluster, ...
)
```

### 4. 실시간 추천 스코어 계산
- **3가지 추천 전략** 구현
  - `rec_quality`: 품질 중심 (평점/리뷰 60%)
  - `rec_balanced`: 균형 중심 (품질 40% + 대기 35%)
  - `rec_convenience`: 편의성 중심 (대기시간 50%)
- 현재 시간대 방문 패턴 반영
- 실시간 대기 인원 기반 동적 계산

**추천 로직:**
```
rec_quality = 0.6×quality + 0.2×rating + 0.2×(1-wait_norm)
rec_balanced = 0.4×quality + 0.35×rating + 0.25×(1-wait_norm)
rec_convenience = 0.3×quality + 0.2×rating + 0.5×(1-wait_norm)
```

### 5. 맵 검색 데이터 수집
- 사용자 맵 검색 이력 추적
- 지역/도시/카테고리별 검색 통계
- 대시보드 필터링 데이터 소스

### 6. DAG 모니터링 (monitor_dag_status)
- **실패한 DAG/Task 자동 감지**
- 30분 이상 running 상태인 DAG 알림
- **Slack 웹훅**을 통한 실시간 알림

**알림 시나리오:**
```
Airflow DAG 모니터링 알림
실패한 DAG: kakao_crawl_v2 (2025-12-26 14:30)
실패한 Task: load_to_redshift (2025-12-26 14:35)
장시간 실행 DAG: feature_engineering (35분 경과)
```

### 7. DBT 분석 파이프라인 (dbt_analytics_daily)
- DBT 모델 자동 실행
- 일일 분석 지표 계산
- 데이터 품질 검증

## 기술 스택

### Workflow Orchestration
- **Apache Airflow 3.1.2** - 워크플로우 관리
- **CeleryExecutor** - 분산 작업 처리
- **Redis** - 메시지 브로커
- **PostgreSQL** - 메타데이터 저장소

### Data Collection
- **Selenium 4.10.0** - 웹 크롤링
- **Tesseract OCR** - 이미지 텍스트 인식
- **OpenCV** - 이미지 전처리
- **Kakao REST API** - 맛집 정보 조회

### Data Warehouse
- **Amazon Redshift** - 분석용 데이터베이스
- **Amazon S3** - 데이터 레이크
- **DBT** - 데이터 변환

### DevOps
- **Docker Compose** - 컨테이너 오케스트레이션
- **GitHub Actions** - CI/CD
  - Ruff 코드 품질 검사
  - EC2 자동 배포 (Bastion Host 경유)
- **AWS EC2** - Airflow 서버 호스팅

### Monitoring
- **Slack Webhooks** - 알림
- **Airflow UI** - DAG 모니터링

## 시스템 아키텍처

### 데이터 파이프라인 흐름

```


### Airflow 아키텍처

```
┌──────────────────────────────────────────────────────┐
│                 Docker Compose                       │
├──────────────────────────────────────────────────────┤
│                                                      │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────┐ │
│  │   Scheduler  │  │   Webserver  │  │  Worker   │ │
│  │              │  │   (UI:8080)  │  │  (Celery) │ │
│  └──────┬───────┘  └──────────────┘  └─────┬─────┘ │
│         │                                   │       │
│         └────────────┬──────────────────────┘       │
│                      ▼                              │
│         ┌────────────────────────┐                  │
│         │  PostgreSQL (Metadata) │                  │
│         └────────────────────────┘                  │
│                      ▼                              │
│         ┌────────────────────────┐                  │
│         │   Redis (Message Q)    │                  │
│         └────────────────────────┘                  │
│                                                      │
│  Volumes:                                           │
│  - ./dags       → DAG 파일                          │
│  - ./logs       → 실행 로그                         │
│  - ./config     → airflow.cfg                       │
│  - ./plugins    → 커스텀 플러그인                    │
└──────────────────────────────────────────────────────┘
```

### CI/CD 파이프라인

```
┌─────────────────┐
│  GitHub Push    │
│   (main 브랜치)  │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────┐
│    GitHub Actions               │
├─────────────────────────────────┤
│  1. Ruff 코드 검사              │
│     - Linting (F,E,W,I,B,S)    │
│     - 코드 품질 검증            │
│                                 │
│  2. CD (main 브랜치만)          │
│     - SSH Bastion Host         │
│     - EC2 git pull             │
└────────┬────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│   EC2 (Private Subnet)          │
│   Bastion Host 경유             │
├─────────────────────────────────┤
│  cd /home/ubuntu/CatchData-     │
│     Airflow                     │
│  git pull origin main           │
│  → Airflow 자동 DAG 갱신        │
└─────────────────────────────────┘
```

## 설치 및 실행

### 사전 요구사항
- Docker & Docker Compose
- AWS 계정 (S3, Redshift, EC2)
- Kakao Developers API Key
- Slack Webhook URL (선택)

### 1. 저장소 클론
```bash
git clone https://github.com/Team5-CatchData/CatchData-Airflow.git
cd CatchData-Airflow/catchdata-airflow
```

### 2. 환경변수 설정
`.env` 파일을 생성하고 다음 내용을 설정:
```env
# Airflow
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=

# AWS Credentials
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_DEFAULT_REGION=

# S3
S3_BUCKET_NAME=

# Redshift
REDSHIFT_HOST=
REDSHIFT_PORT=
REDSHIFT_DB=
REDSHIFT_USER=
REDSHIFT_PASSWORD=

# API Keys (Airflow Variables)
KAKAO_REST_API_KEY=
SLACK_WEBHOOK_URL=
```

### 3. Docker 이미지 빌드

install-airflow-dbt.md 참고

### 5. Airflow 웹 UI 접속
브라우저에서 접속(해당 프로젝트는 bastion을 통하여 private subnet에서 실행):
- URL: `http://localhost:8080`
- 기본 계정: `airflow / airflow`

### 6. Airflow Connection 설정
Airflow UI → Admin → Connections에서 redshift와 RDS 커넥션 추가

### 7. Airflow Variables 설정
Admin → Variables에서 추가:
```json
{
  "KAKAO_REST_API_KEY": "your_api_key",
  "SLACK_WEBHOOK_URL": "https://hooks.slack.com/services/...",
  "S3_BUCKET_NAME": "your-bucket-name"
}
```

### 8. DAG 실행
- Webserver UI에서 DAG 활성화
- 수동 실행: DAG 선택 → Trigger DAG

### 9. 서비스 중지
```bash
docker compose down
```

### 10. 로그 확인
```bash
# 전체 로그
docker compose logs -f

# 특정 서비스 로그
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-worker
```

## 프로젝트 구조

```
CatchData-Airflow/
├── .github/
│   └── workflows/
│       ├── ruffcheck.yml        # Ruff 린팅 (push/PR 시)
│       └── CD.yml               # EC2 자동 배포 (main 브랜치)
│
├── catchdata-airflow/
│   ├── dags/                    # Airflow DAG 파일
│   │   ├── ver2_01_kakao_crawl_all_in_one.py
│   │   ├── ver2_02_load_s3_to_redshift.py
│   │   ├── ver2_03_redshift_static_feature_update.py
│   │   ├── ver2_04_calculate_realtime_scores.py
│   │   ├── ver2_05_map_search.py
│   │   ├── monitor_dag_status.py
│   │   └── dbt_analytics_daily.py
│   │
│   ├── dbt/                     # DBT 프로젝트
│   │   └── common/
│   │       └── analyses/        # SQL 분석 쿼리
│   │
│   ├── config/
│   │   └── airflow.cfg          # Airflow 설정
│   │
│   ├── logs/                    # 실행 로그
│   ├── plugins/                 # 커스텀 플러그인
│   │
│   ├── docker-compose.yaml      # Docker 서비스 정의
│   ├── Dockerfile               # 커스텀 Airflow 이미지
│   ├── requirements.txt         # Python 의존성
│   └── .env                     # 환경변수 (git ignore)
│
├── install-airflow-dbt.sh       # 설치 스크립트
├── install-airflow-dbt.md       # 설치 가이드
└── README.md                    # 프로젝트 문서
```

## DAG 구성

### 1. ver2_01_kakao_crawl_all_in_one
**스케줄**: 매일 00:00 KST
**실행 시간**: 약 30-60분

```python
DAG 구조:
start
  → crawl_and_upload_to_s3  # 멀티프로세스 크롤링 + S3 업로드
  → slack_success           # 성공 알림
  → end
```

**주요 작업:**
1. 카카오맵 API로 식당 기본 정보 수집
2. Selenium으로 대기 정보 크롤링
3. OCR로 대기 인원 이미지 파싱
4. CSV 생성 및 S3 업로드

### 2. ver2_02_load_s3_to_redshift
**스케줄**: 매일 01:00 KST (ver2_01 이후)
**실행 시간**: 약 5-10분

```python
DAG 구조:
start
  → drop_temp_table
  → create_temp_table
  → load_from_s3           # COPY 명령
  → merge_to_main          # 중복 제거 병합
  → drop_temp_table
  → end
```

### 3. ver2_03_redshift_static_feature_update
**스케줄**: 매일 02:00 KST
**실행 시간**: 약 10-20분

```python
DAG 구조:
start
  → calculate_features     # 시간대별 집계
  → update_quality_score   # 품질 점수 계산
  → clustering             # 유사 맛집 그룹핑
  → end
```

**생성 피처:**
- `time0` ~ `time23`: 시간대별 방문자 수
- `quality_score`: 평점, 리뷰 수, 카테고리 반영
- `cluster`: K-Means 클러스터 ID
- `base_population`: 정규화된 기본 방문자 수

### 4. ver2_04_calculate_realtime_scores
**스케줄**: 매시간 정각 (Hourly)
**실행 시간**: 약 2-5분

```python
DAG 구조:
start
  → create_realtime_table
  → calculate_scores       # Python 연산
  → insert_scores          # Redshift 삽입
  → end
```

**계산 로직:**
```python
# 현재 시각 기준 방문 패턴 반영
now_hour = datetime.now().hour
time_column = f"time{now_hour}"

# 3가지 추천 전략
rec_quality = 0.6*quality + 0.2*rating + 0.2*(1-wait)
rec_balanced = 0.4*quality + 0.35*rating + 0.25*(1-wait)
rec_convenience = 0.3*quality + 0.2*rating + 0.5*(1-wait)
```

### 5. ver2_05_map_search
**스케줄**: 매일 03:00 KST
**실행 시간**: 약 5분

```python
DAG 구조:
start
  → aggregate_search_logs  # 사용자 검색 통계
  → update_dashboard_data  # 대시보드 데이터 갱신
  → end
```

### 6. monitor_dag_status
**스케줄**: 매시간 정각 (Hourly)
**실행 시간**: 약 1분

```python
DAG 구조:
start
  → check_failed_dags      # 실패한 DAG 감지
  → check_long_running     # 30분+ running DAG 감지
  → send_slack_alert       # Slack 알림
  → end
```

### 7. dbt_analytics_daily
**스케줄**: 매일 04:00 KST
**실행 시간**: 약 10분

```python
DAG 구조:
start
  → dbt_run                # DBT 모델 실행
  → dbt_test               # 데이터 품질 검증
  → end
```

## 주요 테이블 스키마

### raw_data.eating_house (원본 데이터)
```sql
CREATE TABLE raw_data.eating_house (
    id BIGINT PRIMARY KEY,
    place_name VARCHAR(255),
    address_name VARCHAR(500),
    road_address_name VARCHAR(500),
    phone VARCHAR(50),
    category_name VARCHAR(100),
    x DECIMAL(20, 16),  -- 경도
    y DECIMAL(20, 16),  -- 위도
    place_url VARCHAR(500),
    rating DECIMAL(3, 2),
    review_count INTEGER,
    waiting_teams INTEGER,
    current_visitors INTEGER,
    created_at TIMESTAMP DEFAULT GETDATE()
);
```

### analytics.derived_features_base (정적 피처)
```sql
CREATE TABLE analytics.derived_features_base (
    id BIGINT PRIMARY KEY,
    base_population NUMERIC(18, 4),
    quality_score NUMERIC(18, 4),
    rating NUMERIC(3, 2),
    time0 NUMERIC(18, 4),
    time1 NUMERIC(18, 4),
    ...
    time23 NUMERIC(18, 4),
    cluster INTEGER,
    category_name VARCHAR(100),
    updated_at TIMESTAMP DEFAULT GETDATE()
);
```

### analytics.realtime_waiting (실시간 추천)
```sql
CREATE TABLE analytics.realtime_waiting (
    id BIGINT,
    current_visitors NUMERIC(18, 4),
    waiting INTEGER,
    rec_quality NUMERIC(18, 6),
    rec_balanced NUMERIC(18, 6),
    rec_convenience NUMERIC(18, 6),
    calculation_timestamp TIMESTAMP
)
DISTKEY(id)
SORTKEY(calculation_timestamp);
```

## 성능 최적화

### 크롤링 최적화
- **멀티프로세싱**: CPU 코어 수 기반 병렬 처리
- **ChromeDriver Lock**: 동시 다운로드 방지 (threading.Lock)
- **헤드리스 모드**: GUI 없이 크롤링 하기 위함 + GPU 렌더링으로 인한 메모리 낭비 X

### Redshift 최적화
- **DISTKEY**: `id` 컬럼으로 분산 (JOIN 성능 향상)
- **SORTKEY**: `calculation_timestamp` 기준 정렬 (시계열 쿼리 최적화)
- **COPY 명령**: S3에서 대용량 데이터 고속 로드

### Airflow 최적화
- **CeleryExecutor**: Worker 수평 확장 가능
- **Task 병렬화**: 독립적인 Task는 동시 실행
- **Connection Pool**: DB 연결 재사용

## 모니터링 대시보드

### Airflow UI 주요 화면
1. **DAGs 뷰**: 전체 DAG 상태 한눈에 확인
2. **Graph 뷰**: Task 간 의존성 시각화
3. **Gantt 차트**: Task 실행 시간 분석
4. **Logs**: 각 Task 실행 로그 확인

## 기여 가이드

### 코드 스타일
- **Ruff** 린터 규칙 준수
- 체크 항목: `F,E,W,I,B,S` (보안, 품질, 가독성)
- 예외: `E501` (라인 길이), `S608` (SQL 인젝션 경고)

### Pull Request 프로세스
1. Feature 브랜치 생성
2. Ruff 검사 통과 확인
3. PR 생성 (main 브랜치로)
4. 리뷰 후 merge → 자동 배포

### 로컬 Ruff 검사
```bash
# 설치
pip install ruff

# 검사
ruff check . --select F,E,W,I,B,S --ignore E501,S608,S110

# 자동 수정
ruff check . --fix
```

## 참고 자료

- [Apache Airflow 공식 문서](https://airflow.apache.org/docs/)
- [Redshift COPY 명령](https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html)
- [Selenium Python 가이드](https://selenium-python.readthedocs.io/)
- [DBT 문서](https://docs.getdbt.com/)

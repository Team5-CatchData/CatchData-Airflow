import pandas as pd
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import Numeric, String, Integer, BigInteger, Float, text

# =========================
# 기본 설정
# =========================
REDSHIFT_CONN_ID = "redshift_conn"
SCHEMA_NAME = "analytics"
RAW_TABLE1 = "raw_data.kakao_crawl"
RAW_TABLE2 = "analytics.realtime_waiting"
RAW_TABLE3 = "analytics.derived_features_base"
FINAL_TABLE_NAME = "map_search"
SLACK_WEBHOOK_URL = ("https://hooks.slack.com/services/T09SZ0BSHEU"
                     "/B0A3W3R4H9D/Ea5DqrFBnQKc3SzbSuNhcmZo")

# =========================
# 💡 SQL: 최종 테이블 생성 스키마
# =========================
FINAL_TABLE_CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{FINAL_TABLE_NAME} (
    id BIGINT,
    name VARCHAR(50),
    region VARCHAR(50),
    city VARCHAR(50),
    category VARCHAR(100),
    x FLOAT,
    y FLOAT,
    waiting INTEGER,
    rating FLOAT,
    phone VARCHAR(50),
    image_url VARCHAR(500),
    address VARCHAR(300),
    rec_quality FLOAT,
    rec_balanced FLOAT,
    rec_convenience FLOAT,
    cluster INTEGER
) DISTSTYLE EVEN;
"""

def full_static_feature_pipeline():
    """
    원본 및 실시간 테이블을 조인하여 검색 전용 테이블을 생성하고 원자적으로 갱신합니다.
    """

    # Redshift Hook 초기화
    redshift_hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)
    engine = redshift_hook.get_sqlalchemy_engine()

    # 1. Redshift에서 통합 데이터 로드
    print("--- 1. Redshift에서 원본 데이터 로드 시작 ---")
    sql_select = f"""
    SELECT 
        A.id,
        A.place_name as name,
        A.address_name as address,
        A.category_name,
        A.x,
        A.y,
        B.waiting,
        A.rating,
        A.phone,
        A.img_url as image_url,
        B.rec_quality,
        B.rec_balanced,
        B.rec_convenience,
        C.cluster
    FROM {RAW_TABLE1} A
    INNER JOIN {RAW_TABLE2} B 
        ON A.id = B.id
    LEFT JOIN {RAW_TABLE3} C
        ON A.id = C.id;
    """

    df = redshift_hook.get_pandas_df(sql_select)

    if df.empty:
        print("경고: 원본 테이블에 데이터가 없습니다. 파이프라인을 종료합니다.")
        return

    # [중복 제거 로직 추가] id 기준으로 하나만 남김
    # print(f"중복 제거 전 데이터: {len(df)}개")
    # df = df.drop_duplicates(subset=['id'], keep='first')
    # print(f"✅ 중복 제거 후 데이터: {len(df)}개")

    # 2. 데이터 전처리
    print("--- 2. 데이터 전처리 시작 ---")
    df['category'] = df['category_name'].str.split('>').str[-1].str.strip()
    address_split = df['address'].str.split(' ', n=2, expand=True)
    df['region'] = address_split[0]
    df['city'] = address_split[1]

    final_df = df[['id', 'name', 'region', 'city', 'category', 'x', 'y', 
                   'waiting', 'rating', 'phone', 'image_url', 'address',
                   'rec_quality', 'rec_balanced', 'rec_convenience', 'cluster']].copy()

    # 3. Redshift 원자적 교체 실행
    STAGING_TABLE = f"{FINAL_TABLE_NAME}_staging"
    BACKUP_TABLE = f"{FINAL_TABLE_NAME}_old"

    # 3-1. [에러 방지] Staging 테이블 명시적 삭제
    print(f"--- 3-1. 기존 Staging 테이블 정리: {STAGING_TABLE} ---")
    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {SCHEMA_NAME}.{STAGING_TABLE};"))
        conn.execute(text("COMMIT;"))

    # 데이터 타입 매핑
    dtype_mapping = {
        'id': BigInteger(),
        'x': Numeric(15, 12),
        'y': Numeric(15, 12),
        'rating': Numeric(3, 1),
        'name': String(50),
        'category': String(50),
        'image_url': String(500),
        'address': String(300),
        'rec_quality': Numeric(15, 14),
        'rec_balanced': Numeric(15, 14),
        'rec_convenience': Numeric(15, 14)
    }

    # Staging 테이블 로드 (if_exists='fail'로 설정하여 충돌 방지)
    print(f"--- 3-2. Staging 테이블 로드 실행 ---")
    final_df.to_sql(
        name=STAGING_TABLE,
        con=engine,
        schema=SCHEMA_NAME,
        if_exists='fail', 
        index=False,
        dtype=dtype_mapping
    )

    # 3-3. 트랜잭션 시작 (Atomic Swap)
    # RENAME 구문에서 스키마명을 제외하여 Redshift 표준 준수
    sql_commands = f"""
    BEGIN;
    DROP TABLE IF EXISTS {SCHEMA_NAME}.{BACKUP_TABLE};
    ALTER TABLE {SCHEMA_NAME}.{FINAL_TABLE_NAME} RENAME TO {BACKUP_TABLE};
    ALTER TABLE {SCHEMA_NAME}.{STAGING_TABLE} RENAME TO {FINAL_TABLE_NAME};
    COMMIT;
    DROP TABLE IF EXISTS {SCHEMA_NAME}.{BACKUP_TABLE};
    """

    redshift_hook.run(sql_commands)
    print(f"✅ {SCHEMA_NAME}.{FINAL_TABLE_NAME} 갱신 완료")
    
    # Slack 알림
    payload = {"text": (
        f"*ver2_05_map_search.py*\n"
        f"📌 ✅ {SCHEMA_NAME}.{FINAL_TABLE_NAME} 갱신 완료 {len(final_df)} rows\n")}
    requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)

# =========================
# DAG 정의
# =========================
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="ver2_05_map_search",
    default_args=default_args,
    description="카카오 원본 데이터와 실시간 대기 데이터를 합쳐 지도 검색용 테이블 생성",
    schedule=None,
    catchup=False,
    max_active_runs=1  # 🚨 동시 실행으로 인한 트랜잭션 충돌 방지
) as dag:

    t0_create_table = SQLExecuteQueryOperator(
        task_id="create_final_table_if_not_exists",
        conn_id=REDSHIFT_CONN_ID,
        sql=FINAL_TABLE_CREATE_SQL,
    )

    t1_full_pipeline = PythonOperator(
        task_id="run_full_static_feature_pipeline",
        python_callable=full_static_feature_pipeline,
    )

    t0_create_table >> t1_full_pipeline
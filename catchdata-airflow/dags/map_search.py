import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import Numeric, String

# =========================
# 기본 설정
# =========================
REDSHIFT_CONN_ID = "redshift_conn"
SCHEMA_NAME = "analytics"
RAW_TABLE1 = "raw_data.kakao_crawl"
RAW_TABLE2 = "analytics.realtime_waiting"
FINAL_TABLE_NAME = "map_search"

# =========================
# 💡 SQL: 최종 테이블 생성 스키마
# =========================
# SQL 문법 오류(콤마) 수정 및 VARCHAR 길이 최적화
FINAL_TABLE_CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{FINAL_TABLE_NAME} (
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
    rec_convenience FLOAT
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
    # category_name을 가져와서 파이썬에서 category로 가공할 예정
    print("--- 1. Redshift에서 원본 데이터 로드 시작 ---")
    sql_select = f"""
    SELECT 
        A.place_name as name,
        A.address_name as address,
        A.category_name,  -- 원본 카테고리 로드
        A.x,
        A.y,
        B.waiting,
        A.rating,
        A.phone,
        A.img_url as image_url,
        B.rec_quality,
        B.rec_balanced,
        B.rec_convenience
    FROM {RAW_TABLE1} A
    INNER JOIN {RAW_TABLE2} B 
        ON CAST(A.id AS VARCHAR) = B.id;
    """

    df = redshift_hook.get_pandas_df(sql_select)

    if df.empty:
        print("경고: 원본 테이블에 데이터가 없습니다. 파이프라인을 종료합니다.")
        return

    print(f"✅ 데이터 로드 완료: {len(df)}개")

    # 2. 데이터 전처리 (Python/Pandas 환경)
    print("--- 2. 데이터 전처리 시작 ---")
    
    # 2-1. category 가공 (마지막 요소 추출)
    df['category'] = df['category_name'].str.split('>').str[-1].str.strip()

    # 2-2. address 분리 (region, city)
    # n=2를 주어 첫 두 단어만 분리하고 나머지는 유지
    address_split = df['address'].str.split(' ', n=2, expand=True)
    df['region'] = address_split[0]
    df['city'] = address_split[1]

    # 2-3. 최종 테이블 컬럼 순서 및 구성 확정
    final_df = df[['name', 'region', 'city', 'category', 'x', 'y', 
                   'waiting', 'rating', 'phone', 'image_url', 'address',
                   'rec_quality', 'rec_balanced', 'rec_convenience']].copy()

    print("✅ 전처리 완료")

    # 3. Redshift 원자적 교체 실행
    STAGING_TABLE = f"{FINAL_TABLE_NAME}_staging"
    BACKUP_TABLE = f"{FINAL_TABLE_NAME}_old"

    # 3-1. Staging 테이블에 로드
    # 데이터 타입을 명시적으로 지정하여 Redshift 스키마와 일치시킴
    dtype_mapping = {
        'x': Numeric(15, 12),
        'y': Numeric(15, 12),
        'rating': Numeric(3, 1),
        'name': String(50),
        'category': String(50),
        'image_url': String(500), # 🚨 이 부분이 핵심 해결책
        'address': String(300),
        'rec_quality': Numeric(15,14),
        'rec_balanced': Numeric(15,14),
        'rec_convenience': Numeric(15,14)
    }

    print(f"--- 3. Staging 테이블 로드: {STAGING_TABLE} ---")
    final_df.to_sql(
        name=STAGING_TABLE,
        con=engine,
        schema=SCHEMA_NAME,
        if_exists='replace',
        index=False,
        dtype=dtype_mapping
    )

    # 3-2. 트랜잭션 시작 (Atomic Swap)
    sql_commands = f"""
    BEGIN;
    -- 1. 기존 테이블 백업 (존재할 때만 실행되도록 IF EXISTS는 RENAME에서 직접 지원 안하므로 수동 확인 필요하나
    -- t0 태스크가 테이블 존재를 보장함)
    DROP TABLE IF EXISTS {SCHEMA_NAME}.{BACKUP_TABLE};
    ALTER TABLE {SCHEMA_NAME}.{FINAL_TABLE_NAME} RENAME TO {BACKUP_TABLE};

    -- 2. Staging을 최종으로 승격
    ALTER TABLE {SCHEMA_NAME}.{STAGING_TABLE} RENAME TO {FINAL_TABLE_NAME};
    COMMIT;

    -- 3. 백업 정리
    DROP TABLE IF EXISTS {SCHEMA_NAME}.{BACKUP_TABLE};
    """

    redshift_hook.run(sql_commands)
    print(f"✅ {SCHEMA_NAME}.{FINAL_TABLE_NAME} 갱신 완료")

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
    dag_id="redshift_map_search_update_pipeline",
    default_args=default_args,
    description="카카오 원본 데이터와 실시간 대기 데이터를 합쳐 지도 검색용 테이블 생성",
    schedule=None,
    catchup=False
) as dag:

    # T0. 테이블 생성 보장
    t0_create_table = SQLExecuteQueryOperator(
        task_id="create_final_table_if_not_exists",
        conn_id=REDSHIFT_CONN_ID,
        sql=FINAL_TABLE_CREATE_SQL,
    )

    # T1. 메인 파이프라인 실행
    t1_full_pipeline = PythonOperator(
        task_id="run_full_static_feature_pipeline",
        python_callable=full_static_feature_pipeline,
    )

    t0_create_table >> t1_full_pipeline
import json
import math
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import (
    SQLExecuteQueryOperator,  # 테이블 생성용
)
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import Numeric

# =========================
# 기본 설정
# =========================
REDSHIFT_CONN_ID = "redshift_conn"
SCHEMA_NAME = "analytics"
RAW_TABLE = "raw_data.kakao_crawl"
FINAL_TABLE_NAME = "derived_features_base"

# 가중치 설정 (base_population 계산용)
W_REVIEW = 1.0
W_BLOG = 0.7

# 24개 시간대 컬럼 이름 정의
TIME_COLUMNS = [f'time{i}' for i in range(24)]

# =========================
# 💡 SQL: 최종 테이블 생성 스키마
# =========================
# Redshift에 최적화된 테이블 스키마 정의
FINAL_TABLE_CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{FINAL_TABLE_NAME} (
    id VARCHAR(256) PRIMARY KEY,
    base_population NUMERIC(18, 4),
    quality_score NUMERIC(18, 4),
    rating NUMERIC(3, 2),
    -- 24개 시간대 컬럼 (방문자 수는 작으므로 SMALLINT 사용)
    {', '.join([f'{col} SMALLINT' for col in TIME_COLUMNS])},
    calculated_at TIMESTAMP
)
-- id를 기준으로 데이터 분산 및 정렬하여 조인 및 쿼리 성능 최적화
DISTKEY(id) 
SORTKEY(calculated_at);
"""

# =========================
# 💡 단일 통합 함수: 모든 로직을 순차적으로 실행 (Atomic Replacement)
# =========================
def full_static_feature_pipeline():
    """
    hourly_visit JSON을 24개 컬럼으로 변환하고, 
    Redshift 테이블 이름 교체를 통해 원자적으로 갱신합니다.
    """

    # Redshift Hook 초기화
    redshift_hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)
    engine = redshift_hook.get_sqlalchemy_engine()

    # 1. Redshift에서 원본 데이터 로드
    print("--- 1. Redshift에서 원본 데이터 로드 시작 ---")
    sql_select = f"""
    SELECT 
        id, 
        rating, 
        review_count, 
        blog_count, 
        hourly_visit
    FROM {RAW_TABLE};
    """

    df = redshift_hook.get_pandas_df(sql_select)

    if df.empty:
        print(f"경고: {RAW_TABLE} 테이블에 데이터가 없습니다. 파이프라인을 종료합니다.")
        return

    print(f"✅ 원본 데이터 로드 완료: {len(df)}개")


    # 2. 파생 변수 계산 및 hourly_visit 분리 (Python/Pandas 환경)
    print("--- 2. 파생 변수 계산 및 hourly_visit 분리 시작 ---")

    # --- base_population 계산 ---
    df['base_population'] = (
        df['review_count'].apply(math.log1p) * W_REVIEW +
        df['blog_count'].apply(math.log1p) * W_BLOG
    )

    # --- quality_score 계산 ---
    df['quality_score'] = df['base_population'] * df['rating'].astype(float, errors='ignore')

    # --- hourly_visit JSON 파싱 및 24개 컬럼 분리 ---
    def safe_loads(json_str):
        """JSON 파싱 중 오류 발생 시 0으로 채워진 리스트를 반환"""
        try:
            if pd.isna(json_str) or json_str is None:
                return [0] * 24
            return json.loads(json_str)
        except Exception:
            # 리스트 길이가 24가 아닌 경우에도 0으로 채워진 리스트 반환
            return [0] * 24

    df['hourly_list'] = df['hourly_visit'].apply(safe_loads)

    # 24개 시간대별 컬럼 생성
    # Redshift의 작은 정수형(SMALLINT)으로 저장하기 위해 타입 변환
    df[TIME_COLUMNS] = pd.DataFrame(df['hourly_list'].to_list(), index=df.index).astype('int16')
    df.drop(columns=['hourly_list', 'hourly_visit'], inplace=True)


    # --- 최종 테이블 구조 준비 ---
    final_df = df[[
        'id',
        'base_population',
        'quality_score',
        'rating',
        *TIME_COLUMNS
    ]].copy()

    final_df['calculated_at'] = datetime.now()

    print("✅ 파생 변수 및 시간대 컬럼 계산 완료")


    # 3. Redshift 테이블 이름 변경을 통한 원자적 교체
    print("--- 3. Redshift 테이블 이름 교체 시작 (Atomic Replacement) ---")

    # 💡 임시 테이블 및 백업 테이블 이름 정의
    STAGING_TABLE = 'derived_features_staging'
    BACKUP_TABLE = 'derived_features_old'

    # 💡 데이터 타입 매핑 정의 (Redshift SMALLINT로 매핑하기 위해 명시)
    dtype_mapping = {
        'base_population': Numeric(18, 4),
        'quality_score': Numeric(18, 4),
        'rating': Numeric(3, 2),
        # TIME_COLUMNS의 타입은 int16을 통해 SMALLINT로 자동으로 추론되도록 합니다.
    }

    # 3-1. 계산된 final_df를 임시 Staging 테이블에 로드
    final_df.to_sql(
        name=STAGING_TABLE,
        con=engine,
        schema=SCHEMA_NAME,
        if_exists='replace',
        index=False,
        dtype=dtype_mapping
    )

    print(f"   -> Staging 테이블 로드 완료: {SCHEMA_NAME}.{STAGING_TABLE}")


    # 3-2. Redshift 트랜잭션 시작 및 테이블 이름 교체 실행
    sql_commands = f"""
    BEGIN;

    -- 1. 기존 최종 테이블을 백업 테이블로 이름 변경
    ALTER TABLE {SCHEMA_NAME}.{FINAL_TABLE_NAME} RENAME TO {BACKUP_TABLE};

    -- 2. 임시 테이블을 최종 테이블 이름으로 변경 (원자적 교체)
    ALTER TABLE {SCHEMA_NAME}.{STAGING_TABLE} RENAME TO {FINAL_TABLE_NAME};

    COMMIT;

    -- 3. 이전 버전의 백업 테이블 정리
    DROP TABLE IF EXISTS {SCHEMA_NAME}.{BACKUP_TABLE};
    """


    redshift_hook.run(sql_commands)

    print(f"✅ {SCHEMA_NAME}.{FINAL_TABLE_NAME} 테이블이 {len(final_df)}개 레코드로 서비스 중단 없이 갱신되었습니다.")


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
    dag_id="redshift_static_feature_update",
    default_args=default_args,
    description="hourly_visit을 24개 time 컬럼으로 분리하고 RENAME을 통해 Redshift 테이블을 원자적으로 갱신합니다.",
    schedule="@daily",
    catchup=False
) as dag:

    # T0. 최종 테이블이 없는 경우 생성 (최초 실행 시 안정성 확보)
    t0_create_table = SQLExecuteQueryOperator(
        task_id="create_final_table_if_not_exists",
        conn_id=REDSHIFT_CONN_ID,
        sql=FINAL_TABLE_CREATE_SQL,
    )

    # T1. 데이터 로드, 계산 및 최종 테이블 갱신
    t1_full_pipeline = PythonOperator(
        task_id="run_full_static_feature_pipeline",
        python_callable=full_static_feature_pipeline,
    )

    # 파이프라인 흐름 정의: 테이블 생성 확인 후 데이터 갱신
    t0_create_table >> t1_full_pipeline

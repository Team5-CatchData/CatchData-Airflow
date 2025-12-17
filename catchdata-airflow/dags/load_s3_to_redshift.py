import boto3
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone

REDSHIFT_HOST = "RedshiftHost"
REDSHIFT_PORT = 5439
REDSHIFT_USER = "Redshift_user"
REDSHIFT_PASSWORD = "Redshift_Password"
REDSHIFT_DB = "dev"
KST = timezone(timedelta(hours=9))
time_stamp = datetime.now(KST).strftime("%Y%m%d")

S3_BUCKET = "427paul-test-bucket"
S3_KAKAO_INFO = f"kakao_crawl/eating_house_{time_stamp}.csv"
S3_KAKAO_IMG = f"kakao_img_url/eating_house_img_url_{time_stamp}.csv"
TARGET_TABLE_INFO = "raw_data.kakao_crawl"


def load_s3_to_redshift():
    time_stamp = datetime.now().strftime("%Y%m%d")

    COPY_SQL = f"""
    COPY raw_data.kakao_crawl_stg
    FROM 's3://427paul-test-bucket/kakao_crawl/eating_house_{time_stamp}.csv'
    credentials 'aws_iam_role=arn:aws:iam::903836366474:role/redshift.read.s3'
    delimiter ','
    IGNOREHEADER 1
    removequotes;
    """

    SWAP_SQL = """
    BEGIN;

    DROP TABLE IF EXISTS raw_data.kakao_crawl_backup;
    ALTER TABLE raw_data.kakao_crawl RENAME TO kakao_crawl_backup;
    ALTER TABLE raw_data.kakao_crawl_stg RENAME TO kakao_crawl;

    COMMIT;

    DROP TABLE IF EXISTS raw_data.kakao_crawl_backup;
    """

    conn = psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
        dbname=REDSHIFT_DB
    )
    cur = conn.cursor()

    try:
        cur.execute("DROP TABLE IF EXISTS raw_data.kakao_crawl_stg;")
        cur.execute("CREATE TABLE raw_data.kakao_crawl_stg (LIKE raw_data.kakao_crawl);")

        print("▶ COPY to STAGING")
        cur.execute(COPY_SQL)

        print("▶ ATOMIC TABLE SWAP")
        cur.execute(SWAP_SQL)

        conn.commit()
        print("✅ 데이터 교체 완료")

    except Exception as e:
        conn.rollback()
        print("❌ 실패", e)
        raise
    finally:
        cur.close()
        conn.close()

    
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "규영",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}


with DAG(
    dag_id="load_s3_to_redshift",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args
):

    load_task = PythonOperator(
        task_id="load_img_url_to_redshift",
        python_callable=load_s3_to_redshift
    )
    
    trigger_static_feature_dag = TriggerDagRunOperator(
        task_id="trigger_redshift_static_feature_update",
        trigger_dag_id="redshift_static_feature_update",  # 실행할 DAG ID
        wait_for_completion=False,   # 보통 False (비동기)
        reset_dag_run=True,          # 같은 execution_date 있으면 새로 실행
    )
    
    load_task >> trigger_static_feature_dag

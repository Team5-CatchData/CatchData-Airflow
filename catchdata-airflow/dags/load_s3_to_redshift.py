from datetime import datetime, timedelta, timezone

import psycopg2
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

REDSHIFT_HOST = "default-workgroup.903836366474.ap-northeast-2.redshift-serverless.amazonaws.com"
REDSHIFT_PORT = 5439
REDSHIFT_USER = "dev_admin"
REDSHIFT_PASSWORD = "StrongPassword123!"
REDSHIFT_DB = "dev"
KST = timezone(timedelta(hours=9))
time_stamp = datetime.now(KST).strftime("%Y%m%d")
# time_stamp = "20251219"
S3_BUCKET = "team5-batch"
S3_KAKAO_INFO = f"raw_data/kakao/eating_house_{time_stamp}.csv"
TARGET_TABLE_INFO = "raw_data.kakao_crawl"

SLACK_WEBHOOK_URL = ("https://hooks.slack.com/services/T09SZ0BSHEU"
                     "/B0A3W3R4H9D/Ea5DqrFBnQKc3SzbSuNhcmZo")


def load_s3_to_redshift():
    time_stamp = datetime.now().strftime("%Y%m%d")

    COPY_SQL = f"""
    COPY raw_data.kakao_crawl_stg
    FROM 's3://team5-batch/raw_data/kakao/eating_house_{time_stamp}.csv'
    REGION 'ap-northeast-2'
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
        
        payload = {"text": (f"*load_s3_to_redshift.py*\n"
                            "📌 S3 데이터 {S3_KAKAO_INFO} 데이터 -> Redshift {TARGET_TABLE_INFO} 적재 완료\n")}

        requests.post(
            SLACK_WEBHOOK_URL,
            json=payload,
            timeout=10,
        )

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




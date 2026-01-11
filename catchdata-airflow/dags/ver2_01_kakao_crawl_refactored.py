from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, Variable
from plugins.operators.kakao_api_operator import KakaoAPIOperator
from plugins.operators.kakao_crawl_operator import KakaoCrawlOperator
from plugins.operators.s3_upload_operator import S3UploadOperator


# =========================
# 기본 설정
# =========================
REST_API_KEY = Variable.get("KAKAO_REST_API_KEY")
SLACK_WEBHOOK_URL = Variable.get("SLACK_WEBHOOK_URL")
BUCKET_NAME = Variable.get("S3_BUCKET_NAME", default_var="427paul-test-bucket")

KST = timezone(timedelta(hours=9))
time_stamp = datetime.now(KST).strftime("%Y%m%d")
OUTPUT_KEY = f"kakao_crawl/eating_house_{time_stamp}.csv"


# =========================
# DAG 정의
# =========================
default_args = {
    "owner": "jaehyeon",
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="ver2_01_kakao_crawl_refactored",
    start_date=datetime(2025, 1, 1),
    schedule="0 3 * * 1",  # 매주 월요일 03:00 실행
    catchup=False,
    default_args=default_args,
    tags=['kakao', 'crawl', 's3', 'refactored']
) as dag:

    # Task 1: Kakao API로 음식점 목록 수집
    collect_task = KakaoAPIOperator(
        task_id='collect_kakao_data',
        kakao_api_key=REST_API_KEY,
        districts=['홍대', '대치동'],
        categories=['한식', '일식', '중식', '양식', '술집', '고기집',
                   '치킨', '분식', '샤브샤브', '간식', '뷔페'],
        page_size=6,
        max_pages=2,
        slack_webhook_url=SLACK_WEBHOOK_URL,
    )

    # Task 2: 병렬 크롤링으로 상세 정보 수집
    crawl_task = KakaoCrawlOperator(
        task_id='crawl_place_details',
        max_workers=4,
        slack_webhook_url=SLACK_WEBHOOK_URL,
    )

    # Task 3: S3에 결과 업로드
    upload_task = S3UploadOperator(
        task_id='upload_to_s3',
        aws_conn_id='aws_default',
        bucket_name=BUCKET_NAME,
        key=OUTPUT_KEY,
        slack_webhook_url=SLACK_WEBHOOK_URL,
    )

    # Task 의존성 설정
    collect_task >> crawl_task >> upload_task

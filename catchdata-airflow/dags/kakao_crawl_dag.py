from airflow import DAG
from airflow.decorators import task
from datetime import datetime

import json
import pandas as pd
import sys
sys.path.append("/opt/airflow/scripts")
sys.path.append("/opt/airflow")



from scripts.kakao_crawler import (
    collect_places_single_keyword,
    run_region_crawler
)

with DAG(
    dag_id="kakao_crawl_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,   # 수동 실행
    catchup=False,
) as dag:

    @task
    def crawl_hongdae():
        records = collect_places_single_keyword("홍대 음식점", "서울 마포구")
        results = run_region_crawler(records)
        return results

    @task
    def crawl_daechi():
        records = collect_places_single_keyword("대치동 음식점", "서울 강남구 대치동")
        results = run_region_crawler(records)
        return results

    @task
    def save_csv(hongdae_results, daechi_results):
        rows = []

        # 홍대
        for r in hongdae_results:
            rows.append({
                "place_id": r["place_id"],
                "place_name": r["name"],
                "address_name": r["address"],
                "road_address_name": r["road_address"],
                "category_name": r["category"],
                "category_group_name": r["category_group_name"],
                "category_group_code": r["category_group_code"],
                "phone": r["phone"],
                "x": r["x"],
                "y": r["y"],
                "place_url": r["place_url"],
                "rating": r["detail"]["rating"],
                "review_count": r["detail"]["review_count"],
                "blog_count": r["detail"]["blog_count"],
                "updated_at": r["detail"]["updated_at"],
                "business_hours": json.dumps(r["detail"]["business_hours"], ensure_ascii=False),
                "hourly_visit": json.dumps(r["detail"]["hourly_visit"]),
                "waiting": 0
            })

        # 대치동
        for r in daechi_results:
            rows.append({
                "place_id": r["place_id"],
                "place_name": r["name"],
                "address_name": r["address"],
                "road_address_name": r["road_address"],
                "category_name": r["category"],
                "category_group_name": r["category_group_name"],    
                "category_group_code": r["category_group_code"],
                "phone": r["phone"],
                "x": r["x"],
                "y": r["y"],
                "place_url": r["place_url"],
                "rating": r["detail"]["rating"],
                "review_count": r["detail"]["review_count"],
                "blog_count": r["detail"]["blog_count"],
                "updated_at": r["detail"]["updated_at"],
                "business_hours": json.dumps(r["detail"]["business_hours"], ensure_ascii=False),
                "hourly_visit": json.dumps(r["detail"]["hourly_visit"]),
                "waiting": 0
            })

        df = pd.DataFrame(rows)
        df.to_csv("/opt/airflow/dags/hongdae_daechi_test.csv", index=False, encoding="utf-8-sig")
        print("📁 저장 완료 → /opt/airflow/hongdae_daechi_test.csv")

    # DAG Flow
    h = crawl_hongdae()
    d = crawl_daechi()
    save_csv(h, d)

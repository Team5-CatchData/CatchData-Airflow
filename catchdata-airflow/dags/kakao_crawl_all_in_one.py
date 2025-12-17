import os
import time
import base64
import pandas as pd
import multiprocessing
import threading

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone

import requests
import boto3

# ChromeDriver 다운로드 Lock (동시 다운로드 방지)
_driver_lock = threading.Lock()


# =========================
#  기본 설정
# =========================
AWS_ACCESS_KEY = "AWS_ACCESS_KEY"
AWS_SECRET_KEY = "AWS_SEVCRET_KEY"

KST = timezone(timedelta(hours=9))
time_stamp = datetime.now(KST).strftime("%Y%m%d")
BUCKET_NAME = "427paul-test-bucket"
OUTPUT_KEY = f"kakao_crawl/eating_house_{time_stamp}.csv"


# =========================
# 크롤링 함수
# =========================
def crawl_kakao_place(place_url):
    import time
    import base64
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    import numpy as np
    import cv2
    
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1280,800")
    options.add_argument("user-agent=Mozilla/5.0")
    
    # Lock을 사용하여 ChromeDriver 다운로드 동시성 문제 방지
    with _driver_lock:
        driver_path = ChromeDriverManager().install()

    driver = webdriver.Chrome(
        service=Service(driver_path),
        options=options
    )

    wait = WebDriverWait(driver, 10)

    driver.get(place_url)
    time.sleep(1.0)

    # 방문자 그래프 이미지 처리
    img_values = None
    try:
        canvas = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.view_chart canvas"))
        )
        img_base64 = driver.execute_script(
            "return arguments[0].toDataURL('image/png').substring(22);",
            canvas
        )
        img_data = base64.b64decode(img_base64)
        img = cv2.imdecode(np.frombuffer(img_data, np.uint8), cv2.IMREAD_COLOR)
        h, w, _ = img.shape
        hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
        mask = cv2.inRange(hsv, np.array([90, 40, 40]), np.array([250, 180, 255]))
        values = []
        x_positions = [int((i + 0.5) * w / 24) for i in range(24)]
        for x in x_positions:
            ys = np.where(mask[:, x] > 0)[0]
            values.append(
                round((h - ys[0]) / h * 100, 1) if len(ys) else np.nan
            )
        clean = np.array(values)
        idx = np.arange(24)
        if np.any(~np.isnan(clean)):
            clean[np.isnan(clean)] = np.interp(
                idx[np.isnan(clean)], idx[~np.isnan(clean)], clean[~np.isnan(clean)]
            )
        img_values = clean.tolist()
    except:
        img_values = [0] * 24

    # 별점
    try:
        rating = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "span.num_star"))).text
    except:
        rating = 0

    # 후기 & 블로그 수
    review_cnt = 0
    blog_cnt = 0
    try:
        titles = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "span.info_tit")))
        counts = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "span.info_num")))
        title_list = [t.text for t in titles]
        count_list = [c.text for c in counts]
        if "후기" in title_list:
            review_cnt = count_list[title_list.index("후기")]
        if "블로그" in title_list:
            blog_cnt = count_list[title_list.index("블로그")]
    except:
        pass

        
    # 이미지 URL
    img_url = None
    try:
        # 사진 목록 영역
        container = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.inner_board"))
        )

        imgs = container.find_elements(By.TAG_NAME, "img")

        for img in imgs:
            src = img.get_attribute("src")
            if src and src.startswith("http"):
                img_url = src   # ✅ 첫 번째 이미지 발견 즉시 반환
                break
    except:
        pass

    driver.quit()

    return {
        "rating": rating,
        "review_count": review_cnt,
        "blog_count": blog_cnt,
        "hourly_visit": img_values,
        "img_url":img_url,
        "waiting": 0,
        "update_time": time.strftime("%Y-%m-%d")
    }


def process_row(row):
    # place_url = f"https://place.map.kakao.com/{row['id']}"
    return crawl_kakao_place(row['place_url'])


# =========================
# 통합 작업 함수
# =========================
def run_all_tasks(**context):
    """
    1. Kakao API로 음식점 목록 수집
    2. 병렬 크롤링으로 상세 정보 수집
    3. S3에 결과 업로드
    """
    
    # ========================================
    # TASK 1: Kakao API 목록 수집
    # ========================================
    print("=" * 60)
    print("🔎 TASK 1 시작: Kakao API 음식점 목록 수집")
    print("=" * 60)
    
    REST_API_KEY = "REST_API_KEY"
    url = "https://dapi.kakao.com/v2/local/search/keyword.json"
    headers = {"Authorization": f"KakaoAK {REST_API_KEY}"}
    
    all_results = []

    query = "홍대 음식점"
    for page in range(1, 2):
        params = {
            "query": query,
            "size": 15,
            "page": page
        }

        res = requests.get(url, params=params, headers=headers).json()
        docs = res.get("documents", [])

        if not docs:
            break

        all_results.extend(docs)
        time.sleep(0.3)
        
    query = "대치동 음식점"
        
    for page in range(1, 2):
        params = {
            "query": query,
            "size": 15,
            "page": page
        }

        res = requests.get(url, params=params, headers=headers).json()
        docs = res.get("documents", [])

        if not docs:
            break
        
        all_results.extend(docs)
        time.sleep(0.3)
        
    df = pd.DataFrame(all_results)

    
    # 주소 필터 - 서울 마포구만
    # df = df[df["address_name"].str.startswith("서울 마포구")]
    
    # 음식점만 (FD6)
    df = df[df["category_group_code"] == "FD6"]
    
    print(f"✅ TASK 1 완료: 총 {len(df)}개 음식점 목록 수집 완료")
    print("=" * 60)
    print()
    
    # ========================================
    # TASK 2: 병렬 크롤링으로 상세 정보 수집
    # ========================================
    print("=" * 60)
    print("🕷️ TASK 2 시작: 음식점 상세 정보 병렬 크롤링")
    print("=" * 60)
    
    # ChromeDriver 미리 다운로드 (동시 다운로드 방지)
    print("ChromeDriver 다운로드 중...")
    from selenium import webdriver
    from webdriver_manager.chrome import ChromeDriverManager
    driver_path = ChromeDriverManager().install()
    print(f"ChromeDriver 준비 완료: {driver_path}")
    
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    workers = min(4, multiprocessing.cpu_count())
    print(f"병렬 처리 워커 수: {workers}")
    
    results = []
    tasks = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        for i, row in df.iterrows():
            tasks.append(executor.submit(process_row, row))
        
        completed = 0
        for future in as_completed(tasks):
            try:
                results.append(future.result())
                completed += 1
                if completed % 5 == 0 or completed == len(tasks):
                    print(f"진행 상황: {completed}/{len(tasks)} 완료")
            except Exception as e:
                print(f"크롤링 실패: {str(e)}")
                # 실패한 경우 빈 데이터 추가
                results.append({
                    "rating": 0,
                    "review_count": 0,
                    "blog_count": 0,
                    "hourly_visit": [0] * 24,
                    "img_url" : "None",
                    "waiting": 0,
                    "update_time": time_stamp
                })
                completed += 1

    # distance, place_url 컬럼 제거
    df = df.drop(columns=["distance", "place_url"], errors="ignore")
    
    final_df = pd.concat([df.reset_index(drop=True), pd.DataFrame(results)], axis=1)
    print(f"✅ TASK 2 완료: 총 {len(final_df)}개 음식점 크롤링 완료")
    print("=" * 60)
    print(final_df.head())
    print("=" * 60)
    print()
    
    # ========================================
    # TASK 3: S3에 결과 업로드
    # ========================================
    print("=" * 60)
    print("☁️ TASK 3 시작: S3에 결과 업로드")
    print("=" * 60)
    
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    # UTF-8 BOM 추가로 한글 깨짐 방지 (Excel에서도 정상 표시)
    csv_buffer = final_df.to_csv(index=False, encoding='utf-8-sig')

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=OUTPUT_KEY,
        Body=csv_buffer.encode("utf-8-sig"),
        ContentType="text/csv; charset=utf-8"
    )

    print(f"✅ TASK 3 완료: S3 업로드 성공")
    print(f"📁 저장 위치: s3://{BUCKET_NAME}/{OUTPUT_KEY}")
    print(f"📊 업로드된 데이터: {len(final_df)}행, {len(final_df.columns)}열")
    print("=" * 60)
    print()
    print("🎉 전체 작업 완료!")


# =========================
# DAG 정의
# =========================

from airflow.operators.trigger_dagrun import TriggerDagRunOperator


default_args = {
    "owner": "규영",
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="kakao_crawl_all_in_one",
    start_date=datetime(2025, 1, 1),
    schedule="0 3 * * 1", # 매주 월요일 03:00 실행
    catchup=False,
    default_args=default_args
):

    run_all = PythonOperator(
        task_id="run_all_tasks",
        python_callable=run_all_tasks
    )
    
    trigger_load_redshift = TriggerDagRunOperator(
        task_id="trigger_load_s3_to_redshift",
        trigger_dag_id="load_s3_to_redshift",
        wait_for_completion=False,
        reset_dag_run=False
    )

    # run_all 끝나면 extract_kakao_url DAG 실행됨
    run_all >> trigger_load_redshift
    # run_all
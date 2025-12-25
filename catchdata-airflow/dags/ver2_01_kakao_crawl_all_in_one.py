import base64
import multiprocessing
import threading
import time
from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd
import requests
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Variable

# ChromeDriver 다운로드 Lock (동시 다운로드 방지)
_driver_lock = threading.Lock()


# =========================
#  기본 설정
# =========================
REST_API_KEY = Variable.get("KAKAO_REST_API_KEY")
SLACK_WEBHOOK_URL = Variable.get("SLACK_WEBHOOK_URL")
KST = timezone(timedelta(hours=9))
time_stamp = datetime.now(KST).strftime("%Y%m%d")
BUCKET_NAME = Variable.get("S3_BUCKET_NAME", default_var="427paul-test-bucket")
OUTPUT_KEY = f"kakao_crawl/eating_house_{time_stamp}.csv"


# =========================
# 크롤링 함수
# =========================
def crawl_kakao_place(id):
    import time

    import cv2
    import numpy as np
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait
    from webdriver_manager.chrome import ChromeDriverManager

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
    place_url = f"https://place.map.kakao.com/{id}"
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
    except Exception as e:
        print(f"{place_url} : 방문자 그래프를 처리할 수 없어 중단합니다 ({e})")
        driver.quit()
        return None
        # img_values = [0] * 24

    # 별점
    try:
        rating = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "span.num_star"))
        ).text
    except Exception:
        rating = 0

    # 후기 & 블로그 수
    review_cnt = 0
    blog_cnt = 0
    try:
        titles = wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "span.info_tit"))
        )
        counts = wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "span.info_num"))
        )
        title_list = [t.text for t in titles]
        count_list = [c.text for c in counts]
        if "후기" in title_list:
            review_cnt = count_list[title_list.index("후기")]
        if "블로그" in title_list:
            blog_cnt = count_list[title_list.index("블로그")]
    except Exception:
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
    except Exception:
        pass

    driver.quit()

    return {
        "id" : id,
        "rating": rating,
        "review_count": review_cnt,
        "blog_count": blog_cnt,
        "hourly_visit": img_values,
        "img_url":img_url,
        "update_time": time.strftime("%Y-%m-%d")
    }


def process_row(row):
    # place_url = f"https://place.map.kakao.com/{row['id']}"
    return crawl_kakao_place(row['id'])
    # return crawl_kakao_place(row['place_url'])


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

    url = "https://dapi.kakao.com/v2/local/search/keyword.json"
    headers = {"Authorization": f"KakaoAK {REST_API_KEY}"}

    districts = ['홍대', '대치동']
    # districts = ['청담동', '롯데월드몰', '압구정', '성수동', '강남역', '건대', '홍대', '대치동']
    categories = ['한식', '일식', '중식', '양식', '술집', '고기집',
                '치킨', '분식', '샤브샤브', '간식', '뷔페'] # 키워드 세분화

    all_results = []

    for loc in districts:
        print(f"\n>>> {loc} 지역 수집 시작...")
        district_count = 0

        for cat in categories:
            query = f"{loc} {cat}"

            for page in range(1, 2): # 각 세부 키워드당 45개씩 수집
                params = {"query": query, "size": 6, "page": page}
                res = requests.get(url, params=params, headers=headers, timeout=10).json()
                docs = res.get("documents", [])

                if not docs:
                    break

                # for doc in docs:
                #     doc['main_district'] = loc # 어느 지역인지 저장
                #     doc['sub_category'] = cat  # 어떤 키워드로 찾았는지 저장

                all_results.extend(docs)
                district_count += len(docs)

                if res.get("meta", {}).get("is_end"):
                    break
                time.sleep(0.1)

        print(f"{loc} 수집 완료 (누적: {district_count}개)")

    # 데이터프레임 생성 및 중복 제거
    df_final = pd.DataFrame(all_results)
    # df_final = df_final.drop_duplicates(subset=['id']).reset_index(drop=True)

    #수집된 총 데이터 수
    crawl_data_len = len(df_final)
    print(f"\n🎉 전체 수집 종료! 최종 데이터: {crawl_data_len}개")

    df = df_final
    # 음식점만 (FD6)
    df = df[df["category_group_code"] == "FD6"]

    # 음식점 필터링 후 데이터 수
    only_FD6 = len(df)

    print(f"음식점 전처리 후 데이터 수: {only_FD6}")

    # id를 기준으로 중복 제거 (첫 번째 데이터만 남김)
    df = df.drop_duplicates(subset=['id'], keep='first')

    # 중복 제거 후 데이터 수
    drop_duplicate = len(df)
    print(f"중복 제거 후 데이터 수: {drop_duplicate}")

    print(f"✅ TASK 1 완료: 총 {drop_duplicate}개 음식점 목록 수집 완료")
    print("=" * 60)
    print()

    payload = {"text": (f"📌 *ver2_01_kakao_crawl_all_on_one.py*\n"
                        f"카카오 API에서 출력된 총 데이터 수 : {crawl_data_len}개\n"
                        f"음식점 전처리 후 데이터 수 : {only_FD6}\n"
                        f"중복 제거 후 데이터 수 : {drop_duplicate}")}
    requests.post(
        SLACK_WEBHOOK_URL,
        json=payload,
        timeout=10,
    )

    # ========================================
    # TASK 2: 병렬 크롤링으로 상세 정보 수집
    # ========================================
    print("=" * 60)
    print("🕷️ TASK 2 시작: 음식점 상세 정보 병렬 크롤링")
    print("=" * 60)

    # ChromeDriver 미리 다운로드 (동시 다운로드 방지)
    print("ChromeDriver 다운로드 중...")
    from webdriver_manager.chrome import ChromeDriverManager
    driver_path = ChromeDriverManager().install()
    print(f"ChromeDriver 준비 완료: {driver_path}")

    from concurrent.futures import ThreadPoolExecutor, as_completed

    workers = min(4, multiprocessing.cpu_count())
    print(f"병렬 처리 워커 수: {workers}")

    results = []
    tasks = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        for _i, row in df.iterrows():
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
                    "id":row['id'],
                    "rating": 0,
                    "review_count": 0,
                    "blog_count": 0,
                    "hourly_visit": [0] * 24,
                    "img_url" : "None",
                    "update_time": time_stamp
                })
                completed += 1

    results = [r for r in results if r is not None]
    results_df = pd.DataFrame(results)

    final_df = pd.merge(df, results_df, on='id', how='inner')

    # distance, place_url 컬럼 제거
    final_df = final_df.drop(columns=["distance", "place_url"], errors="ignore")

    before_drop = len(final_df)

    # id를 기준으로 중복 제거 (첫 번째 데이터만 남김)
    final_df = final_df.drop_duplicates(subset=['id'], keep='first')
    after_drop = len(final_df)

    payload = {"text": (f"📌 *ver2_01_kakao_crawl_all_on_one.py*\n"
                        f"크롤링 {before_drop}개 음식점 목록 수집 완료\n"
                        f"전처리 후 {after_drop}개 음식점 목록 S3 적재 시작\n")}
    requests.post(
        SLACK_WEBHOOK_URL,
        json=payload,
        timeout=10,
    )


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

    # AWS 자격증명은 Airflow Connection 또는 환경변수에서 자동으로 가져옴
    s3 = boto3.client("s3")

    # UTF-8 BOM 추가로 한글 깨짐 방지 (Excel에서도 정상 표시)
    csv_buffer = final_df.to_csv(index=False, encoding='utf-8-sig')

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=OUTPUT_KEY,
        Body=csv_buffer.encode("utf-8-sig"),
        ContentType="text/csv; charset=utf-8"
    )

    print("✅ TASK 3 완료: S3 업로드 성공")
    print(f"📁 저장 위치: s3://{BUCKET_NAME}/{OUTPUT_KEY}")
    print(f"📊 업로드된 데이터: {len(final_df)}행, {len(final_df.columns)}열")
    print("=" * 60)
    print()
    print("🎉 전체 작업 완료!")
    payload = {"text": ("*ver2_01_kakao_crawl_all_in_one.py*\n"
        f"📌 kakao_crawl/eating_house_{time_stamp}.csv 업로드 완료\n"
                        f"총 {len(final_df)}개 데이터 S3 적재 완료")}

    requests.post(
        SLACK_WEBHOOK_URL,
        json=payload,
        timeout=10,
    )


# =========================
# DAG 정의
# =========================


default_args = {
    "owner": "규영",
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="ver2_01_kakao_crawl_all_in_one",
    start_date=datetime(2025, 1, 1),
    schedule="0 3 * * 1", # 매주 월요일 03:00 실행
    catchup=False,
    default_args=default_args
):

    run_all = PythonOperator(
        task_id="run_all_tasks",
        python_callable=run_all_tasks
    )

    # trigger_load_redshift = TriggerDagRunOperator(
    #     task_id="trigger_load_s3_to_redshift",
    #     trigger_dag_id="load_s3_to_redshift",
    #     wait_for_completion=False,
    #     reset_dag_run=False
    # )

    # run_all 끝나면 extract_kakao_url DAG 실행됨
    # run_all >> trigger_load_redshift
    run_all

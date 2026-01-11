import base64
import multiprocessing
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import cv2
import numpy as np
import pandas as pd
import requests
from airflow.models import BaseOperator
from airflow.utils.context import Context
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

# ChromeDriver 다운로드 Lock (동시 다운로드 방지)
_driver_lock = threading.Lock()


class KakaoCrawlOperator(BaseOperator):
    """
    Kakao Place 페이지를 병렬 크롤링하여 상세 정보를 수집하는 커스텀 오퍼레이터

    Selenium을 사용하여 음식점 상세 페이지를 크롤링하고 방문자 그래프, 별점, 후기 등을 수집합니다.

    :param input_dataframe: 크롤링할 음식점 목록 (DataFrame)
    :type input_dataframe: pd.DataFrame
    :param max_workers: 병렬 처리 워커 수 (기본값: CPU 코어 수, 최대 4)
    :type max_workers: int
    :param slack_webhook_url: Slack Webhook URL (선택사항)
    :type slack_webhook_url: str, optional

    **책임:**

    - Selenium을 사용한 웹 크롤링
    - 병렬 처리 (ThreadPoolExecutor)
    - 방문자 그래프 이미지 분석
    - 별점, 후기, 블로그 수 추출
    - 크롤링 실패 처리 및 재시도
    - Slack 알림 전송

    **Example:**

    .. code-block:: python

        from operators.kakao_crawl_operator import KakaoCrawlOperator

        crawl_task = KakaoCrawlOperator(
            task_id='crawl_details',
            input_dataframe=collected_df,
            max_workers=4,
        )

    **Returns:**

    실행 결과 딕셔너리:
        - status (str): 'success'
        - crawled_count (int): 크롤링 성공한 데이터 수
        - failed_count (int): 크롤링 실패한 데이터 수
        - dataframe (pd.DataFrame): 크롤링 결과가 병합된 데이터프레임
    """

    template_fields = ('slack_webhook_url',)

    def __init__(
        self,
        input_dataframe: pd.DataFrame = None,
        max_workers: int = None,
        slack_webhook_url: str = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.input_dataframe = input_dataframe
        self.max_workers = max_workers or min(4, multiprocessing.cpu_count())
        self.slack_webhook_url = slack_webhook_url

    def _send_slack_notification(self, message: str):
        """
        Slack으로 알림을 전송합니다.

        :param message: 전송할 메시지
        :type message: str
        """
        if not self.slack_webhook_url:
            return

        try:
            payload = {"text": message}
            requests.post(self.slack_webhook_url, json=payload, timeout=10)
        except Exception as e:
            self.log.warning(f"Slack 알림 전송 실패: {e}")

    def _crawl_place_detail(self, place_id: str) -> Optional[Dict[str, Any]]:
        """
        특정 음식점의 상세 정보를 크롤링합니다.

        :param place_id: Kakao Place ID
        :type place_id: str
        :return: 크롤링 결과 딕셔너리 (실패 시 None)
        :rtype: Optional[Dict[str, Any]]
        """
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

        driver = webdriver.Chrome(service=Service(driver_path), options=options)
        wait = WebDriverWait(driver, 10)

        try:
            place_url = f"https://place.map.kakao.com/{place_id}"
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
                self.log.warning(f"{place_url}: 방문자 그래프 처리 실패 ({e})")
                driver.quit()
                return None

            # 별점
            rating = 0
            try:
                rating = wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "span.num_star"))
                ).text
            except Exception:
                pass

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
                container = wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.inner_board"))
                )
                imgs = container.find_elements(By.TAG_NAME, "img")
                for img in imgs:
                    src = img.get_attribute("src")
                    if src and src.startswith("http"):
                        img_url = src
                        break
            except Exception:
                pass

            driver.quit()

            KST = timezone(timedelta(hours=9))
            return {
                "id": place_id,
                "rating": rating,
                "review_count": review_cnt,
                "blog_count": blog_cnt,
                "hourly_visit": img_values,
                "img_url": img_url,
                "update_time": datetime.now(KST).strftime("%Y-%m-%d")
            }

        except Exception as e:
            self.log.error(f"크롤링 실패 (ID: {place_id}): {e}")
            driver.quit()
            return None

    def _parallel_crawl(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        병렬 처리로 크롤링을 실행합니다.

        :param df: 크롤링할 음식점 목록
        :type df: pd.DataFrame
        :return: 크롤링 결과 리스트
        :rtype: List[Dict[str, Any]]
        """
        self.log.info(f"병렬 처리 워커 수: {self.max_workers}")

        # ChromeDriver 미리 다운로드
        self.log.info("ChromeDriver 다운로드 중...")
        driver_path = ChromeDriverManager().install()
        self.log.info(f"ChromeDriver 준비 완료: {driver_path}")

        results = []
        tasks = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for _, row in df.iterrows():
                tasks.append(executor.submit(self._crawl_place_detail, row['id']))

            completed = 0
            for future in as_completed(tasks):
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                    completed += 1

                    if completed % 5 == 0 or completed == len(tasks):
                        self.log.info(f"진행 상황: {completed}/{len(tasks)} 완료")
                except Exception as e:
                    self.log.error(f"크롤링 실패: {str(e)}")
                    completed += 1

        return results

    def execute(self, context: Context) -> Dict[str, Any]:
        """
        음식점 상세 정보를 병렬 크롤링하는 메인 실행 로직

        이 메서드는 다음 단계를 수행:
        1. ChromeDriver 준비
        2. 병렬 크롤링 실행
        3. 크롤링 결과와 원본 데이터 병합
        4. Slack 알림 전송

        :param context: Airflow 실행 컨텍스트
        :type context: Context
        :return: 실행 결과 딕셔너리
        :rtype: Dict[str, Any]
        :raises Exception: 크롤링 중 오류 발생 시
        """
        self.log.info("=" * 60)
        self.log.info("🕷️ Kakao Place 상세 정보 병렬 크롤링 시작")
        self.log.info("=" * 60)

        # XCom에서 데이터 가져오기 (이전 task에서 전달받은 경우)
        if self.input_dataframe is None:
            ti = context['ti']
            previous_result = ti.xcom_pull(task_ids='collect_kakao_data')
            if previous_result and 'dataframe' in previous_result:
                self.input_dataframe = previous_result['dataframe']
            else:
                raise ValueError("입력 데이터프레임이 없습니다")

        df = self.input_dataframe
        total_count = len(df)
        self.log.info(f"크롤링 대상: {total_count}개")

        # 병렬 크롤링 실행
        crawl_results = self._parallel_crawl(df)
        crawled_count = len(crawl_results)
        failed_count = total_count - crawled_count

        self.log.info(f"크롤링 성공: {crawled_count}개")
        self.log.info(f"크롤링 실패: {failed_count}개")

        # 결과 병합
        results_df = pd.DataFrame(crawl_results)
        final_df = pd.merge(df, results_df, on='id', how='inner')

        # 불필요한 컬럼 제거
        final_df = final_df.drop(columns=["distance", "place_url"], errors="ignore")

        # 중복 제거
        final_df = final_df.drop_duplicates(subset=['id'], keep='first')

        self.log.info(f"최종 데이터: {len(final_df)}개")

        # Slack 알림
        slack_message = (
            f"📌 *KakaoCrawlOperator*\n"
            f"크롤링 완료\n"
            f"- 대상: {total_count}개\n"
            f"- 성공: {crawled_count}개\n"
            f"- 실패: {failed_count}개\n"
            f"- 최종: {len(final_df)}개"
        )
        self._send_slack_notification(slack_message)

        self.log.info("=" * 60)
        self.log.info(f"✅ 완료: 총 {len(final_df)}개 음식점 크롤링 완료")
        self.log.info("=" * 60)

        return {
            'status': 'success',
            'crawled_count': crawled_count,
            'failed_count': failed_count,
            'dataframe': final_df
        }

import time
from typing import Any, Dict, List

import pandas as pd
import requests
from airflow.models import BaseOperator
from airflow.utils.context import Context


class KakaoAPIOperator(BaseOperator):
    """
    Kakao API로 음식점 목록을 수집하는 커스텀 오퍼레이터

    Kakao Local API를 사용하여 지역별, 카테고리별 음식점 데이터를 수집합니다.

    :param kakao_api_key: Kakao REST API Key
    :type kakao_api_key: str
    :param districts: 검색할 지역 리스트
    :type districts: List[str]
    :param categories: 검색할 음식 카테고리 리스트
    :type categories: List[str]
    :param page_size: 페이지당 결과 수 (기본값: 15, 최대: 15)
    :type page_size: int
    :param max_pages: 최대 검색 페이지 수 (기본값: 3)
    :type max_pages: int
    :param slack_webhook_url: Slack Webhook URL (선택사항)
    :type slack_webhook_url: str, optional

    **책임:**

    - Kakao API 호출 및 데이터 수집
    - 지역별, 카테고리별 데이터 수집 로직
    - 중복 제거 및 전처리
    - Slack 알림 전송

    **Example:**

    .. code-block:: python

        from operators.kakao_api_operator import KakaoAPIOperator

        collect_task = KakaoAPIOperator(
            task_id='collect_kakao_data',
            kakao_api_key='your_api_key',
            districts=['홍대', '강남역'],
            categories=['한식', '일식'],
        )

    **Returns:**

    실행 결과 딕셔너리:
        - status (str): 'success'
        - total_count (int): 수집된 총 데이터 수
        - filtered_count (int): 음식점만 필터링한 데이터 수
        - final_count (int): 중복 제거 후 최종 데이터 수
        - dataframe (pd.DataFrame): 수집된 데이터프레임
    """

    template_fields = ('kakao_api_key', 'slack_webhook_url')

    def __init__(
        self,
        kakao_api_key: str,
        districts: List[str] = None,
        categories: List[str] = None,
        page_size: int = 15,
        max_pages: int = 3,
        slack_webhook_url: str = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.kakao_api_key = kakao_api_key
        self.districts = districts
        self.categories = categories or ['한식', '일식', '중식', '양식', '술집', '고기집',
                                          '치킨', '분식', '샤브샤브', '간식', '뷔페']
        self.page_size = min(page_size, 15)  # Kakao API 최대 15
        self.max_pages = max_pages
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

    def _collect_by_district_and_category(
        self, district: str, category: str, headers: Dict[str, str]
    ) -> List[Dict]:
        """
        특정 지역과 카테고리에 대한 데이터를 수집합니다.

        :param district: 검색할 지역
        :type district: str
        :param category: 검색할 카테고리
        :type category: str
        :param headers: API 요청 헤더
        :type headers: Dict[str, str]
        :return: 수집된 데이터 리스트
        :rtype: List[Dict]
        """
        url = "https://dapi.kakao.com/v2/local/search/keyword.json"
        query = f"{district} {category}"
        results = []

        for page in range(1, self.max_pages + 1):
            params = {
                "query": query,
                "size": self.page_size,
                "page": page
            }

            try:
                response = requests.get(url, params=params, headers=headers, timeout=10)
                response.raise_for_status()
                data = response.json()
                docs = data.get("documents", [])

                if not docs:
                    break

                results.extend(docs)

                if data.get("meta", {}).get("is_end"):
                    break

                time.sleep(0.1)  # API Rate Limiting 방지

            except Exception as e:
                self.log.error(f"API 호출 실패 ({district} {category}, page {page}): {e}")
                break

        return results

    def _collect_all_data(self) -> pd.DataFrame:
        """
        모든 지역과 카테고리에 대한 데이터를 수집합니다.

        :return: 수집된 데이터프레임
        :rtype: pd.DataFrame
        """
        headers = {"Authorization": f"KakaoAK {self.kakao_api_key}"}
        all_results = []

        for district in self.districts:
            self.log.info(f">>> {district} 지역 수집 시작...")
            district_count = 0

            for category in self.categories:
                results = self._collect_by_district_and_category(
                    district, category, headers
                )
                all_results.extend(results)
                district_count += len(results)

            self.log.info(f"{district} 수집 완료 (누적: {district_count}개)")

        return pd.DataFrame(all_results)

    def _filter_and_deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        음식점만 필터링하고 중복을 제거합니다.

        :param df: 원본 데이터프레임
        :type df: pd.DataFrame
        :return: 전처리된 데이터프레임
        :rtype: pd.DataFrame
        """
        # 음식점만 필터링 (FD6)
        df_filtered = df[df["category_group_code"] == "FD6"].copy()

        # ID 기준 중복 제거
        df_final = df_filtered.drop_duplicates(subset=['id'], keep='first')

        return df_final

    def execute(self, context: Context) -> Dict[str, Any]:
        """
        Kakao API로 음식점 목록을 수집하는 메인 실행 로직

        이 메서드는 다음 단계를 수행:
        1. Kakao API로 지역별/카테고리별 데이터 수집
        2. 음식점만 필터링 (FD6)
        3. 중복 제거
        4. Slack 알림 전송

        :param context: Airflow 실행 컨텍스트
        :type context: Context
        :return: 실행 결과 딕셔너리
        :rtype: Dict[str, Any]
        :raises Exception: 데이터 수집 중 오류 발생 시
        """
        self.log.info("=" * 60)
        self.log.info("🔎 Kakao API 음식점 목록 수집 시작")
        self.log.info("=" * 60)

        # 1. 모든 데이터 수집
        df_raw = self._collect_all_data()
        total_count = len(df_raw)
        self.log.info(f"전체 수집 완료: {total_count}개")

        # 2. 음식점 필터링 및 중복 제거
        df_final = self._filter_and_deduplicate(df_raw)
        filtered_count = len(df_raw[df_raw["category_group_code"] == "FD6"])
        final_count = len(df_final)

        self.log.info(f"음식점 필터링 후: {filtered_count}개")
        self.log.info(f"중복 제거 후: {final_count}개")

        # 3. Slack 알림
        slack_message = (
            f"📌 *KakaoAPIOperator*\n"
            f"카카오 API 수집 완료\n"
            f"- 전체: {total_count}개\n"
            f"- 음식점: {filtered_count}개\n"
            f"- 최종: {final_count}개"
        )
        self._send_slack_notification(slack_message)

        self.log.info("=" * 60)
        self.log.info(f"✅ 완료: 총 {final_count}개 음식점 목록 수집")
        self.log.info("=" * 60)

        return {
            'status': 'success',
            'total_count': total_count,
            'filtered_count': filtered_count,
            'final_count': final_count,
            'dataframe': df_final
        }

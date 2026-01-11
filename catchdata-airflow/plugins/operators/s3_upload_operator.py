from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import pandas as pd
import requests
from airflow.models import BaseOperator
from airflow.utils.context import Context

from plugins.hooks.s3_hook import S3Hook


class S3UploadOperator(BaseOperator):
    """
    DataFrame을 CSV로 변환하여 S3에 업로드하는 커스텀 오퍼레이터

    S3Hook을 사용하여 S3 연결을 관리하고, DataFrame을 CSV 형식으로 S3에 업로드합니다.

    :param aws_conn_id: AWS 연결 ID
    :type aws_conn_id: str
    :param bucket_name: S3 버킷 이름
    :type bucket_name: str
    :param key: S3 객체 키 (파일 경로)
    :type key: str
    :param input_dataframe: 업로드할 DataFrame (선택사항, XCom에서 가져올 수 있음)
    :type input_dataframe: pd.DataFrame, optional
    :param slack_webhook_url: Slack Webhook URL (선택사항)
    :type slack_webhook_url: str, optional

    **책임:**

    - S3Hook 인스턴스 생성 및 관리
    - DataFrame을 CSV로 변환
    - S3에 CSV 업로드
    - UTF-8 BOM 추가 (Excel 호환성)
    - Slack 알림 전송

    **Example:**

    .. code-block:: python

        from operators.s3_upload_operator import S3UploadOperator

        upload_task = S3UploadOperator(
            task_id='upload_to_s3',
            bucket_name='my-bucket',
            key='data/output.csv',
            input_dataframe=df,
        )

    **Returns:**

    실행 결과 딕셔너리:
        - status (str): 'success'
        - bucket (str): S3 버킷 이름
        - key (str): S3 객체 키
        - row_count (int): 업로드된 행 수
        - column_count (int): 업로드된 열 수
        - s3_uri (str): S3 URI (s3://bucket/key)
    """

    template_fields = ('bucket_name', 'key', 'slack_webhook_url')

    def __init__(
        self,
        aws_conn_id: str = 'aws_default',
        bucket_name: str = None,
        key: str = None,
        input_dataframe: pd.DataFrame = None,
        slack_webhook_url: str = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.key = key
        self.input_dataframe = input_dataframe
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

    def _upload_to_s3(self, hook: S3Hook, df: pd.DataFrame) -> Dict[str, Any]:
        """
        DataFrame을 S3에 CSV로 업로드합니다.

        :param hook: S3Hook 인스턴스
        :type hook: S3Hook
        :param df: 업로드할 DataFrame
        :type df: pd.DataFrame
        :return: 업로드 결과 정보
        :rtype: Dict[str, Any]
        """
        s3_client = hook.get_client()

        # UTF-8 BOM 추가로 한글 깨짐 방지 (Excel 호환성)
        csv_buffer = df.to_csv(index=False, encoding='utf-8-sig')

        s3_client.put_object(
            Bucket=self.bucket_name,
            Key=self.key,
            Body=csv_buffer.encode("utf-8-sig"),
            ContentType="text/csv; charset=utf-8"
        )

        return {
            'bucket': self.bucket_name,
            'key': self.key,
            'row_count': len(df),
            'column_count': len(df.columns),
            's3_uri': f"s3://{self.bucket_name}/{self.key}"
        }

    def execute(self, context: Context) -> Dict[str, Any]:
        """
        DataFrame을 S3에 업로드하는 메인 실행 로직

        이 메서드는 다음 단계를 수행:
        1. S3Hook 인스턴스 생성
        2. 입력 DataFrame 준비 (XCom에서 가져오기 또는 직접 전달)
        3. DataFrame을 CSV로 변환
        4. S3에 업로드
        5. Slack 알림 전송

        :param context: Airflow 실행 컨텍스트
        :type context: Context
        :return: 실행 결과 딕셔너리
        :rtype: Dict[str, Any]
        :raises ValueError: 입력 데이터가 없는 경우
        :raises Exception: S3 업로드 중 오류 발생 시
        """
        self.log.info("=" * 60)
        self.log.info("☁️ S3에 CSV 업로드 시작")
        self.log.info("=" * 60)

        hook = S3Hook(aws_conn_id=self.aws_conn_id)

        # XCom에서 데이터 가져오기 (이전 task에서 전달받은 경우)
        if self.input_dataframe is None:
            ti = context['ti']
            previous_result = ti.xcom_pull(task_ids=context['task'].upstream_task_ids)

            # 여러 upstream task 중에서 dataframe 찾기
            if isinstance(previous_result, list):
                for result in previous_result:
                    if isinstance(result, dict) and 'dataframe' in result:
                        self.input_dataframe = result['dataframe']
                        break
            elif isinstance(previous_result, dict) and 'dataframe' in previous_result:
                self.input_dataframe = previous_result['dataframe']

            if self.input_dataframe is None:
                raise ValueError("입력 데이터프레임이 없습니다")

        df = self.input_dataframe

        self.log.info(f"업로드할 데이터: {len(df)}행, {len(df.columns)}열")
        self.log.info(f"대상: s3://{self.bucket_name}/{self.key}")

        # S3에 업로드
        upload_result = self._upload_to_s3(hook, df)

        self.log.info(f"✅ S3 업로드 성공")
        self.log.info(f"📁 위치: {upload_result['s3_uri']}")
        self.log.info(f"📊 데이터: {upload_result['row_count']}행, {upload_result['column_count']}열")

        # Slack 알림
        KST = timezone(timedelta(hours=9))
        timestamp = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
        slack_message = (
            f"📌 *S3UploadOperator*\n"
            f"S3 업로드 완료\n"
            f"- 위치: `{upload_result['s3_uri']}`\n"
            f"- 데이터: {upload_result['row_count']}행\n"
            f"- 시간: {timestamp}"
        )
        self._send_slack_notification(slack_message)

        self.log.info("=" * 60)
        self.log.info("🎉 전체 작업 완료!")
        self.log.info("=" * 60)

        return {
            'status': 'success',
            **upload_result
        }

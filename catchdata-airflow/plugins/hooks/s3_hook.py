import boto3
from airflow.hooks.base import BaseHook


class S3Hook(BaseHook):
    """
    S3 연결을 관리하는 커스텀 Hook

    AWS S3 클라이언트를 생성하고 관리합니다.

    :param aws_conn_id: AWS 연결 ID (Airflow Connection에 설정된 ID)
    :type aws_conn_id: str

    **책임:**

    - S3 클라이언트 생성 및 관리
    - AWS 자격증명 관리
    """

    def __init__(self, aws_conn_id: str = 'aws_default'):
        super().__init__()
        self.aws_conn_id = aws_conn_id
        self._s3_client = None

    def get_client(self):
        """
        S3 클라이언트를 반환합니다 (Lazy Initialization).

        :return: boto3 S3 클라이언트
        :rtype: boto3.client
        """
        if self._s3_client is None:
            # Airflow Connection에서 자격증명 가져오기 (선택사항)
            # 환경변수나 IAM Role을 사용하는 경우 자동으로 인식됨
            try:
                conn = self.get_connection(self.aws_conn_id)
                self._s3_client = boto3.client(
                    's3',
                    aws_access_key_id=conn.login,
                    aws_secret_access_key=conn.password,
                    region_name=conn.extra_dejson.get('region_name', 'ap-northeast-2')
                )
            except Exception:
                # Connection이 없으면 환경변수나 IAM Role 사용
                self._s3_client = boto3.client('s3')

        return self._s3_client

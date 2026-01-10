from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extensions import connection as PGConnection


class RedshiftToRDSHook(BaseHook):
    """
    Redshift와 RDS 연결을 관리하는 커스텀 훅.

    이 훅은 두 개의 PostgresHook을 조합하여 Redshift와 RDS 연결을 관리합니다.

    :param redshift_conn_id: Redshift 연결 ID
    :type redshift_conn_id: str
    :param rds_conn_id: RDS 연결 ID
    :type rds_conn_id: str

    **책임:**

    - Redshift/RDS 연결 관리 (두 개의 PostgresHook 조합)
    - PostgresHook 인스턴스 제공
    """

    def __init__(
        self,
        redshift_conn_id: str = 'redshift_conn',
        rds_conn_id: str = 'rds_conn',
    ):
        super().__init__()
        self.redshift_conn_id = redshift_conn_id
        self.rds_conn_id = rds_conn_id
        self._redshift_hook = None
        self._rds_hook = None

    @property
    def redshift_hook(self) -> PostgresHook:
        """
        Redshift Hook 인스턴스를 반환합니다 (Lazy Initialization).

        :return: Redshift PostgresHook 인스턴스
        :rtype: PostgresHook
        """
        if self._redshift_hook is None:
            self._redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        return self._redshift_hook

    @property
    def rds_hook(self) -> PostgresHook:
        """
        RDS Hook 인스턴스를 반환합니다 (Lazy Initialization).

        :return: RDS PostgresHook 인스턴스
        :rtype: PostgresHook
        """
        if self._rds_hook is None:
            self._rds_hook = PostgresHook(postgres_conn_id=self.rds_conn_id)
        return self._rds_hook

    def get_redshift_connection(self) -> PGConnection:
        """
        Redshift 데이터베이스 연결을 반환합니다.

        :return: Redshift 데이터베이스 연결 객체
        :rtype: psycopg2.extensions.connection
        """
        return self.redshift_hook.get_conn()

    def get_rds_connection(self) -> PGConnection:
        """
        RDS 데이터베이스 연결을 반환합니다.

        :return: RDS 데이터베이스 연결 객체
        :rtype: psycopg2.extensions.connection
        """
        return self.rds_hook.get_conn()
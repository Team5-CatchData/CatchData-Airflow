from typing import Any, Dict, List, Tuple

from airflow.models import BaseOperator
from airflow.utils.context import Context

from plugins.hooks.redshift_to_rds_hook import RedshiftToRDSHook


class RDSToRedshiftOperator(BaseOperator):
    """
    RDS에서 Redshift로 데이터를 전송하는 커스텀 오퍼레이터

    RedshiftToRDSHook을 사용하여 연결을 관리하고,
    RDS에서 데이터를 추출하여 Redshift로 UPSERT 방식으로 데이터를 전송합니다.

    :param redshift_conn_id: Redshift 연결 ID
    :type redshift_conn_id: str
    :param rds_conn_id: RDS 연결 ID
    :type rds_conn_id: str
    :param source_table: 소스 테이블명 (RDS)
    :type source_table: str
    :param target_table: 타겟 테이블명 (Redshift, 스키마 포함)
    :type target_table: str
    :param conflict_column: CONFLICT 처리 기준 컬럼명
    :type conflict_column: str
    :param columns: 추출할 컬럼 리스트. None인 경우 기본 컬럼 사용
    :type columns: List[str], optional

    **책임:**

    - Hook 인스턴스 생성 및 관리
    - 데이터 추출 쿼리 실행 (RDS)
    - 데이터 적재 쿼리 실행 (Redshift)
    - 트랜잭션 관리
    - 전체 전송 프로세스 오케스트레이션 (추출 → UPSERT)
    - 로깅 및 에러 핸들링
    - 실행 결과 반환 (XCom을 통한 메타데이터 전달)

    **Example:**

    .. code-block:: python

        from operators.rds_to_redshift_operator import RDSToRedshiftOperator

        transfer_task = RDSToRedshiftOperator(
            task_id='transfer_chat_history',
            redshift_conn_id='redshift_conn',
            rds_conn_id='rds_conn',
            source_table='main_chathistory',
            target_table='analytics.chat_history',
            conflict_column='id',
        )

    **Returns:**

    실행 결과 딕셔너리:
        - status (str): 'success' 또는 'skipped'
        - extracted_count (int): 추출된 레코드 수
        - upserted_count (int): UPSERT된 레코드 수
        - source_table (str): 소스 테이블명
        - target_table (str): 타겟 테이블명
    """

    template_fields = ('source_table', 'target_table')

    def __init__(
        self,
        redshift_conn_id: str = 'redshift_conn',
        rds_conn_id: str = 'rds_conn',
        source_table: str = 'main_chathistory',
        target_table: str = 'analytics.chat_history',
        conflict_column: str = 'id',
        columns: List[str] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.rds_conn_id = rds_conn_id
        self.source_table = source_table
        self.target_table = target_table
        self.conflict_column = conflict_column
        self.columns = columns

    def _extract_from_rds(self, hook: RedshiftToRDSHook) -> List[Tuple]:
        """
        RDS에서 데이터를 추출합니다.

        :param hook: RedshiftToRDSHook 인스턴스
        :type hook: RedshiftToRDSHook
        :return: 추출된 레코드 리스트
        :rtype: List[Tuple]
        """
        columns = self.columns
        if columns is None:
            columns = [
                'id',
                'query',
                'answer',
                'created_at',
            ]

        columns_str = ', '.join(columns)
        sql = f"""
            SELECT {columns_str}
            FROM {self.source_table}
            ORDER BY id
        """

        self.log.info(f"RDS에서 데이터 추출 시작: {self.source_table}")
        records = hook.rds_hook.get_records(sql)
        self.log.info(f"추출 완료: {len(records):,}개 레코드")

        return records

    def _upsert_to_redshift(self, hook: RedshiftToRDSHook, records: List[Tuple]) -> int:
        """
        Redshift에 데이터를 UPSERT합니다.

        :param hook: RedshiftToRDSHook 인스턴스
        :type hook: RedshiftToRDSHook
        :param records: 삽입할 레코드 리스트
        :type records: List[Tuple]
        :return: 처리된 레코드 수
        :rtype: int
        """
        if not records:
            self.log.warning("삽입할 레코드가 없습니다")
            return 0

        conn = None
        cursor = None

        try:
            conn = hook.get_redshift_connection()
            cursor = conn.cursor()

            upsert_sql = f"""
                INSERT INTO {self.target_table} (
                    "{self.conflict_column}", query, answer, created_at
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT ("{self.conflict_column}")
                DO UPDATE SET
                    query = EXCLUDED.query,
                    answer = EXCLUDED.answer,
                    created_at = EXCLUDED.created_at
            """

            self.log.info(f"Redshift UPSERT 시작: {len(records):,}개 레코드")
            cursor.executemany(upsert_sql, records)
            conn.commit()

            record_count = len(records)
            self.log.info(f"UPSERT 완료: {record_count:,}개 레코드")

            return record_count

        except Exception as e:
            if conn:
                conn.rollback()
            self.log.error(f"UPSERT 실패: {e}")
            raise

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def execute(self, context: Context) -> Dict[str, Any]:
        """
        RDS에서 Redshift로 데이터를 전송하는 메인 실행 로직

        이 메서드는 다음 단계를 수행:
        1. RedshiftToRDSHook 인스턴스 생성
        2. RDS에서 데이터 추출
        3. Redshift에 UPSERT

        :param context: Airflow 실행 컨텍스트
        :type context: Context
        :return: 실행 결과 딕셔너리 (status, extracted_count, upserted_count, source_table, target_table)
        :rtype: Dict[str, Any]
        :raises Exception: 데이터 전송 중 오류 발생 시
        """
        self.log.info("RDS → Redshift 데이터 전송 시작")

        hook = RedshiftToRDSHook(
            redshift_conn_id=self.redshift_conn_id,
            rds_conn_id=self.rds_conn_id
        )

        # 1. RDS에서 데이터 추출
        self.log.info(f"[1/2] RDS에서 데이터 추출: {self.source_table}")
        records = self._extract_from_rds(hook)

        if not records:
            self.log.warning("추출된 데이터가 없습니다. 작업을 종료합니다.")
            return {
                'status': 'skipped',
                'extracted_count': 0,
                'upserted_count': 0,
                'source_table': self.source_table,
                'target_table': self.target_table
            }

        extracted_count = len(records)
        self.log.info(f"추출 완료: {extracted_count:,}개 레코드")

        # 2. Redshift에 UPSERT
        self.log.info(f"[2/2] Redshift에 UPSERT: {self.target_table}")
        upserted_count = self._upsert_to_redshift(hook, records)

        self.log.info(f"전송 완료: {upserted_count:,}개 레코드 처리")

        return {
            'status': 'success',
            'extracted_count': extracted_count,
            'upserted_count': upserted_count,
            'source_table': self.source_table,
            'target_table': self.target_table
        }

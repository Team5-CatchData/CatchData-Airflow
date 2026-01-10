from datetime import datetime, timedelta

from airflow.sdk import DAG
from plugins.operators.rds_to_redshift_operator import RDSToRedshiftOperator

default_args = {
    'owner': 'jaehyeon',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='rds_to_redshift_transfer',
    default_args=default_args,
    description='RDS → Redshift 데이터 전송 (UPSERT 방식)',
    schedule='30 3 * * *',
    catchup=False,
    tags=['rds', 'redshift', 'upsert', 'chathistory'],
) as dag:

    transfer_task = RDSToRedshiftOperator(
        task_id='transfer_chat_history',
        redshift_conn_id='redshift_conn',
        rds_conn_id='rds_conn',
        source_table='main_chathistory',
        target_table='analytics.chat_history',
        conflict_column='id',
    )

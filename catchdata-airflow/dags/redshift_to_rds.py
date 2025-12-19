import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.utils.state import DagRunState


def transfer_redshift_to_rds(**context):
    """Redshift → RDS 전송 (UPSERT 방식)"""
    
    redshift_hook = PostgresHook(postgres_conn_id="redshift_conn")
    rds_hook = PostgresHook(postgres_conn_id="rds_conn")
    
    # 1. Redshift에서 데이터 추출
    logging.info("1. Redshift 데이터 추출")
    sql = """
        SELECT 
            id, name, region, city, category, rating, 
            phone, x, y, image_url, address,
            rec_quality, rec_balanced, rec_convenience
        FROM analytics.map_search
        ORDER BY id
    """
    
    records = redshift_hook.get_records(sql)
    record_count = len(records)
    logging.info(f"✓ {record_count:,}개 추출 완료")
    
    if not records:
        logging.warning("추출된 데이터가 없습니다")
        return
    
    # 2. RDS에 UPSERT
    logging.info(f"2. RDS UPSERT 시작 ({record_count:,}개)")
    
    conn = rds_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        cursor.executemany("""
            INSERT INTO main_restaurant (
                id, name, region, city, category, rating, phone, x, y,
                image_url, address, rec_quality, rec_balanced, rec_convenience
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) 
            DO UPDATE SET
                name = EXCLUDED.name,
                region = EXCLUDED.region,
                city = EXCLUDED.city,
                category = EXCLUDED.category,
                rating = EXCLUDED.rating,
                phone = EXCLUDED.phone,
                x = EXCLUDED.x,
                y = EXCLUDED.y,
                image_url = EXCLUDED.image_url,
                address = EXCLUDED.address,
                rec_quality = EXCLUDED.rec_quality,
                rec_balanced = EXCLUDED.rec_balanced,
                rec_convenience = EXCLUDED.rec_convenience
        """, records)
        
        conn.commit()
        logging.info(f"✓ {record_count:,}개 UPSERT 완료!")
        
    except Exception as e:
        conn.rollback()
        logging.error(f"✗ UPSERT 실패: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='redshift_to_rds_transfer',
    default_args=default_args,
    description='Redshift → RDS 데이터 전송 (UPSERT 방식)',
    schedule='30 3 * * 1',  # 매주 월요일 새벽 3시 30분
    catchup=False,
    tags=['redshift', 'rds', 'upsert'],
) as dag:
    
    # # 상위 DAG 완료 대기
    # wait_for_pipeline = ExternalTaskSensor(
    #     task_id='wait_for_redshift_pipeline',
    #     external_dag_id='redshift_map_search_update_pipeline',
    #     external_task_id=None,  # DAG 전체 완료 대기
    #     allowed_states=[DagRunState.SUCCESS],
    #     failed_states=[DagRunState.FAILED],
    #     execution_delta=timedelta(hours=0),
    #     poke_interval=60,
    #     timeout=3600,
    #     mode='poke',
    # )
    
    # 데이터 전송
    transfer_task = PythonOperator(
        task_id='transfer_data',
        python_callable=transfer_redshift_to_rds,
    )
    
    # 의존성
    transfer_task
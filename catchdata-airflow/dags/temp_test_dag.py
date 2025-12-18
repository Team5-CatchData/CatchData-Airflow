from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id="temp_test_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,      # 수동 실행
    catchup=False,
    tags=["test", "debug"],
) as dag:

    @task
    def hello():
        print("✅ Airflow DAG is working properly!")

    hello()

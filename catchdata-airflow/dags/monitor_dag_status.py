from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres  import PostgresHook
from datetime import datetime, timedelta
import requests

# =========================
# 설정값
# =========================
SLACK_WEBHOOK_URL = "{{ var.value.SLACK_WEBHOOK_URL }}"

AIRFLOW_BASE_URL = "http://localhost:18080"

CHECK_INTERVAL_MIN = 60        # 최근 1시간
RUNNING_THRESHOLD_MIN = 30    # 30분 이상 running

# =========================
# 모니터링 로직
# =========================
def monitor_dags():
    hook = PostgresHook(postgres_conn_id="airflow_db")
    conn = hook.get_conn()
    cur = conn.cursor()

    # 실패한 DAG
    cur.execute(f"""
        SELECT dag_id, logical_date
        FROM dag_run
        WHERE state = 'failed'
        AND logical_date >= NOW() - INTERVAL '{CHECK_INTERVAL_MIN} minutes'
        ORDER BY logical_date DESC
    """)
    failed_dags = cur.fetchall()

    # 실패한 Task
    cur.execute(f"""
        SELECT dag_id, task_id, logical_date
        FROM task_instance
        WHERE state = 'failed'
        AND logical_date >= NOW() - INTERVAL '{CHECK_INTERVAL_MIN} minutes'
        ORDER BY logical_date DESC
    """)
    failed_tasks = cur.fetchall()
    # 장시간 running DAG
    cur.execute(f"""
        SELECT dag_id, logical_date
        FROM dag_run
        WHERE state = 'running'
        AND logical_date <= NOW() - INTERVAL '{RUNNING_THRESHOLD_MIN} minutes'
        ORDER BY logical_date
    """)
    long_running_dags = cur.fetchall()

    cur.close()
    conn.close()

    if not failed_dags and not failed_tasks and not long_running_dags:
        return  # 알림 보낼 게 없으면 종료

    # =========================
    # Slack 메시지 구성
    # =========================
    message = "*🚨 Airflow DAG 모니터링 알림*\n\n"

    if failed_dags:
        message += "❌ *실패한 DAG (최근 1시간)*\n"
        for dag_id, exec_date in failed_dags:
            message += f"• `{dag_id}` @ {exec_date}\n"
        message += "\n"

    if failed_tasks:
        message += "🧩 *실패한 Task (최근 1시간)*\n"
        for dag_id, task_id, exec_date in failed_tasks:
            message += f"• `{dag_id}.{task_id}` @ {exec_date}\n"
        message += "\n"

    if long_running_dags:
        message += "🕒 *30분 이상 실행 중인 DAG*\n"
        for dag_id, exec_date in long_running_dags:
            message += f"• `{dag_id}` (시작: {exec_date})\n"
        message += "\n"

    # DAG Grid 링크 (중복 제거)
    target_dag_ids = set([d[0] for d in failed_dags + long_running_dags])
    
    if target_dag_ids:
        message += "🔗 *DAG Grid 바로가기*\n"
        for dag_id in target_dag_ids:
            message += f"• <{AIRFLOW_BASE_URL}/dags/{dag_id}/grid|{dag_id} Grid>\n"


    requests.post(
        SLACK_WEBHOOK_URL,
        json={"text": message},
        timeout=10
    )

# =========================
# DAG 정의
# =========================
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="dag_monitoring",
    description="Airflow DAG 상태 모니터링 (실패 / 장기 실행)",
    start_date=datetime(2025, 1, 1),
    schedule="*/10 * * * *",  # 10분마다
    catchup=False,
    default_args=default_args,
    tags=["monitoring", "slack"]
) as dag:

    monitor_task = PythonOperator(
        task_id="monitor_dag_status",
        python_callable=monitor_dags
    )

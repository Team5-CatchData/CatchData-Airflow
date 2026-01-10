"""
Custom Airflow Plugins

이 패키지는 Redshift에서 RDS로 데이터를 전송하는 커스텀 훅과 오퍼레이터를 포함합니다.
"""

from airflow.plugins_manager import AirflowPlugin

from plugins.hooks.redshift_to_rds_hook import RedshiftToRDSHook
from plugins.operators.redshift_to_rds_operator import RedshiftToRDSOperator


class CustomPlugin(AirflowPlugin):
    """커스텀 Airflow 플러그인"""
    name = "custom_plugin"
    hooks = [RedshiftToRDSHook]
    operators = [RedshiftToRDSOperator]

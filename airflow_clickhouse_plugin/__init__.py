from airflow.plugins_manager import AirflowPlugin
from .hooks.clickhouse_hook import ClickHouseHook
from .operators.clickhouse_operator import ClickHouseOperator


class ClickHouseOperatorPlugin(AirflowPlugin):
    name = 'clickhouse_operator'
    operators = [ClickHouseOperator]


class ClickHouseHookPlugin(AirflowPlugin):
    name = 'clickhouse_hook'
    hooks = [ClickHouseHook]

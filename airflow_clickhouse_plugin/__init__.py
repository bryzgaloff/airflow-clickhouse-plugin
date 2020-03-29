from airflow.plugins_manager import AirflowPlugin
from .hooks.clickhouse_hook import ClickHouseHook
from .operators.clickhouse_operator import ClickHouseOperator


class ClickHousePlugin(AirflowPlugin):
    name = 'clickhouse_plugin'
    operators = [ClickHouseOperator]
    hooks = [ClickHouseHook]

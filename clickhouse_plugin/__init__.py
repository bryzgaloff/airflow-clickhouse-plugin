"""
clickhouse_plugin
~~~~~~
"""

from airflow.plugins_manager import AirflowPlugin
from .hooks.clickhouse_hook import ClickHouseHook


class ClickHousePlugin(AirflowPlugin):
    name = 'clickhouse_plugin'
    operators = []
    hooks = [ClickHouseHook]

from airflow.providers.common.sql.sensors.sql import SqlSensor

from airflow_clickhouse_plugin.hooks.clickhouse_dbapi import ClickHouseDbApiHook
from airflow_clickhouse_plugin.operators.clickhouse_dbapi import \
    ClickHouseDbApiHookMixin


class ClickHouseSqlSensor(ClickHouseDbApiHookMixin, SqlSensor):
    def _get_hook(self) -> ClickHouseDbApiHook:
        return self._get_clickhouse_db_api_hook()

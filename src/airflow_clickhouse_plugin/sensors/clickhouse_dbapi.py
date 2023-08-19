import typing as t

from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow_clickhouse_plugin.hooks.clickhouse_dbapi import ClickHouseDbApiHook


class ClickHouseSqlSensor(SqlSensor):
    def __init__(
            self,
            *args,
            database: t.Optional[str] = None,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._database = database

    def _get_hook(self) -> ClickHouseDbApiHook:
        return ClickHouseDbApiHook(
            clickhouse_conn_id=self.conn_id,
            database=self._database,
        )

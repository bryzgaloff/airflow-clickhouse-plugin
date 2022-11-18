from typing import Dict, Callable, Any, Optional

from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.decorators import apply_defaults
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook


class ClickHouseSqlSensor(SqlSensor):
    default_conn_name = ClickHouseHook.default_conn_name

    def __init__(
        self,
        sql: str = None,
        clickhouse_conn_id: str = default_conn_name,
        parameters: Optional[Dict[str, Any]] = None,
        database: Optional[str] = None,
        success: Optional[Callable[[Any], bool]] = None,
        failure: Optional[Callable[[Any], bool]] = None,
        fail_on_empty: bool = False,
        *args,
        **kwargs,
    ):
        super().__init__(
            conn_id=clickhouse_conn_id,
            sql=sql,
            parameters=parameters,
            success=success,
            failure=failure,
            fail_on_empty=fail_on_empty,
            *args,
            **kwargs,
        )
        self._database = database

    def _get_hook(self) -> ClickHouseHook:
        return ClickHouseHook(
            clickhouse_conn_id=self.conn_id,
            database=self._database,
        )

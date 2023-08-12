import typing as t

from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow_clickhouse_plugin.hooks.clickhouse_dbapi_hook import ClickHouseDbApiHook


class ClickHouseSqlSensor(SqlSensor):
    template_fields: t.Sequence[str] = ('sql',)
    template_ext: t.Sequence[str] = ('.sql',)
    default_conn_name = ClickHouseDbApiHook.default_conn_name

    def __init__(
        self,
        sql: str = None,
        clickhouse_conn_id: str = default_conn_name,
        parameters: t.Optional[t.Dict[str, t.Any]] = None,
        database: t.Optional[str] = None,
        success: t.Optional[t.Callable[[t.Any], bool]] = None,
        failure: t.Optional[t.Callable[[t.Any], bool]] = None,
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

    def _get_hook(self) -> ClickHouseDbApiHook:
        return ClickHouseDbApiHook(
            clickhouse_conn_id=self.conn_id,
            database=self._database,
        )

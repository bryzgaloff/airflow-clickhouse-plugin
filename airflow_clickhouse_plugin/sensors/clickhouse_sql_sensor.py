from airflow.sensors.sql_sensor import SqlSensor
from airflow_clickhouse_plugin.hooks import clickhouse_hook


class ClickHouseSqlSensor(SqlSensor):
    def __init__(
        self,
        clickhouse_conn_id: str = "clickhouse_default",
        database=None,
        *args,
        **kwargs
    ):
        super(ClickHouseSqlSensor, self).__init__(conn_id=None, *args, **kwargs)
        self._conn_id = clickhouse_conn_id
        self._database = database

    def _get_hook(self):
        return clickhouse_hook.ClickHouseHook(
            clickhouse_conn_id=self._conn_id, database=self._database,
        )

    def poke(self, context):
        return super(ClickHouseSqlSensor, self).poke(context)

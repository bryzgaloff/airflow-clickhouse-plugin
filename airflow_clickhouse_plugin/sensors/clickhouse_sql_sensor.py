from airflow.exceptions import AirflowException
from airflow.sensors.sql_sensor import SqlSensor
from airflow_clickhouse_plugin.hooks import clickhouse_hook


class ClickHouseSqlSensor(SqlSensor):
    def __init__(
        self,
        clickhouse_conn_id: str = 'clickhouse_default',
        database: str = None,
        *args,
        **kwargs,
    ):
        super(ClickHouseSqlSensor, self).__init__(conn_id=None, *args, **kwargs)
        self._conn_id = clickhouse_conn_id
        self._database = database

    def poke(self, context):
        hook = clickhouse_hook.ClickHouseHook(
            clickhouse_conn_id=self._conn_id, database=self._database,
        )

        self.log.info("Poking: %s (with parameters %s)", self.sql, self.parameters)
        records = hook.get_records(self.sql, self.parameters)
        if not records:
            if self.fail_on_empty:
                raise AirflowException(
                    "No rows returned, raising as per fail_on_empty flag"
                )
            else:
                return False
        first_cell = records[0][0]
        if self.failure is not None:
            if callable(self.failure):
                if self.failure(first_cell):
                    raise AirflowException(
                        "Failure criteria met. self.failure({}) returned True".format(
                            first_cell
                        )
                    )
            else:
                raise AirflowException(
                    "self.failure is present, but not callable -> {}".format(
                        self.success
                    )
                )
        if self.success is not None:
            if callable(self.success):
                return self.success(first_cell)
            else:
                raise AirflowException(
                    "self.success is present, but not callable -> {}".format(
                        self.success
                    )
                )
        return str(first_cell) not in ("0", "")

from typing import *

from airflow.models import BaseOperator

from clickhouse_plugin.hooks import ClickHouseHook


class ClickHouseOperator(BaseOperator):
    template_fields = ('_sql',)

    def __init__(
            self,
            sql: Union[str, Iterable[str]],
            clickhouse_conn_id: str = 'clickhouse_default',
            parameters: Dict[str, Any] = None,
            database=None,
            *args, **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._sql = sql
        self._conn_id = clickhouse_conn_id
        self._parameters = parameters
        self._database = database

    def execute(self, context: Dict[str, Any]) -> Any:
        hook = ClickHouseHook(
            clickhouse_conn_id=self._conn_id,
            database=self._database,
        )
        return hook.run(self._sql, self._parameters)

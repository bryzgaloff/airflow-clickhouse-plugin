import typing as t

from airflow.models import BaseOperator

from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook


class ClickHouseOperator(BaseOperator):
    template_fields = ('_sql',)
    template_ext: t.Sequence[str] = ('.sql',)
    template_fields_renderers = {'_sql': 'sql'}
    default_conn_name = ClickHouseHook.default_conn_name

    def __init__(
            self,
            sql: t.Union[str, t.Iterable[str]],
            clickhouse_conn_id: str = default_conn_name,
            parameters: t.Optional[t.Dict[str, t.Any]] = None,
            database: t.Optional[str] = None,
            *args,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._sql = sql
        self._conn_id = clickhouse_conn_id
        self._parameters = parameters
        self._database = database

    def execute(self, context: t.Dict[str, t.Any]) -> t.Any:
        hook = ClickHouseHook(
            clickhouse_conn_id=self._conn_id,
            database=self._database,
        )
        return hook.run(self._sql, self._parameters)

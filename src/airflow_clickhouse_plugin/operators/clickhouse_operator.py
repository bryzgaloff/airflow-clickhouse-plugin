import typing as t

from airflow.models import BaseOperator

from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook, ExecuteParamsT


class BaseClickHouseOperator(BaseOperator):
    """
    A superclass for operator classes. Defines __init__ with common arguments.

    Includes arguments of clickhouse_driver.Client.execute.
    """

    template_fields = ('_sql',)
    template_ext: t.Sequence[str] = ('.sql',)
    template_fields_renderers = {'_sql': 'sql'}

    def __init__(
            self,
            *args,
            sql: t.Union[str, t.Iterable[str]],
            # arguments of clickhouse_driver.Client.execute
            params: t.Optional[ExecuteParamsT] = None,
            with_column_types: bool = False,
            external_tables: t.Optional[t.List[dict]] = None,
            query_id: t.Optional[str] = None,
            settings: t.Dict[str, t.Any] = None,
            types_check: bool = False,
            columnar: bool = False,
            # arguments of ClickHouseHook.__init__
            clickhouse_conn_id: str = ClickHouseHook.default_conn_name,
            database: t.Optional[str] = None,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self._sql = sql

        self._params = params
        self._with_column_types = with_column_types
        self._external_tables = external_tables
        self._query_id = query_id
        self._settings = settings
        self._types_check = types_check
        self._columnar = columnar

        self._clickhouse_conn_id = clickhouse_conn_id
        self._database = database


class ClickHouseOperator(BaseClickHouseOperator, BaseOperator):
    def execute(self, context: t.Dict[str, t.Any]) -> t.Any:
        hook = ClickHouseHook(
            clickhouse_conn_id=self._clickhouse_conn_id,
            database=self._database,
        )
        return hook.execute(
            self._sql,
            self._params,
            self._with_column_types,
            self._external_tables,
            self._query_id,
            self._settings,
            self._types_check,
            self._columnar,
        )

import typing as t

from airflow.models import BaseOperator

from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook, \
    ExecuteParamsT, ExecuteReturnT, default_conn_name


class BaseClickHouseOperator(BaseOperator):
    """
    A superclass for operator classes. Defines __init__ with common arguments.

    Includes arguments of clickhouse_driver.Client.execute.
    """

    template_fields = (  # all str-containing arguments
        '_sql',
        '_parameters',
        '_external_tables',
        '_query_id',
        '_settings',
        '_database',
    )
    template_ext: t.Sequence[str] = ('.sql',)
    template_fields_renderers = {
        '_sql': 'sql',
        '_parameters': 'json',
        '_external_tables': 'json',
        '_settings': 'json',
    }

    def __init__(
            self,
            *args,
            sql: t.Union[str, t.Iterable[str]],
            # arguments of clickhouse_driver.Client.execute
            parameters: t.Optional[ExecuteParamsT] = None,
            with_column_types: bool = False,
            external_tables: t.Optional[t.List[dict]] = None,
            query_id: t.Optional[str] = None,
            settings: t.Dict[str, t.Any] = None,
            types_check: bool = False,
            columnar: bool = False,
            # arguments of ClickHouseHook.__init__
            clickhouse_conn_id: str = default_conn_name,
            database: t.Optional[str] = None,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self._sql = sql

        self._parameters = parameters
        self._with_column_types = with_column_types
        self._external_tables = external_tables
        self._query_id = query_id
        self._settings = settings
        self._types_check = types_check
        self._columnar = columnar

        self._clickhouse_conn_id = clickhouse_conn_id
        self._database = database

    def _hook_execute(self) -> ExecuteReturnT:
        hook = ClickHouseHook(
            clickhouse_conn_id=self._clickhouse_conn_id,
            database=self._database,
        )
        return hook.execute(
            self._sql,
            self._parameters,
            self._with_column_types,
            self._external_tables,
            self._query_id,
            self._settings,
            self._types_check,
            self._columnar,
        )


class ClickHouseOperator(BaseClickHouseOperator, BaseOperator):
    def execute(self, context: t.Dict[str, t.Any]) -> ExecuteReturnT:
        return self._hook_execute()

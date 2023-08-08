from typing import Any, Dict, Iterable, Optional, Sequence, Union

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook


class ClickHouseOperator(BaseOperator):
    template_fields = ('_sql',)
    template_ext: Sequence[str] = ('.sql',)
    template_fields_renderers = {'_sql': 'sql'}
    default_conn_name = ClickHouseHook.default_conn_name

    def __init__(
            self,
            sql: Union[str, Iterable[str]],
            clickhouse_conn_id: str = default_conn_name,
            parameters: Optional[Dict[str, Any]] = None,
            database: Optional[str] = None,
            with_column_types: Optional[bool] = False,
            types_check: Optional[bool] = False,
            query_id: Optional[str] = None,
            *args,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._sql = sql
        self._conn_id = clickhouse_conn_id
        self._parameters = parameters
        self._database = database
        self._with_column_types = with_column_types
        self._types_check = types_check
        self._query_id = query_id

    def execute(self, context: Dict[str, Any]) -> Any:
        hook = ClickHouseHook(
            clickhouse_conn_id=self._conn_id,
            database=self._database,
        )
        return hook.run(self._sql, self._parameters, self._with_column_types, self._types_check, self._query_id)

import contextlib
from itertools import islice
from typing import *

from airflow.hooks.base import BaseHook
from clickhouse_driver import Client


class ClickHouseHook(BaseHook):
    conn_name_attr = 'clickhouse_conn_id'
    default_conn_name = 'clickhouse_default'

    def __init__(
            self,
            clickhouse_conn_id: str = default_conn_name,
            database: Optional[str] = None,
    ):
        self.clickhouse_conn_id = clickhouse_conn_id
        self.database = database

    def get_conn(self) -> Client:
        conn = self.get_connection(self.clickhouse_conn_id)
        connection_kwargs = conn.extra_dejson.copy()
        if conn.port:
            connection_kwargs.update(port=int(conn.port))
        if conn.login:
            connection_kwargs.update(user=conn.login)
        if conn.password:
            connection_kwargs.update(password=conn.password)
        if self.database:
            connection_kwargs.update(database=self.database)
        elif conn.schema:
            connection_kwargs.update(database=conn.schema)
        return Client(conn.host or 'localhost', **connection_kwargs)

    def get_records(self, sql: str, parameters: Optional[dict] = None) -> List[Tuple]:
        self._log_query(sql, parameters)
        with _disconnecting(self.get_conn()) as client:
            return client.execute(sql, params=parameters)

    def get_first(self, sql: str, parameters: Optional[dict] = None) -> Optional[Tuple]:
        self._log_query(sql, parameters)
        with _disconnecting(self.get_conn()) as client:
            try:
                return next(client.execute_iter(sql, params=parameters))
            except StopIteration:
                return None

    def get_pandas_df(self, sql: str):
        import pandas as pd
        rows, columns_defs = self.run(sql, with_column_types=True)
        columns = [column_name for column_name, _ in columns_defs]
        return pd.DataFrame(rows, columns=columns)

    def run(
            self,
            sql: Union[str, Iterable[str]],
            parameters: Union[None, dict, list, tuple, Generator] = None,
            with_column_types: bool = False,
            types_check: bool = False,
    ) -> Any:
        if isinstance(sql, str):
            sql = (sql,)
        with _disconnecting(self.get_conn()) as conn:
            last_result = None
            for s in sql:
                self._log_query(s, parameters)
                last_result = conn.execute(
                    s,
                    params=parameters,
                    with_column_types=with_column_types,
                    types_check=types_check,
                )

        return last_result

    def _log_query(
            self,
            sql: str,
            parameters: Union[None, dict, list, tuple, Generator],
    ) -> None:
        self.log.info(
            '%s%s', sql,
            f' with {self._log_params(parameters)}' if parameters else '',
        )

    @staticmethod
    def _log_params(
            parameters: Union[dict, list, tuple, Generator],
            limit: int = 10,
    ) -> str:
        if isinstance(parameters, Generator) or len(parameters) <= limit:
            return str(parameters)
        if isinstance(parameters, dict):
            head = dict(islice(parameters.items(), limit))
        else:
            head = parameters[:limit]
        head_str = str(head)
        closing_paren = head_str[-1]
        return f'{head_str[:-1]} … and {len(parameters) - limit} ' \
            f'more parameters{closing_paren}'


_InnerT = TypeVar('_InnerT')


@contextlib.contextmanager
def _disconnecting(thing: _InnerT) -> ContextManager[_InnerT]:
    """
    Context to automatically disconnect something at the end of a block.

    Similar to ``contextlib.closing`` but calls .disconnect() method on exit.

    Code like this:

    >>> with _disconnecting(<module>.open(<arguments>)) as f:
    >>>     <block>

    is equivalent to this:

    >>> f = <module>.open(<arguments>)
    >>> try:
    >>>     <block>
    >>> finally:
    >>>     f.disconnect()
    """
    try:
        yield thing
    finally:
        thing.disconnect()

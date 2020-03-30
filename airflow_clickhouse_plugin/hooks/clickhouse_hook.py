from itertools import islice
from typing import *

from airflow.hooks.base_hook import BaseHook
from clickhouse_driver import Client


class ClickHouseHook(BaseHook):
    _DEFAULT_CONN_ID = 'clickhouse_default'

    def __init__(
            self,
            clickhouse_conn_id: str = _DEFAULT_CONN_ID,
            database: str = None,
    ):
        super().__init__(source=None)
        self.clickhouse_conn_id = clickhouse_conn_id
        self.database = database

    def get_conn(self) -> Client:
        conn = self.get_connection(self.clickhouse_conn_id)
        return self.create_connection(
            host=conn.host or 'localhost',
            port=int(conn.port) if conn.port else 9000,
            user=conn.login or 'default',
            password=conn.password or '',
            database=self.database or conn.schema or 'default',
        )

    @classmethod
    def create_connection(
            cls, host: str, port: int, database: str, user: str, password: str
    ) -> Client:
        return Client(host, port, database, user, password)

    def get_records(self, sql: str, parameters: dict = None) -> List[Tuple]:
        self._log_query(sql, parameters)
        with disconnecting(self.get_conn()) as client:
            return client.execute(sql, params=parameters)

    def get_first(self, sql: str, parameters: dict = None) -> Tuple:
        self._log_query(sql, parameters)
        with disconnecting(self.get_conn()) as client:
            return next(client.execute_iter(sql, params=parameters))

    def get_pandas_df(self, *args, **kwargs):
        raise NotImplementedError

    def run(
            self,
            sql: Union[str, Iterable[str]],
            parameters: Union[dict, list, tuple, Generator] = None,
    ) -> Any:
        if isinstance(sql, str):
            sql = (sql,)
        with disconnecting(self.get_conn()) as conn:
            last_result = None
            for s in sql:
                self._log_query(s, parameters)
                last_result = conn.execute(s, params=parameters)
        return last_result

    def _log_query(
            self,
            sql: str,
            parameters: Union[dict, list, tuple, Generator],
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


class disconnecting(ContextManager, Generic[_InnerT]):
    """ Context to automatically disconnect something at the end of a block.

    Similar to contextlib.closing but calls .disconnect() method on exit.

    Code like this:

    >>> with disconnecting(<module>.open(<arguments>)) as f:
    >>>     <block>

    is equivalent to this:

    >>> f = <module>.open(<arguments>)
    >>> try:
    >>>     <block>
    >>> finally:
    >>>     f.disconnect()
    """

    def __init__(self, thing: _InnerT):
        self._thing = thing

    def __enter__(self) -> _InnerT:
        return self._thing

    def __exit__(self, *exc_info) -> None:
        self._thing.disconnect()

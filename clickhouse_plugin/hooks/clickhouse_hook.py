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
        return ClickHouseHook.create_connection(
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
        self.log.info(f'{sql} with {parameters}' if parameters else sql)
        with disconnecting(self.get_conn()) as client:
            return client.execute(sql, params=parameters)

    def get_first(self, sql: str, parameters: dict = None) -> Tuple:
        self.log.info(f'{sql} with {parameters}' if parameters else sql)
        with disconnecting(self.get_conn()) as client:
            return next(client.execute_iter(sql, params=parameters))

    def get_pandas_df(self, *args, **kwargs):
        raise NotImplementedError

    def run(
            self,
            sql: str,
            parameters: Union[dict, list, tuple, Generator] = None,
    ) -> Any:
        self.log.info(f'{sql} with {parameters}' if parameters else sql)
        with disconnecting(self.get_conn()) as conn:
            return conn.execute(sql, params=parameters)


_InnerT = TypeVar('_InnerT')


class disconnecting(ContextManager, Generic[_InnerT]):
    """Context to automatically disconnects something at the end of a block.

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

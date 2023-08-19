import contextlib
import logging
from itertools import islice
import typing as t

from airflow.hooks.base import BaseHook
import clickhouse_driver

# annotated according to clickhouse_driver.Client.execute comments
_ParamT = t.NewType('_ParamT', t.Union[list, tuple, dict])
ExecuteParamsT = t.NewType(
    'ExecuteParamsT',
    t.Union[
        # INSERT queries
        t.List[_ParamT], t.Tuple[_ParamT, ...], t.Generator[_ParamT, None, None],
        # SELECT queries
        dict,
    ],
)
ExecuteReturnT = t.NewType(
    # clickhouse_driver.Client.execute return type
    'ExecuteReturnT',
    t.Union[
        int,  # number of inserted rows
        t.List[tuple],  # list of tuples with rows/columns
        t.Tuple[t.List[tuple], t.List[t.Tuple[str, str]]],  # with_column_types
    ],
)


class BaseClickHouseHook(BaseHook):
    default_conn_name = 'clickhouse_default'

    def __init__(
            self,
            *args,
            clickhouse_conn_id: str = default_conn_name,
            database: t.Optional[str] = None,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._clickhouse_conn_id = clickhouse_conn_id
        self._database = database

    def get_conn(self) -> clickhouse_driver.Client:
        conn = self.get_connection(self._clickhouse_conn_id)
        connection_kwargs = conn.extra_dejson.copy()
        # Connection attributes can be parsed to empty strings by urllib.unparse
        if conn.port:
            connection_kwargs.update(port=conn.port)
        if conn.login:
            connection_kwargs.update(user=conn.login)
        if conn.password:
            connection_kwargs.update(password=conn.password)
        if self._database is not None:
            connection_kwargs.update(database=self._database)
        elif conn.schema:
            connection_kwargs.update(database=conn.schema)
        return clickhouse_driver.Client(conn.host or 'localhost', **connection_kwargs)


class ClickHouseHook(BaseClickHouseHook):
    def execute(
            self,
            sql: t.Union[str, t.Iterable[str]],
            # arguments of clickhouse_driver.Client.execute
            params: t.Optional[ExecuteParamsT] = None,
            with_column_types: bool = False,
            external_tables: t.Optional[t.List[dict]] = None,
            query_id: t.Optional[str] = None,
            settings: t.Dict[str, t.Any] = None,
            types_check: bool = False,
            columnar: bool = False,
    ) -> ExecuteReturnT:
        """
        Passes arguments to ``clickhouse_driver.Client.execute``.

        Allows execution of multiple queries, if ``sql`` argument is an
        iterable. Returns results of the last query's execution.
        """
        if isinstance(sql, str):
            sql = (sql,)
        with _disconnecting(self.get_conn()) as conn:
            last_result = None
            for query in sql:
                _log_query(self.log, query, params)
                last_result = conn.execute(
                    query,
                    params=params,
                    with_column_types=with_column_types,
                    external_tables=external_tables,
                    query_id=query_id,
                    settings=settings,
                    types_check=types_check,
                    columnar=columnar,
                )
        return last_result


def _log_query(logger: logging.Logger, query: str, params: ExecuteParamsT) -> None:
    params_formatted = f' with {_format_params(params)}' if params else ''
    logger.info('%s%s', query, params_formatted)


def _format_params(parameters: ExecuteParamsT, limit: int = 10) -> str:
    if isinstance(parameters, t.Generator) or len(parameters) <= limit:
        return str(parameters)
    if isinstance(parameters, dict):
        head = dict(islice(parameters.items(), limit))
    else:
        head = parameters[:limit]
    head_str = str(head)
    closing_paren = head_str[-1]
    return f'{head_str[:-1]} … and {len(parameters) - limit} ' \
        f'more parameters{closing_paren}'


_DisconnectingT = t.TypeVar('_DisconnectingT')


@contextlib.contextmanager
def _disconnecting(thing: _DisconnectingT) -> t.ContextManager[_DisconnectingT]:
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

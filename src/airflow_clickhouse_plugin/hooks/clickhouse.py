import contextlib
import typing as t
from itertools import islice

import clickhouse_driver
from airflow.hooks.base import BaseHook
from airflow.models import Connection

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


class ExternalTable(t.TypedDict):
    name: str
    structure: t.List[t.Tuple[str, str]]
    data: t.List[t.Dict[str, t.Any]]


default_conn_name = 'clickhouse_default'


class ClickHouseHook(BaseHook):
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
        return clickhouse_driver.Client(**conn_to_kwargs(conn, self._database))

    def execute(
            self,
            sql: t.Union[str, t.Iterable[str]],
            # arguments of clickhouse_driver.Client.execute
            params: t.Optional[ExecuteParamsT] = None,
            with_column_types: bool = False,
            external_tables: t.Optional[t.List[ExternalTable]] = None,
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
                self.log.info(_format_query_log(query, params))
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


def conn_to_kwargs(conn: Connection, database: t.Optional[str]) -> t.Dict[str, t.Any]:
    """ Translate Airflow Connection to clickhouse-driver Connection kwargs. """
    connection_kwargs = conn.extra_dejson.copy()
    # Connection attributes can be parsed to empty strings by urllib.unparse
    connection_kwargs['host'] = conn.host or 'localhost'
    if conn.port:
        connection_kwargs.update(port=conn.port)
    if conn.login:
        connection_kwargs.update(user=conn.login)
    if conn.password:
        connection_kwargs.update(password=conn.password)
    if database is not None:
        connection_kwargs.update(database=database)
    elif conn.schema:
        connection_kwargs.update(database=conn.schema)
    return connection_kwargs


def _format_query_log(query: str, params: ExecuteParamsT) -> str:
    return ''.join((query, f' with {_format_params(params)}' if params else ''))


def _format_params(params: ExecuteParamsT, limit: int = 10) -> str:
    if isinstance(params, t.Generator) or len(params) <= limit:
        return str(params)
    if isinstance(params, dict):
        head = dict(islice(params.items(), limit))
    else:
        head = params[:limit]
    head_str = str(head)
    closing_paren = head_str[-1]
    return f'{head_str[:-1]} … and {len(params) - limit} ' \
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

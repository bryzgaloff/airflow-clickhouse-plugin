from contextlib import AbstractContextManager

from airflow.hooks.base_hook import BaseHook
from clickhouse_driver import Client
from clickhouse_driver.result import IterQueryResult


class ClickHouseHook(BaseHook):
    conn_name_attr = 'clickhouse_conn_id'
    default_conn_name = 'clickhouse_default'

    def __init__(self, *args, **kwargs):
        if len(args) == 1:
            setattr(self, self.conn_name_attr, args[0])
        elif self.conn_name_attr not in kwargs:
            setattr(self, self.conn_name_attr, self.default_conn_name)
        else:
            setattr(self, self.conn_name_attr, kwargs[self.conn_name_attr])
        self.database = kwargs.pop("database", None)

    def get_conn(self) -> Client:
        conn = self.get_connection(getattr(self, self.conn_name_attr))

        conn_config = {
            "user": conn.login or 'default',
            "password": conn.password or '',
            "host": conn.host or 'localhost',
            "database": self.database or conn.schema or 'default'
        }

        if not conn.port:
            conn_config["port"] = 9000
        else:
            conn_config["port"] = int(conn.port)

        connection = ClickHouseHook.create_connection(conn_config)
        return connection

    @classmethod
    def create_connection(cls, config: dict) -> Client:
        return Client(**config)

    def get_records(self, sql, parameters=None):
        with disconnecting(self.get_conn()) as conn:
            return conn.execute(sql, params=parameters)

    def get_records_iter(self, sql, parameters=None) -> IterQueryResult:
        with disconnecting(self.get_conn()) as conn:
            return conn.execute_iter(sql, params=parameters)

    def get_first(self, sql, parameters=None):
        return next(self.get_records_iter(sql, parameters))

    def get_pandas_df(self, sql, parameters=None):
        raise NotImplementedError()

    def run(self, sql, parameters=None):
        with disconnecting(self.get_conn()) as conn:
            return conn.execute(sql, params=parameters)


class disconnecting(AbstractContextManager):
    """Context to automatically close something at the end of a block.

    Code like this:

        with closing(<module>.open(<arguments>)) as f:
            <block>

    is equivalent to this:

        f = <module>.open(<arguments>)
        try:
            <block>
        finally:
            f.close()

    """
    def __init__(self, thing):
        self.thing = thing
    def __enter__(self):
        return self.thing
    def __exit__(self, *exc_info):
        self.thing.disconnect()
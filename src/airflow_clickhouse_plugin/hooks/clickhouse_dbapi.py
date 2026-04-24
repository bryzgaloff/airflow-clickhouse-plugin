import typing as t

import clickhouse_driver
from airflow.providers.common.sql.hooks.sql import DbApiHook

from airflow_clickhouse_plugin.hooks.clickhouse import conn_to_kwargs, \
    default_conn_name


class ClickHouseDbApiHook(DbApiHook):
    conn_name_attr = 'clickhouse_conn_id'
    clickhouse_conn_id: str  # set by DbApiHook.__init__
    default_conn_name = default_conn_name

    def __init__(self, *args, schema: t.Optional[str] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self._schema = schema

    def get_conn(self) -> clickhouse_driver.dbapi.Connection:
        airflow_conn = self.get_connection(self.clickhouse_conn_id)
        return clickhouse_driver.dbapi \
            .connect(**conn_to_kwargs(airflow_conn, self._schema))

    def get_openlineage_database_info(self, connection):
        from airflow.providers.openlineage.sqlparser import DatabaseInfo

        authority = self.get_openlineage_authority_part(
            connection,
            default_port=9000,
        )

        return DatabaseInfo(scheme="clickhouse", authority=authority)

    def get_openlineage_default_schema(self):
        return self._schema or "default"

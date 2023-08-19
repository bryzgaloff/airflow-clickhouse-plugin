import clickhouse_driver
from airflow.providers.common.sql.hooks.sql import DbApiHook

from airflow_clickhouse_plugin.hooks.clickhouse import BaseClickHouseHook


class ClickHouseDbApiHook(BaseClickHouseHook, DbApiHook):
    conn_name_attr = 'clickhouse_conn_id'
    clickhouse_conn_id: str  # set by DbApiHook.__init__
    connector = clickhouse_driver.dbapi

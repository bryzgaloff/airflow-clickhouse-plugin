import typing as t

from airflow.providers.common.sql.operators import sql

from airflow_clickhouse_plugin.hooks.clickhouse import default_conn_name
from airflow_clickhouse_plugin.hooks.clickhouse_dbapi import \
    ClickHouseDbApiHook


class ClickHouseDbApiHookMixin(object):
    # these attributes are defined in both BaseSQLOperator and SqlSensor
    conn_id: str
    hook_params: t.Optional[dict]

    def _get_clickhouse_db_api_hook(self, **extra_hook_params) -> ClickHouseDbApiHook:
        hook_params = {} if self.hook_params is None else self.hook_params.copy()
        hook_params.update(extra_hook_params)
        return ClickHouseDbApiHook(
            clickhouse_conn_id=self.conn_id or default_conn_name,
            **hook_params,
        )


class ClickHouseBaseDbApiOperator(ClickHouseDbApiHookMixin, sql.BaseSQLOperator):
    def get_db_hook(self) -> ClickHouseDbApiHook:
        return self._get_clickhouse_db_api_hook(schema=self.database)


class ClickHouseSQLExecuteQueryOperator(
    ClickHouseBaseDbApiOperator,
    sql.SQLExecuteQueryOperator,
):
    pass


class ClickHouseSQLColumnCheckOperator(
    ClickHouseBaseDbApiOperator,
    sql.SQLColumnCheckOperator,
):
    pass


class ClickHouseSQLTableCheckOperator(
    ClickHouseBaseDbApiOperator,
    sql.SQLTableCheckOperator,
):
    pass


class ClickHouseSQLCheckOperator(
    ClickHouseBaseDbApiOperator,
    sql.SQLCheckOperator,
):
    pass


class ClickHouseSQLValueCheckOperator(
    ClickHouseBaseDbApiOperator,
    sql.SQLValueCheckOperator,
):
    pass


class ClickHouseSQLIntervalCheckOperator(
    ClickHouseBaseDbApiOperator,
    sql.SQLIntervalCheckOperator,
):
    pass


class ClickHouseSQLThresholdCheckOperator(
    ClickHouseBaseDbApiOperator,
    sql.SQLThresholdCheckOperator,
):
    pass


class ClickHouseBranchSQLOperator(
    ClickHouseBaseDbApiOperator,
    sql.BranchSQLOperator,
):
    pass

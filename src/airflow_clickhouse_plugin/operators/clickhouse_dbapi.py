import typing as t

from airflow.providers.common.sql.operators import sql

from airflow_clickhouse_plugin.hooks.clickhouse_dbapi import \
    ClickHouseDbApiHook


class ClickHouseDbApiHookMixin(object):
    # these attributes are defined in both BaseSQLOperator and SqlSensor
    conn_id: str
    hook_params: t.Optional[dict]

    def _get_clickhouse_db_api_hook(self, **extra_hook_params) -> ClickHouseDbApiHook:
        hook_kwargs = {}
        if self.conn_id is not None:
            hook_kwargs['clickhouse_conn_id'] = self.conn_id
        if self.hook_params is not None:
            hook_kwargs.update(self.hook_params)
        hook_kwargs.update(extra_hook_params)
        return ClickHouseDbApiHook(**hook_kwargs)


class ClickHouseBaseDbApiOperator(ClickHouseDbApiHookMixin, sql.BaseSQLOperator):
    def get_db_hook(self) -> ClickHouseDbApiHook:
        return self._get_clickhouse_db_api_hook(schema=self.database)


class ClickHouseSQLExecuteQueryOperator(
    sql.SQLExecuteQueryOperator,
    ClickHouseBaseDbApiOperator,
):
    pass


class ClickHouseSQLColumnCheckOperator(
    sql.SQLColumnCheckOperator,
    ClickHouseBaseDbApiOperator,
):
    pass


class ClickHouseSQLTableCheckOperator(
    sql.SQLTableCheckOperator,
    ClickHouseBaseDbApiOperator,
):
    pass


class ClickHouseSQLCheckOperator(
    sql.SQLCheckOperator,
    ClickHouseBaseDbApiOperator,
):
    pass


class ClickHouseSQLValueCheckOperator(
    sql.SQLValueCheckOperator,
    ClickHouseBaseDbApiOperator,
):
    pass


class ClickHouseSQLIntervalCheckOperator(
    sql.SQLIntervalCheckOperator,
    ClickHouseBaseDbApiOperator,
):
    pass


class ClickHouseSQLThresholdCheckOperator(
    sql.SQLThresholdCheckOperator,
    ClickHouseBaseDbApiOperator,
):
    pass


class ClickHouseBranchSQLOperator(
    sql.BranchSQLOperator,
    ClickHouseBaseDbApiOperator,
):
    pass

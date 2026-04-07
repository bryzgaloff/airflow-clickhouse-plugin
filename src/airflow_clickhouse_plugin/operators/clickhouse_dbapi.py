import typing as t

from airflow.providers.common.sql.operators import sql

from airflow_clickhouse_plugin.hooks.clickhouse_dbapi import ClickHouseDbApiHook


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
    ClickHouseDbApiHookMixin,
    sql.SQLExecuteQueryOperator,
):
    def get_db_hook(self) -> ClickHouseDbApiHook:
        return self._get_clickhouse_db_api_hook(schema=self.database)


class ClickHouseSQLColumnCheckOperator(
    ClickHouseDbApiHookMixin,
    sql.SQLColumnCheckOperator,
):
    def get_db_hook(self) -> ClickHouseDbApiHook:
        return self._get_clickhouse_db_api_hook(schema=self.database)


class ClickHouseSQLTableCheckOperator(
    ClickHouseDbApiHookMixin,
    sql.SQLTableCheckOperator,
):
    def get_db_hook(self) -> ClickHouseDbApiHook:
        return self._get_clickhouse_db_api_hook(schema=self.database)


class ClickHouseSQLCheckOperator(
    ClickHouseDbApiHookMixin,
    sql.SQLCheckOperator,
):
    def get_db_hook(self) -> ClickHouseDbApiHook:
        return self._get_clickhouse_db_api_hook(schema=self.database)


class ClickHouseSQLValueCheckOperator(
    ClickHouseDbApiHookMixin,
    sql.SQLValueCheckOperator,
):
    def get_db_hook(self) -> ClickHouseDbApiHook:
        return self._get_clickhouse_db_api_hook(schema=self.database)


class ClickHouseSQLIntervalCheckOperator(
    ClickHouseDbApiHookMixin,
    sql.SQLIntervalCheckOperator,
):
    def get_db_hook(self) -> ClickHouseDbApiHook:
        return self._get_clickhouse_db_api_hook(schema=self.database)


class ClickHouseSQLThresholdCheckOperator(
    ClickHouseDbApiHookMixin,
    sql.SQLThresholdCheckOperator,
):
    def get_db_hook(self) -> ClickHouseDbApiHook:
        return self._get_clickhouse_db_api_hook(schema=self.database)


class ClickHouseBranchSQLOperator(
    ClickHouseDbApiHookMixin,
    sql.BranchSQLOperator,
):
    def get_db_hook(self) -> ClickHouseDbApiHook:
        return self._get_clickhouse_db_api_hook(schema=self.database)

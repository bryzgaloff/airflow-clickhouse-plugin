from airflow.providers.common.sql.operators import sql

from airflow_clickhouse_plugin.hooks.clickhouse_dbapi import \
    ClickHouseDbApiHook


class ClickHouseBaseDbApiOperator(sql.BaseSQLOperator):
    def get_db_hook(self) -> ClickHouseDbApiHook:
        return ClickHouseDbApiHook(
            clickhouse_conn_id=self.conn_id,
            database=self.database,
            **self.hook_params,
        )


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

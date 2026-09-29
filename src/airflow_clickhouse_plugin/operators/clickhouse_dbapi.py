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


class ClickHouseBaseDbApiOperator(
    ClickHouseDbApiHookMixin,
    sql.BaseSQLOperator,
):
    # Explicitly define __init__ to prevent Airflow's BaseOperatorMeta from breaking MRO.
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_db_hook(self) -> ClickHouseDbApiHook:
        return self._get_clickhouse_db_api_hook(schema=self.database)


class ClickHouseSQLExecuteQueryOperator(
    ClickHouseBaseDbApiOperator,
    sql.SQLExecuteQueryOperator,
):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


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
    def __init__(
        self,
        *args,
        table: str,
        metrics_thresholds: t.Dict[str, int],
        date_filter_column: t.Optional[str] = "ds",
        days_back: int = -7,
        ratio_formula: t.Optional[str] = "max_over_min",
        ignore_zero: bool = True,
        conn_id: t.Optional[str] = None,
        database: t.Optional[str] = None,
        **kwargs,
    ):
        super().__init__(
            *args,
            table=table,
            metrics_thresholds=metrics_thresholds,
            date_filter_column=date_filter_column,
            days_back=days_back,
            ratio_formula=ratio_formula,
            ignore_zero=ignore_zero,
            conn_id=conn_id,
            database=database,
            **kwargs,
        )
        # parent class constructor internals can vary between Airflow versions,
        # so we rely purely on input args
        metrics_sorted = sorted(metrics_thresholds.keys())
        days_back = -abs(days_back)
        sqlexp = ", ".join(metrics_sorted)

        sqlt = f"SELECT {sqlexp} FROM {table} WHERE {date_filter_column}="
        self.sql1 = sqlt + "toDate('{{ ds }}')"
        self.sql2 = sqlt + "toDate('{{ macros.ds_add(ds, " + str(days_back) + ") }}')"
        self.sql: t.List[str] = [self.sql1, self.sql2]


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

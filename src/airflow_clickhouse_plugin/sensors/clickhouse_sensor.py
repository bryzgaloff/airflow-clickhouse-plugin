import typing as t

from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator

from airflow_clickhouse_plugin.hooks.clickhouse_hook import ExecuteReturnT
from airflow_clickhouse_plugin.operators.clickhouse_operator import \
    BaseClickHouseOperator


class ClickHouseSensor(BaseClickHouseOperator, BaseSensorOperator):
    """ Pokes using clickhouse_driver.Client.execute. """

    def __init__(
            self,
            *args,
            is_failure: t.Callable[[ExecuteReturnT], bool] = None,
            is_success: t.Callable[[ExecuteReturnT], bool] = None,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._is_failure = is_failure
        self._is_success = bool if is_success is None else is_success

    def poke(self, context: dict) -> bool:
        result = self._hook_execute()
        if self._is_failure is not None and self._is_failure(result):
            raise AirflowException('is_failure callable returned True')
        return self._is_success(result)

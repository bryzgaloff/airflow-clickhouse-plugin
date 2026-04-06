import unittest
from datetime import datetime

from airflow import DAG

from airflow_clickhouse_plugin.operators.clickhouse import (
    ClickHouseOperator,
)


class ClickHouseOperatorTestCase(unittest.TestCase):
    def test_execute(self):
        with DAG('test_clickhouse', start_date=datetime(2021, 1, 1)):
            task = ClickHouseOperator(
                task_id='test1',
                sql='SELECT 1',
            )
            task.execute(context={})


if __name__ == '__main__':
    unittest.main()

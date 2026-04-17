import unittest
import unittest.mock
from datetime import datetime, timedelta, timezone

from airflow import DAG

from airflow_clickhouse_plugin.operators.clickhouse_dbapi import (
    ClickHouseSQLCheckOperator,
    ClickHouseSQLColumnCheckOperator,
    ClickHouseSQLExecuteQueryOperator,
    ClickHouseSQLTableCheckOperator,
    ClickHouseSQLThresholdCheckOperator,
    ClickHouseSQLValueCheckOperator,
)


class ClickHouseSQLExecuteQueryOperatorTestCase(unittest.TestCase):
    def test_execute(self):
        with DAG('test_clickhouse_dbapi', start_date=datetime(2021, 1, 1)) as dag:
            task = ClickHouseSQLExecuteQueryOperator(
                task_id='test-execute-query-operator',
                conn_id=None,
                sql='SELECT 1',
            )
            result = task.execute(context={})
            self.assertEqual(result, [(1, )])


class ClickHouseSQLCheckOperatorTestCase(unittest.TestCase):
    def test_execute(self):
        with DAG('test_clickhouse_dbapi', start_date=datetime(2021, 1, 1)):
            task = ClickHouseSQLCheckOperator(
                task_id='test-check-operator',
                conn_id=None,
                database='system',
                sql='SELECT 1',
            )
            # no exception should be raised
            task.execute(context={})


class ClickHouseSQLTableCheckOperatorTestCase(unittest.TestCase):
    def test_execute(self):
        with DAG('test_clickhouse_dbapi', start_date=datetime(2021, 1, 1)):
            task = ClickHouseSQLTableCheckOperator(
                task_id='test-table-check-operator',
                conn_id=None,
                database='system',
                table='one',
                checks={
                    'row_count_check': {'check_statement': 'COUNT(*) = 1'},
                    'column_value_check': {'check_statement': 'dummy = 0'},
                },
            )
            # no exception should be raised
            task.execute(context={})


class ClickHouseSQLColumnCheckOperatorTestCase(unittest.TestCase):
    def test_execute(self):
        with DAG('test_clickhouse_dbapi', start_date=datetime(2021, 1, 1)):
            task = ClickHouseSQLColumnCheckOperator(
                task_id='test-column-check-operator',
                conn_id=None,
                database='system',
                table='one',
                column_mapping={
                    'dummy': {
                        'null_check': {
                            'equal_to': 0,
                            'tolerance': 0,
                        },
                        'distinct_check': {
                            'equal_to': 1,
                        },
                        'min': {
                            'equal_to': 0,
                        },
                        'max': {
                            'equal_to': 0,
                        },
                    }
                },
            )
            # no exception should be raised
            task.execute(context={})


class ClickHouseSQLValueCheckOperatorTestCase(unittest.TestCase):
    def test_execute(self):
        with DAG('test_clickhouse_dbapi', start_date=datetime(2021, 1, 1)):
            task = ClickHouseSQLValueCheckOperator(
                task_id='test-value-check-operator',
                conn_id=None,
                sql='SELECT 1',
                pass_value=1,
                tolerance=0,
            )
            # no exception should be raised
            task.execute(context={})


class ClickHouseSQLThresholdCheckOperatorTestCase(unittest.TestCase):
    def test_execute(self):
        with DAG('test_clickhouse_dbapi', start_date=datetime(2021, 1, 1)):
            task = ClickHouseSQLThresholdCheckOperator(
                task_id='test-threshold-check-operator',
                conn_id=None,
                sql='SELECT 1',
                min_threshold=1,
                max_threshold=2,
            )
            # no exception should be raised
            task.execute(context={})


if __name__ == '__main__':
    unittest.main()

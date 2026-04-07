import unittest
import unittest.mock
from datetime import datetime

from airflow import DAG
from airflow.models.dagrun import DagRun
from airflow.operators.python import PythonOperator

from airflow_clickhouse_plugin.operators.clickhouse_dbapi import (
    ClickHouseBranchSQLOperator,
    ClickHouseSQLCheckOperator,
    ClickHouseSQLColumnCheckOperator,
    ClickHouseSQLExecuteQueryOperator,
    ClickHouseSQLTableCheckOperator,
    ClickHouseSQLThresholdCheckOperator,
    ClickHouseSQLValueCheckOperator,
)


class ClickHouseSQLExecuteQueryOperatorTestCase(unittest.TestCase):
    def test_execute(self):
        with DAG('test_clickhouse_dbapi', start_date=datetime(2021, 1, 1)):
            task = ClickHouseSQLExecuteQueryOperator(
                task_id='test-execute-query-operator',
                conn_id=None,
                sql='SELECT 1',
            )
            task.execute(context={})


class ClickHouseSQLCheckOperatorTestCase(unittest.TestCase):
    def test_execute(self):
        with DAG('test_clickhouse_dbapi', start_date=datetime(2021, 1, 1)):
            task = ClickHouseSQLCheckOperator(
                task_id='test-check-operator',
                conn_id=None,
                database='system',
                sql='SELECT 1',
            )
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
            task.execute(context={})


class ClickHouseBranchSQLOperatorTestCase(unittest.TestCase):
    def test_execute(self):
        with DAG('test_clickhouse_dbapi', start_date=datetime(2021, 1, 1)):
            branch_task = ClickHouseBranchSQLOperator(
                task_id='test-branch-check-operator',
                conn_id=None,
                sql='SELECT 1',
                follow_task_ids_if_true=['true_task'],
                follow_task_ids_if_false=['false_task'],
            )
            PythonOperator(
                task_id='true_task',
                python_callable=lambda: 1,
            )
            PythonOperator(
                task_id='false_task',
                python_callable=lambda: 0,
            )

            task_instance = unittest.mock.Mock()
            task_instance.task = branch_task
            task_instance.get_dagrun.return_value = unittest.mock.Mock(spec=DagRun)
            branch_task.execute(context={'ti': task_instance})


if __name__ == '__main__':
    unittest.main()

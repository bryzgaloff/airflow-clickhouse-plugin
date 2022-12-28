import datetime
import os
import unittest
from tempfile import NamedTemporaryFile
from unittest import mock

from airflow.models import DAG

from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator

DEFAULT_DATE = datetime.datetime.now()


class ClickHouseOperatorTestCase(unittest.TestCase):
    @mock.patch(
        'airflow_clickhouse_plugin'
        '.operators.clickhouse_operator.ClickHouseHook',
    )
    def test(self, clickhouse_hook_mock: mock.MagicMock):
        sql = object()
        clickhouse_conn_id = object()
        parameters = object()
        database = object()
        op = ClickHouseOperator(
            task_id='_', sql=sql, clickhouse_conn_id=clickhouse_conn_id,
            parameters=parameters, database=database,
        )
        op.execute(context=dict())
        clickhouse_hook_mock.assert_called_once_with(
            clickhouse_conn_id=clickhouse_conn_id,
            database=database,
        )
        clickhouse_hook_mock().run.assert_called_once_with(sql, parameters)

    @mock.patch(
        'airflow_clickhouse_plugin'
        '.operators.clickhouse_operator.ClickHouseHook',
    )
    def test_defaults(self, clickhouse_hook_mock: mock.MagicMock):
        sql = 'SELECT 1'
        op = ClickHouseOperator(task_id='_', sql=sql)
        op.execute(context=dict())
        clickhouse_hook_mock.assert_called_once_with(
            clickhouse_conn_id=ClickHouseHook.default_conn_name,
            database=None,
        )
        clickhouse_hook_mock().run.assert_called_once_with(sql, None)

    def test_template_fields_overrides(self):
        assert ClickHouseOperator.template_fields == ('_sql',)

    def test_resolve_template_files_value(self):
        with NamedTemporaryFile(suffix='.sql') as f:
            f.write(b'{{ ds }}')
            f.flush()
            template_dir = os.path.dirname(f.name)
            template_file = os.path.basename(f.name)

            with DAG('test-dag', start_date=DEFAULT_DATE, template_searchpath=template_dir):
                task = ClickHouseOperator(task_id='test_task', sql='SELECT 1')

            task._sql = template_file
            task.resolve_template_files()

        assert task._sql == '{{ ds }}'

    def test_resolve_template_files_list(self):
        with NamedTemporaryFile(suffix='.sql') as f:
            f.write(b'{{ ds }}')
            f.flush()
            template_dir = os.path.dirname(f.name)
            template_file = os.path.basename(f.name)

            with DAG('test-dag', start_date=DEFAULT_DATE, template_searchpath=template_dir):
                task = ClickHouseOperator(task_id='test_task', sql='SELECT 1')

            task._sql = [template_file, 'some_string']
            task.resolve_template_files()

        assert task._sql == ['{{ ds }}', 'some_string']


if __name__ == '__main__':
    unittest.main()

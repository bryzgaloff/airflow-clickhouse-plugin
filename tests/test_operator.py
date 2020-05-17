import unittest
from unittest import mock

from airflow_clickhouse_plugin import ClickHouseOperator
from tests.util import LocalClickHouseHook


class ClickHouseOperatorTestCase(unittest.TestCase):
    def test_operator(self):
        value = 1
        operator = ClickHouseOperator(
            task_id='test',
            sql=('SELECT %(value)s', 'SELECT %(value)s'),
            parameters={'value': value},
        )
        last_result = operator.execute(context=dict())
        self.assertListEqual([(value,)], last_result)

    @classmethod
    def setUpClass(cls):
        cls._hook_patch = mock.patch(
            'airflow_clickhouse_plugin.hooks.clickhouse_hook.ClickHouseHook',
            new=LocalClickHouseHook,
        )
        cls._hook_patch.__enter__()
    _hook_patch: mock._patch

    @classmethod
    def tearDownClass(cls):
        cls._hook_patch.__exit__(None, None, None)


if __name__ == '__main__':
    unittest.main()

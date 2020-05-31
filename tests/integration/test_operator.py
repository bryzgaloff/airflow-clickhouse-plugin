import unittest

from airflow_clickhouse_plugin import ClickHouseOperator


class BasicTestCase(unittest.TestCase):
    def test_operator(self):
        value = 1
        operator = ClickHouseOperator(
            task_id='test',
            sql=('SELECT %(val0)s', 'SELECT %(val1)s'),
            parameters={'val0': value + 1, 'val1': value},
        )
        last_result = operator.execute(context=dict())
        self.assertListEqual([(value,)], last_result)


if __name__ == '__main__':
    unittest.main()

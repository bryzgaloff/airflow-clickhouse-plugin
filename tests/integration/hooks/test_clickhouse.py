import unittest

from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook


class ClickHouseHookTestCase(unittest.TestCase):
    def test_execute(self):
        return_value = ClickHouseHook().execute(
            'SELECT sum(value) * %(multiplier)s AS output FROM ext',
            params={'multiplier': 2},
            with_column_types=True,
            external_tables=[{
                'name': 'ext',
                'structure': [('value', 'Int32')],
                'data': [{'value': 1}, {'value': 2}],
            }],
            query_id='airflow-clickhouse-plugin-test',
            types_check=True,
            columnar=True,
        )
        self.assertTupleEqual(([(6,)], [('output', 'Int64')]), return_value)


if __name__ == '__main__':
    unittest.main()

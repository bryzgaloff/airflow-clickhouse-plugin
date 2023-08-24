import unittest

from airflow_clickhouse_plugin.hooks.clickhouse_dbapi import ClickHouseDbApiHook


class ClickHouseDbApiHookTestCase(unittest.TestCase):
    def test_get_records(self):
        records = ClickHouseDbApiHook().get_records(
            '''
                SELECT number * %(multiplier)s AS output
                FROM system.numbers
                LIMIT 1 OFFSET 1
            ''',
            parameters={'multiplier': 2},
        )
        self.assertListEqual([(2,)], records)


if __name__ == '__main__':
    unittest.main()

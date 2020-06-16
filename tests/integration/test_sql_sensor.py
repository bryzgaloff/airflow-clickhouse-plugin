import unittest

from airflow_clickhouse_plugin import ClickHouseSqlSensor


class BasicTestCase(unittest.TestCase):
    def test_sql_sensor(self):
        self.assertFalse(
            ClickHouseSqlSensor(task_id='_', sql='SELECT 0').poke(None),
        )
        self.assertTrue(
            ClickHouseSqlSensor(task_id='_', sql='SELECT 1').poke(None),
        )
        self.assertTrue(
            ClickHouseSqlSensor(task_id='_', sql='SELECT NULL').poke(None),
        )
        self.assertFalse(
            ClickHouseSqlSensor(task_id='_', sql='SELECT 1 WHERE 0').poke(None),
        )


if __name__ == '__main__':
    unittest.main()

import unittest
from unittest import mock

from airflow.exceptions import AirflowException
from airflow_clickhouse_plugin import ClickHouseSqlSensor


class ClickHouseSqlSensorTestCase(unittest.TestCase):
    @mock.patch(
        'airflow_clickhouse_plugin.sensors.clickhouse_sql_sensor.ClickHouseHook'
    )
    def test_clickhouse_sql_sensor_poke(self, mock_hook):
        op = ClickHouseSqlSensor(task_id='sql_sensor_check', sql='SELECT 1',)

        mock_get_records = mock_hook().get_records

        mock_get_records.return_value = []
        self.assertFalse(op.poke(None))

        mock_get_records.return_value = [[None]]
        self.assertFalse(op.poke(None))

        mock_get_records.return_value = [['None']]
        self.assertTrue(op.poke(None))

        mock_get_records.return_value = [[0.0]]
        self.assertFalse(op.poke(None))

        mock_get_records.return_value = [[0]]
        self.assertFalse(op.poke(None))

        mock_get_records.return_value = [['0']]
        self.assertTrue(op.poke(None))

        mock_get_records.return_value = [['1']]
        self.assertTrue(op.poke(None))

    @mock.patch(
        'airflow_clickhouse_plugin.sensors.clickhouse_sql_sensor.ClickHouseHook'
    )
    def test_clickhouse_sql_sensor_poke_fail_on_empty(self, mock_hook):
        op = ClickHouseSqlSensor(
            task_id='sql_sensor_check', sql='SELECT 1', fail_on_empty=True,
        )

        mock_get_records = mock_hook().get_records

        mock_get_records.return_value = []
        self.assertRaises(AirflowException, op.poke, None)

    @mock.patch(
        'airflow_clickhouse_plugin.sensors.clickhouse_sql_sensor.ClickHouseHook'
    )
    def test_clickhouse_sql_sensor_poke_success(self, mock_hook):
        op = ClickHouseSqlSensor(
            task_id='sql_sensor_check', sql='SELECT 1', success=lambda x: x in [1],
        )

        mock_get_records = mock_hook().get_records

        mock_get_records.return_value = []
        self.assertFalse(op.poke(None))

        mock_get_records.return_value = [[1]]
        self.assertTrue(op.poke(None))

        mock_get_records.return_value = [['1']]
        self.assertFalse(op.poke(None))

    @mock.patch(
        'airflow_clickhouse_plugin.sensors.clickhouse_sql_sensor.ClickHouseHook'
    )
    def test_clickhouse_sql_sensor_poke_failure(self, mock_hook):
        op = ClickHouseSqlSensor(
            task_id='sql_sensor_check', sql='SELECT 1', failure=lambda x: x in [1],
        )

        mock_get_records = mock_hook().get_records

        mock_get_records.return_value = []
        self.assertFalse(op.poke(None))

        mock_get_records.return_value = [[1]]
        self.assertRaises(AirflowException, op.poke, None)

    @mock.patch(
        'airflow_clickhouse_plugin.sensors.clickhouse_sql_sensor.ClickHouseHook'
    )
    def test_clickhouse_sql_sensor_poke_failure_success(self, mock_hook):
        op = ClickHouseSqlSensor(
            task_id='sql_sensor_check',
            sql='SELECT 1',
            failure=lambda x: x in [1],
            success=lambda x: x in [2],
        )

        mock_get_records = mock_hook().get_records

        mock_get_records.return_value = []
        self.assertFalse(op.poke(None))

        mock_get_records.return_value = [[1]]
        self.assertRaises(AirflowException, op.poke, None)

        mock_get_records.return_value = [[2]]
        self.assertTrue(op.poke(None))

    @mock.patch(
        'airflow_clickhouse_plugin.sensors.clickhouse_sql_sensor.ClickHouseHook'
    )
    def test_clickhouse_sql_sensor_poke_failure_success_same(self, mock_hook):
        op = ClickHouseSqlSensor(
            task_id='sql_sensor_check',
            sql='SELECT 1',
            failure=lambda x: x in [1],
            success=lambda x: x in [1],
        )

        mock_get_records = mock_hook().get_records

        mock_get_records.return_value = []
        self.assertFalse(op.poke(None))

        mock_get_records.return_value = [[1]]
        self.assertRaises(AirflowException, op.poke, None)

    @mock.patch(
        'airflow_clickhouse_plugin.sensors.clickhouse_sql_sensor.ClickHouseHook'
    )
    def test_clickhouse_sql_sensor_poke_invalid_failure(self, mock_hook):
        op = ClickHouseSqlSensor(
            task_id='sql_sensor_check', sql='SELECT 1', failure=[1],
        )

        mock_get_records = mock_hook().get_records

        mock_get_records.return_value = [[1]]
        self.assertRaises(AirflowException, op.poke, None)

    @mock.patch(
        'airflow_clickhouse_plugin.sensors.clickhouse_sql_sensor.ClickHouseHook'
    )
    def test_clickhouse_sql_sensor_poke_invalid_success(self, mock_hook):
        op = ClickHouseSqlSensor(
            task_id='sql_sensor_check', sql='SELECT 1', success=[1],
        )

        mock_get_records = mock_hook().get_records

        mock_get_records.return_value = [[1]]
        self.assertRaises(AirflowException, op.poke, None)


if __name__ == '__main__':
    unittest.main()

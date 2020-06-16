import unittest
from unittest import mock

from airflow.exceptions import AirflowException
from airflow.sensors.sql_sensor import SqlSensor

from airflow_clickhouse_plugin import ClickHouseSqlSensor


class ClickHouseSqlSensorTestCase(unittest.TestCase):
    def test_clickhouse_sql_sensor_poke(self):
        op = ClickHouseSqlSensor(task_id='_', sql='',)
        for return_value, expected_result in (
                ([], False),
                ([[None]], True),
                ([['None']], True),
                ([[0.0]], True),
                ([[0]], False),
                ([['0']], False),
                ([['1']], True),
        ):
            with self.subTest(return_value):
                self._get_records_mock.return_value = return_value
                self.assertEqual(expected_result, op.poke(context=None))

    def test_clickhouse_sql_sensor_poke_fail_on_empty(self):
        op = ClickHouseSqlSensor(task_id='_', sql='', fail_on_empty=True)
        self._get_records_mock.return_value = []
        with self.assertRaises(AirflowException):
            op.poke(context=None)

    def test_clickhouse_sql_sensor_poke_success(self):
        op = ClickHouseSqlSensor(task_id='_', sql='', success=[1].__contains__)
        for return_value, expected_result in (
                ([], False),
                ([[1]], True),
                ([['1']], False),
        ):
            with self.subTest(return_value):
                self._get_records_mock.return_value = return_value
                self.assertEqual(expected_result, op.poke(context=None))

    def test_clickhouse_sql_sensor_poke_failure(self):
        op = ClickHouseSqlSensor(task_id='_', sql='', failure=[1].__contains__)

        self._get_records_mock.return_value = []
        with self.subTest(self._get_records_mock.return_value):
            self.assertFalse(op.poke(None))

        self._get_records_mock.return_value = [[1]]
        with self.subTest(self._get_records_mock.return_value):
            with self.assertRaises(AirflowException):
                op.poke(context=None)

    def test_clickhouse_sql_sensor_poke_failure_success(self):
        op = ClickHouseSqlSensor(
            task_id='_', sql='',
            failure=[1].__contains__, success=[2].__contains__,
        )
        for return_value, expected_result in (
                ([], False),
                ([[2]], True),
        ):
            with self.subTest(return_value):
                self._get_records_mock.return_value = return_value
                self.assertEqual(expected_result, op.poke(context=None))
        self._get_records_mock.return_value = [[1]]
        with self.subTest(self._get_records_mock.return_value):
            with self.assertRaises(AirflowException):
                op.poke(context=None)

    def test_clickhouse_sql_sensor_poke_failure_success_same(self):
        fn = [2].__contains__
        op = ClickHouseSqlSensor(task_id='_', sql='', failure=fn, success=fn)

        self._get_records_mock.return_value = []
        with self.subTest(self._get_records_mock.return_value):
            self.assertFalse(op.poke(None))

        self._get_records_mock.return_value = [[2]]
        with self.subTest(self._get_records_mock.return_value):
            with self.assertRaises(AirflowException):
                op.poke(context=None)

    def test_clickhouse_sql_sensor_poke_invalid_failure(self):
        op = ClickHouseSqlSensor(task_id='_', sql='', failure=[1])
        self._get_records_mock.return_value = [[1]]
        with self.assertRaises(AirflowException):
            op.poke(context=None)

    def test_clickhouse_sql_sensor_poke_invalid_success(self):
        op = ClickHouseSqlSensor(task_id='_', sql='', success=[1])
        self._get_records_mock.return_value = [[1]]
        with self.assertRaises(AirflowException):
            op.poke(context=None)

    _get_records_patch: mock._patch
    _get_records_mock: mock.MagicMock

    @classmethod
    def setUpClass(cls):
        cls._get_records_patch = \
            mock.patch('airflow_clickhouse_plugin.ClickHouseHook.get_records')
        cls._get_records_mock = cls._get_records_patch.__enter__()

    @classmethod
    def tearDownClass(cls) -> None:
        cls._get_records_patch.__exit__(None, None, None)


class ClickHouseLegacySqlSensorTestCase(unittest.TestCase):
    @mock.patch(
        'airflow_clickhouse_plugin.ClickHouseSqlSensor'
            '._ClickHouseSqlSensor__poke_clickhouse',
    )
    @mock.patch('airflow.sensors.sql_sensor.SqlSensor.poke')
    @mock.patch('airflow.sensors.sql_sensor.SqlSensor._get_hook', create=True)
    def test_get_hook_defined(
            self,
            _: mock.MagicMock,  # force creation of _get_hook
            sql_sensor_poke_mock: mock.MagicMock,
            clickhouse_sensor_poke_mock: mock.MagicMock,
    ):
        """ Test for case when ``SqlSensor._get_hook`` method is present. """
        op = ClickHouseSqlSensor(task_id='_', sql='')
        context = dict(some_test_context=True)
        op.poke(context)
        sql_sensor_poke_mock.assert_called_once_with(context)
        clickhouse_sensor_poke_mock.assert_not_called()

    @mock.patch(
        'airflow_clickhouse_plugin.ClickHouseSqlSensor'
            '._ClickHouseSqlSensor__poke_clickhouse',
    )
    @mock.patch('airflow.sensors.sql_sensor.SqlSensor.poke')
    @mock.patch('airflow.sensors.sql_sensor.SqlSensor._get_hook', create=True)
    def test_get_hook_missing(
            self,
            sql_sensor_get_hook_mock: mock.MagicMock,
            sql_sensor_poke_mock: mock.MagicMock,
            clickhouse_sensor_poke_mock: mock.MagicMock,
    ):
        """ Test for case when ``SqlSensor._get_hook`` method is present. """
        op = ClickHouseSqlSensor(task_id='_', sql='')
        del SqlSensor._get_hook
        op.poke(context=None)
        sql_sensor_poke_mock.assert_not_called()
        clickhouse_sensor_poke_mock.assert_called_once_with()
        # line below prevents AttributeError on patch exit
        SqlSensor._get_hook = sql_sensor_get_hook_mock


if __name__ == '__main__':
    unittest.main()

import unittest
from unittest import mock
from packaging.version import Version
from airflow.exceptions import AirflowException
from airflow.version import version as airflow_version

from airflow_clickhouse_plugin.sensors.clickhouse_sql_sensor import ClickHouseSqlSensor


class ClickHouseSqlSensorTestCase(unittest.TestCase):
    def test_clickhouse_sql_sensor_poke(self):
        op = ClickHouseSqlSensor(task_id='_', sql='',)
        for return_value, expected_result in (
                ([], False),
                ([[None]], False),
                ([['None']], True),
                ([[0.0]], False),
                ([[0]], False),
                ([['0']], True),
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
        cls._get_records_patch = mock.patch(
            'airflow_clickhouse_plugin.hooks.clickhouse_hook'
            '.ClickHouseHook.get_records',
        )
        cls._get_records_mock = cls._get_records_patch.__enter__()

    @classmethod
    def tearDownClass(cls) -> None:
        cls._get_records_patch.__exit__(None, None, None)


def _get_sql_module_path():
    if Version(airflow_version) < Version('2.4'):
        return 'airflow'
    return 'airflow.providers.common.sql'


class ClickHouseLegacySqlSensorTestCase(unittest.TestCase):

    @mock.patch(f"{_get_sql_module_path()}.sensors.sql.SqlSensor.poke")
    @mock.patch(f"{_get_sql_module_path()}.sensors.sql.SqlSensor._get_hook", create=True)
    def test_get_hook_defined(
            self,
            _: mock.MagicMock,  # force creation of _get_hook
            sql_sensor_poke_mock: mock.MagicMock,
    ):
        """ Test for case when ``SqlSensor._get_hook`` method is present. """
        op = ClickHouseSqlSensor(task_id='_', sql='')
        context = dict(some_test_context=True)
        op.poke(context)
        sql_sensor_poke_mock.assert_called_once_with(context)


if __name__ == '__main__':
    unittest.main()

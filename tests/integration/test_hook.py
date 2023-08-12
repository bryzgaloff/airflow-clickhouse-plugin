import unittest

from clickhouse_driver.errors import ServerException, ErrorCodes

from airflow_clickhouse_plugin.hooks.clickhouse_dbapi_hook import ClickHouseDbApiHook
from tests.integration.util import ClickHouseConnectionEnvVarTestCase


class BasicTestCase(ClickHouseConnectionEnvVarTestCase):
    _hook: ClickHouseDbApiHook

    def setUp(self):
        self._hook = ClickHouseDbApiHook()

    def test_connection_recreated(self):
        temp_table_name = 'test_temp_table'
        result = self._hook.run((
            f'CREATE TEMPORARY TABLE {temp_table_name} (test_field UInt8)',
            f'INSERT INTO {temp_table_name} '
            f'SELECT number FROM system.numbers WHERE number < 5 LIMIT 5',
            f'SELECT SUM(test_field) FROM {temp_table_name}',
        ))
        self.assertListEqual([(10,)], result)
        try:
            # a new connection is created and temp table is absent
            self._hook.run(f'SELECT * FROM {temp_table_name}')
        except ServerException as err:
            self.assertEqual(ErrorCodes.UNKNOWN_TABLE, err.code)
        else:
            raise AssertionError('server did not raise an error')

    def test_get_first(self):
        self.assertTupleEqual((1,), self._hook.get_first('SELECT 1'))
        self.assertIsNone(self._hook.get_first('SELECT 1 WHERE 0'))


class BasicInsertTestCase(ClickHouseConnectionEnvVarTestCase):
    _hook: ClickHouseDbApiHook

    def setUp(self):
        self._hook = ClickHouseDbApiHook()
        self._temp_table_name = f'test_temp_table_for_insert'
        self._hook.run(f'CREATE TABLE {self._temp_table_name} (test_field UInt8) ENGINE MergeTree() ORDER BY test_field')

    def test_simple_insert(self):
        result = self._hook.run(f'INSERT INTO {self._temp_table_name} (test_field) VALUES', [(3,)])
        self.assertEqual(1, result)

        select_result = self._hook.run(f'SELECT test_field FROM {self._temp_table_name} WHERE test_field = 3')
        self.assertListEqual([(3,)], select_result)

    def test_simple_insert_without_types_check(self):
        from clickhouse_driver import errors
        with self.assertRaises(errors.TypeMismatchError) as e:
            self._hook.run(f'INSERT INTO {self._temp_table_name} (test_field) VALUES', [(-1,)])
        self.assertNotIn('Expected UInt8', str(e.exception))

    def test_simple_insert_with_types_check(self):
        from datetime import date
        from clickhouse_driver import errors
        with self.assertRaises(errors.TypeMismatchError) as e:
            self._hook.run(
                f'INSERT INTO {self._temp_table_name} (test_field) VALUES',
                [(date(2012, 10, 25), )],
                types_check=True,
            )
        self.assertIn('Expected UInt8', str(e.exception))

    def tearDown(self) -> None:
        self._hook.run(f'DROP TABLE IF EXISTS {self._temp_table_name}')


try:
    import pandas as pd
except ImportError:
    pass
else:
    class GetAsPandasDfTestCase(ClickHouseConnectionEnvVarTestCase):
        def _test(self, sql: str, expected_df):
            actual_df = ClickHouseDbApiHook().get_pandas_df(sql)
            self.assertListEqual(
                list(actual_df.columns),
                list(expected_df.columns),
            )
            self.assertListEqual(
                actual_df.to_dict('records'),
                expected_df.to_dict('records'),
            )

        def test(self):
            self._test(
                '''
                    SELECT
                        number,
                        concat('result: ', toString(number + number)) AS nSum
                    FROM system.numbers
                    WHERE number < 4
                    LIMIT 3
                ''',
                pd.DataFrame.from_dict(dict(
                    number=(0, 1, 2),
                    nSum=('result: 0', 'result: 2', 'result: 4'),
                ))
            )

        def test_empty_df(self):
            self._test(
                '''
                    SELECT
                        number,
                        concat('result: ', toString(number + number)) AS n_sum
                    FROM (
                        SELECT number
                        FROM system.numbers
                        WHERE number < 4
                        LIMIT 3
                    )
                    WHERE number > 4
                ''',
                pd.DataFrame(columns=['number', 'n_sum']),
            )


if __name__ == '__main__':
    unittest.main()

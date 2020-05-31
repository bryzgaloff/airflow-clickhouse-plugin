from unittest.case import TestCase

from clickhouse_driver.errors import ServerException, ErrorCodes

from tests.integration.util import LocalClickHouseHook


class BasicTestCase(TestCase):
    def test_temp_table(self):
        hook = LocalClickHouseHook()
        temp_table_name = 'test_temp_table'
        result = hook.run((
            f'CREATE TEMPORARY TABLE {temp_table_name} (test_field UInt8)',
            f'INSERT INTO {temp_table_name} '
            f'SELECT number FROM system.numbers WHERE number < 5 LIMIT 5',
            f'SELECT SUM(test_field) FROM {temp_table_name}',
        ))
        self.assertListEqual([(10,)], result)
        try:
            # a new connection is created
            hook.run(f'SELECT * FROM {temp_table_name}')
        except ServerException as err:
            self.assertEqual(ErrorCodes.UNKNOWN_TABLE, err.code)
        else:
            raise AssertionError('server did not raise an error')


class GetAsPandasTestCase(TestCase):
    def test_get_pandas_df(self):
        import pandas as pd

        hook = LocalClickHouseHook()
        for sql, expected in (
            (
                '''
                SELECT
                    number,
                    concat('result: ', toString(number + number)) AS n_sum
                FROM system.numbers
                WHERE number < 4
                LIMIT 3
                ''',
                pd.DataFrame.from_dict({
                    'number': (0, 1, 2),
                    'n_sum': ('result: 0', 'result: 2', 'result: 4'),
                })
            ),
            # empty df
            (
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
                pd.DataFrame(columns=['number', 'n_sum'])
            )
        ):
            df = hook.get_pandas_df(sql)
            self.assertListEqual(list(df.columns), list(expected.columns))
            self.assertListEqual(
                df.to_dict('records'),
                expected.to_dict('records'),
            )

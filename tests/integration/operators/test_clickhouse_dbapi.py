import unittest
from datetime import datetime, timedelta, timezone

from airflow import DAG

from airflow_clickhouse_plugin.operators.clickhouse_dbapi import (
    ClickHouseSQLIntervalCheckOperator,
)


class ClickHouseSQLIntervalCheckOperatorTestCase(unittest.TestCase):
    def test_execute(self):
        with DAG('test_clickhouse_dbapi', start_date=datetime(2021, 1, 1)):
            task = ClickHouseSQLIntervalCheckOperator(
                task_id='test-interval-check-operator',
                conn_id=None,
                database='system',
                table='metric_log',
                date_filter_column='event_time_microseconds',
                ignore_zero=True,
                metrics_thresholds={'COUNT(*)': 0},
            )

            def ds_add(ds, days):
                return ds + timedelta(days=days)

            task.render_template_fields(
                context={
                    'ds': datetime.now(tz=timezone.utc),
                    'macros': {'ds_add': ds_add},
                }
            )
            # no exception should be raised
            task.execute(context={})


if __name__ == '__main__':
    unittest.main()

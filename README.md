# Airflow ClickHouse Plugin

Provides `ClickHouseHook` and `ClickHouseOperator` for [Apache Airflow][airflow] 
    based on [mymarilyn/clickhouse-driver][ch-driver].

# Features

1. SQL queries are templated.
2. Can run multiple SQL queries per single `ClickHouseOperator`.
3. Result of the last query of `ClickHouseOperator` instance is pushed to XCom.
4. Executed queries are logged in a pretty form.
5. Uses effective native ClickHouse TCP protocol thanks to 
    [clickhouse-driver][ch-driver-docs]. Does not support HTTP protocol.
6. Supports extra ClickHouse [connection parameters][ch-driver-connection] such
    as various timeouts, `compression`, `secure`, etc through Airflow
    [Connection.extra][airflow-conn-extra] property.

# Installation

`pip install -U airflow-clickhouse-plugin`

# Usage

See [examples](#examples) below.

## ClickHouseOperator Reference

To import `ClickHouseOperator` use:
    `from airflow.operators.clickhouse_operator import ClickHouseOperator`

Supported kwargs:
* `sql`: templated query (if argument is a single `str`) or queries (if iterable
    of `str`'s).
* `clickhouse_conn_id`: connection id. Connection schema is described
    [below](#clickhouse-connection-schema).
* `parameters`: passed to clickhouse-driver [execute method][ch-driver-execute].
  * If multiple queries are provided via `sql` then the parameters are passed to
      _all_ of them.
  * Parameters are _not_ templated.
* `database`: if present, overrides database defined by connection.
* Other kwargs (including the required `task_id`) are inherited from Airflow 
    [BaseOperator][airflow-base-op].

The result of the _last_ query is pushed to XCom.

## ClickHouseHook Reference

To import `ClickHouseHook` use:
    `from airflow.hooks.clickhouse_hook import ClickHouseHook`

Supported kwargs of constructor (`__init__` method):
* `clickhouse_conn_id`: connection id. Connection schema is described
    [below](#clickhouse-connection-schema).
* `database`: if present, overrides database defined by connection.

Supports all of the methods of the Airflow [BaseHook][airflow-base-hook]
    including:
* `get_records(sql: str, parameters: dict=None)`: returns result of the query
    as a list of tuples. Materializes all the records in memory.
* `get_first(sql: str, parameters: dict=None)`: returns the first row of the
    result. Does not load the whole dataset into memory because of using
    [execute_iter][ch-driver-execute-iter].
* `run(sql, parameters)`: runs a single query (specified argument of type `str`)
    or multiple queries (if iterable of `str`). `parameters` can have any form
    supported by [execute][ch-driver-execute] method of clickhouse-driver.
  * If single query is run then returns its result. If multiple queries are run
      then returns the result of the last of them.
  * If multiple queries are given then `parameters` are passed to _all_ of them.
  * Materializes all the records in memory (uses simple `execute` but not 
      `execute_iter`).
    * To achieve results streaming by `execute_iter` use it directly via
        `hook.get_conn().execute_iter(…)`
        (see [execute_iter reference][ch-driver-execute-iter]).
  * Every `run` call uses a new connection which is closed when finished.
* `get_conn()`: returns the underlying
    [clickhouse_driver.Client][ch-driver-client] instance.

## ClickHouse Connection schema

[clickhouse_driver.Client][ch-driver-client] is initiated with attributes stored
    in Airflow [Connection attributes][airflow-connection-attrs]. The mapping of
    the attributes is listed below:
  
| Airflow Connection attribute | `Client.__init__` argument |
| --- | --- |
| `host` | `host` |
| `port` | `port` |
| `schema` | `database` |
| `login` | `user` |
| `password` | `password` |

If you pass `database` argument to `ClickHouseOperator` or `ClickHouseHook`
    explicitly then it is passed to the `Client` instead of the `schema`
    attribute of the Airflow connection.

### Extra arguments

You may also pass [additional arguments][ch-driver-connection], such as
    timeouts, `compression`, `secure`, etc through
    [Connection.extra][airflow-conn-extra] attribute. The attribute should
    contain a JSON object which will be [deserialized][airflow-conn-dejson] and
    all of its properties will be passed as-is to the `Client`.

For example, if Airflow connection contains `extra={"secure":true}` then
    the `Client.__init__` will receive `secure=True` keyword argument in
    addition to other non-empty connection attributes.

### Default values

If the Airflow connection attribute is not set then it is not passed to the
    `Client` at all. In that case the default value of the corresponding
    [clickhouse_driver.Connection][ch-driver-connection] argument is used (e.g.
    `user` defaults to `'default'`).

This means that Airflow ClickHouse Plugin does not itself define any default
    values for the ClickHouse connection. You may fully rely on default values
    of the `clickhouse-driver` version you use. The only exception is `host`: if
    the attribute of Airflow connection is not set then `'localhost'` is used.

## Examples

### ClickHouseOperator

```python
from airflow import DAG
from airflow.operators.clickhouse_operator import ClickHouseOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

with DAG(
        dag_id='update_income_aggregate',
        start_date=days_ago(2),
) as dag:
    ClickHouseOperator(
        task_id='update_income_aggregate',
        database='default',
        sql=(
            "INSERT INTO aggregate "
                "SELECT eventDt, sum(price * qty) AS income FROM sales "
                "WHERE eventDt = '{{ ds }}' GROUP BY eventDt",
            "OPTIMIZE TABLE aggregate ON CLUSTER {{ var.value.cluster_name }} "
                "PARTITION toDate('{{ execution_date.format('%Y-%m-01') }}')",
            "SELECT sum(income) FROM aggregate "
                "WHERE eventDt BETWEEN "
                "'{{ execution_date.start_of('month').to_date_string() }}'"
                "AND '{{ execution_date.end_of('month').to_date_string() }}'",
            # result of the last query is pushed to XCom
        ),
        clickhouse_conn_id='clickhouse_test',
    ) >> PythonOperator(
        task_id='print_month_income',
        provide_context=True,
        python_callable=lambda task_instance, **_:
            # pulling XCom value and printing it
            print(task_instance.xcom_pull(task_ids='update_income_aggregate')),
    )
```

### ClickHouseHook

```python
from airflow import DAG
from airflow.hooks.clickhouse_hook import ClickHouseHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


def mysql_to_clickhouse():
    mysql_hook = MySqlHook()
    ch_hook = ClickHouseHook()
    records = mysql_hook.get_records('SELECT * FROM some_mysql_table')
    ch_hook.run('INSERT INTO some_ch_table VALUES', records)


with DAG(
        dag_id='mysql_to_clickhouse',
        start_date=days_ago(2),
) as dag:
    dag >> PythonOperator(
        task_id='mysql_to_clickhouse',
        python_callable=mysql_to_clickhouse,
    )
```

Important note: don't try to insert values using 
    `ch_hook.run('INSERT INTO some_ch_table VALUES (1)')` literal form.
    clickhouse-driver [requires][ch-driver-insert] values for `INSERT` query to
    be provided via `parameters` due to specifics of the native ClickHouse
    protocol.

# Default connection

By default the hook and operator use `connection_id='clickhouse_default'`.

# How to run tests

## Unit tests

From the root project directory: `python -m unittest discover -s tests/unit`

## Integration tests

Integration tests require an access to ClickHouse server. Tests use connection
    URI defined [via environment variable][airflow-conn-env]
    `AIRFLOW_CONN_CLICKHOUSE_DEFAULT` with `clickhouse://localhost` as default.

Run from the project root: `python -m unittest discover -s tests/integration` 

## All tests

From the root project directory: `python -m unittest discover -s tests`

# Contributors

* Anton Bryzgalov, [@bryzgaloff](https://github.com/bryzgaloff)
* Viktor Taranenko, [@viktortnk](https://github.com/viktortnk)
* Danila Ganchar, [@d-ganchar](https://github.com/d-ganchar)
* Mikhail, [@glader](https://github.com/glader)
* Alexander Chashnikov, [@ne1r0n](https://github.com/ne1r0n)


[airflow]: https://airflow.apache.org/
[ch-driver]: https://github.com/mymarilyn/clickhouse-driver
[ch-driver-docs]: https://clickhouse-driver.readthedocs.io/en/latest/
[ch-driver-execute]: https://clickhouse-driver.readthedocs.io/en/latest/quickstart.html#selecting-data
[airflow-base-op]: https://airflow.apache.org/docs/stable/_api/airflow/models/baseoperator/index.html
[airflow-base-hook]: https://airflow.apache.org/docs/stable/_api/airflow/hooks/base_hook/index.html
[ch-driver-execute-iter]: https://clickhouse-driver.readthedocs.io/en/latest/quickstart.html#streaming-results
[ch-driver-insert]: https://clickhouse-driver.readthedocs.io/en/latest/quickstart.html#inserting-data
[ch-driver-client]: https://clickhouse-driver.readthedocs.io/en/latest/api.html#client
[airflow-conn-extra]: https://airflow.apache.org/docs/stable/_api/airflow/models/connection/index.html#airflow.models.connection.Connection.extra
[ch-driver-connection]: https://clickhouse-driver.readthedocs.io/en/latest/api.html#connection
[airflow-connection-attrs]: https://airflow.apache.org/docs/1.10.6/_api/airflow/models/index.html?highlight=connection#airflow.models.Connection
[airflow-conn-dejson]: https://airflow.apache.org/docs/1.10.6/_api/airflow/models/index.html?highlight=connection#airflow.models.Connection.extra_dejson
[airflow-conn-env]: https://airflow.apache.org/docs/stable/howto/connection/index.html#storing-a-connection-in-environment-variables

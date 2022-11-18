# Airflow ClickHouse Plugin

![PyPI - Downloads](https://img.shields.io/pypi/dm/airflow-clickhouse-plugin)
![GitHub Workflow Status](https://img.shields.io/github/workflow/status/bryzgaloff/airflow-clickhouse-plugin/Run%20tests)
![GitHub contributors](https://img.shields.io/github/contributors/bryzgaloff/airflow-clickhouse-plugin?color=blue)

Provides `ClickHouseOperator`, `ClickHouseHook` and `ClickHouseSqlSensor` for
    [Apache Airflow][airflow] based on [mymarilyn/clickhouse-driver][ch-driver].

Top-1% downloads [on PyPI](https://pypi.org/project/airflow-clickhouse-plugin/).

# Features

1. SQL queries are templated.
2. Can run multiple SQL queries per single `ClickHouseOperator`.
3. Result of the last query of `ClickHouseOperator` instance is pushed to XCom.
4. Executed queries are logged in a pretty form.
5. Uses efficient native ClickHouse TCP protocol thanks to 
    [clickhouse-driver][ch-driver-docs]. **Does not support HTTP protocol.**
6. Supports extra ClickHouse [connection parameters][ch-driver-connection] such
    as various timeouts, `compression`, `secure`, etc through Airflow
    [Connection.extra][airflow-conn-extra] property.

# Installation and dependencies

`pip install -U airflow-clickhouse-plugin`

Dependencies: `apache-airflow` and `clickhouse-driver`.

## Python and Airflow versions support

Different versions of the plugin support different combinations of Python and
    Airflow versions. We _primarily_ support **Airflow 2.0+ and Python 3.7+**.
    If you need to use the plugin with older Python-Airflow combinations, pick a
    suitable plugin version:

| airflow-clickhouse-plugin version | Airflow version         | Python version     |
|-----------------------------------|-------------------------|--------------------|
| 0.9.0                             | ~=2.0.0,\>=2.2.0,<2.5.0 | ~=3.7              |
| 0.8.2                             | \>=2.0.0,<2.4.0         | ~=3.7              |
| 0.8.0,0.8.1                       | \>=2.0.0,<2.3.0         | ~=3.6              |
| 0.7.0                             | \>=2.0.0,<2.2.0         | ~=3.6              |
| 0.6.0                             | ~=2.0.1                 | ~=3.6              |
| \>=0.5.4,<0.6.0                   | ~=1.10.6                | \>=2.7 or >=3.5.\* |
| \>=0.5.0,<0.5.4                   | ==1.10.6                | \>=2.7 or >=3.5.\* |

`~=` means compatible release, see [PEP 440][pep-440-compatible-releases] for an
    explanation.

## Note on pandas dependency

Starting from Airflow 2.2 `pandas` is now an [extra requirement][pandas-extra].
    To install `airflow-clickhouse-plugin` with `pandas` support, use
    `pip install airflow-clickhouse-plugin[pandas]`.

**Important**: this works only with `pip` 21+. So to handle `pandas` dependency
    properly  you may need to first upgrade `pip` using `pip install -U pip`.

If you are not able to upgrade `pip` to 21+, install dependency directly using
    `pip install apache-airflow[pandas]==` (specifying current Airflow version).
    Simple one-liner:
    `pip install "apache-airflow[pandas]==$(pip freeze | grep apache-airflow== | cut -d'=' -f3)"`.

# Usage

To see examples [scroll down](#examples). To run them, [create an Airflow connection to ClickHouse](#how-to-create-an-airflow-connection-to-clickhouse).

## ClickHouseOperator Reference

To import `ClickHouseOperator` use:
    `from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator`

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

See [example](#clickhouseoperator-example) below.

## ClickHouseHook Reference

To import `ClickHouseHook` use:
    `from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook`

Supported kwargs of constructor (`__init__` method):
* `clickhouse_conn_id`: connection id. Connection schema is described
    [below](#clickhouse-connection-schema).
* `database`: if present, overrides database defined by connection.

Supports all the methods of the Airflow [BaseHook][airflow-base-hook] including:
* `get_records(sql: str, parameters: dict=None)`: returns result of the query
    as a list of tuples. Materializes all the records in memory.
* `get_first(sql: str, parameters: dict=None)`: returns the first row of the
    result. Does not load the whole dataset into memory because of using
    [execute_iter][ch-driver-execute-iter]. If the dataset is empty then returns
    `None` following [fetchone][python-db-api-2-fetchone] semantics.
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

See [example](#clickhousehook-example) below.

## ClickHouseSqlSensor Reference

Sensor fully inherits from [Airflow SQLSensor][airflow-sql-sensor] and therefore
    fully implements its interface using `ClickHouseHook` to fetch the SQL
    execution result and supports templating of `sql` argument.

See [example](#clickhousesqlsensor-example) below.

## How to create an Airflow connection to ClickHouse

As a `type` of a new connection, choose **SQLite**. `host` should be set to
    ClickHouse host's IP or domain name.

There is **no** special ClickHouse connection type yet, so we use SQLite as
    the closest one.

The rest of the connection details may be skipped as they
    [have defaults](#default-values) defined by `clickhouse-driver`. If
    you use non-default values, set them according to the
    [connection schema](#clickhouse-connection-schema).

If you use a secure connection to ClickHouse (this requires additional
    configurations on ClickHouse side), set `extra` to `{"secure":true}`.

### ClickHouse Connection schema

[clickhouse_driver.Client][ch-driver-client] is initialized with attributes stored
    in Airflow [Connection attributes][airflow-connection-attrs]. The mapping of
    the attributes is listed below:
  
| Airflow Connection attribute | `Client.__init__` argument |
| --- | --- |
| `host` | `host` |
| `port` | `port` |
| `schema` | `database` |
| `login` | `user` |
| `password` | `password` |
| `extra` | `**kwargs` |

`database` argument of `ClickHouseOperator` or `ClickHouseHook` overrides
    `schema` attribute of the Airflow connection.

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
    of the [clickhouse-driver][ch-driver] version you use. The only exception is
    `host`: if the attribute of Airflow connection is not set then `'localhost'`
    is used.

### Default connection

By default, the plugin uses `connection_id='clickhouse_default'`.

## Examples

### ClickHouseOperator Example

```python
from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
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
            '''
                INSERT INTO aggregate
                SELECT eventDt, sum(price * qty) AS income FROM sales
                WHERE eventDt = '{{ ds }}' GROUP BY eventDt
            ''', '''
                OPTIMIZE TABLE aggregate ON CLUSTER {{ var.value.cluster_name }}
                PARTITION toDate('{{ execution_date.format('%Y-%m-01') }}')
            ''', '''
                SELECT sum(income) FROM aggregate
                WHERE eventDt BETWEEN
                    '{{ execution_date.start_of('month').to_date_string() }}'
                    AND '{{ execution_date.end_of('month').to_date_string() }}'
            ''',
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

### ClickHouseHook Example

```python
from airflow import DAG
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
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

### ClickHouseSqlSensor Example

```python
from airflow import DAG
from airflow_clickhouse_plugin.sensors.clickhouse_sql_sensor import ClickHouseSqlSensor
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow.utils.dates import days_ago


with DAG(
        dag_id='listen_warnings',
        start_date=days_ago(2),
) as dag:
    dag >> ClickHouseSqlSensor(
        task_id='poke_events_count',
        database='monitor',
        sql="SELECT count() FROM warnings WHERE eventDate = '{{ ds }}'",
        success=lambda cnt: cnt > 10000,
    ) >> ClickHouseOperator(
        task_id='create_alert',
        database='alerts',
        sql='''
            INSERT INTO events SELECT eventDate, count()
            FROM monitor.warnings WHERE eventDate = '{{ ds }}'
        ''',
    )
```

# How to run tests

## Unit tests

From the root project directory: `python -m unittest discover -s tests/unit`

## Integration tests

Integration tests require access to ClickHouse server. Tests use connection
    URI defined [via environment variable][airflow-conn-env]
    `AIRFLOW_CONN_CLICKHOUSE_DEFAULT` with `clickhouse://localhost` as default.

Run from the project root: `python -m unittest discover -s tests/integration` 

## All tests

From the root project directory: `python -m unittest discover -s tests`

### Github Actions

[GitHub Action][github-action-src] is set up for this project.

### Run tests using Docker

Run ClickHouse server inside Docker:

```bash
docker exec -it $(docker run --rm -d yandex/clickhouse-server) bash
```

The above command will open `bash` inside the container.

Install dependencies into container and run tests (execute inside container):

```bash
apt-get update
apt-get install -y python3.8 python3-pip git
git clone https://github.com/whisklabs/airflow-clickhouse-plugin.git
cd airflow-clickhouse-plugin
python3.8 -m pip install -r requirements.txt
python3.8 -m unittest discover -s tests
```

# Contributors

* Created by Anton Bryzgalov, [@bryzgaloff](https://github.com/bryzgaloff), originally at [Whisk, Samsung](https://github.com/whisklabs)
* Inspired by Viktor Taranenko, [@viktortnk](https://github.com/viktortnk) (Whisk, Samsung)

Community contributors:

* Danila Ganchar, [@d-ganchar](https://github.com/d-ganchar)
* Mikhail, [@glader](https://github.com/glader)
* Alexander Chashnikov, [@ne1r0n](https://github.com/ne1r0n)
* Simone Brundu, [@saimon46](https://github.com/saimon46)
* [@gkarg](https://github.com/gkarg)
* Stanislav Morozov, [@r3b-fish](https://github.com/r3b-fish)
* Sergey Bychkov, [@SergeyBychkov](https://github.com/SergeyBychkov)
* [@was-av](https://github.com/was-av)


[airflow]: https://airflow.apache.org/
[ch-driver]: https://github.com/mymarilyn/clickhouse-driver
[ch-driver-docs]: https://clickhouse-driver.readthedocs.io/en/latest/
[ch-driver-execute]: https://clickhouse-driver.readthedocs.io/en/latest/quickstart.html#selecting-data
[airflow-base-op]: https://airflow.apache.org/docs/2.1.0/_api/airflow/models/baseoperator/index.html
[airflow-base-hook]: https://airflow.apache.org/docs/apache-airflow/2.1.0/_api/airflow/hooks/base/index.html#airflow.hooks.base.BaseHook
[ch-driver-execute-iter]: https://clickhouse-driver.readthedocs.io/en/latest/quickstart.html#streaming-results
[ch-driver-insert]: https://clickhouse-driver.readthedocs.io/en/latest/quickstart.html#inserting-data
[ch-driver-client]: https://clickhouse-driver.readthedocs.io/en/latest/api.html#client
[airflow-conn-extra]: https://airflow.apache.org/docs/2.1.0/_api/airflow/models/connection/index.html#airflow.models.connection.Connection.extra
[ch-driver-connection]: https://clickhouse-driver.readthedocs.io/en/latest/api.html#connection
[airflow-connection-attrs]: https://airflow.apache.org/docs/apache-airflow/2.1.0/_api/airflow/models/index.html?highlight=connection#airflow.models.Connection
[airflow-conn-dejson]: https://airflow.apache.org/docs/apache-airflow/2.1.0/_api/airflow/models/index.html?highlight=connection#airflow.models.Connection.extra_dejson
[airflow-conn-env]: https://airflow.apache.org/docs/apache-airflow/2.1.0/howto/connection.html#storing-a-connection-in-environment-variables
[python-db-api-2-fetchone]: https://www.python.org/dev/peps/pep-0249/#fetchone
[cloud-composer-versions]: https://cloud.google.com/composer/docs/concepts/versioning/composer-versions#supported_versions
[airflow-sql-sensor]: https://airflow.apache.org/docs/2.1.0/_api/airflow/sensors/sql/index.html
[github-action-src]: https://github.com/whisklabs/airflow-clickhouse-plugin/tree/master/.github/workflows
[pandas-extra]: https://github.com/apache/airflow/commit/2c26b15a8087cb8a81eb19fedbc768bd6da92df7#diff-60f61ab7a8d1910d86d9fda2261620314edcae5894d5aaa236b821c7256badd7
[pep-440-compatible-releases]: https://peps.python.org/pep-0440/#compatible-release

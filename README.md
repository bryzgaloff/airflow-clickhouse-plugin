# Airflow ClickHouse Plugin

![PyPI - Downloads](https://img.shields.io/pypi/dm/airflow-clickhouse-plugin)
![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/bryzgaloff/airflow-clickhouse-plugin/tests.yml?branch=master)
![GitHub contributors](https://img.shields.io/github/contributors/bryzgaloff/airflow-clickhouse-plugin?color=blue)

🔝 The most popular [Apache Airflow][airflow] plugin for ClickHouse, ranked in the top 1% of downloads [on PyPI](https://pypi.org/project/airflow-clickhouse-plugin/). Based on awesome [mymarilyn/clickhouse-driver][ch-driver].

This plugin provides two families of operators: richer [`clickhouse_driver.Client.execute`-based](#clickhouse-driver-family) and standardized [compatible with Python DB API 2.0](#python-db-api-20-family).

Both operators' families are fully supported and covered with tests for different versions of Airflow and Python.

## `clickhouse-driver` family

- `ClickHouseOperator`
- `ClickHouseHook`
- `ClickHouseSensor`

These operators are based on [mymarilyn/clickhouse-driver][ch-driver]'s `Client.execute` method and arguments. They offer a full functionality of `clickhouse-driver` and are recommended if you are starting fresh with ClickHouse in Airflow.

### Features

- **SQL Templating**: SQL queries and other parameters are templated.
- **Multiple SQL Queries**: execute run multiple SQL queries within a single `ClickHouseOperator`. The result of the last query is pushed to XCom (configurable by `do_xcom_push`).
- **Logging**: executed queries are logged in a visually pleasing format, making it easier to track and debug.
- **Efficient Native ClickHouse Protocol**: Utilizes efficient _native_ ClickHouse TCP protocol, thanks to [clickhouse-driver][ch-driver-docs]. **Does not support HTTP protocol.**
- **Custom Connection Parameters**: Supports additional ClickHouse [connection parameters][ch-driver-connection], such as various timeouts, `compression`, `secure`, through the Airflow [Connection.extra][airflow-conn-extra] property.

See reference and examples [below](#usage).

### Installation and dependencies

`pip install -U airflow-clickhouse-plugin`

Dependencies: only `apache-airflow` and `clickhouse-driver`.

## Python DB API 2.0 family

- Operators:
  - `ClickHouseSQLExecuteQueryOperator`
  - `ClickHouseSQLColumnCheckOperator`
  - `ClickHouseSQLTableCheckOperator`
  - `ClickHouseSQLCheckOperator`
  - `ClickHouseSQLValueCheckOperator`
  - `ClickHouseSQLIntervalCheckOperator`
  - `ClickHouseSQLThresholdCheckOperator`
  - `ClickHouseBranchSQLOperator`
- `ClickHouseDbApiHook`
- `ClickHouseSqlSensor`

These operators combine [`clickhouse_driver.dbapi`][ch-driver-db-api] with [apache-airflow-providers-common-sql]. While they have limited functionality compared to `Client.execute` (not all arguments are supported), they provide a standardized interface. This is useful when porting Airflow pipelines to ClickHouse from another SQL provider backed by `common.sql` Airflow package, such as MySQL, Postgres, BigQuery, and others.

The feature set of this version is fully based on `common.sql` Airflow provider: refer to its [reference][common-sql-reference] and [examples][common-sql-examples] for details.

An example is also available [below](#db-api-20-clickhousesqlsensor-and-clickhousesqlexecutequeryoperator-example).

### Installation and dependencies

Add `common.sql` extra when installing the plugin: `pip install -U airflow-clickhouse-plugin[common.sql]` — to enable DB API 2.0 operators.

Dependencies: `apache-airflow-providers-common-sql` (usually pre-packed with Airflow) in addition to `apache-airflow` and `clickhouse-driver`.

## Python and Airflow versions support

Different versions of the plugin support different combinations of Python and Airflow versions. We _primarily_ support **Airflow 2.0+ and Python 3.8+**. If you need to use the plugin with older Python-Airflow combinations, pick a suitable plugin version:

| airflow-clickhouse-plugin version | Airflow version         | Python version     |
|-----------------------------------|-------------------------|--------------------|
| 1.3.0                             | \>=2.0.0,<2.10.0        | ~=3.8              |
| 1.2.0                             | \>=2.0.0,<2.9.0         | ~=3.8              |
| 1.1.0                             | \>=2.0.0,<2.8.0         | ~=3.8              |
| 1.0.0                             | \>=2.0.0,<2.7.0         | ~=3.8              |
| 0.11.0                            | ~=2.0.0,\>=2.2.0,<2.7.0 | ~=3.7              |
| 0.10.0,0.10.1                     | ~=2.0.0,\>=2.2.0,<2.6.0 | ~=3.7              |
| 0.9.0,0.9.1                       | ~=2.0.0,\>=2.2.0,<2.5.0 | ~=3.7              |
| 0.8.2                             | \>=2.0.0,<2.4.0         | ~=3.7              |
| 0.8.0,0.8.1                       | \>=2.0.0,<2.3.0         | ~=3.6              |
| 0.7.0                             | \>=2.0.0,<2.2.0         | ~=3.6              |
| 0.6.0                             | ~=2.0.1                 | ~=3.6              |
| \>=0.5.4,<0.6.0                   | ~=1.10.6                | \>=2.7 or >=3.5.\* |
| \>=0.5.0,<0.5.4                   | ==1.10.6                | \>=2.7 or >=3.5.\* |

`~=` means compatible release, see [PEP 440][pep-440-compatible-releases] for an explanation.

[DB API 2.0 functionality](#python-db-api-20-family) requires `apache-airflow>=2.2` and `apache-airflow-providers-common-sql>=1.3`: earlier versions are not supported.

Previous versions of the plugin might require `pandas` extra: `pip install airflow-clickhouse-plugin[pandas]==0.11.0`. Check out earlier versions of `README.md` for details.

# Usage

To see examples [scroll down](#examples). To run them, [create an Airflow connection to ClickHouse](#how-to-create-an-airflow-connection-to-clickhouse).

## ClickHouseOperator reference

To import `ClickHouseOperator` use `from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator`.

Supported arguments:
* `sql` (templated, required): query (if argument is a single `str`) or multiple queries (iterable of `str`). Supports files with `.sql` extension.
* `clickhouse_conn_id`: Airflow connection id. Connection schema is described [below](#clickhouse-connection-schema). Default connection id is `clickhouse_default`.
* Arguments of [`clickhouse_driver.Client.execute` method][ch-driver-execute-summary]:
  * `parameters` (templated): passed `params` of the `execute` method. (Renamed to avoid name conflict with Airflow tasks' `params` argument.)
    * `dict` for `SELECT` queries.
    * `list`/`tuple`/generator for `INSERT` queries.
    * If multiple queries are provided via `sql` then the `parameters` are passed to _all_ of them.
  * `with_column_types` (not templated).
  * `external_tables` (templated).
  * `query_id` (templated).
  * `settings` (templated).
  * `types_check` (not templated).
  * `columnar` (not templated).
  * For the documentation of these arguments, refer to [`clickhouse_driver.Client.execute` API reference][ch-driver-execute-reference].
* `database` (templated): if present, overrides `schema` of Airflow connection.
* Other arguments (including a required `task_id`) are inherited from Airflow [BaseOperator][airflow-base-op].

Result of the _last_ query is pushed to XCom (disable using `do_xcom_push=False` argument).

In other words, the operator simply wraps [`ClickHouseHook.execute` method](#clickhousehook-reference).

See [example](#clickhouseoperator-example) below.

## ClickHouseHook reference

To import `ClickHouseHook` use `from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook`.

Supported kwargs of constructor (`__init__` method):
* `clickhouse_conn_id`: Airflow connection id. Connection schema is described [below](#clickhouse-connection-schema). Default connection id is `clickhouse_default`.
* `database`: if present, overrides `schema` of Airflow connection.

Defines `ClickHouseHook.execute` method which simply wraps [`clickhouse_driver.Client.execute`][ch-driver-execute-reference]. It has all the same arguments, except of:
* `sql` (instead of `execute`'s `query`): query (if argument is a single `str`) or multiple queries (iterable of `str`).

`ClickHouseHook.execute` returns a result of the _last_ query.

Also, the hook defines `get_conn()` method which returns an underlying [`clickhouse_driver.Client`][ch-driver-client] instance.

See [example](#clickhousehook-example) below.

## ClickHouseSensor reference

To import `ClickHouseSensor` use `from airflow_clickhouse_plugin.sensors.clickhouse import ClickHouseSensor`.

This class wraps [`ClickHouseHook.execute` method](#clickhousehook-reference) into an [Airflow sensor][airflow-sensor]. Supports all the arguments of [`ClickHouseOperator`](#clickhouseoperator-reference) and additionally:
* `is_success`: a callable which accepts a single argument — a return value of `ClickHouseHook.execute`. If a return value of `is_success` is truthy, the sensor succeeds. By default, the callable is `bool`: i.e. if the return value of `ClickHouseHook.execute` is truthy, the sensor succeeds. Usually, `execute` is a list of records returned by query: thus, by default it is falsy if no records are returned.
* `is_failure`: a callable which accepts a single argument — a return value of `ClickHouseHook.execute`. If a return value of `is_failure` is truthy, the sensor raises `AirflowException`. By default, `is_failure` is `None` and no failure check is performed.

See [example](#clickhousesensor-example) below.

## How to create an Airflow connection to ClickHouse

As a `type` of a new connection, choose **SQLite** or any other SQL database. There is **no** special ClickHouse connection type yet, so we use any SQL as the closest one.

All the connection attributes are optional: default host is `localhost` and other credentials [have defaults](#default-values) defined by `clickhouse-driver`. If you use non-default values, set them according to the [connection schema](#clickhouse-connection-schema).

If you use a secure connection to ClickHouse (this requires additional configurations on ClickHouse side), set `extra` to `{"secure":true}`. All `extra` connection parameters are passed to [`clickhouse_driver.Client`][ch-driver-client] as-is.

### ClickHouse connection schema

[`clickhouse_driver.Client`][ch-driver-client] is initialized with attributes stored in Airflow [Connection attributes][airflow-connection-howto]:
  
| Airflow Connection attribute | `Client.__init__` argument |
|------------------------------|----------------------------|
| `host`                       | `host`                     |
| `port` (`int`)               | `port`                     |
| `schema`                     | `database`                 |
| `login`                      | `user`                     |
| `password`                   | `password`                 |
| `extra`                      | `**kwargs`                 |

`database` argument of `ClickHouseOperator`, `ClickHouseHook`, `ClickHouseSensor`, and others overrides `schema` attribute of the Airflow connection.

### Extra arguments

You may set non-standard arguments of [`clickhouse_driver.Client`][ch-driver-client], such as timeouts, `compression`, `secure`, etc. using Airflow's [`Connection.extra`][airflow-conn-extra] attribute. The attribute should contain a JSON object which will be [deserialized][airflow-conn-dejson] and all of its properties will be passed as-is to the `Client`.

For example, if Airflow connection contains `extra='{"secure": true}'` then the `Client.__init__` will receive `secure=True` keyword argument in addition to other connection attributes.

#### Compression

You should install specific packages to support compression. For example, for lz4:

```bash
pip3 install clickhouse-cityhash lz4
```

Then you should include `compression` parameter in airflow connection uri: `extra='{"compression":"lz4"}'`. You can get additional information about extra options from [official documentation of clickhouse-driver][ch-driver-pypi-install].

Connection URI with compression will look like `clickhouse://login:password@host:port/?compression=lz4`.

See [official documentation][airflow-connection-howto] to learn more about connections management in Airflow.

### Default Values

If some Airflow connection attribute is not set, it is not passed to `clickhouse_driver.Client`. In such cases, the plugin uses a default value from the corresponding [`clickhouse_driver.Connection`][ch-driver-connection] argument. For instance, `user` defaults to `'default'`.

This means that the plugin itself does not define any default values for the ClickHouse connection. You may fully rely on default values of the [clickhouse-driver][ch-driver] version you use.

The only exception is `host`: if the attribute of Airflow connection is not set then `'localhost'` is used.

### Default connection

By default, the plugin uses Airflow connection with id `'clickhouse_default'`.

## Examples

### ClickHouseOperator example

```python
from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.operators.python import PythonOperator
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
        # query_id is templated and allows to quickly identify query in ClickHouse logs
        query_id='{{ ti.dag_id }}-{{ ti.task_id }}-{{ ti.run_id }}-{{ ti.try_number }}',
        clickhouse_conn_id='clickhouse_test',
    ) >> PythonOperator(
        task_id='print_month_income',
        python_callable=lambda task_instance:
            # pulling XCom value and printing it
            print(task_instance.xcom_pull(task_ids='update_income_aggregate')),
    )
```

### ClickHouseHook example

```python
from airflow import DAG
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def sqlite_to_clickhouse():
    sqlite_hook = SqliteHook()
    ch_hook = ClickHouseHook()
    records = sqlite_hook.get_records('SELECT * FROM some_sqlite_table')
    ch_hook.execute('INSERT INTO some_ch_table VALUES', records)


with DAG(
        dag_id='sqlite_to_clickhouse',
        start_date=days_ago(2),
) as dag:
    dag >> PythonOperator(
        task_id='sqlite_to_clickhouse',
        python_callable=sqlite_to_clickhouse,
    )
```

Important note: don't try to insert values using `ch_hook.execute('INSERT INTO some_ch_table VALUES (1)')` literal form. [`clickhouse-driver` requires][ch-driver-insert] values for `INSERT` query to be provided via `parameters` due to specifics of the native ClickHouse protocol.

### ClickHouseSensor example

```python
from airflow import DAG
from airflow_clickhouse_plugin.sensors.clickhouse import ClickHouseSensor
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.utils.dates import days_ago


with DAG(
        dag_id='listen_warnings',
        start_date=days_ago(2),
) as dag:
    dag >> ClickHouseSensor(
        task_id='poke_events_count',
        database='monitor',
        sql="SELECT count() FROM warnings WHERE eventDate = '{{ ds }}'",
        is_success=lambda cnt: cnt > 10000,
    ) >> ClickHouseOperator(
        task_id='create_alert',
        database='alerts',
        sql='''
            INSERT INTO events SELECT eventDate, count()
            FROM monitor.warnings WHERE eventDate = '{{ ds }}'
        ''',
    )
```

### DB API 2.0: ClickHouseSqlSensor and ClickHouseSQLExecuteQueryOperator example

```python
from airflow import DAG
from airflow_clickhouse_plugin.sensors.clickhouse_dbapi import ClickHouseSqlSensor
from airflow_clickhouse_plugin.operators.clickhouse_dbapi import ClickHouseSQLExecuteQueryOperator
from airflow.utils.dates import days_ago


with DAG(
        dag_id='listen_warnings',
        start_date=days_ago(2),
) as dag:
    dag >> ClickHouseSqlSensor(
        task_id='poke_events_count',
        hook_params=dict(schema='monitor'),
        sql="SELECT count() FROM warnings WHERE eventDate = '{{ ds }}'",
        success=lambda cnt: cnt > 10000,
        conn_id=None,  # required by common.sql SqlSensor; use None for default
    ) >> ClickHouseSQLExecuteQueryOperator(
        task_id='create_alert',
        database='alerts',
        sql='''
            INSERT INTO events SELECT eventDate, count()
            FROM monitor.warnings WHERE eventDate = '{{ ds }}'
        ''',
    )
```

# How to run tests

Unit tests: `python3 -m unittest discover -t tests -s unit`

Integration tests require access to a ClickHouse server. Here is how to set up a local test environment using Docker:
* Run ClickHouse server in a local Docker container: `docker run -p 9000:9000 --ulimit nofile=262144:262144 -it clickhouse/clickhouse-server`
* Run tests with Airflow connection details set [via environment variable][airflow-conn-env]: `PYTHONPATH=src AIRFLOW_CONN_CLICKHOUSE_DEFAULT=clickhouse://localhost python3 -m unittest discover -t tests -s integration`
* Stop the container after running the tests to deallocate its resources.

Run all (unit&integration) tests with ClickHouse connection defined: `PYTHONPATH=src AIRFLOW_CONN_CLICKHOUSE_DEFAULT=clickhouse://localhost python3 -m unittest discover -s tests`

## GitHub Actions

[GitHub Action][github-action-src] is configured for this project.

## Run all tests inside Docker

Start a ClickHouse server inside Docker: `docker exec -it $(docker run --rm -d clickhouse/clickhouse-server) bash`

The above command will open `bash` inside the container.

Install dependencies into container and run tests (execute inside container):

```bash
apt-get update
apt-get install -y python3 python3-pip git make
git clone https://github.com/whisklabs/airflow-clickhouse-plugin.git
cd airflow-clickhouse-plugin
python3 -m pip install -r requirements.txt
PYTHONPATH=src AIRFLOW_CONN_CLICKHOUSE_DEFAULT=clickhouse://localhost python3 -m unittest discover -s tests
```

Stop the container.

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
* Maxim Tarasov, [@MaximTar](https://github.com/MaximTar)
* [@dvnrvn](https://github.com/dvnrvn)
* Giovanni Corsetti, [@CorsettiS](https://github.com/CorsettiS)
* Dmytro Zhyzniev, [@1ng4lipt](https://github.com/1ng4lipt)
* Anton Bezdenezhnykh, [@GaMeRaM](https://github.com/GaMeRaM)
* Andrey, [@bobelev](https://github.com/bobelev)


[airflow]: https://airflow.apache.org/
[ch-driver]: https://github.com/mymarilyn/clickhouse-driver
[ch-driver-docs]: https://clickhouse-driver.readthedocs.io/en/latest/
[ch-driver-execute-summary]: https://clickhouse-driver.readthedocs.io/en/latest/quickstart.html#selecting-data
[ch-driver-execute-reference]: https://clickhouse-driver.readthedocs.io/en/latest/api.html#clickhouse_driver.Client.execute
[airflow-base-op]: https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/baseoperator/index.html
[ch-driver-insert]: https://clickhouse-driver.readthedocs.io/en/latest/quickstart.html#inserting-data
[ch-driver-client]: https://clickhouse-driver.readthedocs.io/en/latest/api.html#client
[ch-driver-connection]: https://clickhouse-driver.readthedocs.io/en/latest/api.html#connection
[airflow-conn-extra]: https://airflow.apache.org/docs/2.1.0/_api/airflow/models/connection/index.html#airflow.models.connection.Connection.extra
[airflow-connection-howto]: https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html
[airflow-conn-dejson]: https://airflow.apache.org/docs/apache-airflow/2.1.0/_api/airflow/models/index.html?highlight=connection#airflow.models.Connection.extra_dejson
[airflow-conn-env]: https://airflow.apache.org/docs/apache-airflow/2.1.0/howto/connection.html#storing-a-connection-in-environment-variables
[github-action-src]: https://github.com/whisklabs/airflow-clickhouse-plugin/tree/master/.github/workflows
[pep-440-compatible-releases]: https://peps.python.org/pep-0440/#compatible-release
[apache-airflow-providers-common-sql]: https://airflow.apache.org/docs/apache-airflow-providers-common-sql/stable/index.html
[db-api-pep]: https://peps.python.org/pep-0249/
[airflow-sensor]: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html
[ch-driver-pypi-install]: https://clickhouse-driver.readthedocs.io/en/latest/installation.html#installation-pypi
[common-sql-reference]: https://airflow.apache.org/docs/apache-airflow-providers-common-sql/stable/_api/airflow/providers/common/sql/index.html
[common-sql-examples]: https://airflow.apache.org/docs/apache-airflow-providers-common-sql/stable/operators.html
[ch-driver-db-api]: https://clickhouse-driver.readthedocs.io/en/latest/dbapi.html

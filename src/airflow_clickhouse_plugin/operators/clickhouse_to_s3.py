#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import enum
from collections import namedtuple
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Iterable, Mapping, Sequence

from typing_extensions import Literal

from airflow.exceptions import AirflowException
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class FILE_FORMAT(enum.Enum):
    """Possible file formats."""

    CSV = enum.auto()
    JSON = enum.auto()
    PARQUET = enum.auto()


FileOptions = namedtuple("FileOptions", ["mode", "suffix", "function"])

FILE_OPTIONS_MAP = {
    FILE_FORMAT.CSV: FileOptions("r+", ".csv", "to_csv"),
    FILE_FORMAT.JSON: FileOptions("r+", ".json", "to_json"),
    FILE_FORMAT.PARQUET: FileOptions("rb+", ".parquet", "to_parquet"),
}


class ClickhouseToS3Operator(BaseOperator):
    """
    Saves data from a specific SQL query into a file in S3.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ClickhouseToS3Operator`

    :param query: the clickhouse query to be executed. If you want to execute a file, place the absolute path of it,
        ending with .clickhouse extension. (templated)
    :param s3_bucket: bucket where the data will be stored. (templated)
    :param s3_key: desired key for the file. It includes the name of the file. (templated)
    :param replace: whether or not to replace the file in S3 if it previously existed
    :param clickhouse_conn_id: reference to a specific database.
    :param clickhouse_hook_params: Extra config params to be passed to the underlying hook.
        Should match the desired hook constructor params.
    :param parameters: (optional) the parameters to render the SQL query with.
    :param aws_conn_id: reference to a specific S3 connection
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                (unless use_ssl is False), but SSL certificates will not be verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                You can specify this argument if you want to use a different
                CA cert bundle than the one used by botocore.
    :param file_format: the destination file format, only string 'csv', 'json' or 'parquet' is accepted.
    :param pd_kwargs: arguments to include in DataFrame ``.to_parquet()``, ``.to_json()`` or ``.to_csv()``.
    :param groupby_kwargs: argument to include in DataFrame ``groupby()``.
    """

    template_fields: Sequence[str] = (
        "s3_bucket",
        "s3_key",
        "query",
        "clickhouse_conn_id",
    )
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {
        "query": "sql",
        "pd_kwargs": "json",
    }

    def __init__(
        self,
        *,
        query: str,
        s3_bucket: str,
        s3_key: str,
        clickhouse_conn_id: str,
        clickhouse_hook_params: dict | None = None,
        parameters: None | Mapping | Iterable = None,
        replace: bool = False,
        aws_conn_id: str = "aws_default",
        verify: bool | str | None = None,
        file_format: Literal["csv", "json", "parquet"] = "csv",
        pd_kwargs: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.query = query
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.clickhouse_conn_id = clickhouse_conn_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.replace = replace
        self.pd_kwargs = pd_kwargs or {}
        self.parameters = parameters
        self.clickhouse_hook_params = clickhouse_hook_params

        if "path_or_buf" in self.pd_kwargs:
            raise AirflowException("The argument path_or_buf is not allowed, please remove it")

        try:
            self.file_format = FILE_FORMAT[file_format.upper()]
        except KeyError:
            raise AirflowException(f"The argument file_format doesn't support {file_format} value.")

    @staticmethod
    def _fix_dtypes(df: pd.DataFrame, file_format: FILE_FORMAT) -> None:
        """
        Mutate DataFrame to set dtypes for float columns containing NaN values.

        Set dtype of object to str to allow for downstream transformations.
        """
        try:
            import numpy as np
            import pandas as pd
        except ImportError as e:
            from airflow.exceptions import AirflowOptionalProviderFeatureException

            raise AirflowOptionalProviderFeatureException(e)

        for col in df:

            if df[col].dtype.name == "object" and file_format == "parquet":
                # if the type wasn't identified or converted, change it to a string so if can still be
                # processed.
                df[col] = df[col].astype(str)

            if "float" in df[col].dtype.name and df[col].hasnans:
                # inspect values to determine if dtype of non-null values is int or float
                notna_series = df[col].dropna().values
                if np.equal(notna_series, notna_series.astype(int)).all():
                    # set to dtype that retains integers and supports NaNs
                    # The type ignore can be removed here if https://github.com/numpy/numpy/pull/23690
                    # is merged and released as currently NumPy does not consider None as valid for x/y.
                    df[col] = np.where(df[col].isnull(), None, df[col])  # type: ignore[call-overload]
                    df[col] = df[col].astype(pd.Int64Dtype())
                elif np.isclose(notna_series, notna_series.astype(int)).all():
                    # set to float dtype that retains floats and supports NaNs
                    # The type ignore can be removed here if https://github.com/numpy/numpy/pull/23690
                    # is merged and released
                    df[col] = np.where(df[col].isnull(), None, df[col])  # type: ignore[call-overload]
                    df[col] = df[col].astype(pd.Float64Dtype())

    def execute(self, context: Context) -> None:
        clickhouse_hook = ClickHouseHook(
            clickhouse_conn_id=self.clickhouse_conn_id,
            database='default',
        )
        s3_conn = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        data_df = clickhouse_hook.get_pandas_df(sql=self.query)
        self.log.info("Data from Clickhouse obtained")

        self._fix_dtypes(data_df, self.file_format)
        file_options = FILE_OPTIONS_MAP[self.file_format]

        with NamedTemporaryFile(mode=file_options.mode, suffix=file_options.suffix) as tmp_file:

            self.log.info("Writing data to temp file")
            getattr(data_df, file_options.function)(tmp_file.name, **self.pd_kwargs)

            self.log.info("Uploading data to S3")
            s3_conn.load_file(
                filename=tmp_file.name, key=self.s3_key, bucket_name=self.s3_bucket, replace=self.replace
            )

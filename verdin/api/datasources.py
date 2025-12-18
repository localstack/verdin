from typing import Literal, Iterable, TypedDict

import requests

from .base import Api, ApiError, ApiResponse


class DataSourceNotFoundError(ApiError):
    """
    Specific ApiError representing a 404 Not Found when database names are given.
    """


class DataSourceInfo(TypedDict):
    """
    A data source info object. Example::

        {
          "cluster": "tinybird",
          "created_at": "2025-12-17 13:18:09.799040",
          "description": "Simple Key-Value Data Source",
          "engine": {
            "engine": "MergeTree",
            "engine_full": "MergeTree ORDER BY tuple()",
            "sorting_key": "tuple()"
          },
          "errors_discarded_at": null,
          "headers": {},
          "id": "t_e1ea6e1e32004989af509b034b0987c1",
          "indexes": [],
          "last_commit": {
            "content_sha": "",
            "path": "",
            "status": "ok"
          },
          "name": "simple",
          "new_columns_detected": false,
          "project": null,
          "replicated": false,
          "schema": {
            "columns": [
              {
                "auto": false,
                "codec": null,
                "default_value": null,
                "jsonpath": "$.Id",
                "name": "id",
                "normalized_name": "id",
                "nullable": false,
                "type": "UUID"
              },
              {
                "auto": false,
                "codec": null,
                "default_value": null,
                "jsonpath": "$.Timestamp",
                "name": "timestamp",
                "normalized_name": "timestamp",
                "nullable": false,
                "type": "DateTime64(6)"
              },
              {
                "auto": false,
                "codec": null,
                "default_value": null,
                "jsonpath": "$.Key",
                "name": "key",
                "normalized_name": "key",
                "nullable": false,
                "type": "String"
              },
              {
                "auto": false,
                "codec": null,
                "default_value": null,
                "jsonpath": "$.Value",
                "name": "value",
                "normalized_name": "value",
                "nullable": false,
                "type": "String"
              }
            ],
            "sql_schema": "`id` UUID `json:$.Id`, `timestamp` DateTime64(6) `json:$.Timestamp`, `key` String `json:$.Key`, `value` String `json:$.Value`"
          },
          "shared_with": [],
          "statistics": {
            "bytes": 0,
            "row_count": 0
          },
          "tags": {},
          "type": "ndjson",
          "updated_at": "2025-12-17 13:18:09.799040",
          "used_by": [
            {
              "id": "t_c50152ced57b46b99acf14930b9c6906",
              "name": "simple_kv"
            }
          ],
          "version": 0
        }
    """

    cluster: str
    created_at: str
    description: str
    engine: dict
    errors_discarded_at: str | None
    headers: dict
    id: str
    indexes: list
    last_commit: dict
    name: str
    new_columns_detected: bool
    project: str | None
    replicated: bool
    schema: dict
    shared_with: list
    statistics: dict
    tags: dict
    type: str
    updated_at: str
    used_by: list[dict]
    version: int


class DataSourceAppendInfo(TypedDict):
    """Information about a data source returned when appending to the data source."""

    cluster: str
    created_at: str
    description: str
    engine: dict  # TODO: {'engine': 'MergeTree', 'sorting_key': 'tuple()'}
    errors_discarded_at: str | None
    headers: dict
    id: str
    last_commit: dict  # TODO: {'content_sha': '', 'path': '', 'status': 'ok'}
    name: str
    project: str | None
    replicated: bool
    shared_with: list  # TODO
    tags: dict
    type: str
    updated_at: str
    used_by: list  # TODO
    version: int


class ListDataSourcesResponse(ApiResponse):
    @property
    def datasources(self) -> list[DataSourceInfo]:
        return self.json.get("datasources", [])


class AppendDataResponse(ApiResponse):
    @property
    def datasource(self) -> DataSourceAppendInfo:
        return self.json.get("datasource", {})

    @property
    def import_id(self) -> str:
        return self.json.get("import_id")

    @property
    def invalid_lines(self) -> int:
        return self.json.get("invalid_lines")

    @property
    def quarantine_rows(self) -> int:
        return self.json.get("quarantine_rows")

    @property
    def error(self) -> str | None:
        error = self.json.get("error")
        if not error:
            return None
        return error


class GetDataSourceInformationResponse(ApiResponse):
    @property
    def info(self) -> DataSourceInfo:
        """
        Returns the data source information.

        Example::

        {
            "id": "t_bd1c62b5e67142bd9bf9a7f113a2b6ea",
            "name": "datasource_name",
            "statistics": {
                "bytes": 430833,
                "row_count": 3980
            },
            "used_by": [{
                "id": "t_efdc62b5e67142bd9bf9a7f113a34353",
                "name": "pipe_using_datasource_name"
            }]
            "updated_at": "2018-09-07 23:50:32.322461",
            "created_at": "2018-11-28 23:50:32.322461",
            "type": "csv"
        }

        """
        return self.json


class DataSourcesApi(Api):
    """
    ``/v0/datasources`` API client.

    TODO: missing APIs:
     * Creating data sources (POST /v0/datasources with mode=create)
     * Replacing data sources (POST /v0/datasources with mode=replace)
     * Alter data source (POST /v0/datasources/:name/alter)
     * Delete data (POST /v0/datasources/:name/delete)
     * Drop data source (DELETE /v0/datasources/:name)
     * Update data source attributes (PUT /v0/datasources/:name)
    """

    endpoint: str = "/v0/datasources"

    session: requests.Session

    def __init__(self, token: str, host: str = None):
        super().__init__(token, host)

        self.session = requests.Session()
        if self.token:
            self.session.headers.update({"Authorization": f"Bearer {self.token}"})

    def append(
        self,
        name: str,
        data: str | bytes | Iterable[bytes] | Iterable[str],
        dialect_delimiter: str = None,
        dialect_new_line: str = None,
        dialect_escapechar: str = None,
        progress: bool = False,
        format: Literal["csv", "ndjson", "parquet"] = None,
    ) -> AppendDataResponse:
        """
        Makes a POST request to ``/v0/datasources`` endpoint with mode=append, which appends data to the datasource.

        The data is expected to already be encoded in the format specified by the format parameter. You can pass
        generators or other iterables as data. For example::

            records = [...]  # some list of dicts

            def _data():
                # creates an NDJSON stream
                for r in records:
                    yield json.dumps(r) + "\\n"

            response = ds.append("my_table", _data(), format="ndjson")

        :param name: Name of the data source to append data to.
        :param data: Data to append.
        :param dialect_delimiter: The one-character string separating the fields. We try to guess the delimiter based
            on the CSV contents using some statistics, but sometimes we fail to identify the correct one. If you know
            your CSV’s field delimiter, you can use this parameter to explicitly define it.
        :param dialect_new_line: The one- or two-character string separating the records. We try to guess the delimiter
            based on the CSV contents using some statistics, but sometimes we fail to identify the correct one. If you
            know your CSV’s record delimiter, you can use this parameter to explicitly define it.
        :param dialect_escapechar: The escapechar removes any special meaning from the following character. This is
            useful if the CSV does not use double quotes to encapsulate a column but uses double quotes in the content
            of a column and it is escaped with, e.g. a backslash.
        :param progress: When using true and sending the data in the request body, Tinybird will return block status
            while loading using Line-delimited JSON. TODO: currently not supported
        :param format: Default: csv. Indicates the format of the data to be ingested in the Data Source. By default is
            csv and you should specify format=ndjson for NDJSON format, and format=parquet for Parquet files.
        :return: A ``AppendDataResponse`` object.
        """
        if progress:
            raise NotImplementedError

        params = {
            "mode": "append",
            "name": name,
        }

        if dialect_delimiter:
            params["dialect_delimiter"] = dialect_delimiter
        if dialect_new_line:
            params["dialect_new_line"] = dialect_new_line
        if dialect_escapechar:
            params["dialect_escapechar"] = dialect_escapechar
        if format:
            params["format"] = format

        headers = {}
        if format == "csv":
            headers["Content-Type"] = "text/html; charset=utf-8"
        if format == "ndjson":
            headers["Content-Type"] = "application/x-ndjson; charset=utf-8"

        response = self.session.request(
            method="POST",
            url=f"{self.host}{self.endpoint}",
            params=params,
            headers=headers,
            data=data,
        )

        if not response.ok:
            raise ApiError(response)

        return AppendDataResponse(response)

    def list(self) -> ListDataSourcesResponse:
        """
        Makes a GET request to ``/v0/datasources`` endpoint, which returns a list of datasources.

        :return: A ``ListDataSourcesResponse`` object
        """
        response = self.session.request(
            method="GET",
            url=f"{self.host}{self.endpoint}",
        )

        if not response.ok:
            raise ApiError(response)

        return ListDataSourcesResponse(response)

    def get_information(self, name: str) -> GetDataSourceInformationResponse:
        """
        Makes a GET request to ``/v0/datasources/<name>`` endpoint, which returns information about the datasource.

        :param name: The name of the datasource to get information about.
        :return: A ``GetDataSourceInformationResponse``
        """
        response = self.session.request(
            method="GET",
            url=f"{self.host}{self.endpoint}/{name}",
        )

        if response.status_code == 404:
            raise DataSourceNotFoundError(response)

        if not response.ok:
            raise ApiError(response)

        return GetDataSourceInformationResponse(response)

    def truncate(self, name: str):
        """
        Makes a POST request to ``/v0/datasources/:name/truncate``, which truncates the datasource.

        :param name: The name of the datasource to truncate.
        """
        response = self.session.request(
            method="POST",
            url=f"{self.host}{self.endpoint}/{name}/truncate",
        )

        if response.status_code == 404:
            raise DataSourceNotFoundError(response)

        if not response.ok:
            raise ApiError(response)

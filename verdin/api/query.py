import csv
import json
from typing import Literal, Optional, TypedDict, Any

import requests

from verdin.api import ApiResponse
from verdin.api.base import Api, ApiError

QueryOutputFormat = Literal[
    "CSV",
    "CSVWithNames",
    "JSON",
    "TSV",
    "TSVWithNames",
    "PrettyCompact",
    "JSONEachRow",
    "Parquet",
    "Prometheus",
]
"""See https://www.tinybird.co/docs/api-reference/query-api#id10

+---------------+--------------------------------------------------+
| Format        | Description                                      |
+===============|==================================================+
| CSV           | CSV without header                               |
+---------------+--------------------------------------------------+
| CSVWithNames  | CSV with header                                  |
+---------------+--------------------------------------------------+
| JSON          | JSON including data, statistics and schema info  |
+---------------+--------------------------------------------------+
| TSV           | TSV without header                               |
+---------------+--------------------------------------------------+
| TSVWithNames  | TSV with header                                  |
+---------------+--------------------------------------------------+
| PrettyCompact | Formatted table                                  |
+---------------+--------------------------------------------------+
| JSONEachRow   | Newline-delimited JSON values (NDJSON)           |
+---------------+--------------------------------------------------+
| Parquet       | Apache Parquet                                   |
+---------------+--------------------------------------------------+
| Prometheus    | Prometheus text-based format                     |
+---------------+--------------------------------------------------+
"""


class QueryStatistics(TypedDict):
    bytes_read: int
    elapsed: float
    rows_read: int


class QueryMetadata(TypedDict):
    name: str
    type: str


QueryData = list[dict[str, Any]]


class QueryResponse(ApiResponse):
    @property
    def data(self) -> QueryData:
        raise NotImplementedError


class QueryJsonResponse(QueryResponse):
    @property
    def data(self) -> QueryData:
        """
        Returns the data returned by the query, which is a list of dictionaries representing the records in rows.

        :return: List of records.
        """
        return self.json.get("data", [])

    @property
    def meta(self) -> list[QueryMetadata]:
        """
        Returns the QueryMetadata from the query, which includes attributes and their types.

        :return: The QueryMetadata
        """
        return self.json.get("meta", [])

    @property
    def rows(self) -> int:
        """
        Returns the number of rows returned by the query.

        :return: The number of rows returned by the query.
        """
        return self.json.get("rows")

    @property
    def statistics(self) -> QueryStatistics:
        """
        Returns the query statistics, which include the number of bytes read, the number of rows read, and the elapsed.
        :return: The QueryStatistics objects.
        """
        return self.json.get("statistics", {})

    @property
    def empty(self) -> bool:
        """
        A property to check if the data in the result is empty.

        This property evaluates whether the "data" field within the "result"
        attribute is empty.

        :return: Returns True if the "data" field in "result" is missing or empty,
            otherwise returns False.
        """
        return not self.json.get("data")


class QueryNdjsonResponse(QueryResponse):
    @property
    def data(self) -> list[dict]:
        """Parses the CSV response body into a list of dictionaries."""
        for line in self.text.splitlines():
            json.loads(line)
        return [json.loads(line) for line in self.text.strip().splitlines()]


class QueryCsvResponse(QueryResponse):
    def __init__(self, response: requests.Response, delimiter: str = ","):
        super().__init__(response)
        self.delimiter = delimiter

    @property
    def data(self) -> list[dict]:
        """Parses the CSV response body into a list of dictionaries."""
        reader = csv.DictReader(
            self.text.splitlines(),
            delimiter=self.delimiter,
        )
        return list(reader)


class QueryApi(Api):
    """
    The Query API allows you to query your Pipes and Data Sources inside Tinybird as if you were running SQL statements
    against a standard database.

    See https://www.tinybird.co/docs/api-reference/query-api.
    """

    endpoint: str = "/v0/sql"

    session: requests.Session

    def __init__(self, token: str, host: str = None):
        super().__init__(token, host)

        self.session = requests.Session()
        if self.token:
            self.session.headers.update({"Authorization": f"Bearer {self.token}"})

    def query(
        self,
        query: str,
        pipeline: str = None,
        parameters: dict[str, str | None] = None,
        output_format_json_quote_64bit_integers: bool = False,
        output_format_json_quote_denormals: bool = False,
        output_format_parquet_string_as_string: bool = False,
        format: QueryOutputFormat = "JSON",
    ) -> QueryResponse | QueryJsonResponse | QueryNdjsonResponse | QueryCsvResponse:
        """
        Executes a SQL query using the engine. As a response, it gives you the query metadata, the resulting data and
        some performance statistics.

        The return type will depend on the desired ``format``. For the following formats, we return special response
        objects that contain the parsed data:
        * ``JSON``: ``QueryJsonResponse`` (default)
        * ``CSVWithNames``: QueryCsvResponse
        * ``TSVWithNames``: QueryCsvResponse
        * ``JSONEachRow``: ``QueryNdjsonResponse``

        For all other formats, we return a generic ``QueryResponse`` object, that allows you to access the raw response
        body via ``response.text`` (str) or ``response.content`` (bytes).

        :param query: The SQL query
        :param pipeline: (Optional) The name of the pipe. It allows writing a query like 'SELECT * FROM _' where '_' is
            a placeholder for the 'pipeline' parameter
        :param parameters: Additional query parameters
        :param output_format_json_quote_64bit_integers: (Optional) Controls quoting of 64-bit or bigger integers (like
            UInt64 or Int128) when they are output in a JSON format. Such integers are enclosed in quotes by default.
            This behavior is compatible with most JavaScript implementations. Possible values: False — Integers are
            output without quotes. True — Integers are enclosed in quotes. Default value is False
        :param output_format_json_quote_denormals: (Optional) Controls representation of inf and nan on the UI instead
            of null e.g when dividing by 0 - inf and when there is no representation of a number in Javascript - nan.
            Default value is false
        :param output_format_parquet_string_as_string: (Optional) Use Parquet String type instead of Binary for String
            columns. Possible values: False - disabled, True - enabled. Default value is False
        :param format: Output format of the query results (defaults to JSON)
        :return: QueryResponse object containing the query results
        """

        query = _sql_with_format(query, format)

        data: dict[str, str | int] = dict(parameters) if parameters else {}
        if query:
            data["q"] = query
        if pipeline:
            data["pipeline"] = pipeline
        if output_format_json_quote_64bit_integers:
            data["output_format_json_quote_64bit_integers"] = 1
        if output_format_json_quote_denormals:
            data["output_format_json_quote_denormals"] = 1
        if output_format_parquet_string_as_string:
            data["output_format_parquet_string_as_string"] = 1

        # if the query is too large, the web server (nginx) will respond with "414 Request-URI Too Large". it seems
        # this limit is around 8kb, so if it's too large, we use a POST request instead.
        qsize = 1  # include the "?" character
        for k, v in data.items():
            if v is None:
                continue
            qsize += len(k) + len(v) + 2  # include the ``&`` and ``=`` character

        if qsize > 8192 or parameters:
            response = self.session.request(
                method="POST",
                url=f"{self.host}{self.endpoint}",
                data=data,
            )
        else:
            response = self.session.request(
                method="GET",
                url=f"{self.host}{self.endpoint}",
                params=data,
            )

        if not response.ok:
            raise ApiError(response)

        # format-specific response objects
        if format == "JSON":
            return QueryJsonResponse(response)
        if format == "CSVWithNames":
            return QueryCsvResponse(response)
        if format == "TSVWithNames":
            return QueryCsvResponse(response, delimiter="\t")
        if format == "JSONEachRow":
            return QueryNdjsonResponse(response)

        return QueryResponse(response)


def _sql_with_format(sql, output_format: Optional[QueryOutputFormat] = None) -> str:
    """
    Returns a formatted SQL query with the given output format. If no output format is specified, the query is
    returned as is.

    :param output_format: The output format to use (suffixes ``FORMAT <format>`` to the query)
    :return: An SQL string
    """
    # TODO: handle potentially already existing FORMAT string
    if not output_format:
        return sql
    return sql + f" FORMAT {output_format}"

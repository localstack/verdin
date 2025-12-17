import enum
import logging
from typing import Any, Optional, TypedDict

import requests

from . import config

LOG = logging.getLogger(__name__)


class OutputFormat(enum.Enum):
    # https://docs.tinybird.co/api-reference/query-api.html#id6
    CSV = "CSV"
    CSVWithNames = "CSVWithNames"
    JSON = "JSON"
    TSV = "TSV"
    TSVWithNames = "TSVWithNames"
    PrettyCompact = "PrettyCompact"
    JSONEachRow = "JSONEachRow"


class QueryMetadata(TypedDict):
    name: str
    type: str


class Statistics(TypedDict):
    elapsed: float
    rows_read: int
    bytes_read: int


JsonData = dict[str, Any]
QueryJsonData = list[dict[str, Any]]


class JsonResult(TypedDict):
    meta: list[QueryMetadata]
    data: QueryJsonData
    rows: int
    statistics: Statistics


class QueryJsonResult:
    response: requests.Response
    result: JsonResult

    def __init__(self, response: requests.Response):
        self.response = response
        self.result = response.json()

    @property
    def empty(self):
        """
        A property to check if the data in the result is empty.

        This property evaluates whether the "data" field within the "result"
        attribute is empty.

        :return: Returns True if the "data" field in "result" is missing or empty,
            otherwise returns False.
        """
        return not self.result.get("data")

    @property
    def meta(self) -> list[QueryMetadata]:
        """
        Returns the QueryMetadata from the query, which includes attributes and their types.

        :return: The QueryMetadata
        """
        return self.result.get("meta")

    @property
    def data(self) -> QueryJsonData:
        """
        Returns the data from the query, which is a list of dictionaries representing the rows of the query result.

        :return: The QueryJsonData
        """
        return self.result.get("data")


class QueryError(Exception):
    def __init__(self, response: requests.Response) -> None:
        self.response = response
        msg = response.text
        try:
            doc = response.json()
            if doc["error"]:
                msg = doc["error"]
        except Exception:
            pass
        super().__init__(f"{response.status_code}: {msg}")


class SqlQuery:
    """
    Tinybird SQL Query. https://docs.tinybird.co/api-reference/query-api.html#get--v0-sql
    """

    endpoint: str = "/v0/sql"

    sql: str
    format: Optional[OutputFormat]

    def __init__(
        self, sql: str, token, format: Optional[OutputFormat] = None, api: str = None
    ) -> None:
        self.sql = sql
        self.format = format or OutputFormat.JSON
        self.token = token
        self.api = (api or config.API_URL).rstrip("/") + self.endpoint

    def get(self, format: Optional[OutputFormat] = None) -> requests.Response:
        """
        Runs the query and returns the response.

        TODO: replicate tinybird API concepts instead of returning Response

        :param format: Overwrite the default output format set in the constructor.
        :return: the HTTP response
        """
        query = {"q": self._sql_with_format(format or self.format)}

        headers = {"Content-Type": "text/html; charset=utf-8"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        LOG.debug(
            "querying %s with query: %s",
            self.api,
            query,
        )
        response = requests.get(url=self.api, params=query, headers=headers)

        if not response.ok:
            raise QueryError(response)

        return response

    def json(self) -> QueryJsonResult:
        """
        Runs the query and returns the result in JSON output format.

        :return: A QueryJsonResult containing the result of the query.
        """
        response = self.get(OutputFormat.JSON)

        return QueryJsonResult(response)

    def _sql_with_format(self, output_format: Optional[OutputFormat] = None) -> str:
        """
        Returns a formatted SQL query with the given output format. If no output format is specified, the query is
        returned as is.

        :param output_format: The output format to use (suffixes ``FORMAT <format>`` to the query)
        :return: An SQL string
        """
        # TODO: handle potentially already existing FORMAT string
        if not output_format:
            return self.sql
        return self.sql + f" FORMAT {output_format.value}"

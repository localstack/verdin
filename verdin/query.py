import enum
import logging
from typing import Any, Optional, TypedDict

import requests

from . import config
from .api import ApiError
from .api.query import QueryApi

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
        host = (api or config.API_URL).rstrip("/")
        self.api = host + self.endpoint
        self._query_api = QueryApi(token=token, host=host)

    def get(self, format: Optional[OutputFormat] = None) -> requests.Response:
        """
        Runs the query and returns the response.

        TODO: replicate tinybird API concepts instead of returning Response

        :param format: Overwrite the default output format set in the constructor.
        :return: the HTTP response
        """

        LOG.debug(
            "querying %s with query: %s",
            self.api,
            self.sql,
        )

        try:
            response = self._query_api.query(
                self.sql,
                format=(format or self.format).value,
            )
            return response._response
        except ApiError as e:
            raise QueryError(response=e._response) from e

    def json(self) -> QueryJsonResult:
        """
        Runs the query and returns the result in JSON output format.

        :return: A QueryJsonResult containing the result of the query.
        """
        response = self.get(OutputFormat.JSON)

        return QueryJsonResult(response)

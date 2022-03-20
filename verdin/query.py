import enum
import logging
from typing import Any, Dict, List, Optional, TypedDict

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


JsonData = Dict[str, Any]


class JsonResult(TypedDict):
    meta: List[QueryMetadata]
    data: List[JsonData]
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
        return not self.result.get("data")

    @property
    def meta(self) -> List[QueryMetadata]:
        return self.result.get("meta")

    @property
    def data(self) -> List[JsonData]:
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

    def __init__(self, sql: str, token, format: Optional[OutputFormat] = None, api=None) -> None:
        self.sql = sql
        self.format = format or OutputFormat.JSON
        self.token = token
        self.api = (api or config.API_URL).rstrip("/") + self.endpoint

    def get(self, format: Optional[OutputFormat] = None):
        # TODO: replicate tinybird API concepts instead of returning Response
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
        response = self.get(OutputFormat.JSON)

        return QueryJsonResult(response)

    def _sql_with_format(self, output_format: Optional[OutputFormat] = None):
        # TODO: handle potentially already existing FORMAT string
        if not output_format:
            return self.sql
        return self.sql + f" FORMAT {output_format.value}"

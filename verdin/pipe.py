import logging
from typing import Any, Iterator, Optional

import requests

from . import config
from .api import ApiError
from .api.pipes import PipesApi

LOG = logging.getLogger(__name__)

PipeMetadata = list[tuple[str, str]]
PipeJsonData = list[dict[str, Any]]


class PipeError(Exception):
    """
    Wrapper of the HTTP response returned by a Pipe query if the HTTP response is not a 2XX code.
    """

    response: requests.Response

    def __init__(self, response: requests.Response) -> None:
        self.response = response
        self.json: dict = response.json()
        super().__init__(self.description)

    @property
    def description(self) -> str:
        return self.json.get("error")


class PipeJsonResponse:
    """
    Wrapper of the HTTP response returned by a Pipe query.
    """

    response: requests.Response
    result: dict

    def __init__(self, response: requests.Response):
        self.response = response
        self.result = response.json()

    @property
    def empty(self) -> bool:
        """
        A property to check if the data in the result is empty.

        This property evaluates whether the "data" field within the "result"
        attribute is empty.

        :return: Returns True if the "data" field in "result" is missing or empty,
            otherwise returns False.
        """
        return not self.result.get("data")

    @property
    def meta(self) -> PipeMetadata:
        """
        Returns the PipeMetadata from the query, which includes attributes and their types.

        :return: The PipeMetadata
        """
        return [(t["name"], t["type"]) for t in self.result.get("meta", [])]

    @property
    def data(self) -> PipeJsonData:
        """
        Returns the data from the query, which is a list of dictionaries representing the rows of the query result.

        :return: The PipeJsonData
        """
        return self.result.get("data")


PipePageIterator = Iterator[PipeJsonResponse]


class PagedPipeQuery(PipePageIterator):
    # TODO: allow passing of custom parameters

    pipe: "Pipe"

    def __init__(self, pipe: "Pipe", page_size: int = 50, start_at: int = 0):
        self.pipe = pipe
        self.limit = page_size
        self.offset = start_at

    def __iter__(self):
        return self

    def __next__(self):
        sql = f"SELECT * FROM _ LIMIT {self.limit} OFFSET {self.offset}"
        response = self.pipe.sql(sql)
        if response.empty:
            raise StopIteration()
        self.offset += self.limit
        return response


class Pipe:
    """
    Model abstraction of a tinybird Pipe.

    TODO: implement csv mode
    """

    endpoint: str = "/v0/pipes"

    name: str
    version: Optional[int]
    resource: str

    def __init__(self, name, token, version: int = None, api: str = None) -> None:
        super().__init__()
        self.name = name
        self.token = token
        self.version = version
        self.resource = (api or config.API_URL).rstrip("/") + self.endpoint

        self._pipes_api = PipesApi(token, host=(api or config.API_URL).rstrip("/"))

    @property
    def canonical_name(self) -> str:
        """
        Returns the name of the pipe that can be queried. If a version is specified, the name will be suffixed with
        ``__v<version>``. Otherwise, this just returns the name. Note that versions are discouraged in the current
        tinybird workflows.

        :return: The canonical name of the pipe that can be used in queries
        """
        if self.version is not None:
            return f"{self.name}__v{self.version}"
        else:
            return self.name

    @property
    def pipe_url(self) -> str:
        """
        Returns the API URL of this pipe. It's something like ``https://api.tinybird.co/v0/pipes/my_pipe.json``.

        :return: The Pipe API URL
        """
        return self.resource + "/" + self.canonical_name + ".json"

    def query(self, params: dict[str, str] = None) -> PipeJsonResponse:
        """
        Query the pipe endpoint using the given dynamic parameters. Note that the pipe needs to be exposed as an
        endpoint.

        See: https://www.tinybird.co/docs/forward/work-with-data/query-parameters#use-pipes-api-endpoints-with-dynamic-parameters

        :param params: The dynamic parameters of the pipe and the values for your query
        :return: a PipeJsonResponse containing the result of the query
        """
        try:
            response = self._pipes_api.query(
                self.canonical_name,
                parameters=params,
                format="json",
            )
            return PipeJsonResponse(response._response)
        except ApiError as e:
            raise PipeError(e._response)

    def pages(self, page_size: int = 50, start_at: int = 0) -> PipePageIterator:
        """
        Returns an iterator over the pipe's data pages. Each page contains ``page_size`` records.

        TODO: currently we don't support dynamic parameters with paged queries

        :param page_size: The size of each page (default 50)
        :param start_at: The offset at which to start (default 0)
        :return:
        """
        return PagedPipeQuery(pipe=self, page_size=page_size, start_at=start_at)

    def sql(self, query: str) -> PipeJsonResponse:
        """
        Run an SQL query against the pipe. For example:

            pipe.sql("select count() from _")

        See https://docs.tinybird.co/api-reference/query-api.html

        :param query: The SQL query to run
        :return: The result of the query
        """
        try:
            response = self._pipes_api.query(self.canonical_name, query=query, format="json")
            return PipeJsonResponse(response._response)
        except ApiError as e:
            raise PipeError(e._response)

    def __str__(self):
        return f"Pipe({self.canonical_name})"

    def __repr__(self):
        return self.__str__()

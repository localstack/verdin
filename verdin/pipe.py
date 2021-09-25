import logging
from typing import Any, Dict, List, Optional, Tuple

import requests

from . import config

LOG = logging.getLogger(__name__)

PipeMetadata = List[Tuple[str, str]]
PipeJsonData = List[Dict[str, Any]]


class PipeError(Exception):
    response: requests.Response

    def __init__(self, response) -> None:
        self.response = response
        self.json: Dict = response.json()
        super().__init__(self.description)

    @property
    def description(self):
        return self.json.get("error")


class PipeJsonResponse:
    response: requests.Response
    result: Dict

    def __init__(self, response):
        self.response = response
        self.result = response.json()

    @property
    def empty(self):
        return not self.result.get("data")

    @property
    def meta(self) -> PipeMetadata:
        return [(t["name"], t["type"]) for t in self.result.get("meta", [])]

    @property
    def data(self) -> PipeJsonData:
        return self.result.get("data")


class Pipe:
    """
    Model abstraction of a tinybird Pipe.

    TODO: implement csv mode
    """

    endpoint: str = "/v0/pipes"

    name: str
    version: Optional[int]
    resource: str

    def __init__(self, name, token, version: int = None, api=None) -> None:
        super().__init__()
        self.name = name
        self.token = token
        self.version = version
        self.resource = (api or config.API_URL).rstrip("/") + self.endpoint

    @property
    def canonical_name(self):
        if self.version is not None:
            return f"{self.name}__v{self.version}"
        else:
            return self.name

    @property
    def pipe_url(self):
        return self.resource + "/" + self.canonical_name + ".json"

    def query(self, params=None) -> PipeJsonResponse:
        params = params or dict()
        if "token" not in params and self.token:
            params["token"] = self.token

        response = requests.get(self.pipe_url, params=params)

        if response.ok:
            return PipeJsonResponse(response)
        else:
            raise PipeError(response)

    def sql(self, query: str) -> PipeJsonResponse:
        """
        Run an SQL query against the pipe. For example:

            pipe.sql("select count() from _")

        See https://docs.tinybird.co/api-reference/query-api.html
        """
        headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
        params = {"q": query}

        response = requests.get(self.pipe_url, headers=headers, params=params)

        if response.ok:
            return PipeJsonResponse(response)
        else:
            raise PipeError(response)

    def __str__(self):
        return f"Pipe({self.canonical_name})"

    def __repr__(self):
        return self.__str__()

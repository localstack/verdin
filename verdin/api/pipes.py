import csv
import json
from typing import TypedDict, Literal

import requests

from .base import Api, ApiResponse, ApiError

PipeOutputFormat = Literal["csv", "json", "ndjson", "parquet", "prometheus"]


class PipeNotFoundError(ApiError):
    """
    Specific ApiError representing a 404 Not Found when pipe names are given.
    """


class PipeNode(TypedDict):
    id: str
    name: str
    sql: str
    deployment_suffix: str | None
    description: str | None
    materialized: bool | None
    cluster: str | None
    tags: dict
    created_at: str
    updated_at: str
    version: int
    project: str | None
    result: str | None
    ignore_sql_errors: bool
    node_type: str
    dependencies: list[str] | None
    params: list | None


class PipeListInfo(TypedDict):
    id: str
    name: str
    description: str
    endpoint: str
    created_at: str
    updated_at: str
    parent: str | None
    nodes: list[PipeNode]
    url: str


class PipeInfo(TypedDict):
    """
    A document returned by the pipe information endpoint. Example::

    {
      "content": "VERSION 0\n\nDESCRIPTION >\n    Endpoint to select unique ...",
      "created_at": "2025-12-17 13:18:09.799374",
      "description": "Endpoint to select unique key/value pairs from simple",
      "edited_by": null,
      "endpoint": "t_54dffae578ef47238fd51e9849f79a1f",
      "id": "t_c50152ced57b46b99acf14930b9c6906",
      "last_commit": {
        "content_sha": "",
        "path": "",
        "status": "None"
      },
      "name": "simple_kv",
      "nodes": [
        {
          "cluster": null,
          "created_at": "2025-12-17 13:18:09.799385",
          "dependencies": [
            "simple"
          ],
          "deployment_suffix": "",
          "description": null,
          "id": "t_54dffae578ef47238fd51e9849f79a1f",
          "ignore_sql_errors": false,
          "materialized": null,
          "name": "endpoint",
          "node_type": "endpoint",
          "params": [],
          "project": null,
          "result": null,
          "sql": "%\n    SELECT key, value\n    FROM simple\n    ...",
          "tags": {},
          "updated_at": "2025-12-17 13:18:09.799385",
          "version": 0
        }
      ],
      "parent": null,
      "path": "endpoints/simple_kv.pipe",
      "type": "endpoint",
      "updated_at": "2025-12-17 13:18:09.799394",
      "url": "http://localhost:8001/v0/pipes/simple_kv.json",
      "workspace_id": "2244743a-d384-478f-a9f5-ea4848c56427"
    }
    """

    content: str
    created_at: str
    description: str
    edited_by: str | None
    endpoint: str
    id: str
    last_commit: dict
    name: str
    nodes: list[PipeNode]
    parent: str | None
    path: str
    type: str
    updated_at: str
    url: str
    workspace_id: str


class ListPipesResponse(ApiResponse):
    @property
    def pipes(self) -> list[PipeListInfo]:
        return self.json.get("pipes", [])


class GetPipeInformationResponse(ApiResponse):
    @property
    def info(self) -> PipeInfo:
        return self.json


class QueryPipeResponse(ApiResponse):
    @property
    def data(self) -> list[dict]:
        raise NotImplementedError


class QueryPipeJsonResponse(QueryPipeResponse):
    @property
    def data(self) -> list[dict]:
        return self.json.get("data", [])

    @property
    def meta(self) -> list[dict]:
        return self.json.get("meta", [])

    @property
    def rows(self) -> int:
        return self.json.get("rows")

    @property
    def statistics(self) -> dict:
        return self.json.get("statistics", {})


class QueryPipeNdjsonResponse(QueryPipeResponse):
    @property
    def data(self) -> list[dict]:
        """Parses the CSV response body into a list of dictionaries."""
        for line in self.text.splitlines():
            print(line)
            json.loads(line)
        return [json.loads(line) for line in self.text.strip().splitlines()]


class QueryPipeCsvResponse(QueryPipeResponse):
    @property
    def data(self) -> list[dict]:
        """Parses the CSV response body into a list of dictionaries."""
        reader = csv.DictReader(self.text.splitlines())
        return list(reader)


class PipesApi(Api):
    """
    Pipes API. See https://www.tinybird.co/docs/api-reference/pipe-api

    TODO: missing APIs to implement
        * Creating a new pipe (POST /v0/pipes)
        * Append a node to a pipe (POST /v0/pipes/:name/nodes)
        * Delete a node from a pipe (DELETE /v0/pipes/:name/nodes/:node_id)
        * Update a node in a pipe (PUT /v0/pipes/:name/nodes/:node_id)
        * Delete a pipe (DELETE /v0/pipes/:name)
        * Change a pipe's metadata (PUT /v0/pipes/:name)
        * Explain a pipe (GET /v0/pipes/:name/explain)
    """

    endpoint: str = "/v0/pipes"

    session: requests.Session

    def __init__(self, token: str, host: str = None):
        super().__init__(token, host)

        self.session = requests.Session()
        if self.token:
            self.session.headers.update({"Authorization": f"Bearer {self.token}"})

    def list(
        self,
        dependencies: bool = False,
        attrs: list[str] = None,
        node_attrs: list[str] = None,
    ) -> ListPipesResponse:
        """
        Get a list of pipes in your account. Makes a GET request to ``/v0/pipes`` endpoint, which returns a list of
        pipes.

        :param dependencies: Include dependent data sources and pipes, default is false
        :param attrs: List of pipe attributes to return (e.g. '["name","description"]')
        :param node_attrs: List of node attributes to return (e.g. '["id","name"]')
        :return: A ``ListPipesResponse`` object
        """
        params = {}
        if dependencies:
            params["dependencies"] = "true"
        if attrs:
            params["attrs"] = ",".join(attrs)
        if node_attrs:
            params["node_attrs"] = ",".join(node_attrs)

        response = self.session.request(
            method="GET",
            url=f"{self.host}{self.endpoint}",
            params=params,
        )

        if not response.ok:
            raise ApiError(response)

        return ListPipesResponse(response)

    def query(
        self,
        name: str,
        query: str = None,
        parameters: dict[str, str | None] = None,
        format: PipeOutputFormat = "json",
    ) -> QueryPipeResponse | QueryPipeJsonResponse | QueryPipeNdjsonResponse | QueryPipeCsvResponse:
        """
        Query the Pipe. Makes a GET request to ``/v0/pipes/<name>.<format>`` endpoint, which returns the query result.
        The return value depends on the format parameter. Currently, parquet and prometheus formats are only supported
        as raw outputs. For all others you can call ``response.data`` and receive a list of dictionary records.

        When using an additional SQL query (through the ``query`` parameter) for the Pipe, you can use the
        ``_`` shortcut, which refers to your Pipe name. You can pass both ``parameters`` and ``query``.

        :param name: The name of the pipe to query.
        :param query: Optional query to execute against the pipe.
        :param parameters: The dynamic parameters passed to the pipe.
        :param format: The output format (default: json).
        :return: A ``QueryPipeResponse`` object that is specific to the output format.
        """

        params = dict(parameters) if parameters else {}
        if query:
            params["q"] = query

        # if the query is too large, the web server (nginx) will respond with "414 Request-URI Too Large". it seems
        # this limit is around 8kb, so if it's too large, we use a POST request instead.
        qsize = 1  # include the "?" character
        for k, v in params.items():
            if v is None:
                continue
            qsize += len(k) + len(v) + 2  # include the ``&`` and ``=`` character

        if qsize > 8192:
            response = self.session.request(
                method="POST",
                url=f"{self.host}{self.endpoint}/{name}.{format}",
                data=params,
            )
        else:
            response = self.session.request(
                method="GET",
                url=f"{self.host}{self.endpoint}/{name}.{format}",
                params=params,
            )

        if response.status_code == 404:
            raise PipeNotFoundError(response)

        if not response.ok:
            raise ApiError(response)

        # format-specific response objects
        if format == "json":
            return QueryPipeJsonResponse(response)
        if format == "ndjson":
            return QueryPipeNdjsonResponse(response)
        if format == "csv":
            return QueryPipeCsvResponse(response)

        # prometheus and parquet formats are currently only supported as raw outputs

        return QueryPipeResponse(response)

    def get_information(self, name: str) -> GetPipeInformationResponse:
        """
        Makes a GET request to ``/v0/pipes/<name>`` endpoint, which returns the pipe information.
        See: https://www.tinybird.co/docs/api-reference/pipe-api#get--v0-pipes-(.+\.pipe)

        :param name: The name or ID of the pipe.
        :return: A ``GetPipeInformationResponse`` object
        """
        response = self.session.request(
            method="GET",
            url=f"{self.host}{self.endpoint}/{name}",
        )

        if response.status_code == 404:
            raise PipeNotFoundError(response)

        if not response.ok:
            raise ApiError(response)

        return GetPipeInformationResponse(response)

from typing import TypedDict

import requests

from verdin.api import ApiResponse
from verdin.api.base import Api, ApiError


class TokenNotFoundError(ApiError):
    """Specific ApiError representing a 404 Not Found when token names are given."""


class Scope(TypedDict):
    type: str
    resource: str | None
    filter: str | None


class TokenInfo(TypedDict):
    id: str
    token: str
    scopes: list[Scope]
    name: str
    description: str | None
    origin: dict | None
    host: str
    is_internal: bool


class ListTokensResponse(ApiResponse):
    @property
    def tokens(self) -> list[TokenInfo]:
        return self.json.get("tokens", [])


class GetTokenInfoResponse(ApiResponse):
    @property
    def info(self) -> TokenInfo:
        return self.json


class TokensApi(Api):
    """
    Tokens API client.

    TODO: The following APIs are not yet implemented (note that some workspaces only allow resource modification
     through deployments anyway)
     - Create a new Token: Static or JWT (POST /v0/tokens)
     - Refresh a static token (POST /v0/tokens/:name/refresh)
     - Delete a Token (DELETE /v0/tokens/:name)
     - Modify a Token (PUT /v0/tokens/:name)
    """

    endpoint: str = "/v0/tokens"

    session: requests.Session

    def __init__(self, token: str, host: str = None):
        super().__init__(token, host)

        self.session = requests.Session()
        if self.token:
            self.session.headers.update({"Authorization": f"Bearer {self.token}"})

    def get_information(self, token: str):
        """
        Fetches information about a particular Static Token. Makes a GET request to ``/v0/tokens/:name``. If the token
        doesn't exist, a 403 may be returned ("Not enough permissions to get information about this token").

        :param token: The token identifier.
        :return: A ``GetTokenInfoResponse`` object.
        """
        response = self.session.request(
            method="GET",
            url=f"{self.host}{self.endpoint}/{token}",
        )

        if not response.ok:
            raise ApiError(response)

        return GetTokenInfoResponse(response)

    def list(self) -> ListTokensResponse:
        """
        Retrieves all workspace Static Tokens. Makes a GET request to ``/v0/tokens``.

        :return: A ``ListTokensResponse`` object.
        """
        response = self.session.request(
            method="GET",
            url=f"{self.host}{self.endpoint}",
        )

        if not response.ok:
            raise ApiError(response)

        return ListTokensResponse(response)

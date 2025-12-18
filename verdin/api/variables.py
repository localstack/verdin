from typing import TypedDict, Literal

import requests

from verdin.api.base import Api, ApiError, ApiResponse


class VariableNotFoundError(ApiError):
    """
    Specific ApiError representing a 404 Not Found when variable names are given.
    """


class VariableInfo(TypedDict):
    """
    A variable info object. Example::

        {
            "name": "test_password",
            "type": "secret",
            "created_at": "2024-06-21T10:27:57",
            "updated_at": "2024-06-21T10:27:57",
            "edited_by": "token: 'admin token'"
        }
    """

    name: str
    type: str
    created_at: str
    updated_at: str
    edited_by: str


class CreateVariableResponse(ApiResponse):
    @property
    def variable(self) -> VariableInfo:
        """
        Returns the variable information.
        """
        return self.json


class UpdateVariableResponse(ApiResponse):
    @property
    def variable(self) -> VariableInfo:
        """
        Returns the variable information.
        """
        return self.json


class DeleteVariableResponse(ApiResponse):
    @property
    def ok(self) -> bool:
        """
        Returns whether the operation was successful.
        """
        return self.json.get("ok", False)


class ListVariablesResponse(ApiResponse):
    @property
    def variables(self) -> list[VariableInfo]:
        """
        Returns the list of variables.
        """
        return self.json.get("variables", [])


class GetVariableResponse(ApiResponse):
    @property
    def variable(self) -> VariableInfo:
        """
        Returns the variable information.
        """
        return self.json


class VariablesApi(Api):
    """
    ``/v0/variables`` API client.

    This API allows you to create, update, delete, and list environment variables
    that can be used in Pipes in a Workspace.
    """

    endpoint: str = "/v0/variables"

    session: requests.Session

    def __init__(self, token: str, host: str = None):
        super().__init__(token, host)

        self.session = requests.Session()
        if self.token:
            self.session.headers.update({"Authorization": f"Bearer {self.token}"})

    def create(
        self,
        name: str,
        value: str,
        type: Literal["secret"] = "secret",
    ) -> CreateVariableResponse:
        """
        Creates a new environment variable.

        :param name: The name of the variable.
        :param value: The value of the variable.
        :param type: The type of the variable. Defaults to 'secret'.
        :return: A ``CreateVariableResponse`` object.
        """
        data = {
            "name": name,
            "value": value,
            "type": type,
        }

        response = self.session.request(
            method="POST",
            url=f"{self.host}{self.endpoint}",
            data=data,
        )

        if not response.ok:
            raise ApiError(response)

        return CreateVariableResponse(response)

    def delete(self, name: str) -> DeleteVariableResponse:
        """
        Deletes an environment variable.

        :param name: The name of the variable to delete.
        :return: A ``DeleteVariableResponse`` object.
        """
        response = self.session.request(
            method="DELETE",
            url=f"{self.host}{self.endpoint}/{name}",
        )

        if response.status_code == 404:
            raise VariableNotFoundError(response)

        if not response.ok:
            raise ApiError(response)

        return DeleteVariableResponse(response)

    def update(
        self,
        name: str,
        value: str,
    ) -> UpdateVariableResponse:
        """
        Updates an environment variable.

        :param name: The name of the variable to update.
        :param value: The new value of the variable.
        :return: A ``UpdateVariableResponse`` object.
        """
        data = {
            "value": value,
        }

        response = self.session.request(
            method="PUT",
            url=f"{self.host}{self.endpoint}/{name}",
            data=data,
        )

        if response.status_code == 404:
            raise VariableNotFoundError(response)

        if not response.ok:
            raise ApiError(response)

        return UpdateVariableResponse(response)

    def list(self) -> ListVariablesResponse:
        """
        Lists all environment variables.

        :return: A ``ListVariablesResponse`` object.
        """
        response = self.session.request(
            method="GET",
            url=f"{self.host}{self.endpoint}",
        )

        if not response.ok:
            raise ApiError(response)

        return ListVariablesResponse(response)

    def get(self, name: str) -> GetVariableResponse:
        """
        Gets information about a specific environment variable.

        :param name: The name of the variable to get.
        :return: A ``GetVariableResponse`` object.
        """
        response = self.session.request(
            method="GET",
            url=f"{self.host}{self.endpoint}/{name}",
        )

        if response.status_code == 404:
            raise VariableNotFoundError(response)

        if not response.ok:
            raise ApiError(response)

        return GetVariableResponse(response)

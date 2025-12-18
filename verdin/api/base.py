import requests


class Api:
    DEFAULT_HOST = "https://api.tinybird.co"

    host: str
    token: str

    def __init__(self, token: str, host: str = None):
        self.token = token
        self.host = host or Api.DEFAULT_HOST


class ApiResponse:
    _response: requests.Response

    def __init__(self, response: requests.Response):
        self._response = response
        self._json: dict | None = None  # cache for json response

    @property
    def text(self) -> str:
        """
        Returns the body of the HTTP response as a string.

        :return: The response body as a string.
        """
        return self._response.text

    @property
    def content(self) -> bytes:
        """
        Returns the body of the HTTP response as bytes.

        :return: The response body as a bytes.
        """
        return self._response.content

    @property
    def json(self) -> dict:
        """
        Parses the JSON response and returns a dictionary. It caches the result so that later calls to this method
        do not parse the response every time.

        :return: The parsed JSON response.
        """
        if self._json:
            return self._json

        self._json = self._response.json()
        return self._json


class ApiError(Exception):
    """
    Exception that represents a non-200 HTTP response from the API.
    """

    _response: requests.Response

    def __init__(self, response: requests.Response):
        self._response = response
        super().__init__(self._render_message(response))

    def _render_message(self, response: requests.Response) -> str:
        error = None
        documentation = None

        if response.headers.get("Content-Type").startswith("application/json"):
            doc = response.json()
            error = doc.get("error")
            documentation = doc.get("documentation")

        if not error:
            error = response.text

        error.rstrip(".")

        message = f"API Error ({response.status_code}): {error}."

        if documentation:
            message += f" Documentation: {documentation}"

        return message

    @property
    def status_code(self) -> int:
        return self._response.status_code

    @property
    def text(self) -> str:
        return self._response.text

    @property
    def json(self) -> str:
        return self._response.json()

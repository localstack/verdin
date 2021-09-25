from . import config
from .datasource import Datasource
from .pipe import Pipe


class Client:
    """
    Tinybird HTTP client that holds the access token and provides factory methods for resources.
    """

    def __init__(self, token: str, api: str = None):
        self.api = (api or config.API_URL).lstrip("/")
        self.token = token

    def pipe(self, name: str, version: int = None) -> Pipe:
        """
        Create an object representing a pipe with the given name, e.g.,
        "localstack_dashboard_events.json"
        """
        return Pipe(name, token=self.token, version=version, api=self.api)

    def datasource(self, name: str, version: int = None) -> Datasource:
        """
        Create an object representing a datasource with a given name.
        """
        return Datasource(name, token=self.token, version=version, api=self.api)

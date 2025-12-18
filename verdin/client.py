from typing import Optional

from . import config
from .api.apis import Apis
from .datasource import Datasource
from .pipe import Pipe
from .query import OutputFormat, SqlQuery


class Client:
    """
    Tinybird HTTP client that holds the access token and provides factory methods for resources.
    """

    def __init__(self, token: str, api: str = None):
        self.host = (api or config.API_URL).lstrip("/")
        self.token = token
        self._api = Apis(self.token, self.host)

    @property
    def api(self) -> Apis:
        """
        Returns an ``Apis`` object that gives you access to the tinybird API objects.
        :return: An ``Apis`` object
        """
        return self._api

    def pipe(self, name: str, version: int = None) -> Pipe:
        """
        Create an object representing a pipe with the given name, e.g.,
        "localstack_dashboard_events.json"
        """
        return Pipe(name, token=self.token, version=version, api=self.host)

    def datasource(self, name: str, version: int = None) -> Datasource:
        """
        Create an object representing a datasource with a given name.
        """
        return Datasource(name, token=self.token, version=version, api=self.host)

    def sql(self, sql: str, format: Optional[OutputFormat] = None) -> SqlQuery:
        return SqlQuery(sql, format=format, token=self.token, api=self.host)

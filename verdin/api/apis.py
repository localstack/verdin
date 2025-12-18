from .datasources import DataSourcesApi
from .events import EventsApi
from .pipes import PipesApi
from .query import QueryApi
from .tokens import TokensApi
from .variables import VariablesApi


class Apis:
    """
    Factory for Api objects.
    """

    _token: str
    _host: str | None

    def __init__(self, token: str, host: str = None):
        self._token = token
        self._host = host

    @property
    def datasources(self) -> DataSourcesApi:
        return DataSourcesApi(self._token, self._host)

    @property
    def events(self) -> EventsApi:
        return EventsApi(self._token, self._host)

    @property
    def pipes(self) -> PipesApi:
        return PipesApi(self._token, self._host)

    @property
    def query(self) -> QueryApi:
        return QueryApi(self._token, self._host)

    @property
    def tokens(self) -> TokensApi:
        return TokensApi(self._token, self._host)

    @property
    def variables(self) -> VariablesApi:
        return VariablesApi(self._token, self._host)

from .datasources import DataSourcesApi
from .events import EventsApi


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
    def events(self) -> EventsApi:
        return EventsApi(self._token, self._host)

    @property
    def datasources(self) -> DataSourcesApi:
        return DataSourcesApi(self._token, self._host)

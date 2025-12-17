import gzip
import json
import logging

import requests

from .base import Api, ApiError, ApiResponse

LOG = logging.getLogger(__name__)


class EventsResponse(ApiResponse):
    @property
    def successful_rows(self) -> int:
        return self.json.get("successful_rows")

    @property
    def quarantined_rows(self) -> int:
        return self.json.get("quarantined_rows")


class EventsApi(Api):
    endpoint: str = "/v0/events"

    session: requests.Session

    def __init__(self, token: str, host: str = None):
        super().__init__(token, host)

        self.session = requests.Session()
        if self.token:
            self.session.headers.update({"Authorization": f"Bearer {self.token}"})

    def send(
        self,
        name: str,
        records: list[dict],
        wait: bool = False,
        json_encoder: type = None,
        compress: bool = False,
    ) -> EventsResponse:
        """
        Makes a POST request to ``/v0/events?name=<name>`` with NDJSON encoded data.

        :param name: Name or ID of the target Data Source to append data to it
        :param records: List of JSON records to append. Records will be converted to NDJSON using ``json.dumps``
        :param wait: 'false' by default. Set to 'true' to wait until the write is acknowledged by the database.
            Enabling this flag makes it possible to retry on database errors, but it introduces additional latency.
            It's recommended to enable it in use cases in which data loss avoidance is critical. Disable it otherwise.
        :param json_encoder: The JSON Encoder class passed to ``json.dumps``. Defaults to ``json.JSONEncoder``.
        :param compress: Whether to compress the data using gzip. Defaults to False.

        :return: The EventsResponse
        :raises ApiError: If the request failed
        """
        url = f"{self.host}{self.endpoint}"

        docs = [json.dumps(doc, cls=json_encoder) for doc in records]
        data = "\n".join(docs)

        params = {"name": name}
        if wait:
            params["wait"] = "true"

        LOG.debug("sending %d ndjson records to %s via %s", len(records), name, url)

        headers = {"Content-Type": "application/x-ndjson"}

        if compress:
            headers["Content-Encoding"] = "gzip"
            data = gzip.compress(data.encode("utf-8"))

        response = self.session.post(
            url=url,
            params=params,
            headers=headers,
            data=data,
        )

        if not response.ok:
            raise ApiError(response)

        return EventsResponse(response)

import csv
import io
import logging
import os
from typing import Any, List, Optional, Tuple, Union

import requests

from . import config

LOG = logging.getLogger(__name__)

Record = Union[Tuple, List[Any]]
Records = List[Record]


def to_csv(records: Records) -> str:
    output = io.StringIO()
    write_csv(output, records)
    return output.getvalue()


def write_csv(file, records: Records):
    # TODO: do proper type conversion here to optimize for CSV input
    #  see: https://guides.tinybird.co/guide/fine-tuning-csvs-for-fast-ingestion

    writer = csv.writer(file, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")

    for record in records:
        writer.writerow(record)


class Datasource:
    """
    Abstract tinybird datasource.
    """

    endpoint: str = "/v0/datasources"

    name: str
    version: Optional[int]

    def __init__(self, name, token, version: int = None, api=None) -> None:
        self.name = name
        self.token = token
        self.version = version
        self.api = (api or config.API_URL).rstrip("/") + self.endpoint

    @property
    def canonical_name(self):
        if self.version is not None:
            return f"{self.name}__v{self.version}"
        else:
            return self.name

    def append(self, records: List[Record]) -> requests.Response:
        # TODO: replicate tinybird API concepts instead of returning Response
        query = {"name": self.canonical_name, "mode": "append"}

        headers = {"Content-Type": "text/html; charset=utf-8"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        data = self.to_csv(records)

        LOG.debug(
            "appending %d records to %s via %s(%s)",
            len(records),
            self,
            self.api,
            query,
        )
        # TODO: use multipart
        return requests.post(url=self.api, params=query, headers=headers, data=data)

    @staticmethod
    def to_csv(records: List[List[Any]]):
        return to_csv(records)

    def __str__(self):
        return f"Datasource({self.canonical_name})"

    def __repr__(self):
        return self.__str__()


class FileDatasource(Datasource):
    # for debugging/development purposes

    def __init__(self, path: str):
        name = os.path.basename(path)
        super().__init__(name, None)
        self.path = path

    def append(self, records: Records) -> requests.Response:
        if records:
            with open(self.path, "a") as fd:
                write_csv(fd, records)

        response = requests.Response()
        response.status_code = 200
        return response

    def readlines(self) -> List[str]:
        with open(self.path, "r") as fd:
            return fd.readlines()

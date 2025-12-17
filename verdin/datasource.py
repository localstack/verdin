import csv
import io
import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple, Union, TYPE_CHECKING

import requests

from . import config

if TYPE_CHECKING:
    from _typeshed import SupportsWrite

LOG = logging.getLogger(__name__)

Record = Union[Tuple, List[Any]]
Records = List[Record]


def to_csv(records: Records, **kwargs) -> str:
    """
    Convert the given records to CSV using a CSV writer, and return them as a single string.

    :param records: The records to convert to CSV.
    :param kwargs: Args to be passed to ``csv.writer``.
    :return: A string representing the CSV
    """
    output = io.StringIO()
    write_csv(output, records, **kwargs)
    return output.getvalue()


def write_csv(file: "SupportsWrite[str]", records: Records, **kwargs):
    """
    Converts the given records to CSV and writes them to the given file.

    :param file: The file passed to the CSV writer.
    :param records: The records to convert to CSV.
    :param kwargs: Args to be passed to ``csv.writer``.
    """
    # TODO: do proper type conversion here to optimize for CSV input
    #  see: https://guides.tinybird.co/guide/fine-tuning-csvs-for-fast-ingestion

    if "delimiter" in kwargs:
        if kwargs["delimiter"] is None:
            del kwargs["delimiter"]

    writer = csv.writer(file, quoting=csv.QUOTE_MINIMAL, lineterminator="\n", **kwargs)

    for record in records:
        writer.writerow(record)


class Datasource:
    """
    Abstract tinybird datasource.
    """

    endpoint: str = "/v0/datasources"

    name: str
    version: Optional[int]

    def __init__(self, name, token, version: int = None, api: str = None) -> None:
        self.name = name
        self.token = token
        self.version = version
        self.api = (api or config.API_URL).rstrip("/") + self.endpoint

    @property
    def canonical_name(self) -> str:
        """
        Returns the name of the table that can be queried. If a version is specified, the name will be suffixed with
        ``__v<version>``. Otherwise, this just returns the name. Note that versions are discouraged in the current
        tinybird workflows.

        :return: The canonical name of the table that can be used in queries
        """
        if self.version is not None:
            return f"{self.name}__v{self.version}"
        else:
            return self.name

    def append(self, records: Records, *args, **kwargs) -> requests.Response:
        """Calls ``append_csv``."""
        # TODO: replicate tinybird API concepts instead of returning Response
        return self.append_csv(records, *args, **kwargs)

    def append_csv(self, records: Records, delimiter: str = ",") -> requests.Response:
        """
        Makes a POST request to the datasource using mode=append with CSV data. This appends data to the table.

        :param records: List of records to append. The will be converted to CSV using the provided delimiter.
        :param delimiter: Optional delimiter (defaults to ",")
        :return: The HTTP response
        """
        params = {"name": self.canonical_name, "mode": "append"}
        if delimiter:
            params["dialect_delimiter"] = delimiter

        headers = {"Content-Type": "text/html; charset=utf-8"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        data = self.to_csv(records, delimiter=delimiter)

        LOG.debug(
            "appending %d csv records to %s via %s(%s)",
            len(records),
            self,
            self.api,
            params,
        )
        # TODO: use multipart
        return requests.post(url=self.api, params=params, headers=headers, data=data)

    def append_ndjson(self, records: List[Dict]) -> requests.Response:
        """
        Makes a POST request to the datasource using mode=append with ndjson data. This appends data to the table.

        :param records: List of JSON records to append. The will be converted to NDJSON using ``json.dumps``
        :return: The HTTP response
        """
        # TODO: generalize appending in different formats
        query = {"name": self.canonical_name, "mode": "append", "format": "ndjson"}

        headers = {"Content-Type": "application/x-ndjson; charset=utf-8"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        docs = [json.dumps(record) for record in records]
        data = "\n".join(docs)

        LOG.debug(
            "appending %d ndjson records to %s via %s(%s)",
            len(records),
            self,
            self.api,
            query,
        )
        return requests.post(url=self.api, params=query, headers=headers, data=data)

    def truncate(self):
        """
        Truncate the datasource which removes all records in the table.
        """
        headers = {}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        url = f"{self.api}/{self.canonical_name}/truncate"
        LOG.debug(
            "truncating table %s",
            self.canonical_name,
        )
        requests.post(url=url, headers=headers)

    @staticmethod
    def to_csv(records: List[List[Any]], **kwargs):
        return to_csv(records, **kwargs)

    def __str__(self):
        return f"Datasource({self.canonical_name})"

    def __repr__(self):
        return self.__str__()


class FileDatasource(Datasource):
    """
    Datasource that writes into a file, used for testing and development purposes.
    """

    def __init__(self, path: str):
        name = os.path.basename(path)
        super().__init__(name, None)
        self.path = path

    def append_csv(self, records: Records, *args, **kwargs) -> requests.Response:
        if records:
            with open(self.path, "a") as fd:
                write_csv(fd, records)

        response = requests.Response()
        response.status_code = 200
        return response

    def append_ndjson(self, records: List[Dict]) -> requests.Response:
        raise NotImplementedError

    def readlines(self) -> List[str]:
        with open(self.path, "r") as fd:
            return fd.readlines()

    def truncate(self):
        raise NotImplementedError

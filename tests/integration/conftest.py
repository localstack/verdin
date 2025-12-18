import os
import time

import pytest

from verdin.api import ApiError
from verdin.client import Client
from verdin.test.cli import TinybirdCli
from verdin.test.container import TinybirdLocalContainer

# os.environ["SKIP_TINYBIRD_LOCAL_START"] = "1"


def _is_skip_tinybird_local_start() -> bool:
    """
    Set SKIP_TINYBIRD_LOCAL_START=1 if you have a tb local container running already with the project deployed. This
    allows faster iterations.
    """
    return os.environ.get("SKIP_TINYBIRD_LOCAL_START") in ["1", "true", "True", True]


@pytest.fixture(scope="session")
def client(tinybird_local_container) -> Client:
    return tinybird_local_container.client()


@pytest.fixture(scope="session")
def cli(tinybird_local_container) -> TinybirdCli:
    project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "project"))

    return TinybirdCli(
        host=tinybird_local_container.url,
        local=True,
        cwd=project_dir,
    )


@pytest.fixture(scope="session", autouse=True)
def tinybird_local_container():
    """
    Starts a tinybird local container in the background and waits until it becomes available.
    """
    project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "project"))

    container = TinybirdLocalContainer(cwd=project_dir)

    if not _is_skip_tinybird_local_start():
        container.start()

    container.wait_is_up()

    yield container

    # cleanup
    if not _is_skip_tinybird_local_start():
        container.stop()


@pytest.fixture(scope="session", autouse=True)
def deployed_project(cli):
    if _is_skip_tinybird_local_start():
        yield
        return

    time.sleep(5)
    cli.deploy(wait=True, auto=True)
    yield


@pytest.fixture(autouse=True)
def _truncate_datasource(client):
    # make sure to truncate "simple" datasource and its quarantine table before and after each test

    client.api.datasources.truncate("simple")
    try:
        # also truncate the quarantine table if it exists
        client.api.datasources.truncate("simple_quarantine")
    except ApiError:
        pass

    yield
    client.api.datasources.truncate("simple")

    try:
        client.api.datasources.truncate("simple_quarantine")
    except ApiError:
        pass

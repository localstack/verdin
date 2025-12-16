import os
import time

import pytest

from verdin.client import Client
from verdin.test.cli import TinybirdCli
from verdin.test.container import TinybirdLocalContainer


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

    container.start()
    container.wait_is_up()

    yield container

    # cleanup
    container.stop()


@pytest.fixture(scope="session", autouse=True)
def deployed_project(cli):
    time.sleep(5)
    cli.deploy(wait=True, auto=True)
    yield

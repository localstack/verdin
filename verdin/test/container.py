import subprocess
import time

import requests

from verdin.test.cli import TinybirdCli
from verdin.client import Client


class TinybirdLocalContainer:
    # TODO: easier configuration of compatibility mode

    def __init__(self, cwd: str = None):
        """
        Creates a new TinybirdLocalContainer instance.

        :param cwd: The current working directory to use for the tinybird local container.
        """
        self.cwd = cwd
        self.url = "http://localhost:7181"
        self.proc: None | subprocess.Popen = None

    def start(self):
        """
        Start the tinybird local container in a background process.
        """
        cli = TinybirdCli(cwd=self.cwd, local=True)
        self.proc = cli.local_start(daemon=True, skip_new_version=True)

    def client(self) -> Client:
        """
        Returns a tinybird Client that connects to this container with admin privileged.

        :return: Tinybird Client
        """
        cli = TinybirdCli(host=self.url, cwd=self.cwd, local=True)

        cli_tokens = cli.token_ls()

        # i'm not really sure why this is needed, but when we use a token returned by the /tokens api, the
        # client cannot find datasources created through ``tb deploy``.
        token_to_use = None
        for token in cli_tokens:
            if token.name == "admin local_testing@tinybird.co":
                token_to_use = token.token
                break

        return Client(
            token=token_to_use,
            api=self.url,
        )

    def wait_is_up(self, timeout: int = 120):
        """
        Wait for the container to appear by querying the tokens endpoint.

        :param timeout: Timeout in seconds
        :raises TimeoutError: If the container does not appear within the timeout
        """
        # Wait for the service to become available
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = requests.get(f"{self.url}/tokens")
                if response.status_code == 200:
                    break
            except requests.RequestException:
                pass
            time.sleep(1)
        else:
            raise TimeoutError("Tinybird container failed to start within timeout")

    def stop(self):
        """
        Stops and removes the tinybird local container.
        """
        cli = TinybirdCli(cwd=self.cwd, local=True)
        cli.local_stop()

        if self.proc:
            self.proc.kill()
            self.proc = None

        cli.local_remove()

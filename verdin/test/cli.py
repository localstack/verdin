"""Wrapper around the Tinybird CLI to make available the main commands programmatically."""

import dataclasses
import logging
import os
import re
import subprocess

LOG = logging.getLogger(__name__)


@dataclasses.dataclass
class Token:
    id: str
    name: str
    token: str


class CliError(Exception):
    def __init__(self, output: str, orig: subprocess.SubprocessError) -> None:
        super().__init__(output)
        self.orig = orig


class TinybirdCli:
    """Interface around the Tinybird CLI"""

    def __init__(self, host: str = None, token: str = None, cwd: str = None, local: bool = False):
        self.host = host
        self.token = token
        self.cwd = cwd
        self.local = local

    def _env(self) -> dict:
        """
        Returns a dictionary of environment variables to be used when calling tb CLI commands.
        """
        _env = dict(os.environ)

        if self.host:
            _env["TB_HOST"] = self.host
        if self.token:
            _env["TB_TOKEN"] = self.token

        return _env

    def _get_base_args(self) -> list[str]:
        args = ["tb"]
        if self.local:
            args.append("--local")
        return args

    def token_ls(self) -> list[Token]:
        """
        List all tokens.

        :return: List of Token instances
        """
        args = [*self._get_base_args(), "token", "ls"]

        output = subprocess.check_output(
            args,
            encoding="utf-8",
            cwd=self.cwd,
            env=self._env(),
        )
        """
        output looks like this (unfortunately --output=json doesn't work)

        ** Tokens:
        --------------------------------------------------------------------------------
        id: 63678691-7e28-4f2d-8ef7-243ab19ad7cb
        name: workspace admin token
        token: p.eyJ1IjogIjU2ZThhYmMzLWRjNmYtNDcyYi05Yzg1LTdkZjFiZmUyNjU5YyIsICJpZCI6ICI2MzY3ODY5MS03ZTI4LTRmMmQtOGVmNy0yNDNhYjE5YWQ3Y2IiLCAiaG9zdCI6ICJsb2NhbCJ9.4gzsbiG1cnrIDUfHTxfQd0ZN57YkiOKEIyvuTlnLiaM
        --------------------------------------------------------------------------------
        id: 489c8ca1-195b-4383-a388-d84068ff1b2c
        name: admin local_testing@tinybird.co 
        token: p.eyJ1IjogIjU2ZThhYmMzLWRjNmYtNDcyYi05Yzg1LTdkZjFiZmUyNjU5YyIsICJpZCI6ICI0ODljOGNhMS0xOTViLTQzODMtYTM4OC1kODQwNjhmZjFiMmMiLCAiaG9zdCI6ICJsb2NhbCJ9.MmcBjRTCg6dX53sWsZAv6QzHRHKxwu-pEWkqx8opLHA
        --------------------------------------------------------------------------------
        """
        tokens = []
        current_token = {}

        for line in output.splitlines():
            # remove color codes
            line = re.sub(r"\x1b\[[0-9;]*m", "", line)
            line = line.strip()
            if line.startswith("id: "):
                current_token = {}
                current_token["id"] = line[4:]
            elif line.startswith("name: "):
                current_token["name"] = line[6:]
            elif line.startswith("token: "):
                current_token["token"] = line[7:]
                tokens.append(Token(**current_token))

        return tokens

    def local_start(
        self, daemon: bool = False, skip_new_version: bool = False, volumes_path: str = None
    ) -> subprocess.Popen:
        """
        Run ``tb local start`` and return the subprocess.
        """
        args = ["tb", "local", "start"]
        if daemon:
            args.append("-d")
        if skip_new_version:
            args.append("--skip-new-version")
        if volumes_path:
            args.append("--volumes-path")
            args.append(volumes_path)

        return subprocess.Popen(args, cwd=self.cwd, env=self._env())

    def local_stop(self):
        """
        Run ``tb local stop``.
        """
        subprocess.check_output(["tb", "local", "stop"])

    def local_remove(self):
        """
        Run ``tb local remove``.
        """
        subprocess.check_output(
            ["tb", "local", "remove"],
            input=b"y\n",
        )

    def deploy(
        self, wait: bool = False, auto: bool = False, allow_destructive_operations: bool = False
    ):
        args = [*self._get_base_args(), "deploy"]

        if wait:
            args.append("--wait")
        if auto:
            args.append("--auto")
        if allow_destructive_operations:
            args.append("--allow-destructive-operations")

        try:
            output = subprocess.check_output(
                args,
                encoding="utf-8",
                cwd=self.cwd,
                env=self._env(),
            )
        except subprocess.CalledProcessError as e:
            raise CliError(f"Failed to deploy project:\n{e.output}", e) from e

        return output

import time
import uuid
from typing import Callable


def retry(
    fn: Callable,
    args: tuple = None,
    kwargs: dict = None,
    retries: int = 3,
    interval: float = 1,
):
    """
    Retries the execution of a function ``fn`` for a specified number of attempts (``retries``) with a delay
    between attempts (``interval``). If all attempts fail, a ``TimeoutError`` is raised indicating the final
    error encountered.

    :param fn: The callable function to be executed.
    :param args: A tuple of positional arguments to pass to the ``fn``. Defaults to an empty tuple if not provided.
    :param kwargs: A dictionary of keyword arguments to pass to the ``fn``. Defaults to an empty dictionary if not
        provided.
    :param retries: The number of retry attempts before raising a ``TimeoutError``. Defaults to 3.
    :param interval: The time (in seconds) to wait between each retry attempt. Defaults to 1.0 seconds.
    :return: The result returned by successfully calling the ``fn`` with the specified ``args`` and ``kwargs``.
        Returns `None` only if no successful result is obtained after all retry attempts.
    """
    args = args or ()
    kwargs = kwargs or {}

    for i in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if i == retries - 1:
                raise TimeoutError(f"Gave up after {retries} retries, final error: {e}") from e
            else:
                time.sleep(interval)
                continue

    return None


def short_id() -> str:
    return str(uuid.uuid4())[-8:]

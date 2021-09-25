import logging
import multiprocessing
import time
from queue import Empty, Queue
from typing import Optional, Tuple

import requests

from .datasource import Datasource, Records

LOG = logging.getLogger(__name__)


class StopWorker(Exception):
    """
    An exception that indicates to stop the the QueueingDatasourceAppender worker.
    """

    marker = "__STOP__"

    batch: Optional[Records]

    def __init__(self, batch: Records = None):
        self.batch = batch


class QueuingDatasourceAppender:
    """
    A QueuingDatasourceAppender reads batches of records from a source Queue and appends the batches to a data
    source. Once rate limited, it waits for the instructed amount of time (or if that is not specified,
    whatever default_retry_after is set to), before appending again.

    See https://docs.tinybird.co/api-reference/api-reference.html#limits-title

    TODO: synchronize multiple appenders through a RateLimiter concurrency structure: data source share rate limiting
     across account or workspace (not sure), so running multiple separate queuing datasource appenders will lead to
     excessive rate limiting.
    """

    default_retry_after: float = 12
    wait_after_rate_limit: float = 12

    source: Queue
    destination: Datasource
    min_interval: float

    def __init__(self, source: Queue, destination: Datasource, min_interval: float = 5) -> None:
        """
        :param source: a queue that buffers records to be appended to the datasource
        :param destination: the datasource to append to
        :param min_interval: the minimal time to wait between batches
        """
        super().__init__()
        self.source = source
        self.destination = destination
        self.stopped = multiprocessing.Event()
        self.min_interval = min_interval

    def close(self):
        if self.stopped.is_set():
            return
        self.stopped.set()
        self.source.put_nowait(StopWorker.marker)

    def run(self):
        try:
            while not self.stopped.is_set():
                try:
                    then = time.time()
                    batch, error = self._do_next_batch()

                    if error is not None:
                        # TODO: make sure the batch is not dropped on error. however, if the batch is
                        #  not appendable (for example because of errors in the data), then we need to
                        #  make sure the batch is either dropped, or we try to find the record that
                        #  causes the error.

                        raise error

                    duration = time.time() - then
                    LOG.debug("processing batch took %.2f", duration)
                    if self.min_interval:
                        self.stopped.wait(self.min_interval)

                except StopWorker as e:
                    LOG.info("indicated worker shutdown, trying to flush batch")
                    if e.batch:
                        self._retry_batch(e.batch, max_retries=2)
                    return

                except Exception:
                    LOG.exception("exception while processing batch, events will be dropped")
        finally:
            LOG.info(
                "shutting down DatasourceQueueWorker, %d elements remaining",
                self.source.qsize(),
            )

    def _get_batch(self, n=None) -> Records:
        """
        Reads the next batch from the queue.

        :param n: the maximum number of items to batch (default is entire queue)
        :return: the items from the queue as Batch
        :raises StopWorker if the StopWorker.marker was retrieved from the queue
        """
        q = self.source
        item = q.get()

        if item == StopWorker.marker:
            raise StopWorker()

        result = [item]  # block until we have at least one item

        if not n:
            n = q.qsize()

        try:
            while len(result) <= n:
                item = q.get(block=False)

                if item == StopWorker.marker:
                    raise StopWorker(result)

                result.append(item)
        except Empty:
            pass

        return result

    def _parse_retry_seconds(self, response: requests.Response) -> float:
        retry = response.headers.get("Retry-After")
        if retry:
            try:
                return float(retry) + 0.5
            except ValueError as e:
                LOG.error("error while parsing Retry-After value '%s': %s", retry, e)

        return self.default_retry_after

    def _retry_batch(self, batch, max_retries=10) -> Tuple[requests.Response, bool]:
        """
        Tries to append the given batch to the datasource for max_retries amount of times. It
        only retries if the request was rate limited, and waits for a certain amount of time
        afterwards.

        :param batch: a list of records to append to the datasource
        :param max_retries: max number of retries (defaults to 10)
        :return: a tuple with the last response and a boolean flag indicating whether the request
                 was rate-limited
        """
        limited = False
        response = None

        for _ in range(max_retries):
            response = self.destination.append(batch)

            if response.ok:
                return response, limited

            if response.status_code == 429:
                wait = self._parse_retry_seconds(response)
                limited = True
                LOG.debug(
                    "rate limited by API, keeping %d records safe for %d seconds: %s",
                    len(batch),
                    wait,
                    response.text,
                )
                time.sleep(wait)
                continue

            LOG.warning(
                "unhandled error %d: %s while appending to datasource, dropping batch",
                response.status_code,
                response.text,
            )
            return response, limited

        return response, limited

    def _do_next_batch(self) -> Tuple[Records, Optional[Exception]]:
        batch = self._get_batch()

        try:
            LOG.debug(
                "processing batch of size %d into datasource %s",
                len(batch),
                self.destination.name,
            )

            response, limited = self._retry_batch(batch)

            if limited:
                # if the request was rate-limited, we'll try again after X-Ratelimit-Reset, or the
                # wait_after_rate_limit value if it is set
                try:
                    if self.wait_after_rate_limit:
                        wait = self.wait_after_rate_limit
                    else:
                        wait = float(response.headers.get("X-Ratelimit-Reset", 0))

                    LOG.info(
                        "waiting %d second until rate-limit window resets before batching again",
                        wait,
                    )
                    time.sleep(wait)
                except ValueError:
                    LOG.exception("error while parsing X-Ratelimit-Reset value")

        except Exception as e:
            return batch, e

        return batch, None

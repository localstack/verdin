import threading
import time
from queue import Queue

import requests

from verdin.datasource import Datasource
from verdin.worker import QueuingDatasourceAppender


class QueueingDatasource(Datasource):
    def __init__(self, name, queue=None):
        super().__init__(name, None)
        self.queue = queue or Queue()

    def append(self, records) -> requests.Response:
        if records:
            self.queue.put(records)

        response = requests.Response()
        response.status_code = 200
        return response


class TestQueuingDatasourceAppender:
    def test_batching(self):
        source = Queue()
        destination = QueueingDatasource("datasource")

        appender = QueuingDatasourceAppender(source, destination)
        appender.min_interval = 0

        source.put(("a", 1))
        source.put(("b", 2))
        source.put(("c", 3))

        thread = threading.Thread(target=appender.run)
        thread.start()

        batch = destination.queue.get(timeout=1)
        assert len(batch) == 3
        assert batch[0] == ("a", 1)
        assert batch[1] == ("b", 2)
        assert batch[2] == ("c", 3)

        source.put(("d", 4))

        batch = destination.queue.get(timeout=1)
        assert len(batch) == 1
        assert batch[0] == ("d", 4)

        appender.close()
        thread.join(timeout=2)
        assert appender.stopped.is_set()

    def test_stop_while_running(self):
        # instrument the queue
        source = Queue()
        destination = QueueingDatasource("datasource")
        appender = QueuingDatasourceAppender(source, destination)
        appender.min_interval = 0

        thread = threading.Thread(target=appender.run)
        thread.start()
        time.sleep(0.2)

        appender.close()
        thread.join(timeout=2)
        assert appender.stopped.is_set()

    def test_retry(self):
        class MockQueueingDatasource(QueueingDatasource):
            first_call = True

            def append(self, records) -> requests.Response:
                if self.first_call:
                    self.first_call = False

                    response = requests.Response()
                    response.status_code = 429
                    response.headers["Retry-After"] = "1"
                    return response

                return super().append(records)

        source = Queue()
        destination = MockQueueingDatasource("datasource")
        appender = QueuingDatasourceAppender(source, destination)
        appender.min_interval = 0
        appender.wait_after_rate_limit = 0.5

        source.put(("a", 1))
        source.put(("b", 2))

        thread = threading.Thread(target=appender.run)
        thread.start()
        time.sleep(0.5)

        # should not be batched because we're still retrying with the previous batch
        source.put(("c", 3))

        batch = destination.queue.get(timeout=5)
        assert len(batch) == 2

        batch = destination.queue.get(timeout=5)
        assert len(batch) == 1
        assert batch[0] == ("c", 3)

        appender.close()
        thread.join(timeout=5)
        assert appender.stopped.is_set()

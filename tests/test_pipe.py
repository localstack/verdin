from typing import Dict, Optional

from verdin.pipe import PagedPipeQuery, PipeMetadata


class MockPipeJsonResponse:
    def __init__(self, empty: bool, data: Optional[Dict], meta: PipeMetadata):
        self.empty = empty
        self.data = data
        self.meta = meta


class TestPagedPipeQuery:
    def test(self):
        queries = list()

        class MockPipe:
            def sql(self, query):
                queries.append(query)

                if len(queries) == 2:
                    return MockPipeJsonResponse(empty=True, data=None, meta=[])

                return MockPipeJsonResponse(empty=False, data={}, meta=[])

        for page in PagedPipeQuery(pipe=MockPipe(), page_size=10, start_at=0):
            assert page.empty is False

        assert len(queries) == 2
        assert queries[0] == "SELECT * FROM _ LIMIT 10 OFFSET 0"
        assert queries[1] == "SELECT * FROM _ LIMIT 10 OFFSET 10"

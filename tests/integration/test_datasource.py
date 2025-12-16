import logging


LOG = logging.getLogger(__name__)


class TestDatasource:
    def test_append_ndjson_and_query(self, client):
        ds = client.datasource("simple")
        ds.append_ndjson(
            [
                {
                    "id": "e7f2af3e-99d1-4d4f-8a8c-d6aee4ab89b0",
                    "created": "2024-01-23T10:30:00.123456",
                    "value": "test value",
                }
            ]
        )

        query = client.sql("SELECT * FROM simple")
        response = query.json()
        assert response.data == [
            {
                "id": "e7f2af3e-99d1-4d4f-8a8c-d6aee4ab89b0",
                "timestamp": "2024-01-23 10:30:00.123456",
                "value": "test value",
            },
        ]

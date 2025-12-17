import logging


LOG = logging.getLogger(__name__)


class TestDatasource:
    def test_append_ndjson_query_truncate(self, client):
        ds = client.datasource("simple")
        ds.truncate()

        ds.append_ndjson(
            [
                {
                    "Id": "e7f2af3e-99d1-4d4f-8a8c-d6aee4ab89b0",
                    "Timestamp": "2024-01-23T10:30:00.123456",
                    "Key": "foo",
                    "Value": "bar",
                },
                {
                    "Id": "d7792957-21d8-46e6-a4e0-188eb36e2758",
                    "Timestamp": "2024-02-23T11:45:00.234567",
                    "Key": "baz",
                    "Value": "ed",
                },
            ]
        )

        query = client.sql("SELECT * FROM simple")
        response = query.json()
        assert response.data == [
            {
                "id": "e7f2af3e-99d1-4d4f-8a8c-d6aee4ab89b0",
                "timestamp": "2024-01-23 10:30:00.123456",
                "key": "foo",
                "value": "bar",
            },
            {
                "id": "d7792957-21d8-46e6-a4e0-188eb36e2758",
                "timestamp": "2024-02-23 11:45:00.234567",
                "key": "baz",
                "value": "ed",
            },
        ]

        query = client.sql("SELECT count(*) as cnt FROM simple")
        response = query.json()
        assert response.data == [{"cnt": 2}]

        # remove all records from the table
        ds.truncate()

        # check that the table is empty
        query = client.sql("SELECT count(*) as cnt FROM simple")
        response = query.json()
        assert response.data == [{"cnt": 0}]

        query = client.sql("SELECT * FROM simple")
        response = query.json()
        assert response.data == []

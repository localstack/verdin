import pytest

from tests.utils import retry


class TestEventsApi:
    @pytest.mark.parametrize("compress", [True, False])
    def test_events(self, client, compress):
        events = client.api.events

        records = [
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

        response = events.send("simple", records, compress=compress)

        assert response.successful_rows == 2
        assert response.quarantined_rows == 0

        def _wait_for_count(cnt: int):
            query = client.sql("SELECT count(*) as cnt FROM simple")
            assert query.json().data == [{"cnt": cnt}]

        retry(_wait_for_count, args=(2,))

    def test_events_wait(self, client):
        events = client.api.events

        records = [
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

        response = events.send("simple", records, wait=True)

        assert response.successful_rows == 2
        assert response.quarantined_rows == 0

        query = client.sql("SELECT count(*) as cnt FROM simple")
        assert query.json().data == [{"cnt": 2}]

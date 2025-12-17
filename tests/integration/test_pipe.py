class TestPipe:
    def test_pipe_query(self, client):
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
                {
                    "Id": "fc71d4d5-7e0c-492a-9e3f-8f1cde9bcfaf",
                    "Timestamp": "2024-03-23T11:45:00.234567",
                    "Key": "foo",
                    "Value": "bar2",
                },
            ]
        )

        pipe = client.pipe("simple_kv")

        response = pipe.query()
        assert response.data == [
            {"key": "baz", "value": "ed"},
            {"key": "foo", "value": "bar2"},
        ]

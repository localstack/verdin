import pytest


class TestQueryApi:
    @pytest.fixture(autouse=True)
    def _put_records(self, client):
        client.api.events.send(
            "simple",
            wait=True,
            records=[
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
            ],
        )

    def test_query_datasource_json(self, client):
        response = client.api.query.query("SELECT key, value FROM simple ORDER BY `key` ASC")

        assert response.data == [{"key": "baz", "value": "ed"}, {"key": "foo", "value": "bar"}]
        assert response.meta == [
            {"name": "key", "type": "String"},
            {"name": "value", "type": "String"},
        ]
        assert response.rows == 2
        assert response.statistics["rows_read"] == 2

    def test_query_pipe(self, client):
        response = client.api.query.query("SELECT * FROM simple_kv ORDER BY `key` ASC")

        assert response.data == [{"key": "baz", "value": "ed"}, {"key": "foo", "value": "bar"}]
        assert response.meta == [
            {"name": "key", "type": "String"},
            {"name": "value", "type": "String"},
        ]
        assert response.rows == 2
        assert response.statistics["rows_read"] == 2

    def test_query_pipe_parameters(self, client):
        response = client.api.query.query(
            "SELECT key, value FROM simple_pipe", parameters={"key": "foo"}
        )

        assert response.data == [{"key": "foo", "value": "bar"}]
        assert response.meta == [
            {"name": "key", "type": "String"},
            {"name": "value", "type": "String"},
        ]
        assert response.rows == 1
        assert response.statistics["rows_read"] == 2

    def test_query_pipe_with_none_parameters(self, client):
        response = client.api.query.query(
            "SELECT key, value FROM simple_pipe ORDER BY key ASC", parameters={"key": None}
        )

        assert response.data == [{"key": "baz", "value": "ed"}, {"key": "foo", "value": "bar"}]
        assert response.meta == [
            {"name": "key", "type": "String"},
            {"name": "value", "type": "String"},
        ]
        assert response.rows == 2
        assert response.statistics["rows_read"] == 2

    def test_query_pipeline_json(self, client):
        response = client.api.query.query(
            "SELECT * FROM _ ORDER BY `key` ASC", pipeline="simple_kv"
        )

        assert response.data == [{"key": "baz", "value": "ed"}, {"key": "foo", "value": "bar"}]
        assert response.meta == [
            {"name": "key", "type": "String"},
            {"name": "value", "type": "String"},
        ]
        assert response.rows == 2
        assert response.statistics["rows_read"] == 2

    def test_query_csv(self, client):
        response = client.api.query.query(
            "SELECT key, value FROM simple ORDER BY `key` ASC", format="CSV"
        )

        assert response.text == '"baz","ed"\n"foo","bar"\n'

    def test_query_csv_with_names(self, client):
        response = client.api.query.query(
            "SELECT key, value FROM simple ORDER BY `key` ASC", format="CSVWithNames"
        )

        assert (
            response.text
            == '"key","value"\n"baz","ed"\n"foo","bar"\n'
            != '"baz","ed"\n"foo","bar"\n'
        )
        # CSV with names can be parsed as data!
        assert response.data == [{"key": "baz", "value": "ed"}, {"key": "foo", "value": "bar"}]

    def test_query_tsv(self, client):
        response = client.api.query.query(
            "SELECT key, value FROM simple ORDER BY `key` ASC", format="TSV"
        )

        assert response.text == "baz\ted\nfoo\tbar\n"

    def test_query_tsv_with_names(self, client):
        response = client.api.query.query(
            "SELECT key, value FROM simple ORDER BY `key` ASC", format="TSVWithNames"
        )

        assert response.text == "key\tvalue\nbaz\ted\nfoo\tbar\n"
        assert response.data == [{"key": "baz", "value": "ed"}, {"key": "foo", "value": "bar"}]

    def test_query_ndjson(self, client):
        response = client.api.query.query(
            "SELECT key, value FROM simple ORDER BY `key` ASC", format="JSONEachRow"
        )

        assert (
            response.text
            == '{"key":"baz","value":"ed"}\n{"key":"foo","value":"bar"}\n'
            != '"key","value"\n"baz","ed"\n"foo","bar"\n'
        )
        # CSV with names can be parsed as data!
        assert response.data == [{"key": "baz", "value": "ed"}, {"key": "foo", "value": "bar"}]

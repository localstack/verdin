import pytest

from verdin.api.pipes import PipeNotFoundError


class TestPipesApi:
    def test_list(self, client):
        response = client.api.pipes.list(
            attrs=["id", "name"],
            node_attrs=[],
        )

        for pipe in response.pipes:
            assert {"id", "name", "url"} == set(pipe.keys())
            assert pipe["id"] is not None
            assert pipe["name"] is not None

        assert "simple_kv" in [pipe["name"] for pipe in response.pipes]
        assert "simple_pipe" in [pipe["name"] for pipe in response.pipes]

    def test_query_json(self, client):
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

        response = client.api.pipes.query("simple_kv", format="json")
        assert response.data == [{"key": "baz", "value": "ed"}, {"key": "foo", "value": "bar"}]
        assert response.meta == [
            {"name": "key", "type": "String"},
            {"name": "value", "type": "String"},
        ]
        assert response.rows == 2
        assert response.statistics["rows_read"] == 2

    @pytest.mark.parametrize("format", ["csv", "ndjson", "json"])
    def test_query_formats(self, client, format):
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
                {
                    "Id": "2b84e03e-dbcf-4141-9656-94ff8ac8c036",
                    "Timestamp": "2024-03-23T11:45:00.345678",
                    "Key": "format",
                    "Value": format,
                },
            ],
        )

        response = client.api.pipes.query("simple_kv", format=format)
        assert response.data == [
            {"key": "baz", "value": "ed"},
            {"key": "foo", "value": "bar"},
            {"key": "format", "value": format},
        ]

    def test_query_with_params(self, client):
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
                {
                    "Id": "2b84e03e-dbcf-4141-9656-94ff8ac8c036",
                    "Timestamp": "2024-03-23T11:45:00.345678",
                    "Key": "foo",
                    "Value": "bar2",
                },
            ],
        )

        response = client.api.pipes.query("simple_pipe", parameters={"key": "foo"})
        assert response.data == [
            {
                "id": "e7f2af3e-99d1-4d4f-8a8c-d6aee4ab89b0",
                "key": "foo",
                "timestamp": "2024-01-23 10:30:00.123456",
                "value": "bar",
            },
            {
                "id": "2b84e03e-dbcf-4141-9656-94ff8ac8c036",
                "key": "foo",
                "timestamp": "2024-03-23 11:45:00.345678",
                "value": "bar2",
            },
        ]

        response = client.api.pipes.query("simple_pipe", parameters={"key": "does not exist"})
        assert response.data == []

        # test with none parameters
        response = client.api.pipes.query("simple_pipe", parameters={"key": None})
        # simple check to make sure it returned all items
        assert [(record["key"], record["value"]) for record in response.data] == [
            ("foo", "bar"),
            ("baz", "ed"),
            ("foo", "bar2"),
        ]

    def test_query_with_sql(self, client):
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
                {
                    "Id": "2b84e03e-dbcf-4141-9656-94ff8ac8c036",
                    "Timestamp": "2024-03-23T11:45:00.345678",
                    "Key": "foo",
                    "Value": "bar2",
                },
            ],
        )

        response = client.api.pipes.query(
            "simple_pipe",
            query="SELECT timestamp,key,value FROM _ ORDER BY `value` DESC",
            parameters={"key": "foo"},
        )

        assert response.data == [
            {
                "timestamp": "2024-03-23 11:45:00.345678",
                "key": "foo",
                "value": "bar2",
            },
            {
                "timestamp": "2024-01-23 10:30:00.123456",
                "key": "foo",
                "value": "bar",
            },
        ]

    def test_query_with_sql_too_long(self, client):
        chars = "a" * 6000
        # ``chars`` ends up in both the query and parameters, making the total body ~12k, but the query <8k, which is a
        # requirement.

        response = client.api.pipes.query(
            "simple_pipe",
            query=f"SELECT * FROM _ WHERE `value` = '{chars}'",
            parameters={"key": chars},
        )

        # ofcourse the query is nonsense and returns nothing
        assert response.data == []

    def test_query_with_non_existing_pipe(self, client):
        with pytest.raises(PipeNotFoundError) as e:
            client.api.pipes.query("non_existent_pipe")

        assert e.match("The pipe 'non_existent_pipe' does not exist")
        assert e.value.status_code == 404

    def test_get_information(self, client):
        response = client.api.pipes.get_information("simple_kv")
        assert response.info["name"] == "simple_kv"
        assert response.info["type"] == "endpoint"

        # check that it also works with the pipe's ID
        response = client.api.pipes.get_information(response.info["id"])
        assert response.info["name"] == "simple_kv"
        assert response.info["type"] == "endpoint"

    def test_get_information_non_existing_pipe(self, client):
        with pytest.raises(PipeNotFoundError) as e:
            client.api.pipes.get_information("non_existent_pipe")

        assert e.match("Pipe 'non_existent_pipe' not found")
        assert e.value.status_code == 404

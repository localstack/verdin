import json

import pytest

from verdin.api import ApiError
from verdin.api.datasources import DataSourceNotFoundError
from tests.utils import retry


class TestDataSourcesApi:
    def test_list(self, client):
        response = client.api.datasources.list()

        assert len(response.datasources) >= 1

        # find "simple" datasource in the list of data sources
        ds = None
        for datasource in response.datasources:
            if datasource["name"] == "simple":
                ds = datasource
                break

        assert ds

        # smoke tests some attributes
        assert ds["engine"]["engine"] == "MergeTree"
        assert "simple_kv" in [x["name"] for x in ds["used_by"]]

    def test_get_information(self, client):
        response = client.api.datasources.get_information("simple")

        # smoke tests some attributes
        assert response.info["name"] == "simple"
        assert response.info["engine"]["engine"] == "MergeTree"
        assert "simple_kv" in [x["name"] for x in response.info["used_by"]]

    def test_get_information_on_non_existing_datasource(self, client):
        with pytest.raises(DataSourceNotFoundError) as e:
            client.api.datasources.get_information("non_existing_datasource")

        e.match('Data Source "non_existing_datasource" does not exist')
        assert e.value.status_code == 404

    def test_truncate(self, client):
        ds = client.datasource("simple")
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

        def _wait_for_count(cnt: int):
            query = client.sql("SELECT count(*) as cnt FROM simple")
            assert query.json().data == [{"cnt": cnt}]

        retry(_wait_for_count, args=(2,))

        client.api.datasources.truncate("simple")

        retry(_wait_for_count, args=(0,))

    def test_append_to_non_existing_data_source(self, client):
        with pytest.raises(ApiError) as e:
            client.api.datasources.append("non_existing_datasource", "foo,bar\n")

        # this is odd behavior, but currently, this raises a 403, with the error
        # "Adding or modifying data sources to this workspace can only be done via deployments"
        # due to the way tinybird behaves (apparently it doesn't check mode=append)

        assert e.value.status_code == 403

    def test_append_csv(self, client):
        ds = client.api.datasources

        data = "5b6859d2-e060-40a4-949a-7e7fab8e3207,2024-01-23T10:30:00.123456,foo,bar\n"
        data += "af49ffce-559c-426e-9787-ddb08628b547,2024-02-23T11:45:00.234567,baz,ed"

        response = ds.append("simple", data)
        assert not response.error
        assert response.quarantine_rows == 0
        assert response.invalid_lines == 0
        assert response.datasource["name"] == "simple"

        assert client.sql("SELECT * FROM simple").json().data == [
            {
                "id": "5b6859d2-e060-40a4-949a-7e7fab8e3207",
                "timestamp": "2024-01-23 10:30:00.123456",
                "key": "foo",
                "value": "bar",
            },
            {
                "id": "af49ffce-559c-426e-9787-ddb08628b547",
                "timestamp": "2024-02-23 11:45:00.234567",
                "key": "baz",
                "value": "ed",
            },
        ]

    def test_append_csv_with_invalid_data(self, client):
        ds = client.api.datasources

        data = "5b6859d2-e060-40a4-949a-7e7fab8e3207,2024-01-23T10:30:00.123456,foo,bar\n"
        data += "af49ffce-559c-426e-9787-ddb08628b5472024-02-23T11:45:00.234567,baz,ed"  # error in this line

        response = ds.append("simple", data)
        assert (
            response.error
            == "There was an error with file contents: 1 row in quarantine and 1 invalid line."
        )
        assert response.invalid_lines == 1
        assert response.quarantine_rows == 1

    def test_append_ndjson(self, client):
        ds = client.api.datasources
        ds.truncate("simple")

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

        def _data():
            for r in records:
                yield json.dumps(r) + "\n"

        response = ds.append("simple", _data(), format="ndjson")
        assert not response.error
        assert response.quarantine_rows == 0
        assert response.invalid_lines == 0
        assert response.datasource["name"] == "simple"

        assert client.sql("SELECT * FROM simple").json().data == [
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

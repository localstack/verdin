import pytest
from pytest_httpserver import HTTPServer
from werkzeug import Response

from verdin.query import QueryError, SqlQuery

_mock_json_response = """{
"meta": [
    {
        "name": "VendorID",
        "type": "Int32"
    },
    {
        "name": "tpep_pickup_datetime",
        "type": "DateTime"
    },
    {
        "name": "passenger_count",
        "type": "Int32"
    }
],
"data": [
    {
        "VendorID": 2,
        "tpep_pickup_datetime": "2001-01-05 11:45:23",
        "passenger_count": 5
    },
    {
        "VendorID": 2,
        "tpep_pickup_datetime": "2002-12-31 23:01:55",
        "passenger_count": 3
    }
],
"rows": 2,
"rows_before_limit_at_least": 4,
"statistics":
    {
        "elapsed": 0.00091042,
        "rows_read": 4,
        "bytes_read": 296
    }
}"""


def test_json(httpserver: HTTPServer):
    def handler(request):
        return Response(_mock_json_response, 200)

    httpserver.expect_request(
        "/v0/sql", query_string={"q": "select * from mytable FORMAT JSON"}
    ).respond_with_handler(handler)

    query = SqlQuery("select * from mytable", token="12345", api=httpserver.url_for("/"))

    response = query.json()

    assert response.meta[0] == {"name": "VendorID", "type": "Int32"}
    assert len(response.data) == 2


def test_json_error(httpserver: HTTPServer):
    def handler(request):
        return Response('{"error": "invalid datasource"}', 403)

    httpserver.expect_request(
        "/v0/sql", query_string={"q": "select * from mytable FORMAT JSON"}
    ).respond_with_handler(handler)

    query = SqlQuery("select * from mytable", token="12345", api=httpserver.url_for("/"))

    with pytest.raises(QueryError) as e:
        query.json()
    e.match("403")
    e.match("invalid datasource")

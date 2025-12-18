from pytest_httpserver import HTTPServer
from werkzeug import Response

from verdin.datasource import Datasource, FileDatasource


class TestDatasource:
    def test_to_csv(self):
        records = [["a", "1", "{}"], ["b", "2", '{"foo":"bar","baz":"ed"}']]

        csv = Datasource.to_csv(records)

        assert csv == """a,1,{}\nb,2,"{""foo"":""bar"",""baz"":""ed""}"\n"""

    def test_to_csv_with_delimiter(self):
        records = [["a", "1", "{}"], ["b", "2", '{"foo":"bar","baz":"ed"}']]

        csv = Datasource.to_csv(records, delimiter=";")

        assert csv == """a;1;{}\nb;2;"{""foo"":""bar"",""baz"":""ed""}"\n"""

    def test_append(self, httpserver: HTTPServer):
        ds = Datasource("mydatasource", "123456", api=httpserver.url_for("/"))

        expected_data = '''a,1,{}\nb,2,"{""foo"":""bar"",""baz"":""ed""}"'''

        def handler(request):
            actual_data = request.data.decode()
            assert expected_data in actual_data
            return Response("", 200)

        httpserver.expect_request(
            "/v0/datasources",
            query_string={
                "name": "mydatasource",
                "mode": "append",
                "dialect_delimiter": ",",
                "format": "csv",
            },
        ).respond_with_handler(handler)

        response = ds.append([["a", "1", "{}"], ["b", "2", '{"foo":"bar","baz":"ed"}']])
        httpserver.check()
        assert response.ok


class TestFileDatasource:
    def test_append(self, tmp_path):
        file_path = tmp_path / "myfile.csv"
        ds = FileDatasource(str(file_path))

        records = [["a", "1", "{}"], ["b", "2", '{"foo":"bar","baz":"ed"}']]
        ds.append(records)

        records = [["c", "3", "{}"]]
        ds.append(records)

        expected = """a,1,{}\nb,2,"{""foo"":""bar"",""baz"":""ed""}"\nc,3,{}\n"""
        actual = file_path.read_text()

        assert actual == expected

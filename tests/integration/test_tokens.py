import pytest

from verdin.api import ApiError


class TestTokensApi:
    def test_list(self, client):
        api = client.api.tokens
        tokens = api.list().tokens
        assert tokens
        assert "admin local_testing@tinybird.co" in [token["name"] for token in tokens]

    def test_get_information(self, client):
        api = client.api.tokens

        token = api.get_information("admin local_testing@tinybird.co").info
        assert token["name"] == "admin local_testing@tinybird.co"
        assert token["token"].startswith("p.e")

        # make sure it also works with the id
        token = api.get_information(token["id"]).info
        assert token["name"] == "admin local_testing@tinybird.co"

    def test_get_information_on_non_existing_token(self, client):
        api = client.api.tokens

        with pytest.raises(ApiError) as e:
            api.get_information("NON EXISTING TOKEN")

        assert e.match("Token has not enough permissions to get information about this token")
        assert e.value.status_code == 403

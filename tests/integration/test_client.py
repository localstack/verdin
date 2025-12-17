def test_client_has_token(client):
    """Makes sure the client fixture loaded the admin token correctly"""
    assert client.token.startswith("p.e")

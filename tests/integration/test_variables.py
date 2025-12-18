from datetime import datetime
import time

import pytest

from tests.utils import short_id
from verdin.api.variables import VariableNotFoundError


class TestVariables:
    def test_integration(self, client):
        # E2E test for variables API
        variable_name = f"test_variable_{short_id()}"
        variable_value = "test_value"

        # List variables and make sure the variable_name is not in the list
        response = client.api.variables.list()
        variable_names = [var["name"] for var in response.variables]
        assert variable_name not in variable_names

        # Make sure an API Error with 404 is raised when getting a non-existent variable
        with pytest.raises(VariableNotFoundError) as e:
            client.api.variables.get(variable_name)
        assert e.value.status_code == 404

        # Create the variable
        create_response = client.api.variables.create(name=variable_name, value=variable_value)
        assert create_response.variable["name"] == variable_name
        assert create_response.variable["type"] == "secret"

        # List again and check that it's there
        list_response = client.api.variables.list()
        variable_names = [var["name"] for var in list_response.variables]
        assert variable_name in variable_names

        # Get the variable and assert the response
        get_response = client.api.variables.get(variable_name)
        assert get_response.variable["name"] == variable_name
        assert get_response.variable["type"] == "secret"

        # delete the variable and check again
        response = client.api.variables.delete(variable_name)
        assert response.ok

        with pytest.raises(VariableNotFoundError) as e:
            client.api.variables.get(variable_name)

    def test_get_non_existing_variable(self, client):
        with pytest.raises(VariableNotFoundError) as e:
            client.api.variables.get("non_existing_variable")

        assert e.match("Not found")
        assert e.value.status_code == 404

    def test_delete_non_existing_variable(self, client):
        with pytest.raises(VariableNotFoundError) as e:
            client.api.variables.delete("non_existing_variable")

        assert e.match("Variable not found")
        assert e.value.status_code == 404

    def test_update_non_existing_variable(self, client):
        with pytest.raises(VariableNotFoundError) as e:
            client.api.variables.update("non_existing_variable", value="foo")

        assert e.match("Variable not found")
        assert e.value.status_code == 404

    def test_update_variable(self, client):
        variable_name = f"test_variable_{short_id()}"
        variable_value = "test_value"

        response = client.api.variables.create(name=variable_name, value=variable_value)

        assert datetime.fromisoformat(response.variable["created_at"]).replace(
            microsecond=0
        ) == datetime.fromisoformat(response.variable["updated_at"]).replace(microsecond=0)

        time.sleep(1)

        response = client.api.variables.update(name=variable_name, value=variable_value + "1")
        assert response.variable["created_at"] != response.variable["updated_at"]

        client.api.variables.delete(variable_name)

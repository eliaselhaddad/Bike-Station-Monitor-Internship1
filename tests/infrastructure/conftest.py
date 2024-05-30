import pytest
from yaml import safe_load


@pytest.fixture(scope="session")
def service_config():
    with open("./infrastructure/config/service-config.yaml") as file:
        return safe_load(file)

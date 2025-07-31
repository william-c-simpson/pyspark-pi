import argparse

import pytest

from tests.fixtures.spark_fixtures import spark
from tests.fixtures.pi_fixtures import connection_info, test_data

def pytest_addoption(parser: argparse.ArgumentParser) -> None:
    parser.addoption(
        "--integration",
        action="store_true",
        default=False,
        help="Run integration tests that require external services."
    )

def pytest_collection_modifyitems(config, items):
    if config.getoption("--integration"):
        return

    skip_integration = pytest.mark.skip(reason="Need --integration to run")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)
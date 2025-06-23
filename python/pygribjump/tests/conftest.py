import os
import pathlib

import pytest


@pytest.fixture(scope="session", autouse=True)
def gribjump_env() -> None:
    """
    Sets default environment variables that are not dependent on individual testsetup.
    """
    os.environ["GRIBJUMP_IGNORE_GRID"] = "1"


@pytest.fixture
def data_path(request) -> pathlib.Path:
    """
    Provides path to test data at '<src-root>/python/pygribjump/tests/data'
    """
    # path = request.config.rootpath / "python" / "pygribjump" / "tests" / "data"
    path = pathlib.Path(__file__).parent / "data"
    assert path.exists()
    return path

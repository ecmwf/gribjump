from importlib.metadata import version

import pygribjump


def test_version():
    assert pygribjump.__version__ == version("pygribjump")
# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

"""
Shared fixtures for the pygribjump (pybind11) API tests.

NOTE:

To be able to run the tests, pygribjump needs to find the native libraries.
If not installed into a default location export the following environment
variables in the shell running pytest:
    GRIBJUMP_DIR = <cmake-build-root-for-gribjump> or <custom-install-location>
    FDB5_DIR = <cmake-build-root-for-fdb> or <custom-install-location>
"""

import os
import pathlib
import shutil

import pytest


@pytest.fixture(scope="session", autouse=True)
def gribjump_env() -> None:
    """
    Sets default environment variables that are not dependent on individual test setup.
    """
    os.environ["GRIBJUMP_IGNORE_GRID"] = "1"


@pytest.fixture
def data_path() -> pathlib.Path:
    """
    Provides the path to the test data at '<src-root>/tests/pygribjump/data'
    """
    path = pathlib.Path(__file__).parent / "data"
    assert path.exists()
    return path


@pytest.fixture
def grib_file(data_path: pathlib.Path, tmp_path: pathlib.Path) -> pathlib.Path:
    """
    Provides the path to a GRIB file holding a single field of synthetic data.

    The file is copied into this test's temp directory: gribjump caches the
    extraction metadata in a `<file>.gribjump` file next to the data, which
    would otherwise pollute the source tree (and make results order dependent).
    """
    source = data_path / "synth11.grib"
    assert source.exists()

    path = tmp_path / source.name
    shutil.copy(source, path)
    return path

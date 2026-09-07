# (C) Copyright 2025- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

"""
Tests for the version and dependency information exposed by the bindings.
"""

import pygribjump


def test_version_is_exposed() -> None:
    assert isinstance(pygribjump.__version__, str)
    assert pygribjump.__version__ != ""
    assert pygribjump.version() == pygribjump.__version__


def test_library_version_matches_build_version() -> None:
    # pygribjump warns (see _internal) if these differ, in the build tree they must not
    assert pygribjump.library_version() == pygribjump.version()


def test_version_info_contains_dependencies() -> None:
    from pygribjump._internal import version_info

    libraries = {name: (version, sha1, path) for name, version, sha1, path in version_info()}

    assert "gribjump" in libraries
    assert "eckit" in libraries

    version, _, path = libraries["gribjump"]
    assert version == pygribjump.version()
    assert path != ""


def test_public_api_is_exported() -> None:
    for name in pygribjump.__all__:
        assert hasattr(pygribjump, name), f"pygribjump.{name} is not exported"


def test_bindings_are_the_pybind11_ones() -> None:
    from pygribjump._internal import _GribJump

    # The pybind11 extension module, not the cffi wrapper
    assert _GribJump.__module__ == "pygribjump_bindings.pygribjump_bindings"

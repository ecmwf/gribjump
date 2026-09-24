# (C) Copyright 2026- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

"""Configuration tests for the pybind API. Process settings need fresh interpreters."""

import os
import subprocess
import sys
import textwrap

import pytest

from pygribjump import GribJump, configure_process


def run_python(code, tmp_path, grib_file, *, defaults="{}\n", overrides=None):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(defaults)
    env = os.environ.copy()
    # The general Python test fixtures set IGNORE_GRID=1. These tests must exercise
    # both values without mutating the environment of running worker threads.
    for name in (
        "GRIBJUMP_IGNORE_GRID",
        "GRIBJUMP_THREADS",
        "GRIBJUMP_REQUEST_PARSING",
        "FDB_ENABLE_GRIBJUMP",
        "FDB_DISABLE_GRIBJUMP",
    ):
        env.pop(name, None)
    env["GRIBJUMP_CONFIG_FILE"] = str(config_file)
    env.update(overrides or {})
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code), str(grib_file)],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_none_uses_file_defaults_but_empty_dict_uses_object_defaults(tmp_path, grib_file):
    run_python(
        """
        import sys
        from pygribjump import GribJump, GribJumpException, PathExtractionRequest
        request = PathExtractionRequest(sys.argv[1], "file", 0, "", 0, [(0, 5)])
        for gj in (GribJump(), GribJump(None), GribJump(config=None)):
            assert next(iter(gj.extract_from_paths([request]))).values_flat.size == 5
        with GribJump(config={}) as strict:
            try:
                strict.extract_from_paths([request])
            except GribJumpException as error:
                assert "Grid hash" in str(error)
            else:
                raise AssertionError("empty config should require a grid hash")
        """,
        tmp_path,
        grib_file,
        defaults="ignoreGridHash: true\n",
    )


def test_object_isolation_snapshot_and_concurrency(tmp_path, grib_file):
    run_python(
        """
        import sys
        from concurrent.futures import ThreadPoolExecutor
        from pygribjump import GribJump, GribJumpException, PathExtractionRequest

        def extract(gj):
            request = PathExtractionRequest(sys.argv[1], "file", 0, "", 0, [(0, 5)], "wrong")
            return next(iter(gj.extract_from_paths([request]))).values_flat.size

        for strict_first in (True, False):
            strict_config = {"ignoreGridHash": False, "threads": 2, "cache": {"enabled": False}}
            loose_config = {"ignoreGridHash": True}
            first = GribJump(strict_config if strict_first else loose_config)
            second = GribJump(loose_config if strict_first else strict_config)
            strict, loose = (first, second) if strict_first else (second, first)
            strict_config["ignoreGridHash"] = True
            loose_config["ignoreGridHash"] = False
            strict_config["cache"]["enabled"] = True
            # Initialize ecCodes definitions before exercising concurrent extraction.
            assert extract(loose) == 5

            def accepted():
                for _ in range(8):
                    assert extract(loose) == 5

            def rejected():
                for _ in range(8):
                    try:
                        extract(strict)
                    except GribJumpException as error:
                        assert "Grid hash" in str(error)
                    else:
                        raise AssertionError("strict object changed configuration")

            with ThreadPoolExecutor(max_workers=2) as pool:
                success = pool.submit(accepted)
                failure = pool.submit(rejected)
                success.result()
                failure.result()
        """,
        tmp_path,
        grib_file,
    )


def test_nested_config_servermap_and_process_conflicts(tmp_path, grib_file):
    run_python(
        """
        from pygribjump import GribJump, GribJumpException, configure_process
        from pygribjump._internal import _GribJump
        config = {
            "threads": 2,
            "cache": {"enabled": False, "size": 7},
            "server": {"port": 9876},
            "logging": {"progress": "error"},
            "servermap": [{"fdb": "localhost:9000", "gribjump": "localhost:9777"}],
        }
        first = GribJump(config=config)
        second = GribJump(config=config)
        native = _GribJump(config=config)
        configure_process({"threads": 2, "cache.size": 7})
        GribJump({})
        config["cache"]["size"] = 8
        for create in (GribJump, _GribJump, configure_process):
            try:
                create(config)
            except GribJumpException as error:
                assert "cache.size" in str(error)
            else:
                raise AssertionError("conflicting process settings were accepted")
        configure_process({"threads": 2, "cache": {"size": 7}})
        GribJump({"servermap": []})
        try:
            GribJump({"servermap": [{"fdb": "localhost:9000"}]})
        except GribJumpException:
            pass
        else:
            raise AssertionError("servermap was not passed to the native configuration")
        """,
        tmp_path,
        grib_file,
    )


@pytest.mark.parametrize("configure_first", [False, True])
def test_process_configuration_and_request_creation(tmp_path, grib_file, configure_first):
    run_python(
        f"""
        from pygribjump import ExtractionRequest, GribJump, GribJumpException, configure_process
        if {configure_first!r}:
            configure_process({{"threads": 2, "cache": {{"enabled": False}}}})
        request = ExtractionRequest("class=od", [(0, 1)])
        try:
            GribJump({{"threads": 2}})
        except GribJumpException as error:
            assert not {configure_first!r}
            assert "threads" in str(error)
        else:
            assert {configure_first!r}
        """,
        tmp_path,
        grib_file,
        defaults="threads: 1\n",
    )


def test_concurrent_process_initializers(tmp_path, grib_file):
    run_python(
        """
        from concurrent.futures import ThreadPoolExecutor
        from threading import Barrier
        from pygribjump import GribJump, GribJumpException
        start = Barrier(2)
        def create(threads):
            start.wait()
            try:
                GribJump(config={"threads": threads})
                return threads
            except GribJumpException as error:
                assert "threads" in str(error)
                return 0
        with ThreadPoolExecutor(max_workers=2) as pool:
            a = pool.submit(create, 2)
            b = pool.submit(create, 3)
            winners = [a.result(), b.result()]
        assert winners in ([2, 0], [0, 3])
        GribJump(config={"threads": sum(winners)})
        """,
        tmp_path,
        grib_file,
    )


def test_environment_overrides_python_config(tmp_path, grib_file):
    run_python(
        """
        import sys
        from pygribjump import GribJump, PathExtractionRequest
        first = GribJump({"ignoreGridHash": False, "threads": 2})
        second = GribJump({"threads": 4})  # both resolve to the environment's 3
        request = PathExtractionRequest(sys.argv[1], "file", 0, "", 0, [(0, 5)])
        assert next(iter(first.extract_from_paths([request]))).values_flat.size == 5
        """,
        tmp_path,
        grib_file,
        overrides={"GRIBJUMP_IGNORE_GRID": "1", "GRIBJUMP_THREADS": "3"},
    )


@pytest.mark.parametrize("create", [GribJump, configure_process])
@pytest.mark.parametrize(
    "config, message",
    [
        ("config.yaml", "dictionary"),
        ([], "dictionary"),
        (1, "dictionary"),
        ({1: True}, "keys must be strings"),
        ({"cache": {1: True}}, "config.cache keys must be strings"),
        ({"cache": {"enabled": None}}, "config.cache.enabled"),
        ({"servermap": [None]}, "servermap must contain dictionaries"),
        ({"servermap": [{"fdb": object()}]}, "servermap"),
        ({"threads": object()}, "config.threads"),
    ],
)
def test_invalid_config_types(create, config, message):
    with pytest.raises(TypeError, match=message):
        create(config)


@pytest.mark.parametrize("create", [GribJump, configure_process])
def test_empty_config_key_is_rejected(create):
    with pytest.raises(ValueError, match="keys must not be empty"):
        create({"": True})


@pytest.mark.parametrize("create", [GribJump, configure_process])
def test_cyclic_config_is_rejected(create):
    config = {}
    config["cycle"] = config
    with pytest.raises(ValueError, match="nesting"):
        create(config)


def test_configure_process_requires_a_dict():
    with pytest.raises(TypeError, match="dictionary"):
        configure_process(None)


@pytest.mark.parametrize("create", [GribJump, configure_process])
def test_integer_overflow_is_reported(create):
    with pytest.raises(OverflowError):
        create({"threads": 2**100})

"""
NOTE:

To be able to run the tests pyfdb and pygribjump need to find the native libraries.
If not installed into a default location export the following environent
variables in the shell running pytest:
    GRIBJUMP_HOME = <cmake-build-root-for-gribjump> or <custom-install-location>
    FDB5_DIR = <cmake-build-root-for-fdb> or <custom-install-location>
"""

import os
import pathlib
import shutil

import numpy
import pytest
import yaml

# Do not reorder the imports otherwise this will trigger an assertion in eckits
# library registration handling.
import pygribjump as pygj
import pyfdb


@pytest.fixture(scope="function")
def read_only_fdb_setup(data_path, tmp_path) -> pathlib.Path:
    """
    Creates a FDB setup in this tests temp directory.
    Test FDB currently reads all grib files in `tests/data`
    """
    db_store_path = tmp_path / "db_store"
    db_store_path.mkdir(exist_ok=True)
    schema_path = tmp_path / "schema"
    config = dict(
        type="local",
        engine="toc",
        schema=str(schema_path),
        spaces=[
            dict(
                handler="Default",
                roots=[
                    {"path": str(db_store_path)},
                ],
            )
        ],
    )
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.dump(config))
    shutil.copy(data_path / "schema", schema_path)
    os.environ["FDB5_CONFIG_FILE"] = str(config_path)

    # grib_files = data_path.glob("*.grib")
    grib_files = [data_path / "synth11.grib"]
    fdb = pyfdb.FDB()
    for f in grib_files:
        fdb.archive(f.read_bytes())
    return tmp_path


def test_extract_simple_sunshine_case(read_only_fdb_setup) -> None:
    grib_jump = pygj.GribJump()
    reqstrs = [
        {
            "domain": "g",
            "levtype": "sfc",
            "date": "20230508",
            "time": "1200",
            "step": "1",
            "param": "151130",
            "class": "od",
            "type": "fc",
            "stream": "oper",
            "expver": "0001",
        },
    ]
    ranges = [
        [(0, 6)],
    ]

    polyrequest = [(a, b) for a, b in zip(reqstrs, ranges)]

    expected = numpy.array([0.0, numpy.nan, numpy.nan, 3.0, 4.0, numpy.nan])

    actual = grib_jump.extract(polyrequest)
    assert numpy.array_equal(expected, actual[0][0][0][0], equal_nan=True)

def test_axes(read_only_fdb_setup) -> None:
    gribjump = pygj.GribJump()
    req = {
        "date": "20230508",
    }
    ax1 = gribjump.axes(req, level=1) # {'class': ['od'], 'date': ['20230508'], 'domain': ['g'], 'expver': ['0001'], 'stream': ['oper'], 'time': ['1200']}
    ax2 = gribjump.axes(req, level=2) # {'class': ['od'], 'date': ['20230508'], 'domain': ['g'], 'expver': ['0001'], 'levtype': ['sfc'], 'stream': ['oper'], 'time': ['1200'], 'type': ['fc']}
    ax3 = gribjump.axes(req, level=3) # {'class': ['od'], 'date': ['20230508'], 'domain': ['g'], 'expver': ['0001'], 'levelist': [''], 'levtype': ['sfc'], 'param': ['151130'], 'step': ['1'], 'stream': ['oper'], 'time': ['1200'], 'type': ['fc']}

    assert len(ax1.keys()) == 6
    assert len(ax2.keys()) == 8
    assert len(ax3.keys()) == 11

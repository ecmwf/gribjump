
import os
import pathlib
import shutil

import numpy as np
import pytest
import yaml

from pygribjump import GribJump, ExtractionRequest, ExtractionResult, PathExtractionRequest
import pyfdb

context = {
    "source": "pytest",
}

@pytest.fixture(scope="function")
def gj_63_fdb_setup(data_path: pathlib.Path, tmp_path: pathlib.Path) -> pathlib.Path:
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
    shutil.copy(data_path / "schema_63", schema_path)
    os.environ["FDB5_CONFIG_FILE"] = str(config_path)

    grib_file = data_path / "gj-63.grib"

    fdb = pyfdb.FDB()
    fdb.archive(grib_file.read_bytes())
    fdb.flush()

    return tmp_path


def test_gj_63_subhourly(gj_63_fdb_setup) -> None:
    
    gj = GribJump()

    request_base = {
        "class": "d1",
        "stream": "oper",
        "expver": "xxxx",
        "date": "20000101",
        "time": "1200",
        "param": "129",
        "levtype": "ml",
        "type": "fc",
        "dataset": "on-demand-extremes-dt",
        "georef": "u09tvk",
        "levelist": "89",
        "timespan": "none",
    }
    
    # Worked pre-GJ-63 fix.
    request1 = request_base.copy()
    request1["step"] = "1h47m"

    # Failed pre-GJ-63 fix.
    request2 = request_base.copy()
    request2["step"] = "107m"

    ranges = [(0, 10)]
    for result in gj.extract([ExtractionRequest(request1, ranges)]):
        assert np.array_equal(result.values_flat, np.arange(0.0, 10.0))
        
    for result in gj.extract([ExtractionRequest(request2, ranges)]):
        assert np.array_equal(result.values_flat, np.arange(0.0, 10.0))

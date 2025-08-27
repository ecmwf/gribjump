# A standalone example of using gribjump with a temporary FDB setup.
# See also: test_pygribjump.py for more varied examples.

# Note: Depending on whether fdb and gribjump binaries are installed in your python environment,
# you may need to set FDB5_DIR and GRIBJUMP_DIR to your build/install directory to help `findlibs` 
# find the shared libraries.

import os
from pygribjump import GribJump, ExtractionRequest
import pyfdb
import pathlib
import numpy as np

# --- Environment
walkthrough_files_dir = pathlib.Path(__file__).parent / "walkthrough_files"
os.environ["FDB_HOME"] = str(walkthrough_files_dir)
os.environ["GRIBJUMP_CONFIG_FILE"] = str(walkthrough_files_dir / "gribjump_config.yaml")
os.environ["FDB_ENABLE_GRIBJUMP"] = "1" # <-- Enable gribjump plugin for FDB. 

# --- Setup FDB
# Write a few synthetic GRIB fields to the FDB
# This is a synthetic field with 100 points, many of which are masked out.
grib_file = walkthrough_files_dir / "synth11.grib"

requests = [
    {
        "domain": "g",
        "levtype": "sfc",
        "date": "20230508",
        "time": "1200",
        "step": str(step),
        "param": "151130",
        "class": "od",
        "type": "fc",
        "stream": "oper",
        "expver": "xxxx",
    }
    for step in range(3)
]

fdb = pyfdb.FDB()
for request in requests:
    # Write the data. Note, this will automatically create a gribjump index for this file.
    fdb.archive(grib_file.read_bytes(), key=request)
fdb.flush()

# --- Use gribjump to extract some data

gribjump = GribJump()

# Define some regions to extract from each field.
# Each range is inclusive of the start index, exclusive of the end index.
ranges = [
    [(0, 10), (90, 100)], # First 10 and last 10 points
    [(0, 100)], # All points
    [(0, 1), (1, 2), (92, 93)], # Single points
]

gribjump_request=[ExtractionRequest(requests[i], ranges[i]) for i in range(len(ranges))]
result_iterator = gribjump.extract(gribjump_request)

for i, result in enumerate(result_iterator):
    print("Request", i, " - ", requests[i])
    print("Ranges: ", ranges[i])
    print("Values:", result.values)
    print()
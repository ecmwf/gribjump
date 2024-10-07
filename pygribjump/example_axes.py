
import os
os.environ['GRIBJUMP_HOME'] = "/path/to/gribjump/build"
os.environ['GRIBJUMP_CONFIG_FILE'] = "/path/to/gribjump/config.yaml"
os.environ['GRIBJUMP_IGNORE_GRID'] = "1"

import pygribjump as pygj

gj = pygj.GribJump()

req = {
    "class": "od",
    "expver": "0001",
    "stream": "oper",
    "date": "20230710",
    "time": "1200",
    "domain": "g",
    "type": "fc"
}

ax = gj.axes(req)
print(ax)

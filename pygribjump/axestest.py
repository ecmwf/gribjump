
import os
os.environ['GRIBJUMP_HOME'] = '/Users/maby/repo/mars-client-bundle/build/'
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
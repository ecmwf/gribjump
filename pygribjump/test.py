
import os
os.environ['GRIBJUMP_HOME'] = '/Users/maby/repo/mars-client-bundle/build/'
import pygribjump as pygj

gj = pygj.GribJump()

reqstrs = [
    "retrieve,class=od,type=fc,stream=oper,expver=0001,repres=gg,grid=45/45,levtype=sfc,param=151130,date=20230710,time=1200,step=1,domain=g",
    "retrieve,class=od,type=fc,stream=oper,expver=0001,repres=gg,grid=45/45,levtype=sfc,param=151130,date=20230710,time=1200,step=2,domain=g",
    "retrieve,class=od,type=fc,stream=oper,expver=0001,repres=gg,grid=45/45,levtype=sfc,param=151130,date=20230710,time=1200,step=3/4,domain=g"
]

ranges = [
    [(1,5), (10,20)],
    [(1,2), (3,4), (5,6)],
    [(1,20)]
]

polyrequest = [
    (reqstrs[i], ranges[i]) for i in range(len(reqstrs))
]

res = gj.extract(polyrequest)

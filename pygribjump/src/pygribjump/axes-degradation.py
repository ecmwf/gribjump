# Debugging the slow down of the axes call.
import os
# os.environ['GRIBJUMP_HOME'] = "/path/to/gribjump/build"
# os.environ['GRIBJUMP_CONFIG_FILE'] = "/path/to/gribjump/config.yaml"
import pygribjump as pygj
from time import time, sleep

gribjump = pygj.GribJump()

req = {
    "class": "od",
    "expver": "0001",
    "stream": "oper",
    "date": "20241021",
    "time": "1200",
    "domain": "g",
    "type": "fc"
}

Nrepeats = 10
sleep_time = 0.1
times = []
for i in range(Nrepeats):
    t0 = time()
    ax = gribjump.axes(req)
    t1 = time()
    delta = t1-t0
    times.append(delta)
    print(f"Time taken for axes call: {delta:.3f} seconds")

    sleep(sleep_time)

# Summary, print all times
print(f"Summary of {Nrepeats} calls to axes:")
print(f"Min: {min(times):.3f} seconds")
print(f"Max: {max(times):.3f} seconds")
print(f"Mean: {sum(times)/Nrepeats:.3f} seconds")
print(f"Std dev: {sum([(t-sum(times)/Nrepeats)**2 for t in times])**0.5:.3f} seconds")

# Plotting
# import matplotlib.pyplot as plt
# plt.plot(times)
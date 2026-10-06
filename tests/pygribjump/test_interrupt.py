# (C) Copyright 2026- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

"""Real SIGINT delivery, isolated from the pytest process."""

import os
import signal
import subprocess
import sys
import threading

import pytest


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX SIGINT")
def test_sigint_during_local_task_wait(grib_file, tmp_path):
    message = grib_file.read_bytes()
    count = 5000
    path = tmp_path / "many-fields.grib"
    path.write_bytes(message * count)
    config = tmp_path / "gribjump.yaml"
    config.write_text("type: local\n")

    script = """
import signal
import sys
original_handler = signal.getsignal(signal.SIGINT)
from pygribjump import GribJump, PathExtractionRequest

path, size, count = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
requests = [PathExtractionRequest(path, "file", i * size, "", 0, [(0, 100)])
            for i in range(count)]
gj = GribJump()
try:
    gj.extract_from_paths(requests)
except KeyboardInterrupt:
    print("INTERRUPTED", flush=True)
else:
    raise AssertionError("expected KeyboardInterrupt")

# Cancellation must not destroy the instance/pool or replace signal handlers.
assert signal.getsignal(signal.SIGINT) is original_handler
result = next(iter(gj.extract_from_paths(requests[:1])))
assert len(result.values_flat) == 100
print("RECOVERED", flush=True)
"""
    env = dict(os.environ, GRIBJUMP_DEBUG="1", GRIBJUMP_THREADS="1",
               GRIBJUMP_IGNORE_GRID="1", GRIBJUMP_CONFIG_FILE=str(config))
    process = subprocess.Popen(
        [sys.executable, "-u", "-c", script, str(path), str(len(message)), str(count)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env,
    )
    output = []
    sent = threading.Event()

    def read_output():
        for line in process.stdout:
            output.append(line)
            # Synchronise with the native wait, not Python imports/request setup.
            if "Waiting for 1 task" in line and not sent.is_set():
                process.send_signal(signal.SIGINT)
                sent.set()

    reader = threading.Thread(target=read_output, daemon=True)
    reader.start()
    try:
        process.wait(timeout=10)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=2)
        reader.join(timeout=2)
        process.stdout.close()

    log = "".join(output)
    assert sent.is_set(), log
    assert process.returncode == 0, log
    assert "Cancelling pending tasks; waiting for running tasks to finish..." in log, log
    assert "INTERRUPTED" in log, log
    assert "RECOVERED" in log, log
    # The native wait must take its interruption path, not complete normally and
    # leave Python to notice the signal only after the C++ call returns.
    assert "All tasks complete" not in log.split("INTERRUPTED", 1)[0], log

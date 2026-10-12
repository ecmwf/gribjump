# (C) Copyright 2026- ECMWF. Licensed under the Apache Licence Version 2.0.
"""Status API/CLI smoke tests against the real threaded server."""
import json
import os
import shutil
import socket
import subprocess
import threading
import time

import pytest

from pygribjump import GribJump, GribJumpException
from pygribjump._internal import _GribJump


@pytest.fixture
def server(tmp_path):
    executable = shutil.which("gribjump-server")
    if not executable:
        pytest.skip("gribjump-server not built")
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        port = reserved.getsockname()[1]
    config = tmp_path / "server.yaml"
    config.write_text(f"server:\n  port: {port}\nthreads: 2\n")
    env = dict(os.environ, GRIBJUMP_CONFIG_FILE=str(config), GRIBJUMP_THREADS="2")
    with (tmp_path / "server.log").open("w+") as log:
        process = subprocess.Popen([executable], env=env, stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    log.seek(0)
                    pytest.fail(log.read())
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                        break
                except OSError:
                    time.sleep(0.02)
            else:
                pytest.fail("server did not start")
            yield f"127.0.0.1:{port}"
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


@pytest.mark.parametrize("api", [GribJump, _GribJump])
def test_no_servers(api):
    with pytest.raises(GribJumpException, match="requires remote servers"):
        api({}).status()


@pytest.mark.parametrize("api", [GribJump, _GribJump])
def test_json_status(api, server):
    config = {
        "type": "remote", "uri": server,
        "clientProtocolVersion": 3,
        "servermap": [
            {"fdb": "catalogue:9000", "gribjump": server},
            {"mars": "catalogue:9001", "gribjump": server},
        ],
    }
    result = api(config).status(timeout=1)
    assert isinstance(result, str)
    document = json.loads(result)
    assert len(document["servers"]) == 1
    peer = document["servers"][0]
    assert peer["endpoint"] == server
    assert peer["state"] == "ok"
    assert peer["status"]["threads"]["worker_count"] == 2
    assert peer["status"]["active_requests"] == []
    assert peer["status"]["server"]["uptime_seconds"] >= 0


def test_partial_failure_timeout_and_gil_release(server):
    # A Python peer must acquire the GIL to accept the connection while status
    # is waiting in C++. Leave it open without replying to exercise the deadline.
    with socket.socket() as stalled:
        stalled.bind(("127.0.0.1", 0))
        stalled.listen()
        stalled.settimeout(3)
        endpoint = f"127.0.0.1:{stalled.getsockname()[1]}"
        accepted = threading.Event()
        finish = threading.Event()

        def serve():
            with stalled.accept()[0]:
                accepted.set()
                finish.wait(3)

        thread = threading.Thread(target=serve)
        thread.start()
        try:
            config = {"uri": server, "servermap": [{"fdb": "catalogue:9000", "gribjump": endpoint}]}
            result = json.loads(GribJump(config).status(timeout=0.15, ctx={"job": "status-test"}))
            assert accepted.is_set()
            peers = {peer["endpoint"]: peer for peer in result["servers"]}
            assert peers[server]["state"] == "ok"
            assert peers[endpoint]["state"] == "unreachable"
            assert "timed out" in peers[endpoint]["error"]
        finally:
            finish.set()
            thread.join(timeout=4)


def test_cli_dumps_json_with_partial_failure(server, tmp_path):
    executable = shutil.which("gribjump-status")
    if not executable:
        pytest.skip("gribjump-status not built")
    with socket.socket() as unreachable:
        unreachable.bind(("127.0.0.1", 0))  # reserved, but not listening
        endpoint = f"127.0.0.1:{unreachable.getsockname()[1]}"
        config = tmp_path / "client.yaml"
        config.write_text(
            f"uri: {server}\nservermap:\n  - fdb: catalogue:9000\n    gribjump: {endpoint}\n"
        )
        result = subprocess.run(
            [executable, f"--config={config}", "--timeout=1"],
            capture_output=True, text=True, timeout=5,
        )
    assert result.returncode == 0, result.stderr
    document = json.loads(result.stdout)  # stdout must be JSON only
    peers = {peer["endpoint"]: peer for peer in document["servers"]}
    assert peers[server]["state"] == "ok"
    assert peers[endpoint]["state"] == "unreachable"

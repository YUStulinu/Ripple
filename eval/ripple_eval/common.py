"""Paths, the Ripple binary, and a small client for its HTTP API."""
from __future__ import annotations

import http.client
import json
import socket
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
RESULTS = ROOT / "results"
DOCS = ROOT / "docs"
EXE = ".exe" if sys.platform == "win32" else ""
RIPPLE = ROOT / "engine" / "target" / "release" / f"ripple{EXE}"
TSBS_FILE = DATA / "tsbs" / "cpu-100x24h.prom"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Server:
    """A `ripple serve` process on a free port."""

    def __init__(self, data_dir: Path, *extra: str, log: Path | None = None):
        self.port = free_port()
        self.data_dir = data_dir
        self.log = open(log, "ab") if log else subprocess.DEVNULL
        t0 = time.perf_counter()
        self.proc = subprocess.Popen([str(RIPPLE), "serve", "--data", str(data_dir), "--addr", f"127.0.0.1:{self.port}", *extra],
                                     stdout=self.log, stderr=self.log)
        self.wait_ready()
        self.startup_seconds = time.perf_counter() - t0

    def wait_ready(self, timeout: float = 120):
        end = time.time() + timeout
        while time.time() < end:
            if self.proc.poll() is not None:
                raise RuntimeError(f"ripple exited with {self.proc.returncode}")
            try:
                self.get("/api/v1/labels")
                return
            except OSError:
                time.sleep(0.02)
        raise TimeoutError("ripple did not start")

    def conn(self) -> http.client.HTTPConnection:
        return http.client.HTTPConnection("127.0.0.1", self.port, timeout=600)

    def get(self, path: str, conn: http.client.HTTPConnection | None = None):
        c = conn or self.conn()
        c.request("GET", path)
        r = c.getresponse()
        body = r.read()
        if r.status != 200:
            raise RuntimeError(f"{path}: {r.status} {body[:300]!r}")
        return json.loads(body) if r.getheader("Content-Type", "").startswith("application/json") else body.decode()

    def post(self, body: bytes, conn: http.client.HTTPConnection | None = None) -> int:
        return self.post_checked(body, conn)[0]

    def post_checked(self, body: bytes, conn: http.client.HTTPConnection | None = None) -> tuple[int, int]:
        """(status, samples rejected as out of order or too old)."""
        c = conn or self.conn()
        c.request("POST", "/api/v1/import/prometheus", body=body, headers={"Content-Type": "text/plain"})
        r = c.getresponse()
        r.read()
        return r.status, int(r.getheader("X-Ripple-Rejected", "0"))

    def query_range(self, q: str, start_ms: int, end_ms: int, step_ms: int, conn=None) -> list:
        qs = urllib.parse.urlencode({"query": q, "start": start_ms / 1000, "end": end_ms / 1000, "step": step_ms / 1000})
        return self.get(f"/api/v1/query_range?{qs}", conn)["data"]["result"]

    def query(self, q: str, t_ms: int, conn=None) -> dict:
        qs = urllib.parse.urlencode({"query": q, "time": t_ms / 1000})
        return self.get(f"/api/v1/query?{qs}", conn)["data"]

    def stats(self) -> dict:
        return self.get("/api/v1/status/tsdb")["data"]

    def kill(self):
        """Hard kill (TerminateProcess on Windows, SIGKILL elsewhere): no shutdown code runs."""
        self.proc.kill()
        self.proc.wait()

    def stop(self):
        self.kill()
        if self.log is not subprocess.DEVNULL:
            self.log.close()


def save(name: str, obj) -> None:
    RESULTS.mkdir(exist_ok=True)
    (RESULTS / name).write_text(json.dumps(obj, indent=1))

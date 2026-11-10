"""Recovery after a forced stop: no acknowledged sample may be lost.

    python -m ripple_eval.crash [--rounds 30]

A writer POSTs batches of 200 series x 5 timestamps (simulated time moves 10 s per timestamp)
to a Ripple server whose block range is 10 minutes, so head cuts, WAL checkpoints and
compactions happen every few seconds of real time. After a random 0.3 to 2.5 s the server is
killed (TerminateProcess on Windows, SIGKILL on Linux: no shutdown code runs), restarted on the
same directory, and everything is read back with /api/v1/export. Checks:

1. every sample of every acknowledged batch (HTTP 204) is present, with the same value;
2. the batch in flight at the kill is either entirely present or entirely absent;
3. nothing else is present (no duplicates, no invented samples).

A process kill tests the software's crash consistency (torn records, interrupted block writes and
compactions). It cannot test power loss, where data not yet fsynced would be lost from the OS
cache; that is what the WAL fsync before each acknowledgement is for.

Writes results/crash.json.
"""
from __future__ import annotations

import argparse
import random
import shutil
import threading
import time

from .common import DATA, Server, save

SERIES = 200
STEPS = 5


def batch_text(k: int, t0: int) -> tuple[bytes, dict]:
    lines, expect = [], {}
    for step in range(STEPS):
        t = t0 + (k * STEPS + step) * 10_000
        for s in range(SERIES):
            v = (k * 7919 + s * 31 + step) % 1000 / 8
            lines.append(f'crash_test{{series="{s}",group="{s % 7}"}} {v} {t}')
            expect[(str(s), t)] = v
    return ("\n".join(lines) + "\n").encode(), expect


def read_back(srv: Server) -> dict:
    text = srv.get("/api/v1/export?match[]=crash_test")
    out = {}
    dups = 0
    for line in text.splitlines():
        lab, v, t = line.rsplit(" ", 2)
        s = lab.split('series="')[1].split('"')[0]
        key = (s, int(t))
        dups += key in out
        out[key] = float(v)
    return out, dups


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rounds", type=int, default=30)
    a = ap.parse_args()
    d = DATA / "crash"
    shutil.rmtree(d, ignore_errors=True)
    rng = random.Random(3)
    acked: dict = {}
    k = 0
    t0 = 1_700_000_000_000
    rounds = []
    srv = Server(d, "--block-range", "10m")
    for r in range(a.rounds):
        stop = threading.Event()
        state = {"inflight": None, "acked_batches": 0, "error": None}

        def writer():
            nonlocal k
            c = srv.conn()
            while not stop.is_set():
                body, exp = batch_text(k, t0)
                state["inflight"] = exp
                try:
                    st, rejected = srv.post_checked(body, c)
                except OSError:
                    return  # the server died mid-request: this batch is unacknowledged
                if st != 204 or rejected:
                    state["error"] = f"status {st}, {rejected} samples rejected"
                    return
                acked.update(exp)
                state["inflight"] = None
                state["acked_batches"] += 1
                k += 1

        w = threading.Thread(target=writer)
        w.start()
        time.sleep(rng.uniform(0.3, 2.5))
        srv.kill()
        stop.set()
        w.join()
        if state["error"]:
            raise RuntimeError(f"round {r + 1}: {state['error']}")
        inflight = state["inflight"]
        if inflight is not None:
            k += 1  # never reuse the timestamps of a batch that may have been written
        t_restart = time.perf_counter()
        srv = Server(d, "--block-range", "10m")
        recovery = time.perf_counter() - t_restart
        got, dups = read_back(srv)
        st = srv.stats()
        missing = sum(1 for key, v in acked.items() if got.get(key) != v)
        inflight_present = 0 if inflight is None else sum(1 for key in inflight if key in got)
        inflight_ok = inflight is None or inflight_present in (0, len(inflight))
        if inflight is not None and inflight_present == len(inflight):
            acked.update(inflight)  # it made it to disk: from now on it must stay
        extra = sum(1 for key in got if key not in acked)
        rounds.append({"acked_batches": state["acked_batches"], "acked_samples_total": len(acked), "missing": missing,
                       "inflight_present": inflight_present, "inflight_all_or_nothing": inflight_ok, "extra": extra, "duplicates": dups,
                       "recovery_seconds": recovery, "blocks": len(st["blocks"]), "wal_truncated_bytes": st["wal_truncated_bytes"],
                       "replayed_samples": st["replayed_samples"]})
        print(f"round {r + 1:2d}: {state['acked_batches']:5d} batches, {len(acked):8d} acknowledged samples, missing {missing}, extra {extra}, duplicates {dups}, "
              f"in-flight {inflight_present}/{0 if inflight is None else len(inflight)}, restart {recovery:.2f} s, "
              f"{len(st['blocks'])} blocks, {st['wal_truncated_bytes']} torn bytes")
    srv.stop()
    summary = {"rounds": len(rounds), "samples_acknowledged": len(acked), "lost": sum(x["missing"] for x in rounds),
               "extra": sum(x["extra"] for x in rounds), "duplicates": sum(x["duplicates"] for x in rounds),
               "inflight_violations": sum(not x["inflight_all_or_nothing"] for x in rounds),
               "torn_tails_repaired": sum(x["wal_truncated_bytes"] > 0 for x in rounds),
               "recovery_seconds_max": max(x["recovery_seconds"] for x in rounds)}
    print(summary)
    save("crash.json", {"summary": summary, "rounds": rounds})


if __name__ == "__main__":
    main()

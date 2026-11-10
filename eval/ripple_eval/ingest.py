"""Write throughput and storage size: Ripple vs SQLite on the same TSBS-style data.

    python -m ripple_eval.ingest

1. `ripple load`: the engine reading the text file directly (parsing included), with the WAL
   fsynced every 10,000 samples, then without fsync.
2. Ripple over HTTP: 4 client threads each POST their own hosts' lines (10,000 per request, in
   time order) to /api/v1/import/prometheus; every request is acknowledged after the WAL fsync.
3. SQLite (the Python standard library's build), the same samples from the generator's arrays
   (no text parsing, which favours SQLite): table samples(series_id, ts, value) with primary key
   (series_id, ts), WITHOUT ROWID, WAL journal, synchronous=FULL, one transaction per 10,000
   samples (the same durability point as Ripple's batches).

Bytes per sample: Ripple's blocks (chunks + index) after compaction; SQLite's database file after
a checkpoint. Writes results/ingest.json.
"""
from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import threading
import time
from pathlib import Path

from . import tsbs
from .common import DATA, RIPPLE, TSBS_FILE, Server, save

BATCH = 10_000
THREADS = 4


def ripple_load(fsync: bool) -> dict:
    d = DATA / "bench-load"
    shutil.rmtree(d, ignore_errors=True)
    args = [str(RIPPLE), "load", "--data", str(d), "--batch", str(BATCH), str(TSBS_FILE)]
    if not fsync:
        args.insert(-1, "--no-fsync")
    out = subprocess.run(args, capture_output=True, text=True, check=True).stdout
    return json.loads(out)


def split_by_host(n_parts: int) -> list[Path]:
    """Splits the TSBS file into per-thread files, each holding whole hosts in time order."""
    parts = [DATA / "tsbs" / f"part{i}.prom" for i in range(n_parts)]
    if all(p.exists() for p in parts):
        return parts
    fs = [open(p, "w", newline="\n", buffering=1 << 22) for p in parts]
    with open(TSBS_FILE, buffering=1 << 22) as f:
        for line in f:
            h = line.index('hostname="host_') + 15
            n = int(line[h:line.index('"', h)])
            fs[n % n_parts].write(line)
    for x in fs:
        x.close()
    return parts


def chunks(path: Path, lines_per_chunk: int):
    """Yields byte chunks of about `lines_per_chunk` whole lines, without per-line Python work."""
    approx = lines_per_chunk * 235
    with open(path, "rb") as f:
        rest = b""
        while True:
            block = f.read(approx)
            if not block:
                if rest:
                    yield rest
                return
            block = rest + block
            cut = block.rfind(b"\n") + 1
            yield block[:cut]
            rest = block[cut:]


def ripple_http() -> dict:
    d = DATA / "bench-http"
    shutil.rmtree(d, ignore_errors=True)
    parts = split_by_host(THREADS)
    srv = Server(d)
    sent = [0] * THREADS
    errors = []

    def worker(i):
        c = srv.conn()
        for body in chunks(parts[i], BATCH):
            st = srv.post(body, c)
            if st != 204:
                errors.append(st)
                return
            sent[i] += body.count(b"\n")

    t0 = time.perf_counter()
    ts = [threading.Thread(target=worker, args=(i,)) for i in range(THREADS)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    secs = time.perf_counter() - t0
    st = srv.stats()
    srv.stop()
    if errors:
        raise RuntimeError(f"HTTP errors: {errors[:5]}")
    n = sum(sent)
    return {"samples": n, "seconds": secs, "samples_per_second": n / secs, "threads": THREADS, "batch": BATCH,
            "appended": st["appended"], "rejected": st["out_of_order"] + st["too_old"]}


def sqlite_load() -> dict:
    db = DATA / "bench-sqlite" / "tsbs.db"
    shutil.rmtree(db.parent, ignore_errors=True)
    db.parent.mkdir(parents=True)
    con = sqlite3.connect(db)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=FULL")
    ser = tsbs.series(100)
    cols = list(ser[0].labels)
    con.execute(f"CREATE TABLE series (id INTEGER PRIMARY KEY, {', '.join(c + ' TEXT' for c in cols)})")
    con.execute("CREATE INDEX series_field_host ON series(field, hostname)")
    con.execute("CREATE TABLE samples (series_id INTEGER, ts INTEGER, value REAL, PRIMARY KEY (series_id, ts)) WITHOUT ROWID")
    con.executemany(f"INSERT INTO series VALUES (?, {', '.join('?' * len(cols))})", [(i, *s.labels.values()) for i, s in enumerate(ser)])
    con.commit()
    n_series = len(ser)
    n = 0
    t0 = time.perf_counter()
    pending = []
    for ts, vals in tsbs.batches(100, 24):
        for t, row in zip(ts.tolist(), vals.tolist()):
            pending.extend(zip(range(n_series), [t] * n_series, row))
            if len(pending) >= BATCH:
                con.executemany("INSERT INTO samples VALUES (?, ?, ?)", pending)
                con.commit()
                n += len(pending)
                pending = []
    if pending:
        con.executemany("INSERT INTO samples VALUES (?, ?, ?)", pending)
        con.commit()
        n += len(pending)
    secs = time.perf_counter() - t0
    con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    con.close()
    size = db.stat().st_size
    return {"samples": n, "seconds": secs, "samples_per_second": n / secs, "bytes": size, "bytes_per_sample": size / n,
            "sqlite_version": sqlite3.sqlite_version}


def main():
    out = {}
    out["ripple_load_fsync"] = ripple_load(True)
    print("ripple load (fsync):", out["ripple_load_fsync"])
    out["ripple_load_no_fsync"] = ripple_load(False)
    print("ripple load (no fsync):", out["ripple_load_no_fsync"])
    out["ripple_http"] = ripple_http()
    print("ripple HTTP:", out["ripple_http"])
    out["sqlite"] = sqlite_load()
    print("sqlite:", out["sqlite"])
    # Leave a fsynced load in data/bench-load for the query benchmark.
    out["ripple_load_fsync_final"] = ripple_load(True)
    save("ingest.json", out)


if __name__ == "__main__":
    main()

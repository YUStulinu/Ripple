"""Query latency on the TSBS-style data: Ripple (PromQL over HTTP) vs SQLite (SQL, in-process).

    python -m ripple_eval.queries [--runs 30]

The query types are TSBS's "devops" queries, written once in PromQL and once in SQL against the
tables built by ripple_eval.ingest. Each run picks random hosts and a random time window
(seeded). The first run of every query is checked: Ripple's answer must equal SQLite's.

Bucket alignment: a PromQL range query evaluates `max_over_time(x[1m])` at the step ends, over
the window (t - 1m, t]; the SQL groups samples by the same windows.

Writes results/queries.json.
"""
from __future__ import annotations

import argparse
import math
import random
import sqlite3
import statistics
import time

from . import tsbs
from .common import DATA, Server, save

HOUR = 3_600_000
MINUTE = 60_000
START = tsbs.START_MS
END = START + 24 * HOUR
FIVE = ["usage_user", "usage_system", "usage_idle", "usage_nice", "usage_iowait"]


def hosts_re(hosts: list[str]) -> str:
    return "|".join(hosts)


def sql_ids(con, fields: list[str], hosts: list[str] | None) -> list[int]:
    q = f"SELECT id FROM series WHERE field IN ({','.join('?' * len(fields))})"
    args = list(fields)
    if hosts is not None:
        q += f" AND hostname IN ({','.join('?' * len(hosts))})"
        args += hosts
    return [r[0] for r in con.execute(q, args)]


def sql_buckets(con, agg: str, inner: str, fields, hosts, start, end, step, by_host=False, by_field=False):
    """Two-level aggregation: `inner` over each series' samples in (t - step, t], then `agg`
    across series grouped by field/host. Returns {(group..., t): value}."""
    ids = sql_ids(con, fields, hosts)
    groups = ", ".join(x for x, on in (("s.hostname", by_host), ("s.field", by_field)) if on)
    gsel = (groups + ", ") if groups else ""
    q = f"""
        WITH per AS (
            SELECT series_id, ? + ((ts - ? + ? - 1) / ?) * ? AS t, {inner}(value) AS v
            FROM samples
            WHERE series_id IN ({','.join(map(str, ids))}) AND ts > ? AND ts <= ?
            GROUP BY series_id, t)
        SELECT {gsel}per.t, {agg}(per.v) FROM per JOIN series s ON s.id = per.series_id
        GROUP BY {gsel}per.t"""
    rows = con.execute(q, (start, start, step, step, step, start - step, end)).fetchall()
    return {tuple(r[:-1]): r[-1] for r in rows}


def promql_points(result, keys) -> dict:
    out = {}
    for s in result:
        g = tuple(s["metric"].get(k, "") for k in keys)
        for t, v in s["values"]:
            out[g + (round(float(t) * 1000),)] = float(v)
    return out


def close(a: dict, b: dict) -> bool:
    if a.keys() != b.keys():
        return False
    return all(math.isclose(a[k], b[k], rel_tol=1e-9, abs_tol=1e-9) for k in a)


def cases(rng: random.Random, n_hosts: int):
    """(name, promql builder, sql builder) for TSBS's devops query types."""

    def pick(k):
        return [f"host_{i}" for i in rng.sample(range(n_hosts), k)]

    def window(hours):
        s = START + rng.randrange(0, (24 - hours) * 60) * MINUTE
        return s + MINUTE, s + hours * HOUR  # first step end, last step end

    def groupby(nf, nh, hours):
        def make():
            hs, (s, e) = pick(nh), window(hours)
            f = FIVE[:nf]
            sel = f'cpu{{field=~"{hosts_re(f)}",hostname=~"{hosts_re(hs)}"}}'
            pq = (f"max by (field) (max_over_time({sel}[1m]))", s, e, MINUTE, ["field"])
            sq = lambda con: sql_buckets(con, "MAX", "MAX", f, hs, s, e, MINUTE, by_field=True)
            return pq, sq
        return make

    def max_all(nh):
        def make():
            hs = pick(nh)
            s0 = START + rng.randrange(0, 16) * HOUR
            s, e = s0 + HOUR, s0 + 8 * HOUR
            pq = (f'max by (field) (max_over_time(cpu{{hostname=~"{hosts_re(hs)}"}}[1h]))', s, e, HOUR, ["field"])
            sq = lambda con: sql_buckets(con, "MAX", "MAX", tsbs.FIELDS, hs, s, e, HOUR, by_field=True)
            return pq, sq
        return make

    def double(nf):
        def make():
            f = tsbs.FIELDS[:nf]
            s0 = START + rng.randrange(0, 12) * HOUR
            s, e = s0 + HOUR, s0 + 12 * HOUR
            pq = (f'avg by (hostname, field) (avg_over_time(cpu{{field=~"{hosts_re(f)}"}}[1h]))', s, e, HOUR, ["hostname", "field"])
            sq = lambda con: sql_buckets(con, "AVG", "AVG", f, None, s, e, HOUR, by_host=True, by_field=True)
            return pq, sq
        return make

    def high_cpu(nh):
        def make():
            hs = None if nh is None else pick(nh)
            s0 = START + rng.randrange(0, 12) * HOUR
            s, e = s0 + 10_000, s0 + 12 * HOUR
            hsel = "" if hs is None else f',hostname=~"{hosts_re(hs)}"'
            pq = (f'cpu{{field="usage_user"{hsel}}} > 90', s, e, 10_000, ["hostname"])

            def sq(con):
                ids = sql_ids(con, ["usage_user"], hs)
                rows = con.execute(f"""SELECT s.hostname, x.ts, x.value FROM samples x JOIN series s ON s.id = x.series_id
                    WHERE x.series_id IN ({','.join(map(str, ids))}) AND x.ts >= ? AND x.ts <= ? AND x.value > 90""", (s, e)).fetchall()
                return {(h, t): v for h, t, v in rows}
            return pq, sq
        return make

    def lastpoint():
        def make():
            t = END - 10_000
            pq = ("cpu", None, t, None, ["hostname", "field"])

            def sq(con):
                rows = con.execute("""SELECT s.hostname, s.field, x.value FROM series s JOIN samples x ON x.series_id = s.id
                    AND x.ts = (SELECT MAX(ts) FROM samples y WHERE y.series_id = s.id AND y.ts <= ?)""", (t,)).fetchall()
                return {(h, f, t): v for h, f, v in rows}
            return pq, sq
        return make

    def orderby_limit():
        def make():
            t = START + rng.randrange(60, 24 * 60) * MINUTE
            s, e = t - 4 * MINUTE, t
            pq = ('max(max_over_time(cpu{field="usage_user"}[1m]))', s, e, MINUTE, [])
            sq = lambda con: sql_buckets(con, "MAX", "MAX", ["usage_user"], None, s, e, MINUTE)
            return pq, sq
        return make

    return [
        ("single-groupby-1-1-1", groupby(1, 1, 1)), ("single-groupby-1-1-12", groupby(1, 1, 12)), ("single-groupby-1-8-1", groupby(1, 8, 1)),
        ("single-groupby-5-1-1", groupby(5, 1, 1)), ("single-groupby-5-1-12", groupby(5, 1, 12)), ("single-groupby-5-8-1", groupby(5, 8, 1)),
        ("cpu-max-all-1", max_all(1)), ("cpu-max-all-8", max_all(8)),
        ("double-groupby-1", double(1)), ("double-groupby-5", double(5)), ("double-groupby-all", double(10)),
        ("high-cpu-1", high_cpu(1)), ("high-cpu-all", high_cpu(None)),
        ("lastpoint", lastpoint()), ("groupby-orderby-limit", orderby_limit()),
    ]


def run_promql(srv, conn, pq):
    q, s, e, step, keys = pq
    if s is None:  # instant query
        data = srv.query(q, e, conn)
        return {tuple(x["metric"].get(k, "") for k in keys) + (e,): float(x["value"][1]) for x in data["result"]}
    return promql_points(srv.query_range(q, s, e, step, conn), keys)


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=30)
    a = ap.parse_args()
    con = sqlite3.connect(DATA / "bench-sqlite" / "tsbs.db")
    con.execute("PRAGMA query_only=1")
    srv = Server(DATA / "bench-load")
    conn = srv.conn()
    out = {}
    rng = random.Random(11)
    for name, make in cases(rng, 100):
        rt, st = [], []
        agree = None
        for i in range(a.runs):
            pq, sq = make()
            t0 = time.perf_counter()
            r = run_promql(srv, conn, pq)
            rt.append(time.perf_counter() - t0)
            t0 = time.perf_counter()
            s = sq(con)
            st.append(time.perf_counter() - t0)
            if i == 0:
                agree = close(r, s)
                if not agree:
                    only = set(r) ^ set(s)
                    print(f"  {name}: MISMATCH ({len(r)} vs {len(s)} points; {len(only)} keys differ, e.g. {list(only)[:3]})")
        out[name] = {"points": len(r), "agree": agree,
                     "ripple_ms": {"median": statistics.median(rt) * 1e3, "p90": pct(rt, 90) * 1e3},
                     "sqlite_ms": {"median": statistics.median(st) * 1e3, "p90": pct(st, 90) * 1e3}}
        print(f"{name:24s} ripple {out[name]['ripple_ms']['median']:8.2f} ms   sqlite {out[name]['sqlite_ms']['median']:8.2f} ms   "
              f"{len(r):6d} points  agree={agree}")
    srv.stop()
    save("queries.json", out)


if __name__ == "__main__":
    main()

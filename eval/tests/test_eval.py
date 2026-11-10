"""Tests of the evaluation helpers. The end-to-end test needs the release build of ripple."""
import math
import sqlite3

import numpy as np
import pytest

from ripple_eval import common, crash, queries, tsbs


def test_tsbs_shape():
    s = tsbs.series(3)
    assert len(s) == 30
    assert s[0].labels["hostname"] == "host_0" and s[0].labels["field"] == "usage_user"
    assert set(s[0].labels) == {"hostname", "region", "datacenter", "rack", "os", "arch", "team", "service", "service_version",
                                "service_environment", "field"}
    assert s[0].labels["datacenter"] in tsbs.REGIONS[s[0].labels["region"]]
    # The same seed gives the same hosts.
    assert tsbs.hosts(5) == tsbs.hosts(5)


def test_random_walks_are_clamped_and_continuous():
    blocks = list(tsbs.walks(50, 20000, block=7000))
    v = np.vstack(blocks)
    assert v.shape == (20000, 50)
    assert v.min() >= 0 and v.max() <= 100
    steps = np.abs(np.diff(v, axis=0))
    assert steps.max() < 6  # N(0, 1) steps, also across block boundaries


def test_batches_timestamps():
    ts = np.concatenate([t for t, _ in tsbs.batches(1, 0.5)])
    assert ts[0] == tsbs.START_MS and len(ts) == 180
    assert (np.diff(ts) == tsbs.INTERVAL_MS).all()


def test_crash_batches_are_deterministic():
    a, ea = crash.batch_text(3, 0)
    b, eb = crash.batch_text(3, 0)
    assert a == b and ea == eb and len(ea) == crash.SERIES * crash.STEPS
    assert a.count(b"\n") == len(ea)


def test_sql_buckets_match_promql_windows():
    """max_over_time(x[1m]) at step end t covers (t - 1m, t]; the SQL grouping must agree."""
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE series (id INTEGER PRIMARY KEY, field TEXT, hostname TEXT)")
    con.execute("CREATE TABLE samples (series_id INTEGER, ts INTEGER, value REAL)")
    con.execute("INSERT INTO series VALUES (0, 'usage_user', 'host_0')")
    start = 1_000_000
    rows = [(0, start + k * 10_000, float(k)) for k in range(-6, 13)]
    con.executemany("INSERT INTO samples VALUES (?, ?, ?)", rows)
    got = queries.sql_buckets(con, "MAX", "MAX", ["usage_user"], ["host_0"], start, start + 120_000, 60_000, by_field=True)
    # Window (start - 60 s, start] holds k = -5..0, so the max is 0; then 6, then 12.
    assert got == {("usage_user", start): 0.0, ("usage_user", start + 60_000): 6.0, ("usage_user", start + 120_000): 12.0}


def test_promql_points_and_compare():
    r = [{"metric": {"field": "a"}, "values": [[1.5, "2"], [2.5, "NaN"]]}]
    p = queries.promql_points(r, ["field"])
    assert p[("a", 1500)] == 2.0 and math.isnan(p[("a", 2500)])
    assert queries.close({("a", 1): 1.0}, {("a", 1): 1.0 + 1e-12})
    assert not queries.close({("a", 1): 1.0}, {("a", 2): 1.0})


@pytest.mark.skipif(not common.RIPPLE.exists(), reason="needs the release build")
def test_server_round_trip(tmp_path):
    srv = common.Server(tmp_path / "db")
    try:
        assert srv.post(b'm{a="1"} 1.5 1000\nm{a="1"} 2.5 2000\n') == 204
        res = srv.query_range('m{a="1"}', 1000, 2000, 1000)
        assert res[0]["values"] == [[1.0, "1.5"], [2.0, "2.5"]]
        assert "m{a=\"1\"} 2.5 2000" in srv.get("/api/v1/export?match[]=m")
        srv.kill()
        srv = common.Server(tmp_path / "db")  # acknowledged data survives a hard kill
        assert srv.stats()["replayed_samples"] == 2
    finally:
        srv.stop()

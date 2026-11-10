"""Load generator following the Time Series Benchmark Suite's "devops cpu-only" use case.

    python -m ripple_eval.tsbs --hosts 100 --hours 24 --out ../data/tsbs/cpu-100x24h.prom

TSBS itself is not used (no third-party code runs here); this re-creates the shape of its data:
- one host per series group, tagged like TSBS: hostname, region, datacenter, rack, os, arch,
  team, service, service_version, service_environment (drawn from the same value sets);
- 10 CPU fields per host (usage_user, usage_system, ..., usage_guest_nice), each a random walk
  with N(0, 1) steps clamped to [0, 100], as TSBS's ClampedRandomWalkDistribution;
- one reading every 10 seconds, starting 2016-01-01T00:00:00Z (TSBS's defaults).

TSBS models a measurement `cpu` with fields; here each field is a series of the metric `cpu`
with a `field` label, so PromQL can group by it.

Output: the Prometheus text format with millisecond timestamps, in time order (every series at
t, then every series at t + 10 s), the order an agent would send them.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import numpy as np

START_MS = 1451606400000  # 2016-01-01T00:00:00Z
INTERVAL_MS = 10_000
FIELDS = ["usage_user", "usage_system", "usage_idle", "usage_nice", "usage_iowait", "usage_irq", "usage_softirq", "usage_steal", "usage_guest", "usage_guest_nice"]
REGIONS = {
    "us-east-1": ["us-east-1a", "us-east-1b", "us-east-1c", "us-east-1e"],
    "us-west-1": ["us-west-1a", "us-west-1b"],
    "us-west-2": ["us-west-2a", "us-west-2b", "us-west-2c"],
    "eu-west-1": ["eu-west-1a", "eu-west-1b", "eu-west-1c"],
    "eu-central-1": ["eu-central-1a", "eu-central-1b"],
    "ap-southeast-1": ["ap-southeast-1a", "ap-southeast-1b"],
    "ap-southeast-2": ["ap-southeast-2a", "ap-southeast-2b"],
    "ap-northeast-1": ["ap-northeast-1a", "ap-northeast-1c"],
    "sa-east-1": ["sa-east-1a", "sa-east-1b", "sa-east-1c"],
}
OS = ["Ubuntu16.10", "Ubuntu16.04LTS", "Ubuntu15.10"]
ARCH = ["x64", "x86"]
TEAM = ["SF", "NYC", "LON", "CHI"]
ENV = ["production", "staging", "test"]


@dataclass
class Series:
    labels: dict[str, str]

    def prefix(self) -> str:
        inner = ",".join(f'{k}="{v}"' for k, v in sorted(self.labels.items()))
        return f"cpu{{{inner}}}"


def hosts(n: int, seed: int = 123) -> list[dict[str, str]]:
    rng = np.random.default_rng(seed)
    out = []
    regions = list(REGIONS)
    for i in range(n):
        region = regions[rng.integers(len(regions))]
        dcs = REGIONS[region]
        out.append({
            "hostname": f"host_{i}", "region": region, "datacenter": dcs[rng.integers(len(dcs))], "rack": str(rng.integers(100)),
            "os": OS[rng.integers(3)], "arch": ARCH[rng.integers(2)], "team": TEAM[rng.integers(4)], "service": str(rng.integers(20)),
            "service_version": str(rng.integers(2)), "service_environment": ENV[rng.integers(3)],
        })
    return out


def series(n_hosts: int) -> list[Series]:
    return [Series({**h, "field": f}) for h in hosts(n_hosts) for f in FIELDS]


def walks(n_series: int, n_steps: int, seed: int = 7, block: int = 8640) -> Iterator[np.ndarray]:
    """Clamped random walks, yielded as (steps, series) blocks so memory stays bounded."""
    rng = np.random.default_rng(seed)
    cur = rng.uniform(0, 100, n_series)
    done = 0
    while done < n_steps:
        k = min(block, n_steps - done)
        out = np.empty((k, n_series))
        steps = rng.normal(0, 1, (k, n_series))
        for i in range(k):
            cur = np.clip(cur + steps[i], 0, 100)
            out[i] = cur
        yield out
        done += k


def batches(n_hosts: int, hours: float) -> Iterator[tuple[np.ndarray, np.ndarray]]:
    """(timestamps ms, values[steps, series]) blocks, in time order."""
    n_steps = int(hours * 3600 * 1000 // INTERVAL_MS)
    s = series(n_hosts)
    t0 = START_MS
    for vals in walks(len(s), n_steps):
        ts = t0 + np.arange(len(vals), dtype=np.int64) * INTERVAL_MS
        t0 = int(ts[-1]) + INTERVAL_MS
        yield ts, vals


def write(path: Path, n_hosts: int, hours: float) -> int:
    prefixes = [x.prefix() for x in series(n_hosts)]
    n = 0
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="\n", buffering=1 << 22) as f:
        for ts, vals in batches(n_hosts, hours):
            for t, row in zip(ts.tolist(), vals.tolist()):
                f.write("".join(f"{p} {v!r} {t}\n" for p, v in zip(prefixes, row)))
                n += len(row)
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hosts", type=int, default=100)
    ap.add_argument("--hours", type=float, default=24)
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()
    n = write(a.out, a.hosts, a.hours)
    print(f"{a.out}: {n} samples, {a.out.stat().st_size / 1e6:.0f} MB")


if __name__ == "__main__":
    main()

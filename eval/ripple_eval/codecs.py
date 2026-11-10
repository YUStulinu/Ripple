"""Bytes per sample for several encodings of the same samples (`ripple codecs`), on two datasets:

    python -m ripple_eval.codecs [--live-url http://127.0.0.1:9201]

- the TSBS-style data (full-precision random walks, a regular 10 s interval);
- real metrics from this computer, collected by the agent (exported from the live server with
  /api/v1/export into data/live-export.prom; the export is skipped if the file exists and no
  URL is given).

Writes results/codecs.json.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import urllib.parse
import urllib.request

from .common import DATA, RIPPLE, TSBS_FILE, save

LIVE = DATA / "live-export.prom"


def export_live(url: str):
    qs = urllib.parse.urlencode({"match[]": '{__name__=~".+"}'})
    with urllib.request.urlopen(f"{url}/api/v1/export?{qs}", timeout=600) as r:
        LIVE.write_bytes(r.read())


def run(path) -> dict:
    out = subprocess.run([str(RIPPLE), "codecs", str(path)], capture_output=True, text=True, check=True).stdout
    return json.loads(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live-url")
    a = ap.parse_args()
    if a.live_url:
        export_live(a.live_url)
    res = {"tsbs": run(TSBS_FILE)}
    print("tsbs:", json.dumps(res["tsbs"], indent=1))
    if LIVE.exists():
        res["live"] = run(LIVE)
        print("live:", json.dumps(res["live"], indent=1))
    save("codecs.json", res)


if __name__ == "__main__":
    main()

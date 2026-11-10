"""Plots for the README from results/*.json.

    python -m ripple_eval.report
"""
from __future__ import annotations

import json

from .common import DOCS, RESULTS

BLUE, ORANGE, GREEN, GRAY = "#2a78d6", "#eb6834", "#1baf7a", "#8a94a6"


def style(ax):
    ax.grid(axis="x", alpha=0.25)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)


def main():
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    DOCS.mkdir(exist_ok=True)
    codecs = json.loads((RESULTS / "codecs.json").read_text())
    names = [("gorilla", "Gorilla chunks (Ripple)"), ("decimal_zstd3", "decimal + delta + zstd\n(VictoriaMetrics' approach)"),
             ("zstd3_raw", "zstd -3 on raw columns"), ("zstd19_raw", "zstd -19 on raw columns"), ("raw", "uncompressed")]
    sets = [k for k in ("tsbs", "live") if k in codecs]
    titles = {"tsbs": "TSBS-style data (random-walk floats)", "live": "real metrics from this computer"}
    fig, axes = plt.subplots(1, len(sets), figsize=(6.2 * len(sets), 3.6), sharey=True)
    axes = axes if len(sets) > 1 else [axes]
    for ax, k in zip(axes, sets):
        vals = [codecs[k][n]["bytes_per_sample"] for n, _ in names]
        colors = [BLUE, ORANGE, GRAY, GRAY, GRAY]
        bars = ax.barh(range(len(names)), vals, color=colors, height=0.6)
        for b, v in zip(bars, vals):
            ax.text(v + 0.2, b.get_y() + b.get_height() / 2, f"{v:.2f}", va="center", fontsize=9)
        ax.set_yticks(range(len(names)), [n for _, n in names], fontsize=9)
        ax.invert_yaxis()
        ax.set_title(f"{titles[k]}: {codecs[k]['samples'] / 1e6:.2f} M samples, {codecs[k]['series']} series", loc="left", fontsize=10)
        ax.set_xlabel("bytes per sample")
        style(ax)
    fig.tight_layout()
    fig.savefig(DOCS / "codecs.png", dpi=130)
    plt.close(fig)

    q = json.loads((RESULTS / "queries.json").read_text())
    names = list(q)
    fig, ax = plt.subplots(figsize=(8.5, 0.32 * len(names) + 1.2))
    y = range(len(names))
    ax.barh([i - 0.2 for i in y], [q[n]["ripple_ms"]["median"] for n in names], height=0.38, color=BLUE, label="Ripple (PromQL over HTTP)")
    ax.barh([i + 0.2 for i in y], [q[n]["sqlite_ms"]["median"] for n in names], height=0.38, color=ORANGE, label="SQLite (SQL, in-process)")
    ax.set_yticks(list(y), names, fontsize=9)
    ax.invert_yaxis()
    ax.set_xscale("log")
    ax.set_xlabel("median latency (ms, log scale)")
    ax.legend(frameon=False, fontsize=9, loc="lower right")
    ax.set_title("TSBS devops queries on 100 hosts x 24 h (8.64 M samples)", loc="left", fontsize=10)
    style(ax)
    fig.tight_layout()
    fig.savefig(DOCS / "queries.png", dpi=130)
    print("wrote docs/codecs.png, docs/queries.png")


if __name__ == "__main__":
    main()

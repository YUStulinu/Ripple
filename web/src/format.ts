// Formatting and time helpers for the dashboard (pure functions, unit-tested).

export type Unit = "bytes" | "bytes/s" | "percent" | "seconds" | "ops" | "none";

const NICE_STEPS = [1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 10800, 21600, 43200, 86400];

/** A step (seconds) giving at most `maxPoints` points over `rangeSeconds`, rounded to a familiar value. */
export function pickStep(rangeSeconds: number, maxPoints = 300, minStep = 15): number {
  const raw = Math.max(rangeSeconds / maxPoints, minStep);
  return NICE_STEPS.find((s) => s >= raw) ?? Math.ceil(raw / 86400) * 86400;
}

/** The range-vector window for rate(): at least four scrapes (15 s each) and at least one step. */
export function rateWindow(step: number): string {
  const s = Math.max(60, step * 2);
  return s % 3600 === 0 ? `${s / 3600}h` : s % 60 === 0 ? `${s / 60}m` : `${s}s`;
}

/** Replaces $__rate_interval in a dashboard query with a window suited to the step. */
export function expandQuery(q: string, step: number): string {
  return q.replaceAll("$__rate_interval", rateWindow(step));
}

function scaled(v: number, base: number, units: string[]): string {
  let i = 0;
  let x = Math.abs(v);
  while (x >= base && i < units.length - 1) {
    x /= base;
    i++;
  }
  const digits = x >= 100 ? 0 : x >= 10 ? 1 : 2;
  return `${v < 0 ? "-" : ""}${x.toFixed(digits)} ${units[i]}`.trim();
}

export function formatValue(v: number | null | undefined, unit: Unit): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "–";
  if (!Number.isFinite(v)) return v > 0 ? "+Inf" : "-Inf";
  switch (unit) {
    case "bytes":
      return scaled(v, 1024, ["B", "KiB", "MiB", "GiB", "TiB"]);
    case "bytes/s":
      return scaled(v, 1024, ["B/s", "KiB/s", "MiB/s", "GiB/s"]);
    case "percent":
      return `${v.toFixed(Math.abs(v) >= 10 ? 1 : 2)}%`;
    case "seconds": {
      const a = Math.abs(v);
      if (a === 0) return "0 s";
      if (a < 1e-3) return `${(v * 1e6).toFixed(0)} µs`;
      if (a < 1) return `${(v * 1e3).toFixed(a < 0.01 ? 2 : 1)} ms`;
      return `${v.toFixed(2)} s`;
    }
    case "ops":
      return scaled(v, 1000, ["/s", "k/s", "M/s", "G/s"]);
    default:
      return scaled(v, 1000, ["", "k", "M", "G", "T"]);
  }
}

/** A short legend name for a series: the labels that differ between the series of a panel. */
export function legendNames(metrics: Record<string, string>[]): string[] {
  const keys = new Set<string>();
  for (const m of metrics) for (const k of Object.keys(m)) keys.add(k);
  const varying = [...keys].filter((k) => new Set(metrics.map((m) => m[k] ?? "")).size > 1).sort();
  return metrics.map((m) => {
    const ks = varying.length ? varying : Object.keys(m).sort();
    const parts = ks.filter((k) => m[k] !== undefined).map((k) => (k === "__name__" ? m[k] : `${k}=${m[k]}`));
    return parts.length ? parts.join(", ") : "value";
  });
}

/** Aligns a Prometheus matrix on the union of its timestamps (for uPlot). */
export function alignMatrix(result: { values: [number, string][] }[]): { xs: number[]; ys: (number | null)[][] } {
  const ts = new Set<number>();
  for (const s of result) for (const [t] of s.values) ts.add(t);
  const xs = [...ts].sort((a, b) => a - b);
  const index = new Map(xs.map((t, i) => [t, i]));
  const ys = result.map((s) => {
    const row: (number | null)[] = new Array(xs.length).fill(null);
    for (const [t, v] of s.values) row[index.get(t)!] = Number(v);
    return row;
  });
  return { xs, ys };
}

export function bytesPerSample(bytes: number, samples: number): string {
  return samples > 0 ? (bytes / samples).toFixed(2) : "–";
}

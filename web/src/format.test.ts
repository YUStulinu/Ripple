import { describe, expect, it } from "vitest";
import { alignMatrix, bytesPerSample, expandQuery, formatValue, legendNames, pickStep, rateWindow } from "./format";

describe("steps", () => {
  it("keeps about 300 points with familiar steps", () => {
    expect(pickStep(3600)).toBe(15);
    expect(pickStep(6 * 3600)).toBe(120);
    expect(pickStep(86400)).toBe(300);
    expect(pickStep(7 * 86400)).toBe(3600);
  });
  it("rate windows cover several scrapes", () => {
    expect(rateWindow(15)).toBe("1m");
    expect(rateWindow(300)).toBe("10m");
    expect(rateWindow(3600)).toBe("2h");
    expect(expandQuery("rate(x[$__rate_interval]) / rate(y[$__rate_interval])", 15)).toBe("rate(x[1m]) / rate(y[1m])");
  });
});

describe("values", () => {
  it("formats units", () => {
    expect(formatValue(1536, "bytes")).toBe("1.50 KiB");
    expect(formatValue(3 * 1024 ** 3, "bytes")).toBe("3.00 GiB");
    expect(formatValue(0.00042, "seconds")).toBe("420 µs");
    expect(formatValue(0.0123, "seconds")).toBe("12.3 ms");
    expect(formatValue(843000, "ops")).toBe("843 k/s");
    expect(formatValue(12.345, "percent")).toBe("12.3%");
    expect(formatValue(NaN, "none")).toBe("–");
    expect(formatValue(Infinity, "none")).toBe("+Inf");
    expect(bytesPerSample(700, 100)).toBe("7.00");
  });
});

describe("series", () => {
  it("legend shows only the labels that differ", () => {
    expect(legendNames([{ mode: "user", job: "node" }, { mode: "idle", job: "node" }])).toEqual(["mode=user", "mode=idle"]);
    expect(legendNames([{}])).toEqual(["value"]);
    expect(legendNames([{ __name__: "up", job: "a" }])).toEqual(["up, job=a"]);
  });
  it("aligns series with different timestamps", () => {
    const { xs, ys } = alignMatrix([
      { values: [[1, "1"], [2, "2"]] },
      { values: [[2, "5"], [3, "6"]] },
    ]);
    expect(xs).toEqual([1, 2, 3]);
    expect(ys).toEqual([[1, 2, null], [null, 5, 6]]);
  });
});

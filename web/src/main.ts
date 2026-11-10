import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import { DASHBOARDS, type Panel } from "./dashboards";
import { alignMatrix, bytesPerSample, expandQuery, formatValue, legendNames, pickStep, type Unit } from "./format";
import "./style.css";

const COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#a35fd8", "#d6a42a", "#e0457b", "#4fb3c8", "#7a8597"];
const $ = <T extends HTMLElement>(id: string) => document.getElementById(id) as T;

interface Series {
  metric: Record<string, string>;
  values: [number, string][];
}

function store<T>(key: string, def: T): T {
  try {
    const v = localStorage.getItem(key);
    return v ? (JSON.parse(v) as T) : def;
  } catch {
    return def;
  }
}
function keep(key: string, v: unknown) {
  try {
    localStorage.setItem(key, JSON.stringify(v));
  } catch {
    /* storage unavailable */
  }
}

async function api(path: string, params: Record<string, string | number>): Promise<any> {
  const body = new URLSearchParams(Object.entries(params).map(([k, v]) => [k, String(v)]));
  const r = await fetch(path, { method: "POST", body, headers: { "Content-Type": "application/x-www-form-urlencoded" } });
  const j = await r.json();
  if (j.status !== "success") throw new Error(j.error ?? r.statusText);
  return j.data;
}

async function rangeQuery(q: string, rangeS: number): Promise<{ series: Series[]; step: number }> {
  const end = Date.now() / 1000;
  const step = pickStep(rangeS);
  const data = await api("/api/v1/query_range", { query: expandQuery(q, step), start: end - rangeS, end, step });
  return { series: data.result as Series[], step };
}

const charts: uPlot[] = [];

function plot(el: HTMLElement, series: Series[], unit: Unit, names?: string[]): uPlot | null {
  el.innerHTML = "";
  if (!series.length) {
    el.innerHTML = `<div class="empty">No data in this range</div>`;
    return null;
  }
  const { xs, ys } = alignMatrix(series);
  const labels = names ?? legendNames(series.map((s) => s.metric));
  const css = getComputedStyle(document.documentElement);
  const grid = css.getPropertyValue("--grid").trim();
  const muted = css.getPropertyValue("--muted").trim();
  const axis = { stroke: muted, grid: { stroke: grid, width: 1 }, ticks: { stroke: grid, width: 1 } };
  const opts: uPlot.Options = {
    width: el.clientWidth || 500,
    height: 220,
    series: [
      {},
      ...labels.map((l, i) => ({
        label: l,
        stroke: COLORS[i % COLORS.length],
        width: 2,
        spanGaps: false,
        value: (_u: uPlot, v: number | null) => formatValue(v, unit),
      })),
    ],
    axes: [axis, { ...axis, size: 70, values: (_u: uPlot, vals: number[]) => vals.map((v) => formatValue(v, unit)) }],
    legend: { show: series.length <= 12 },
    cursor: { drag: { x: false, y: false } },
  };
  const u = new uPlot(opts, [xs, ...ys] as uPlot.AlignedData, el);
  charts.push(u);
  return u;
}

addEventListener("resize", () => {
  for (const u of charts) {
    const w = u.root.parentElement?.clientWidth;
    if (w) u.setSize({ width: w, height: 220 });
  }
});

// ---------------------------------------------------------------- views

let tab = store("ripple.tab", "host");
let rangeS = store("ripple.range", 3600);
let refreshS = store("ripple.refresh", 15);
let timer: number | undefined;

const TABS = [...DASHBOARDS.map((d) => [d.id, d.title]), ["explore", "Query console"], ["storage", "Storage"]];

function renderTabs() {
  const nav = $("tabs");
  nav.innerHTML = "";
  for (const [id, title] of TABS) {
    const b = document.createElement("button");
    b.textContent = title;
    b.className = id === tab ? "on" : "";
    b.onclick = () => {
      tab = id;
      keep("ripple.tab", id);
      renderTabs();
      render();
    };
    nav.append(b);
  }
}

async function panelView(container: HTMLElement, p: Panel) {
  const box = document.createElement("div");
  box.className = "panel";
  box.innerHTML = `<h3><span></span><span class="last"></span></h3><div class="chart"></div>`;
  (box.querySelector("h3 span") as HTMLElement).textContent = p.title;
  container.append(box);
  const chart = box.querySelector(".chart") as HTMLElement;
  try {
    const results = await Promise.all(p.queries.map((q) => rangeQuery(q, rangeS)));
    const series = results.flatMap((r) => r.series);
    let names: string[] | undefined;
    if (p.queries.length > 1) {
      // One series per query: name them after what differs between the queries.
      names = results.flatMap((r, i) => r.series.map(() => shortName(p.queries[i])));
    }
    plot(chart, series, p.unit, names);
    const last = series.map((s) => Number(s.values.at(-1)?.[1]));
    if (series.length === 1) (box.querySelector(".last") as HTMLElement).textContent = formatValue(last[0], p.unit);
  } catch (e) {
    chart.innerHTML = `<div class="err"></div>`;
    (chart.firstChild as HTMLElement).textContent = (e as Error).message;
  }
}

function shortName(q: string): string {
  const m = q.match(/histogram_quantile\(([\d.]+)/);
  if (m) return `p${Math.round(Number(m[1]) * 100)}`;
  const n = q.match(/([a-z_]+)\)?\s*$/i) ?? q.match(/\(([a-z_:]+)/i);
  return n ? n[1] : q;
}

async function dashboardView(id: string) {
  const d = DASHBOARDS.find((x) => x.id === id)!;
  const view = $("view");
  const grid = document.createElement("div");
  grid.className = "grid";
  view.append(grid);
  await Promise.all(d.panels.map((p) => panelView(grid, p)));
}

function exploreView() {
  const view = $("view");
  const history = store<string[]>("ripple.history", []);
  view.innerHTML = `<div class="console">
    <textarea id="q" spellcheck="false" placeholder='e.g. sum by (mode) (rate(node_cpu_seconds_total[1m]))'></textarea>
    <div class="row"><button class="run" id="go">Run</button><span>Ctrl+Enter runs the query</span><span id="took"></span></div>
    <div class="panel"><div class="chart" id="out"></div></div>
    <div id="table"></div>
    <div class="history"><strong>Recent</strong><ul id="hist"></ul></div>
  </div>`;
  const ta = $<HTMLTextAreaElement>("q");
  ta.value = store("ripple.query", 'sum by (mode) (rate(node_cpu_seconds_total{mode!="idle"}[1m]))');
  const renderHistory = () => {
    const ul = $("hist");
    ul.innerHTML = "";
    for (const h of history) {
      const li = document.createElement("li");
      li.textContent = h;
      li.onclick = () => {
        ta.value = h;
        run();
      };
      ul.append(li);
    }
  };
  const run = async () => {
    const q = ta.value.trim();
    if (!q) return;
    keep("ripple.query", q);
    if (history[0] !== q) {
      history.unshift(q);
      history.splice(20);
      keep("ripple.history", history);
      renderHistory();
    }
    const t0 = performance.now();
    const out = $("out");
    const table = $("table");
    try {
      const { series, step } = await rangeQuery(q, rangeS);
      $("took").textContent = `${series.length} series, step ${step} s, ${(performance.now() - t0).toFixed(0)} ms`;
      plot(out, series, "none");
      const names = legendNames(series.map((s) => s.metric));
      table.innerHTML = `<table><thead><tr><th>Series</th><th>Last value</th></tr></thead><tbody></tbody></table>`;
      const tb = table.querySelector("tbody")!;
      series.slice(0, 200).forEach((s, i) => {
        const tr = document.createElement("tr");
        tr.innerHTML = `<td></td><td></td>`;
        (tr.children[0] as HTMLElement).textContent = `{${Object.entries(s.metric).map(([k, v]) => `${k}="${v}"`).join(", ")}}` || names[i];
        (tr.children[1] as HTMLElement).textContent = s.values.at(-1)?.[1] ?? "";
        tb.append(tr);
      });
    } catch (e) {
      out.innerHTML = `<div class="err"></div>`;
      (out.firstChild as HTMLElement).textContent = (e as Error).message;
      table.innerHTML = "";
      $("took").textContent = "";
    }
  };
  ta.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) {
      e.preventDefault();
      run();
    }
  });
  $("go").onclick = run;
  renderHistory();
  run();
}

async function storageView() {
  const view = $("view");
  const r = await fetch("/api/v1/status/tsdb");
  const st = (await r.json()).data;
  const stat = (k: string, v: string) => `<div class="stat"><div class="k">${k}</div><div class="v">${v}</div></div>`;
  const blocks = st.blocks as any[];
  view.innerHTML = `<div class="stats">
      ${stat("Series in the head", formatValue(st.head.series, "none"))}
      ${stat("Samples in the head", formatValue(st.head.samples, "none"))}
      ${stat("Samples in blocks", formatValue(st.block_samples, "none"))}
      ${stat("Bytes per sample (blocks)", bytesPerSample(st.block_bytes, st.block_samples))}
      ${stat("Block storage", formatValue(st.block_bytes, "bytes"))}
      ${stat("Write-ahead log", formatValue(st.wal_bytes, "bytes"))}
      ${stat("Head cuts / compactions", `${st.head_cuts} / ${st.compactions}`)}
      ${stat("Rejected samples", formatValue(st.out_of_order + st.too_old, "none"))}
    </div>
    <div class="panel"><table><thead><tr><th>Block</th><th>From</th><th>To</th><th>Level</th><th>Series</th><th>Samples</th><th>Bytes</th><th>Bytes/sample</th></tr></thead><tbody>
    ${blocks
      .map(
        (b) => `<tr><td>${b.id}</td><td>${new Date(b.mint).toISOString().slice(0, 16).replace("T", " ")}</td><td>${new Date(b.end).toISOString().slice(0, 16).replace("T", " ")}</td>
        <td>${b.level}</td><td>${b.series}</td><td>${b.samples}</td><td>${formatValue(b.chunk_bytes + b.index_bytes, "bytes")}</td><td>${bytesPerSample(b.chunk_bytes + b.index_bytes, b.samples)}</td></tr>`,
      )
      .join("") || `<tr><td colspan="8">No blocks yet: the head is cut into a block once it spans 1.5 block ranges (3 hours by default).</td></tr>`}
    </tbody></table></div>`;
}

function render() {
  for (const u of charts.splice(0)) u.destroy();
  $("view").innerHTML = "";
  if (tab === "explore") exploreView();
  else if (tab === "storage") storageView();
  else dashboardView(tab);
}

function schedule() {
  if (timer) clearInterval(timer);
  // The console is not refreshed automatically: the user runs queries there.
  if (refreshS > 0) timer = window.setInterval(() => tab !== "explore" && render(), refreshS * 1000);
}

$<HTMLSelectElement>("range").value = String(rangeS);
$<HTMLSelectElement>("refresh").value = String(refreshS);
$<HTMLSelectElement>("range").onchange = (e) => {
  rangeS = Number((e.target as HTMLSelectElement).value);
  keep("ripple.range", rangeS);
  render();
};
$<HTMLSelectElement>("refresh").onchange = (e) => {
  refreshS = Number((e.target as HTMLSelectElement).value);
  keep("ripple.refresh", refreshS);
  schedule();
};
renderTabs();
render();
schedule();

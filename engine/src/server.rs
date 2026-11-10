//! HTTP API: ingestion in the Prometheus text format, the Prometheus query API, Ripple's own
//! metrics, and the dashboard's static files.
//!
//! | Endpoint | |
//! |---|---|
//! | `POST /api/v1/import/prometheus` | text-format samples; 204 once they are in the fsynced WAL |
//! | `GET/POST /api/v1/query` | `query`, `time` |
//! | `GET/POST /api/v1/query_range` | `query`, `start`, `end`, `step` |
//! | `GET /api/v1/series` | `match[]` |
//! | `GET /api/v1/labels`, `/api/v1/label/<name>/values` | |
//! | `GET /api/v1/export` | `match[]`, `start`, `end`: raw samples in the text format |
//! | `GET /api/v1/status/tsdb` | storage statistics |
//! | `GET /metrics` | Ripple's own metrics |

use crate::metrics::Histogram;
use crate::promql::{self, Expr};
use crate::storage::{Db, Matcher};
use serde_json::json;
use std::collections::BTreeMap;
use std::io::Read;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tiny_http::{Header, Method, Request, Response, Server};

pub fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

pub fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < b.len() => {
                let hex = |c: u8| (c as char).to_digit(16);
                match (hex(b[i + 1]), hex(b[i + 2])) {
                    (Some(h), Some(l)) => {
                        out.push((h * 16 + l) as u8);
                        i += 2;
                    }
                    _ => out.push(b'%'),
                }
            }
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub fn parse_query_string(q: &str) -> Vec<(String, String)> {
    q.split('&')
        .filter(|kv| !kv.is_empty())
        .map(|kv| {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            (percent_decode(k), percent_decode(v))
        })
        .collect()
}

/// A time parameter: Unix seconds (with fractions) or RFC 3339 is not supported; seconds only.
fn parse_time(s: &str) -> Result<i64, String> {
    s.parse::<f64>().map(|x| (x * 1000.0).round() as i64).map_err(|_| format!("bad time {s:?} (Unix seconds expected)"))
}

fn parse_step(s: &str) -> Result<i64, String> {
    match s.parse::<f64>() {
        Ok(x) => Ok((x * 1000.0).round() as i64),
        Err(_) => promql::parse_duration(s),
    }
}

fn selector_matchers(s: &str) -> Result<Vec<Matcher>, String> {
    match promql::parse(s)? {
        Expr::Selector { matchers, range: None, .. } => Ok(matchers),
        _ => Err(format!("{s:?} is not a series selector")),
    }
}

pub struct State {
    pub db: Arc<Db>,
    pub web: Option<PathBuf>,
    pub started: Instant,
    pub http: Mutex<BTreeMap<String, Histogram>>,
    pub requests: Mutex<BTreeMap<(String, u16), u64>>,
}

type Resp = Response<std::io::Cursor<Vec<u8>>>;

fn json_resp(code: u16, v: serde_json::Value) -> Resp {
    Response::from_data(serde_json::to_vec(&v).unwrap())
        .with_status_code(code)
        .with_header(Header::from_bytes("Content-Type", "application/json").unwrap())
}

fn error(code: u16, kind: &str, msg: &str) -> Resp {
    json_resp(code, json!({"status": "error", "errorType": kind, "error": msg}))
}

fn ok(data: serde_json::Value) -> Resp {
    json_resp(200, json!({"status": "success", "data": data}))
}

fn text(code: u16, body: String, ctype: &str) -> Resp {
    Response::from_data(body.into_bytes()).with_status_code(code).with_header(Header::from_bytes("Content-Type", ctype).unwrap())
}

impl State {
    fn handle(&self, req: &mut Request) -> (String, Resp) {
        let url = req.url().to_string();
        let (path, qs) = url.split_once('?').unwrap_or((&url, ""));
        let mut params = parse_query_string(qs);
        let mut body = Vec::new();
        if *req.method() == Method::Post {
            if let Err(e) = req.as_reader().take(256 << 20).read_to_end(&mut body) {
                return ("other".into(), error(400, "bad_data", &e.to_string()));
            }
            let form = req.headers().iter().any(|h| h.field.equiv("Content-Type") && h.value.as_str().starts_with("application/x-www-form-urlencoded"));
            if form {
                params.extend(parse_query_string(&String::from_utf8_lossy(&body)));
            }
        }
        let get = |k: &str| params.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str());
        let handler = match path {
            "/api/v1/import/prometheus" => "import",
            "/api/v1/query" => "query",
            "/api/v1/query_range" => "query_range",
            "/api/v1/series" => "series",
            "/api/v1/labels" => "labels",
            "/api/v1/export" => "export",
            "/api/v1/status/tsdb" => "status",
            "/metrics" => "metrics",
            p if p.starts_with("/api/v1/label/") => "label_values",
            _ => "static",
        };
        let resp = match handler {
            "import" => {
                if *req.method() != Method::Post {
                    return (handler.into(), error(405, "bad_data", "POST the samples"));
                }
                let text_body = String::from_utf8_lossy(&body);
                match self.db.append_text(&text_body, now_ms()) {
                    Ok(r) => {
                        let mut resp = Response::from_data(Vec::new()).with_status_code(204);
                        if r.out_of_order + r.too_old > 0 {
                            resp = resp.with_header(Header::from_bytes("X-Ripple-Rejected", (r.out_of_order + r.too_old).to_string()).unwrap());
                        }
                        resp
                    }
                    Err(e) if e.starts_with("line ") => error(400, "bad_data", &e),
                    Err(e) => error(500, "internal", &e.to_string()),
                }
            }
            "query" => {
                let Some(q) = get("query") else { return (handler.into(), error(400, "bad_data", "query is required")) };
                let t = match get("time").map(parse_time).unwrap_or(Ok(now_ms())) {
                    Ok(t) => t,
                    Err(e) => return (handler.into(), error(400, "bad_data", &e)),
                };
                match promql::instant(&self.db, q, t) {
                    Ok(v) => ok(promql::to_json(&v, t)),
                    Err(e) => error(400, "bad_data", &e),
                }
            }
            "query_range" => {
                let args = (|| -> Result<(String, i64, i64, i64), String> {
                    let q = get("query").ok_or("query is required")?.to_string();
                    let start = parse_time(get("start").ok_or("start is required")?)?;
                    let end = parse_time(get("end").ok_or("end is required")?)?;
                    let step = parse_step(get("step").ok_or("step is required")?)?;
                    Ok((q, start, end, step))
                })();
                match args.and_then(|(q, s, e, st)| promql::range(&self.db, &q, s, e, st)) {
                    Ok(v) => ok(promql::to_json(&v, 0)),
                    Err(e) => error(400, "bad_data", &e),
                }
            }
            "series" => {
                let mut out = std::collections::BTreeSet::new();
                for (k, v) in &params {
                    if k == "match[]" {
                        match selector_matchers(v) {
                            Ok(ms) => out.extend(self.db.series(&ms)),
                            Err(e) => return (handler.into(), error(400, "bad_data", &e)),
                        }
                    }
                }
                ok(json!(out.iter().map(|l| l.to_json()).collect::<Vec<_>>()))
            }
            "labels" => ok(json!(self.db.label_names())),
            "label_values" => {
                let name = percent_decode(path.trim_start_matches("/api/v1/label/").trim_end_matches("/values"));
                ok(json!(self.db.label_values(&name)))
            }
            "export" => {
                let start = get("start").map(parse_time).unwrap_or(Ok(i64::MIN));
                let end = get("end").map(parse_time).unwrap_or(Ok(i64::MAX));
                let (Ok(start), Ok(end)) = (start, end) else { return (handler.into(), error(400, "bad_data", "bad start or end")) };
                let mut s = String::new();
                for (k, v) in &params {
                    if k == "match[]" {
                        let ms = match selector_matchers(v) {
                            Ok(m) => m,
                            Err(e) => return (handler.into(), error(400, "bad_data", &e)),
                        };
                        for (l, samples) in self.db.select(&ms, start, end) {
                            let name = l.to_string();
                            for (t, x) in samples {
                                s.push_str(&format!("{name} {x} {t}\n"));
                            }
                        }
                    }
                }
                text(200, s, "text/plain; version=0.0.4")
            }
            "status" => ok(self.db.stats()),
            "metrics" => text(200, self.own_metrics(), "text/plain; version=0.0.4"),
            _ => self.static_file(path),
        };
        (handler.into(), resp)
    }

    fn static_file(&self, path: &str) -> Resp {
        let Some(root) = &self.web else { return text(404, "not found\n".into(), "text/plain") };
        let rel = path.trim_start_matches('/');
        if rel.split('/').any(|c| c == "..") {
            return text(400, "bad path\n".into(), "text/plain");
        }
        let mut p = root.join(if rel.is_empty() { "index.html" } else { rel });
        if !p.is_file() {
            p = root.join("index.html");
        }
        match std::fs::read(&p) {
            Ok(b) => {
                let ct = match p.extension().and_then(|e| e.to_str()) {
                    Some("html") => "text/html; charset=utf-8",
                    Some("js") => "text/javascript",
                    Some("css") => "text/css",
                    Some("svg") => "image/svg+xml",
                    Some("json") => "application/json",
                    _ => "application/octet-stream",
                };
                Response::from_data(b).with_header(Header::from_bytes("Content-Type", ct).unwrap())
            }
            Err(_) => text(404, "not found\n".into(), "text/plain"),
        }
    }

    fn own_metrics(&self) -> String {
        use std::fmt::Write;
        let st = self.db.stats();
        let c = self.db.counters.lock().unwrap().clone();
        let mut s = String::new();
        let mut g = |name: &str, kind: &str, help: &str, v: f64| {
            let _ = writeln!(s, "# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {v}");
        };
        g("ripple_samples_appended_total", "counter", "Samples written.", c.appended as f64);
        g("ripple_samples_rejected_total", "counter", "Samples rejected as out of order or too old.", (c.out_of_order + c.too_old) as f64);
        g("ripple_series_created_total", "counter", "Series created.", c.series_created as f64);
        g("ripple_head_series", "gauge", "Series in the in-memory head.", st["head"]["series"].as_f64().unwrap_or(0.0));
        g("ripple_head_samples", "gauge", "Samples in the in-memory head.", st["head"]["samples"].as_f64().unwrap_or(0.0));
        g("ripple_head_chunk_bytes", "gauge", "Compressed bytes in the head.", st["head"]["chunk_bytes"].as_f64().unwrap_or(0.0));
        g("ripple_blocks", "gauge", "Persisted blocks.", st["blocks"].as_array().map(|a| a.len()).unwrap_or(0) as f64);
        g("ripple_block_bytes", "gauge", "Bytes in blocks (chunks and index).", st["block_bytes"].as_f64().unwrap_or(0.0));
        g("ripple_block_samples", "gauge", "Samples in blocks.", st["block_samples"].as_f64().unwrap_or(0.0));
        g("ripple_wal_bytes", "gauge", "Bytes in the write-ahead log.", st["wal_bytes"].as_f64().unwrap_or(0.0));
        g("ripple_head_cuts_total", "counter", "Blocks written from the head.", c.head_cuts as f64);
        g("ripple_compactions_total", "counter", "Block merges.", c.compactions as f64);
        g("ripple_uptime_seconds", "gauge", "Seconds since start.", self.started.elapsed().as_secs_f64());
        let _ = writeln!(s, "# HELP ripple_wal_fsync_duration_seconds WAL fsync latency.\n# TYPE ripple_wal_fsync_duration_seconds histogram");
        c.fsync_seconds.write(&mut s, "ripple_wal_fsync_duration_seconds", "");
        let _ = writeln!(s, "# HELP ripple_http_request_duration_seconds HTTP request latency by handler.\n# TYPE ripple_http_request_duration_seconds histogram");
        for (h, hist) in self.http.lock().unwrap().iter() {
            hist.write(&mut s, "ripple_http_request_duration_seconds", &format!("handler=\"{h}\""));
        }
        let _ = writeln!(s, "# HELP ripple_http_requests_total HTTP requests by handler and status code.\n# TYPE ripple_http_requests_total counter");
        for ((h, code), n) in self.requests.lock().unwrap().iter() {
            let _ = writeln!(s, "ripple_http_requests_total{{handler=\"{h}\",code=\"{code}\"}} {n}");
        }
        s
    }
}

/// Serves until the process ends. Background maintenance runs every `maintain_every`.
pub fn serve(state: Arc<State>, addr: &str, threads: usize, maintain_every: std::time::Duration) -> std::io::Result<()> {
    let server = Arc::new(Server::http(addr).map_err(|e| std::io::Error::other(e.to_string()))?);
    eprintln!("ripple listening on http://{addr}");
    {
        let db = state.db.clone();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(maintain_every);
                if let Err(e) = db.maintain(now_ms()) {
                    eprintln!("maintenance: {e}");
                }
            }
        });
    }
    let mut handles = vec![];
    for _ in 0..threads {
        let server = server.clone();
        let state = state.clone();
        handles.push(std::thread::spawn(move || {
            for mut req in server.incoming_requests() {
                let t0 = Instant::now();
                let (handler, resp) = state.handle(&mut req);
                let code = resp.status_code().0;
                let _ = req.respond(resp);
                let dt = t0.elapsed().as_secs_f64();
                state.http.lock().unwrap().entry(handler.clone()).or_insert_with(Histogram::latency).observe(dt);
                *state.requests.lock().unwrap().entry((handler, code)).or_insert(0) += 1;
            }
        }));
    }
    for h in handles {
        let _ = h.join();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decoding() {
        assert_eq!(percent_decode("a%3Db+c%7B%22"), "a=b c{\"");
        assert_eq!(percent_decode("100%"), "100%");
        let p = parse_query_string("query=sum(rate(x%5B5m%5D))&match%5B%5D=up&time=1.5");
        assert_eq!(p[0].1, "sum(rate(x[5m]))");
        assert_eq!(p[1], ("match[]".into(), "up".into()));
        assert_eq!(parse_time("1.5"), Ok(1500));
        assert_eq!(parse_step("15s"), Ok(15000));
        assert_eq!(parse_step("30"), Ok(30000));
    }
}

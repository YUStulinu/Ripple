use ripple::promql;
use ripple::server::{self, State, now_ms};
use ripple::storage::{Db, Options};
use serde_json::json;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const USAGE: &str = "usage:
  ripple serve  --data DIR [--addr 127.0.0.1:9201] [--web DIR] [--block-range 2h] [--retention 0] [--no-fsync] [--threads N]
  ripple load   --data DIR [--batch 10000] [--block-range 2h] [--no-fsync] [--no-compact] FILE...
  ripple query  --data DIR QUERY [--time SECONDS | --start S --end S --step DUR]
  ripple stats  --data DIR
  ripple codecs FILE...      compare chunk encodings on text-format samples (see ripple::codecs)";

fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1).cloned())
}

fn has(args: &[String], name: &str) -> bool {
    args.iter().any(|a| a == name)
}

/// Positional arguments: everything that is neither a flag nor a flag's value.
fn positional(args: &[String]) -> Vec<String> {
    let switches = ["--no-fsync", "--no-compact"];
    let mut out = vec![];
    let mut i = 0;
    while i < args.len() {
        if args[i].starts_with("--") {
            i += if switches.contains(&args[i].as_str()) { 1 } else { 2 };
        } else {
            out.push(args[i].clone());
            i += 1;
        }
    }
    out
}

fn options(args: &[String]) -> Result<Options, String> {
    let mut o = Options::default();
    if let Some(r) = flag(args, "--block-range") {
        o.block_range = promql::parse_duration(&r)?;
    }
    if let Some(r) = flag(args, "--retention") {
        o.retention = if r == "0" { 0 } else { promql::parse_duration(&r)? };
    }
    o.fsync = !has(args, "--no-fsync");
    Ok(o)
}

fn open(args: &[String]) -> Result<Db, String> {
    let dir = flag(args, "--data").ok_or("--data is required")?;
    Db::open(&PathBuf::from(dir), options(args)?).map_err(|e| e.to_string())
}

fn run(args: Vec<String>) -> Result<(), String> {
    let cmd = args.first().ok_or(USAGE)?.clone();
    let args = &args[1..];
    match cmd.as_str() {
        "serve" => {
            let t0 = Instant::now();
            let db = Arc::new(open(args)?);
            let st = db.stats();
            eprintln!(
                "opened in {:.2} s: {} head series, {} samples replayed, {} blocks, {} bytes cut from the WAL tail",
                t0.elapsed().as_secs_f64(),
                st["head"]["series"],
                st["replayed_samples"],
                st["blocks"].as_array().map(|a| a.len()).unwrap_or(0),
                st["wal_truncated_bytes"]
            );
            let threads = flag(args, "--threads").and_then(|x| x.parse().ok()).unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4));
            let state = Arc::new(State {
                db,
                web: flag(args, "--web").map(PathBuf::from),
                started: Instant::now(),
                http: Mutex::new(Default::default()),
                requests: Mutex::new(Default::default()),
            });
            let addr = flag(args, "--addr").unwrap_or_else(|| "127.0.0.1:9201".into());
            server::serve(state, &addr, threads, Duration::from_secs(10)).map_err(|e| e.to_string())
        }
        "load" => {
            let db = open(args)?;
            let batch_size: usize = flag(args, "--batch").and_then(|x| x.parse().ok()).unwrap_or(10_000);
            let files = positional(args);
            if files.is_empty() {
                return Err("no input files".into());
            }
            let t0 = Instant::now();
            let mut n = 0u64;
            let mut rejected = 0u64;
            let mut batch = String::with_capacity(batch_size * 256);
            let mut lines = 0usize;
            let mut maint = Duration::ZERO;
            let mut batches = 0u64;
            let send = |batch: &mut String, n: &mut u64, rejected: &mut u64| -> Result<(), String> {
                let res = db.append_text(batch, now_ms())?;
                *n += res.appended as u64;
                *rejected += (res.out_of_order + res.too_old) as u64;
                batch.clear();
                Ok(())
            };
            for f in &files {
                let r = BufReader::with_capacity(1 << 20, std::fs::File::open(f).map_err(|e| format!("{f}: {e}"))?);
                for line in r.lines() {
                    batch.push_str(&line.map_err(|e| e.to_string())?);
                    batch.push('\n');
                    lines += 1;
                    if lines.is_multiple_of(batch_size) {
                        send(&mut batch, &mut n, &mut rejected)?;
                        batches += 1;
                        // Maintenance as a server would run it, timed separately.
                        if batches.is_multiple_of(100) {
                            let m0 = Instant::now();
                            db.maintain(i64::MIN).map_err(|e| e.to_string())?;
                            maint += m0.elapsed();
                        }
                    }
                }
            }
            if !batch.is_empty() {
                send(&mut batch, &mut n, &mut rejected)?;
            }
            db.flush().map_err(|e| e.to_string())?;
            let secs = t0.elapsed().as_secs_f64();
            // Persist the rest of the head and compact, so the size covers everything.
            let c0 = Instant::now();
            if !has(args, "--no-compact") {
                db.cut_head(true).map_err(|e| e.to_string())?;
                while db.compact().map_err(|e| e.to_string())? > 0 {}
            }
            let st = db.stats();
            let bs = st["block_samples"].as_f64().unwrap_or(0.0).max(1.0);
            println!(
                "{}",
                json!({
                    "samples": n, "rejected": rejected, "seconds": secs, "samples_per_second": n as f64 / secs,
                    "maintenance_seconds": maint.as_secs_f64(), "final_flush_and_compaction_seconds": c0.elapsed().as_secs_f64(),
                    "fsync": db.opts.fsync, "batch": batch_size,
                    "block_samples": st["block_samples"], "block_bytes": st["block_bytes"], "block_chunk_bytes": st["block_chunk_bytes"],
                    "bytes_per_sample": st["block_bytes"].as_f64().unwrap_or(0.0) / bs,
                    "chunk_bytes_per_sample": st["block_chunk_bytes"].as_f64().unwrap_or(0.0) / bs,
                    "blocks": st["blocks"].as_array().map(|a| a.len()), "series": db.series(&[ripple::storage::Matcher::new("__name__", "=~", ".+")?]).len(),
                })
            );
            Ok(())
        }
        "query" => {
            let db = open(args)?;
            let q = positional(args).into_iter().next().ok_or("no query")?;
            let t0 = Instant::now();
            let secs = |k: &str| flag(args, k).map(|x| x.parse::<f64>().map(|v| (v * 1000.0) as i64).map_err(|e| e.to_string()));
            let v = match (secs("--start"), secs("--end")) {
                (Some(s), Some(e)) => {
                    let step = promql::parse_duration(&flag(args, "--step").unwrap_or_else(|| "60s".into()))?;
                    promql::range(&db, &q, s?, e?, step)?
                }
                _ => {
                    let t = secs("--time").unwrap_or(Ok(now_ms()))?;
                    promql::instant(&db, &q, t)?
                }
            };
            println!("{}", promql::to_json(&v, secs("--time").and_then(|x| x.ok()).unwrap_or(now_ms())));
            eprintln!("{:.3} ms", t0.elapsed().as_secs_f64() * 1000.0);
            Ok(())
        }
        "stats" => {
            let db = open(args)?;
            println!("{}", serde_json::to_string_pretty(&db.stats()).unwrap());
            Ok(())
        }
        "codecs" => {
            let files = positional(args);
            println!("{}", serde_json::to_string_pretty(&ripple::codecs::compare_files(&files)?).unwrap());
            Ok(())
        }
        _ => Err(USAGE.into()),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let Err(e) = run(args) {
        eprintln!("{e}");
        std::process::exit(1);
    }
}

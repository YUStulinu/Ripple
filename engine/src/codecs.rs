//! Compares encodings of the same samples, for the evaluation.
//!
//! - `raw`: 8-byte timestamp + 8-byte value.
//! - `gorilla`: Ripple's chunks (120 samples each, as in Prometheus).
//! - `decimal_zstd`: the approach VictoriaMetrics documents for its storage, re-implemented
//!   here: blocks of up to 8192 samples per series; values converted to integers with one decimal
//!   exponent per block (falling back to raw floats when that is not exact); timestamps and
//!   values delta-encoded (second-order deltas for monotonic series), written as zigzag varints
//!   and compressed with zstd.
//! - `zstd_raw`: general-purpose compression of the raw columns (zstd levels 3 and 19).
//!
//! Every encoding is decoded again and checked bit for bit.

use crate::bits::{get_varint, put_varint};
use crate::chunk::{ChunkIter, ChunkWriter, SAMPLES_PER_CHUNK};
use crate::labels::{Labels, parse_line};
use serde_json::json;
use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::time::Instant;

pub const VM_BLOCK: usize = 8192;

pub fn gorilla_encode(s: &[(i64, f64)]) -> Vec<Vec<u8>> {
    s.chunks(SAMPLES_PER_CHUNK)
        .map(|c| {
            let mut w = ChunkWriter::new();
            for &(t, v) in c {
                w.append(t, v);
            }
            w.bytes()
        })
        .collect()
}

/// Smallest decimal exponent e (>= -18) with v == m * 10^e exactly, or None.
fn decimal_exp(v: f64) -> Option<i32> {
    if !v.is_finite() {
        return None;
    }
    if v == 0.0 {
        return Some(0);
    }
    for k in 0..=18 {
        let scaled = v * 10f64.powi(k);
        if scaled.abs() >= 9.0e15 {
            return None;
        }
        let m = scaled.round();
        if m / 10f64.powi(k) == v {
            return Some(-k);
        }
    }
    None
}

fn deltas(xs: &[i64], second: bool, out: &mut Vec<u8>) {
    let mut prev = 0i64;
    let mut prev_d = 0i64;
    for (i, &x) in xs.iter().enumerate() {
        let d = x.wrapping_sub(prev);
        put_varint(out, if second && i >= 2 { d.wrapping_sub(prev_d) } else { d });
        prev_d = d;
        prev = x;
    }
}

fn undeltas(buf: &[u8], n: usize, second: bool) -> Option<Vec<i64>> {
    let mut p = 0;
    let mut out = Vec::with_capacity(n);
    let (mut prev, mut prev_d) = (0i64, 0i64);
    for i in 0..n {
        let x = get_varint(buf, &mut p)?;
        let d = if second && i >= 2 { prev_d.wrapping_add(x) } else { x };
        prev = prev.wrapping_add(d);
        prev_d = d;
        out.push(prev);
    }
    Some(out)
}

/// One block: [flags][exponent][len ts][zstd ts][zstd values].
pub fn decimal_encode(s: &[(i64, f64)], level: i32) -> Vec<u8> {
    let ts: Vec<i64> = s.iter().map(|x| x.0).collect();
    let exp = s.iter().map(|x| decimal_exp(x.1)).try_fold(i32::MAX, |acc, e| e.map(|e| acc.min(e)));
    let ints: Option<Vec<i64>> = exp.and_then(|e| {
        s.iter()
            .map(|x| {
                let m = (x.1 * 10f64.powi(-e)).round();
                let back = if e >= 0 { m * 10f64.powi(e) } else { m / 10f64.powi(-e) };
                (m.abs() < 9.0e15 && back.to_bits() == x.1.to_bits()).then_some(m as i64)
            })
            .collect()
    });
    let mut out = vec![];
    let mut tbuf = vec![];
    deltas(&ts, true, &mut tbuf);
    let mut vbuf = vec![];
    let (flags, e) = match &ints {
        Some(v) => {
            let monotonic = v.windows(2).all(|w| w[1] >= w[0]);
            deltas(v, monotonic, &mut vbuf);
            (if monotonic { 2u8 } else { 1u8 }, exp.unwrap())
        }
        None => {
            for x in s {
                vbuf.extend_from_slice(&x.1.to_bits().to_le_bytes());
            }
            (0u8, 0)
        }
    };
    let tz = zstd::bulk::compress(&tbuf, level).unwrap();
    let vz = zstd::bulk::compress(&vbuf, level).unwrap();
    out.push(flags);
    out.push(e as i8 as u8);
    put_varint(&mut out, tz.len() as i64);
    out.extend_from_slice(&tz);
    out.extend_from_slice(&vz);
    out
}

pub fn decimal_decode(b: &[u8], n: usize) -> Option<Vec<(i64, f64)>> {
    let flags = b[0];
    let e = b[1] as i8 as i32;
    let mut p = 2;
    let tl = get_varint(b, &mut p)? as usize;
    let tbuf = zstd::bulk::decompress(&b[p..p + tl], n * 10 + 16).ok()?;
    let vbuf = zstd::bulk::decompress(&b[p + tl..], n * 10 + 16).ok()?;
    let ts = undeltas(&tbuf, n, true)?;
    let vals: Vec<f64> = match flags {
        0 => vbuf.chunks(8).map(|c| f64::from_bits(u64::from_le_bytes(c.try_into().unwrap()))).collect(),
        _ => {
            let ints = undeltas(&vbuf, n, flags == 2)?;
            ints.into_iter()
                .map(|m| if e >= 0 { m as f64 * 10f64.powi(e) } else { m as f64 / 10f64.powi(-e) })
                .collect()
        }
    };
    Some(ts.into_iter().zip(vals).collect())
}

fn raw_columns(s: &[(i64, f64)]) -> Vec<u8> {
    let mut b = Vec::with_capacity(s.len() * 16);
    for x in s {
        b.extend_from_slice(&x.0.to_le_bytes());
    }
    for x in s {
        b.extend_from_slice(&x.1.to_bits().to_le_bytes());
    }
    b
}

pub fn read_series(files: &[String]) -> Result<Vec<(Labels, Vec<(i64, f64)>)>, String> {
    let mut map: HashMap<Labels, Vec<(i64, f64)>> = HashMap::new();
    for f in files {
        let r = BufReader::with_capacity(1 << 20, std::fs::File::open(f).map_err(|e| format!("{f}: {e}"))?);
        for line in r.lines() {
            let line = line.map_err(|e| e.to_string())?;
            if let Some(s) = parse_line(&line)? {
                let t = s.timestamp.ok_or("every sample needs a timestamp")?;
                map.entry(s.labels).or_default().push((t, s.value));
            }
        }
    }
    let mut v: Vec<_> = map.into_iter().collect();
    for (_, s) in v.iter_mut() {
        s.sort_by_key(|x| x.0);
        s.dedup_by_key(|x| x.0);
    }
    v.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(v)
}

fn same(a: &[(i64, f64)], b: &[(i64, f64)]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.0 == y.0 && x.1.to_bits() == y.1.to_bits())
}

pub fn compare(series: &[(Labels, Vec<(i64, f64)>)]) -> Result<serde_json::Value, String> {
    let n: usize = series.iter().map(|s| s.1.len()).sum();
    if n == 0 {
        return Err("no samples".into());
    }
    let per = |bytes: usize| bytes as f64 / n as f64;
    let mut res = serde_json::Map::new();
    res.insert("series".into(), json!(series.len()));
    res.insert("samples".into(), json!(n));
    res.insert("raw".into(), json!({"bytes_per_sample": 16.0}));

    // Gorilla.
    let t0 = Instant::now();
    let enc: Vec<Vec<Vec<u8>>> = series.iter().map(|s| gorilla_encode(&s.1)).collect();
    let enc_s = t0.elapsed().as_secs_f64();
    let t0 = Instant::now();
    let mut ok = true;
    let mut total = 0f64;
    for (s, chunks) in series.iter().zip(&enc) {
        let dec: Vec<_> = chunks.iter().flat_map(|c| ChunkIter::new(c)).collect();
        total += dec.iter().map(|x| x.1).filter(|x| x.is_finite()).sum::<f64>();
        ok &= same(&s.1, &dec);
    }
    let dec_s = t0.elapsed().as_secs_f64();
    if !ok {
        return Err("gorilla round trip failed".into());
    }
    let bytes: usize = enc.iter().flatten().map(|c| c.len()).sum();
    res.insert("gorilla".into(), json!({"bytes_per_sample": per(bytes), "encode_msamples_per_s": n as f64 / enc_s / 1e6,
                                        "decode_msamples_per_s": n as f64 / dec_s / 1e6, "checksum": total}));

    // Decimal + delta + zstd.
    for level in [1, 3] {
        let t0 = Instant::now();
        let enc: Vec<Vec<Vec<u8>>> = series.iter().map(|s| s.1.chunks(VM_BLOCK).map(|c| decimal_encode(c, level)).collect()).collect();
        let enc_s = t0.elapsed().as_secs_f64();
        let t0 = Instant::now();
        let mut ok = true;
        let mut float_blocks = 0;
        let mut blocks = 0;
        for (s, bs) in series.iter().zip(&enc) {
            let mut dec = vec![];
            for (b, c) in bs.iter().zip(s.1.chunks(VM_BLOCK)) {
                dec.extend(decimal_decode(b, c.len()).ok_or("decimal decode failed")?);
                blocks += 1;
                float_blocks += (b[0] == 0) as usize;
            }
            ok &= same(&s.1, &dec);
        }
        let dec_s = t0.elapsed().as_secs_f64();
        if !ok {
            return Err("decimal round trip failed".into());
        }
        let bytes: usize = enc.iter().flatten().map(|c| c.len()).sum();
        res.insert(format!("decimal_zstd{level}"), json!({"bytes_per_sample": per(bytes), "encode_msamples_per_s": n as f64 / enc_s / 1e6,
                                                         "decode_msamples_per_s": n as f64 / dec_s / 1e6, "blocks": blocks, "float_fallback_blocks": float_blocks}));
    }

    // General-purpose compression of raw columns, per 8192-sample block.
    for level in [3, 19] {
        let t0 = Instant::now();
        let bytes: usize = series
            .iter()
            .flat_map(|s| s.1.chunks(VM_BLOCK).map(|c| zstd::bulk::compress(&raw_columns(c), level).unwrap().len()).collect::<Vec<_>>())
            .sum();
        res.insert(format!("zstd{level}_raw"), json!({"bytes_per_sample": per(bytes), "encode_msamples_per_s": n as f64 / t0.elapsed().as_secs_f64() / 1e6}));
    }
    Ok(serde_json::Value::Object(res))
}

pub fn compare_files(files: &[String]) -> Result<serde_json::Value, String> {
    compare(&read_series(files)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_exponents() {
        assert_eq!(decimal_exp(42.0), Some(0));
        assert_eq!(decimal_exp(58.3), Some(-1));
        assert_eq!(decimal_exp(0.125), Some(-3));
        assert_eq!(decimal_exp(f64::NAN), None);
    }

    #[test]
    fn all_codecs_round_trip() {
        let mut rng = fastrand::Rng::with_seed(5);
        let mut series = vec![];
        for k in 0..20 {
            let mut v = 50.0;
            let mut c = 0.0;
            let s: Vec<(i64, f64)> = (0..10_000)
                .map(|i| {
                    v += rng.f64() - 0.5;
                    c += rng.u32(0..100) as f64;
                    let x = match k % 4 {
                        0 => (v * 10.0f64).round() / 10.0, // one decimal, like TSBS
                        1 => c,                           // a counter
                        2 => rng.f64(),                    // full-precision floats
                        _ => if i % 7 == 0 { f64::NAN } else { 1.5 },
                    };
                    (1_600_000_000_000 + i * 10_000 + rng.i64(0..3), x)
                })
                .collect();
            series.push((Labels::new(vec![("__name__".into(), format!("m{k}"))]), s));
        }
        let r = compare(&series).unwrap();
        assert!(r["gorilla"]["bytes_per_sample"].as_f64().unwrap() < 16.0);
        assert!(r["decimal_zstd3"]["float_fallback_blocks"].as_u64().unwrap() >= 5);
    }
}

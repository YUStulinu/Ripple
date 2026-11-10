//! The database: an in-memory head (recent data, protected by the WAL) and immutable on-disk
//! blocks, each covering a fixed time range, merged by compaction into larger ones.
//!
//! Directory layout:
//! ```text
//! data/wal/00000001 ...           write-ahead log segments
//! data/blocks/<id>/meta.json      time range, counts, compaction level, parent blocks
//! data/blocks/<id>/chunks         concatenated Gorilla chunks
//! data/blocks/<id>/index          series labels and, per series, chunk time ranges and offsets
//! ```
//! A block is written to `<id>.tmp`, fsynced and renamed, so a crash leaves either nothing or a
//! complete block. A compacted block lists its parents; if a crash leaves both, the parents are
//! deleted on open. Samples older than the newest block's end are rejected, so after a crash the
//! WAL replay never duplicates what a block already holds.

use crate::bits::{get_str, get_uvarint, get_varint, put_str, put_uvarint, put_varint};
use crate::chunk::{ChunkIter, ChunkWriter, SAMPLES_PER_CHUNK, chunk_len};
use crate::labels::{Labels, ParsedSample, parse_float, parse_line};
use crate::metrics::Histogram;
use crate::wal::{self, Record, Wal};
use regex::Regex;
use serde_json::json;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, RwLock};
use std::time::Instant;

// ---------------------------------------------------------------- matchers

#[derive(Clone, Debug)]
pub enum MatchOp {
    Eq,
    Neq,
    Re(Regex),
    Nre(Regex),
}

#[derive(Clone, Debug)]
pub struct Matcher {
    pub name: String,
    pub value: String,
    pub op: MatchOp,
}

impl Matcher {
    pub fn eq(name: &str, value: &str) -> Matcher {
        Matcher { name: name.into(), value: value.into(), op: MatchOp::Eq }
    }

    pub fn new(name: &str, op: &str, value: &str) -> Result<Matcher, String> {
        let re = || Regex::new(&format!("^(?:{value})$")).map_err(|e| e.to_string());
        let op = match op {
            "=" => MatchOp::Eq,
            "!=" => MatchOp::Neq,
            "=~" => MatchOp::Re(re()?),
            "!~" => MatchOp::Nre(re()?),
            _ => return Err(format!("unknown matcher {op}")),
        };
        Ok(Matcher { name: name.into(), value: value.into(), op })
    }

    /// A missing label counts as the empty string, as in Prometheus.
    pub fn matches(&self, l: &Labels) -> bool {
        let v = l.get(&self.name).unwrap_or("");
        match &self.op {
            MatchOp::Eq => v == self.value,
            MatchOp::Neq => v != self.value,
            MatchOp::Re(r) => r.is_match(v),
            MatchOp::Nre(r) => !r.is_match(v),
        }
    }
}

type Postings = HashMap<String, HashMap<String, Vec<u32>>>;

fn add_postings(p: &mut Postings, l: &Labels, id: u32) {
    for (n, v) in l.0.iter() {
        p.entry(n.clone()).or_default().entry(v.clone()).or_default().push(id);
    }
}

/// Candidate series ids: the intersection of the postings of every non-empty equality matcher.
fn candidates(p: &Postings, n: usize, ms: &[Matcher]) -> Vec<u32> {
    let mut lists: Vec<&Vec<u32>> = vec![];
    for m in ms {
        if matches!(m.op, MatchOp::Eq) && !m.value.is_empty() {
            match p.get(&m.name).and_then(|x| x.get(&m.value)) {
                Some(l) => lists.push(l),
                None => return vec![],
            }
        }
    }
    if lists.is_empty() {
        return (0..n as u32).collect();
    }
    lists.sort_by_key(|l| l.len());
    let mut out = lists[0].clone();
    for l in &lists[1..] {
        let mut j = 0;
        out.retain(|x| {
            while j < l.len() && l[j] < *x {
                j += 1;
            }
            j < l.len() && l[j] == *x
        });
    }
    out
}

// ---------------------------------------------------------------- head

#[derive(Clone)]
pub struct ClosedChunk {
    pub mint: i64,
    pub maxt: i64,
    pub data: Vec<u8>,
}

pub struct MemSeries {
    pub labels: Labels,
    pub chunks: Vec<ClosedChunk>,
    pub head: ChunkWriter,
    pub last_t: i64,
}

impl MemSeries {
    fn new(labels: Labels) -> Self {
        MemSeries { labels, chunks: vec![], head: ChunkWriter::new(), last_t: i64::MIN }
    }

    fn append(&mut self, t: i64, v: f64, block_range: i64) {
        // A chunk never spans a block boundary, so cutting a block only moves whole chunks.
        let crosses = !self.head.is_empty() && t.div_euclid(block_range) != self.head.mint.div_euclid(block_range);
        if self.head.len() >= SAMPLES_PER_CHUNK || crosses {
            let full = std::mem::take(&mut self.head);
            self.chunks.push(ClosedChunk { mint: full.mint, maxt: full.maxt, data: full.bytes() });
        }
        self.head.append(t, v);
        self.last_t = t;
    }

    fn samples_in(&self, mint: i64, maxt: i64, out: &mut Vec<(i64, f64)>) {
        for c in &self.chunks {
            if c.maxt >= mint && c.mint <= maxt {
                out.extend(ChunkIter::new(&c.data).filter(|s| s.0 >= mint && s.0 <= maxt));
            }
        }
        if !self.head.is_empty() && self.head.maxt >= mint && self.head.mint <= maxt {
            out.extend(self.head.iter().filter(|s| s.0 >= mint && s.0 <= maxt));
        }
    }

    fn num_samples(&self) -> usize {
        self.chunks.iter().map(|c| chunk_len(&c.data)).sum::<usize>() + self.head.len()
    }

    fn mint(&self) -> Option<i64> {
        self.chunks.first().map(|c| c.mint).or((!self.head.is_empty()).then_some(self.head.mint))
    }
}

#[derive(Default)]
pub struct Head {
    pub series: Vec<MemSeries>,
    by_labels: HashMap<Labels, u32>,
    /// Exact series text (as received) to series number, for [`Db::append_text`].
    text_cache: HashMap<String, u32>,
    postings: Postings,
    pub num_samples: u64,
    pub maxt: i64,
}

impl Head {
    fn get_or_create(&mut self, l: &Labels) -> (u32, bool) {
        if let Some(&r) = self.by_labels.get(l) {
            return (r, false);
        }
        let r = self.series.len() as u32;
        self.series.push(MemSeries::new(l.clone()));
        self.by_labels.insert(l.clone(), r);
        add_postings(&mut self.postings, l, r);
        (r, true)
    }

    pub fn mint(&self) -> Option<i64> {
        self.series.iter().filter_map(|s| s.mint()).min()
    }
}

// ---------------------------------------------------------------- blocks

#[derive(Clone, Debug)]
pub struct ChunkMeta {
    pub mint: i64,
    pub maxt: i64,
    pub offset: u64,
    pub len: u32,
}

pub struct Block {
    pub id: String,
    pub dir: PathBuf,
    pub mint: i64,
    /// Exclusive end of the block's time range.
    pub end: i64,
    pub level: u32,
    pub num_samples: u64,
    pub sources: Vec<String>,
    pub series: Vec<(Labels, Vec<ChunkMeta>)>,
    postings: Postings,
    chunks: Vec<u8>,
    pub index_bytes: u64,
}

const INDEX_MAGIC: &[u8; 4] = b"RPI1";

fn write_block(blocks_dir: &Path, mint: i64, end: i64, level: u32, sources: &[String], series: &[(Labels, Vec<&[u8]>, Vec<(i64, i64)>)]) -> io::Result<String> {
    let id = format!("{:016x}-{:08x}", mint as u64, fastrand::u32(..));
    let tmp = blocks_dir.join(format!("{id}.tmp"));
    fs::create_dir_all(&tmp)?;
    let mut chunks = Vec::new();
    let mut index = INDEX_MAGIC.to_vec();
    put_uvarint(&mut index, series.len() as u64);
    let mut samples = 0u64;
    let mut nchunks = 0u64;
    for (l, data, ranges) in series {
        put_uvarint(&mut index, l.0.len() as u64);
        for (n, v) in l.0.iter() {
            put_str(&mut index, n);
            put_str(&mut index, v);
        }
        put_uvarint(&mut index, data.len() as u64);
        for (d, &(a, b)) in data.iter().zip(ranges) {
            put_varint(&mut index, a);
            put_uvarint(&mut index, (b - a) as u64);
            put_uvarint(&mut index, chunks.len() as u64);
            put_uvarint(&mut index, d.len() as u64);
            chunks.extend_from_slice(d);
            samples += chunk_len(d) as u64;
            nchunks += 1;
        }
    }
    let crc = crc32fast::hash(&index);
    index.extend_from_slice(&crc.to_le_bytes());
    for (name, bytes) in [("chunks", &chunks), ("index", &index)] {
        let mut f = fs::File::create(tmp.join(name))?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    let meta = json!({"id": id, "mint": mint, "end": end, "level": level, "sources": sources,
                      "series": series.len(), "samples": samples, "chunks": nchunks});
    let mut f = fs::File::create(tmp.join("meta.json"))?;
    f.write_all(serde_json::to_string_pretty(&meta).unwrap().as_bytes())?;
    f.sync_all()?;
    drop(f);
    fs::rename(&tmp, blocks_dir.join(&id))?;
    #[cfg(unix)]
    if let Ok(f) = fs::File::open(blocks_dir) {
        let _ = f.sync_all();
    }
    Ok(id)
}

/// Deletes a block directory so that a crash part-way leaves no half-deleted block: it is first
/// renamed (atomically) to `<id>.deleted`, which `Db::open` removes.
fn delete_block_dir(dir: &Path) -> io::Result<()> {
    let gone = dir.with_extension("deleted");
    fs::rename(dir, &gone)?;
    fs::remove_dir_all(&gone)
}

impl Block {
    pub fn open(dir: &Path) -> io::Result<Block> {
        let bad = |m: &str| io::Error::new(io::ErrorKind::InvalidData, format!("{}: {m}", dir.display()));
        let meta: serde_json::Value = serde_json::from_slice(&fs::read(dir.join("meta.json"))?).map_err(|e| bad(&e.to_string()))?;
        let index = fs::read(dir.join("index"))?;
        if index.len() < 8 || &index[..4] != INDEX_MAGIC {
            return Err(bad("bad index"));
        }
        let (body, crc) = index.split_at(index.len() - 4);
        if crc32fast::hash(body) != u32::from_le_bytes(crc.try_into().unwrap()) {
            return Err(bad("index checksum"));
        }
        let chunks = fs::read(dir.join("chunks"))?;
        let mut i = 4;
        let n = get_uvarint(body, &mut i).ok_or_else(|| bad("index"))?;
        let mut series = Vec::with_capacity(n as usize);
        let mut postings = Postings::new();
        let parse = |i: &mut usize| -> Option<(Labels, Vec<ChunkMeta>)> {
            let nl = get_uvarint(body, i)?;
            let mut l = Vec::with_capacity(nl as usize);
            for _ in 0..nl {
                l.push((get_str(body, i)?, get_str(body, i)?));
            }
            let nc = get_uvarint(body, i)?;
            let mut cs = Vec::with_capacity(nc as usize);
            for _ in 0..nc {
                let mint = get_varint(body, i)?;
                let maxt = mint + get_uvarint(body, i)? as i64;
                cs.push(ChunkMeta { mint, maxt, offset: get_uvarint(body, i)?, len: get_uvarint(body, i)? as u32 });
            }
            Some((Labels(l.into()), cs))
        };
        for k in 0..n {
            let s = parse(&mut i).ok_or_else(|| bad("index entry"))?;
            add_postings(&mut postings, &s.0, k as u32);
            series.push(s);
        }
        Ok(Block {
            id: meta["id"].as_str().unwrap_or_default().to_string(),
            dir: dir.to_path_buf(),
            mint: meta["mint"].as_i64().ok_or_else(|| bad("meta"))?,
            end: meta["end"].as_i64().ok_or_else(|| bad("meta"))?,
            level: meta["level"].as_u64().unwrap_or(1) as u32,
            num_samples: meta["samples"].as_u64().unwrap_or(0),
            sources: meta["sources"].as_array().map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect()).unwrap_or_default(),
            series,
            postings,
            index_bytes: index.len() as u64,
            chunks,
        })
    }

    fn chunk(&self, m: &ChunkMeta) -> &[u8] {
        &self.chunks[m.offset as usize..m.offset as usize + m.len as usize]
    }

    pub fn disk_bytes(&self) -> u64 {
        self.chunks.len() as u64 + self.index_bytes
    }
}

// ---------------------------------------------------------------- database

#[derive(Clone, Debug)]
pub struct Options {
    /// Time range of a level-1 block, ms (Prometheus: 2 h).
    pub block_range: i64,
    /// Fsync the WAL before acknowledging a write.
    pub fsync: bool,
    /// Delete blocks that end before now - retention (0 keeps everything).
    pub retention: i64,
}

impl Default for Options {
    fn default() -> Self {
        Options { block_range: 2 * 3600 * 1000, fsync: true, retention: 0 }
    }
}

pub struct Inner {
    pub head: Head,
    pub blocks: Vec<Block>,
    /// Samples older than this are rejected: they would belong to a block already written.
    pub min_valid: i64,
}

#[derive(Debug, Clone)]
pub struct Counters {
    pub appended: u64,
    pub out_of_order: u64,
    pub too_old: u64,
    pub series_created: u64,
    pub head_cuts: u64,
    pub compactions: u64,
    pub wal_truncated_bytes: u64,
    pub replayed_samples: u64,
    pub fsync_seconds: Histogram,
}

impl Default for Counters {
    fn default() -> Self {
        Counters {
            appended: 0, out_of_order: 0, too_old: 0, series_created: 0, head_cuts: 0, compactions: 0,
            wal_truncated_bytes: 0, replayed_samples: 0, fsync_seconds: Histogram::latency(),
        }
    }
}

pub struct Db {
    pub dir: PathBuf,
    pub opts: Options,
    pub inner: RwLock<Inner>,
    append_lock: Mutex<Wal>,
    /// Serialises head cuts and compactions (they run without blocking appends).
    maint_lock: Mutex<()>,
    pub counters: Mutex<Counters>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct AppendResult {
    pub appended: usize,
    pub out_of_order: usize,
    pub too_old: usize,
}

impl Db {
    pub fn open(dir: &Path, opts: Options) -> io::Result<Db> {
        let bdir = dir.join("blocks");
        fs::create_dir_all(&bdir)?;
        let mut blocks = vec![];
        for e in fs::read_dir(&bdir)? {
            let p = e?.path();
            if p.extension().is_some_and(|x| x == "tmp" || x == "deleted") {
                fs::remove_dir_all(&p)?; // an interrupted block write or deletion
            } else if p.is_dir() {
                blocks.push(Block::open(&p)?);
            }
        }
        // A compaction that crashed before deleting its inputs: the inputs are redundant.
        let parents: std::collections::HashSet<String> = blocks.iter().flat_map(|b| b.sources.clone()).collect();
        for b in blocks.iter().filter(|b| parents.contains(&b.id)) {
            delete_block_dir(&b.dir)?;
        }
        blocks.retain(|b| !parents.contains(&b.id));
        blocks.sort_by_key(|b| b.mint);
        let min_valid = blocks.iter().map(|b| b.end).max().unwrap_or(i64::MIN);
        let (wal, replay) = Wal::open(&dir.join("wal"))?;
        let mut head = Head::default();
        let mut refs: HashMap<u64, u32> = HashMap::new();
        let mut counters = Counters { wal_truncated_bytes: replay.truncated_bytes, ..Default::default() };
        head.maxt = i64::MIN;
        for r in replay.records {
            match r {
                Record::Series(s) => {
                    for (wr, l) in s {
                        refs.insert(wr, head.get_or_create(&l).0);
                    }
                }
                Record::Samples(s) => {
                    for (wr, t, v) in s {
                        let Some(&r) = refs.get(&wr) else { continue };
                        let ms = &mut head.series[r as usize];
                        if t < min_valid || t <= ms.last_t {
                            continue;
                        }
                        ms.append(t, v, opts.block_range);
                        head.num_samples += 1;
                        head.maxt = head.maxt.max(t);
                        counters.replayed_samples += 1;
                    }
                }
            }
        }
        let db = Db {
            dir: dir.to_path_buf(),
            opts,
            inner: RwLock::new(Inner { head, blocks, min_valid }),
            append_lock: Mutex::new(wal),
            maint_lock: Mutex::new(()),
            counters: Mutex::new(counters),
        };
        // Rewrite the log so it refers to the head's own series numbers.
        db.checkpoint()?;
        Ok(db)
    }

    /// Appends a batch. It is durable (WAL fsynced) when this returns `Ok`. Samples older than
    /// the series' newest sample are rejected, as Prometheus does by default.
    pub fn append(&self, batch: &[ParsedSample], default_t: i64) -> io::Result<AppendResult> {
        let mut wal = self.append_lock.lock().unwrap();
        let mut res = AppendResult::default();
        let mut new_series = vec![];
        let mut samples: Vec<(u64, i64, f64)> = Vec::with_capacity(batch.len());
        {
            let mut inner = self.inner.write().unwrap();
            let min_valid = inner.min_valid;
            let mut last: HashMap<u32, i64> = HashMap::new();
            for s in batch {
                let t = s.timestamp.unwrap_or(default_t);
                if t < min_valid {
                    res.too_old += 1;
                    continue;
                }
                let (r, created) = inner.head.get_or_create(&s.labels);
                if created {
                    new_series.push((r as u64, s.labels.clone()));
                }
                let prev = *last.get(&r).unwrap_or(&inner.head.series[r as usize].last_t);
                if t <= prev {
                    res.out_of_order += 1;
                    continue;
                }
                last.insert(r, t);
                samples.push((r as u64, t, s.value));
            }
        }
        self.commit(&mut wal, new_series, samples, res)
    }

    /// Appends text-format lines. A series seen before is recognised by its exact text (the part
    /// before the value), so only new series pay for parsing their labels, the same idea as
    /// Prometheus' scrape cache. A malformed line rejects the whole batch.
    pub fn append_text(&self, text: &str, default_t: i64) -> Result<AppendResult, String> {
        let mut wal = self.append_lock.lock().unwrap();
        let mut res = AppendResult::default();
        let mut new_series = vec![];
        let mut samples: Vec<(u64, i64, f64)> = Vec::with_capacity(text.len() / 64);
        {
            let mut inner = self.inner.write().unwrap();
            let min_valid = inner.min_valid;
            let mut last: HashMap<u32, i64> = HashMap::new();
            for (n, raw) in text.lines().enumerate() {
                let line = raw.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                let key_end = match line.rfind('}') {
                    Some(i) => i + 1,
                    None => line.find(|c: char| c.is_ascii_whitespace()).unwrap_or(line.len()),
                };
                let key = &line[..key_end];
                let (r, value, ts) = if let Some(&r) = inner.head.text_cache.get(key) {
                    let mut it = line[key_end..].split_ascii_whitespace();
                    let value = it.next().and_then(parse_float).ok_or_else(|| format!("line {}: bad value", n + 1))?;
                    let ts = match it.next() {
                        Some(t) => Some(t.parse::<i64>().map_err(|_| format!("line {}: bad timestamp", n + 1))?),
                        None => None,
                    };
                    (r, value, ts)
                } else {
                    let s = parse_line(line).map_err(|e| format!("line {}: {e}", n + 1))?.ok_or("unexpected comment")?;
                    let (r, created) = inner.head.get_or_create(&s.labels);
                    if created {
                        new_series.push((r as u64, s.labels.clone()));
                    }
                    inner.head.text_cache.insert(key.to_string(), r);
                    (r, s.value, s.timestamp)
                };
                let t = ts.unwrap_or(default_t);
                if t < min_valid {
                    res.too_old += 1;
                    continue;
                }
                let prev = *last.get(&r).unwrap_or(&inner.head.series[r as usize].last_t);
                if t <= prev {
                    res.out_of_order += 1;
                    continue;
                }
                last.insert(r, t);
                samples.push((r as u64, t, value));
            }
        }
        self.commit(&mut wal, new_series, samples, res).map_err(|e| e.to_string())
    }

    fn commit(&self, wal: &mut Wal, new_series: Vec<(u64, Labels)>, samples: Vec<(u64, i64, f64)>, mut res: AppendResult) -> io::Result<AppendResult> {
        // The WAL write and fsync happen without the head lock, so queries are not blocked.
        let mut buf = Vec::with_capacity(samples.len() * 12 + 64);
        let n_new = new_series.len();
        if !new_series.is_empty() {
            wal::encode_series(&new_series, &mut buf);
        }
        if !samples.is_empty() {
            wal::encode_samples(&samples, &mut buf);
        }
        if !buf.is_empty() {
            wal.write(&buf)?;
            if self.opts.fsync {
                let t0 = Instant::now();
                wal.sync()?;
                self.counters.lock().unwrap().fsync_seconds.observe(t0.elapsed().as_secs_f64());
            } else {
                wal.write(&[])?;
            }
        }
        let mut inner = self.inner.write().unwrap();
        for &(r, t, v) in &samples {
            inner.head.series[r as usize].append(t, v, self.opts.block_range);
            inner.head.maxt = inner.head.maxt.max(t);
        }
        inner.head.num_samples += samples.len() as u64;
        res.appended = samples.len();
        let mut c = self.counters.lock().unwrap();
        c.appended += res.appended as u64;
        c.out_of_order += res.out_of_order as u64;
        c.too_old += res.too_old as u64;
        c.series_created += n_new as u64;
        Ok(res)
    }

    /// Flushes buffered WAL data (for `fsync: false`).
    pub fn flush(&self) -> io::Result<()> {
        self.append_lock.lock().unwrap().sync()
    }

    /// Every series matching all matchers, with its samples in [mint, maxt], in time order.
    pub fn select(&self, ms: &[Matcher], mint: i64, maxt: i64) -> Vec<(Labels, Vec<(i64, f64)>)> {
        let inner = self.inner.read().unwrap();
        let mut out: BTreeMap<Labels, Vec<(i64, f64)>> = BTreeMap::new();
        for b in inner.blocks.iter().filter(|b| b.end > mint && b.mint <= maxt) {
            for id in candidates(&b.postings, b.series.len(), ms) {
                let (l, cs) = &b.series[id as usize];
                if !ms.iter().all(|m| m.matches(l)) {
                    continue;
                }
                let v = out.entry(l.clone()).or_default();
                for c in cs.iter().filter(|c| c.maxt >= mint && c.mint <= maxt) {
                    v.extend(ChunkIter::new(b.chunk(c)).filter(|s| s.0 >= mint && s.0 <= maxt));
                }
            }
        }
        let h = &inner.head;
        for id in candidates(&h.postings, h.series.len(), ms) {
            let s = &h.series[id as usize];
            if !ms.iter().all(|m| m.matches(&s.labels)) {
                continue;
            }
            let v = out.entry(s.labels.clone()).or_default();
            s.samples_in(mint, maxt, v);
        }
        out.into_iter().filter(|(_, v)| !v.is_empty()).collect()
    }

    /// Label sets of matching series (no samples decoded).
    pub fn series(&self, ms: &[Matcher]) -> Vec<Labels> {
        let inner = self.inner.read().unwrap();
        let mut out = std::collections::BTreeSet::new();
        for b in &inner.blocks {
            for id in candidates(&b.postings, b.series.len(), ms) {
                let l = &b.series[id as usize].0;
                if ms.iter().all(|m| m.matches(l)) {
                    out.insert(l.clone());
                }
            }
        }
        for id in candidates(&inner.head.postings, inner.head.series.len(), ms) {
            let l = &inner.head.series[id as usize].labels;
            if ms.iter().all(|m| m.matches(l)) {
                out.insert(l.clone());
            }
        }
        out.into_iter().collect()
    }

    pub fn label_names(&self) -> Vec<String> {
        let inner = self.inner.read().unwrap();
        let mut s: std::collections::BTreeSet<String> = inner.head.postings.keys().cloned().collect();
        for b in &inner.blocks {
            s.extend(b.postings.keys().cloned());
        }
        s.into_iter().collect()
    }

    pub fn label_values(&self, name: &str) -> Vec<String> {
        let inner = self.inner.read().unwrap();
        let mut s = std::collections::BTreeSet::new();
        for p in std::iter::once(&inner.head.postings).chain(inner.blocks.iter().map(|b| &b.postings)) {
            if let Some(m) = p.get(name) {
                s.extend(m.keys().cloned());
            }
        }
        s.into_iter().collect()
    }

    /// Rewrites the WAL from the head's current contents.
    fn checkpoint_with(&self, wal: &mut Wal) -> io::Result<()> {
        let inner = self.inner.read().unwrap();
        let mut recs = vec![];
        let series: Vec<(u64, Labels)> = inner.head.series.iter().enumerate().map(|(i, s)| (i as u64, s.labels.clone())).collect();
        for c in series.chunks(10_000) {
            recs.push(Record::Series(c.to_vec()));
        }
        let mut batch = vec![];
        for (i, s) in inner.head.series.iter().enumerate() {
            let mut v = vec![];
            s.samples_in(i64::MIN, i64::MAX, &mut v);
            batch.extend(v.into_iter().map(|(t, x)| (i as u64, t, x)));
            if batch.len() >= 50_000 {
                recs.push(Record::Samples(std::mem::take(&mut batch)));
            }
        }
        if !batch.is_empty() {
            recs.push(Record::Samples(batch));
        }
        wal.checkpoint(&recs)
    }

    pub fn checkpoint(&self) -> io::Result<()> {
        let mut wal = self.append_lock.lock().unwrap();
        self.checkpoint_with(&mut wal)
    }

    /// Moves head data into blocks, one per block range, keeping the newest half range or more in
    /// memory: a cut happens once the head spans 1.5 block ranges, as in Prometheus. All complete
    /// ranges are cut in one pass with a single WAL checkpoint, so a head that fell far behind
    /// (a backfill, or a server that was down) catches up in linear time. `force` cuts everything.
    /// Returns whether any block was written.
    pub fn cut_head(&self, force: bool) -> io::Result<bool> {
        let _maint = self.maint_lock.lock().unwrap();
        let r = self.opts.block_range;
        let (start, boundary) = {
            // Freeze everything below the boundary: from here on, appends older than it are
            // rejected, so the chunks copied below cannot change and writes are not blocked
            // while the blocks are written.
            let _wal = self.append_lock.lock().unwrap();
            let mut inner = self.inner.write().unwrap();
            let Some(mint) = inner.head.mint() else { return Ok(false) };
            let maxt = inner.head.maxt;
            let start = mint.div_euclid(r) * r;
            let boundary = if force { maxt.div_euclid(r) * r + r } else { (maxt - r / 2).div_euclid(r) * r };
            if boundary <= start || (!force && maxt - mint < r * 3 / 2) {
                return Ok(false);
            }
            inner.min_valid = inner.min_valid.max(boundary);
            (start, boundary)
        };
        // Whole chunks go to the block of their range (chunks never straddle a boundary).
        let mut new_blocks = vec![];
        {
            let inner = self.inner.read().unwrap();
            let mut w = start;
            while w < boundary {
                let we = w + r;
                let mut series: Vec<(Labels, Vec<Vec<u8>>, Vec<(i64, i64)>)> = vec![];
                for s in &inner.head.series {
                    let mut data = vec![];
                    let mut ranges = vec![];
                    for c in s.chunks.iter().filter(|c| c.mint >= w && c.mint < we) {
                        data.push(c.data.clone());
                        ranges.push((c.mint, c.maxt));
                    }
                    if !s.head.is_empty() && s.head.mint >= w && s.head.mint < we && s.head.maxt < boundary {
                        data.push(s.head.bytes());
                        ranges.push((s.head.mint, s.head.maxt));
                    }
                    if !data.is_empty() {
                        series.push((s.labels.clone(), data, ranges));
                    }
                }
                if !series.is_empty() {
                    series.sort_by(|a, b| a.0.cmp(&b.0));
                    let refs: Vec<(Labels, Vec<&[u8]>, Vec<(i64, i64)>)> =
                        series.iter().map(|(l, d, r)| (l.clone(), d.iter().map(|x| x.as_slice()).collect(), r.clone())).collect();
                    let id = write_block(&self.dir.join("blocks"), w, we, 1, &[], &refs)?;
                    new_blocks.push(Block::open(&self.dir.join("blocks").join(&id))?);
                }
                w = we;
            }
        }
        if new_blocks.is_empty() {
            return Ok(false);
        }
        let n_blocks = new_blocks.len() as u64;
        {
            let mut inner = self.inner.write().unwrap();
            let mut new_head = Head { maxt: inner.head.maxt, ..Default::default() };
            for mut s in std::mem::take(&mut inner.head.series) {
                s.chunks.retain(|c| c.maxt >= boundary);
                if !s.head.is_empty() && s.head.maxt < boundary {
                    s.head = ChunkWriter::new();
                }
                if s.chunks.is_empty() && s.head.is_empty() {
                    continue; // the series has no data left in memory
                }
                let n = s.num_samples() as u64;
                let (r, _) = new_head.get_or_create(&s.labels);
                new_head.series[r as usize] = s;
                new_head.num_samples += n;
            }
            inner.head = new_head;
            inner.blocks.extend(new_blocks);
            inner.blocks.sort_by_key(|b| b.mint);
        }
        let mut wal = self.append_lock.lock().unwrap();
        self.checkpoint_with(&mut wal)?;
        self.counters.lock().unwrap().head_cuts += n_blocks;
        Ok(true)
    }

    /// Merges level-k blocks that fill a window of 3^k block ranges, oldest first. Returns the
    /// number of merges done.
    pub fn compact(&self) -> io::Result<usize> {
        let _maint = self.maint_lock.lock().unwrap();
        let mut done = 0;
        for level in 1..=3u32 {
            loop {
                let window = self.opts.block_range * 3i64.pow(level);
                let plan: Option<Vec<String>> = {
                    let inner = self.inner.read().unwrap();
                    let newest_end = inner.blocks.iter().map(|b| b.end).max().unwrap_or(i64::MIN);
                    let mut groups: BTreeMap<i64, Vec<&Block>> = BTreeMap::new();
                    for b in inner.blocks.iter().filter(|b| b.end - b.mint < window) {
                        groups.entry(b.mint.div_euclid(window)).or_default().push(b);
                    }
                    groups
                        .into_iter()
                        .find(|(w, bs)| {
                            let wend = (w + 1) * window;
                            bs.len() >= 2 && bs.iter().all(|b| b.end <= wend) && newest_end >= wend
                        })
                        .map(|(_, bs)| bs.iter().map(|b| b.id.clone()).collect())
                };
                let Some(ids) = plan else { break };
                self.merge(&ids, level + 1)?;
                done += 1;
            }
        }
        self.counters.lock().unwrap().compactions += done as u64;
        Ok(done)
    }

    fn merge(&self, ids: &[String], level: u32) -> io::Result<()> {
        let id = {
            let inner = self.inner.read().unwrap();
            let mut parts: Vec<&Block> = inner.blocks.iter().filter(|b| ids.contains(&b.id)).collect();
            parts.sort_by_key(|b| b.mint);
            let mut merged: BTreeMap<&Labels, (Vec<&[u8]>, Vec<(i64, i64)>)> = BTreeMap::new();
            for b in &parts {
                for (l, cs) in &b.series {
                    let e = merged.entry(l).or_default();
                    for c in cs {
                        e.0.push(b.chunk(c));
                        e.1.push((c.mint, c.maxt));
                    }
                }
            }
            let series: Vec<_> = merged.into_iter().map(|(l, (d, r))| (l.clone(), d, r)).collect();
            let mint = parts.iter().map(|b| b.mint).min().unwrap();
            let end = parts.iter().map(|b| b.end).max().unwrap();
            write_block(&self.dir.join("blocks"), mint, end, level, ids, &series)?
        };
        let block = Block::open(&self.dir.join("blocks").join(&id))?;
        let old: Vec<PathBuf> = {
            let mut inner = self.inner.write().unwrap();
            let old = inner.blocks.iter().filter(|b| ids.contains(&b.id)).map(|b| b.dir.clone()).collect();
            inner.blocks.retain(|b| !ids.contains(&b.id));
            inner.blocks.push(block);
            inner.blocks.sort_by_key(|b| b.mint);
            old
        };
        for d in old {
            delete_block_dir(&d)?;
        }
        Ok(())
    }

    /// Deletes blocks that end before `now - retention`.
    pub fn apply_retention(&self, now: i64) -> io::Result<usize> {
        if self.opts.retention <= 0 {
            return Ok(0);
        }
        let old: Vec<PathBuf> = {
            let mut inner = self.inner.write().unwrap();
            let cut = now - self.opts.retention;
            let old = inner.blocks.iter().filter(|b| b.end <= cut).map(|b| b.dir.clone()).collect();
            inner.blocks.retain(|b| b.end > cut);
            old
        };
        let n = old.len();
        for d in old {
            delete_block_dir(&d)?;
        }
        Ok(n)
    }

    /// One round of background maintenance.
    pub fn maintain(&self, now: i64) -> io::Result<()> {
        while self.cut_head(false)? {}
        self.compact()?;
        self.apply_retention(now)?;
        Ok(())
    }

    pub fn stats(&self) -> serde_json::Value {
        let inner = self.inner.read().unwrap();
        let c = self.counters.lock().unwrap().clone();
        let wal_bytes = self.append_lock.lock().unwrap().disk_bytes();
        let head_chunk_bytes: usize = inner.head.series.iter().map(|s| s.chunks.iter().map(|c| c.data.len()).sum::<usize>() + s.head.encoded_len()).sum();
        let blocks: Vec<_> = inner
            .blocks
            .iter()
            .map(|b| json!({"id": b.id, "mint": b.mint, "end": b.end, "level": b.level, "series": b.series.len(), "samples": b.num_samples,
                            "chunk_bytes": b.chunks.len(), "index_bytes": b.index_bytes}))
            .collect();
        let block_samples: u64 = inner.blocks.iter().map(|b| b.num_samples).sum();
        let block_bytes: u64 = inner.blocks.iter().map(|b| b.disk_bytes()).sum();
        let block_chunk_bytes: u64 = inner.blocks.iter().map(|b| b.chunks.len() as u64).sum();
        json!({
            "head": {"series": inner.head.series.len(), "samples": inner.head.num_samples, "chunk_bytes": head_chunk_bytes,
                     "mint": inner.head.mint(), "maxt": inner.head.maxt},
            "blocks": blocks,
            "block_samples": block_samples, "block_bytes": block_bytes, "block_chunk_bytes": block_chunk_bytes,
            "wal_bytes": wal_bytes,
            "min_valid": inner.min_valid,
            "appended": c.appended, "out_of_order": c.out_of_order, "too_old": c.too_old, "series_created": c.series_created,
            "head_cuts": c.head_cuts, "compactions": c.compactions, "wal_truncated_bytes": c.wal_truncated_bytes,
            "replayed_samples": c.replayed_samples,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(name: &str, host: &str, t: i64, v: f64) -> ParsedSample {
        ParsedSample { labels: Labels::new(vec![("__name__".into(), name.into()), ("host".into(), host.into())]), value: v, timestamp: Some(t) }
    }

    fn opts() -> Options {
        Options { block_range: 3_600_000, fsync: false, retention: 0 }
    }

    fn all(db: &Db, name: &str) -> Vec<(Labels, Vec<(i64, f64)>)> {
        db.select(&[Matcher::eq("__name__", name)], i64::MIN, i64::MAX)
    }

    #[test]
    fn append_select_rejects_out_of_order() {
        let d = tempfile::tempdir().unwrap();
        let db = Db::open(d.path(), opts()).unwrap();
        let r = db.append(&[sample("cpu", "a", 1000, 1.0), sample("cpu", "a", 2000, 2.0), sample("cpu", "a", 1500, 9.0), sample("cpu", "b", 1000, 3.0)], 0).unwrap();
        assert_eq!(r, AppendResult { appended: 3, out_of_order: 1, too_old: 0 });
        let got = all(&db, "cpu");
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].1, vec![(1000, 1.0), (2000, 2.0)]);
        let m = [Matcher::eq("__name__", "cpu"), Matcher::new("host", "=~", "b|c").unwrap()];
        assert_eq!(db.select(&m, 0, 5000).len(), 1);
        assert_eq!(db.label_values("host"), vec!["a", "b"]);
        assert_eq!(db.select(&[Matcher::eq("__name__", "cpu"), Matcher::eq("zone", "")], 0, 5000).len(), 2);
    }

    /// Writes 10 hours of 4 series every 10 s, cutting and compacting along the way, and checks
    /// that everything reads back, also after reopening.
    #[test]
    fn blocks_compaction_and_reopen() {
        let d = tempfile::tempdir().unwrap();
        let expect: Vec<(i64, f64)> = (0..3600).map(|i| (i * 10_000, (i as f64).sin())).collect();
        {
            let db = Db::open(d.path(), opts()).unwrap();
            for chunk in expect.chunks(100) {
                let batch: Vec<_> = chunk.iter().flat_map(|&(t, v)| (0..4).map(move |h| sample("m", &h.to_string(), t, v + h as f64))).collect();
                db.append(&batch, 0).unwrap();
                db.maintain(0).unwrap();
            }
            db.flush().unwrap();
            let st = db.stats();
            assert!(st["head_cuts"].as_u64().unwrap() >= 8, "{st}");
            assert!(st["compactions"].as_u64().unwrap() >= 2, "{st}");
            let got = all(&db, "m");
            assert_eq!(got.len(), 4);
            assert_eq!(got[0].1, expect);
            // Too old for the head once blocks cover it.
            assert_eq!(db.append(&[sample("m", "0", 5, 1.0)], 0).unwrap().too_old, 1);
        }
        let db = Db::open(d.path(), opts()).unwrap();
        let got = all(&db, "m");
        assert_eq!(got[3].1.len(), 3600);
        assert_eq!(got[3].1[1234], (12_340_000, (1234f64).sin() + 3.0));
        let partial = db.select(&[Matcher::eq("host", "2")], 3_000_000, 3_100_000);
        assert_eq!(partial[0].1.len(), 11);
    }

    /// Head cuts and compactions run while another thread writes: nothing is lost or doubled.
    #[test]
    fn maintenance_concurrent_with_appends() {
        let d = tempfile::tempdir().unwrap();
        let db = std::sync::Arc::new(Db::open(d.path(), Options { block_range: 600_000, ..opts() }).unwrap());
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let m = {
            let (db, stop) = (db.clone(), stop.clone());
            std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    db.maintain(0).unwrap();
                }
            })
        };
        let mut rejected = 0;
        for k in 0..2000i64 {
            let batch: Vec<_> = (0..5).map(|h| sample("w", &h.to_string(), k * 10_000, k as f64)).collect();
            let r = db.append(&batch, 0).unwrap();
            rejected += r.out_of_order + r.too_old;
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        m.join().unwrap();
        assert_eq!(rejected, 0);
        let got = all(&db, "w");
        assert_eq!(got.len(), 5);
        for (_, s) in &got {
            assert_eq!(s.len(), 2000);
            assert!(s.iter().enumerate().all(|(k, &(t, v))| t == k as i64 * 10_000 && v == k as f64));
        }
        assert!(db.stats()["head_cuts"].as_u64().unwrap() > 0, "no block was cut while writing");
    }

    #[test]
    fn text_append_matches_parsed_append() {
        let d = tempfile::tempdir().unwrap();
        let db = Db::open(d.path(), opts()).unwrap();
        let text = "# HELP m x
m{b=\"2\",a=\"1\"} 1 1000
m{a=\"1\",b=\"2\"} 2 2000
m{a=\"1\", b=\"2\"} 3 1500
up 1

";
        let r = db.append_text(text, 5000).unwrap();
        assert_eq!(r, AppendResult { appended: 3, out_of_order: 1, too_old: 0 });
        let got = all(&db, "m");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].1, vec![(1000, 1.0), (2000, 2.0)]);
        assert_eq!(all(&db, "up")[0].1, vec![(5000, 1.0)]);
        let again = db.append_text("m{b=\"2\",a=\"1\"} 4 3000
", 0).unwrap();
        assert_eq!(again.appended, 1);
        assert!(db.append_text("m{a=\"1\"} x 1
", 0).unwrap_err().starts_with("line 1"));
        assert!(db.append_text("m{b=\"2\",a=\"1\"} 5 abc
", 0).is_err());
    }

    #[test]
    fn recovers_after_crash_without_losing_acknowledged_data() {
        let d = tempfile::tempdir().unwrap();
        {
            let db = Db::open(d.path(), Options { fsync: true, ..opts() }).unwrap();
            for i in 0..50 {
                db.append(&[sample("x", "a", i * 1000, i as f64)], 0).unwrap();
            }
            std::mem::forget(db); // no clean shutdown
        }
        let db = Db::open(d.path(), opts()).unwrap();
        assert_eq!(all(&db, "x")[0].1.len(), 50);
        assert_eq!(db.stats()["replayed_samples"], 50);
    }

    #[test]
    fn crash_leftovers_are_cleaned_up() {
        let d = tempfile::tempdir().unwrap();
        {
            let db = Db::open(d.path(), opts()).unwrap();
            for i in 0..2000 {
                db.append(&[sample("x", "a", i * 10_000, 1.0)], 0).unwrap();
            }
            db.maintain(0).unwrap();
            db.flush().unwrap();
        }
        // An interrupted block write and a copy of a compacted block's parent.
        fs::create_dir_all(d.path().join("blocks/abc.tmp")).unwrap();
        // A deletion interrupted half-way: only the renamed directory, with some files left.
        fs::create_dir_all(d.path().join("blocks/def.deleted")).unwrap();
        fs::write(d.path().join("blocks/def.deleted/chunks"), b"x").unwrap();
        let before = all(&Db::open(d.path(), opts()).unwrap(), "x")[0].1.len();
        assert!(!d.path().join("blocks/abc.tmp").exists());
        assert!(!d.path().join("blocks/def.deleted").exists());
        assert_eq!(before, 2000);
    }
}

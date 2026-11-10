//! Write-ahead log: every accepted batch is written here and fsynced before it is acknowledged.
//!
//! The log is a directory of numbered segment files. Each record is
//! `[type u8][payload length u32 LE][CRC-32 of the payload u32 LE][payload]`.
//! Replay stops at the first record that is short or fails its checksum (a write torn by a crash),
//! truncates the segment there and deletes any later segments.
//!
//! A checkpoint rewrites what the head still holds into a new segment that starts with a
//! `CHECKPOINT` record; on open, segments before the newest checkpoint are ignored and deleted, so
//! a crash between writing the checkpoint and deleting the old segments is harmless.

use crate::bits::{get_str, get_uvarint, get_varint, put_str, put_uvarint, put_varint};
use crate::labels::Labels;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

const SERIES: u8 = 1;
const SAMPLES: u8 = 2;
const CHECKPOINT: u8 = 3;
const HEADER: usize = 9;
pub const SEGMENT_SIZE: u64 = 64 << 20;

#[derive(Debug, Clone, PartialEq)]
pub enum Record {
    Series(Vec<(u64, Labels)>),
    Samples(Vec<(u64, i64, f64)>),
}

pub fn encode(rec: &Record, out: &mut Vec<u8>) {
    match rec {
        Record::Series(s) => encode_series(s, out),
        Record::Samples(s) => encode_samples(s, out),
    }
}

pub fn encode_series(s: &[(u64, Labels)], out: &mut Vec<u8>) {
    let start = out.len();
    out.extend_from_slice(&[0; HEADER]);
    put_uvarint(out, s.len() as u64);
    for (r, l) in s {
        put_uvarint(out, *r);
        put_uvarint(out, l.0.len() as u64);
        for (n, v) in l.0.iter() {
            put_str(out, n);
            put_str(out, v);
        }
    }
    finish(out, start, SERIES);
}

pub fn encode_samples(s: &[(u64, i64, f64)], out: &mut Vec<u8>) {
    let start = out.len();
    out.extend_from_slice(&[0; HEADER]);
    put_uvarint(out, s.len() as u64);
    let base = s.first().map(|x| x.1).unwrap_or(0);
    put_varint(out, base);
    for (r, t, v) in s {
        put_uvarint(out, *r);
        put_varint(out, t - base);
        out.extend_from_slice(&v.to_le_bytes());
    }
    finish(out, start, SAMPLES);
}

fn finish(out: &mut [u8], start: usize, ty: u8) {
    let len = (out.len() - start - HEADER) as u32;
    let crc = crc32fast::hash(&out[start + HEADER..]);
    out[start] = ty;
    out[start + 1..start + 5].copy_from_slice(&len.to_le_bytes());
    out[start + 5..start + 9].copy_from_slice(&crc.to_le_bytes());
}

fn decode(ty: u8, p: &[u8]) -> Option<Option<Record>> {
    let mut i = 0;
    match ty {
        SERIES => {
            let n = get_uvarint(p, &mut i)?;
            let mut s = Vec::with_capacity(n as usize);
            for _ in 0..n {
                let r = get_uvarint(p, &mut i)?;
                let nl = get_uvarint(p, &mut i)?;
                let mut l = Vec::with_capacity(nl as usize);
                for _ in 0..nl {
                    l.push((get_str(p, &mut i)?, get_str(p, &mut i)?));
                }
                s.push((r, Labels(l.into())));
            }
            Some(Some(Record::Series(s)))
        }
        SAMPLES => {
            let n = get_uvarint(p, &mut i)?;
            let base = get_varint(p, &mut i)?;
            let mut s = Vec::with_capacity(n as usize);
            for _ in 0..n {
                let r = get_uvarint(p, &mut i)?;
                let t = base + get_varint(p, &mut i)?;
                let v = f64::from_le_bytes(p.get(i..i + 8)?.try_into().ok()?);
                i += 8;
                s.push((r, t, v));
            }
            Some(Some(Record::Samples(s)))
        }
        CHECKPOINT => Some(None),
        _ => None,
    }
}

pub struct Wal {
    dir: PathBuf,
    seg: u64,
    size: u64,
    out: BufWriter<File>,
    pub bytes_written: u64,
    pub fsyncs: u64,
}

pub struct Replay {
    pub records: Vec<Record>,
    /// Bytes cut off a torn or corrupt tail (0 after a clean shutdown).
    pub truncated_bytes: u64,
    pub segments: usize,
}

fn segments(dir: &Path) -> io::Result<Vec<u64>> {
    let mut s: Vec<u64> = fs::read_dir(dir)?
        .filter_map(|e| e.ok()?.file_name().to_str()?.parse().ok())
        .collect();
    s.sort();
    Ok(s)
}

fn seg_path(dir: &Path, n: u64) -> PathBuf {
    dir.join(format!("{n:08}"))
}

fn sync_dir(dir: &Path) {
    // Directory fsync makes renames and new files durable on Linux; Windows has no equivalent.
    #[cfg(unix)]
    if let Ok(f) = File::open(dir) {
        let _ = f.sync_all();
    }
    #[cfg(not(unix))]
    let _ = dir;
}

impl Wal {
    /// Opens the log, returning the records to replay.
    pub fn open(dir: &Path) -> io::Result<(Wal, Replay)> {
        fs::create_dir_all(dir)?;
        for e in fs::read_dir(dir)? {
            let p = e?.path();
            if p.extension().is_some_and(|x| x == "tmp") {
                fs::remove_file(p)?;
            }
        }
        let mut segs = segments(dir)?;
        // Start from the newest segment that begins with a checkpoint.
        let mut first = 0;
        for (k, &s) in segs.iter().enumerate() {
            let mut b = [0u8; 1];
            if File::open(seg_path(dir, s))?.read(&mut b)? == 1 && b[0] == CHECKPOINT {
                first = k;
            }
        }
        for &s in &segs[..first] {
            fs::remove_file(seg_path(dir, s))?;
        }
        segs.drain(..first);
        let mut records = vec![];
        let mut truncated = 0u64;
        let mut stop_at: Option<usize> = None;
        for (k, &s) in segs.iter().enumerate() {
            let data = fs::read(seg_path(dir, s))?;
            let mut i = 0;
            let mut good = true;
            while i < data.len() {
                if i + HEADER > data.len() {
                    good = false;
                    break;
                }
                let ty = data[i];
                let len = u32::from_le_bytes(data[i + 1..i + 5].try_into().unwrap()) as usize;
                let crc = u32::from_le_bytes(data[i + 5..i + 9].try_into().unwrap());
                let Some(p) = data.get(i + HEADER..i + HEADER + len) else {
                    good = false;
                    break;
                };
                if crc32fast::hash(p) != crc {
                    good = false;
                    break;
                }
                match decode(ty, p) {
                    Some(Some(r)) => records.push(r),
                    Some(None) => {}
                    None => {
                        good = false;
                        break;
                    }
                }
                i += HEADER + len;
            }
            if !good {
                truncated += (data.len() - i) as u64;
                let f = OpenOptions::new().write(true).open(seg_path(dir, s))?;
                f.set_len(i as u64)?;
                f.sync_all()?;
                stop_at = Some(k);
                break;
            }
        }
        if let Some(k) = stop_at {
            for &s in &segs[k + 1..] {
                truncated += fs::metadata(seg_path(dir, s))?.len();
                fs::remove_file(seg_path(dir, s))?;
            }
            segs.truncate(k + 1);
        }
        let n = segs.len();
        let seg = segs.last().copied().unwrap_or(0) + 1;
        let out = BufWriter::with_capacity(1 << 20, File::create(seg_path(dir, seg))?);
        sync_dir(dir);
        Ok((Wal { dir: dir.to_path_buf(), seg, size: 0, out, bytes_written: 0, fsyncs: 0 }, Replay { records, truncated_bytes: truncated, segments: n }))
    }

    /// Appends encoded records (see [`encode`]); call [`Wal::sync`] before acknowledging.
    pub fn write(&mut self, bytes: &[u8]) -> io::Result<()> {
        if self.size > 0 && self.size + bytes.len() as u64 > SEGMENT_SIZE {
            self.sync()?;
            self.seg += 1;
            self.out = BufWriter::with_capacity(1 << 20, File::create(seg_path(&self.dir, self.seg))?);
            self.size = 0;
            sync_dir(&self.dir);
        }
        self.out.write_all(bytes)?;
        self.size += bytes.len() as u64;
        self.bytes_written += bytes.len() as u64;
        Ok(())
    }

    pub fn sync(&mut self) -> io::Result<()> {
        self.out.flush()?;
        self.out.get_ref().sync_data()?;
        self.fsyncs += 1;
        Ok(())
    }

    /// Replaces the whole log with `records` (what the head still holds).
    pub fn checkpoint(&mut self, records: &[Record]) -> io::Result<()> {
        self.sync()?;
        let n = self.seg + 1;
        let tmp = self.dir.join(format!("{n:08}.tmp"));
        let mut buf = vec![];
        let start = buf.len();
        buf.extend_from_slice(&[0; HEADER]);
        finish(&mut buf, start, CHECKPOINT);
        for r in records {
            encode(r, &mut buf);
        }
        {
            let mut f = File::create(&tmp)?;
            f.write_all(&buf)?;
            f.sync_all()?;
        }
        fs::rename(&tmp, seg_path(&self.dir, n))?;
        sync_dir(&self.dir);
        // The new segment is durable; older ones are now redundant.
        self.seg = n + 1;
        self.out = BufWriter::with_capacity(1 << 20, File::create(seg_path(&self.dir, self.seg))?);
        self.size = 0;
        for s in segments(&self.dir)? {
            if s < n {
                fs::remove_file(seg_path(&self.dir, s))?;
            }
        }
        sync_dir(&self.dir);
        Ok(())
    }

    pub fn disk_bytes(&self) -> u64 {
        segments(&self.dir).map(|s| s.iter().filter_map(|&n| fs::metadata(seg_path(&self.dir, n)).ok()).map(|m| m.len()).sum()).unwrap_or(0)
            + self.out.buffer().len() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lab(n: &str) -> Labels {
        Labels::new(vec![("__name__".into(), n.into())])
    }

    #[test]
    fn replays_and_survives_a_torn_tail() {
        let d = tempfile::tempdir().unwrap();
        {
            let (mut w, r) = Wal::open(d.path()).unwrap();
            assert!(r.records.is_empty());
            let mut b = vec![];
            encode(&Record::Series(vec![(1, lab("a")), (2, lab("b"))]), &mut b);
            encode(&Record::Samples(vec![(1, 1000, 1.5), (2, 990, -2.0)]), &mut b);
            w.write(&b).unwrap();
            w.sync().unwrap();
            let mut b = vec![];
            encode(&Record::Samples(vec![(1, 2000, 3.0)]), &mut b);
            w.write(&b[..b.len() - 3]).unwrap(); // torn
            w.sync().unwrap();
        }
        let (mut w, r) = Wal::open(d.path()).unwrap();
        assert_eq!(r.records.len(), 2);
        assert!(r.truncated_bytes > 0);
        assert_eq!(r.records[1], Record::Samples(vec![(1, 1000, 1.5), (2, 990, -2.0)]));
        // Appending after the repair works and a checkpoint replaces everything.
        let mut b = vec![];
        encode(&Record::Samples(vec![(2, 3000, 4.0)]), &mut b);
        w.write(&b).unwrap();
        w.sync().unwrap();
        w.checkpoint(&[Record::Series(vec![(7, lab("c"))]), Record::Samples(vec![(7, 5, 6.0)])]).unwrap();
        drop(w);
        let (_, r) = Wal::open(d.path()).unwrap();
        assert_eq!(r.records, vec![Record::Series(vec![(7, lab("c"))]), Record::Samples(vec![(7, 5, 6.0)])]);
        assert_eq!(r.truncated_bytes, 0);
    }

    #[test]
    fn corrupt_byte_stops_replay() {
        let d = tempfile::tempdir().unwrap();
        {
            let (mut w, _) = Wal::open(d.path()).unwrap();
            for i in 0..10 {
                let mut b = vec![];
                encode(&Record::Samples(vec![(1, i, i as f64)]), &mut b);
                w.write(&b).unwrap();
            }
            w.sync().unwrap();
        }
        let seg = seg_path(d.path(), 1);
        let mut data = fs::read(&seg).unwrap();
        let rec = data.len() / 10;
        data[rec * 6 + HEADER + 1] ^= 0x40;
        fs::write(&seg, &data).unwrap();
        let (_, r) = Wal::open(d.path()).unwrap();
        assert_eq!(r.records.len(), 6);
    }
}

//! Gorilla-style chunks (Pelkonen et al., VLDB 2015), with the timestamp buckets Prometheus uses
//! for millisecond timestamps.
//!
//! Layout: a 2-byte big-endian sample count, then a bit stream:
//! - sample 0: timestamp as a zigzag varint, value as 64 raw bits;
//! - sample 1: timestamp delta as a zigzag varint, value XOR-encoded;
//! - then delta-of-delta timestamps: `0` (same interval), `10`+14 bits, `110`+17 bits,
//!   `1110`+20 bits, `1111`+64 bits;
//! - values: `0` if equal to the previous one, else `1` then either `0` + the meaningful bits
//!   inside the previous leading/trailing-zero window, or `1` + 5 bits of leading zeros + 6 bits
//!   of length + the meaningful bits.

use crate::bits::{BitReader, BitWriter};

/// Samples per chunk before a new one is started (Prometheus uses the same number).
pub const SAMPLES_PER_CHUNK: usize = 120;

#[derive(Clone, Debug)]
pub struct ChunkWriter {
    bw: BitWriter,
    count: u16,
    t: i64,
    t_delta: i64,
    v: u64,
    leading: u8,
    trailing: u8,
    pub mint: i64,
    pub maxt: i64,
}

impl Default for ChunkWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkWriter {
    pub fn new() -> Self {
        let mut bw = BitWriter::with_capacity(160);
        bw.write_bits(0, 16); // sample count, patched in bytes()
        ChunkWriter { bw, count: 0, t: 0, t_delta: 0, v: 0, leading: 0xff, trailing: 0, mint: i64::MAX, maxt: i64::MIN }
    }

    pub fn len(&self) -> usize {
        self.count as usize
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn append(&mut self, t: i64, v: f64) {
        let vb = v.to_bits();
        match self.count {
            0 => {
                self.bw.write_varint(t);
                self.bw.write_bits(vb, 64);
                self.mint = t;
            }
            1 => {
                let d = t - self.t;
                self.bw.write_varint(d);
                self.t_delta = d;
                self.write_value(vb);
            }
            _ => {
                let d = t - self.t;
                let dod = d - self.t_delta;
                if dod == 0 {
                    self.bw.write_bit(false);
                } else if (-(1 << 13) + 1..=(1 << 13)).contains(&dod) {
                    self.bw.write_bits(0b10, 2);
                    self.bw.write_bits(dod as u64 & ((1 << 14) - 1), 14);
                } else if (-(1 << 16) + 1..=(1 << 16)).contains(&dod) {
                    self.bw.write_bits(0b110, 3);
                    self.bw.write_bits(dod as u64 & ((1 << 17) - 1), 17);
                } else if (-(1 << 19) + 1..=(1 << 19)).contains(&dod) {
                    self.bw.write_bits(0b1110, 4);
                    self.bw.write_bits(dod as u64 & ((1 << 20) - 1), 20);
                } else {
                    self.bw.write_bits(0b1111, 4);
                    self.bw.write_bits(dod as u64, 64);
                }
                self.t_delta = d;
                self.write_value(vb);
            }
        }
        self.t = t;
        self.v = vb;
        self.maxt = t;
        self.count += 1;
    }

    fn write_value(&mut self, vb: u64) {
        let delta = vb ^ self.v;
        if delta == 0 {
            self.bw.write_bit(false);
            return;
        }
        self.bw.write_bit(true);
        let mut lead = delta.leading_zeros() as u8;
        let trail = delta.trailing_zeros() as u8;
        if lead >= 32 {
            lead = 31; // 5 bits
        }
        if self.leading != 0xff && lead >= self.leading && trail >= self.trailing {
            self.bw.write_bit(false);
            let sig = 64 - self.leading as u32 - self.trailing as u32;
            self.bw.write_bits(delta >> self.trailing, sig);
            return;
        }
        self.leading = lead;
        self.trailing = trail;
        self.bw.write_bit(true);
        self.bw.write_bits(lead as u64, 5);
        let sig = 64 - lead as u32 - trail as u32;
        self.bw.write_bits((sig & 63) as u64, 6); // 64 is stored as 0
        self.bw.write_bits(delta >> trail, sig);
    }

    /// The encoded chunk (count patched in).
    pub fn bytes(&self) -> Vec<u8> {
        let mut b = self.bw.buf.clone();
        b[0] = (self.count >> 8) as u8;
        b[1] = self.count as u8;
        b
    }

    pub fn encoded_len(&self) -> usize {
        self.bw.buf.len()
    }

    pub fn iter(&self) -> ChunkIter<'_> {
        // The count in the buffer is not patched yet; pass it explicitly.
        ChunkIter::with_count(&self.bw.buf, self.count)
    }
}

pub struct ChunkIter<'a> {
    r: BitReader<'a>,
    n: u16,
    i: u16,
    t: i64,
    t_delta: i64,
    v: u64,
    leading: u32,
    trailing: u32,
    pub err: bool,
}

impl<'a> ChunkIter<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        let n = if buf.len() >= 2 { u16::from_be_bytes([buf[0], buf[1]]) } else { 0 };
        Self::with_count(buf, n)
    }

    fn with_count(buf: &'a [u8], n: u16) -> Self {
        let mut r = BitReader::new(buf);
        r.read_bits(16);
        ChunkIter { r, n, i: 0, t: 0, t_delta: 0, v: 0, leading: 0, trailing: 0, err: false }
    }

    fn read_value(&mut self) -> Option<()> {
        if !self.r.read_bit()? {
            return Some(());
        }
        if self.r.read_bit()? {
            self.leading = self.r.read_bits(5)? as u32;
            let mut sig = self.r.read_bits(6)? as u32;
            if sig == 0 {
                sig = 64;
            }
            self.trailing = 64 - self.leading - sig;
        }
        let sig = 64 - self.leading - self.trailing;
        let bits = self.r.read_bits(sig)?;
        self.v ^= bits << self.trailing;
        Some(())
    }

    fn step(&mut self) -> Option<(i64, f64)> {
        match self.i {
            0 => {
                self.t = self.r.read_varint()?;
                self.v = self.r.read_bits(64)?;
            }
            1 => {
                self.t_delta = self.r.read_varint()?;
                self.t += self.t_delta;
                self.read_value()?;
            }
            _ => {
                let mut prefix = 0;
                while prefix < 4 && self.r.read_bit()? {
                    prefix += 1;
                }
                let dod = match prefix {
                    0 => 0,
                    1 => sign_extend(self.r.read_bits(14)?, 14),
                    2 => sign_extend(self.r.read_bits(17)?, 17),
                    3 => sign_extend(self.r.read_bits(20)?, 20),
                    _ => self.r.read_bits(64)? as i64,
                };
                self.t_delta += dod;
                self.t += self.t_delta;
                self.read_value()?;
            }
        }
        self.i += 1;
        Some((self.t, f64::from_bits(self.v)))
    }
}

fn sign_extend(v: u64, bits: u32) -> i64 {
    let shift = 64 - bits;
    ((v << shift) as i64) >> shift
}

impl Iterator for ChunkIter<'_> {
    type Item = (i64, f64);
    fn next(&mut self) -> Option<(i64, f64)> {
        if self.i >= self.n || self.err {
            return None;
        }
        let s = self.step();
        if s.is_none() {
            self.err = true;
        }
        s
    }
}

/// Number of samples in an encoded chunk.
pub fn chunk_len(buf: &[u8]) -> usize {
    if buf.len() >= 2 { u16::from_be_bytes([buf[0], buf[1]]) as usize } else { 0 }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(samples: &[(i64, f64)]) -> usize {
        let mut w = ChunkWriter::new();
        for &(t, v) in samples {
            w.append(t, v);
        }
        let live: Vec<_> = w.iter().collect();
        let b = w.bytes();
        let got: Vec<_> = ChunkIter::new(&b).collect();
        assert_eq!(got.len(), samples.len());
        for (a, b) in got.iter().zip(samples).chain(live.iter().zip(samples)) {
            assert_eq!(a.0, b.0);
            assert_eq!(a.1.to_bits(), b.1.to_bits());
        }
        b.len()
    }

    #[test]
    fn regular_scrapes_compress_well() {
        // A counter scraped every 15 s with a little jitter.
        let s: Vec<_> = (0..120).map(|i| (1_700_000_000_000 + i * 15_000 + (i % 3), (i * 37) as f64)).collect();
        let n = round_trip(&s);
        assert!(n < 120 * 4, "{n} bytes"); // jittered timestamps cost 16 bits each, integer counters ~2 bytes
        // A constant gauge on an exact interval: about 2 bits a sample.
        let c: Vec<_> = (0..120).map(|i| (i * 10_000, 42.5)).collect();
        assert!(round_trip(&c) < 50);
    }

    #[test]
    fn random_values_and_gaps() {
        let mut rng = fastrand::Rng::with_seed(3);
        for _ in 0..300 {
            let mut t = rng.i64(-1_000_000_000_000..1_000_000_000_000);
            let mut s = vec![];
            for _ in 0..rng.usize(1..=120) {
                let v = match rng.u8(0..5) {
                    0 => f64::from_bits(rng.u64(..)),
                    1 => rng.f64() * 100.0,
                    2 => rng.i32(..) as f64,
                    3 => f64::NAN,
                    _ => s.last().map(|x: &(i64, f64)| x.1).unwrap_or(0.0),
                };
                s.push((t, v));
                t += match rng.u8(0..4) {
                    0 => 15_000,
                    1 => rng.i64(0..20_000),
                    2 => rng.i64(0..1_000_000_000),
                    _ => rng.i64(0..i64::MAX / 1_000_000),
                };
            }
            round_trip(&s);
        }
    }

    #[test]
    fn truncated_chunk_stops_cleanly() {
        let mut w = ChunkWriter::new();
        for i in 0..50 {
            w.append(i * 1000, i as f64 * 1.5);
        }
        let b = w.bytes();
        let mut it = ChunkIter::new(&b[..b.len() / 2]);
        let n = it.by_ref().count();
        assert!(n < 50 && it.err);
    }
}

//! Bit-level writer and reader, most significant bit first.

#[derive(Clone, Default, Debug)]
pub struct BitWriter {
    pub buf: Vec<u8>,
    /// Bits used in the last byte (0 means the last byte is full or there is none).
    used: u8,
}

impl BitWriter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(bytes: usize) -> Self {
        BitWriter { buf: Vec::with_capacity(bytes), used: 0 }
    }

    #[inline]
    pub fn write_bit(&mut self, bit: bool) {
        if self.used == 0 {
            self.buf.push(0);
        }
        if bit {
            *self.buf.last_mut().unwrap() |= 0x80 >> self.used;
        }
        self.used = (self.used + 1) & 7;
    }

    /// Writes the low `n` bits of `v` (n <= 64).
    #[inline]
    pub fn write_bits(&mut self, v: u64, mut n: u32) {
        debug_assert!(n <= 64);
        while n > 0 {
            if self.used == 0 {
                self.buf.push(0);
            }
            let free = 8 - self.used as u32;
            let take = free.min(n);
            let chunk = ((v >> (n - take)) & ((1u64 << take) - 1)) as u8;
            *self.buf.last_mut().unwrap() |= chunk << (free - take);
            self.used = ((self.used as u32 + take) & 7) as u8;
            n -= take;
        }
    }

    pub fn write_uvarint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.write_bits((v as u8 | 0x80) as u64, 8);
            v >>= 7;
        }
        self.write_bits(v, 8);
    }

    pub fn write_varint(&mut self, v: i64) {
        self.write_uvarint(((v << 1) ^ (v >> 63)) as u64);
    }

    pub fn bit_len(&self) -> usize {
        if self.used == 0 { self.buf.len() * 8 } else { (self.buf.len() - 1) * 8 + self.used as usize }
    }
}

pub struct BitReader<'a> {
    buf: &'a [u8],
    pos: usize, // in bits
}

impl<'a> BitReader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        BitReader { buf, pos: 0 }
    }

    #[inline]
    pub fn read_bit(&mut self) -> Option<bool> {
        let byte = *self.buf.get(self.pos >> 3)?;
        let bit = byte & (0x80 >> (self.pos & 7)) != 0;
        self.pos += 1;
        Some(bit)
    }

    #[inline]
    pub fn read_bits(&mut self, mut n: u32) -> Option<u64> {
        if self.pos + n as usize > self.buf.len() * 8 {
            return None;
        }
        let mut v = 0u64;
        while n > 0 {
            let byte = self.buf[self.pos >> 3];
            let off = (self.pos & 7) as u32;
            let avail = 8 - off;
            let take = avail.min(n);
            let bits = (byte >> (avail - take)) as u64 & ((1u64 << take) - 1);
            v = if take == 64 { bits } else { (v << take) | bits };
            self.pos += take as usize;
            n -= take;
        }
        Some(v)
    }

    pub fn read_uvarint(&mut self) -> Option<u64> {
        let mut v = 0u64;
        let mut shift = 0;
        loop {
            let b = self.read_bits(8)?;
            v |= (b & 0x7f) << shift;
            if b < 0x80 {
                return Some(v);
            }
            shift += 7;
            if shift > 63 {
                return None;
            }
        }
    }

    pub fn read_varint(&mut self) -> Option<i64> {
        let u = self.read_uvarint()?;
        Some(((u >> 1) as i64) ^ -((u & 1) as i64))
    }
}

/// Byte-level varints for the WAL and index files.
pub fn put_uvarint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

pub fn put_varint(out: &mut Vec<u8>, v: i64) {
    put_uvarint(out, ((v << 1) ^ (v >> 63)) as u64);
}

pub fn get_uvarint(buf: &[u8], pos: &mut usize) -> Option<u64> {
    let mut v = 0u64;
    let mut shift = 0;
    loop {
        let b = *buf.get(*pos)?;
        *pos += 1;
        v |= ((b & 0x7f) as u64) << shift;
        if b < 0x80 {
            return Some(v);
        }
        shift += 7;
        if shift > 63 {
            return None;
        }
    }
}

pub fn get_varint(buf: &[u8], pos: &mut usize) -> Option<i64> {
    let u = get_uvarint(buf, pos)?;
    Some(((u >> 1) as i64) ^ -((u & 1) as i64))
}

pub fn put_str(out: &mut Vec<u8>, s: &str) {
    put_uvarint(out, s.len() as u64);
    out.extend_from_slice(s.as_bytes());
}

pub fn get_str(buf: &[u8], pos: &mut usize) -> Option<String> {
    let n = get_uvarint(buf, pos)? as usize;
    let s = buf.get(*pos..*pos + n)?;
    *pos += n;
    String::from_utf8(s.to_vec()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_random_widths() {
        let mut rng = fastrand::Rng::with_seed(7);
        let mut w = BitWriter::new();
        let mut items = vec![];
        for _ in 0..5000 {
            let n = rng.u32(1..=64);
            let v = if n == 64 { rng.u64(..) } else { rng.u64(..) & ((1u64 << n) - 1) };
            w.write_bits(v, n);
            items.push((v, n));
            if rng.bool() {
                let i = rng.i64(..) >> rng.u32(0..63);
                w.write_varint(i);
                items.push((i as u64, 0));
            }
        }
        let mut r = BitReader::new(&w.buf);
        for (v, n) in items {
            if n == 0 {
                assert_eq!(r.read_varint().unwrap(), v as i64);
            } else {
                assert_eq!(r.read_bits(n).unwrap(), v);
            }
        }
    }

    #[test]
    fn byte_varints() {
        let mut b = vec![];
        for v in [0i64, 1, -1, 63, -64, 1 << 40, i64::MIN, i64::MAX] {
            put_varint(&mut b, v);
        }
        let mut p = 0;
        for v in [0i64, 1, -1, 63, -64, 1 << 40, i64::MIN, i64::MAX] {
            assert_eq!(get_varint(&b, &mut p), Some(v));
        }
    }
}

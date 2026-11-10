//! A fixed-bucket histogram for Ripple's own metrics, in the Prometheus text format.

#[derive(Clone, Debug)]
pub struct Histogram {
    pub bounds: Vec<f64>,
    pub counts: Vec<u64>,
    pub sum: f64,
    pub count: u64,
}

impl Histogram {
    pub fn new(bounds: &[f64]) -> Self {
        Histogram { bounds: bounds.to_vec(), counts: vec![0; bounds.len()], sum: 0.0, count: 0 }
    }

    /// Exponential buckets from 50 µs to about 26 s.
    pub fn latency() -> Self {
        let b: Vec<f64> = (0..20).map(|i| 5e-5 * 2f64.powi(i)).collect();
        Self::new(&b)
    }

    pub fn observe(&mut self, v: f64) {
        for (b, c) in self.bounds.iter().zip(self.counts.iter_mut()) {
            if v <= *b {
                *c += 1;
            }
        }
        self.sum += v;
        self.count += 1;
    }

    /// `name_bucket{labels,le="..."}` lines plus `_sum` and `_count`.
    pub fn write(&self, out: &mut String, name: &str, labels: &str) {
        use std::fmt::Write;
        let sep = if labels.is_empty() { "" } else { "," };
        for (b, c) in self.bounds.iter().zip(&self.counts) {
            let _ = writeln!(out, "{name}_bucket{{{labels}{sep}le=\"{b}\"}} {c}");
        }
        let _ = writeln!(out, "{name}_bucket{{{labels}{sep}le=\"+Inf\"}} {}", self.count);
        let braces = if labels.is_empty() { String::new() } else { format!("{{{labels}}}") };
        let _ = writeln!(out, "{name}_sum{braces} {}", self.sum);
        let _ = writeln!(out, "{name}_count{braces} {}", self.count);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cumulative_buckets() {
        let mut h = Histogram::new(&[0.1, 1.0]);
        for v in [0.05, 0.5, 5.0] {
            h.observe(v);
        }
        assert_eq!(h.counts, vec![1, 2]);
        let mut s = String::new();
        h.write(&mut s, "x", "a=\"b\"");
        assert!(s.contains("x_bucket{a=\"b\",le=\"+Inf\"} 3"));
        assert!(s.contains("x_count{a=\"b\"} 3"));
    }
}

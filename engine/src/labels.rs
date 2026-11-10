//! Label sets and the Prometheus text exposition format.

use std::fmt;
use std::sync::Arc;

pub const METRIC_NAME: &str = "__name__";

/// A label set sorted by name; the metric name is the `__name__` label. Cloning is cheap (shared).
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Labels(pub Arc<[(String, String)]>);

impl Labels {
    pub fn new(mut pairs: Vec<(String, String)>) -> Self {
        pairs.retain(|(_, v)| !v.is_empty());
        pairs.sort();
        pairs.dedup_by(|a, b| a.0 == b.0);
        Labels(pairs.into())
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        self.0.iter().find(|(n, _)| n == name).map(|(_, v)| v.as_str())
    }

    pub fn name(&self) -> &str {
        self.get(METRIC_NAME).unwrap_or("")
    }

    pub fn without_name(&self) -> Labels {
        Labels(self.0.iter().filter(|(n, _)| n != METRIC_NAME).cloned().collect())
    }

    /// Keeps only `names` (with `keep`) or drops them (without).
    pub fn project(&self, names: &[String], keep: bool) -> Labels {
        Labels(self.0.iter().filter(|(n, _)| names.iter().any(|x| x == n) == keep).cloned().collect())
    }

    pub fn with(&self, name: &str, value: &str) -> Labels {
        let mut v: Vec<_> = self.0.iter().filter(|(n, _)| n != name).cloned().collect();
        v.push((name.to_string(), value.to_string()));
        Labels::new(v)
    }

    pub fn to_json(&self) -> serde_json::Value {
        let mut m = serde_json::Map::new();
        for (n, v) in self.0.iter() {
            m.insert(n.clone(), serde_json::Value::String(v.clone()));
        }
        serde_json::Value::Object(m)
    }
}

impl fmt::Display for Labels {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{{", self.name())?;
        let mut first = true;
        for (n, v) in self.0.iter() {
            if n == METRIC_NAME {
                continue;
            }
            if !first {
                write!(f, ",")?;
            }
            first = false;
            write!(f, "{n}=\"{}\"", v.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n"))?;
        }
        write!(f, "}}")
    }
}

impl fmt::Debug for Labels {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[derive(Debug, PartialEq)]
pub struct ParsedSample {
    pub labels: Labels,
    pub value: f64,
    pub timestamp: Option<i64>,
}

fn is_name_char(c: u8, first: bool) -> bool {
    c.is_ascii_alphabetic() || c == b'_' || c == b':' || (!first && c.is_ascii_digit())
}

pub fn parse_float(s: &str) -> Option<f64> {
    match s {
        "NaN" | "nan" => Some(f64::NAN),
        "+Inf" | "Inf" | "+inf" | "inf" => Some(f64::INFINITY),
        "-Inf" | "-inf" => Some(f64::NEG_INFINITY),
        _ => s.parse().ok(),
    }
}

/// Parses one line of the text format (`name{a="b"} value [timestamp_ms]`). Comments and blank
/// lines give `Ok(None)`.
pub fn parse_line(line: &str) -> Result<Option<ParsedSample>, String> {
    let b = line.trim_end_matches('\r').as_bytes();
    let mut i = 0;
    while i < b.len() && (b[i] == b' ' || b[i] == b'\t') {
        i += 1;
    }
    if i == b.len() || b[i] == b'#' {
        return Ok(None);
    }
    let start = i;
    while i < b.len() && is_name_char(b[i], i == start) {
        i += 1;
    }
    if i == start {
        return Err(format!("bad metric name in {line:?}"));
    }
    let mut pairs = vec![(METRIC_NAME.to_string(), line[start..i].to_string())];
    if i < b.len() && b[i] == b'{' {
        i += 1;
        loop {
            while i < b.len() && (b[i] == b' ' || b[i] == b',') {
                i += 1;
            }
            if i < b.len() && b[i] == b'}' {
                i += 1;
                break;
            }
            let ns = i;
            while i < b.len() && is_name_char(b[i], i == ns) {
                i += 1;
            }
            if i == ns || i + 1 >= b.len() || b[i] != b'=' || b[i + 1] != b'"' {
                return Err(format!("bad label in {line:?}"));
            }
            let name = line[ns..i].to_string();
            i += 2;
            let mut val = Vec::new();
            loop {
                match b.get(i) {
                    None => return Err(format!("unterminated label value in {line:?}")),
                    Some(b'"') => {
                        i += 1;
                        break;
                    }
                    Some(b'\\') => {
                        match b.get(i + 1) {
                            Some(b'n') => val.push(b'\n'),
                            Some(&c) => val.push(c),
                            None => return Err(format!("bad escape in {line:?}")),
                        }
                        i += 2;
                    }
                    Some(&c) => {
                        val.push(c);
                        i += 1;
                    }
                }
            }
            pairs.push((name, String::from_utf8(val).map_err(|e| e.to_string())?));
        }
    }
    let mut rest = line[i..].split_ascii_whitespace();
    let value = rest.next().and_then(parse_float).ok_or_else(|| format!("bad value in {line:?}"))?;
    let timestamp = match rest.next() {
        Some(t) => Some(t.parse::<i64>().map_err(|_| format!("bad timestamp in {line:?}"))?),
        None => None,
    };
    Ok(Some(ParsedSample { labels: Labels::new(pairs), value, timestamp }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_text_format() {
        let s = parse_line(r#"http_requests_total{method="post",code="200"} 1027 1395066363000"#).unwrap().unwrap();
        assert_eq!(s.labels.name(), "http_requests_total");
        assert_eq!(s.labels.get("code"), Some("200"));
        assert_eq!(s.value, 1027.0);
        assert_eq!(s.timestamp, Some(1395066363000));
        let s = parse_line(r#"msdos_file_access_time_seconds{path="C:\\DIR\\FILE.TXT",error="Cannot find file:\n\"FILE.TXT\""} 1.458255915e9"#)
            .unwrap()
            .unwrap();
        assert_eq!(s.labels.get("path"), Some(r"C:\DIR\FILE.TXT"));
        assert_eq!(s.labels.get("error"), Some("Cannot find file:\n\"FILE.TXT\""));
        assert!(parse_line("metric_without_labels +Inf").unwrap().unwrap().value.is_infinite());
        assert!(parse_line("# HELP x y").unwrap().is_none());
        assert!(parse_line("bad{x=1} 2").is_err());
        let s = parse_line(r#"a{b="1",} 2"#).unwrap().unwrap();
        assert_eq!(s.labels.to_string(), r#"a{b="1"}"#);
    }

    #[test]
    fn display_round_trips() {
        let l = Labels::new(vec![("__name__".into(), "m".into()), ("z".into(), "a\"b".into()), ("a".into(), "x".into())]);
        let s = parse_line(&format!("{l} 1")).unwrap().unwrap();
        assert_eq!(s.labels, l);
    }
}

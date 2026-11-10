//! A subset of PromQL.
//!
//! Supported: number literals; instant and range selectors with `=`, `!=`, `=~`, `!~` and
//! `offset`; arithmetic (`+ - * / % ^`), comparisons (with `bool`), `and`/`or`/`unless`, with
//! `on(...)`/`ignoring(...)` one-to-one matching; aggregations `sum avg min max count group
//! stddev stdvar topk bottomk quantile` with `by`/`without`; functions `rate irate increase delta
//! idelta {avg,min,max,sum,count,last,stddev}_over_time quantile_over_time histogram_quantile
//! abs ceil floor round sqrt exp ln log2 log10 clamp clamp_min clamp_max time vector scalar sort
//! sort_desc timestamp`.
//!
//! Not supported: `group_left`/`group_right`, subqueries, the `@` modifier, native histograms,
//! string functions such as `label_replace`.

use crate::labels::{Labels, METRIC_NAME, parse_float};
use crate::storage::{Db, Matcher};
use std::collections::{BTreeMap, HashMap};

pub const LOOKBACK_MS: i64 = 5 * 60 * 1000;
pub const MAX_POINTS: i64 = 11_000;
pub const MAX_SAMPLES: usize = 50_000_000;

// ---------------------------------------------------------------- lexer

#[derive(Clone, Debug, PartialEq)]
enum Tok {
    Ident(String),
    Num(f64),
    Str(String),
    Dur(i64),
    Op(&'static str),
    Eof,
}

pub fn parse_duration(s: &str) -> Result<i64, String> {
    let b = s.as_bytes();
    let mut i = 0;
    let mut total = 0i64;
    if s.is_empty() {
        return Err("empty duration".into());
    }
    while i < b.len() {
        let st = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        let n: i64 = s[st..i].parse().map_err(|_| format!("bad duration {s:?}"))?;
        let us = i;
        while i < b.len() && b[i].is_ascii_alphabetic() {
            i += 1;
        }
        let unit = match &s[us..i] {
            "ms" => 1,
            "s" => 1000,
            "m" => 60_000,
            "h" => 3_600_000,
            "d" => 86_400_000,
            "w" => 7 * 86_400_000,
            "y" => 365 * 86_400_000,
            _ => return Err(format!("bad duration {s:?}")),
        };
        total += n * unit;
    }
    Ok(total)
}

const OPS: [&str; 24] = ["!=", "=~", "!~", "==", ">=", "<=", "{", "}", "(", ")", "[", "]", ",", "=", "+", "-", "*", "/", "%", "^", ">", "<", ":", "@"];

fn lex(src: &str) -> Result<Vec<Tok>, String> {
    let b = src.as_bytes();
    let mut i = 0;
    let mut out = vec![];
    while i < b.len() {
        let c = b[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if c == b'#' {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if c == b'"' || c == b'\'' || c == b'`' {
            let q = c;
            i += 1;
            let mut s = Vec::new();
            loop {
                match b.get(i) {
                    None => return Err("unterminated string".into()),
                    Some(&x) if x == q => {
                        i += 1;
                        break;
                    }
                    Some(b'\\') if q != b'`' => {
                        let e = *b.get(i + 1).ok_or("bad escape")?;
                        s.push(match e {
                            b'n' => b'\n',
                            b't' => b'\t',
                            x => x,
                        });
                        i += 2;
                    }
                    Some(&x) => {
                        s.push(x);
                        i += 1;
                    }
                }
            }
            out.push(Tok::Str(String::from_utf8(s).map_err(|e| e.to_string())?));
            continue;
        }
        // A duration inside brackets or after `offset`.
        let after_offset = matches!(out.last(), Some(Tok::Ident(s)) if s == "offset");
        if c.is_ascii_digit() && (after_offset || matches!(out.last(), Some(Tok::Op("[")))) {
            let st = i;
            while i < b.len() && b[i].is_ascii_alphanumeric() {
                i += 1;
            }
            out.push(Tok::Dur(parse_duration(&src[st..i])?));
            continue;
        }
        if c.is_ascii_digit() || (c == b'.' && b.get(i + 1).is_some_and(|x| x.is_ascii_digit())) {
            let st = i;
            if src[i..].starts_with("0x") {
                i += 2;
                while i < b.len() && b[i].is_ascii_hexdigit() {
                    i += 1;
                }
                out.push(Tok::Num(i64::from_str_radix(&src[st + 2..i], 16).map_err(|e| e.to_string())? as f64));
                continue;
            }
            while i < b.len() && (b[i].is_ascii_digit() || b[i] == b'.') {
                i += 1;
            }
            if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
                i += 1;
                if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
                    i += 1;
                }
                while i < b.len() && b[i].is_ascii_digit() {
                    i += 1;
                }
            }
            out.push(Tok::Num(src[st..i].parse().map_err(|_| format!("bad number {}", &src[st..i]))?));
            continue;
        }
        if c.is_ascii_alphabetic() || c == b'_' || c == b':' && !matches!(out.last(), Some(Tok::Op("["))) {
            let st = i;
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b':') {
                i += 1;
            }
            let w = &src[st..i];
            match w {
                "Inf" | "inf" | "NaN" | "nan" => out.push(Tok::Num(parse_float(w).unwrap())),
                _ => out.push(Tok::Ident(w.to_string())),
            }
            continue;
        }
        let op = OPS.iter().find(|o| src[i..].starts_with(**o)).ok_or_else(|| format!("unexpected character {:?}", c as char))?;
        out.push(Tok::Op(op));
        i += op.len();
    }
    out.push(Tok::Eof);
    Ok(out)
}

// ---------------------------------------------------------------- AST and parser

#[derive(Clone, Debug)]
pub enum Expr {
    Num(f64),
    Str(String),
    Selector { matchers: Vec<Matcher>, range: Option<i64>, offset: i64, id: usize },
    Call { func: String, args: Vec<Expr> },
    Agg { op: String, param: Option<Box<Expr>>, expr: Box<Expr>, grouping: Vec<String>, without: bool },
    Binary { op: String, lhs: Box<Expr>, rhs: Box<Expr>, return_bool: bool, on: Option<Vec<String>>, ignoring: Vec<String> },
    Neg(Box<Expr>),
}

struct Parser {
    toks: Vec<Tok>,
    pos: usize,
    selectors: usize,
}

const AGGS: [&str; 11] = ["sum", "avg", "min", "max", "count", "group", "stddev", "stdvar", "topk", "bottomk", "quantile"];

fn precedence(t: &Tok) -> Option<(u8, &'static str)> {
    match t {
        Tok::Ident(s) if s == "or" => Some((1, "or")),
        Tok::Ident(s) if s == "and" => Some((2, "and")),
        Tok::Ident(s) if s == "unless" => Some((2, "unless")),
        Tok::Op(o @ ("==" | "!=" | ">" | "<" | ">=" | "<=")) => Some((3, o)),
        Tok::Op(o @ ("+" | "-")) => Some((4, o)),
        Tok::Op(o @ ("*" | "/" | "%")) => Some((5, o)),
        Tok::Op("^") => Some((6, "^")),
        _ => None,
    }
}

impl Parser {
    fn peek(&self) -> &Tok {
        &self.toks[self.pos]
    }

    fn next(&mut self) -> Tok {
        let t = self.toks[self.pos].clone();
        if self.pos < self.toks.len() - 1 {
            self.pos += 1;
        }
        t
    }

    fn expect(&mut self, op: &str) -> Result<(), String> {
        match self.next() {
            Tok::Op(o) if o == op => Ok(()),
            t => Err(format!("expected {op:?}, found {t:?}")),
        }
    }

    fn is_op(&self, op: &str) -> bool {
        matches!(self.peek(), Tok::Op(o) if *o == op)
    }

    fn is_word(&self, w: &str) -> bool {
        matches!(self.peek(), Tok::Ident(s) if s == w)
    }

    fn expr(&mut self, min_prec: u8) -> Result<Expr, String> {
        let mut lhs = self.unary()?;
        while let Some((prec, op)) = precedence(self.peek()) {
            if prec < min_prec {
                break;
            }
            self.next();
            let mut return_bool = false;
            if self.is_word("bool") {
                self.next();
                return_bool = true;
            }
            let mut on = None;
            let mut ignoring = vec![];
            if self.is_word("on") || self.is_word("ignoring") {
                let is_on = self.is_word("on");
                self.next();
                let l = self.label_list()?;
                if is_on { on = Some(l) } else { ignoring = l }
            }
            if self.is_word("group_left") || self.is_word("group_right") {
                return Err("group_left/group_right are not supported".into());
            }
            // ^ is right-associative, the rest left-associative.
            let rhs = self.expr(if op == "^" { prec } else { prec + 1 })?;
            lhs = Expr::Binary { op: op.to_string(), lhs: Box::new(lhs), rhs: Box::new(rhs), return_bool, on, ignoring };
        }
        Ok(lhs)
    }

    fn unary(&mut self) -> Result<Expr, String> {
        if self.is_op("-") || self.is_op("+") {
            let neg = self.is_op("-");
            self.next();
            let e = self.expr(6)?; // binds tighter than * but looser than ^
            return Ok(match (neg, e) {
                (true, Expr::Num(n)) => Expr::Num(-n),
                (true, e) => Expr::Neg(Box::new(e)),
                (false, e) => e,
            });
        }
        let mut e = self.primary()?;
        // Postfix: range, offset.
        loop {
            if self.is_op("[") {
                self.next();
                let d = match self.next() {
                    Tok::Dur(d) => d,
                    t => return Err(format!("expected a duration, found {t:?}")),
                };
                if self.is_op(":") {
                    return Err("subqueries are not supported".into());
                }
                self.expect("]")?;
                match &mut e {
                    Expr::Selector { range, .. } if range.is_none() => *range = Some(d),
                    _ => return Err("a range can only follow an instant selector".into()),
                }
            } else if self.is_word("offset") {
                self.next();
                let neg = if self.is_op("-") {
                    self.next();
                    true
                } else {
                    false
                };
                let d = match self.next() {
                    Tok::Dur(d) => d,
                    Tok::Num(n) => (n * 1000.0) as i64,
                    t => return Err(format!("expected a duration, found {t:?}")),
                };
                match &mut e {
                    Expr::Selector { offset, .. } => *offset = if neg { -d } else { d },
                    _ => return Err("offset must follow a selector".into()),
                }
            } else if self.is_op("@") {
                return Err("the @ modifier is not supported".into());
            } else {
                break;
            }
        }
        Ok(e)
    }

    fn label_list(&mut self) -> Result<Vec<String>, String> {
        self.expect("(")?;
        let mut v = vec![];
        while !self.is_op(")") {
            match self.next() {
                Tok::Ident(s) => v.push(s),
                t => return Err(format!("expected a label name, found {t:?}")),
            }
            if self.is_op(",") {
                self.next();
            }
        }
        self.next();
        Ok(v)
    }

    fn matchers(&mut self, out: &mut Vec<Matcher>) -> Result<(), String> {
        self.expect("{")?;
        while !self.is_op("}") {
            let name = match self.next() {
                Tok::Ident(s) => s,
                t => return Err(format!("expected a label name, found {t:?}")),
            };
            let op = match self.next() {
                Tok::Op(o @ ("=" | "!=" | "=~" | "!~")) => o,
                t => return Err(format!("expected a match operator, found {t:?}")),
            };
            let val = match self.next() {
                Tok::Str(s) => s,
                t => return Err(format!("expected a string, found {t:?}")),
            };
            out.push(Matcher::new(&name, op, &val)?);
            if self.is_op(",") {
                self.next();
            }
        }
        self.next();
        Ok(())
    }

    fn selector(&mut self, name: Option<String>) -> Result<Expr, String> {
        let mut ms = vec![];
        if let Some(n) = name {
            ms.push(Matcher::eq(METRIC_NAME, &n));
        }
        if self.is_op("{") {
            self.matchers(&mut ms)?;
        }
        if ms.is_empty() || ms.iter().all(|m| m.matches(&Labels::default())) {
            return Err("a selector needs at least one matcher that does not match the empty string".into());
        }
        self.selectors += 1;
        Ok(Expr::Selector { matchers: ms, range: None, offset: 0, id: self.selectors - 1 })
    }

    fn primary(&mut self) -> Result<Expr, String> {
        match self.next() {
            Tok::Num(n) => Ok(Expr::Num(n)),
            Tok::Str(s) => Ok(Expr::Str(s)),
            Tok::Op("(") => {
                let e = self.expr(0)?;
                self.expect(")")?;
                Ok(e)
            }
            Tok::Op("{") => {
                self.pos -= 1;
                self.selector(None)
            }
            Tok::Ident(w) if AGGS.contains(&w.as_str()) && (self.is_op("(") || self.is_word("by") || self.is_word("without")) => {
                let mut grouping = vec![];
                let mut without = false;
                let mut have_grouping = false;
                if self.is_word("by") || self.is_word("without") {
                    without = self.is_word("without");
                    self.next();
                    grouping = self.label_list()?;
                    have_grouping = true;
                }
                self.expect("(")?;
                let mut args = vec![self.expr(0)?];
                while self.is_op(",") {
                    self.next();
                    args.push(self.expr(0)?);
                }
                self.expect(")")?;
                if !have_grouping && (self.is_word("by") || self.is_word("without")) {
                    without = self.is_word("without");
                    self.next();
                    grouping = self.label_list()?;
                }
                let needs_param = matches!(w.as_str(), "topk" | "bottomk" | "quantile");
                if args.len() != if needs_param { 2 } else { 1 } {
                    return Err(format!("wrong number of arguments to {w}"));
                }
                let expr = Box::new(args.pop().unwrap());
                let param = args.pop().map(Box::new);
                Ok(Expr::Agg { op: w, param, expr, grouping, without })
            }
            Tok::Ident(w) if self.is_op("(") && !matches!(w.as_str(), "by" | "without" | "on" | "ignoring") => {
                self.next();
                let mut args = vec![];
                while !self.is_op(")") {
                    args.push(self.expr(0)?);
                    if self.is_op(",") {
                        self.next();
                    } else if !self.is_op(")") {
                        return Err(format!("expected , or ) in the arguments of {w}"));
                    }
                }
                self.next();
                check_call(&w, &args)?;
                Ok(Expr::Call { func: w, args })
            }
            Tok::Ident(w) => self.selector(Some(w)),
            t => Err(format!("unexpected {t:?}")),
        }
    }
}

fn is_range(e: &Expr) -> bool {
    matches!(e, Expr::Selector { range: Some(_), .. })
}

fn check_call(f: &str, args: &[Expr]) -> Result<(), String> {
    let n = args.len();
    let ok = match f {
        "rate" | "irate" | "increase" | "delta" | "idelta" | "avg_over_time" | "min_over_time" | "max_over_time" | "sum_over_time"
        | "count_over_time" | "last_over_time" | "stddev_over_time" => n == 1 && is_range(&args[0]),
        "quantile_over_time" => n == 2 && is_range(&args[1]),
        "histogram_quantile" | "clamp_min" | "clamp_max" => n == 2,
        "clamp" => n == 3,
        "abs" | "ceil" | "floor" | "sqrt" | "exp" | "ln" | "log2" | "log10" | "vector" | "scalar" | "sort" | "sort_desc" | "timestamp" => n == 1,
        "round" => n == 1 || n == 2,
        "time" => n == 0,
        _ => return Err(format!("unknown function {f}")),
    };
    if ok { Ok(()) } else { Err(format!("wrong arguments to {f}")) }
}

pub fn parse(src: &str) -> Result<Expr, String> {
    let mut p = Parser { toks: lex(src)?, pos: 0, selectors: 0 };
    let e = p.expr(0)?;
    if *p.peek() != Tok::Eof {
        return Err(format!("unexpected {:?} after the expression", p.peek()));
    }
    if is_range(&e) {
        // A bare range selector is only valid in an instant query; the evaluator rejects it in
        // range queries.
    }
    Ok(e)
}

// ---------------------------------------------------------------- evaluation

#[derive(Clone, Debug)]
pub enum Value {
    Scalar(f64),
    Vector(Vec<(Labels, f64)>),
    Matrix(Vec<(Labels, Vec<(i64, f64)>)>),
    Str(String),
}

struct Eval<'a> {
    data: HashMap<usize, Vec<(Labels, Vec<(i64, f64)>)>>,
    _db: &'a Db,
}

fn collect_selectors<'e>(e: &'e Expr, out: &mut Vec<&'e Expr>) {
    match e {
        Expr::Selector { .. } => out.push(e),
        Expr::Call { args, .. } => args.iter().for_each(|a| collect_selectors(a, out)),
        Expr::Agg { param, expr, .. } => {
            if let Some(p) = param {
                collect_selectors(p, out);
            }
            collect_selectors(expr, out);
        }
        Expr::Binary { lhs, rhs, .. } => {
            collect_selectors(lhs, out);
            collect_selectors(rhs, out);
        }
        Expr::Neg(x) => collect_selectors(x, out),
        _ => {}
    }
}

/// Samples with t in (lo, hi] (binary search on a time-sorted slice).
fn window(s: &[(i64, f64)], lo: i64, hi: i64) -> &[(i64, f64)] {
    let a = s.partition_point(|x| x.0 <= lo);
    let b = s.partition_point(|x| x.0 <= hi);
    &s[a..b]
}

fn extrapolated(s: &[(i64, f64)], t: i64, range: i64, counter: bool, is_rate: bool) -> Option<f64> {
    if s.len() < 2 {
        return None;
    }
    let (first, last) = (s[0], s[s.len() - 1]);
    let mut result = last.1 - first.1;
    if counter {
        for w in s.windows(2) {
            if w[1].1 < w[0].1 {
                result += w[0].1;
            }
        }
    }
    let range_start = (t - range) as f64 / 1000.0;
    let range_end = t as f64 / 1000.0;
    let mut to_start = first.0 as f64 / 1000.0 - range_start;
    let to_end = range_end - last.0 as f64 / 1000.0;
    let sampled = (last.0 - first.0) as f64 / 1000.0;
    let avg = sampled / (s.len() - 1) as f64;
    if counter && result > 0.0 && first.1 >= 0.0 {
        let to_zero = sampled * (first.1 / result);
        if to_zero < to_start {
            to_start = to_zero;
        }
    }
    let threshold = avg * 1.1;
    let mut interval = sampled;
    interval += if to_start < threshold { to_start } else { avg / 2.0 };
    interval += if to_end < threshold { to_end } else { avg / 2.0 };
    let mut factor = interval / sampled;
    if is_rate {
        factor /= range as f64 / 1000.0;
    }
    Some(result * factor)
}

fn quantile(q: f64, v: &mut [f64]) -> f64 {
    if v.is_empty() || q.is_nan() {
        return f64::NAN;
    }
    if q < 0.0 {
        return f64::NEG_INFINITY;
    }
    if q > 1.0 {
        return f64::INFINITY;
    }
    v.sort_by(|a, b| a.total_cmp(b));
    let rank = q * (v.len() - 1) as f64;
    let lo = rank.floor() as usize;
    let hi = (lo + 1).min(v.len() - 1);
    let w = rank - lo as f64;
    v[lo] * (1.0 - w) + v[hi] * w
}

fn range_fn(f: &str, s: &[(i64, f64)], t: i64, range: i64, param: f64) -> Option<f64> {
    if s.is_empty() {
        return None;
    }
    let vals = || s.iter().map(|x| x.1);
    Some(match f {
        "rate" => extrapolated(s, t, range, true, true)?,
        "increase" => extrapolated(s, t, range, true, false)?,
        "delta" => extrapolated(s, t, range, false, false)?,
        "irate" | "idelta" => {
            if s.len() < 2 {
                return None;
            }
            let (a, b) = (s[s.len() - 2], s[s.len() - 1]);
            let d = if f == "irate" && b.1 < a.1 { b.1 } else { b.1 - a.1 };
            if f == "irate" { d / ((b.0 - a.0) as f64 / 1000.0) } else { d }
        }
        "avg_over_time" => vals().sum::<f64>() / s.len() as f64,
        "min_over_time" => vals().fold(f64::INFINITY, f64::min),
        "max_over_time" => vals().fold(f64::NEG_INFINITY, f64::max),
        "sum_over_time" => vals().sum(),
        "count_over_time" => s.len() as f64,
        "last_over_time" => s[s.len() - 1].1,
        "stddev_over_time" => {
            let m = vals().sum::<f64>() / s.len() as f64;
            (vals().map(|x| (x - m) * (x - m)).sum::<f64>() / s.len() as f64).sqrt()
        }
        "quantile_over_time" => quantile(param, &mut vals().collect::<Vec<_>>()),
        _ => unreachable!(),
    })
}

/// Prometheus' bucketQuantile: linear interpolation inside the bucket holding the rank.
pub fn bucket_quantile(q: f64, buckets: &mut [(f64, f64)]) -> f64 {
    if q.is_nan() {
        return f64::NAN;
    }
    if q < 0.0 {
        return f64::NEG_INFINITY;
    }
    if q > 1.0 {
        return f64::INFINITY;
    }
    buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
    if buckets.len() < 2 || buckets.last().unwrap().0 != f64::INFINITY {
        return f64::NAN;
    }
    // Counts must not decrease (they can, slightly, when scrapes race with updates).
    let mut maxc = f64::NEG_INFINITY;
    for b in buckets.iter_mut() {
        maxc = maxc.max(b.1);
        b.1 = maxc;
    }
    let total = buckets.last().unwrap().1;
    if total == 0.0 || total.is_nan() {
        return f64::NAN;
    }
    let mut rank = q * total;
    let b = buckets.partition_point(|x| x.1 < rank);
    if b == buckets.len() - 1 {
        return buckets[buckets.len() - 2].0;
    }
    if b == 0 && buckets[0].0 <= 0.0 {
        return buckets[0].0;
    }
    let (mut start, end, mut count) = (0.0, buckets[b].0, buckets[b].1);
    if b > 0 {
        start = buckets[b - 1].0;
        count -= buckets[b - 1].1;
        rank -= buckets[b - 1].1;
    }
    start + (end - start) * (rank / count)
}

fn signature(l: &Labels, on: &Option<Vec<String>>, ignoring: &[String]) -> Labels {
    match on {
        Some(names) => l.project(names, true),
        None => {
            let mut drop = ignoring.to_vec();
            drop.push(METRIC_NAME.into());
            l.project(&drop, false)
        }
    }
}

fn arith(op: &str, a: f64, b: f64) -> Option<f64> {
    Some(match op {
        "+" => a + b,
        "-" => a - b,
        "*" => a * b,
        "/" => a / b,
        "%" => a % b,
        "^" => a.powf(b),
        _ => return None,
    })
}

fn compare(op: &str, a: f64, b: f64) -> bool {
    match op {
        "==" => a == b,
        "!=" => a != b,
        ">" => a > b,
        "<" => a < b,
        ">=" => a >= b,
        "<=" => a <= b,
        _ => false,
    }
}

fn is_cmp(op: &str) -> bool {
    matches!(op, "==" | "!=" | ">" | "<" | ">=" | "<=")
}

impl Eval<'_> {
    fn scalar_arg(&self, e: &Expr, t: i64) -> Result<f64, String> {
        match self.eval(e, t)? {
            Value::Scalar(x) => Ok(x),
            _ => Err("expected a scalar argument".into()),
        }
    }

    fn eval(&self, e: &Expr, t: i64) -> Result<Value, String> {
        Ok(match e {
            Expr::Num(n) => Value::Scalar(*n),
            Expr::Str(s) => Value::Str(s.clone()),
            Expr::Neg(x) => match self.eval(x, t)? {
                Value::Scalar(v) => Value::Scalar(-v),
                Value::Vector(v) => Value::Vector(v.into_iter().map(|(l, x)| (l.without_name(), -x)).collect()),
                _ => return Err("cannot negate this".into()),
            },
            Expr::Selector { range, offset, id, .. } => {
                let data = &self.data[id];
                let at = t - offset;
                match range {
                    None => Value::Vector(
                        data.iter()
                            .filter_map(|(l, s)| window(s, at - LOOKBACK_MS, at).last().map(|x| (l.clone(), x.1)))
                            .collect(),
                    ),
                    Some(r) => Value::Matrix(
                        data.iter()
                            .map(|(l, s)| (l.clone(), window(s, at - r, at).to_vec()))
                            .filter(|(_, s)| !s.is_empty())
                            .collect(),
                    ),
                }
            }
            Expr::Call { func, args } => self.call(func, args, t)?,
            Expr::Agg { op, param, expr, grouping, without } => {
                let Value::Vector(v) = self.eval(expr, t)? else { return Err(format!("{op} needs an instant vector")) };
                let p = match param {
                    Some(p) => Some(self.scalar_arg(p, t)?),
                    None => None,
                };
                Value::Vector(aggregate(op, p, v, grouping, *without))
            }
            Expr::Binary { op, lhs, rhs, return_bool, on, ignoring } => {
                let a = self.eval(lhs, t)?;
                let b = self.eval(rhs, t)?;
                binary(op, a, b, *return_bool, on, ignoring)?
            }
        })
    }

    fn call(&self, f: &str, args: &[Expr], t: i64) -> Result<Value, String> {
        match f {
            "time" => return Ok(Value::Scalar(t as f64 / 1000.0)),
            "vector" => {
                let x = self.scalar_arg(&args[0], t)?;
                return Ok(Value::Vector(vec![(Labels::default(), x)]));
            }
            "scalar" => {
                let Value::Vector(v) = self.eval(&args[0], t)? else { return Err("scalar() needs a vector".into()) };
                return Ok(Value::Scalar(if v.len() == 1 { v[0].1 } else { f64::NAN }));
            }
            "histogram_quantile" => {
                let q = self.scalar_arg(&args[0], t)?;
                let Value::Vector(v) = self.eval(&args[1], t)? else { return Err("histogram_quantile needs a vector".into()) };
                let mut groups: BTreeMap<Labels, Vec<(f64, f64)>> = BTreeMap::new();
                for (l, x) in v {
                    let Some(le) = l.get("le").and_then(parse_float) else { continue };
                    groups.entry(l.project(&["le".into(), METRIC_NAME.into()], false)).or_default().push((le, x));
                }
                return Ok(Value::Vector(groups.into_iter().map(|(l, mut b)| (l, bucket_quantile(q, &mut b))).collect()));
            }
            _ => {}
        }
        if let Some((range_arg, param)) = match f {
            "quantile_over_time" => Some((&args[1], self.scalar_arg(&args[0], t)?)),
            "rate" | "irate" | "increase" | "delta" | "idelta" | "avg_over_time" | "min_over_time" | "max_over_time" | "sum_over_time"
            | "count_over_time" | "last_over_time" | "stddev_over_time" => Some((&args[0], 0.0)),
            _ => None,
        } {
            let Expr::Selector { range: Some(r), offset, id, .. } = range_arg else { unreachable!() };
            let at = t - offset;
            let keep_name = f == "last_over_time";
            let mut out = vec![];
            for (l, s) in &self.data[id] {
                if let Some(x) = range_fn(f, window(s, at - r, at), at, *r, param) {
                    out.push((if keep_name { l.clone() } else { l.without_name() }, x));
                }
            }
            return Ok(Value::Vector(out));
        }
        let v = match self.eval(&args[0], t)? {
            Value::Vector(v) => v,
            _ => return Err(format!("{f} needs an instant vector")),
        };
        let p1 = if args.len() > 1 { Some(self.scalar_arg(&args[1], t)?) } else { None };
        let p2 = if args.len() > 2 { Some(self.scalar_arg(&args[2], t)?) } else { None };
        let map = |g: &dyn Fn(f64) -> f64| Value::Vector(v.iter().map(|(l, x)| (l.without_name(), g(*x))).collect());
        Ok(match f {
            "abs" => map(&f64::abs),
            "ceil" => map(&f64::ceil),
            "floor" => map(&f64::floor),
            "sqrt" => map(&f64::sqrt),
            "exp" => map(&f64::exp),
            "ln" => map(&f64::ln),
            "log2" => map(&f64::log2),
            "log10" => map(&f64::log10),
            "round" => {
                let to = p1.unwrap_or(1.0);
                map(&|x| (x / to + 0.5).floor() * to)
            }
            "clamp_min" => map(&|x| x.max(p1.unwrap())),
            "clamp_max" => map(&|x| x.min(p1.unwrap())),
            "clamp" => {
                let (lo, hi) = (p1.unwrap(), p2.unwrap());
                if lo > hi { Value::Vector(vec![]) } else { map(&|x| x.clamp(lo, hi)) }
            }
            "timestamp" => {
                // The timestamp of the sample each series' value came from.
                let Expr::Selector { offset, id, range: None, .. } = &args[0] else { return Err("timestamp() needs a selector".into()) };
                let at = t - offset;
                Value::Vector(
                    self.data[id]
                        .iter()
                        .filter_map(|(l, s)| window(s, at - LOOKBACK_MS, at).last().map(|x| (l.without_name(), x.0 as f64 / 1000.0)))
                        .collect(),
                )
            }
            "sort" | "sort_desc" => {
                let mut v = v;
                v.sort_by(|a, b| if f == "sort" { a.1.total_cmp(&b.1) } else { b.1.total_cmp(&a.1) });
                Value::Vector(v)
            }
            _ => return Err(format!("unknown function {f}")),
        })
    }
}

fn aggregate(op: &str, p: Option<f64>, v: Vec<(Labels, f64)>, grouping: &[String], without: bool) -> Vec<(Labels, f64)> {
    let key = |l: &Labels| {
        if without {
            let mut drop = grouping.to_vec();
            drop.push(METRIC_NAME.into());
            l.project(&drop, false)
        } else {
            l.project(grouping, true)
        }
    };
    let mut groups: BTreeMap<Labels, Vec<(Labels, f64)>> = BTreeMap::new();
    for (l, x) in v {
        groups.entry(key(&l)).or_default().push((l, x));
    }
    let mut out = vec![];
    for (g, items) in groups {
        let vals = || items.iter().map(|x| x.1);
        let n = items.len() as f64;
        let x = match op {
            "sum" => vals().sum(),
            "avg" => vals().sum::<f64>() / n,
            "min" => vals().fold(f64::NAN, |a, b| if a.is_nan() || b < a { b } else { a }),
            "max" => vals().fold(f64::NAN, |a, b| if a.is_nan() || b > a { b } else { a }),
            "count" => n,
            "group" => 1.0,
            "stddev" | "stdvar" => {
                let m = vals().sum::<f64>() / n;
                let var = vals().map(|x| (x - m) * (x - m)).sum::<f64>() / n;
                if op == "stddev" { var.sqrt() } else { var }
            }
            "quantile" => quantile(p.unwrap_or(f64::NAN), &mut vals().collect::<Vec<_>>()),
            "topk" | "bottomk" => {
                let k = p.unwrap_or(0.0).max(0.0) as usize;
                let mut items = items;
                items.sort_by(|a, b| if op == "topk" { b.1.total_cmp(&a.1) } else { a.1.total_cmp(&b.1) });
                out.extend(items.into_iter().take(k));
                continue;
            }
            _ => unreachable!(),
        };
        out.push((g, x));
    }
    out
}

fn binary(op: &str, a: Value, b: Value, return_bool: bool, on: &Option<Vec<String>>, ignoring: &[String]) -> Result<Value, String> {
    let cmp = is_cmp(op);
    Ok(match (a, b) {
        (Value::Scalar(x), Value::Scalar(y)) => {
            Value::Scalar(if cmp { if compare(op, x, y) { 1.0 } else { 0.0 } } else { arith(op, x, y).ok_or("set operators need vectors")? })
        }
        (Value::Vector(v), Value::Scalar(y)) | (Value::Scalar(y), Value::Vector(v)) if matches!(op, "and" | "or" | "unless") => {
            let _ = (v, y);
            return Err(format!("{op} needs vectors on both sides"));
        }
        (Value::Vector(v), Value::Scalar(y)) => Value::Vector(vector_scalar(op, v, y, false, return_bool)),
        (Value::Scalar(x), Value::Vector(v)) => Value::Vector(vector_scalar(op, v, x, true, return_bool)),
        (Value::Vector(l), Value::Vector(r)) => {
            let sig = |x: &Labels| signature(x, on, ignoring);
            match op {
                "and" | "or" | "unless" => {
                    let rs: std::collections::HashSet<Labels> = r.iter().map(|x| sig(&x.0)).collect();
                    let mut out: Vec<_> = l.iter().filter(|x| (op == "unless") != rs.contains(&sig(&x.0)) || op == "or").cloned().collect();
                    if op == "or" {
                        let ls: std::collections::HashSet<Labels> = l.iter().map(|x| sig(&x.0)).collect();
                        out.extend(r.into_iter().filter(|x| !ls.contains(&sig(&x.0))));
                    }
                    Value::Vector(out)
                }
                _ => {
                    let mut rm: HashMap<Labels, f64> = HashMap::new();
                    for (lab, x) in &r {
                        if rm.insert(sig(lab), *x).is_some() {
                            return Err("many-to-one matching is not supported (duplicate series on the right)".into());
                        }
                    }
                    let mut seen = std::collections::HashSet::new();
                    let mut out = vec![];
                    for (lab, x) in l {
                        let s = sig(&lab);
                        let Some(&y) = rm.get(&s) else { continue };
                        if !seen.insert(s.clone()) {
                            return Err("many-to-one matching is not supported (duplicate series on the left)".into());
                        }
                        let res_labels = if on.is_some() { s } else { lab.without_name() };
                        if cmp {
                            let c = compare(op, x, y);
                            if return_bool {
                                out.push((res_labels, if c { 1.0 } else { 0.0 }));
                            } else if c {
                                out.push((lab, x));
                            }
                        } else {
                            out.push((res_labels, arith(op, x, y).unwrap()));
                        }
                    }
                    Value::Vector(out)
                }
            }
        }
        _ => return Err(format!("unsupported operands for {op}")),
    })
}

fn vector_scalar(op: &str, v: Vec<(Labels, f64)>, s: f64, scalar_left: bool, return_bool: bool) -> Vec<(Labels, f64)> {
    let mut out = vec![];
    for (l, x) in v {
        let (a, b) = if scalar_left { (s, x) } else { (x, s) };
        if is_cmp(op) {
            let c = compare(op, a, b);
            if return_bool {
                out.push((l.without_name(), if c { 1.0 } else { 0.0 }));
            } else if c {
                out.push((l, x));
            }
        } else {
            out.push((l.without_name(), arith(op, a, b).unwrap()));
        }
    }
    out
}

/// Loads the data every selector needs for evaluation at times in [start, end].
fn prepare<'a>(db: &'a Db, e: &Expr, start: i64, end: i64) -> Result<Eval<'a>, String> {
    let mut sels = vec![];
    collect_selectors(e, &mut sels);
    let mut data = HashMap::new();
    let mut total = 0usize;
    for s in sels {
        let Expr::Selector { matchers, range, offset, id } = s else { unreachable!() };
        let look = range.unwrap_or(LOOKBACK_MS);
        let d = db.select(matchers, start - offset - look + 1, end - offset);
        total += d.iter().map(|x| x.1.len()).sum::<usize>();
        if total > MAX_SAMPLES {
            return Err(format!("query would load more than {MAX_SAMPLES} samples"));
        }
        data.insert(*id, d);
    }
    Ok(Eval { data, _db: db })
}

/// Instant query at time `t` (ms).
pub fn instant(db: &Db, q: &str, t: i64) -> Result<Value, String> {
    let e = parse(q)?;
    let ev = prepare(db, &e, t, t)?;
    ev.eval(&e, t)
}

/// Range query: a matrix with one point per step for every series that has a value then.
pub fn range(db: &Db, q: &str, start: i64, end: i64, step: i64) -> Result<Value, String> {
    if step <= 0 {
        return Err("step must be positive".into());
    }
    if end < start {
        return Err("end is before start".into());
    }
    if (end - start) / step > MAX_POINTS {
        return Err(format!("more than {MAX_POINTS} points per series; use a larger step"));
    }
    let e = parse(q)?;
    if is_range(&e) {
        return Err("a range query must return an instant vector or a scalar".into());
    }
    let ev = prepare(db, &e, start, end)?;
    let mut out: BTreeMap<Labels, Vec<(i64, f64)>> = BTreeMap::new();
    let mut t = start;
    while t <= end {
        match ev.eval(&e, t)? {
            Value::Scalar(x) => out.entry(Labels::default()).or_default().push((t, x)),
            Value::Vector(v) => {
                for (l, x) in v {
                    out.entry(l).or_default().push((t, x));
                }
            }
            _ => return Err("a range query must return an instant vector or a scalar".into()),
        }
        t += step;
    }
    Ok(Value::Matrix(out.into_iter().collect()))
}

fn fmt_val(x: f64) -> String {
    if x.is_nan() {
        "NaN".into()
    } else if x == f64::INFINITY {
        "+Inf".into()
    } else if x == f64::NEG_INFINITY {
        "-Inf".into()
    } else {
        format!("{x}")
    }
}

/// The Prometheus HTTP API representation of a result.
pub fn to_json(v: &Value, t: i64) -> serde_json::Value {
    use serde_json::json;
    let ts = |t: i64| json!(t as f64 / 1000.0);
    match v {
        Value::Scalar(x) => json!({"resultType": "scalar", "result": [ts(t), fmt_val(*x)]}),
        Value::Str(s) => json!({"resultType": "string", "result": [ts(t), s]}),
        Value::Vector(v) => json!({"resultType": "vector", "result": v.iter().map(|(l, x)| json!({"metric": l.to_json(), "value": [ts(t), fmt_val(*x)]})).collect::<Vec<_>>()}),
        Value::Matrix(m) => json!({"resultType": "matrix", "result": m.iter().map(|(l, s)| json!({"metric": l.to_json(), "values": s.iter().map(|(t, x)| json!([ts(*t), fmt_val(*x)])).collect::<Vec<_>>()})).collect::<Vec<_>>()}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::parse_line;
    use crate::storage::{Db, Options};

    fn db_with(lines: &[&str]) -> (tempfile::TempDir, Db) {
        let d = tempfile::tempdir().unwrap();
        let db = Db::open(d.path(), Options { fsync: false, ..Default::default() }).unwrap();
        let batch: Vec<_> = lines.iter().filter_map(|l| parse_line(l).unwrap()).collect();
        db.append(&batch, 0).unwrap();
        (d, db)
    }

    fn vector(v: Value) -> Vec<(String, f64)> {
        let Value::Vector(v) = v else { panic!("{v:?}") };
        let mut out: Vec<_> = v.into_iter().map(|(l, x)| (l.to_string(), x)).collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    fn counter_lines() -> Vec<String> {
        // http_requests_total for two instances: +10 every 15 s on a, +1 on b, with a reset on b.
        let mut v = vec![];
        for i in 0..=40i64 {
            let t = i * 15_000;
            v.push(format!("http_requests_total{{instance=\"a\",job=\"api\",code=\"200\"}} {} {t}", i * 10));
            let b = if i < 20 { i } else { i - 20 };
            v.push(format!("http_requests_total{{instance=\"b\",job=\"api\",code=\"200\"}} {b} {t}"));
            v.push(format!("http_requests_total{{instance=\"b\",job=\"api\",code=\"500\"}} {} {t}", i * 2));
        }
        v
    }

    #[test]
    fn parses_typical_queries() {
        for q in [
            r#"sum by (job) (rate(http_requests_total{code=~"5.."}[5m]))"#,
            r#"histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket[5m])) by (le, handler))"#,
            "max_over_time(cpu_usage_user{hostname=\"host_1\"}[1h] offset 1d) * 100 > bool 50",
            "-a + b * c ^ 2 ^ 3 / 4 % 5",
            "topk(3, sum without (cpu) (node_cpu_seconds_total))",
            "a and on(x) b or c unless ignoring(y) d",
        ] {
            parse(q).unwrap_or_else(|e| panic!("{q}: {e}"));
        }
        for bad in ["sum(", "rate(x)", "{}", r#"{a=""}"#, "x[5m] [5m]", "foo{a=1}", "a group_left b", "x[5m:1m]"] {
            assert!(parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn precedence() {
        let (_d, db) = db_with(&[]);
        let s = |q| match instant(&db, q, 0).unwrap() {
            Value::Scalar(x) => x,
            v => panic!("{v:?}"),
        };
        assert_eq!(s("1 + 2 * 3"), 7.0);
        assert_eq!(s("2 ^ 3 ^ 2"), 512.0);
        assert_eq!(s("-2 ^ 2"), -4.0);
        assert_eq!(s("(1 + 2) * 3 - 4 / 2"), 7.0);
        assert_eq!(s("10 % 4 > bool 1"), 1.0);
        assert_eq!(s("time()"), 0.0);
    }

    #[test]
    fn rate_matches_prometheus_extrapolation() {
        let lines = counter_lines();
        let (_d, db) = db_with(&lines.iter().map(|s| s.as_str()).collect::<Vec<_>>());
        // At t = 600 s, the 5m window (300 s, 600 s] holds 20 samples of a counter rising
        // 10 every 15 s: the rate is exactly 10/15 per second.
        let v = vector(instant(&db, r#"rate(http_requests_total{instance="a"}[5m])"#, 600_000).unwrap());
        assert_eq!(v.len(), 1);
        assert!((v[0].1 - 10.0 / 15.0).abs() < 1e-9, "{v:?}");
        assert!(!v[0].0.contains("http_requests_total"));
        // Counter reset on b/200 at sample 20 (t = 300 s): the increase still counts 1 per 15 s.
        let v = vector(instant(&db, r#"increase(http_requests_total{instance="b",code="200"}[5m])"#, 600_000).unwrap());
        assert!((v[0].1 - 20.0).abs() < 1e-9, "{v:?}");
        let v = vector(instant(&db, "sum by (code) (rate(http_requests_total[5m]))", 600_000).unwrap());
        assert_eq!(v.len(), 2);
        assert!((v[0].1 - (10.0 + 1.0) / 15.0).abs() < 1e-9, "{v:?}");
        assert!((v[1].1 - 2.0 / 15.0).abs() < 1e-9);
        let v = vector(instant(&db, "irate(http_requests_total{code=\"500\"}[1m])", 600_000).unwrap());
        assert!((v[0].1 - 2.0 / 15.0).abs() < 1e-9);
    }

    #[test]
    fn histogram_quantile_interpolates() {
        let mut lines = vec![];
        // 100 observations: 50 below 0.1, 40 in (0.1, 0.5], 9 in (0.5, 1], 1 above 1.
        for (le, c) in [("0.1", 50), ("0.5", 90), ("1", 99), ("+Inf", 100)] {
            lines.push(format!("lat_bucket{{le=\"{le}\",h=\"x\"}} {c} 1000"));
        }
        let (_d, db) = db_with(&lines.iter().map(|s| s.as_str()).collect::<Vec<_>>());
        let q = |p: f64| vector(instant(&db, &format!("histogram_quantile({p}, lat_bucket)"), 1000).unwrap())[0].1;
        assert!((q(0.5) - 0.1).abs() < 1e-12);
        assert!((q(0.7) - (0.1 + 0.4 * 20.0 / 40.0)).abs() < 1e-12);
        assert!((q(0.95) - (0.5 + 0.5 * 5.0 / 9.0)).abs() < 1e-12);
        assert_eq!(q(0.999), 1.0); // in the +Inf bucket: the highest finite bound
        assert!(q(1.5).is_infinite());
        let v = vector(instant(&db, "histogram_quantile(0.5, lat_bucket)", 1000).unwrap());
        assert_eq!(v[0].0, r#"{h="x"}"#);
    }

    #[test]
    fn aggregations_binary_ops_and_matching() {
        let (_d, db) = db_with(&[
            r#"cpu{host="a",dc="x"} 10 1000"#,
            r#"cpu{host="b",dc="x"} 30 1000"#,
            r#"cpu{host="c",dc="y"} 50 1000"#,
            r#"cores{host="a",dc="x"} 2 1000"#,
            r#"cores{host="b",dc="x"} 4 1000"#,
        ]);
        let q = |s: &str| vector(instant(&db, s, 1000).unwrap());
        assert_eq!(q("avg by (dc) (cpu)"), vec![(r#"{dc="x"}"#.into(), 20.0), (r#"{dc="y"}"#.into(), 50.0)]);
        assert_eq!(q("count without (host) (cpu)"), vec![(r#"{dc="x"}"#.into(), 2.0), (r#"{dc="y"}"#.into(), 1.0)]);
        assert_eq!(q("topk(1, cpu)"), vec![(r#"cpu{dc="y",host="c"}"#.into(), 50.0)]);
        assert_eq!(q("cpu / cores"), vec![(r#"{dc="x",host="a"}"#.into(), 5.0), (r#"{dc="x",host="b"}"#.into(), 7.5)]);
        assert_eq!(q("cpu > 20"), vec![(r#"cpu{dc="x",host="b"}"#.into(), 30.0), (r#"cpu{dc="y",host="c"}"#.into(), 50.0)]);
        assert_eq!(q("cpu unless cores"), vec![(r#"cpu{dc="y",host="c"}"#.into(), 50.0)]);
        assert_eq!(q("sum(cpu) by (dc) / on(dc) sum(cores) by (dc)"), vec![(r#"{dc="x"}"#.into(), 40.0 / 6.0)]);
        assert_eq!(q(r#"quantile(0.5, cpu)"#), vec![("{}".into(), 30.0)]);
        // The lookback window: a sample older than 5 minutes is not returned.
        assert!(vector(instant(&db, "cpu", 1000 + LOOKBACK_MS + 1).unwrap()).is_empty());
    }

    #[test]
    fn range_query_steps() {
        let lines = counter_lines();
        let (_d, db) = db_with(&lines.iter().map(|s| s.as_str()).collect::<Vec<_>>());
        let Value::Matrix(m) = range(&db, r#"sum(rate(http_requests_total{instance="a"}[1m]))"#, 60_000, 600_000, 60_000).unwrap() else { panic!() };
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].1.len(), 10);
        assert!(m[0].1.iter().all(|x| (x.1 - 10.0 / 15.0).abs() < 1e-9), "{m:?}");
        assert!(range(&db, "x[5m]", 0, 10, 1).is_err());
        assert!(range(&db, "x", 0, 1_000_000_000, 1).is_err());
    }
}

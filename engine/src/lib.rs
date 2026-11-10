//! Ripple: a time-series database with Gorilla-compressed chunks, a write-ahead log, blocks with
//! compaction, and a subset of PromQL.

// Series are passed around as (labels, samples) tuples; naming every such type adds little.
#![allow(clippy::type_complexity)]

pub mod bits;
pub mod chunk;
pub mod labels;
pub mod wal;
pub mod storage;
pub mod promql;
pub mod metrics;
pub mod codecs;
pub mod server;

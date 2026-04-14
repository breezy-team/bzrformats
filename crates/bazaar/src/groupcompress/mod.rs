//! Groupcompress format: delta wire encoding, block framing, and the
//! compressor / manager glue that backs the Python `GroupCompressVersionedFiles`.
//!
//! # Submodule tour
//!
//! - [`delta`] — low-level delta wire format: base128 integers, copy/insert
//!   instructions, and whole-delta apply. Structured [`delta::DeltaError`]
//!   lets callers discriminate truncated streams, out-of-range copies, and
//!   length mismatches without string matching.
//! - [`line_delta`] — line-oriented delta generator that mirrors the
//!   original Python `LinesDeltaIndex`. Produces the same on-wire format as
//!   the other delta producers but operates over line arrays rather than
//!   byte streams.
//! - [`rabin_delta`] — rolling-hash delta generator (Rabin fingerprinting)
//!   for long byte streams.
//! - [`block`] — groupcompress block framing: item type byte, base128
//!   length, fulltext vs delta payload.
//! - [`compressor`] — `TraditionalGroupCompressor` and
//!   `RabinGroupCompressor` — the two high-level "add bytes, get back a
//!   key" entry points.
//! - [`manager`] — block rebuild / well-utilised / trim policy helpers
//!   used by `_LazyGroupContentManager` on the Python side.
//! - [`wire`] — `groupcompress-block` network record framing (header
//!   lines plus wire prefix construction).
//! - [`sort`] — `sort_gc_optimal`, the topological groupcompress-ordering
//!   routine used when streaming records to a target repository.

pub mod block;
pub mod compressor;
pub mod delta;
pub mod line_delta;
pub mod manager;
pub mod rabin_delta;
pub mod sort;
pub mod wire;
use sha1::{Digest as _, Sha1};

lazy_static::lazy_static! {
    pub static ref NULL_SHA1: Vec<u8> = format!("{:x}", Sha1::new().finalize()).as_bytes().to_vec();
}

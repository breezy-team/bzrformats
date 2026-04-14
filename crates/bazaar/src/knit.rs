//! Knit format parsing and serialization.
//!
//! Port of the pure-logic pieces of `bzrformats/knit.py`: fulltext and
//! line-delta parse/serialize for the annotated and plain variants, plus
//! the `get_line_delta_blocks` matching-block extractor. Content objects,
//! record I/O, and VersionedFile plumbing stay in Python.
//!
//! # Pure-Rust entry points
//!
//! For downstream Rust callers that want to work with knit data without
//! going through the Python bindings, the relevant pieces are:
//!
//! ## Fulltext / line-delta layer
//!
//! - [`parse_fulltext`] / [`lower_fulltext`] — round-trip the annotated
//!   fulltext wire format.
//! - [`parse_line_delta_annotated`] / [`lower_line_delta_annotated`] —
//!   annotated line-delta round-trip.
//! - [`parse_line_delta_plain`] / [`lower_line_delta_plain`] / [`parse_line_delta_raw`]
//!   / [`lower_line_delta_raw`] — plain (unannotated) variants.
//! - [`get_line_delta_blocks`] — extract matching `(parent_offset, target_offset, length)`
//!   blocks from a delta.
//!
//! ## On-disk record layer
//!
//! - [`decode_record_gz`] — gunzip a `data` payload into a decompressed
//!   body. Usually followed by one of the borrowing parsers below.
//! - [`readlines`] — split a decompressed body into borrowed lines (the
//!   knit wire format keeps `\n` terminators on every line; zero-copy).
//! - [`parse_header_line`] / [`RecordHeaderRef`] — parse a `version <id>
//!   <count> <digest>` line into borrowed fields.
//! - [`parse_record_body_unchecked`] — header + body lines as borrowed
//!   slices of a caller-owned decompressed buffer. Checks the line count
//!   and `end` marker.
//! - [`parse_record_unchecked`] / [`RecordHeader`] — owning wrapper
//!   around the above for call-sites that need a detached result.
//! - [`parse_record_header_only`] — lenient header-only variant that does
//!   not validate the body (used by the raw-read path).
//! - [`record_to_data`] — the inverse: frame a body into a compressed
//!   knit record.
//!
//! ## Network record layer
//!
//! - [`parse_network_record_header`] / [`NetworkRecordHeader`] — parse
//!   the variable-length header of a `knit-*-gz` network record.
//! - [`build_network_record`] (with the [`NO_PARENTS`] sentinel for the
//!   `None`-parents case) — inverse of the above.
//! - [`KnitDeltaClosureRecord`] / [`build_knit_delta_closure_wire`] —
//!   serialise a `knit-delta-closure` batch of records for over-the-wire
//!   streaming.
//!
//! ## Supporting helpers
//!
//! - [`split_keys_by_prefix`] — order-preserving groupby over a list of
//!   knit keys. Used by the Python `_split_by_prefix` on the checkout
//!   batching path.
//!
//! All of the above share a single [`KnitError`] enum; functions return
//! `Result<_, KnitError>` so callers only need one error match-arm set.

/// Unified error type for every fallible operation in this module.
///
/// The enum covers four loosely-related families — fulltext / line-delta
/// parsing, on-disk record parsing, network record header parsing, and
/// record serialization. They share a single type so callers only need
/// one `match` arm set; each variant's docstring names the function
/// family it belongs to.
///
/// `KnitError` is `Clone + Eq` so it can participate in test assertions
/// directly (`assert_eq!(err, KnitError::TruncatedDelta)`). The one
/// underlying `std::io::Error` path (gzip decompression) is normalised
/// into a `String` for the same reason: corrupt compressed bodies
/// reliably produce textual diagnostics and carrying a live `io::Error`
/// across the enum would poison `Clone + Eq`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KnitError {
    // --- fulltext / line-delta layer ---
    /// A fulltext or delta line had no space separating origin from text.
    MissingOrigin(Vec<u8>),
    /// A delta header `start,end,count` was malformed.
    BadDeltaHeader(Vec<u8>),
    /// A delta header said N lines but the iterator ran out earlier.
    TruncatedDelta,

    // --- on-disk record layer ---
    /// Gzip decompression failed. The inner string is the `io::Error`
    /// message from flate2 / the underlying reader.
    Gzip(String),
    /// Record body was empty — no header line at all.
    EmptyRecord,
    /// `version <id> <count> <digest>` header had the wrong number of
    /// space-separated fields.
    HeaderFields(Vec<u8>),
    /// `count` field of a header line wasn't a valid integer.
    HeaderCount(Vec<u8>),
    /// Line count declared by the header didn't match the body.
    LineCount { declared: usize, actual: usize },
    /// The `end <version_id>` trailer didn't match the expected value.
    BadEndMarker { expected: Vec<u8>, actual: Vec<u8> },
    /// [`record_to_data`] was given a non-empty body whose last line did
    /// not end in `\n`.
    MissingTrailingNewline,

    // --- network record layer ---
    /// `parse_network_record_header`: the key segment had no `\n`
    /// terminator.
    NetworkMissingKeyTerminator,
    /// `parse_network_record_header`: the parent-list segment had no
    /// `\n` terminator.
    NetworkMissingParentsTerminator,
    /// `parse_network_record_header`: the noeol flag byte was missing
    /// (input ended before the record body).
    NetworkMissingNoEolByte,
}

impl std::fmt::Display for KnitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KnitError::MissingOrigin(l) => {
                write!(f, "annotated knit line missing origin: {:?}", l)
            }
            KnitError::BadDeltaHeader(h) => write!(f, "bad delta header: {:?}", h),
            KnitError::TruncatedDelta => write!(f, "delta truncated: too few lines"),
            KnitError::Gzip(msg) => write!(f, "corrupt compressed record: {}", msg),
            KnitError::EmptyRecord => write!(f, "empty knit record"),
            KnitError::HeaderFields(h) => {
                write!(f, "unexpected number of elements in record header: {:?}", h)
            }
            KnitError::HeaderCount(h) => {
                write!(f, "record header line count is not an integer: {:?}", h)
            }
            KnitError::LineCount { declared, actual } => {
                write!(
                    f,
                    "incorrect number of lines {} != {} in record",
                    actual, declared
                )
            }
            KnitError::BadEndMarker { expected, actual } => write!(
                f,
                "unexpected version end line {:?}, wanted {:?}",
                actual, expected
            ),
            KnitError::MissingTrailingNewline => {
                write!(f, "corrupt lines value: last line missing trailing newline")
            }
            KnitError::NetworkMissingKeyTerminator => {
                write!(f, "knit network record key missing newline terminator")
            }
            KnitError::NetworkMissingParentsTerminator => {
                write!(f, "knit network record parents missing newline terminator")
            }
            KnitError::NetworkMissingNoEolByte => {
                write!(f, "knit network record missing noeol byte")
            }
        }
    }
}

impl std::error::Error for KnitError {}

/// One hunk of an annotated line delta: `(start, end, count, lines)` where
/// `lines` is a sequence of `(origin, text)` pairs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaHunk<T> {
    pub start: usize,
    pub end: usize,
    pub count: usize,
    pub lines: Vec<T>,
}

/// One `(origin, text)` pair from an annotated fulltext or delta body.
pub type AnnotatedLine = (Vec<u8>, Vec<u8>);

/// Parse an annotated fulltext — a sequence of `origin text\n` byte lines —
/// into a list of `(origin, text)` pairs. The text slice keeps its trailing
/// newline just as the Python implementation does.
pub fn parse_fulltext(lines: &[&[u8]]) -> Result<Vec<AnnotatedLine>, KnitError> {
    lines.iter().map(|l| split_annotated(l)).collect()
}

/// Invert [`parse_fulltext`] — emit one `origin text` byte line per entry.
pub fn lower_fulltext(content: &[(Vec<u8>, Vec<u8>)]) -> Vec<Vec<u8>> {
    content
        .iter()
        .map(|(origin, text)| {
            let mut out = Vec::with_capacity(origin.len() + 1 + text.len());
            out.extend_from_slice(origin);
            out.push(b' ');
            out.extend_from_slice(text);
            out
        })
        .collect()
}

/// Parse an annotated line-delta body: repeated `start,end,count\n` headers
/// followed by `count` `origin text\n` lines each.
pub fn parse_line_delta_annotated(
    lines: &[&[u8]],
) -> Result<Vec<DeltaHunk<AnnotatedLine>>, KnitError> {
    parse_line_delta_inner(lines, true).map(|hunks| {
        hunks
            .into_iter()
            .map(|h| DeltaHunk {
                start: h.start,
                end: h.end,
                count: h.count,
                lines: h
                    .lines
                    .into_iter()
                    .map(|line| match line {
                        ParsedLine::Annotated(o, t) => (o, t),
                        ParsedLine::Plain(_) => unreachable!(),
                    })
                    .collect(),
            })
            .collect()
    })
}

/// Parse a plain line-delta body: same headers, but each data line has its
/// origin stripped in the output.
pub fn parse_line_delta_plain(lines: &[&[u8]]) -> Result<Vec<DeltaHunk<Vec<u8>>>, KnitError> {
    parse_line_delta_inner(lines, false).map(|hunks| {
        hunks
            .into_iter()
            .map(|h| DeltaHunk {
                start: h.start,
                end: h.end,
                count: h.count,
                lines: h
                    .lines
                    .into_iter()
                    .map(|line| match line {
                        ParsedLine::Plain(t) => t,
                        ParsedLine::Annotated(_, t) => t,
                    })
                    .collect(),
            })
            .collect()
    })
}

/// Serialize an annotated delta back to the on-disk byte form.
pub fn lower_line_delta_annotated(delta: &[DeltaHunk<AnnotatedLine>]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    for hunk in delta {
        out.push(format!("{},{},{}\n", hunk.start, hunk.end, hunk.count).into_bytes());
        for (origin, text) in &hunk.lines {
            let mut line = Vec::with_capacity(origin.len() + 1 + text.len());
            line.extend_from_slice(origin);
            line.push(b' ');
            line.extend_from_slice(text);
            out.push(line);
        }
    }
    out
}

/// Parse an unannotated (raw) line-delta body: `start,end,count\n` headers
/// followed by `count` raw text lines each. Mirrors
/// `KnitPlainFactory.parse_line_delta`.
pub fn parse_line_delta_raw(lines: &[&[u8]]) -> Result<Vec<DeltaHunk<Vec<u8>>>, KnitError> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let (start, end, count) = parse_delta_header(lines[i])?;
        i += 1;
        if i + count > lines.len() {
            return Err(KnitError::TruncatedDelta);
        }
        let hunk_lines: Vec<Vec<u8>> = lines[i..i + count].iter().map(|l| l.to_vec()).collect();
        i += count;
        out.push(DeltaHunk {
            start,
            end,
            count,
            lines: hunk_lines,
        });
    }
    Ok(out)
}

/// Serialize an unannotated line-delta back to bytes. Mirrors
/// `KnitPlainFactory.lower_line_delta`.
pub fn lower_line_delta_raw(delta: &[DeltaHunk<Vec<u8>>]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    for hunk in delta {
        out.push(format!("{},{},{}\n", hunk.start, hunk.end, hunk.count).into_bytes());
        for line in &hunk.lines {
            out.push(line.clone());
        }
    }
    out
}

/// Yield matching blocks from a knit delta walk, preserving the historical
/// last-line EOL-sensitivity quirk described in `get_line_delta_blocks`.
///
/// The `delta` hunks are `(s_begin, s_end, t_len)` tuples (the body lines
/// are irrelevant to block extraction).
pub fn get_line_delta_blocks(
    delta: &[(usize, usize, usize)],
    source: &[&[u8]],
    target: &[&[u8]],
) -> Vec<(usize, usize, usize)> {
    let target_len = target.len();
    let mut out = Vec::new();
    let mut s_pos = 0usize;
    let mut t_pos = 0usize;
    for &(s_begin, s_end, t_len) in delta {
        let true_n = s_begin - s_pos;
        let mut n = true_n;
        if n > 0 {
            // knit deltas don't reliably flag whether the last line differs
            // due to eol handling, so skip the final pair if it's a mismatch.
            if source[s_pos + n - 1] != target[t_pos + n - 1] {
                n -= 1;
            }
            if n > 0 {
                out.push((s_pos, t_pos, n));
            }
        }
        t_pos += t_len + true_n;
        s_pos = s_end;
    }
    let mut n = target_len - t_pos;
    if n > 0 {
        if source[s_pos + n - 1] != target[t_pos + n - 1] {
            n -= 1;
        }
        if n > 0 {
            out.push((s_pos, t_pos, n));
        }
    }
    // Sentinel terminator, mirroring SequenceMatcher.get_matching_blocks().
    out.push((s_pos + (target_len - t_pos), target_len, 0));
    out
}

// ---- internals ----

enum ParsedLine {
    Annotated(Vec<u8>, Vec<u8>),
    Plain(Vec<u8>),
}

fn split_annotated(line: &[u8]) -> Result<(Vec<u8>, Vec<u8>), KnitError> {
    let sp = line
        .iter()
        .position(|&b| b == b' ')
        .ok_or_else(|| KnitError::MissingOrigin(line.to_vec()))?;
    Ok((line[..sp].to_vec(), line[sp + 1..].to_vec()))
}

fn parse_line_delta_inner(
    lines: &[&[u8]],
    annotated: bool,
) -> Result<Vec<DeltaHunk<ParsedLine>>, KnitError> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let header = lines[i];
        i += 1;
        let (start, end, count) = parse_delta_header(header)?;
        if i + count > lines.len() {
            return Err(KnitError::TruncatedDelta);
        }
        let mut hunk_lines = Vec::with_capacity(count);
        for raw in &lines[i..i + count] {
            let (origin, text) = split_annotated(raw)?;
            hunk_lines.push(if annotated {
                ParsedLine::Annotated(origin, text)
            } else {
                ParsedLine::Plain(text)
            });
        }
        i += count;
        out.push(DeltaHunk {
            start,
            end,
            count,
            lines: hunk_lines,
        });
    }
    Ok(out)
}

fn parse_delta_header(line: &[u8]) -> Result<(usize, usize, usize), KnitError> {
    let trimmed = line.strip_suffix(b"\n").unwrap_or(line);
    let mut parts = trimmed.split(|&b| b == b',');
    let mut next = || -> Result<usize, KnitError> {
        let part = parts
            .next()
            .ok_or_else(|| KnitError::BadDeltaHeader(line.to_vec()))?;
        std::str::from_utf8(part)
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| KnitError::BadDeltaHeader(line.to_vec()))
    };
    let start = next()?;
    let end = next()?;
    let count = next()?;
    if parts.next().is_some() {
        return Err(KnitError::BadDeltaHeader(line.to_vec()));
    }
    Ok((start, end, count))
}

/// Build details extracted from a knit network record header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkRecordHeader<'a> {
    /// Tuple-segment key (`key.split(b"\x00")` in the Python original).
    pub key: Vec<&'a [u8]>,
    /// `None` for the literal `b"None:"`, else the parsed parent key list.
    pub parents: Option<Vec<Vec<&'a [u8]>>>,
    /// `"fulltext"` or `"line-delta"` (chosen by the storage kind on the
    /// caller side; this struct just carries the noeol flag).
    pub noeol: bool,
    /// Slice of the original input that contains the raw record body.
    pub raw_record: &'a [u8],
}

/// Parse the variable-length header of a `knit-*-gz` network record.
///
/// `bytes` is the full record and `start` is the offset just past the
/// storage-kind line (the same `line_end` the Python caller computes via
/// `network_bytes_to_kind_and_offset`).
pub fn parse_network_record_header(
    bytes: &[u8],
    start: usize,
) -> Result<NetworkRecordHeader<'_>, KnitError> {
    let key_end = bytes[start..]
        .iter()
        .position(|&b| b == b'\n')
        .map(|i| start + i)
        .ok_or(KnitError::NetworkMissingKeyTerminator)?;
    let key: Vec<&[u8]> = bytes[start..key_end].split(|&b| b == b'\x00').collect();

    let parents_start = key_end + 1;
    let parents_end = bytes[parents_start..]
        .iter()
        .position(|&b| b == b'\n')
        .map(|i| parents_start + i)
        .ok_or(KnitError::NetworkMissingParentsTerminator)?;
    let parents_line = &bytes[parents_start..parents_end];
    let parents = if parents_line == b"None:" {
        None
    } else {
        Some(
            parents_line
                .split(|&b| b == b'\t')
                .filter(|seg| !seg.is_empty())
                .map(|seg| seg.split(|&b| b == b'\x00').collect::<Vec<&[u8]>>())
                .collect(),
        )
    };

    let noeol_pos = parents_end + 1;
    if noeol_pos >= bytes.len() {
        return Err(KnitError::NetworkMissingNoEolByte);
    }
    let noeol = bytes[noeol_pos] == b'N';
    let raw_record = &bytes[noeol_pos + 1..];

    Ok(NetworkRecordHeader {
        key,
        parents,
        noeol,
        raw_record,
    })
}

/// Serialize a knit network record, inverse of [`parse_network_record_header`].
///
/// Mirrors `KnitContentFactory._create_network_bytes`: writes the storage
/// kind line, the `\x00`-joined key, the `\t`-separated parent list (or
/// `None:` when `parents` is `None`), the noeol flag byte, and the raw
/// record body.
/// Typed sentinel for passing `None` as the parents argument of
/// [`build_network_record`] without having to spell out a turbofish. The
/// types `&[u8]` / `&[&[u8]]` here are inert — the option is always `None`
/// — but they're concrete enough to pin the generic parameters.
pub const NO_PARENTS: Option<&[&[&[u8]]]> = None;

/// Write a `\x00`-joined knit key into `out`.
fn write_joined_key<Seg: AsRef<[u8]>>(out: &mut Vec<u8>, key: &[Seg]) {
    for (i, segment) in key.iter().enumerate() {
        if i > 0 {
            out.push(b'\x00');
        }
        out.extend_from_slice(segment.as_ref());
    }
}

/// Serialize a knit network record, inverse of [`parse_network_record_header`].
///
/// Mirrors `KnitContentFactory._create_network_bytes`: writes the storage
/// kind line, the `\x00`-joined key, the `\t`-separated parent list (or
/// `None:` when `parents` is `None`), the noeol flag byte, and the raw
/// record body.
///
/// The generic bounds let callers pass slices of `Vec<u8>`, `&[u8]`, or any
/// other byte-segment type — only `parents` still needs a slice-of-slices
/// shape because the parent list is itself a list of keys.
pub fn build_network_record<Seg, PK>(
    storage_kind: &[u8],
    key: &[Seg],
    parents: Option<&[PK]>,
    noeol: bool,
    raw_record: &[u8],
) -> Vec<u8>
where
    Seg: AsRef<[u8]>,
    PK: AsRef<[Seg]>,
{
    let mut out = Vec::with_capacity(storage_kind.len() + raw_record.len() + 32);
    out.extend_from_slice(storage_kind);
    out.push(b'\n');
    write_joined_key(&mut out, key);
    out.push(b'\n');
    match parents {
        None => out.extend_from_slice(b"None:"),
        Some(list) => {
            for (i, parent) in list.iter().enumerate() {
                if i > 0 {
                    out.push(b'\t');
                }
                write_joined_key(&mut out, parent.as_ref());
            }
        }
    }
    out.push(b'\n');
    out.push(if noeol { b'N' } else { b' ' });
    out.extend_from_slice(raw_record);
    out
}

/// Group keys by their first segment, preserving first-seen order per group
/// and the global order in which new prefixes appeared.
///
/// Mirrors `KnitVersionedFiles._split_by_prefix`: single-segment keys land
/// under the empty-bytes prefix, everything else under `key[0]`.
///
/// Returns `(buckets, prefix_order)` where each bucket holds a borrowed
/// slice of the original keys and the prefix byte slice itself is also a
/// borrow (either an empty slice or a reference to the first segment of
/// the first key that landed in the bucket). Preserves the input order
/// both globally (in `prefix_order`) and within each bucket.
#[allow(clippy::type_complexity)]
pub fn split_keys_by_prefix<'a, K, Seg>(
    keys: &'a [K],
) -> (Vec<(&'a [u8], Vec<&'a K>)>, Vec<&'a [u8]>)
where
    K: AsRef<[Seg]> + 'a,
    Seg: AsRef<[u8]> + 'a,
{
    use std::collections::HashMap;
    const EMPTY: &[u8] = b"";
    let mut buckets: Vec<(&'a [u8], Vec<&'a K>)> = Vec::new();
    let mut index: HashMap<&'a [u8], usize> = HashMap::new();
    let mut prefix_order: Vec<&'a [u8]> = Vec::new();
    for key in keys {
        let segments: &'a [Seg] = key.as_ref();
        let prefix: &'a [u8] = if segments.len() == 1 {
            EMPTY
        } else {
            segments[0].as_ref()
        };
        match index.get(prefix) {
            Some(&i) => buckets[i].1.push(key),
            None => {
                index.insert(prefix, buckets.len());
                prefix_order.push(prefix);
                buckets.push((prefix, vec![key]));
            }
        }
    }
    (buckets, prefix_order)
}

/// One entry of the `_raw_record_map` table that
/// [`build_knit_delta_closure_wire`] consumes.
///
/// Generic over `Seg: AsRef<[u8]>` so callers can populate the struct with
/// either owned `Vec<u8>` segments or borrowed `&[u8]` slices — whichever
/// shape matches where the data lives. The inner containers are plain
/// slices; wrap them in `&Vec<Seg>` or `&[Seg]` at the call site.
///
/// `parents` is `None` for the literal `None:` parents line (the Python side
/// distinguishes this via `global_map.get(key)` returning `None`).
pub struct KnitDeltaClosureRecord<'a, Seg: AsRef<[u8]>> {
    pub key: &'a [Seg],
    pub parents: Option<&'a [&'a [Seg]]>,
    pub method: &'a [u8],
    pub noeol: bool,
    pub next: Option<&'a [Seg]>,
    pub record_bytes: &'a [u8],
}

/// Serialize a `knit-delta-closure` wire record.
///
/// Mirrors `_ContentMapGenerator._wire_bytes` byte-for-byte. The Python parser
/// is `_NetworkContentMapGenerator`; the on-wire format is: storage kind line,
/// `annotated` flag line, `\t`-joined emit keys line, then a run of records
/// each carrying `key / parents / method / noeol flag / next / byte count /
/// record body`.
///
/// `EK` is any key container for the emit-keys list (e.g. `Vec<Seg>` or
/// `&[Seg]`), and `Seg` is the byte-segment type shared by keys, parent
/// keys, and the `next` link inside each record.
pub fn build_knit_delta_closure_wire<EK, Seg>(
    annotated: bool,
    emit_keys: &[EK],
    records: &[KnitDeltaClosureRecord<'_, Seg>],
) -> Vec<u8>
where
    EK: AsRef<[Seg]>,
    Seg: AsRef<[u8]>,
{
    let body_estimate: usize = records.iter().map(|r| r.record_bytes.len() + 64).sum();
    let mut out = Vec::with_capacity(64 + body_estimate);
    out.extend_from_slice(b"knit-delta-closure\n");
    if annotated {
        out.extend_from_slice(b"annotated");
    }
    out.push(b'\n');
    for (i, key) in emit_keys.iter().enumerate() {
        if i > 0 {
            out.push(b'\t');
        }
        write_joined_key(&mut out, key.as_ref());
    }
    out.push(b'\n');
    for rec in records {
        write_joined_key(&mut out, rec.key);
        out.push(b'\n');
        match rec.parents {
            None => out.extend_from_slice(b"None:"),
            Some(list) => {
                for (i, parent) in list.iter().enumerate() {
                    if i > 0 {
                        out.push(b'\t');
                    }
                    write_joined_key(&mut out, parent);
                }
            }
        }
        out.push(b'\n');
        out.extend_from_slice(rec.method);
        out.push(b'\n');
        out.push(if rec.noeol { b'T' } else { b'F' });
        out.push(b'\n');
        if let Some(next) = rec.next {
            write_joined_key(&mut out, next);
        }
        out.push(b'\n');
        out.extend_from_slice(rec.record_bytes.len().to_string().as_bytes());
        out.push(b'\n');
        out.extend_from_slice(rec.record_bytes);
    }
    out
}

/// Fields of a parsed knit record header: `(method, version_id, count, digest)`.
///
/// Mirrors the 4-tuple returned by `_KnitData._split_header`, but typed.
/// Prefer [`RecordHeaderRef`] for borrowing parsers that can tie their output
/// to the lifetime of the source buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordHeader {
    pub method: Vec<u8>,
    pub version_id: Vec<u8>,
    pub count: usize,
    pub digest: Vec<u8>,
}

/// Borrowing counterpart to [`RecordHeader`]: the four byte-slice fields all
/// alias a single source buffer (typically the gunzipped record body), so no
/// allocations are needed when the caller already owns that buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordHeaderRef<'a> {
    pub method: &'a [u8],
    pub version_id: &'a [u8],
    pub count: usize,
    pub digest: &'a [u8],
}

impl RecordHeaderRef<'_> {
    pub fn to_owned(&self) -> RecordHeader {
        RecordHeader {
            method: self.method.to_vec(),
            version_id: self.version_id.to_vec(),
            count: self.count,
            digest: self.digest.to_vec(),
        }
    }
}

/// Parse a knit header line (`version <id> <count> <digest>`), either with
/// or without the trailing newline. Borrows the input: all four fields in
/// the returned `RecordHeaderRef` are slices of `line`.
///
/// The whole line (including any newline the caller passed in) is threaded
/// into the [`KnitError::HeaderFields`] / [`KnitError::HeaderCount`] variants
/// so diagnostics match the original input.
pub fn parse_header_line(line: &[u8]) -> Result<RecordHeaderRef<'_>, KnitError> {
    let trimmed = line.strip_suffix(b"\n").unwrap_or(line);
    let fields: Vec<&[u8]> = trimmed.split(|&b| b == b' ').collect();
    if fields.len() != 4 {
        return Err(KnitError::HeaderFields(line.to_vec()));
    }
    let count: usize = std::str::from_utf8(fields[2])
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| KnitError::HeaderCount(line.to_vec()))?;
    Ok(RecordHeaderRef {
        method: fields[0],
        version_id: fields[1],
        count,
        digest: fields[3],
    })
}

/// Split a gunzipped record body into `\n`-terminated lines, matching
/// `BytesIO(data).readlines()` semantics (trailing-newline-inclusive, and a
/// final unterminated tail is kept as its own line).
fn split_readlines(data: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut start = 0;
    for (i, &b) in data.iter().enumerate() {
        if b == b'\n' {
            out.push(data[start..=i].to_vec());
            start = i + 1;
        }
    }
    if start < data.len() {
        out.push(data[start..].to_vec());
    }
    out
}

/// Decompress and parse a raw knit record as produced by `_record_to_data`.
///
/// Returns the header fields plus the body lines (header and end-marker
/// removed). Mirrors `_KnitData._parse_record_unchecked`: gzip decode, pull
/// off the `version <id> <count> <digest>` header, verify the line count,
/// verify the trailing `end <id>\n` marker.
/// Gunzip a knit record, returning its decompressed body. Thin convenience
/// so callers can own the buffer and then run the borrowing parsers below
/// without paying for a second allocation.
pub fn decode_record_gz(data: &[u8]) -> Result<Vec<u8>, KnitError> {
    use flate2::read::GzDecoder;
    use std::io::Read;

    let mut decoder = GzDecoder::new(data);
    let mut decompressed = Vec::new();
    decoder
        .read_to_end(&mut decompressed)
        .map_err(|e| KnitError::Gzip(e.to_string()))?;
    Ok(decompressed)
}

/// Split a gunzipped knit record body into borrowed lines (trailing-newline
/// included, final unterminated tail kept). Same semantics as the Python
/// `BytesIO(data).readlines()` call this replaces, but without allocating
/// a `Vec<u8>` per line.
pub fn readlines(data: &[u8]) -> Vec<&[u8]> {
    ReadLines::new(data).collect()
}

/// Streaming variant of [`readlines`]: yields one borrowed line at a time
/// so callers working with very large decompressed bodies don't have to
/// allocate a `Vec<&[u8]>` to index into.
#[derive(Debug, Clone)]
pub struct ReadLines<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> ReadLines<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }
}

impl<'a> Iterator for ReadLines<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.data.len() {
            return None;
        }
        let start = self.pos;
        match self.data[start..].iter().position(|&b| b == b'\n') {
            Some(off) => {
                let end = start + off + 1;
                self.pos = end;
                Some(&self.data[start..end])
            }
            None => {
                self.pos = self.data.len();
                Some(&self.data[start..])
            }
        }
    }
}

/// Parse an already-decompressed knit record body into its header and body
/// lines, borrowing from `decompressed`. Inverse of [`record_to_data`]
/// composed with [`decode_record_gz`]. Validates line count and the `end`
/// marker like [`parse_record_unchecked`], and returns slices into
/// `decompressed` so no per-line allocation is needed.
pub fn parse_record_body_unchecked(
    decompressed: &[u8],
) -> Result<(RecordHeaderRef<'_>, Vec<&[u8]>), KnitError> {
    let mut lines = readlines(decompressed);
    if lines.is_empty() {
        return Err(KnitError::EmptyRecord);
    }
    let header_line = lines.remove(0);
    let header = parse_header_line(header_line)?;

    if lines.is_empty() {
        return Err(KnitError::LineCount {
            declared: header.count,
            actual: 0,
        });
    }
    let last_line = lines.pop().unwrap();
    if lines.len() != header.count {
        return Err(KnitError::LineCount {
            declared: header.count,
            actual: lines.len(),
        });
    }
    let mut expected_end = b"end ".to_vec();
    expected_end.extend_from_slice(header.version_id);
    expected_end.push(b'\n');
    if last_line != expected_end.as_slice() {
        return Err(KnitError::BadEndMarker {
            expected: expected_end,
            actual: last_line.to_vec(),
        });
    }
    Ok((header, lines))
}

/// Owning convenience wrapper around [`decode_record_gz`] +
/// [`parse_record_body_unchecked`]. Retained for call-sites (notably the
/// pyo3 binding) that need an owned result.
pub fn parse_record_unchecked(data: &[u8]) -> Result<(RecordHeader, Vec<Vec<u8>>), KnitError> {
    let decompressed = decode_record_gz(data)?;
    let mut lines = split_readlines(&decompressed);
    if lines.is_empty() {
        return Err(KnitError::EmptyRecord);
    }
    let header_line = lines.remove(0);
    let header = parse_header_line(&header_line)?.to_owned();

    if lines.is_empty() {
        return Err(KnitError::LineCount {
            declared: header.count,
            actual: 0,
        });
    }
    let last_line = lines.pop().unwrap();
    if lines.len() != header.count {
        return Err(KnitError::LineCount {
            declared: header.count,
            actual: lines.len(),
        });
    }
    let mut expected_end = b"end ".to_vec();
    expected_end.extend_from_slice(&header.version_id);
    expected_end.push(b'\n');
    if last_line != expected_end {
        return Err(KnitError::BadEndMarker {
            expected: expected_end,
            actual: last_line,
        });
    }

    Ok((header, lines))
}

/// Gzip-decode just enough of a knit record to parse its header line.
///
/// Used by `_KnitData._parse_record_header`, which needs only the header
/// fields and intentionally does not validate line counts or the end marker
/// (see `test_too_many_lines` / `test_not_enough_lines`).
pub fn parse_record_header_only(data: &[u8]) -> Result<RecordHeader, KnitError> {
    use flate2::read::GzDecoder;
    use std::io::Read;

    let mut decoder = GzDecoder::new(data);
    let mut header_buf = Vec::with_capacity(64);
    let mut byte = [0u8; 1];
    loop {
        match decoder
            .read(&mut byte)
            .map_err(|e| KnitError::Gzip(e.to_string()))?
        {
            0 => break,
            _ => {
                header_buf.push(byte[0]);
                if byte[0] == b'\n' {
                    break;
                }
            }
        }
    }
    if header_buf.is_empty() {
        return Err(KnitError::EmptyRecord);
    }
    Ok(parse_header_line(&header_buf)?.to_owned())
}

/// Serialize a knit record for on-disk storage. Inverse of
/// [`parse_record_unchecked`]; mirrors `_KnitData._record_to_data`.
///
/// Builds the `version <id> <count> <digest>\n` header, the body payload,
/// and the trailing `end <id>\n` marker, then gzip-compresses via
/// [`crate::tuned_gzip::chunks_to_gzip`]. Returns
/// `(compressed_len, compressed_chunks)`.
///
/// * `version_id` – the trailing component of the knit key (`key[-1]`).
/// * `digest` – content sha1 as bytes.
/// * `line_count` – number of logical lines (`len(lines)` on the caller
///   side, not `payload.len()`, since payload may be `dense_lines`).
/// * `payload` – body chunks in order (`dense_lines or lines`).
/// * `has_trailing_newline` – whether `lines[-1]` ends in `\n`. Pass `true`
///   for empty inputs.
pub fn record_to_data<P>(
    version_id: &[u8],
    digest: &[u8],
    line_count: usize,
    payload: &[P],
    has_trailing_newline: bool,
) -> Result<(usize, Vec<Vec<u8>>), KnitError>
where
    P: AsRef<[u8]>,
{
    if !has_trailing_newline {
        return Err(KnitError::MissingTrailingNewline);
    }

    let mut header = Vec::with_capacity(version_id.len() + digest.len() + 16);
    header.extend_from_slice(b"version ");
    header.extend_from_slice(version_id);
    header.extend_from_slice(format!(" {} ", line_count).as_bytes());
    header.extend_from_slice(digest);
    header.push(b'\n');

    let mut end = Vec::with_capacity(version_id.len() + 5);
    end.extend_from_slice(b"end ");
    end.extend_from_slice(version_id);
    end.push(b'\n');

    let mut chunks: Vec<&[u8]> = Vec::with_capacity(payload.len() + 2);
    chunks.push(&header);
    for p in payload {
        chunks.push(p.as_ref());
    }
    chunks.push(&end);

    let compressed = crate::tuned_gzip::chunks_to_gzip(chunks.into_iter());
    let total: usize = compressed.iter().map(|c| c.len()).sum();
    Ok((total, compressed))
}

/// End-to-end conversion of an annotated-fulltext knit record to an
/// unannotated one.
///
/// Inverse-composed from the building blocks above: gunzip the record,
/// parse the header + annotated body, strip each `(origin, text)` pair
/// down to its `text`, and re-serialize as a plain fulltext knit record.
/// Returns a single `Vec<u8>` of gzip-compressed bytes — the caller
/// doesn't need to wrangle the chunk list form.
///
/// Mirrors `bzrformats.knit.FTAnnotatedToUnannotated.get_bytes`.
pub fn recompress_annotated_to_unannotated_fulltext(
    raw_record: &[u8],
) -> Result<Vec<u8>, KnitError> {
    let decompressed = decode_record_gz(raw_record)?;
    let (header, body_lines) = parse_record_body_unchecked(&decompressed)?;
    let annotated: Vec<AnnotatedLine> = parse_fulltext(&body_lines)?;
    let plain_lines: Vec<Vec<u8>> = annotated.into_iter().map(|(_, text)| text).collect();
    let has_trailing_newline = plain_lines.last().is_none_or(|l| l.ends_with(b"\n"));
    let (_, chunks) = record_to_data(
        header.version_id,
        header.digest,
        plain_lines.len(),
        &plain_lines,
        has_trailing_newline,
    )?;
    Ok(chunks.into_iter().flatten().collect())
}

/// End-to-end conversion of an annotated-delta knit record to an
/// unannotated one.
///
/// Gunzip the record, parse the header + delta body via the plain
/// (origin-stripping) parser, then re-serialize via `lower_line_delta_raw`.
/// Mirrors `bzrformats.knit.DeltaAnnotatedToUnannotated.get_bytes`, which
/// pairs `KnitAnnotateFactory.parse_line_delta(plain=True)` with
/// `KnitPlainFactory.lower_line_delta`.
pub fn recompress_annotated_to_unannotated_delta(raw_record: &[u8]) -> Result<Vec<u8>, KnitError> {
    let decompressed = decode_record_gz(raw_record)?;
    let (header, body_lines) = parse_record_body_unchecked(&decompressed)?;
    let plain_delta = parse_line_delta_plain(&body_lines)?;
    let plain_bytes = lower_line_delta_raw(&plain_delta);
    let has_trailing_newline = plain_bytes.last().is_none_or(|l| l.ends_with(b"\n"));
    let (_, chunks) = record_to_data(
        header.version_id,
        header.digest,
        plain_bytes.len(),
        &plain_bytes,
        has_trailing_newline,
    )?;
    Ok(chunks.into_iter().flatten().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refs<'a>(v: &'a [Vec<u8>]) -> Vec<&'a [u8]> {
        v.iter().map(|l| l.as_slice()).collect()
    }

    #[test]
    fn fulltext_round_trip() {
        let content: Vec<AnnotatedLine> = vec![
            (b"rev1".to_vec(), b"first line\n".to_vec()),
            (b"rev2".to_vec(), b"second line\n".to_vec()),
        ];
        let bytes = lower_fulltext(&content);
        assert_eq!(
            bytes,
            vec![
                b"rev1 first line\n".to_vec(),
                b"rev2 second line\n".to_vec(),
            ]
        );
        let parsed = parse_fulltext(&refs(&bytes)).unwrap();
        assert_eq!(parsed, content);
    }

    #[test]
    fn fulltext_rejects_missing_origin() {
        let lines = vec![b"no-space-here".as_slice()];
        assert!(matches!(
            parse_fulltext(&lines),
            Err(KnitError::MissingOrigin(_))
        ));
    }

    #[test]
    fn delta_annotated_round_trip() {
        let delta = vec![
            DeltaHunk {
                start: 0,
                end: 1,
                count: 2,
                lines: vec![
                    (b"r1".to_vec(), b"alpha\n".to_vec()),
                    (b"r1".to_vec(), b"beta\n".to_vec()),
                ],
            },
            DeltaHunk {
                start: 5,
                end: 5,
                count: 1,
                lines: vec![(b"r2".to_vec(), b"gamma\n".to_vec())],
            },
        ];
        let bytes = lower_line_delta_annotated(&delta);
        assert_eq!(
            bytes,
            vec![
                b"0,1,2\n".to_vec(),
                b"r1 alpha\n".to_vec(),
                b"r1 beta\n".to_vec(),
                b"5,5,1\n".to_vec(),
                b"r2 gamma\n".to_vec(),
            ]
        );
        let parsed = parse_line_delta_annotated(&refs(&bytes)).unwrap();
        assert_eq!(parsed, delta);
    }

    #[test]
    fn delta_raw_round_trip() {
        let delta = vec![
            DeltaHunk {
                start: 0,
                end: 0,
                count: 2,
                lines: vec![b"one\n".to_vec(), b"two\n".to_vec()],
            },
            DeltaHunk {
                start: 4,
                end: 5,
                count: 1,
                lines: vec![b"three\n".to_vec()],
            },
        ];
        let bytes = lower_line_delta_raw(&delta);
        assert_eq!(
            bytes,
            vec![
                b"0,0,2\n".to_vec(),
                b"one\n".to_vec(),
                b"two\n".to_vec(),
                b"4,5,1\n".to_vec(),
                b"three\n".to_vec(),
            ]
        );
        let parsed = parse_line_delta_raw(&refs(&bytes)).unwrap();
        assert_eq!(parsed, delta);
    }

    #[test]
    fn delta_plain_strips_origin() {
        let bytes: Vec<Vec<u8>> = vec![
            b"0,1,2\n".to_vec(),
            b"r1 alpha\n".to_vec(),
            b"r1 beta\n".to_vec(),
        ];
        let parsed = parse_line_delta_plain(&refs(&bytes)).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].start, 0);
        assert_eq!(parsed[0].end, 1);
        assert_eq!(parsed[0].count, 2);
        assert_eq!(
            parsed[0].lines,
            vec![b"alpha\n".to_vec(), b"beta\n".to_vec()]
        );
    }

    #[test]
    fn delta_rejects_bad_header() {
        let bytes = vec![b"not,a,number\n".as_slice()];
        assert!(matches!(
            parse_line_delta_annotated(&bytes),
            Err(KnitError::BadDeltaHeader(_))
        ));
    }

    #[test]
    fn delta_rejects_truncated() {
        let bytes = vec![b"0,0,3\n".as_slice(), b"r1 one\n".as_slice()];
        assert_eq!(
            parse_line_delta_annotated(&bytes),
            Err(KnitError::TruncatedDelta)
        );
    }

    fn lines_with_nl(text: &[u8]) -> Vec<Vec<u8>> {
        text.split(|&b| b == b'\n')
            .filter(|l| !l.is_empty())
            .map(|l| {
                let mut v = l.to_vec();
                v.push(b'\n');
                v
            })
            .collect()
    }

    #[test]
    fn line_delta_blocks_equal_inputs() {
        // Empty delta (no changes) on identical inputs yields just the
        // sentinel block covering the whole target.
        let source = lines_with_nl(b"a\nb\nc\n");
        let target = source.clone();
        let delta: Vec<(usize, usize, usize)> = vec![];
        let blocks = get_line_delta_blocks(&delta, &refs(&source), &refs(&target));
        assert_eq!(blocks, vec![(0, 0, 3), (3, 3, 0)]);
    }

    #[test]
    fn line_delta_blocks_noeol_shrinks_trailing_run() {
        // Mirrors test_knit.test_get_line_delta_blocks_noeol: when the last
        // "matching" line pair actually differs only in its trailing \n,
        // the block extractor must shave one line off the run. Here the
        // source has `c` without newline, the target has `c\n`, and the
        // delta flags the final line as modified. The naive extraction
        // would claim `(0, 0, 3)` as a match; the eol quirk drops it to
        // `(0, 0, 2)`.
        let source: Vec<Vec<u8>> = vec![b"a\n".to_vec(), b"b\n".to_vec(), b"c".to_vec()];
        let target: Vec<Vec<u8>> = vec![
            b"a\n".to_vec(),
            b"b\n".to_vec(),
            b"c\n".to_vec(),
            b"d\n".to_vec(),
        ];
        // A single hunk that replaces line 2 (the final 'c'-without-newline)
        // with 2 new lines.
        let delta = vec![(2usize, 3usize, 2usize)];
        let blocks = get_line_delta_blocks(&delta, &refs(&source), &refs(&target));
        // The leading run that looked like 2 matches is actually 1 because
        // the (c, c\n) pair fails the equality check.
        assert_eq!(blocks, vec![(0, 0, 2), (3, 4, 0)]);
    }

    #[test]
    fn line_delta_blocks_replace_middle_line() {
        // source: a b c, target: a X c — a single-line replacement.
        let source = lines_with_nl(b"a\nb\nc\n");
        let target = lines_with_nl(b"a\nX\nc\n");
        // delta replaces lines [1,2) with 1 new line.
        let delta = vec![(1usize, 2usize, 1usize)];
        let blocks = get_line_delta_blocks(&delta, &refs(&source), &refs(&target));
        // Expect [(0, 0, 1), (2, 2, 1), (3, 3, 0)] — matches
        // PatienceSequenceMatcher's shape for a pure replacement.
        assert_eq!(blocks, vec![(0, 0, 1), (2, 2, 1), (3, 3, 0)]);
    }

    #[test]
    fn network_header_no_parents_no_eol() {
        let bytes = b"knit-ft-gz\nfile-id\x00rev\nNone:\nNDATA";
        let header = parse_network_record_header(bytes, 11).unwrap();
        assert_eq!(header.key, vec![b"file-id".as_slice(), b"rev".as_slice()]);
        assert!(header.parents.is_none());
        assert!(header.noeol);
        assert_eq!(header.raw_record, b"DATA");
    }

    #[test]
    fn network_header_with_parents_and_eol() {
        let bytes = b"knit-delta-gz\nf\x00r\nf\x00p1\tf\x00p2\nYBODY";
        let header = parse_network_record_header(bytes, 14).unwrap();
        let parents = header.parents.unwrap();
        assert_eq!(
            parents,
            vec![
                vec![b"f".as_slice(), b"p1".as_slice()],
                vec![b"f".as_slice(), b"p2".as_slice()],
            ]
        );
        assert!(!header.noeol);
        assert_eq!(header.raw_record, b"BODY");
    }

    #[test]
    fn network_header_empty_parents_list_is_some_empty() {
        let bytes = b"knit-ft-gz\nk\n\nNX";
        let header = parse_network_record_header(bytes, 11).unwrap();
        assert_eq!(header.parents.unwrap().len(), 0);
        assert_eq!(header.raw_record, b"X");
    }

    #[test]
    fn split_keys_by_prefix_preserves_first_seen_order() {
        let keys: Vec<Vec<Vec<u8>>> = vec![
            vec![b"file-a".to_vec(), b"rev-1".to_vec()],
            vec![b"file-b".to_vec(), b"rev-1".to_vec()],
            vec![b"file-a".to_vec(), b"rev-2".to_vec()],
            vec![b"lone-rev".to_vec()], // single-segment => empty prefix
            vec![b"file-b".to_vec(), b"rev-2".to_vec()],
        ];
        let (buckets, order) = split_keys_by_prefix(&keys);
        assert_eq!(
            order,
            vec![b"file-a".to_vec(), b"file-b".to_vec(), Vec::<u8>::new()]
        );
        assert_eq!(buckets.len(), 3);
        assert_eq!(buckets[0].0, b"file-a".to_vec());
        assert_eq!(buckets[0].1.len(), 2);
        assert_eq!(buckets[0].1[0], keys[0].as_slice());
        assert_eq!(buckets[0].1[1], keys[2].as_slice());
        assert_eq!(buckets[2].0, Vec::<u8>::new());
        assert_eq!(buckets[2].1, vec![keys[3].as_slice()]);
    }

    #[test]
    fn split_keys_by_prefix_empty_input() {
        let keys: Vec<Vec<Vec<u8>>> = vec![];
        let (buckets, order) = split_keys_by_prefix(&keys);
        assert!(buckets.is_empty());
        assert!(order.is_empty());
    }

    #[test]
    fn knit_delta_closure_wire_matches_python_layout() {
        // Reference bytes built by hand from the Python _wire_bytes layout.
        // emit_keys: [(file, rev1), (rev2,)]
        // records: one with None parents, method "line-delta", noeol=True,
        // next=(), record body b"BODY-1"; second annotated=False path.
        let key1: &[&[u8]] = &[b"file", b"rev1"];
        let key2: &[&[u8]] = &[b"rev2"];
        let emit_keys: &[&[&[u8]]] = &[key1, key2];

        let parent_a: &[&[u8]] = &[b"file", b"p0"];
        let rec2_parents: &[&[&[u8]]] = &[parent_a];
        let next2: &[&[u8]] = &[b"file", b"rev1"];

        let records = [
            KnitDeltaClosureRecord {
                key: key1,
                parents: None,
                method: b"line-delta",
                noeol: true,
                next: None,
                record_bytes: b"BODY-1",
            },
            KnitDeltaClosureRecord {
                key: key2,
                parents: Some(rec2_parents),
                method: b"fulltext",
                noeol: false,
                next: Some(next2),
                record_bytes: b"BODY-2",
            },
        ];

        let out = build_knit_delta_closure_wire(true, emit_keys, &records);

        let mut expected: Vec<u8> = Vec::new();
        expected.extend_from_slice(b"knit-delta-closure\n");
        expected.extend_from_slice(b"annotated\n");
        expected.extend_from_slice(b"file\x00rev1\trev2\n");
        // record 1
        expected.extend_from_slice(b"file\x00rev1\n");
        expected.extend_from_slice(b"None:\n");
        expected.extend_from_slice(b"line-delta\n");
        expected.extend_from_slice(b"T\n");
        expected.extend_from_slice(b"\n"); // empty "next" line
        expected.extend_from_slice(b"6\n"); // len("BODY-1")
        expected.extend_from_slice(b"BODY-1");
        // record 2
        expected.extend_from_slice(b"rev2\n");
        expected.extend_from_slice(b"file\x00p0\n");
        expected.extend_from_slice(b"fulltext\n");
        expected.extend_from_slice(b"F\n");
        expected.extend_from_slice(b"file\x00rev1\n");
        expected.extend_from_slice(b"6\n");
        expected.extend_from_slice(b"BODY-2");

        assert_eq!(out, expected);
    }

    #[test]
    fn knit_delta_closure_wire_unannotated_has_blank_flag_line() {
        let emit_keys: &[&[&[u8]]] = &[];
        let out = build_knit_delta_closure_wire(false, emit_keys, &[]);
        // knit-delta-closure\n + empty-annotated-line\n + empty-keys-line\n
        assert_eq!(out, b"knit-delta-closure\n\n\n".to_vec());
    }

    #[test]
    fn build_network_record_round_trips_none_parents() {
        let key: &[&[u8]] = &[b"file-id", b"rev"];
        let raw = build_network_record(b"knit-ft-gz", key, NO_PARENTS, true, b"DATA");
        let line_end = b"knit-ft-gz\n".len();
        let parsed = parse_network_record_header(&raw, line_end).unwrap();
        assert_eq!(parsed.key, vec![&b"file-id"[..], &b"rev"[..]]);
        assert!(parsed.parents.is_none());
        assert!(parsed.noeol);
        assert_eq!(parsed.raw_record, b"DATA");
    }

    #[test]
    fn build_network_record_round_trips_with_parents_and_eol() {
        let key: &[&[u8]] = &[b"f", b"r"];
        let p1: &[&[u8]] = &[b"f", b"p1"];
        let p2: &[&[u8]] = &[b"f", b"p2"];
        let parents: &[&[&[u8]]] = &[p1, p2];
        let raw = build_network_record(b"knit-delta-gz", key, Some(parents), false, b"BODY");
        let line_end = b"knit-delta-gz\n".len();
        let parsed = parse_network_record_header(&raw, line_end).unwrap();
        assert_eq!(parsed.parents.unwrap().len(), 2);
        assert!(!parsed.noeol);
        assert_eq!(parsed.raw_record, b"BODY");
    }

    #[test]
    fn build_network_record_single_key_segment() {
        let key: &[&[u8]] = &[b"only"];
        let raw = build_network_record(b"knit-ft-gz", key, NO_PARENTS, true, b"X");
        // Reconstruct by hand to pin the on-wire format.
        assert_eq!(raw, b"knit-ft-gz\nonly\nNone:\nNX".to_vec());
    }

    #[test]
    fn network_header_rejects_missing_noeol_byte() {
        let bytes = b"knit-ft-gz\nk\nNone:\n";
        let err = parse_network_record_header(bytes, 11).unwrap_err();
        assert_eq!(err, KnitError::NetworkMissingNoEolByte);
    }

    fn build_record(version_id: &[u8], digest: &[u8], body: &[&[u8]]) -> Vec<u8> {
        let mut header = Vec::new();
        header.extend_from_slice(b"version ");
        header.extend_from_slice(version_id);
        header.extend_from_slice(format!(" {} ", body.len()).as_bytes());
        header.extend_from_slice(digest);
        header.push(b'\n');

        let mut end = Vec::new();
        end.extend_from_slice(b"end ");
        end.extend_from_slice(version_id);
        end.push(b'\n');

        let mut chunks: Vec<&[u8]> = vec![&header];
        chunks.extend_from_slice(body);
        chunks.push(&end);

        let gz = crate::tuned_gzip::chunks_to_gzip(chunks.iter().copied());
        gz.into_iter().flatten().collect()
    }

    #[test]
    fn parse_record_unchecked_round_trip() {
        let body: &[&[u8]] = &[b"first line\n", b"second line\n"];
        let raw = build_record(b"rev-1", b"DIGEST", body);
        let (rec, contents) = parse_record_unchecked(&raw).unwrap();
        assert_eq!(rec.method, b"version");
        assert_eq!(rec.version_id, b"rev-1");
        assert_eq!(rec.count, 2);
        assert_eq!(rec.digest, b"DIGEST");
        assert_eq!(
            contents,
            vec![b"first line\n".to_vec(), b"second line\n".to_vec()]
        );
    }

    #[test]
    fn parse_record_unchecked_zero_body() {
        let raw = build_record(b"rev-0", b"DD", &[]);
        let (rec, contents) = parse_record_unchecked(&raw).unwrap();
        assert_eq!(rec.count, 0);
        assert!(contents.is_empty());
    }

    #[test]
    fn parse_record_unchecked_wrong_line_count() {
        // Build a valid record then re-gzip it with a tampered header that
        // claims too many lines.
        let mut header = b"version rev-x 5 DD\n".to_vec();
        let body = b"only one\n".to_vec();
        let end = b"end rev-x\n".to_vec();
        let chunks: Vec<&[u8]> = vec![&header[..], &body[..], &end[..]];
        let gz = crate::tuned_gzip::chunks_to_gzip(chunks.iter().copied());
        let raw: Vec<u8> = gz.into_iter().flatten().collect();
        // suppress unused_mut lint; header is intentionally mutable to match
        // the surrounding builder style.
        let _ = &mut header;
        let err = parse_record_unchecked(&raw).unwrap_err();
        assert_eq!(
            err,
            KnitError::LineCount {
                declared: 5,
                actual: 1,
            }
        );
    }

    #[test]
    fn parse_record_header_only_ignores_line_count_mismatch() {
        // Record claims 2 body lines but only ships 1. parse_record_unchecked
        // would reject this; parse_record_header_only must accept it so
        // `_KnitData._read_records_iter_raw` stays lenient as the Python
        // tests require.
        let header = b"version rev-id-1 2 DIGEST\n".to_vec();
        let body = b"foo\n".to_vec();
        let end = b"end rev-id-1\n".to_vec();
        let chunks: Vec<&[u8]> = vec![&header, &body, &end];
        let gz = crate::tuned_gzip::chunks_to_gzip(chunks.into_iter());
        let raw: Vec<u8> = gz.into_iter().flatten().collect();

        assert!(parse_record_unchecked(&raw).is_err());
        let rec = parse_record_header_only(&raw).unwrap();
        assert_eq!(rec.version_id, b"rev-id-1");
        assert_eq!(rec.count, 2);
        assert_eq!(rec.digest, b"DIGEST");
    }

    #[test]
    fn parse_record_unchecked_reports_gzip_errors_as_knit_error() {
        // Garbage that isn't a gzip stream at all — flate2 raises an
        // io::Error which we normalise into KnitError::Gzip(String).
        let err = parse_record_unchecked(b"definitely not gzip").unwrap_err();
        assert!(matches!(err, KnitError::Gzip(_)));
        // The Display impl threads through the underlying message.
        assert!(err.to_string().contains("corrupt compressed record"));
    }

    #[test]
    fn readlines_iter_matches_collected_and_handles_unterminated_tail() {
        let data = b"alpha\nbeta\ngamma";
        let streamed: Vec<&[u8]> = ReadLines::new(data).collect();
        assert_eq!(
            streamed,
            vec![&b"alpha\n"[..], &b"beta\n"[..], &b"gamma"[..]]
        );
        assert_eq!(streamed, readlines(data));
        // Empty and single-line edge cases.
        assert!(ReadLines::new(b"").next().is_none());
        assert_eq!(readlines(b"just-one"), vec![&b"just-one"[..]]);
        assert_eq!(readlines(b"\n"), vec![&b"\n"[..]]);
    }

    #[test]
    fn recompress_annotated_to_unannotated_fulltext_strips_origins() {
        // Build an annotated fulltext record by hand, run it through the
        // recompressor, and verify the output parses as a plain knit
        // record carrying just the text bytes.
        let annotated: Vec<AnnotatedLine> = vec![
            (b"rev1".to_vec(), b"alpha\n".to_vec()),
            (b"rev2".to_vec(), b"beta\n".to_vec()),
        ];
        let body = lower_fulltext(&annotated);
        let (_, chunks) = record_to_data(b"rev-id", b"DIGEST", body.len(), &body, true).unwrap();
        let raw: Vec<u8> = chunks.into_iter().flatten().collect();

        let unannotated_raw = recompress_annotated_to_unannotated_fulltext(&raw).unwrap();

        let (header, body_lines) = parse_record_unchecked(&unannotated_raw).unwrap();
        assert_eq!(header.version_id, b"rev-id");
        assert_eq!(header.digest, b"DIGEST");
        assert_eq!(header.count, 2);
        assert_eq!(body_lines, vec![b"alpha\n".to_vec(), b"beta\n".to_vec()]);
    }

    #[test]
    fn recompress_annotated_to_unannotated_delta_strips_origins() {
        let delta = vec![DeltaHunk {
            start: 0,
            end: 1,
            count: 2,
            lines: vec![
                (b"r1".to_vec(), b"alpha\n".to_vec()),
                (b"r2".to_vec(), b"beta\n".to_vec()),
            ],
        }];
        let body = lower_line_delta_annotated(&delta);
        let (_, chunks) = record_to_data(b"rev-id", b"DD", body.len(), &body, true).unwrap();
        let raw: Vec<u8> = chunks.into_iter().flatten().collect();

        let unannotated_raw = recompress_annotated_to_unannotated_delta(&raw).unwrap();

        let (header, body_lines) = parse_record_unchecked(&unannotated_raw).unwrap();
        assert_eq!(header.version_id, b"rev-id");
        assert_eq!(header.digest, b"DD");
        // Plain delta wire format: 1 header line + 2 content lines.
        assert_eq!(body_lines.len(), 3);
        assert_eq!(body_lines[0], b"0,1,2\n".to_vec());
        assert_eq!(body_lines[1], b"alpha\n".to_vec());
        assert_eq!(body_lines[2], b"beta\n".to_vec());
    }

    #[test]
    fn parse_record_body_unchecked_borrows_from_buffer() {
        // Build the decompressed form by hand so we can show the returned
        // slices alias the caller-owned buffer — no per-line allocation.
        let mut body = Vec::new();
        body.extend_from_slice(b"version rev-x 2 DIG\n");
        body.extend_from_slice(b"alpha\n");
        body.extend_from_slice(b"beta\n");
        body.extend_from_slice(b"end rev-x\n");
        let (header, lines) = parse_record_body_unchecked(&body).unwrap();
        assert_eq!(header.method, b"version");
        assert_eq!(header.version_id, b"rev-x");
        assert_eq!(header.count, 2);
        assert_eq!(header.digest, b"DIG");
        assert_eq!(lines, vec![&b"alpha\n"[..], &b"beta\n"[..]]);
        // Prove the returned slices actually borrow from `body`.
        let body_range = body.as_ptr_range();
        for line in &lines {
            let start = line.as_ptr();
            assert!(start >= body_range.start && start < body_range.end);
        }
    }

    #[test]
    fn record_to_data_round_trip_via_parse() {
        let body: Vec<Vec<u8>> = vec![b"alpha\n".to_vec(), b"beta\n".to_vec()];
        let (len, chunks) = record_to_data(b"rev-7", b"DIGEST", body.len(), &body, true).unwrap();
        let raw: Vec<u8> = chunks.into_iter().flatten().collect();
        assert_eq!(len, raw.len());
        let (rec, contents) = parse_record_unchecked(&raw).unwrap();
        assert_eq!(rec.version_id, b"rev-7");
        assert_eq!(rec.count, 2);
        assert_eq!(rec.digest, b"DIGEST");
        assert_eq!(contents, body);
    }

    #[test]
    fn record_to_data_rejects_missing_trailing_newline() {
        let body: Vec<Vec<u8>> = vec![b"no-newline".to_vec()];
        let err = record_to_data(b"rev", b"DD", 1, &body, false).unwrap_err();
        assert_eq!(err, KnitError::MissingTrailingNewline);
    }

    #[test]
    fn record_to_data_empty_body() {
        // Empty `lines` ⇒ has_trailing_newline is vacuously true in the Python
        // original, and the resulting record has zero body lines.
        let empty: Vec<Vec<u8>> = vec![];
        let (_, chunks) = record_to_data(b"rev-0", b"DD", 0, &empty, true).unwrap();
        let raw: Vec<u8> = chunks.into_iter().flatten().collect();
        let (rec, contents) = parse_record_unchecked(&raw).unwrap();
        assert_eq!(rec.count, 0);
        assert!(contents.is_empty());
    }

    #[test]
    fn parse_record_unchecked_bad_end_marker() {
        let mut header = b"version rev-y 1 DD\n".to_vec();
        let body = b"body\n".to_vec();
        let end = b"end wrong-id\n".to_vec();
        let chunks: Vec<&[u8]> = vec![&header[..], &body[..], &end[..]];
        let gz = crate::tuned_gzip::chunks_to_gzip(chunks.iter().copied());
        let raw: Vec<u8> = gz.into_iter().flatten().collect();
        let _ = &mut header;
        let err = parse_record_unchecked(&raw).unwrap_err();
        assert_eq!(
            err,
            KnitError::BadEndMarker {
                expected: b"end rev-y\n".to_vec(),
                actual: b"end wrong-id\n".to_vec(),
            }
        );
    }
}

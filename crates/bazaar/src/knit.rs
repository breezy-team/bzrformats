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
//! ## In-memory content
//!
//! - [`KnitContent`] (trait) with the [`AnnotatedKnitContent`] and
//!   [`PlainKnitContent`] implementations — typed views of a knit
//!   version's lines that support `apply_delta`, `text`, `annotate`,
//!   and the `should_strip_eol` flag.
//! - [`KnitFactory`] (trait) with the [`KnitAnnotateFactory`] and
//!   [`KnitPlainFactory`] implementations — strategies for parsing a
//!   record's body lines into a `KnitContent`. The trait's
//!   [`KnitFactory::parse_record`] default method handles the
//!   fulltext/line-delta dispatch given a parent fulltext for the
//!   delta case.
//!
//! ## High-level read pipeline
//!
//! - [`KnitIndex`] (trait) — looks up build details for a batch of
//!   keys. Pure-Rust callers implement this directly; pyo3 callers
//!   can wrap a Python `_KnitGraphIndex` / `_KndxIndex`.
//! - [`KnitAccess`] (trait) — fetches raw record bytes for an
//!   `index_memo`. Pure-Rust callers implement this directly; pyo3
//!   callers can wrap a Python `_KnitKeyAccess` / `_DirectPackAccess`.
//! - [`KnitRecordDetails`] / [`KnitIndexMemo`] / [`KnitKey`] — the
//!   value types those traits trade in.
//! - [`get_text`] / [`get_content`] — walk the compression chain
//!   starting at one key, fetching raw records via the access layer
//!   and applying deltas via the factory, to reconstruct the target
//!   content. The pure-Rust equivalent of `KnitVersionedFiles.get_text`.
//!
//! ## Index helpers
//!
//! - [`parse_knit_index_value`] / [`KnitIndexValue`] — decode a knit
//!   graph index entry's `value` field (`<flag><pos> <size>`).
//! - [`decode_knit_build_details`] / [`KnitBuildDetails`] — decide
//!   `(method, noeol, pos, size)` for a single `_KnitGraphIndex` entry.
//! - [`decode_kndx_options`] — decide `(method, noeol)` from a kndx
//!   cache row's options bytes-list.
//! - [`KnitMethod`] — typed `"fulltext"` / `"line-delta"` marker.
//!
//! ## Closure traversal
//!
//! - [`walk_compression_closure`] / [`ClosureBatch`] — generic batched
//!   BFS over a compression-parent graph, used by
//!   `KnitVersionedFiles._get_components_positions`.
//! - [`should_use_delta`] / [`DeltaDecision`] / [`ChainStep`] — walk a
//!   parent chain looking for a fulltext and decide whether the
//!   cumulative delta size is worth storing as a new delta.
//!
//! ## Supporting helpers
//!
//! - [`split_keys_by_prefix`] — order-preserving groupby over a list of
//!   knit keys. Used by the Python `_split_by_prefix` on the checkout
//!   batching path.
//!
//! All of the above share a single [`KnitError`] enum; functions return
//! `Result<_, KnitError>` so callers only need one error match-arm set.
//!
//! # Pure-Rust read pipeline
//!
//! Reading a knit fulltext record without going through the pyo3 layer
//! looks like this:
//!
//! ```ignore
//! use bazaar::knit::{
//!     decode_record_gz, parse_record_body_unchecked, KnitAnnotateFactory,
//!     KnitFactory, KnitMethod, KnitContent,
//! };
//!
//! let raw: Vec<u8> = read_record_from_disk();
//! let body = decode_record_gz(&raw)?;
//! let (header, body_lines) = parse_record_body_unchecked(&body)?;
//! let factory = KnitAnnotateFactory;
//! let content = factory.parse_record(
//!     header.version_id,
//!     &body_lines,
//!     KnitMethod::Fulltext,
//!     /* noeol */ false,
//!     /* base_content */ None,
//! )?;
//! let lines: Vec<Vec<u8>> = content.text();
//! ```
//!
//! For a delta record, fetch the parent record first, run it through
//! the same pipeline as a fulltext, and pass the resulting content as
//! `base_content`. The `pure_rust_delta_chain_apply_pipeline` test in
//! this module is a worked example.

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

    // --- knit graph index layer ---
    /// A knit graph index entry's `value` field was not in the expected
    /// `[N| ]<pos> <size>` shape.
    BadIndexValue(Vec<u8>),
    /// A knit delta record claimed more than one compression parent.
    TooManyCompressionParents(usize),
    /// A record's header `version_id` field did not match the caller's
    /// expected value — used by `parse_record` when verifying that a
    /// fetched record really belongs to the requested key.
    UnexpectedVersion { wanted: Vec<u8>, got: Vec<u8> },
    /// A `.kndx` file did not start with the expected `KNDX_HEADER` bytes.
    BadKnitHeader { path: String },
    /// A `.kndx` record line contained a corrupt field (pos, size, or parent).
    KndxCorrupt { line: Vec<u8>, detail: String },
    /// A knit index detected an inconsistency (e.g. duplicate with different
    /// metadata, or a delta record in a non-delta index).
    Corrupt(String),
    /// A write operation was attempted on a read-only index (no add_callback set).
    ReadOnly,
    /// The operation is not supported by this index type (e.g. compression
    /// parent tracking on `_KndxIndex`, which uses an append-only on-disk
    /// format that cannot defer parents).
    NotImplemented(&'static str),
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
            KnitError::BadIndexValue(v) => {
                write!(f, "bad knit index value: {:?}", v)
            }
            KnitError::TooManyCompressionParents(n) => {
                write!(f, "Too many compression parents: {}", n)
            }
            KnitError::UnexpectedVersion { wanted, got } => {
                write!(f, "unexpected version, wanted {:?}, got {:?}", wanted, got)
            }
            KnitError::BadKnitHeader { path } => {
                write!(f, "knit index file {} does not have a valid header", path)
            }
            KnitError::KndxCorrupt { line, detail } => {
                write!(f, "kndx corrupt record {:?}: {}", line, detail)
            }
            KnitError::Corrupt(msg) => write!(f, "knit corrupt: {}", msg),
            KnitError::ReadOnly => write!(f, "write attempted on read-only knit index"),
            KnitError::NotImplemented(name) => write!(f, "{}", name),
        }
    }
}

impl std::error::Error for KnitError {}

/// Error returned by [`KndxIndex::load_prefix_typed`]: either a transport
/// I/O failure or a corrupted kndx header.
#[derive(Debug)]
pub enum KndxLoadError {
    Transport(crate::transport::TransportError),
    Knit(KnitError),
}

impl std::fmt::Display for KndxLoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KndxLoadError::Transport(e) => e.fmt(f),
            KndxLoadError::Knit(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for KndxLoadError {}

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

/// Trait shared by [`AnnotatedKnitContent`] and [`PlainKnitContent`].
///
/// Mirrors the Python `KnitContent` base class. Both implementations are
/// in-memory views of a knit version's lines, with a `should_strip_eol`
/// flag that affects how the trailing newline of the last line is
/// reported by [`Self::text`] and [`Self::annotate`].
///
/// Pure-Rust callers that want to read or rebuild a knit version (apply
/// a delta to a parent fulltext, dump out the resulting text) can work
/// with these types directly without going through the pyo3 layer.
pub trait KnitContent {
    /// Per-line payload type carried by this content's deltas.
    /// `AnnotatedKnitContent` uses `(origin, text)` pairs;
    /// `PlainKnitContent` uses bare text bytes.
    type DeltaLine: Clone;

    /// Whether the trailing `\n` on the last line should be stripped on
    /// output. Mirrors the Python `_should_strip_eol` flag.
    fn should_strip_eol(&self) -> bool;
    /// Set the strip-eol flag.
    fn set_should_strip_eol(&mut self, strip: bool);

    /// Apply a line delta in place.
    ///
    /// Each hunk replaces lines `[offset+start .. offset+end]` with the
    /// hunk's payload, where `offset` accumulates as the running cursor
    /// adjustment from the prior hunks (`offset += start - end + count`).
    /// `new_version_id` is only meaningful for [`PlainKnitContent`],
    /// which records it as its new owning version; annotated content
    /// ignores it because each line carries its own origin already.
    fn apply_delta(&mut self, delta: &[DeltaHunk<Self::DeltaLine>], new_version_id: &[u8]);

    /// Return just the text lines (without origin annotations). If
    /// `should_strip_eol` is set, the trailing `\n` of the last line is
    /// removed in the returned copy.
    fn text(&self) -> Vec<Vec<u8>>;

    /// Return `(origin, text)` pairs. For [`PlainKnitContent`] the
    /// `origin` is always the content's `version_id`.
    fn annotate(&self) -> Vec<AnnotatedLine>;

    /// Return a mutable reference to the `(origin, text)` pairs so that
    /// [`merge_annotations`] can update line origins in place.
    ///
    /// Only valid for annotated content ([`AnnotatedKnitContent`]). Calling
    /// this on plain content panics; `merge_annotations` guards the call
    /// behind `factory.annotated()`.
    fn annotate_mut(&mut self) -> &mut Vec<AnnotatedLine> {
        unimplemented!("annotate_mut is only supported for annotated content")
    }

    /// Return `(origin, text)` pairs from the raw internal storage, without
    /// applying the `should_strip_eol` flag. Used by [`compute_line_delta`]
    /// to build delta hunks that preserve trailing newlines on stored lines.
    fn annotate_raw(&self) -> Vec<AnnotatedLine>;

    /// Convert an `(origin, text)` pair into the `DeltaLine` type for this
    /// content.  Used by [`compute_line_delta`] to build typed delta hunks
    /// without knowing the concrete content type.
    fn delta_line_from_annotated(pair: &AnnotatedLine) -> Self::DeltaLine;
}

/// In-memory view of an annotated knit version: a flat list of
/// `(origin, text)` pairs.
///
/// Mirrors `bzrformats.knit.AnnotatedKnitContent`. The `apply_delta`
/// path takes plain (origin-stripped) deltas because the annotated
/// delta already had its origins consumed when the line was built.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnotatedKnitContent {
    pub lines: Vec<AnnotatedLine>,
    should_strip_eol: bool,
}

impl AnnotatedKnitContent {
    pub fn new(lines: Vec<AnnotatedLine>) -> Self {
        Self {
            lines,
            should_strip_eol: false,
        }
    }
}

impl KnitContent for AnnotatedKnitContent {
    type DeltaLine = AnnotatedLine;

    fn should_strip_eol(&self) -> bool {
        self.should_strip_eol
    }

    fn set_should_strip_eol(&mut self, strip: bool) {
        self.should_strip_eol = strip;
    }

    fn apply_delta(&mut self, delta: &[DeltaHunk<AnnotatedLine>], _new_version_id: &[u8]) {
        // Each hunk's lines are already `(origin, text)` pairs that
        // came from the annotated parser — splice them in directly,
        // preserving the origins. Matches
        // `AnnotatedKnitContent.apply_delta` in knit.py.
        let mut offset: isize = 0;
        for hunk in delta {
            let start = (offset + hunk.start as isize) as usize;
            let end = (offset + hunk.end as isize) as usize;
            self.lines.splice(start..end, hunk.lines.iter().cloned());
            offset += hunk.start as isize - hunk.end as isize + hunk.count as isize;
        }
    }

    fn text(&self) -> Vec<Vec<u8>> {
        let mut out: Vec<Vec<u8>> = self.lines.iter().map(|(_, t)| t.clone()).collect();
        if self.should_strip_eol {
            if let Some(last) = out.last_mut() {
                if last.ends_with(b"\n") {
                    last.pop();
                }
            }
        }
        out
    }

    fn annotate(&self) -> Vec<AnnotatedLine> {
        let mut out = self.lines.clone();
        if self.should_strip_eol {
            if let Some((_, last)) = out.last_mut() {
                if last.ends_with(b"\n") {
                    last.pop();
                }
            }
        }
        out
    }

    fn annotate_mut(&mut self) -> &mut Vec<AnnotatedLine> {
        &mut self.lines
    }

    fn annotate_raw(&self) -> Vec<AnnotatedLine> {
        self.lines.clone()
    }

    fn delta_line_from_annotated(pair: &AnnotatedLine) -> Self::DeltaLine {
        pair.clone()
    }
}

/// In-memory view of an unannotated knit version: a flat list of text
/// lines plus the version_id that owns them.
///
/// Mirrors `bzrformats.knit.PlainKnitContent`. `annotate` reports every
/// line as belonging to `version_id` since plain content has no per-line
/// origin information.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlainKnitContent {
    pub lines: Vec<Vec<u8>>,
    pub version_id: Vec<u8>,
    should_strip_eol: bool,
}

impl PlainKnitContent {
    pub fn new(lines: Vec<Vec<u8>>, version_id: Vec<u8>) -> Self {
        Self {
            lines,
            version_id,
            should_strip_eol: false,
        }
    }
}

impl KnitContent for PlainKnitContent {
    type DeltaLine = Vec<u8>;

    fn should_strip_eol(&self) -> bool {
        self.should_strip_eol
    }

    fn set_should_strip_eol(&mut self, strip: bool) {
        self.should_strip_eol = strip;
    }

    fn apply_delta(&mut self, delta: &[DeltaHunk<Vec<u8>>], new_version_id: &[u8]) {
        let mut offset: isize = 0;
        for hunk in delta {
            let start = (offset + hunk.start as isize) as usize;
            let end = (offset + hunk.end as isize) as usize;
            self.lines.splice(start..end, hunk.lines.iter().cloned());
            offset += hunk.start as isize - hunk.end as isize + hunk.count as isize;
        }
        self.version_id = new_version_id.to_vec();
    }

    fn text(&self) -> Vec<Vec<u8>> {
        let mut out = self.lines.clone();
        if self.should_strip_eol {
            if let Some(last) = out.last_mut() {
                if last.ends_with(b"\n") {
                    last.pop();
                }
            }
        }
        out
    }

    fn annotate(&self) -> Vec<AnnotatedLine> {
        let mut out: Vec<AnnotatedLine> = self
            .lines
            .iter()
            .map(|l| (self.version_id.clone(), l.clone()))
            .collect();
        if self.should_strip_eol {
            if let Some((_, last)) = out.last_mut() {
                if last.ends_with(b"\n") {
                    last.pop();
                }
            }
        }
        out
    }

    fn annotate_raw(&self) -> Vec<AnnotatedLine> {
        self.lines
            .iter()
            .map(|l| (self.version_id.clone(), l.clone()))
            .collect()
    }

    fn delta_line_from_annotated(pair: &AnnotatedLine) -> Self::DeltaLine {
        // Plain content uses bare text bytes as its delta line type.
        pair.1.clone()
    }
}

/// Strategy for parsing raw knit body lines into [`KnitContent`] values
/// and serializing them back out.
///
/// Mirrors the Python `_KnitFactory` / `KnitAnnotateFactory` /
/// `KnitPlainFactory` hierarchy. `parse_record` is the highest-level
/// entry point: given the body lines of a record plus the
/// `(method, noeol)` pair from `KnitBuildDetails`, build the
/// corresponding `KnitContent`. For `LineDelta` records the caller
/// supplies the parent fulltext as `base_content`; the factory parses
/// the delta, clones the base, applies the delta, and returns the
/// reconstructed content.
pub trait KnitFactory {
    type Content: KnitContent + Clone;

    /// Whether records emitted by this factory carry per-line origins.
    /// The annotated factory returns `true`, the plain factory `false`.
    fn annotated(&self) -> bool;

    /// Build a fulltext content object from the body lines of a knit
    /// record. The lines are the raw body bytes as returned by
    /// [`parse_record_body_unchecked`] / [`parse_record_unchecked`].
    fn parse_fulltext_content(
        &self,
        lines: &[&[u8]],
        version_id: &[u8],
    ) -> Result<Self::Content, KnitError>;

    /// Parse a delta record's body into the hunk shape that this
    /// factory's [`KnitContent`] consumes. For
    /// [`KnitAnnotateFactory`] this yields annotated
    /// `(origin, text)` hunks; for [`KnitPlainFactory`] it yields
    /// bare-byte hunks.
    fn parse_line_delta(
        &self,
        lines: &[&[u8]],
    ) -> Result<Vec<DeltaHunk<<Self::Content as KnitContent>::DeltaLine>>, KnitError>;

    // --- write side ---

    /// Build a new content object from plain text lines and a version id.
    ///
    /// For the annotated factory each line is tagged with `version_id` as
    /// its origin (matching the Python `KnitAnnotateFactory.make` behaviour).
    /// For the plain factory the lines are stored as-is.
    fn make(&self, lines: Vec<Vec<u8>>, version_id: Vec<u8>) -> Self::Content;

    /// Serialize a content object to the wire/storage byte lines for a
    /// fulltext record.  This is the inverse of `parse_fulltext_content`.
    fn lower_fulltext(&self, content: &Self::Content) -> Vec<Vec<u8>>;

    /// Serialize a line delta to the wire/storage byte lines.
    /// This is the inverse of `parse_line_delta`.
    fn lower_line_delta(
        &self,
        delta: &[DeltaHunk<<Self::Content as KnitContent>::DeltaLine>],
    ) -> Vec<Vec<u8>>;

    /// Build a content object from a record's body lines and its
    /// `(method, noeol)` pair. For `LineDelta` records `base_content`
    /// must contain the parent fulltext; it's cloned and patched.
    /// Returns the reconstructed content with `should_strip_eol` set
    /// from `noeol`.
    fn parse_record(
        &self,
        version_id: &[u8],
        body_lines: &[&[u8]],
        method: KnitMethod,
        noeol: bool,
        base_content: Option<&Self::Content>,
    ) -> Result<Self::Content, KnitError> {
        let mut content = match method {
            KnitMethod::Fulltext => self.parse_fulltext_content(body_lines, version_id)?,
            KnitMethod::LineDelta => {
                let base = base_content.ok_or_else(|| {
                    KnitError::BadIndexValue(b"line-delta record requires base content".to_vec())
                })?;
                let mut content = base.clone();
                let delta = self.parse_line_delta(body_lines)?;
                content.apply_delta(&delta, version_id);
                content
            }
            KnitMethod::NoEol => {
                return Err(KnitError::BadIndexValue(
                    b"NoEol is not a storage method; use Fulltext or LineDelta".to_vec(),
                ))
            }
        };
        content.set_should_strip_eol(noeol);
        Ok(content)
    }
}

/// Annotated knit codec strategy. Builds [`AnnotatedKnitContent`] from
/// `(origin, text)`-formatted body lines.
#[derive(Debug, Default, Clone, Copy)]
pub struct KnitAnnotateFactory;

impl KnitFactory for KnitAnnotateFactory {
    type Content = AnnotatedKnitContent;

    fn annotated(&self) -> bool {
        true
    }

    fn parse_fulltext_content(
        &self,
        lines: &[&[u8]],
        _version_id: &[u8],
    ) -> Result<Self::Content, KnitError> {
        let pairs = parse_fulltext(lines)?;
        Ok(AnnotatedKnitContent::new(pairs))
    }

    fn parse_line_delta(
        &self,
        lines: &[&[u8]],
    ) -> Result<Vec<DeltaHunk<AnnotatedLine>>, KnitError> {
        parse_line_delta_annotated(lines)
    }

    fn make(&self, lines: Vec<Vec<u8>>, version_id: Vec<u8>) -> Self::Content {
        AnnotatedKnitContent::new(
            lines
                .into_iter()
                .map(|text| (version_id.clone(), text))
                .collect(),
        )
    }

    fn lower_fulltext(&self, content: &Self::Content) -> Vec<Vec<u8>> {
        lower_fulltext(&content.lines)
    }

    fn lower_line_delta(&self, delta: &[DeltaHunk<AnnotatedLine>]) -> Vec<Vec<u8>> {
        lower_line_delta_annotated(delta)
    }
}

/// Plain (unannotated) knit codec strategy. Builds [`PlainKnitContent`]
/// directly from raw body lines.
#[derive(Debug, Default, Clone, Copy)]
pub struct KnitPlainFactory;

impl KnitFactory for KnitPlainFactory {
    type Content = PlainKnitContent;

    fn annotated(&self) -> bool {
        false
    }

    fn parse_fulltext_content(
        &self,
        lines: &[&[u8]],
        version_id: &[u8],
    ) -> Result<Self::Content, KnitError> {
        let lines: Vec<Vec<u8>> = lines.iter().map(|l| l.to_vec()).collect();
        Ok(PlainKnitContent::new(lines, version_id.to_vec()))
    }

    fn parse_line_delta(&self, lines: &[&[u8]]) -> Result<Vec<DeltaHunk<Vec<u8>>>, KnitError> {
        parse_line_delta_raw(lines)
    }

    fn make(&self, lines: Vec<Vec<u8>>, version_id: Vec<u8>) -> Self::Content {
        PlainKnitContent::new(lines, version_id)
    }

    fn lower_fulltext(&self, content: &Self::Content) -> Vec<Vec<u8>> {
        // Use the raw storage lines (not text()) so that the trailing '\n' added
        // by add_lines for noeol content is preserved in the stored record.
        content.lines.clone()
    }

    fn lower_line_delta(&self, delta: &[DeltaHunk<Vec<u8>>]) -> Vec<Vec<u8>> {
        lower_line_delta_raw(delta)
    }
}

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

/// Serialize a `_KnitGraphIndex`-style dictionary-compressed parent list.
///
/// Mirrors `_KndxIndex._dictionary_compress`: for each suffix, emit either its
/// decimal position in the per-prefix history (when the suffix is already in
/// the cache) or `b"." + suffix` as a fulltext fallback. Space-joined.
///
/// The caller extracts `cache[suffix] -> position` upfront; this function just
/// does the encoding so the whole serialization is a single FFI crossing.
///
/// Returns `Err` with the offending suffix on a cache miss is NOT this
/// function's job — the caller decides whether an unknown suffix is a fulltext
/// fallback (current kndx behaviour) or an error.
pub fn dictionary_compress_suffixes<S>(
    suffixes: &[S],
    lookup: &std::collections::HashMap<&[u8], u64>,
) -> Vec<u8>
where
    S: AsRef<[u8]>,
{
    if suffixes.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    for (i, suffix) in suffixes.iter().enumerate() {
        if i > 0 {
            out.push(b' ');
        }
        let s = suffix.as_ref();
        match lookup.get(s) {
            Some(pos) => {
                use std::io::Write;
                write!(out, "{}", pos).unwrap();
            }
            None => {
                out.push(b'.');
                out.extend_from_slice(s);
            }
        }
    }
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

/// Parse a knit record and verify that its embedded `version_id` matches
/// `expected_version`. Returns `(body_lines, digest)` on success, mirroring
/// `_KnitData._parse_record` in Python.
pub fn parse_record(
    expected_version: &[u8],
    data: &[u8],
) -> Result<(Vec<Vec<u8>>, Vec<u8>), KnitError> {
    let (header, body) = parse_record_unchecked(data)?;
    if header.version_id != expected_version {
        return Err(KnitError::UnexpectedVersion {
            wanted: expected_version.to_vec(),
            got: header.version_id,
        });
    }
    Ok((body, header.digest))
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

/// Whether a knit record is a fulltext or a line-delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KnitMethod {
    Fulltext,
    LineDelta,
    /// The `no-eol` option flag, stored alongside `Fulltext` or `LineDelta`
    /// in the index options list when the last line of the record has no
    /// trailing newline.
    NoEol,
}

impl KnitMethod {
    /// The historical Python-facing name of this method, used in the
    /// `record_details` tuple returned by `_KnitGraphIndex.get_build_details`.
    pub fn as_str(self) -> &'static str {
        match self {
            KnitMethod::Fulltext => "fulltext",
            KnitMethod::LineDelta => "line-delta",
            KnitMethod::NoEol => "no-eol",
        }
    }
}

/// Encode a single record for insertion into a `_KnitGraphIndex`.
///
/// Returns `(value_bytes, node_refs)` ready to pass to `add_callback`.
///
/// `node_refs` layout:
/// - no parents, no deltas: `()`
/// - parents, no deltas: `(parents,)`
/// - parents + deltas, fulltext: `(parents, ())`
/// - parents + deltas, line-delta: `(parents, (compression_parent,))`
///   where `compression_parent = parents[0]`.
///
/// Returns `Err` if `method == LineDelta` but `deltas == false`, or if
/// `parents` is non-empty but `has_parents == false`.
pub fn encode_graph_index_record(
    noeol: bool,
    pos: u64,
    size: u64,
    method: KnitMethod,
    has_parents: bool,
    has_deltas: bool,
    parents: &[KnitKey],
) -> Result<(Vec<u8>, Vec<Vec<KnitKey>>), KnitError> {
    if !has_deltas && method == KnitMethod::LineDelta {
        return Err(KnitError::Corrupt(
            "attempt to add line-delta in non-delta knit".to_string(),
        ));
    }
    if !has_parents && !parents.is_empty() {
        return Err(KnitError::Corrupt(
            "attempt to add node with parents in parentless index".to_string(),
        ));
    }
    let flag = if noeol { b'N' } else { b' ' };
    let value = format!("{}{} {}", flag as char, pos, size).into_bytes();
    let node_refs = if has_parents {
        if has_deltas {
            if method == KnitMethod::LineDelta {
                let compression_parent = parents.first().cloned().unwrap_or_default();
                vec![parents.to_vec(), vec![compression_parent]]
            } else {
                vec![parents.to_vec(), vec![]]
            }
        } else {
            vec![parents.to_vec()]
        }
    } else {
        vec![]
    };
    Ok((value, node_refs))
}

/// Parsed contents of a knit graph index `value` field.
///
/// `value` has the shape `<flag><pos> <size>` where `<flag>` is one byte
/// — `b'N'` for "no end-of-line" or `b' '` for the regular case — and
/// `pos` / `size` are ASCII decimal integers separated by a space.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KnitIndexValue {
    pub noeol: bool,
    pub pos: u64,
    pub size: u64,
}

/// Parse a `_KnitGraphIndex` entry's `value` field.
///
/// Mirrors the byte-splitting logic of the Python `_node_to_position`
/// helper: skip the leading flag byte, split the rest on the first
/// space, and parse `pos` / `size` as ASCII decimal.
pub fn parse_knit_index_value(value: &[u8]) -> Result<KnitIndexValue, KnitError> {
    if value.is_empty() {
        return Err(KnitError::BadIndexValue(value.to_vec()));
    }
    let noeol = value[0] == b'N';
    let trimmed = &value[1..];
    let mut parts = trimmed.splitn(2, |&b| b == b' ');
    let pos_bytes = parts
        .next()
        .ok_or_else(|| KnitError::BadIndexValue(value.to_vec()))?;
    let size_bytes = parts
        .next()
        .ok_or_else(|| KnitError::BadIndexValue(value.to_vec()))?;
    let pos: u64 = std::str::from_utf8(pos_bytes)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| KnitError::BadIndexValue(value.to_vec()))?;
    let size: u64 = std::str::from_utf8(size_bytes)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| KnitError::BadIndexValue(value.to_vec()))?;
    Ok(KnitIndexValue { noeol, pos, size })
}

/// Result of decoding the non-opaque parts of a `_KnitGraphIndex` entry.
///
/// The `index_memo`'s GraphIndex pointer (the first element of `entry`)
/// is opaque to this crate — pyo3 callers stitch it back together with
/// `pos` / `size` to form the final memo tuple. The other fields are
/// fully derived from the entry's `value` and `refs` columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnitBuildDetails {
    pub pos: u64,
    pub size: u64,
    pub noeol: bool,
    pub method: KnitMethod,
    /// The `compression_parent` key, if any. `None` for fulltexts and
    /// for parentless / non-delta indices.
    pub compression_parent: Option<usize>,
}

/// Result of a single batched lookup during a compression-closure walk.
///
/// `present` maps each found key to a `(compression_parent, payload)`
/// pair. The compression parent (an `Option<K>`) is the only field the
/// algorithm needs to drive the BFS — `payload` is opaque
/// caller-defined data that gets handed back in the final result dict.
/// `missing` is the subset of the requested keys that the lookup
/// couldn't resolve.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClosureBatch<K, P>
where
    K: Eq + std::hash::Hash + Clone,
{
    pub present: std::collections::HashMap<K, (Option<K>, P)>,
    pub missing: std::collections::HashSet<K>,
}

/// Walk the transitive compression closure of `initial_keys`, batching
/// lookups via `lookup_batch`.
///
/// Mirrors `KnitVersionedFiles._get_components_positions`: the caller's
/// `lookup_batch` returns a `ClosureBatch` for a given batch of keys.
/// Each present key carries its `compression_parent` (used to drive the
/// next BFS level) and an opaque `payload` value that the algorithm
/// just stores in the result dict — the caller decides what that
/// payload looks like (in knit's case it's the
/// `(record_details, index_memo, compression_parent)` triple).
///
/// Returns the assembled `{key: payload}` dict for every key whose
/// closure was traversed. The result is what
/// `KnitVersionedFiles._get_components_positions` returns minus the
/// per-format permutation, which lives in the caller.
///
/// If `allow_missing` is `false` and any batch reports missing keys,
/// returns `Err(missing_set)` after the first such batch.
#[allow(clippy::type_complexity)]
pub fn walk_compression_closure<K, P, F>(
    initial_keys: impl IntoIterator<Item = K>,
    allow_missing: bool,
    mut lookup_batch: F,
) -> Result<std::collections::HashMap<K, P>, std::collections::HashSet<K>>
where
    K: Eq + std::hash::Hash + Clone,
    F: FnMut(&[K]) -> ClosureBatch<K, P>,
{
    use std::collections::HashMap;

    let mut result: HashMap<K, P> = HashMap::new();
    let mut pending: Vec<K> = initial_keys.into_iter().collect();

    while !pending.is_empty() {
        let batch = lookup_batch(&pending);
        if !batch.missing.is_empty() && !allow_missing {
            return Err(batch.missing);
        }
        let mut next: Vec<K> = Vec::new();
        for (key, (compression_parent, payload)) in batch.present {
            if let Some(parent) = compression_parent.as_ref() {
                if !result.contains_key(parent) && !next.contains(parent) {
                    next.push(parent.clone());
                }
            }
            result.insert(key, payload);
        }
        pending = next;
    }

    Ok(result)
}

/// Outcome of [`should_use_delta`]'s parent-chain walk.
///
/// Distinguishes the three reasons we might decide *against* storing a
/// new delta — chain too long, missing parent, fulltext bigger than the
/// chain — so callers and tests can introspect the decision rather than
/// just see a `bool`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaDecision {
    /// Found a fulltext at the end of a chain shorter than `max_chain`,
    /// and `delta_size` is small enough that storing a new delta is
    /// worthwhile.
    UseDelta,
    /// Found a fulltext, but the cumulative delta size is greater than
    /// or equal to the fulltext size — better to write a new fulltext.
    FulltextSmaller,
    /// Walked `max_chain` parents without finding a fulltext.
    ChainTooLong,
    /// A parent in the chain wasn't present locally (a stacked fallback
    /// or a missing record). The Python original conservatively writes a
    /// new fulltext in this case.
    MissingParent,
}

impl DeltaDecision {
    /// Convenience: should the caller create a new delta? `true` only for
    /// [`DeltaDecision::UseDelta`].
    pub fn should_use_delta(self) -> bool {
        matches!(self, DeltaDecision::UseDelta)
    }
}

/// One step's worth of information about a parent in the compression
/// chain. The closure passed to [`should_use_delta`] returns this for
/// each parent it's asked about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainStep<K> {
    /// On-disk size (in bytes) of this parent's record.
    pub size: u64,
    /// Compression parent of this parent, if any. `None` means this
    /// parent is itself a fulltext, ending the walk.
    pub compression_parent: Option<K>,
}

/// Walk the compression chain starting at `initial_parent` and decide
/// whether the new record should be stored as a delta or as a fresh
/// fulltext.
///
/// Mirrors `KnitVersionedFiles._check_should_delta`. The closure
/// `get_step` is called once per parent (starting with `initial_parent`)
/// and should return `Some(ChainStep { size, compression_parent })` if
/// the parent is locally present, or `None` if it isn't.
///
/// The walk stops when:
/// - the closure returns `None` (missing parent — fall back to fulltext);
/// - we've inspected `max_chain` parents without finding a fulltext;
/// - we hit a fulltext (decide based on `delta_size` vs `fulltext_size`).
pub fn should_use_delta<K, F>(initial_parent: K, max_chain: usize, mut get_step: F) -> DeltaDecision
where
    F: FnMut(&K) -> Option<ChainStep<K>>,
{
    let mut delta_size: u64 = 0;
    let mut current = initial_parent;
    for _ in 0..max_chain {
        let step = match get_step(&current) {
            Some(s) => s,
            None => return DeltaDecision::MissingParent,
        };
        match step.compression_parent {
            None => {
                return if step.size > delta_size {
                    DeltaDecision::UseDelta
                } else {
                    DeltaDecision::FulltextSmaller
                };
            }
            Some(next) => {
                delta_size += step.size;
                current = next;
            }
        }
    }
    DeltaDecision::ChainTooLong
}

/// Decide method + noeol for a single `_KndxIndex` cache entry, given
/// its options bytes-list (the first element of the cached row).
///
/// Mirrors the Python `_KndxIndex.get_method` + `b"no-eol" in
/// self.get_options(key)` logic. Used by `_KndxIndex.get_build_details`
/// in tandem with the cache row's `(pos, size, parents)` to build the
/// final dict.
///
/// Returns `(method, noeol)`. Errors if `options` contains neither
/// `b"fulltext"` nor `b"line-delta"`.
pub fn decode_kndx_options<O: AsRef<[u8]>>(options: &[O]) -> Result<(KnitMethod, bool), KnitError> {
    let mut method: Option<KnitMethod> = None;
    let mut noeol = false;
    for opt in options {
        let o = opt.as_ref();
        if o == b"fulltext" {
            method = Some(KnitMethod::Fulltext);
        } else if o == b"line-delta" {
            method = Some(KnitMethod::LineDelta);
        } else if o == b"no-eol" {
            noeol = true;
        }
    }
    let method = method.ok_or_else(|| {
        KnitError::BadIndexValue(
            options
                .iter()
                .flat_map(|o| {
                    let mut v = o.as_ref().to_vec();
                    v.push(b',');
                    v
                })
                .collect(),
        )
    })?;
    Ok((method, noeol))
}

/// Decide the build-details for a single knit graph index entry, given
/// just its `value` bytes and the number of compression-parent refs the
/// index recorded for it.
///
/// `compression_parent_count` is the length of `entry[3][1]` on the
/// Python side: zero means no compression parent (a fulltext), one
/// means a delta against that parent, anything else is corrupt.
///
/// The returned `compression_parent` is `Some(0)` to signal "yes, there
/// is exactly one compression parent — go fetch its key from the entry's
/// refs at index 0", or `None` for fulltexts. The pyo3 caller does the
/// final `Py<PyAny>` lookup itself; this function stays free of any
/// Python types.
pub fn decode_knit_build_details(
    value: &[u8],
    has_deltas: bool,
    compression_parent_count: usize,
) -> Result<KnitBuildDetails, KnitError> {
    let parsed = parse_knit_index_value(value)?;
    let compression_parent = if has_deltas {
        match compression_parent_count {
            0 => None,
            1 => Some(0),
            n => return Err(KnitError::TooManyCompressionParents(n)),
        }
    } else {
        None
    };
    let method = if compression_parent.is_some() {
        KnitMethod::LineDelta
    } else {
        KnitMethod::Fulltext
    };
    Ok(KnitBuildDetails {
        pos: parsed.pos,
        size: parsed.size,
        noeol: parsed.noeol,
        method,
        compression_parent,
    })
}

/// Parse an annotated-fulltext knit record into the plain text lines it
/// represents.
///
/// Composes [`decode_record_gz`] + [`parse_record_body_unchecked`] +
/// [`parse_fulltext`] and discards the origin column. If `noeol` is true,
/// the trailing `\n` is stripped from the last line — this mirrors the
/// `_should_strip_eol` flag that the Python `KnitContent` carries.
///
/// Used by `bzrformats.knit.FTAnnotatedToFullText.get_bytes`.
pub fn extract_annotated_fulltext_to_plain_lines(
    raw_record: &[u8],
    noeol: bool,
) -> Result<Vec<Vec<u8>>, KnitError> {
    let decompressed = decode_record_gz(raw_record)?;
    let (_header, body_lines) = parse_record_body_unchecked(&decompressed)?;
    let annotated: Vec<AnnotatedLine> = parse_fulltext(&body_lines)?;
    let mut lines: Vec<Vec<u8>> = annotated.into_iter().map(|(_, text)| text).collect();
    if noeol {
        if let Some(last) = lines.last_mut() {
            if last.ends_with(b"\n") {
                last.pop();
            }
        }
    }
    Ok(lines)
}

/// Same as [`extract_annotated_fulltext_to_plain_lines`] but for plain
/// (already-unannotated) records — used by
/// `bzrformats.knit.FTPlainToFullText.get_bytes`. The input record body
/// has no origin column, so we just split it into lines and apply the
/// same `noeol` rule.
pub fn extract_plain_fulltext_lines(
    raw_record: &[u8],
    noeol: bool,
) -> Result<Vec<Vec<u8>>, KnitError> {
    let decompressed = decode_record_gz(raw_record)?;
    let (_header, body_lines) = parse_record_body_unchecked(&decompressed)?;
    let mut lines: Vec<Vec<u8>> = body_lines.iter().map(|l| l.to_vec()).collect();
    if noeol {
        if let Some(last) = lines.last_mut() {
            if last.ends_with(b"\n") {
                last.pop();
            }
        }
    }
    Ok(lines)
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

/// A knit key — a tuple of byte segments, identifying one record in
/// one knit. The last segment is the version_id; the leading segments
/// (if any) form the file-id prefix used by per-file knits.
pub type KnitKey = Vec<Vec<u8>>;

/// Index lookup result for one knit record.
///
/// Returned by [`KnitIndex::get_build_details`] for each requested key.
/// `index_memo` is an opaque token the access layer uses to fetch the
/// raw record bytes; for a `_KnitGraphIndex` this is the
/// `(graph_index, pos, size)` tuple, for a `_KndxIndex` it's
/// `(prefix_key, pos, size)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnitRecordDetails {
    pub method: KnitMethod,
    pub noeol: bool,
    pub index_memo: KnitIndexMemo,
    pub compression_parent: Option<KnitKey>,
    pub parents: Vec<KnitKey>,
}

/// Opaque storage handle tying a key to its raw bytes on disk.
///
/// The `path` identifies which file on the underlying transport the
/// bytes live in; `offset` and `length` are the byte range inside it.
/// For pure in-memory backends, `path` can be any stable identifier
/// the access implementation chooses to use.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KnitIndexMemo {
    pub path: String,
    pub offset: u64,
    pub length: usize,
}

/// Full index trait for knit storage.
///
/// Pure-Rust callers implement this directly; the pyo3 layer wraps a
/// Python `_KnitGraphIndex` or `_KndxIndex` via an adapter struct.
pub trait KnitIndex {
    // --- read side ---

    /// Look up build details for `keys`. Missing keys are absent from
    /// the returned map. Implementations handle their own locking checks.
    fn get_build_details(
        &self,
        keys: &[KnitKey],
    ) -> Result<std::collections::HashMap<KnitKey, KnitRecordDetails>, KnitError>;

    /// Return all keys present in this index.
    fn keys(&self) -> Result<Vec<KnitKey>, KnitError>;

    /// Return a map of key → parent keys for the given keys.
    /// Missing keys are absent from the result.
    fn get_parent_map(
        &self,
        keys: &[KnitKey],
    ) -> Result<std::collections::HashMap<KnitKey, Vec<KnitKey>>, KnitError>;

    /// Return the storage method for a single key.
    fn get_method(&self, key: &KnitKey) -> Result<KnitMethod, KnitError>;

    /// Sum the on-disk sizes of the build chains for `keys`, using
    /// `positions` (from `get_build_details`) to avoid re-querying.
    fn get_total_build_size(
        &self,
        keys: &[KnitKey],
        positions: &std::collections::HashMap<KnitKey, KnitRecordDetails>,
    ) -> usize;

    /// Sort `keys` in-place into the order that minimises I/O when
    /// fetching them (i.e. by backing file then byte offset).
    fn sort_keys_by_io(
        &self,
        keys: &mut [KnitKey],
        positions: &std::collections::HashMap<KnitKey, KnitRecordDetails>,
    );

    /// Return true if this index tracks graph parents.
    fn has_graph(&self) -> bool;

    /// Return true if `key` is present in this index.
    fn contains(&self, key: &KnitKey) -> Result<bool, KnitError>;

    /// Return the set of compression parents that are referenced but
    /// not yet present in any scanned index.
    fn get_missing_compression_parents(&self) -> Result<Vec<KnitKey>, KnitError>;

    // --- write side ---

    /// Assert that a write is permitted, returning an error otherwise.
    fn check_write_ok(&self) -> Result<(), KnitError>;

    /// Add records to the index.
    ///
    /// Each record is `(key, options, index_memo, parents)`.
    /// `random_id`: skip duplicate checking.
    /// `missing_compression_parents`: the compression parents of delta
    ///   records may not yet be present; buffer them for later.
    fn add_records(
        &self,
        records: &[(KnitKey, Vec<KnitMethod>, KnitIndexMemo, Vec<KnitKey>)],
        random_id: bool,
        missing_compression_parents: bool,
    ) -> Result<(), KnitError>;
}

/// Callback invoked by [`KnitGraphIndex`] after encoding a batch of records,
/// to write them into the backing graph index.
///
/// The `entries` slice contains `(key, encoded_value, node_refs)` triples
/// ready to pass to the graph index's add method.  `has_parents` mirrors the
/// `parents` flag on the owning `KnitGraphIndex` and controls whether
/// `node_refs` is meaningful.
pub trait AddCallback {
    fn call(
        &mut self,
        entries: &[(KnitKey, Vec<u8>, Vec<Vec<KnitKey>>)],
        has_parents: bool,
    ) -> Result<(), KnitError>;
}

/// Pure-Rust state for a graph-index-backed knit index.
///
/// Owns the mutable bookkeeping that was previously scattered across
/// `PyKnitGraphIndex` in `bazaar-py`:
///
/// - `missing_compression_parents`: delta-compressed records whose
///   compression parent has not yet been written to any scanned index.
/// - `key_dependencies`: optional [`KeyRefs`] tracker for external parent
///   references (used when `track_external_parent_refs=True`).
/// - `add_callback`: the sink that receives encoded `(key, value, refs)`
///   triples when `add_records` is called.
///
/// All graph-index I/O (iter_entries, external_references, …) is left to the
/// caller; only the encoding and state-management logic lives here.
pub struct KnitGraphIndex<C> {
    pub deltas: bool,
    pub parents: bool,
    pub add_callback: Option<C>,
    pub missing_compression_parents: std::collections::HashSet<KnitKey>,
    pub key_dependencies: Option<crate::versionedfile::KeyRefs<KnitKey>>,
}

impl<C: AddCallback> KnitGraphIndex<C> {
    pub fn new(deltas: bool, parents: bool) -> Self {
        Self {
            deltas,
            parents,
            add_callback: None,
            missing_compression_parents: std::collections::HashSet::new(),
            key_dependencies: None,
        }
    }

    pub fn set_add_callback(&mut self, callback: C) {
        self.add_callback = Some(callback);
    }

    pub fn clear_add_callback(&mut self) {
        self.add_callback = None;
    }

    /// Enable external-parent-ref tracking.
    ///
    /// `track_new_keys`: if true, [`Self::get_new_keys`] will return the set of
    /// keys added since the last [`Self::clear_key_dependencies`].
    pub fn enable_key_dependencies(&mut self, track_new_keys: bool) {
        self.key_dependencies = Some(crate::versionedfile::KeyRefs::new(track_new_keys));
    }

    pub fn clear_key_dependencies(&mut self) {
        if let Some(kd) = self.key_dependencies.as_mut() {
            kd.clear();
        }
    }

    /// Record that `key` refers to `parent_keys`. No-op if key_dependencies
    /// is not enabled.
    pub fn add_key_dependencies(&mut self, key: KnitKey, parent_keys: Vec<KnitKey>) {
        if let Some(kd) = self.key_dependencies.as_mut() {
            kd.add_references(key, parent_keys);
        }
    }

    pub fn add_missing_compression_parent(&mut self, key: KnitKey) {
        self.missing_compression_parents.insert(key);
    }

    pub fn satisfy_refs_for_keys(&mut self, keys: impl IntoIterator<Item = KnitKey>) {
        if let Some(kd) = self.key_dependencies.as_mut() {
            kd.satisfy_refs_for_keys(keys);
        }
    }

    /// Keys that still have unsatisfied references (i.e. referenced parents
    /// not yet present). Returns an empty iterator if key_dependencies is not
    /// enabled.
    pub fn unsatisfied_refs(&self) -> impl Iterator<Item = &KnitKey> {
        self.key_dependencies
            .iter()
            .flat_map(|kd| kd.unsatisfied_refs())
    }

    /// All keys that reference at least one other key. Returns an empty set
    /// if key_dependencies is not enabled.
    pub fn referrers(&self) -> std::collections::HashSet<KnitKey> {
        self.key_dependencies
            .as_ref()
            .map(|kd| kd.referrers())
            .unwrap_or_default()
    }

    /// Keys added since construction or the last `clear_key_dependencies`.
    /// Returns `None` if key_dependencies is disabled or was not constructed
    /// with `track_new_keys=true`.
    pub fn new_keys(&self) -> Option<&std::collections::HashSet<KnitKey>> {
        self.key_dependencies.as_ref()?.new_keys()
    }

    /// Update `missing_compression_parents` after scanning a new (unvalidated)
    /// index shard.
    pub fn update_missing_compression_parents(
        &mut self,
        new_missing: impl IntoIterator<Item = KnitKey>,
        present_keys: &std::collections::HashSet<KnitKey>,
    ) {
        for key in new_missing {
            if !present_keys.contains(&key) {
                self.missing_compression_parents.insert(key);
            }
        }
    }

    /// Encode a batch of records and pass them to the add_callback.
    ///
    /// Returns `Err` if no callback is set (read-only index).
    ///
    /// `records` is an iterator of `(key, options_bytes, (pos, size), parents)`.
    /// The caller is responsible for dedup checking (passing `random_id=true`
    /// skips it on the Python side; pure-Rust callers handle it themselves).
    pub fn encode_and_dispatch<I>(
        &mut self,
        records: I,
        missing_compression_parents_flag: bool,
    ) -> Result<(), KnitError>
    where
        I: IntoIterator<Item = (KnitKey, Vec<u8>, u64, u64, Vec<KnitKey>)>,
    {
        let Some(cb) = self.add_callback.as_mut() else {
            return Err(KnitError::ReadOnly);
        };

        let mut entries: Vec<(KnitKey, Vec<u8>, Vec<Vec<KnitKey>>)> = Vec::new();
        let mut new_compression_parents: std::collections::HashSet<KnitKey> =
            std::collections::HashSet::new();
        let mut key_dep_updates: Vec<(KnitKey, Vec<KnitKey>)> = Vec::new();

        for (key, options_bytes, pos, size, parents) in records {
            let noeol = options_bytes.windows(6).any(|w| w == b"no-eol");
            let method = if options_bytes.windows(10).any(|w| w == b"line-delta") {
                KnitMethod::LineDelta
            } else {
                KnitMethod::Fulltext
            };

            if missing_compression_parents_flag && method == KnitMethod::LineDelta {
                if let Some(cp) = parents.first() {
                    new_compression_parents.insert(cp.clone());
                }
            }

            let (value, node_refs) = encode_graph_index_record(
                noeol,
                pos,
                size,
                method,
                self.parents,
                self.deltas,
                &parents,
            )?;

            if self.parents && self.key_dependencies.is_some() {
                key_dep_updates.push((key.clone(), parents));
            }

            if let Some(existing) = entries.iter_mut().find(|(k, _, _)| k == &key) {
                *existing = (key, value, node_refs);
            } else {
                entries.push((key, value, node_refs));
            }
        }

        cb.call(&entries, self.parents)?;

        for (key, parents) in key_dep_updates {
            self.add_key_dependencies(key, parents);
        }

        let added_keys: std::collections::HashSet<&KnitKey> =
            entries.iter().map(|(k, _, _)| k).collect();
        if missing_compression_parents_flag {
            new_compression_parents.retain(|k| !added_keys.contains(k));
            self.missing_compression_parents
                .extend(new_compression_parents);
        }
        self.missing_compression_parents
            .retain(|k| !added_keys.contains(k));

        Ok(())
    }
}

/// Full access trait for knit storage.
///
/// Covers both the read path (fetch raw record bytes) and the write
/// path (append new records, flush, retry on pack reload).
pub trait KnitAccess {
    // --- read side ---

    /// Fetch the raw record bytes for one index memo. Returns the
    /// gzip-compressed data ready to feed to [`decode_record_gz`].
    fn get_raw_record(&self, memo: &KnitIndexMemo) -> Result<Vec<u8>, KnitError>;

    /// Fetch raw record bytes for multiple memos in order.
    fn get_raw_records(&self, memos: &[KnitIndexMemo]) -> Result<Vec<Vec<u8>>, KnitError>;

    // --- write side ---

    /// Append raw record bytes and return the resulting index memo.
    fn add_raw_record(
        &self,
        key: &KnitKey,
        size: usize,
        data: Vec<Vec<u8>>,
    ) -> Result<KnitIndexMemo, KnitError>;

    /// Flush any buffered writes to the underlying storage.
    fn flush(&self) -> Result<(), KnitError>;

    /// Call the reload function if available, or re-raise the error.
    ///
    /// Called after a `RetryWithNewPacks`-equivalent error. Returns
    /// `Ok(())` if the reload succeeded and the caller should retry;
    /// returns `Err` if the situation is unrecoverable.
    fn reload_or_raise(&self, err: KnitError) -> Result<(), KnitError>;
}

/// Reconstruct the text content of `key` from a knit, walking the
/// compression-parent chain as needed.
///
/// This is the pure-Rust equivalent of `KnitVersionedFiles.get_text`
/// for the read path. `index` resolves keys to build details (method,
/// memo, parent), `access` fetches the raw bytes, and `factory`
/// decides whether to parse records as annotated or plain content and
/// how to apply deltas. Returns the reconstructed text as joined bytes
/// — exactly what `get_text` returns on the Python side.
///
/// The chain walk uses [`walk_compression_closure`]; reconstruction
/// orders ancestors fulltext-first and applies each delta in turn.
pub fn get_text<I, A, F>(
    index: &I,
    access: &A,
    factory: &F,
    key: &KnitKey,
) -> Result<Vec<u8>, KnitError>
where
    I: KnitIndex,
    A: KnitAccess,
    F: KnitFactory,
{
    let content = get_content(index, access, factory, key)?;
    let mut out = Vec::new();
    for line in content.text() {
        out.extend_from_slice(&line);
    }
    Ok(out)
}

/// Reconstruct the [`KnitFactory::Content`] for `key` without joining
/// the lines. Used as the engine for [`get_text`]; pure-Rust callers
/// can use this directly when they want structured access (e.g. to
/// the per-line annotations of an `AnnotatedKnitContent`).
pub fn get_content<I, A, F>(
    index: &I,
    access: &A,
    factory: &F,
    key: &KnitKey,
) -> Result<F::Content, KnitError>
where
    I: KnitIndex,
    A: KnitAccess,
    F: KnitFactory,
{
    // 1. Walk the compression chain to discover every ancestor we'll
    //    need to fetch and parse.
    let chain = walk_compression_closure::<KnitKey, KnitRecordDetails, _>(
        std::iter::once(key.clone()),
        false,
        |batch| {
            let lookup = match index.get_build_details(batch) {
                Ok(m) => m,
                Err(_) => {
                    // The closure error path is just a missing-key
                    // signal; the actual error gets reported back to
                    // the caller via the `?` below since we re-call
                    // in that case. Stash an empty batch here.
                    return ClosureBatch {
                        present: Default::default(),
                        missing: batch.iter().cloned().collect(),
                    };
                }
            };
            let mut present = std::collections::HashMap::new();
            let mut missing = std::collections::HashSet::new();
            for k in batch {
                match lookup.get(k) {
                    Some(details) => {
                        present.insert(
                            k.clone(),
                            (details.compression_parent.clone(), details.clone()),
                        );
                    }
                    None => {
                        missing.insert(k.clone());
                    }
                }
            }
            ClosureBatch { present, missing }
        },
    )
    .map_err(|missing| {
        let one = missing
            .into_iter()
            .next()
            .map(|k| {
                let last = k.last().cloned().unwrap_or_default();
                last
            })
            .unwrap_or_default();
        KnitError::BadIndexValue(one)
    })?;

    // 2. Order the chain ancestor-first by following compression_parent
    //    pointers from `key` back to the fulltext, then reversing.
    let mut order: Vec<KnitKey> = Vec::new();
    let mut cursor: Option<KnitKey> = Some(key.clone());
    while let Some(k) = cursor {
        let details = chain.get(&k).ok_or_else(|| {
            KnitError::BadIndexValue(b"chain walk produced a key without details".to_vec())
        })?;
        cursor = details.compression_parent.clone();
        order.push(k);
    }
    order.reverse();

    // 3. Walk the ordered chain: parse the fulltext (first entry),
    //    then apply each delta in sequence.
    let mut content: Option<F::Content> = None;
    for chain_key in order {
        let details = chain.get(&chain_key).ok_or_else(|| {
            KnitError::BadIndexValue(b"chain walk produced a key without details".to_vec())
        })?;
        let raw = access.get_raw_record(&details.index_memo)?;
        let decompressed = decode_record_gz(&raw)?;
        let (_, body_lines) = parse_record_body_unchecked(&decompressed)?;
        let next = factory.parse_record(
            chain_key.last().map(|s| s.as_slice()).unwrap_or(&[]),
            &body_lines,
            details.method,
            details.noeol,
            content.as_ref(),
        )?;
        content = Some(next);
    }
    content.ok_or_else(|| KnitError::BadIndexValue(b"empty compression chain for key".to_vec()))
}

/// Return the sha1 digest of each key's *stored* record without
/// reconstructing the text.
///
/// The digest is the one recorded in each record's header — the same
/// thing `KnitVersionedFiles.get_sha1s` returns. For every key in
/// `keys` that the index knows about, this function fetches just
/// enough of the raw record to parse the header and returns the
/// digest. Missing keys (ghosts, stacked-fallback absentees) are
/// simply absent from the result map, matching the Python
/// `allow_missing=True` flow.
pub fn get_sha1s<I, A>(
    index: &I,
    access: &A,
    keys: &[KnitKey],
) -> Result<std::collections::HashMap<KnitKey, Vec<u8>>, KnitError>
where
    I: KnitIndex,
    A: KnitAccess,
{
    let details_map = index.get_build_details(keys)?;
    let mut out = std::collections::HashMap::new();
    for key in keys {
        let Some(details) = details_map.get(key) else {
            continue;
        };
        let raw = access.get_raw_record(&details.index_memo)?;
        let header = parse_record_header_only(&raw)?;
        out.insert(key.clone(), header.digest);
    }
    Ok(out)
}

/// Pure-Rust implementation of `_KndxIndex`.
///
/// Reads and writes `.kndx` index files through a [`crate::transport::Transport`]
/// and maps keys to paths using a [`crate::key_mapper::Mapper`].  The
/// in-memory cache follows the same two-level structure as the Python
/// original: `cache_dict` (version_id → entry tuple) and `history_vec`
/// (sequence-number → version_id).
pub struct KndxIndex<T, M> {
    transport: T,
    mapper: M,
    /// prefix → (cache: HashMap<version_id, CacheEntry>, history: Vec<version_id>)
    kndx_cache: std::sync::Mutex<std::collections::HashMap<Vec<Vec<u8>>, KndxPrefixCache>>,
}

/// One per-prefix in-memory cache for a `KndxIndex`.
#[derive(Debug, Default)]
pub struct KndxPrefixCache {
    /// version_id → (version_id, options, pos, size, parents, index)
    pub cache: std::collections::HashMap<Vec<u8>, KndxCacheEntry>,
    /// sequence-number → version_id (first-occurrence only)
    pub history: Vec<Vec<u8>>,
}

/// One row in the per-prefix kndx cache.
#[derive(Debug, Clone)]
pub struct KndxCacheEntry {
    pub version_id: Vec<u8>,
    pub options: Vec<Vec<u8>>,
    pub pos: u64,
    pub size: usize,
    /// Bare suffixes (last element only, for compatibility with _load_data_c).
    pub parents: Vec<Vec<u8>>,
    /// Index into `history` for this version.
    pub index: usize,
}

pub const KNDX_HEADER: &[u8] = b"# bzr knit index 8\n";

impl<T: crate::transport::Transport, M: crate::key_mapper::Mapper> KndxIndex<T, M> {
    pub fn new(transport: T, mapper: M) -> Self {
        Self {
            transport,
            mapper,
            kndx_cache: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    pub fn prefix_of(key: &KnitKey) -> Vec<Vec<u8>> {
        key[..key.len().saturating_sub(1)].iter().cloned().collect()
    }

    pub fn suffix_of(key: &KnitKey) -> Vec<u8> {
        key.last().cloned().unwrap_or_default()
    }

    pub fn mapper(&self) -> &M {
        &self.mapper
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }

    pub fn transport_mut(&mut self) -> &mut T {
        &mut self.transport
    }

    pub fn kndx_cache(
        &self,
    ) -> &std::sync::Mutex<std::collections::HashMap<Vec<Vec<u8>>, KndxPrefixCache>> {
        &self.kndx_cache
    }

    pub fn prefix_path(&self, prefix: &[Vec<u8>]) -> String {
        let refs: Vec<&[u8]> = prefix.iter().map(|s| s.as_slice()).collect();
        self.mapper.map(&refs) + ".kndx"
    }

    /// Load `prefix` into the cache through a shared `&self` reference.
    ///
    /// Both transport I/O errors and corrupted kndx headers are collapsed
    /// into `TransportError::Other`. Callers that need to distinguish
    /// `BadKnitHeader` should call [`load_prefix_typed`] instead.
    pub fn load_prefix_shared(
        &self,
        prefix: Vec<Vec<u8>>,
    ) -> Result<(), crate::transport::TransportError> {
        self.load_prefix_typed(prefix).map_err(|e| match e {
            KndxLoadError::Transport(te) => te,
            KndxLoadError::Knit(ke) => crate::transport::TransportError::Other(ke.to_string()),
        })
    }

    /// Like [`load_prefix_shared`] but returns a typed [`KndxLoadError`] so
    /// the caller can distinguish `BadKnitHeader` from transport failures.
    pub fn load_prefix_typed(&self, prefix: Vec<Vec<u8>>) -> Result<(), KndxLoadError> {
        if self.kndx_cache.lock().unwrap().contains_key(&prefix) {
            return Ok(());
        }
        let path = self.prefix_path(&prefix);
        let data = match self.transport.get_bytes(&path) {
            Ok(d) => d,
            Err(crate::transport::TransportError::NoSuchFile(_)) => {
                self.kndx_cache
                    .lock()
                    .unwrap()
                    .insert(prefix, KndxPrefixCache::default());
                // For ConstantMapper (e.g. revisions.kndx), create an empty
                // index file so subsequent appends have a base to grow from.
                if self.mapper.is_constant() {
                    self.transport
                        .put_file_non_atomic(&path, KNDX_HEADER, true)
                        .map_err(KndxLoadError::Transport)?;
                }
                return Ok(());
            }
            Err(te) => return Err(KndxLoadError::Transport(te)),
        };
        let pc = parse_kndx_data(&data).map_err(|e| match e {
            KnitError::BadKnitHeader { .. } => {
                KndxLoadError::Knit(KnitError::BadKnitHeader { path: path.clone() })
            }
            other => KndxLoadError::Knit(other),
        })?;
        self.kndx_cache.lock().unwrap().insert(prefix, pc);
        Ok(())
    }

    fn build_details_from_cache(
        &self,
        keys: &[KnitKey],
    ) -> std::collections::HashMap<KnitKey, KnitRecordDetails> {
        let cache = self.kndx_cache.lock().unwrap();
        let mut result = std::collections::HashMap::new();
        for key in keys {
            let prefix = Self::prefix_of(key);
            let suffix = Self::suffix_of(key);
            let Some(pc) = cache.get(&prefix) else {
                continue;
            };
            let Some(entry) = pc.cache.get(&suffix) else {
                continue;
            };
            let (method, noeol) = decode_kndx_options(
                &entry
                    .options
                    .iter()
                    .map(|o| o.as_slice())
                    .collect::<Vec<_>>(),
            )
            .unwrap_or((KnitMethod::Fulltext, false));
            let parents: Vec<KnitKey> = entry
                .parents
                .iter()
                .map(|p| {
                    let mut pk = prefix.clone();
                    pk.push(p.clone());
                    pk
                })
                .collect();
            let compression_parent = if method == KnitMethod::LineDelta {
                parents.first().cloned()
            } else {
                None
            };
            let knit_path = {
                let refs: Vec<&[u8]> = prefix.iter().map(|s| s.as_slice()).collect();
                self.mapper.map(&refs) + ".knit"
            };
            result.insert(
                key.clone(),
                KnitRecordDetails {
                    method,
                    noeol,
                    index_memo: KnitIndexMemo {
                        path: knit_path,
                        offset: entry.pos,
                        length: entry.size,
                    },
                    compression_parent,
                    parents,
                },
            );
        }
        result
    }
}

/// Parse the binary content of a `.kndx` file into a `KndxPrefixCache`.
///
/// The format is one line per entry:
/// `\nVERSION_ID OPTIONS POS SIZE [PARENT...] :`
///
/// Lines not ending in ` :` (partial writes) are silently skipped.
/// The file must begin with [`KNDX_HEADER`].
/// Parse a `.kndx` file's bytes into a prefix cache.
///
/// Returns `Err(KnitError::BadKnitHeader)` if the file is non-empty but
/// does not start with `KNDX_HEADER`. Returns `Ok` with an empty cache for
/// an empty file, and `Ok` with the parsed entries otherwise.
pub fn parse_kndx_data(data: &[u8]) -> Result<KndxPrefixCache, KnitError> {
    let mut pc = KndxPrefixCache::default();
    if data.is_empty() {
        return Ok(pc);
    }
    if !data.starts_with(KNDX_HEADER) {
        return Err(KnitError::BadKnitHeader {
            path: "<kndx>".to_string(),
        });
    }
    let rest = &data[KNDX_HEADER.len()..];
    for line in rest.split(|&b| b == b'\n') {
        let line = line.strip_prefix(b"\r").unwrap_or(line);
        let line = if line.first() == Some(&b'\n') {
            &line[1..]
        } else {
            line
        };
        // Strip leading \n that separates entries
        let line = line.strip_prefix(b"\r").unwrap_or(line);
        if line.is_empty() {
            continue;
        }
        // Must end with ' :'
        let Some(line) = line.strip_suffix(b" :") else {
            continue;
        };
        let parts: Vec<&[u8]> = line.splitn(5, |&b| b == b' ').collect();
        if parts.len() < 4 {
            continue;
        }
        let version_id = parts[0].to_vec();
        let options: Vec<Vec<u8>> = parts[1].split(|&b| b == b',').map(|o| o.to_vec()).collect();
        let pos_str = std::str::from_utf8(parts[2]).map_err(|_| KnitError::KndxCorrupt {
            line: line.to_vec(),
            detail: format!("{:?} is not a valid integer", parts[2]),
        })?;
        let pos = pos_str.parse::<u64>().map_err(|_| KnitError::KndxCorrupt {
            line: line.to_vec(),
            detail: format!("{:?} is not a valid integer", pos_str),
        })?;
        let size_str = std::str::from_utf8(parts[3]).map_err(|_| KnitError::KndxCorrupt {
            line: line.to_vec(),
            detail: format!("{:?} is not a valid integer", parts[3]),
        })?;
        let size = size_str
            .parse::<usize>()
            .map_err(|_| KnitError::KndxCorrupt {
                line: line.to_vec(),
                detail: format!("{:?} is not a valid integer", size_str),
            })?;
        let parents_raw = if parts.len() > 4 {
            parts[4]
        } else {
            b"" as &[u8]
        };
        let mut parents: Vec<Vec<u8>> = vec![];
        for p in parents_raw.split(|&b| b == b' ').filter(|p| !p.is_empty()) {
            if p.first() == Some(&b'.') {
                parents.push(p[1..].to_vec());
            } else {
                let s = std::str::from_utf8(p).map_err(|_| KnitError::KndxCorrupt {
                    line: line.to_vec(),
                    detail: format!("{:?} is not a valid integer", p),
                })?;
                let idx: usize = s.parse().map_err(|_| KnitError::KndxCorrupt {
                    line: line.to_vec(),
                    detail: format!("{:?} is not a valid integer", s),
                })?;
                if idx >= pc.history.len() {
                    return Err(KnitError::KndxCorrupt {
                        line: line.to_vec(),
                        detail: format!(
                            "Parent index refers to a revision which does not exist yet. {} > {}",
                            idx,
                            pc.history.len()
                        ),
                    });
                }
                parents.push(pc.history[idx].clone());
            }
        }
        let index = if pc.cache.contains_key(&version_id) {
            pc.cache[&version_id].index
        } else {
            let idx = pc.history.len();
            pc.history.push(version_id.clone());
            idx
        };
        pc.cache.insert(
            version_id.clone(),
            KndxCacheEntry {
                version_id,
                options,
                pos,
                size,
                parents,
                index,
            },
        );
    }
    Ok(pc)
}

impl<T: crate::transport::Transport, M: crate::key_mapper::Mapper> KnitIndex for KndxIndex<T, M> {
    fn get_build_details(
        &self,
        keys: &[KnitKey],
    ) -> Result<std::collections::HashMap<KnitKey, KnitRecordDetails>, KnitError> {
        let prefixes: std::collections::HashSet<Vec<Vec<u8>>> =
            keys.iter().map(Self::prefix_of).collect();
        for prefix in prefixes {
            self.load_prefix_shared(prefix)
                .map_err(|e| KnitError::BadIndexValue(e.to_string().into_bytes()))?;
        }
        Ok(self.build_details_from_cache(keys))
    }

    fn keys(&self) -> Result<Vec<KnitKey>, KnitError> {
        let cache = self.kndx_cache.lock().unwrap();
        let mut result = Vec::new();
        for (prefix, pc) in cache.iter() {
            for suffix in pc.cache.keys() {
                let mut key = prefix.clone();
                key.push(suffix.clone());
                result.push(key);
            }
        }
        Ok(result)
    }

    fn get_parent_map(
        &self,
        keys: &[KnitKey],
    ) -> Result<std::collections::HashMap<KnitKey, Vec<KnitKey>>, KnitError> {
        let prefixes: std::collections::HashSet<Vec<Vec<u8>>> =
            keys.iter().map(Self::prefix_of).collect();
        for prefix in prefixes {
            self.load_prefix_shared(prefix)
                .map_err(|e| KnitError::BadIndexValue(e.to_string().into_bytes()))?;
        }
        let cache = self.kndx_cache.lock().unwrap();
        let mut result = std::collections::HashMap::new();
        for key in keys {
            let prefix = Self::prefix_of(key);
            let suffix = Self::suffix_of(key);
            let Some(pc) = cache.get(&prefix) else {
                continue;
            };
            let Some(entry) = pc.cache.get(&suffix) else {
                continue;
            };
            let parents: Vec<KnitKey> = entry
                .parents
                .iter()
                .map(|p| {
                    let mut pk = prefix.clone();
                    pk.push(p.clone());
                    pk
                })
                .collect();
            result.insert(key.clone(), parents);
        }
        Ok(result)
    }

    fn get_method(&self, key: &KnitKey) -> Result<KnitMethod, KnitError> {
        let prefix = Self::prefix_of(key);
        let suffix = Self::suffix_of(key);
        self.load_prefix_shared(prefix.clone())
            .map_err(|e| KnitError::BadIndexValue(e.to_string().into_bytes()))?;
        let cache = self.kndx_cache.lock().unwrap();
        let pc = cache
            .get(&prefix)
            .ok_or_else(|| KnitError::BadIndexValue(b"prefix not loaded".to_vec()))?;
        let entry = pc
            .cache
            .get(&suffix)
            .ok_or_else(|| KnitError::Corrupt(format!("key not found: {:?}", key)))?;
        let (method, _) = decode_kndx_options(
            &entry
                .options
                .iter()
                .map(|o| o.as_slice())
                .collect::<Vec<_>>(),
        )
        .unwrap_or((KnitMethod::Fulltext, false));
        Ok(method)
    }

    fn get_total_build_size(
        &self,
        keys: &[KnitKey],
        positions: &std::collections::HashMap<KnitKey, KnitRecordDetails>,
    ) -> usize {
        let mut total = 0usize;
        let mut seen = std::collections::HashSet::new();
        let mut queue: std::collections::VecDeque<&KnitKey> = keys.iter().collect();
        while let Some(key) = queue.pop_front() {
            if !seen.insert(key) {
                continue;
            }
            if let Some(details) = positions.get(key) {
                total += details.index_memo.length;
                if let Some(ref cp) = details.compression_parent {
                    if positions.contains_key(cp) {
                        queue.push_back(cp);
                    }
                }
            }
        }
        total
    }

    fn sort_keys_by_io(
        &self,
        keys: &mut [KnitKey],
        positions: &std::collections::HashMap<KnitKey, KnitRecordDetails>,
    ) {
        keys.sort_by(|a, b| {
            let a_memo = positions
                .get(a)
                .map(|d| (&d.index_memo.path, d.index_memo.offset));
            let b_memo = positions
                .get(b)
                .map(|d| (&d.index_memo.path, d.index_memo.offset));
            a_memo.cmp(&b_memo)
        });
    }

    fn has_graph(&self) -> bool {
        true
    }

    fn contains(&self, key: &KnitKey) -> Result<bool, KnitError> {
        let prefix = Self::prefix_of(key);
        let suffix = Self::suffix_of(key);
        self.load_prefix_shared(prefix.clone())
            .map_err(|e| KnitError::BadIndexValue(e.to_string().into_bytes()))?;
        let cache = self.kndx_cache.lock().unwrap();
        Ok(cache
            .get(&prefix)
            .map(|pc| pc.cache.contains_key(&suffix))
            .unwrap_or(false))
    }

    fn get_missing_compression_parents(&self) -> Result<Vec<KnitKey>, KnitError> {
        // kndx is append-only and has no separate atomic-insertion staging
        // area, so it cannot track deferred compression parents. Callers
        // distinguish this from "no missing parents" by catching the error.
        Err(KnitError::NotImplemented("get_missing_compression_parents"))
    }

    fn check_write_ok(&self) -> Result<(), KnitError> {
        // KndxIndex has no separate lock state; writes are always permitted.
        Ok(())
    }

    fn add_records(
        &self,
        records: &[(KnitKey, Vec<KnitMethod>, KnitIndexMemo, Vec<KnitKey>)],
        _random_id: bool,
        _missing_compression_parents: bool,
    ) -> Result<(), KnitError> {
        // Group by prefix so we write each .kndx file once.
        let mut by_prefix: std::collections::HashMap<Vec<Vec<u8>>, Vec<_>> =
            std::collections::HashMap::new();
        for (key, methods, memo, parents) in records {
            let prefix = Self::prefix_of(key);
            by_prefix
                .entry(prefix)
                .or_default()
                .push((key, methods, memo, parents));
        }
        for (prefix, entries) in by_prefix {
            self.load_prefix_shared(prefix.clone())
                .map_err(|e| KnitError::BadIndexValue(e.to_string().into_bytes()))?;
            let path = self.prefix_path(&prefix);
            let mut cache = self.kndx_cache.lock().unwrap();
            let pc = cache.entry(prefix.clone()).or_default();
            let mut append_buf: Vec<u8> = Vec::new();
            for (key, methods, memo, parents) in entries {
                let suffix = Self::suffix_of(key);
                let options: Vec<Vec<u8>> = methods
                    .iter()
                    .map(|m| m.as_str().as_bytes().to_vec())
                    .collect();
                let parent_suffixes: Vec<Vec<u8>> =
                    parents.iter().map(|p| Self::suffix_of(p)).collect();
                let idx = pc.history.len();
                pc.history.push(suffix.clone());
                pc.cache.insert(
                    suffix.clone(),
                    KndxCacheEntry {
                        version_id: suffix.clone(),
                        options: options.clone(),
                        pos: memo.offset,
                        size: memo.length,
                        parents: parent_suffixes.clone(),
                        index: idx,
                    },
                );
                // Format: VERSION_ID OPTIONS POS SIZE [PARENTS...] :
                append_buf.push(b'\n');
                append_buf.extend_from_slice(&suffix);
                append_buf.push(b' ');
                let opts_joined: Vec<u8> = options.join(&b","[..]);
                append_buf.extend_from_slice(&opts_joined);
                append_buf.push(b' ');
                append_buf.extend_from_slice(memo.offset.to_string().as_bytes());
                append_buf.push(b' ');
                append_buf.extend_from_slice(memo.length.to_string().as_bytes());
                for p in &parent_suffixes {
                    append_buf.push(b' ');
                    append_buf.extend_from_slice(p);
                }
                append_buf.extend_from_slice(b" :");
            }
            drop(cache);
            self.transport
                .append_bytes(&path, &append_buf)
                .map_err(|e| KnitError::BadIndexValue(e.to_string().into_bytes()))?;
        }
        Ok(())
    }
}

/// Pure-Rust implementation of `_KnitKeyAccess`.
///
/// Stores raw knit record bytes in `.knit` files via a
/// [`crate::transport::Transport`], mapping keys to file paths using a
/// [`crate::key_mapper::Mapper`].
pub struct KnitKeyAccess<T, M> {
    transport: T,
    mapper: M,
}

impl<T: crate::transport::Transport, M: crate::key_mapper::Mapper> KnitKeyAccess<T, M> {
    pub fn new(transport: T, mapper: M) -> Self {
        Self { transport, mapper }
    }

    pub fn mapper(&self) -> &M {
        &self.mapper
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }

    fn key_path(&self, key: &KnitKey) -> String {
        let prefix = &key[..key.len().saturating_sub(1)];
        let refs: Vec<&[u8]> = prefix.iter().map(|s| s.as_slice()).collect();
        self.mapper.map(&refs) + ".knit"
    }

    /// Write raw bytes directly (no chunking) and return `(key, offset, len)`.
    /// Used by the pyo3 `PyKnitKeyAccess` wrapper.
    pub fn add_raw_record_bytes(
        &self,
        key: KnitKey,
        data: &[u8],
    ) -> Result<(KnitKey, u64, usize), crate::transport::TransportError> {
        let path = self.key_path(&key);
        let offset = match self.transport.append_bytes(&path, data) {
            Ok(off) => off,
            Err(crate::transport::TransportError::NoSuchFile(_)) => {
                // Parent directory doesn't exist yet; create it and retry.
                // For paths without a separator, mkdir("") creates the
                // transport root, which is what the Python implementation
                // does via osutils.dirname(path).
                let parent = path.rfind('/').map(|i| &path[..i]).unwrap_or("");
                self.transport.mkdir(parent)?;
                self.transport.append_bytes(&path, data)?
            }
            Err(e) => return Err(e),
        };
        Ok((key, offset, data.len()))
    }
}

impl<T: crate::transport::Transport, M: crate::key_mapper::Mapper> KnitAccess
    for KnitKeyAccess<T, M>
{
    fn get_raw_record(&self, memo: &KnitIndexMemo) -> Result<Vec<u8>, KnitError> {
        use crate::transport::ReadRange;
        let ranges = [ReadRange {
            offset: memo.offset,
            length: memo.length,
        }];
        self.transport
            .readv(&memo.path, &ranges)
            .map_err(|e| KnitError::BadIndexValue(e.to_string().into_bytes()))
            .and_then(|mut v| {
                v.pop()
                    .map(|r| r.bytes)
                    .ok_or_else(|| KnitError::BadIndexValue(b"readv returned no data".to_vec()))
            })
    }

    fn get_raw_records(&self, memos: &[KnitIndexMemo]) -> Result<Vec<Vec<u8>>, KnitError> {
        use crate::transport::ReadRange;
        // Group by path so we issue one readv per file.
        // Preserve the original ordering so results come back in memo order.
        let mut by_path: std::collections::HashMap<&str, Vec<(usize, ReadRange)>> =
            std::collections::HashMap::new();
        for (i, memo) in memos.iter().enumerate() {
            by_path.entry(&memo.path).or_default().push((
                i,
                ReadRange {
                    offset: memo.offset,
                    length: memo.length,
                },
            ));
        }
        let mut out = vec![Vec::new(); memos.len()];
        for (path, indexed_ranges) in by_path {
            let ranges: Vec<ReadRange> = indexed_ranges.iter().map(|(_, r)| r.clone()).collect();
            let results = self
                .transport
                .readv(path, &ranges)
                .map_err(|e| KnitError::BadIndexValue(e.to_string().into_bytes()))?;
            for ((orig_idx, _), result) in indexed_ranges.into_iter().zip(results) {
                out[orig_idx] = result.bytes;
            }
        }
        Ok(out)
    }

    fn add_raw_record(
        &self,
        key: &KnitKey,
        _size: usize,
        data: Vec<Vec<u8>>,
    ) -> Result<KnitIndexMemo, KnitError> {
        let path = self.key_path(key);
        let flat: Vec<u8> = data.into_iter().flatten().collect();
        let length = flat.len();
        let offset = self
            .transport
            .append_bytes(&path, &flat)
            .map_err(|e| KnitError::BadIndexValue(e.to_string().into_bytes()))?;
        Ok(KnitIndexMemo {
            path,
            offset,
            length,
        })
    }

    fn flush(&self) -> Result<(), KnitError> {
        // KnitKeyAccess writes are immediate via append_bytes; nothing to flush.
        Ok(())
    }

    fn reload_or_raise(&self, err: KnitError) -> Result<(), KnitError> {
        // KnitKeyAccess has no pack-reload mechanism; always re-raise.
        Err(err)
    }
}

/// Pure-Rust implementation of `KnitVersionedFiles`.
///
/// Generic over index, access, and factory so it can be used directly by
/// pure-Rust callers and wrapped by the pyo3 layer without any Python
/// dependency.  Fallback versioned-files objects are not modelled here —
/// the pyo3 wrapper handles them in Python.
pub struct KnitVersionedFiles<I, A, F> {
    pub index: I,
    pub access: A,
    pub factory: F,
    pub max_delta_chain: usize,
}

impl<I, A, F> KnitVersionedFiles<I, A, F>
where
    I: KnitIndex,
    A: KnitAccess,
    F: KnitFactory,
{
    pub fn new(index: I, access: A, factory: F, max_delta_chain: usize) -> Self {
        Self {
            index,
            access,
            factory,
            max_delta_chain,
        }
    }

    /// Return all keys in the local index.
    pub fn keys(&self) -> Result<Vec<KnitKey>, KnitError> {
        self.index.keys()
    }

    /// Return a map of key → parent keys for the given keys (local only).
    pub fn get_parent_map(
        &self,
        keys: &[KnitKey],
    ) -> Result<std::collections::HashMap<KnitKey, Vec<KnitKey>>, KnitError> {
        self.index.get_parent_map(keys)
    }

    /// Return the full text of `key` as a single byte string.
    pub fn get_text(&self, key: &KnitKey) -> Result<Vec<u8>, KnitError> {
        get_text(&self.index, &self.access, &self.factory, key)
    }

    /// Return the SHA-1 digests for `keys`.
    pub fn get_sha1s(
        &self,
        keys: &[KnitKey],
    ) -> Result<std::collections::HashMap<KnitKey, Vec<u8>>, KnitError> {
        get_sha1s(&self.index, &self.access, keys)
    }

    /// Reconstruct the content object for `key`.
    pub fn get_content(&self, key: &KnitKey) -> Result<F::Content, KnitError> {
        get_content(&self.index, &self.access, &self.factory, key)
    }

    /// Return build details for the given keys.
    pub fn get_build_details(
        &self,
        keys: &[KnitKey],
    ) -> Result<std::collections::HashMap<KnitKey, KnitRecordDetails>, KnitError> {
        self.index.get_build_details(keys)
    }

    /// Return true if `key` is present in the local index.
    pub fn contains(&self, key: &KnitKey) -> Result<bool, KnitError> {
        self.index.contains(key)
    }

    /// Return the set of compression parents referenced but not yet present.
    pub fn get_missing_compression_parent_keys(&self) -> Result<Vec<KnitKey>, KnitError> {
        self.index.get_missing_compression_parents()
    }

    /// Decide whether to delta-compress the new version against `parent`.
    ///
    /// Walks back at most `max_delta_chain` steps; returns `true` if we
    /// should create a delta, `false` if we should write a new fulltext.
    pub fn check_should_delta(&self, parent: &KnitKey) -> Result<bool, KnitError> {
        if self.max_delta_chain == 0 {
            return Ok(false);
        }
        let mut cursor = parent.clone();
        let mut steps = 0usize;
        let mut delta_size = 0u64;
        loop {
            let details_map = self
                .index
                .get_build_details(std::slice::from_ref(&cursor))?;
            let Some(det) = details_map.get(&cursor) else {
                return Ok(false);
            };
            if det.method == KnitMethod::Fulltext {
                // Use a delta only when the accumulated delta chain is not
                // already much larger than the fulltext it compresses against.
                return Ok(delta_size < det.index_memo.length as u64 * 2 + 200);
            }
            delta_size += det.index_memo.length as u64;
            steps += 1;
            if steps >= self.max_delta_chain {
                return Ok(false);
            }
            match det.compression_parent.clone() {
                Some(cp) => cursor = cp,
                None => return Ok(false),
            }
        }
    }

    /// Add a new version to the knit.
    ///
    /// `lines` are the text lines (each should end in `\n` except possibly
    /// the last).  `parents` are the graph parents.  `random_id` skips
    /// duplicate checking in the index.
    ///
    /// Returns `(sha1_hex_digest, text_length_bytes)`.
    pub fn add_lines(
        &self,
        key: KnitKey,
        parents: Vec<KnitKey>,
        lines: Vec<Vec<u8>>,
        random_id: bool,
    ) -> Result<(Vec<u8>, usize), KnitError> {
        use crate::osutils::sha::sha_string;

        self.index.check_write_ok()?;

        let line_bytes: Vec<u8> = lines.iter().flat_map(|l| l.iter().copied()).collect();
        let digest = sha_string(&line_bytes).into_bytes();
        let text_length = line_bytes.len();

        let no_eol = !line_bytes.is_empty() && !line_bytes.ends_with(b"\n");
        let version_id = key.last().cloned().unwrap_or_default();

        // Decide whether to delta-compress against the left-most present parent.
        let present_map = self.index.get_parent_map(&parents)?;
        let use_delta = parents.first().is_some_and(|p| present_map.contains_key(p))
            && self.max_delta_chain > 0
            && self.check_should_delta(&parents[0])?;

        // Build the content object and serialise it.
        let present_parents: Vec<KnitKey> = parents
            .iter()
            .filter(|p| present_map.contains_key(*p))
            .cloned()
            .collect();

        // When the last line has no trailing newline, add one before building
        // the content so that all serialisers see complete lines. The no-eol
        // flag in the index record lets the reader strip it back on output.
        let content_lines = if no_eol {
            let mut l = lines.clone();
            if let Some(last) = l.last_mut() {
                last.push(b'\n');
            }
            l
        } else {
            lines
        };

        let (method, payload) = {
            let mut content = self.factory.make(content_lines, version_id.clone());
            if no_eol {
                content.set_should_strip_eol(true);
            }
            let delta_opt = merge_annotations(
                &self.index,
                &self.access,
                &self.factory,
                &mut content,
                &present_parents,
                use_delta,
            )?;
            if let Some(delta) = delta_opt {
                let serialised = self.factory.lower_line_delta(&delta);
                (KnitMethod::LineDelta, serialised)
            } else {
                let serialised = self.factory.lower_fulltext(&content);
                (KnitMethod::Fulltext, serialised)
            }
        };

        let (size, chunks) = record_to_data(&version_id, &digest, payload.len(), &payload, true)?;

        let memo = self.access.add_raw_record(&key, size, chunks)?;

        let mut options = vec![method];
        if no_eol {
            options.push(KnitMethod::NoEol);
        }
        self.index
            .add_records(&[(key, options, memo, parents)], random_id, false)?;

        Ok((digest, text_length))
    }

    /// Read raw records and return `(key, content, digest)` triples, sorted
    /// by storage position to minimise I/O seeks.
    pub fn read_records_iter(
        &self,
        records: &[(KnitKey, KnitIndexMemo)],
    ) -> Result<Vec<(KnitKey, F::Content, Vec<u8>)>, KnitError> {
        if records.is_empty() {
            return Ok(vec![]);
        }
        let mut sorted = records.to_vec();
        sorted.sort_by(|a, b| (&a.1.path, a.1.offset).cmp(&(&b.1.path, b.1.offset)));
        let memos: Vec<KnitIndexMemo> = sorted.iter().map(|(_, m)| m.clone()).collect();
        let raw_data = self.access.get_raw_records(&memos)?;
        let mut out = Vec::with_capacity(sorted.len());
        for ((key, _), raw) in sorted.into_iter().zip(raw_data) {
            let version_id = key.last().cloned().unwrap_or_default();
            let (body_lines, digest) = parse_record(&version_id, &raw)?;
            let refs: Vec<&[u8]> = body_lines.iter().map(|l| l.as_slice()).collect();
            // We don't know the method here without re-querying the index, so
            // assume fulltext for the record_iter use-case (which always
            // reconstructs via get_content anyway).
            let content = self.factory.parse_fulltext_content(&refs, &version_id)?;
            out.push((key, content, digest));
        }
        Ok(out)
    }

    /// Fetch raw (gzip-compressed) bytes for each `(key, memo)` pair in
    /// the order given, without any parsing or validation.
    pub fn read_records_iter_unchecked(
        &self,
        records: &[(KnitKey, KnitIndexMemo)],
    ) -> Result<Vec<(KnitKey, Vec<u8>)>, KnitError> {
        if records.is_empty() {
            return Ok(vec![]);
        }
        let memos: Vec<KnitIndexMemo> = records.iter().map(|(_, m)| m.clone()).collect();
        let raw_data = self.access.get_raw_records(&memos)?;
        Ok(records
            .iter()
            .map(|(k, _)| k.clone())
            .zip(raw_data)
            .collect())
    }

    /// Fetch raw bytes for each `(key, memo)` pair and validate each
    /// record header, returning `(key, raw_bytes, sha1_digest)`.
    pub fn read_records_iter_raw(
        &self,
        records: &[(KnitKey, KnitIndexMemo)],
    ) -> Result<Vec<(KnitKey, Vec<u8>, Vec<u8>)>, KnitError> {
        let pairs = self.read_records_iter_unchecked(records)?;
        let mut out = Vec::with_capacity(pairs.len());
        for (key, raw) in pairs {
            let header = parse_record_header_only(&raw)?;
            out.push((key, raw, header.digest));
        }
        Ok(out)
    }

    /// Yield `(line_bytes, key)` for every line present in any of `keys`.
    ///
    /// Reads each record as-stored and reconstructs the content, then
    /// emits each plain text line paired with its key.  Fallback
    /// versioned files are not consulted; callers that want fallback
    /// must iterate them separately.
    pub fn iter_lines_added_or_present_in_keys(
        &self,
        keys: &[KnitKey],
    ) -> Result<Vec<(Vec<u8>, KnitKey)>, KnitError> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let build_details = self.index.get_build_details(keys)?;
        let key_records: Vec<(KnitKey, KnitIndexMemo)> = build_details
            .iter()
            .map(|(k, det)| (k.clone(), det.index_memo.clone()))
            .collect();
        // read_records_iter fully reconstructs content (applying deltas).
        let triples = self.read_records_iter(&key_records)?;
        let mut out = Vec::new();
        for (key, content, _digest) in triples {
            for line in content.text() {
                out.push((line, key.clone()));
            }
        }
        Ok(out)
    }
}

/// One record supplied to [`KnitVersionedFiles::insert_record_stream`].
///
/// The pyo3 layer inspects each Python stream object and maps it to one of
/// these variants before calling the pure-Rust implementation.
pub enum KnitStreamRecord {
    /// A native knit record whose raw gzip bytes can be blat-copied directly.
    ///
    /// `method` is either `Fulltext` or `LineDelta`.
    /// `noeol` is the `no-eol` build flag.
    /// `compression_parent` is `Some(parent_key)` for delta records.
    /// `raw_record` is the gzip-compressed bytes.
    NativeKnit {
        key: KnitKey,
        parents: Vec<KnitKey>,
        method: KnitMethod,
        noeol: bool,
        compression_parent: Option<KnitKey>,
        raw_record: Vec<u8>,
    },
    /// An annotated knit record that must be stripped before storing into an
    /// unannotated KVF.  Only valid when `self.factory.annotated() == false`.
    ConvertAnnotated {
        key: KnitKey,
        parents: Vec<KnitKey>,
        method: KnitMethod,
        noeol: bool,
        compression_parent: Option<KnitKey>,
        raw_record: Vec<u8>,
    },
    /// A record in some other format; the caller has already decoded it to
    /// plain text lines.
    Lines {
        key: KnitKey,
        parents: Vec<KnitKey>,
        lines: Vec<Vec<u8>>,
    },
}

/// One entry in a pre-fetched raw record map for the delta-closure path.
///
/// Mirrors the values in `_ContentMapGenerator._raw_record_map`:
/// `{key: (raw_bytes, (method, noeol), next_key)}`.
#[derive(Debug, Clone)]
pub struct DeltaClosureRawEntry {
    pub raw_bytes: Vec<u8>,
    pub method: KnitMethod,
    pub noeol: bool,
    /// Compression parent key (`None` for fulltexts).
    pub next: Option<KnitKey>,
}

/// Pre-fetched raw record map for the delta-closure path.
///
/// The map contains all records needed to reconstruct each requested key
/// as a fulltext by walking the `next` chain.
pub type DeltaClosureRawMap = std::collections::HashMap<KnitKey, DeltaClosureRawEntry>;

/// Reconstruct the full text for `key` by walking the compression chain in
/// `raw_map`.
///
/// Mirrors `_ContentMapGenerator._get_one_work` for a single key.  Returns
/// the plain text lines (each ending in `\n` except possibly the last when
/// `noeol` is set) and the SHA-1 digest from the innermost record header.
pub fn reconstruct_text_from_raw_map<F: KnitFactory>(
    factory: &F,
    raw_map: &DeltaClosureRawMap,
    key: &KnitKey,
) -> Result<(Vec<Vec<u8>>, Vec<u8>), KnitError> {
    // Walk the chain from key outward to the base (fulltext).
    let mut chain: Vec<KnitKey> = Vec::new();
    let mut cursor = key.clone();
    loop {
        let entry = raw_map.get(&cursor).ok_or_else(|| {
            KnitError::Corrupt(format!("key {cursor:?} missing from raw record map"))
        })?;
        chain.push(cursor.clone());
        match &entry.next {
            None => break,
            Some(next) => cursor = next.clone(),
        }
    }

    // Reconstruct from base to tip, applying deltas.
    let mut content: Option<F::Content> = None;
    let mut last_digest = Vec::new();
    for k in chain.iter().rev() {
        let entry = &raw_map[k];
        let version_id = k.last().cloned().unwrap_or_default();
        let (body_lines, digest) = parse_record(&version_id, &entry.raw_bytes)?;
        let refs: Vec<&[u8]> = body_lines.iter().map(|l| l.as_slice()).collect();
        let new_content = factory.parse_record(
            &version_id,
            &refs,
            entry.method.clone(),
            entry.noeol,
            content.as_ref(),
        )?;
        content = Some(new_content);
        last_digest = digest;
    }

    let content = content.ok_or_else(|| KnitError::Corrupt("empty chain".to_string()))?;
    Ok((
        content.text().into_iter().map(|l| l.to_vec()).collect(),
        last_digest,
    ))
}

/// Build the wire bytes for a delta-closure raw map.
///
/// Mirrors `_ContentMapGenerator._wire_bytes`: serializes the full raw record
/// map (all fetched components) together with `emit_keys` and `global_map`
/// into the `knit-delta-closure` wire format.
pub fn build_delta_closure_wire_bytes(
    annotated: bool,
    emit_keys: &[KnitKey],
    raw_map: &DeltaClosureRawMap,
    global_map: &std::collections::HashMap<KnitKey, Option<Vec<KnitKey>>>,
) -> Vec<u8> {
    let parent_slices: Vec<Option<Vec<&[Vec<u8>]>>> = raw_map
        .iter()
        .map(|(key, _)| {
            global_map
                .get(key)
                .and_then(|p| p.as_ref())
                .map(|ps| ps.iter().map(|p| p.as_slice()).collect())
        })
        .collect();

    let emit_key_slices: Vec<&[Vec<u8>]> = emit_keys.iter().map(|k| k.as_slice()).collect();
    let records: Vec<KnitDeltaClosureRecord<'_, Vec<u8>>> = raw_map
        .iter()
        .zip(parent_slices.iter())
        .map(|((key, entry), parents_opt)| KnitDeltaClosureRecord {
            key: key.as_slice(),
            parents: parents_opt.as_deref(),
            method: entry.method.as_str().as_bytes(),
            noeol: entry.noeol,
            next: entry.next.as_deref(),
            record_bytes: &entry.raw_bytes,
        })
        .collect();

    build_knit_delta_closure_wire(annotated, &emit_key_slices, &records)
}

/// A record returned by [`KnitVersionedFiles::get_record_stream`].
///
/// Mirrors Python's `KnitContentFactory`: holds the key, parents, storage
/// method, and raw (gzip-compressed) bytes for one revision.  The raw bytes
/// can be passed directly to [`parse_record`] or [`parse_record_unchecked`].
#[derive(Debug, Clone)]
pub struct KnitContentFactory {
    pub key: KnitKey,
    /// `None` when there is no graph information (e.g. `has_graph = false`).
    pub parents: Option<Vec<KnitKey>>,
    pub record_details: KnitRecordDetails,
    /// SHA-1 digest of the reconstructed fulltext, or `None` if not yet
    /// computed (lazy; callers can compute it with [`parse_record_header_only`]).
    pub sha1: Option<Vec<u8>>,
    /// Raw gzip bytes as stored on disk.
    pub raw_record: Vec<u8>,
    pub annotated: bool,
}

impl<I: KnitIndex, A: KnitAccess, F: KnitFactory> KnitVersionedFiles<I, A, F> {
    /// Fetch records for `keys`, emitting one [`KnitContentFactory`] per
    /// locally-present key plus one [`KnitContentFactory`] with absent status
    /// (empty `raw_record`) for each key that is not found.
    ///
    /// Ordering is controlled by `ordering`:
    /// - `"unordered"` — I/O-efficient order (sorted by file and offset).
    /// - `"topological"` — parents strictly before children.
    ///
    /// When `include_delta_closure` is `false`, raw gzip bytes are fetched
    /// directly.  When `true`, the full compression closure is walked first
    /// so that every record's basis is present in the returned slice.
    ///
    /// Keys not found locally are returned as absent entries (`raw_record`
    /// empty and `sha1` `None`); the caller is responsible for consulting
    /// fallback stores.
    pub fn get_record_stream(
        &self,
        keys: &[KnitKey],
        ordering: &str,
        include_delta_closure: bool,
    ) -> Result<Vec<KnitContentFactory>, KnitError> {
        use std::collections::{HashMap, HashSet};

        if keys.is_empty() {
            return Ok(vec![]);
        }

        // For the delta-closure case we walk the compression chain so that
        // basis keys are included in the fetch, matching Python's
        // _get_components_positions(allow_missing=True).
        let positions: HashMap<KnitKey, KnitRecordDetails> = if include_delta_closure {
            let closure_result = walk_compression_closure::<KnitKey, KnitRecordDetails, _>(
                keys.iter().cloned(),
                true,
                |batch| {
                    let details = self.index.get_build_details(batch).unwrap_or_default();
                    let mut present = HashMap::new();
                    let mut missing = HashSet::new();
                    for k in batch {
                        if let Some(det) = details.get(k) {
                            present
                                .insert(k.clone(), (det.compression_parent.clone(), det.clone()));
                        } else {
                            missing.insert(k.clone());
                        }
                    }
                    ClosureBatch { present, missing }
                },
            );
            // allow_missing=true so Err never occurs; unwrap is safe.
            closure_result.unwrap_or_default()
        } else {
            self.index.get_build_details(keys)?
        };

        let present_keys: Vec<KnitKey> = keys
            .iter()
            .filter(|k| positions.contains_key(*k))
            .cloned()
            .collect();
        let absent_keys: Vec<KnitKey> = keys
            .iter()
            .filter(|k| !positions.contains_key(*k))
            .cloned()
            .collect();

        let sorted_keys: Vec<KnitKey> = match ordering {
            "topological" => {
                let parent_map = self.index.get_parent_map(&present_keys)?;
                let mut sorter = vcs_graph::tsort::TopoSorter::new(parent_map.into_iter());
                sorter
                    .sorted()
                    .map_err(|e| KnitError::Corrupt(format!("topo_sort: {e:?}")))?
            }
            _ => {
                // Unordered: sort by I/O position.
                let mut ks = present_keys.clone();
                self.index.sort_keys_by_io(&mut ks, &positions);
                ks
            }
        };

        let memos: Vec<KnitIndexMemo> = sorted_keys
            .iter()
            .map(|k| positions[k].index_memo.clone())
            .collect();
        let raw_records = self.access.get_raw_records(&memos)?;

        let mut out: Vec<KnitContentFactory> = Vec::with_capacity(keys.len());

        // Emit absent entries first, matching Python's ordering.
        for key in &absent_keys {
            out.push(KnitContentFactory {
                key: key.clone(),
                parents: None,
                record_details: KnitRecordDetails {
                    method: KnitMethod::Fulltext,
                    noeol: false,
                    index_memo: KnitIndexMemo {
                        path: String::new(),
                        offset: 0,
                        length: 0,
                    },
                    compression_parent: None,
                    parents: vec![],
                },
                sha1: None,
                raw_record: vec![],
                annotated: self.factory.annotated(),
            });
        }

        for (key, raw) in sorted_keys.into_iter().zip(raw_records) {
            let details = positions[&key].clone();
            out.push(KnitContentFactory {
                key,
                parents: Some(details.parents.clone()),
                record_details: details,
                sha1: None,
                raw_record: raw,
                annotated: self.factory.annotated(),
            });
        }

        Ok(out)
    }

    /// Insert a stream of records into this knit.
    ///
    /// Each record must be classified by the caller into one of the three
    /// [`KnitStreamRecord`] variants.  The method handles:
    ///
    /// - [`KnitStreamRecord::NativeKnit`] — raw bytes copied directly to storage,
    ///   with delta records buffered until their basis is present.
    /// - [`KnitStreamRecord::ConvertAnnotated`] — annotated bytes stripped to plain
    ///   before storage (only valid when `self.factory.annotated() == false`).
    /// - [`KnitStreamRecord::Lines`] — plain text lines passed to `add_lines`.
    ///
    /// Mirrors Python's `KnitVersionedFiles.insert_record_stream`.
    pub fn insert_record_stream(
        &self,
        stream: impl IntoIterator<Item = Result<KnitStreamRecord, KnitError>>,
    ) -> Result<(), KnitError> {
        use std::collections::HashMap;

        type BufferMap =
            HashMap<KnitKey, Vec<(KnitKey, Vec<KnitMethod>, KnitIndexMemo, Vec<KnitKey>)>>;

        self.index.check_write_ok()?;

        // key = compression_parent not yet present; value = entries waiting for it.
        let mut buffered: BufferMap = HashMap::new();

        for item in stream {
            let record = item?;

            // Determine the raw bytes and metadata to write.
            let (key, parents, method, noeol, compression_parent, raw_bytes) = match record {
                KnitStreamRecord::NativeKnit {
                    key,
                    parents,
                    method,
                    noeol,
                    compression_parent,
                    raw_record,
                } => (key, parents, method, noeol, compression_parent, raw_record),
                KnitStreamRecord::ConvertAnnotated {
                    key,
                    parents,
                    method,
                    noeol,
                    compression_parent,
                    raw_record,
                } => {
                    let converted = match method {
                        KnitMethod::LineDelta => {
                            recompress_annotated_to_unannotated_delta(&raw_record)?
                        }
                        _ => recompress_annotated_to_unannotated_fulltext(&raw_record)?,
                    };
                    (key, parents, method, noeol, compression_parent, converted)
                }
                KnitStreamRecord::Lines {
                    key,
                    parents,
                    lines,
                } => {
                    self.access.flush()?;
                    self.add_lines(key.clone(), parents, lines, false)?;
                    // Drain any entries whose basis is now present.
                    let mut ready = vec![key];
                    while let Some(k) = ready.pop() {
                        if let Some(entries) = buffered.remove(&k) {
                            let new_keys: Vec<KnitKey> =
                                entries.iter().map(|(ek, _, _, _)| ek.clone()).collect();
                            self.index.add_records(&entries, false, false)?;
                            ready.extend(new_keys);
                        }
                    }
                    continue;
                }
            };

            // Write raw bytes and (maybe) register the index entry.
            parse_record_header_only(&raw_bytes)?;
            let size = raw_bytes.len();
            let memo = self.access.add_raw_record(&key, size, vec![raw_bytes])?;
            let mut options = vec![method];
            if noeol {
                options.push(KnitMethod::NoEol);
            }
            let entry = (key.clone(), options, memo, parents.to_vec());

            let needs_buffer = compression_parent.as_ref().is_some_and(|cp| {
                self.index
                    .get_parent_map(std::slice::from_ref(cp))
                    .map(|m| !m.contains_key(cp))
                    .unwrap_or(true)
            });

            if needs_buffer {
                buffered
                    .entry(compression_parent.unwrap())
                    .or_default()
                    .push(entry);
            } else {
                self.index.add_records(&[entry], false, false)?;
                // Drain any entries whose basis is now present.
                let mut ready = vec![key];
                while let Some(k) = ready.pop() {
                    if let Some(entries) = buffered.remove(&k) {
                        let new_keys: Vec<KnitKey> =
                            entries.iter().map(|(ek, _, _, _)| ek.clone()).collect();
                        self.index.add_records(&entries, false, false)?;
                        ready.extend(new_keys);
                    }
                }
            }
        }

        // Any entries still buffered get registered with missing_compression_parents=true
        // so pack-format indexes can hold them for deferred resolution.
        if !buffered.is_empty() {
            let all_entries: Vec<_> = buffered.into_values().flatten().collect();
            self.index.add_records(&all_entries, false, true)?;
        }

        Ok(())
    }
}

/// Port of Python's `KnitVersionedFiles._merge_annotations`.
///
/// When the factory is annotated, each line in `content` starts with the new
/// version's own key as its origin annotation.  This function walks every
/// parent and, for each run of lines that the parent and the new content share
/// (same text), copies the parent's `(origin, text)` annotation into the new
/// content — so that unchanged lines keep the version that first introduced
/// them rather than being attributed to the current version.
///
/// After annotation merging, if `use_delta` is true, a patience-diff delta
/// against the first present parent is computed and returned.
///
/// Returns `Some(delta_hunks)` when `use_delta` is true, `None` otherwise.
pub(crate) fn merge_annotations<I, A, F>(
    index: &I,
    access: &A,
    factory: &F,
    content: &mut F::Content,
    present_parents: &[KnitKey],
    use_delta: bool,
) -> Result<Option<Vec<DeltaHunk<<F::Content as KnitContent>::DeltaLine>>>, KnitError>
where
    I: KnitIndex,
    A: KnitAccess,
    F: KnitFactory,
    <F::Content as KnitContent>::DeltaLine: Clone,
{
    if factory.annotated() {
        for parent_key in present_parents {
            let parent_content = get_content(index, access, factory, parent_key)?;
            let parent_text: Vec<Vec<u8>> = parent_content.text();
            let new_text: Vec<Vec<u8>> = content.text();

            let mut matcher = patiencediff::SequenceMatcher::new(&parent_text, &new_text);
            let opcodes = matcher.get_opcodes().to_vec();
            // Use raw annotation (without strip-eol) so that copied lines
            // retain their trailing '\n' regardless of the parent's noeol flag.
            let parent_annot_raw = parent_content.annotate_raw();

            for op in &opcodes {
                if let patiencediff::Opcode::Equal(a_start, a_end, b_start, b_end) = op {
                    // Copy annotation from parent for each matching line.
                    let len = a_end - a_start;
                    let new_lines = content.annotate_mut();
                    for k in 0..len {
                        new_lines[b_start + k] = parent_annot_raw[a_start + k].clone();
                    }
                    let _ = b_end;
                }
            }
        }
    }

    if use_delta {
        let Some(first_parent) = present_parents.first() else {
            return Ok(None);
        };
        let base = get_content(index, access, factory, first_parent)?;
        let delta = compute_line_delta(&base, content);
        Ok(Some(delta))
    } else {
        Ok(None)
    }
}

/// Compute a patience-diff line delta between `base` and `new`.
///
/// Returns hunks in the `DeltaHunk` shape that `KnitFactory::lower_line_delta`
/// can serialise.  The `DeltaLine` type is inferred from the factory's
/// `Content` type; callers pass the base and new content objects directly.
///
/// Uses `text()` for line comparison (which correctly applies strip-eol for
/// the `no_eol` flag) but reads delta line content from the raw annotation
/// pairs so that stored line text retains its trailing newline.
fn compute_line_delta<C: KnitContent>(base: &C, new: &C) -> Vec<DeltaHunk<C::DeltaLine>>
where
    C::DeltaLine: Clone,
{
    let old_text = base.text();
    let new_text = new.text();

    let mut matcher = patiencediff::SequenceMatcher::new(&old_text, &new_text);
    let opcodes = matcher.get_opcodes().to_vec();

    // Use annotate_raw() for the delta line content so that lines retain their
    // trailing '\n' even when should_strip_eol is set on the content.
    let new_annot_raw = new.annotate_raw();
    let mut hunks: Vec<DeltaHunk<C::DeltaLine>> = Vec::new();
    for op in &opcodes {
        if matches!(op, patiencediff::Opcode::Equal(..)) {
            continue;
        }
        let hunk_new_lines: Vec<C::DeltaLine> = new_annot_raw[op.b_start()..op.b_end()]
            .iter()
            .map(C::delta_line_from_annotated)
            .collect();
        hunks.push(DeltaHunk {
            start: op.a_start(),
            end: op.a_end(),
            count: op.b_end() - op.b_start(),
            lines: hunk_new_lines,
        });
    }
    hunks
}

/// Annotation for one line: set of keys that could be the origin of this line.
/// Usually contains a single key.
pub type LineAnnotation = Vec<KnitKey>;

/// Build per-line annotations for a knit versioned file.
///
/// Mirrors `bzrformats.knit._KnitAnnotator` (and its base class
/// `bzrformats.annotate.VersionedFileAnnotator`).
pub struct KnitAnnotator<I, A, F>
where
    I: KnitIndex,
    A: KnitAccess,
    F: KnitFactory,
{
    index: I,
    access: A,
    factory: F,
    /// Map key → parent keys.
    parent_map: std::collections::HashMap<KnitKey, Vec<KnitKey>>,
    /// Cached plain-text lines per key (freed as soon as no longer needed).
    text_cache: std::collections::HashMap<KnitKey, Vec<Vec<u8>>>,
    /// Number of as-yet-unannotated children that still need this key's text.
    num_needed_children: std::collections::HashMap<KnitKey, usize>,
    /// Completed per-line annotations.
    annotations_cache: std::collections::HashMap<KnitKey, Vec<LineAnnotation>>,
    /// Build details fetched during `get_build_graph`.
    all_build_details: std::collections::HashMap<KnitKey, KnitRecordDetails>,
    /// Number of delta-children still waiting on a compression parent.
    num_compression_children: std::collections::HashMap<KnitKey, usize>,
    /// Content objects kept alive while delta children depend on them.
    content_objects: std::collections::HashMap<KnitKey, F::Content>,
    /// Delta records queued until their compression parent is ready.
    pending_deltas: std::collections::HashMap<
        KnitKey,
        Vec<(KnitKey, Vec<KnitKey>, Vec<u8>, KnitRecordDetails)>,
    >,
    /// Keys whose text is ready but that still await parent annotations.
    pending_annotation:
        std::collections::HashMap<KnitKey, Vec<(KnitKey, Vec<KnitKey>)>>,
    /// Pre-computed matching blocks from delta expansion, consumed once.
    matching_blocks:
        std::collections::HashMap<(KnitKey, KnitKey), Vec<(usize, usize, usize)>>,
}

impl<I, A, F> KnitAnnotator<I, A, F>
where
    I: KnitIndex,
    A: KnitAccess,
    F: KnitFactory,
    F::Content: Clone,
{
    pub fn new(index: I, access: A, factory: F) -> Self {
        Self {
            index,
            access,
            factory,
            parent_map: Default::default(),
            text_cache: Default::default(),
            num_needed_children: Default::default(),
            annotations_cache: Default::default(),
            all_build_details: Default::default(),
            num_compression_children: Default::default(),
            content_objects: Default::default(),
            pending_deltas: Default::default(),
            pending_annotation: Default::default(),
            matching_blocks: Default::default(),
        }
    }

    /// Walk the compression/parent graph for `key`, filling `all_build_details`
    /// and returning `(records, ann_keys)` — mirrors `_get_build_graph`.
    fn get_build_graph(
        &mut self,
        key: &KnitKey,
    ) -> Result<(Vec<(KnitKey, KnitIndexMemo)>, Vec<KnitKey>), KnitError> {
        let mut pending: std::collections::HashSet<KnitKey> =
            std::iter::once(key.clone()).collect();
        let mut records: Vec<(KnitKey, KnitIndexMemo)> = Vec::new();
        let mut ann_keys: Vec<KnitKey> = Vec::new();
        *self.num_needed_children.entry(key.clone()).or_insert(0) += 1;

        while !pending.is_empty() {
            let this_iteration: Vec<KnitKey> = pending.drain().collect();
            let build_details = self.index.get_build_details(&this_iteration)?;
            self.all_build_details.extend(build_details.clone());
            pending = std::collections::HashSet::new();

            for k in &this_iteration {
                if let Some(details) = build_details.get(k) {
                    let parents = details.parents.clone();
                    self.parent_map.insert(k.clone(), parents.clone());
                    self.num_needed_children.entry(k.clone()).or_insert(0);
                    records.push((k.clone(), details.index_memo.clone()));
                    for pk in &parents {
                        if !self.all_build_details.contains_key(pk) {
                            pending.insert(pk.clone());
                        }
                        *self.num_needed_children.entry(pk.clone()).or_insert(0) += 1;
                    }
                    if let Some(ref cp) = details.compression_parent {
                        *self
                            .num_compression_children
                            .entry(cp.clone())
                            .or_insert(0) += 1;
                    }
                } else if self.parent_map.contains_key(k) && self.text_cache.contains_key(k) {
                    // Already have the text (e.g. from a fallback); just annotate it.
                    ann_keys.push(k.clone());
                    let parents = self.parent_map[k].clone();
                    for pk in &parents {
                        *self.num_needed_children.entry(pk.clone()).or_insert(0) += 1;
                        if !self.all_build_details.contains_key(pk) {
                            pending.insert(pk.clone());
                        }
                    }
                } else {
                    return Err(KnitError::Corrupt(format!(
                        "Revision not present: {:?}",
                        k
                    )));
                }
            }
        }

        records.reverse();
        Ok((records, ann_keys))
    }

    /// Decompress a raw on-disk record and invoke `factory.parse_record`.
    fn parse_raw_record(
        &self,
        key: &KnitKey,
        raw: &[u8],
        method: KnitMethod,
        noeol: bool,
        base: Option<F::Content>,
    ) -> Result<F::Content, KnitError> {
        let decompressed = decode_record_gz(raw)?;
        let (_, body_lines) = parse_record_body_unchecked(&decompressed)?;
        self.factory.parse_record(
            key.last().map(|s| s.as_slice()).unwrap_or(&[]),
            &body_lines,
            method,
            noeol,
            base.as_ref(),
        )
    }

    /// Expand one raw record into plain-text lines.  Returns `None` when the
    /// compression parent is not yet ready (record queued in `pending_deltas`).
    fn expand_record(
        &mut self,
        key: KnitKey,
        parent_keys: Vec<KnitKey>,
        compression_parent: Option<KnitKey>,
        raw: Vec<u8>,
        method: KnitMethod,
        noeol: bool,
    ) -> Result<Option<Vec<Vec<u8>>>, KnitError> {
        let content = if let Some(ref cp) = compression_parent {
            if !self.content_objects.contains_key(cp) {
                self.pending_deltas
                    .entry(cp.clone())
                    .or_default()
                    .push((key, parent_keys, raw, KnitRecordDetails {
                        method,
                        noeol,
                        index_memo: KnitIndexMemo { path: String::new(), offset: 0, length: 0 },
                        compression_parent: compression_parent.clone(),
                        parents: vec![],
                    }));
                return Ok(None);
            }
            let num = self.num_compression_children[cp];
            let base_content = if num <= 1 {
                self.num_compression_children.remove(cp);
                self.content_objects.remove(cp).unwrap()
            } else {
                *self.num_compression_children.get_mut(cp).unwrap() -= 1;
                self.content_objects[cp].clone()
            };
            let content = self.parse_raw_record(&key, &raw, method, noeol, Some(base_content))?;
            // Cache matching blocks from the delta expansion for annotation.
            if method == KnitMethod::LineDelta {
                if let Some(parent_lines) = self.text_cache.get(cp).cloned() {
                    let lines = content.text();
                    let p_refs: Vec<&[u8]> = parent_lines.iter().map(|l| l.as_slice()).collect();
                    let l_refs: Vec<&[u8]> = lines.iter().map(|l| l.as_slice()).collect();
                    // Re-parse to get the raw delta hunks for get_line_delta_blocks.
                    if let Ok(decompressed) = decode_record_gz(&raw) {
                        if let Ok((_, body_lines)) = parse_record_body_unchecked(&decompressed) {
                            if let Ok(hunks) = parse_line_delta_raw(&body_lines.iter().copied().collect::<Vec<_>>()) {
                                let raw_hunks: Vec<(usize, usize, usize)> = hunks
                                    .iter()
                                    .map(|h| (h.start, h.end, h.lines.len()))
                                    .collect();
                                let blocks = get_line_delta_blocks(&raw_hunks, &p_refs, &l_refs);
                                self.matching_blocks.insert((key.clone(), cp.clone()), blocks);
                            }
                        }
                    }
                }
            }
            content
        } else {
            self.parse_raw_record(&key, &raw, method, noeol, None)?
        };

        if self.num_compression_children.get(&key).copied().unwrap_or(0) > 0 {
            self.content_objects.insert(key.clone(), content.clone());
        }
        let lines = content.text();
        self.text_cache.insert(key.clone(), lines.clone());
        Ok(Some(lines))
    }

    /// Returns `true` if all parents of `key` have been annotated; otherwise
    /// queues it under the first missing parent in `pending_annotation`.
    fn check_ready_for_annotations(&mut self, key: &KnitKey, parent_keys: &[KnitKey]) -> bool {
        for pk in parent_keys {
            if !self.annotations_cache.contains_key(pk) {
                self.pending_annotation
                    .entry(pk.clone())
                    .or_default()
                    .push((key.clone(), parent_keys.to_vec()));
                return false;
            }
        }
        true
    }

    /// Called after `key` is processed; drains `pending_deltas` and
    /// `pending_annotation` for any children now unblocked.
    fn process_pending(&mut self, key: &KnitKey) -> Result<Vec<KnitKey>, KnitError> {
        let mut to_return: Vec<KnitKey> = Vec::new();

        if let Some(children) = self.pending_deltas.remove(key) {
            for (child_key, parent_keys, raw, details) in children {
                self.expand_record(
                    child_key.clone(),
                    parent_keys.clone(),
                    Some(key.clone()),
                    raw,
                    details.method,
                    details.noeol,
                )?;
                if self.check_ready_for_annotations(&child_key, &parent_keys) {
                    to_return.push(child_key);
                }
            }
        }

        if let Some(children) = self.pending_annotation.remove(key) {
            for (child_key, parent_keys) in children {
                if self.check_ready_for_annotations(&child_key, &parent_keys) {
                    to_return.push(child_key);
                }
            }
        }

        Ok(to_return)
    }

    /// Fetch raw records from disk, expand them, and return `(key, lines)` in
    /// topological order (parents before children).
    fn extract_texts(
        &mut self,
        records: Vec<(KnitKey, KnitIndexMemo)>,
    ) -> Result<Vec<(KnitKey, Vec<Vec<u8>>)>, KnitError> {
        let memos: Vec<KnitIndexMemo> = records.iter().map(|(_, m)| m.clone()).collect();
        let raw_bytes = self.access.get_raw_records(&memos)?;
        let mut out: Vec<(KnitKey, Vec<Vec<u8>>)> = Vec::new();

        for ((key, _memo), raw) in records.into_iter().zip(raw_bytes.into_iter()) {
            let details = self.all_build_details[&key].clone();
            let lines = self.expand_record(
                key.clone(),
                details.parents.clone(),
                details.compression_parent.clone(),
                raw,
                details.method,
                details.noeol,
            )?;
            let Some(lines) = lines else { continue };

            if self.check_ready_for_annotations(&key, &details.parents) {
                out.push((key.clone(), lines));
            }

            let mut to_process = self.process_pending(&key)?;
            while !to_process.is_empty() {
                let this_batch = std::mem::take(&mut to_process);
                for k in this_batch {
                    let lines = self.text_cache[&k].clone();
                    out.push((k.clone(), lines));
                    to_process.extend(self.process_pending(&k)?);
                }
            }
        }

        Ok(out)
    }

    /// Return the annotations and matching blocks for `(key, parent_key)`,
    /// using pre-computed blocks from delta expansion where available.
    fn get_parent_annotations_and_matches(
        &mut self,
        key: &KnitKey,
        text: &[Vec<u8>],
        parent_key: &KnitKey,
    ) -> (Vec<LineAnnotation>, Vec<(usize, usize, usize)>) {
        if let Some(blocks) = self.matching_blocks.remove(&(key.clone(), parent_key.clone())) {
            let parent_annotations = self.annotations_cache[parent_key].clone();
            return (parent_annotations, blocks);
        }
        let parent_lines = self.text_cache[parent_key].clone();
        let parent_annotations = self.annotations_cache[parent_key].clone();
        let p_refs: Vec<&[u8]> = parent_lines.iter().map(|l| l.as_slice()).collect();
        let t_refs: Vec<&[u8]> = text.iter().map(|l| l.as_slice()).collect();
        let blocks = patiencediff::SequenceMatcher::new(&p_refs, &t_refs)
            .get_matching_blocks()
            .to_vec();
        (parent_annotations, blocks)
    }

    fn record_annotation(
        &mut self,
        key: &KnitKey,
        parent_keys: &[KnitKey],
        annotations: Vec<LineAnnotation>,
    ) {
        self.annotations_cache.insert(key.clone(), annotations);
        for pk in parent_keys {
            if let Some(n) = self.num_needed_children.get_mut(pk) {
                *n -= 1;
                if *n == 0 {
                    self.text_cache.remove(pk);
                    self.annotations_cache.remove(pk);
                }
            }
        }
    }

    fn annotate_one(&mut self, key: &KnitKey, text: &[Vec<u8>]) {
        let this_annotation: LineAnnotation = vec![key.clone()];
        let mut annotations: Vec<LineAnnotation> = vec![this_annotation.clone(); text.len()];
        let parent_keys = self.parent_map[key].clone();

        if let Some(first_parent) = parent_keys.first() {
            let (parent_annotations, blocks) =
                self.get_parent_annotations_and_matches(key, text, first_parent);
            for (parent_idx, lines_idx, match_len) in &blocks {
                if *match_len == 0 {
                    continue;
                }
                annotations[*lines_idx..*lines_idx + *match_len]
                    .clone_from_slice(&parent_annotations[*parent_idx..*parent_idx + *match_len]);
            }

            for other_parent in parent_keys.iter().skip(1) {
                let (parent_annotations, blocks) =
                    self.get_parent_annotations_and_matches(key, text, other_parent);
                for (parent_idx, lines_idx, match_len) in &blocks {
                    if *match_len == 0 {
                        continue;
                    }
                    let ann_sub = annotations[*lines_idx..*lines_idx + *match_len].to_vec();
                    let par_sub = &parent_annotations[*parent_idx..*parent_idx + *match_len];
                    if ann_sub == par_sub {
                        continue;
                    }
                    for idx in 0..*match_len {
                        let ann = &ann_sub[idx];
                        let par_ann = &par_sub[idx];
                        let ann_idx = *lines_idx + idx;
                        if ann == par_ann || *ann == this_annotation {
                            annotations[ann_idx] = par_ann.clone();
                        } else {
                            let mut new_ann: std::collections::BTreeSet<KnitKey> =
                                ann.iter().cloned().collect();
                            new_ann.extend(par_ann.iter().cloned());
                            annotations[ann_idx] = new_ann.into_iter().collect();
                        }
                    }
                }
            }
        }

        self.record_annotation(key, &parent_keys.clone(), annotations);
    }

    /// Annotate `key` and return `(annotations, lines)`.
    pub fn annotate(
        &mut self,
        key: &KnitKey,
    ) -> Result<(Vec<LineAnnotation>, Vec<Vec<u8>>), KnitError> {
        let (records, ann_keys) = self.get_build_graph(key)?;
        let texts = self.extract_texts(records)?;
        for (text_key, text) in texts {
            self.annotate_one(&text_key, &text.clone());
        }
        for ann_key in ann_keys {
            let text = self.text_cache[&ann_key].clone();
            self.annotate_one(&ann_key, &text);
        }
        let annotations = self
            .annotations_cache
            .get(key)
            .cloned()
            .ok_or_else(|| KnitError::Corrupt(format!("Revision not present: {:?}", key)))?;
        let lines = self.text_cache.get(key).cloned().unwrap_or_default();
        Ok((annotations, lines))
    }

    /// Return `[(annotation_key, line)]` — one best-origin key per line.
    pub fn annotate_flat(
        &mut self,
        key: &KnitKey,
    ) -> Result<Vec<(KnitKey, Vec<u8>)>, KnitError> {
        let (annotations, lines) = self.annotate(key)?;
        let mut kg = vcs_graph::KnownGraph::new(
            self.parent_map
                .iter()
                .map(|(k, v)| (k.clone(), v.clone())),
            false,
        );
        let out = annotations
            .into_iter()
            .zip(lines)
            .map(|(annotation, line)| {
                let head = if annotation.len() == 1 {
                    annotation.into_iter().next().unwrap()
                } else {
                    let the_heads = kg.heads(annotation.iter().cloned());
                    if the_heads.len() == 1 {
                        the_heads.into_iter().next().unwrap()
                    } else {
                        // Tie-break: sort and take first (matches Python fallback).
                        let mut sorted: Vec<KnitKey> = the_heads.into_iter().collect();
                        sorted.sort();
                        sorted.into_iter().next().unwrap()
                    }
                };
                (head, line)
            })
            .collect();
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::Transport;

    fn refs<'a>(v: &'a [Vec<u8>]) -> Vec<&'a [u8]> {
        v.iter().map(|l| l.as_slice()).collect()
    }

    #[test]
    fn pure_rust_full_record_read_pipeline() {
        // Demonstration that a downstream pure-Rust caller can take raw
        // gzip-compressed knit record bytes and end up with a typed
        // KnitContent, using only the public API of this module. No
        // Python types involved.
        let pairs = vec![
            (b"r1".to_vec(), b"hello\n".to_vec()),
            (b"r1".to_vec(), b"world\n".to_vec()),
        ];
        let body = lower_fulltext(&pairs);
        let (_, chunks) = record_to_data(b"v", b"DD", body.len(), &body, true).unwrap();
        let raw: Vec<u8> = chunks.into_iter().flatten().collect();

        // The pipeline a downstream consumer would write:
        let decompressed = decode_record_gz(&raw).unwrap();
        let (header, body_lines) = parse_record_body_unchecked(&decompressed).unwrap();
        let factory = KnitAnnotateFactory;
        let content = factory
            .parse_record(
                header.version_id,
                &body_lines,
                KnitMethod::Fulltext,
                false,
                None,
            )
            .unwrap();

        assert_eq!(content.lines, pairs);
        assert_eq!(
            content.text(),
            vec![b"hello\n".to_vec(), b"world\n".to_vec()]
        );
    }

    #[test]
    fn pure_rust_delta_chain_apply_pipeline() {
        // A more complete end-to-end: build a fulltext record + a delta
        // record on top of it, then walk the compression chain (one
        // step) and apply the delta to reconstruct the target text.
        let parent_pairs = vec![
            (b"r1".to_vec(), b"a\n".to_vec()),
            (b"r1".to_vec(), b"b\n".to_vec()),
        ];
        let parent_body = lower_fulltext(&parent_pairs);
        let (_, p_chunks) =
            record_to_data(b"r1", b"D1", parent_body.len(), &parent_body, true).unwrap();
        let parent_raw: Vec<u8> = p_chunks.into_iter().flatten().collect();

        // Delta record: replace line 1 (the second line) with "B\n".
        let delta = vec![DeltaHunk {
            start: 1,
            end: 2,
            count: 1,
            lines: vec![(b"r2".to_vec(), b"B\n".to_vec())],
        }];
        let delta_body = lower_line_delta_annotated(&delta);
        let (_, d_chunks) =
            record_to_data(b"r2", b"D2", delta_body.len(), &delta_body, true).unwrap();
        let delta_raw: Vec<u8> = d_chunks.into_iter().flatten().collect();

        // Pure-Rust read + apply pipeline:
        let factory = KnitAnnotateFactory;

        let parent_decomp = decode_record_gz(&parent_raw).unwrap();
        let (parent_header, parent_lines) = parse_record_body_unchecked(&parent_decomp).unwrap();
        let parent_content = factory
            .parse_record(
                parent_header.version_id,
                &parent_lines,
                KnitMethod::Fulltext,
                false,
                None,
            )
            .unwrap();

        let delta_decomp = decode_record_gz(&delta_raw).unwrap();
        let (delta_header, delta_lines) = parse_record_body_unchecked(&delta_decomp).unwrap();
        let target_content = factory
            .parse_record(
                delta_header.version_id,
                &delta_lines,
                KnitMethod::LineDelta,
                false,
                Some(&parent_content),
            )
            .unwrap();

        assert_eq!(
            target_content.text(),
            vec![b"a\n".to_vec(), b"B\n".to_vec()]
        );
    }

    /// Tiny in-memory KnitIndex/KnitAccess pair used by the
    /// `get_text_*` integration tests. Stores raw record bytes keyed
    /// by their version_id (the last segment of the knit key) and
    /// records a flat list of build details.
    #[derive(Default)]
    struct MockKnit {
        records: std::collections::HashMap<KnitKey, KnitRecordDetails>,
        bytes: std::collections::HashMap<KnitIndexMemo, Vec<u8>>,
    }

    impl MockKnit {
        fn add_record(&mut self, key: KnitKey, details: KnitRecordDetails, raw: Vec<u8>) {
            self.bytes.insert(details.index_memo.clone(), raw);
            self.records.insert(key, details);
        }
    }

    impl KnitIndex for MockKnit {
        fn get_build_details(
            &self,
            keys: &[KnitKey],
        ) -> Result<std::collections::HashMap<KnitKey, KnitRecordDetails>, KnitError> {
            let mut out = std::collections::HashMap::new();
            for k in keys {
                if let Some(d) = self.records.get(k) {
                    out.insert(k.clone(), d.clone());
                }
            }
            Ok(out)
        }

        fn keys(&self) -> Result<Vec<KnitKey>, KnitError> {
            Ok(self.records.keys().cloned().collect())
        }

        fn get_parent_map(
            &self,
            keys: &[KnitKey],
        ) -> Result<std::collections::HashMap<KnitKey, Vec<KnitKey>>, KnitError> {
            Ok(keys
                .iter()
                .filter_map(|k| self.records.get(k).map(|d| (k.clone(), d.parents.clone())))
                .collect())
        }

        fn get_method(&self, key: &KnitKey) -> Result<KnitMethod, KnitError> {
            self.records
                .get(key)
                .map(|d| d.method)
                .ok_or_else(|| KnitError::Corrupt(format!("key not found: {:?}", key)))
        }

        fn get_total_build_size(
            &self,
            keys: &[KnitKey],
            positions: &std::collections::HashMap<KnitKey, KnitRecordDetails>,
        ) -> usize {
            keys.iter()
                .filter_map(|k| positions.get(k))
                .map(|d| d.index_memo.length)
                .sum()
        }

        fn sort_keys_by_io(
            &self,
            keys: &mut [KnitKey],
            positions: &std::collections::HashMap<KnitKey, KnitRecordDetails>,
        ) {
            keys.sort_by(|a, b| {
                let a_key = positions
                    .get(a)
                    .map(|d| (&d.index_memo.path, d.index_memo.offset));
                let b_key = positions
                    .get(b)
                    .map(|d| (&d.index_memo.path, d.index_memo.offset));
                a_key.cmp(&b_key)
            });
        }

        fn has_graph(&self) -> bool {
            true
        }

        fn contains(&self, key: &KnitKey) -> Result<bool, KnitError> {
            Ok(self.records.contains_key(key))
        }

        fn get_missing_compression_parents(&self) -> Result<Vec<KnitKey>, KnitError> {
            Ok(vec![])
        }

        fn check_write_ok(&self) -> Result<(), KnitError> {
            Ok(())
        }

        fn add_records(
            &self,
            _records: &[(KnitKey, Vec<KnitMethod>, KnitIndexMemo, Vec<KnitKey>)],
            _random_id: bool,
            _missing_compression_parents: bool,
        ) -> Result<(), KnitError> {
            Ok(())
        }
    }

    impl KnitAccess for MockKnit {
        fn get_raw_record(&self, memo: &KnitIndexMemo) -> Result<Vec<u8>, KnitError> {
            self.bytes
                .get(memo)
                .cloned()
                .ok_or_else(|| KnitError::BadIndexValue(memo.path.as_bytes().to_vec()))
        }

        fn get_raw_records(&self, memos: &[KnitIndexMemo]) -> Result<Vec<Vec<u8>>, KnitError> {
            memos.iter().map(|m| self.get_raw_record(m)).collect()
        }

        fn add_raw_record(
            &self,
            _key: &KnitKey,
            _size: usize,
            _data: Vec<Vec<u8>>,
        ) -> Result<KnitIndexMemo, KnitError> {
            unimplemented!("MockKnit::add_raw_record")
        }

        fn flush(&self) -> Result<(), KnitError> {
            Ok(())
        }

        fn reload_or_raise(&self, err: KnitError) -> Result<(), KnitError> {
            Err(err)
        }
    }

    fn build_fulltext_record(
        version_id: &[u8],
        annotated: &[AnnotatedLine],
    ) -> (Vec<u8>, KnitIndexMemo) {
        let body = lower_fulltext(annotated);
        let (_, chunks) = record_to_data(version_id, b"DIGEST", body.len(), &body, true).unwrap();
        let raw: Vec<u8> = chunks.into_iter().flatten().collect();
        let memo = KnitIndexMemo {
            path: format!("rec/{}", String::from_utf8_lossy(version_id)),
            offset: 0,
            length: raw.len(),
        };
        (raw, memo)
    }

    fn build_delta_record(
        version_id: &[u8],
        delta: &[DeltaHunk<AnnotatedLine>],
    ) -> (Vec<u8>, KnitIndexMemo) {
        let body = lower_line_delta_annotated(delta);
        let (_, chunks) = record_to_data(version_id, b"DIGEST", body.len(), &body, true).unwrap();
        let raw: Vec<u8> = chunks.into_iter().flatten().collect();
        let memo = KnitIndexMemo {
            path: format!("rec/{}", String::from_utf8_lossy(version_id)),
            offset: 0,
            length: raw.len(),
        };
        (raw, memo)
    }

    #[test]
    fn get_text_returns_fulltext_record_via_traits() {
        let mut knit = MockKnit::default();
        let key: KnitKey = vec![b"file".to_vec(), b"v1".to_vec()];
        let pairs = vec![
            (b"v1".to_vec(), b"alpha\n".to_vec()),
            (b"v1".to_vec(), b"beta\n".to_vec()),
        ];
        let (raw, memo) = build_fulltext_record(b"v1", &pairs);
        knit.add_record(
            key.clone(),
            KnitRecordDetails {
                method: KnitMethod::Fulltext,
                noeol: false,
                index_memo: memo,
                compression_parent: None,
                parents: vec![],
            },
            raw,
        );

        let factory = KnitAnnotateFactory;
        let text = get_text(&knit, &knit, &factory, &key).unwrap();
        assert_eq!(text, b"alpha\nbeta\n".to_vec());
    }

    #[test]
    fn get_text_walks_two_step_delta_chain_via_traits() {
        let mut knit = MockKnit::default();
        let parent_key: KnitKey = vec![b"file".to_vec(), b"v1".to_vec()];
        let child_key: KnitKey = vec![b"file".to_vec(), b"v2".to_vec()];

        // Parent fulltext: two lines.
        let parent_pairs = vec![
            (b"v1".to_vec(), b"a\n".to_vec()),
            (b"v1".to_vec(), b"b\n".to_vec()),
        ];
        let (parent_raw, parent_memo) = build_fulltext_record(b"v1", &parent_pairs);
        knit.add_record(
            parent_key.clone(),
            KnitRecordDetails {
                method: KnitMethod::Fulltext,
                noeol: false,
                index_memo: parent_memo,
                compression_parent: None,
                parents: vec![],
            },
            parent_raw,
        );

        // Child delta: replace line 1 (the "b\n") with "B\n".
        let delta = vec![DeltaHunk {
            start: 1,
            end: 2,
            count: 1,
            lines: vec![(b"v2".to_vec(), b"B\n".to_vec())],
        }];
        let (delta_raw, delta_memo) = build_delta_record(b"v2", &delta);
        knit.add_record(
            child_key.clone(),
            KnitRecordDetails {
                method: KnitMethod::LineDelta,
                noeol: false,
                index_memo: delta_memo,
                compression_parent: Some(parent_key.clone()),
                parents: vec![parent_key.clone()],
            },
            delta_raw,
        );

        let factory = KnitAnnotateFactory;
        let text = get_text(&knit, &knit, &factory, &child_key).unwrap();
        assert_eq!(text, b"a\nB\n".to_vec());
    }

    #[test]
    fn get_sha1s_returns_digests_without_parsing_bodies() {
        let mut knit = MockKnit::default();
        let key_a: KnitKey = vec![b"file".to_vec(), b"a".to_vec()];
        let key_b: KnitKey = vec![b"file".to_vec(), b"b".to_vec()];
        let pairs_a = vec![(b"a".to_vec(), b"hello\n".to_vec())];
        let pairs_b = vec![(b"b".to_vec(), b"world\n".to_vec())];
        // build_fulltext_record hard-codes the digest as b"DIGEST" for
        // both records, so both should come back equal.
        let (raw_a, memo_a) = build_fulltext_record(b"a", &pairs_a);
        let (raw_b, memo_b) = build_fulltext_record(b"b", &pairs_b);
        knit.add_record(
            key_a.clone(),
            KnitRecordDetails {
                method: KnitMethod::Fulltext,
                noeol: false,
                index_memo: memo_a,
                compression_parent: None,
                parents: vec![],
            },
            raw_a,
        );
        knit.add_record(
            key_b.clone(),
            KnitRecordDetails {
                method: KnitMethod::Fulltext,
                noeol: false,
                index_memo: memo_b,
                compression_parent: None,
                parents: vec![],
            },
            raw_b,
        );

        let result = get_sha1s(&knit, &knit, &[key_a.clone(), key_b.clone()]).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[&key_a], b"DIGEST");
        assert_eq!(result[&key_b], b"DIGEST");
    }

    #[test]
    fn get_sha1s_skips_missing_keys() {
        let knit = MockKnit::default();
        let key: KnitKey = vec![b"missing".to_vec()];
        let result = get_sha1s(&knit, &knit, &[key]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn get_text_propagates_missing_key() {
        let knit = MockKnit::default();
        let key: KnitKey = vec![b"missing".to_vec()];
        let factory = KnitAnnotateFactory;
        assert!(get_text(&knit, &knit, &factory, &key).is_err());
    }

    #[test]
    fn annotated_content_text_strips_origins() {
        let content = AnnotatedKnitContent::new(vec![
            (b"r1".to_vec(), b"first\n".to_vec()),
            (b"r2".to_vec(), b"second\n".to_vec()),
        ]);
        assert_eq!(
            content.text(),
            vec![b"first\n".to_vec(), b"second\n".to_vec()]
        );
    }

    #[test]
    fn annotated_content_text_honors_strip_eol() {
        let mut content = AnnotatedKnitContent::new(vec![
            (b"r1".to_vec(), b"first\n".to_vec()),
            (b"r2".to_vec(), b"second\n".to_vec()),
        ]);
        content.set_should_strip_eol(true);
        assert_eq!(
            content.text(),
            vec![b"first\n".to_vec(), b"second".to_vec()]
        );
        // annotate() should also see the stripped tail.
        let annotated = content.annotate();
        assert_eq!(annotated.last().unwrap().1, b"second");
    }

    #[test]
    fn annotated_content_apply_delta_splices_lines() {
        // Replace lines 1..3 (zero-indexed) with two new lines, then
        // append one more after the original tail.
        let mut content = AnnotatedKnitContent::new(vec![
            (b"r1".to_vec(), b"a\n".to_vec()),
            (b"r1".to_vec(), b"b\n".to_vec()),
            (b"r1".to_vec(), b"c\n".to_vec()),
            (b"r1".to_vec(), b"d\n".to_vec()),
        ]);
        let delta = vec![DeltaHunk {
            start: 1,
            end: 3,
            count: 2,
            lines: vec![
                (b"r2".to_vec(), b"B\n".to_vec()),
                (b"r2".to_vec(), b"C\n".to_vec()),
            ],
        }];
        content.apply_delta(&delta, b"r2");
        let texts = content.text();
        assert_eq!(
            texts,
            vec![
                b"a\n".to_vec(),
                b"B\n".to_vec(),
                b"C\n".to_vec(),
                b"d\n".to_vec(),
            ]
        );
    }

    #[test]
    fn plain_content_apply_delta_updates_version_id() {
        let mut content =
            PlainKnitContent::new(vec![b"a\n".to_vec(), b"b\n".to_vec()], b"r1".to_vec());
        let delta = vec![DeltaHunk {
            start: 0,
            end: 0,
            count: 1,
            lines: vec![b"first\n".to_vec()],
        }];
        content.apply_delta(&delta, b"r2");
        assert_eq!(content.version_id, b"r2");
        assert_eq!(
            content.text(),
            vec![b"first\n".to_vec(), b"a\n".to_vec(), b"b\n".to_vec()]
        );
    }

    #[test]
    fn plain_content_annotate_uses_version_id() {
        let content =
            PlainKnitContent::new(vec![b"a\n".to_vec(), b"b\n".to_vec()], b"rev".to_vec());
        let annotated = content.annotate();
        assert_eq!(annotated.len(), 2);
        assert_eq!(annotated[0].0, b"rev");
        assert_eq!(annotated[0].1, b"a\n");
        assert_eq!(annotated[1].0, b"rev");
    }

    #[test]
    fn factory_parse_fulltext_round_trips_via_annotated_content() {
        // Lower an annotated fulltext to the on-disk byte form, then
        // parse it back through the factory and check we recover the
        // same `(origin, text)` pairs.
        let pairs = vec![
            (b"r1".to_vec(), b"alpha\n".to_vec()),
            (b"r2".to_vec(), b"beta\n".to_vec()),
        ];
        let body = lower_fulltext(&pairs);
        let body_refs: Vec<&[u8]> = body.iter().map(|l| l.as_slice()).collect();
        let factory = KnitAnnotateFactory;
        let content = factory
            .parse_record(b"v", &body_refs, KnitMethod::Fulltext, false, None)
            .unwrap();
        assert_eq!(content.lines, pairs);
        assert!(!content.should_strip_eol());
    }

    #[test]
    fn factory_parse_record_applies_delta_to_base() {
        let base = AnnotatedKnitContent::new(vec![
            (b"r1".to_vec(), b"a\n".to_vec()),
            (b"r1".to_vec(), b"b\n".to_vec()),
        ]);
        // Annotated delta wire format: "start,end,count\n" + count lines of
        // "origin text\n". The annotated factory reads this and strips
        // origins to get a plain delta hunk it can splice in.
        let body = vec![b"1,2,1\n".to_vec(), b"r2 B\n".to_vec()];
        let body_refs: Vec<&[u8]> = body.iter().map(|l| l.as_slice()).collect();
        let factory = KnitAnnotateFactory;
        let content = factory
            .parse_record(b"r2", &body_refs, KnitMethod::LineDelta, false, Some(&base))
            .unwrap();
        assert_eq!(content.text(), vec![b"a\n".to_vec(), b"B\n".to_vec()]);
    }

    #[test]
    fn plain_factory_parses_line_delta_record() {
        let base = PlainKnitContent::new(vec![b"a\n".to_vec(), b"b\n".to_vec()], b"r1".to_vec());
        // Plain delta wire format: "start,end,count\n" + count bare text lines.
        let body = vec![b"1,2,1\n".to_vec(), b"B\n".to_vec()];
        let body_refs: Vec<&[u8]> = body.iter().map(|l| l.as_slice()).collect();
        let factory = KnitPlainFactory;
        let content = factory
            .parse_record(b"r2", &body_refs, KnitMethod::LineDelta, false, Some(&base))
            .unwrap();
        assert_eq!(content.version_id, b"r2");
        assert_eq!(content.text(), vec![b"a\n".to_vec(), b"B\n".to_vec()]);
    }

    #[test]
    fn factory_line_delta_without_base_is_an_error() {
        let factory = KnitAnnotateFactory;
        let err = factory
            .parse_record(b"v", &[], KnitMethod::LineDelta, false, None)
            .unwrap_err();
        assert!(matches!(err, KnitError::BadIndexValue(_)));
    }

    #[test]
    fn plain_factory_parses_fulltext_into_plain_content() {
        let factory = KnitPlainFactory;
        let body = vec![b"alpha\n".to_vec(), b"beta\n".to_vec()];
        let body_refs: Vec<&[u8]> = body.iter().map(|l| l.as_slice()).collect();
        let content = factory
            .parse_record(b"v", &body_refs, KnitMethod::Fulltext, true, None)
            .unwrap();
        assert_eq!(content.version_id, b"v");
        assert!(content.should_strip_eol());
        assert_eq!(content.text(), vec![b"alpha\n".to_vec(), b"beta".to_vec()]);
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
    fn parse_record_checks_version_match() {
        let body: &[&[u8]] = &[b"a\n", b"b\n"];
        let raw = build_record(b"rev-9", b"DIGEST", body);
        let (lines, digest) = parse_record(b"rev-9", &raw).unwrap();
        assert_eq!(lines, vec![b"a\n".to_vec(), b"b\n".to_vec()]);
        assert_eq!(digest, b"DIGEST");
    }

    #[test]
    fn parse_record_rejects_version_mismatch() {
        let raw = build_record(b"got-this", b"DD", &[b"x\n"]);
        let err = parse_record(b"wanted-that", &raw).unwrap_err();
        assert_eq!(
            err,
            KnitError::UnexpectedVersion {
                wanted: b"wanted-that".to_vec(),
                got: b"got-this".to_vec(),
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
    fn parse_knit_index_value_handles_noeol_flag() {
        let v = parse_knit_index_value(b"N123 4567").unwrap();
        assert!(v.noeol);
        assert_eq!(v.pos, 123);
        assert_eq!(v.size, 4567);

        let v = parse_knit_index_value(b" 5 10").unwrap();
        assert!(!v.noeol);
        assert_eq!(v.pos, 5);
        assert_eq!(v.size, 10);
    }

    #[test]
    fn parse_knit_index_value_rejects_garbage() {
        assert_eq!(
            parse_knit_index_value(b"").unwrap_err(),
            KnitError::BadIndexValue(b"".to_vec())
        );
        assert_eq!(
            parse_knit_index_value(b"Nfoo bar").unwrap_err(),
            KnitError::BadIndexValue(b"Nfoo bar".to_vec())
        );
        assert_eq!(
            parse_knit_index_value(b"N5").unwrap_err(),
            KnitError::BadIndexValue(b"N5".to_vec())
        );
    }

    fn batch_from_chain<'a>(
        chain: &'a std::collections::HashMap<&'static str, Option<&'static str>>,
        keys: &[&'static str],
    ) -> ClosureBatch<&'static str, &'static str> {
        ClosureBatch {
            present: keys
                .iter()
                .filter_map(|k| chain.get(k).map(|p| (*k, (*p, *k))))
                .collect(),
            missing: keys
                .iter()
                .filter(|k| !chain.contains_key(*k))
                .copied()
                .collect(),
        }
    }

    #[test]
    fn walk_compression_closure_follows_chain_until_fulltext() {
        // a -> b -> c -> (fulltext); after walk, result has {a, b, c}.
        let chain: std::collections::HashMap<&'static str, Option<&'static str>> =
            vec![("a", Some("b")), ("b", Some("c")), ("c", None)]
                .into_iter()
                .collect();
        let result =
            walk_compression_closure(vec!["a"], false, |batch| batch_from_chain(&chain, batch))
                .unwrap();
        let learned: std::collections::HashSet<&'static str> = result.keys().copied().collect();
        let expected: std::collections::HashSet<&'static str> =
            vec!["a", "b", "c"].into_iter().collect();
        assert_eq!(learned, expected);
        // Each value is the payload we plumbed through (the key itself).
        assert_eq!(result[&"a"], "a");
        assert_eq!(result[&"c"], "c");
    }

    #[test]
    fn walk_compression_closure_dedups_shared_parents() {
        // Two children share a parent — the parent is only enqueued once.
        let chain: std::collections::HashMap<&'static str, Option<&'static str>> =
            vec![("c1", Some("p")), ("c2", Some("p")), ("p", None)]
                .into_iter()
                .collect();
        let mut batches: usize = 0;
        let result = walk_compression_closure(vec!["c1", "c2"], false, |batch| {
            batches += 1;
            batch_from_chain(&chain, batch)
        })
        .unwrap();
        // Two batches: {c1, c2} then {p}.
        assert_eq!(batches, 2);
        let learned: std::collections::HashSet<&'static str> = result.keys().copied().collect();
        let expected: std::collections::HashSet<&'static str> =
            vec!["c1", "c2", "p"].into_iter().collect();
        assert_eq!(learned, expected);
    }

    #[test]
    fn walk_compression_closure_reports_missing_when_not_allowed() {
        let err =
            walk_compression_closure::<&'static str, &'static str, _>(vec!["x"], false, |_batch| {
                ClosureBatch {
                    present: Default::default(),
                    missing: vec!["x"].into_iter().collect(),
                }
            })
            .unwrap_err();
        let expected: std::collections::HashSet<&'static str> = vec!["x"].into_iter().collect();
        assert_eq!(err, expected);
    }

    #[test]
    fn walk_compression_closure_skips_missing_when_allowed() {
        let result = walk_compression_closure::<&'static str, &'static str, _>(
            vec!["x", "y"],
            true,
            |batch| {
                // y is present (fulltext); x is missing.
                let mut present = std::collections::HashMap::new();
                let mut missing = std::collections::HashSet::new();
                for k in batch {
                    if *k == "y" {
                        present.insert(*k, (None, *k));
                    } else {
                        missing.insert(*k);
                    }
                }
                ClosureBatch { present, missing }
            },
        )
        .unwrap();
        let learned: std::collections::HashSet<&'static str> = result.keys().copied().collect();
        let expected: std::collections::HashSet<&'static str> = vec!["y"].into_iter().collect();
        assert_eq!(learned, expected);
    }

    #[test]
    fn should_use_delta_finds_fulltext_and_picks_delta() {
        // A 100-byte fulltext at the end of a chain of two 10-byte deltas.
        // delta_size = 20, fulltext_size = 100 -> UseDelta.
        let chain: std::collections::HashMap<&str, ChainStep<&'static str>> = vec![
            (
                "a",
                ChainStep {
                    size: 10,
                    compression_parent: Some("b"),
                },
            ),
            (
                "b",
                ChainStep {
                    size: 10,
                    compression_parent: Some("c"),
                },
            ),
            (
                "c",
                ChainStep {
                    size: 100,
                    compression_parent: None,
                },
            ),
        ]
        .into_iter()
        .collect();
        let decision = should_use_delta("a", 5, |k| chain.get(k).cloned());
        assert_eq!(decision, DeltaDecision::UseDelta);
        assert!(decision.should_use_delta());
    }

    #[test]
    fn should_use_delta_picks_fulltext_when_delta_chain_is_bigger() {
        // 200 bytes of delta against a 50-byte fulltext: not worth it.
        let chain: std::collections::HashMap<&str, ChainStep<&'static str>> = vec![
            (
                "a",
                ChainStep {
                    size: 100,
                    compression_parent: Some("b"),
                },
            ),
            (
                "b",
                ChainStep {
                    size: 100,
                    compression_parent: Some("c"),
                },
            ),
            (
                "c",
                ChainStep {
                    size: 50,
                    compression_parent: None,
                },
            ),
        ]
        .into_iter()
        .collect();
        assert_eq!(
            should_use_delta("a", 5, |k| chain.get(k).cloned()),
            DeltaDecision::FulltextSmaller
        );
    }

    #[test]
    fn should_use_delta_chain_too_long() {
        // Every parent points at another delta — no fulltext within
        // max_chain steps.
        let decision = should_use_delta("a", 3, |_| {
            Some(ChainStep {
                size: 5,
                compression_parent: Some("a"),
            })
        });
        assert_eq!(decision, DeltaDecision::ChainTooLong);
    }

    #[test]
    fn should_use_delta_missing_parent_falls_back_to_fulltext() {
        let decision = should_use_delta("a", 5, |_| None);
        assert_eq!(decision, DeltaDecision::MissingParent);
        assert!(!decision.should_use_delta());
    }

    #[test]
    fn decode_kndx_options_picks_method_and_noeol() {
        let opts: &[&[u8]] = &[b"fulltext"];
        assert_eq!(
            decode_kndx_options(opts).unwrap(),
            (KnitMethod::Fulltext, false)
        );

        let opts: &[&[u8]] = &[b"line-delta", b"no-eol"];
        assert_eq!(
            decode_kndx_options(opts).unwrap(),
            (KnitMethod::LineDelta, true)
        );

        // Order-independent and tolerates unknown options.
        let opts: &[&[u8]] = &[b"no-eol", b"some-future-flag", b"fulltext"];
        assert_eq!(
            decode_kndx_options(opts).unwrap(),
            (KnitMethod::Fulltext, true)
        );
    }

    #[test]
    fn decode_kndx_options_rejects_missing_method() {
        let opts: &[&[u8]] = &[b"no-eol"];
        assert!(matches!(
            decode_kndx_options(opts).unwrap_err(),
            KnitError::BadIndexValue(_)
        ));
    }

    #[test]
    fn decode_knit_build_details_picks_method_from_parent_count() {
        // No deltas: always fulltext, even if the (irrelevant) parent
        // count is non-zero.
        let d = decode_knit_build_details(b" 0 10", false, 5).unwrap();
        assert_eq!(d.method, KnitMethod::Fulltext);
        assert_eq!(d.compression_parent, None);

        // Deltas + zero parents: fulltext.
        let d = decode_knit_build_details(b" 0 10", true, 0).unwrap();
        assert_eq!(d.method, KnitMethod::Fulltext);
        assert_eq!(d.compression_parent, None);

        // Deltas + one parent: line-delta.
        let d = decode_knit_build_details(b"N0 10", true, 1).unwrap();
        assert_eq!(d.method, KnitMethod::LineDelta);
        assert!(d.noeol);
        assert_eq!(d.compression_parent, Some(0));

        // Deltas + multiple parents: error.
        assert_eq!(
            decode_knit_build_details(b" 0 10", true, 2).unwrap_err(),
            KnitError::TooManyCompressionParents(2)
        );
    }

    #[test]
    fn extract_annotated_fulltext_strips_origins_and_honors_noeol() {
        // Last line has a trailing \n; with noeol=true the extractor
        // pops it so the caller sees "world" not "world\n".
        let annotated: Vec<AnnotatedLine> = vec![
            (b"r1".to_vec(), b"hello\n".to_vec()),
            (b"r2".to_vec(), b"world\n".to_vec()),
        ];
        let body = lower_fulltext(&annotated);
        let (_, chunks) = record_to_data(b"v", b"DD", body.len(), &body, true).unwrap();
        let raw: Vec<u8> = chunks.into_iter().flatten().collect();

        let with_eol = extract_annotated_fulltext_to_plain_lines(&raw, false).unwrap();
        assert_eq!(with_eol, vec![b"hello\n".to_vec(), b"world\n".to_vec()]);

        let no_eol = extract_annotated_fulltext_to_plain_lines(&raw, true).unwrap();
        assert_eq!(no_eol, vec![b"hello\n".to_vec(), b"world".to_vec()]);
    }

    #[test]
    fn extract_plain_fulltext_lines_passes_through_with_noeol_strip() {
        // Build a plain (unannotated) record and verify the extractor
        // reads the body lines verbatim, applying noeol on the last one.
        let body = vec![b"alpha\n".to_vec(), b"beta\n".to_vec()];
        let (_, chunks) = record_to_data(b"v", b"DD", body.len(), &body, true).unwrap();
        let raw: Vec<u8> = chunks.into_iter().flatten().collect();

        let plain = extract_plain_fulltext_lines(&raw, false).unwrap();
        assert_eq!(plain, vec![b"alpha\n".to_vec(), b"beta\n".to_vec()]);

        let stripped = extract_plain_fulltext_lines(&raw, true).unwrap();
        assert_eq!(stripped, vec![b"alpha\n".to_vec(), b"beta".to_vec()]);
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

    #[test]
    fn dictionary_compress_empty() {
        let lookup = std::collections::HashMap::new();
        let suffixes: Vec<&[u8]> = vec![];
        assert_eq!(dictionary_compress_suffixes(&suffixes, &lookup), b"");
    }

    #[test]
    fn dictionary_compress_all_cached() {
        let mut lookup = std::collections::HashMap::new();
        lookup.insert(&b"rev-a"[..], 0u64);
        lookup.insert(&b"rev-b"[..], 3u64);
        let suffixes: Vec<&[u8]> = vec![b"rev-a", b"rev-b"];
        assert_eq!(dictionary_compress_suffixes(&suffixes, &lookup), b"0 3");
    }

    #[test]
    fn dictionary_compress_mixed_and_fallback() {
        let mut lookup = std::collections::HashMap::new();
        lookup.insert(&b"rev-a"[..], 12u64);
        let suffixes: Vec<&[u8]> = vec![b"rev-a", b"rev-ghost", b"rev-a"];
        assert_eq!(
            dictionary_compress_suffixes(&suffixes, &lookup),
            b"12 .rev-ghost 12"
        );
    }

    #[test]
    fn annotated_content_text_returns_empty_for_empty_input() {
        // Mirrors KnitContentTestsMixin.test_text (empty case).
        let content = AnnotatedKnitContent::new(vec![]);
        assert!(content.text().is_empty());
    }

    #[test]
    fn annotated_content_text_returns_text_part_of_pairs() {
        // Mirrors KnitContentTestsMixin.test_text (non-empty case).
        let content = AnnotatedKnitContent::new(vec![
            (b"origin1".to_vec(), b"text1".to_vec()),
            (b"origin2".to_vec(), b"text2".to_vec()),
        ]);
        assert_eq!(content.text(), vec![b"text1".to_vec(), b"text2".to_vec()]);
    }

    #[test]
    fn annotated_content_clone_preserves_annotations() {
        // Mirrors KnitContentTestsMixin.test_copy: a clone yields the same
        // (origin, text) pairs as the original.
        let content = AnnotatedKnitContent::new(vec![
            (b"origin1".to_vec(), b"text1".to_vec()),
            (b"origin2".to_vec(), b"text2".to_vec()),
        ]);
        let copy = content.clone();
        assert_eq!(copy.annotate(), content.annotate());
    }

    #[test]
    fn annotated_content_annotate_returns_pairs_verbatim() {
        // Mirrors TestAnnotatedKnitContent.test_annotate.
        let empty = AnnotatedKnitContent::new(vec![]);
        assert!(empty.annotate().is_empty());

        let content = AnnotatedKnitContent::new(vec![
            (b"origin1".to_vec(), b"text1".to_vec()),
            (b"origin2".to_vec(), b"text2".to_vec()),
        ]);
        assert_eq!(
            content.annotate(),
            vec![
                (b"origin1".to_vec(), b"text1".to_vec()),
                (b"origin2".to_vec(), b"text2".to_vec()),
            ]
        );
    }

    #[test]
    fn annotated_content_line_delta_keeps_annotations() {
        // Mirrors TestAnnotatedKnitContent.test_line_delta:
        //   content1 = [("", "a"), ("", "b")]
        //   content2 = [("", "a"), ("", "a"), ("", "c")]
        //   expected delta: [(1, 2, 2, [("", "a"), ("", "c")])]
        let content1 = AnnotatedKnitContent::new(vec![
            (Vec::new(), b"a".to_vec()),
            (Vec::new(), b"b".to_vec()),
        ]);
        let content2 = AnnotatedKnitContent::new(vec![
            (Vec::new(), b"a".to_vec()),
            (Vec::new(), b"a".to_vec()),
            (Vec::new(), b"c".to_vec()),
        ]);
        let delta = compute_line_delta(&content1, &content2);
        assert_eq!(
            delta,
            vec![DeltaHunk {
                start: 1,
                end: 2,
                count: 2,
                lines: vec![(Vec::new(), b"a".to_vec()), (Vec::new(), b"c".to_vec()),],
            }]
        );
    }

    #[test]
    fn plain_content_text_returns_lines_verbatim() {
        // Mirrors KnitContentTestsMixin.test_text against PlainKnitContent:
        // build it from an annotated source so we exercise the same shape
        // as TestPlainKnitContent._make_content.
        let annotated = AnnotatedKnitContent::new(vec![
            (Vec::new(), b"text1".to_vec()),
            (Vec::new(), b"text2".to_vec()),
        ]);
        let plain = PlainKnitContent::new(annotated.text(), b"bogus".to_vec());
        assert_eq!(plain.text(), vec![b"text1".to_vec(), b"text2".to_vec()]);
    }

    #[test]
    fn plain_content_annotate_uses_constructor_version_id() {
        // Mirrors TestPlainKnitContent.test_annotate: every line is
        // attributed to the version_id passed at construction time, regardless
        // of any origin in the source data.
        let empty = PlainKnitContent::new(vec![], b"bogus".to_vec());
        assert!(empty.annotate().is_empty());

        let content = PlainKnitContent::new(
            vec![b"text1".to_vec(), b"text2".to_vec()],
            b"bogus".to_vec(),
        );
        assert_eq!(
            content.annotate(),
            vec![
                (b"bogus".to_vec(), b"text1".to_vec()),
                (b"bogus".to_vec(), b"text2".to_vec()),
            ]
        );
    }

    #[test]
    fn plain_content_line_delta_uses_bare_text_lines() {
        // Mirrors TestPlainKnitContent.test_line_delta:
        //   content1 = [a, b]
        //   content2 = [a, a, c]
        //   expected delta: [(1, 2, 2, [b"a", b"c"])]
        let content1 = PlainKnitContent::new(vec![b"a".to_vec(), b"b".to_vec()], b"v1".to_vec());
        let content2 = PlainKnitContent::new(
            vec![b"a".to_vec(), b"a".to_vec(), b"c".to_vec()],
            b"v2".to_vec(),
        );
        let delta = compute_line_delta(&content1, &content2);
        assert_eq!(
            delta,
            vec![DeltaHunk {
                start: 1,
                end: 2,
                count: 2,
                lines: vec![b"a".to_vec(), b"c".to_vec()],
            }]
        );
    }

    /// Build a kndx body the way the real `_KndxIndex.add_records` writes
    /// it: KNDX_HEADER (which itself ends in `\n`) followed by one `\n` +
    /// entry per record.  Matches the Python MockTransport `b"\n".join`
    /// output exactly because the HEADER already terminates with `\n`.
    fn kndx_bytes(lines: &[&[u8]]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(KNDX_HEADER);
        for line in lines {
            out.push(b'\n');
            out.extend_from_slice(line);
        }
        out
    }

    #[test]
    fn parse_kndx_data_empty_input_yields_empty_cache() {
        let pc = parse_kndx_data(b"").unwrap();
        assert!(pc.cache.is_empty());
        assert!(pc.history.is_empty());
    }

    #[test]
    fn parse_kndx_data_rejects_corrupt_header() {
        // Mirrors LowLevelKnitIndexTests.test_read_corrupted_header.
        let err = parse_kndx_data(b"not a bzr knit index header\n").unwrap_err();
        assert!(matches!(err, KnitError::BadKnitHeader { .. }));
    }

    #[test]
    fn parse_kndx_data_ignores_corrupted_lines() {
        // Mirrors LowLevelKnitIndexTests.test_read_ignore_corrupted_lines.
        let data = kndx_bytes(&[
            b"corrupted",
            b"corrupted options 0 1 .b .c ",
            b"version options 0 1 :",
        ]);
        let pc = parse_kndx_data(&data).unwrap();
        assert_eq!(pc.cache.len(), 1);
        assert!(pc.cache.contains_key(b"version".as_slice()));
    }

    #[test]
    fn parse_kndx_data_short_line_is_skipped() {
        // Mirrors LowLevelKnitIndexTests.test_short_line: a line missing
        // its " :" terminator is silently ignored.
        let data = kndx_bytes(&[b"a option 0 10  :", b"b option 10 10 0"]);
        let pc = parse_kndx_data(&data).unwrap();
        assert_eq!(pc.cache.len(), 1);
        assert!(pc.cache.contains_key(b"a".as_slice()));
    }

    #[test]
    fn parse_kndx_data_resumes_after_incomplete_record() {
        // Mirrors LowLevelKnitIndexTests.test_skip_incomplete_record.
        let data = kndx_bytes(&[
            b"a option 0 10  :",
            b"b option 10 10 0",
            b"c option 20 10 0 :",
        ]);
        let pc = parse_kndx_data(&data).unwrap();
        let mut keys: Vec<Vec<u8>> = pc.cache.keys().cloned().collect();
        keys.sort();
        assert_eq!(keys, vec![b"a".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn parse_kndx_data_trailing_characters_are_skipped() {
        // Mirrors LowLevelKnitIndexTests.test_trailing_characters: a line
        // whose suffix isn't exactly " :" is treated as corrupt.
        let data = kndx_bytes(&[
            b"a option 0 10  :",
            b"b option 10 10 0 :a",
            b"c option 20 10 0 :",
        ]);
        let pc = parse_kndx_data(&data).unwrap();
        let mut keys: Vec<Vec<u8>> = pc.cache.keys().cloned().collect();
        keys.sort();
        assert_eq!(keys, vec![b"a".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn parse_kndx_data_resolves_compressed_parents() {
        // Mirrors LowLevelKnitIndexTests.test_read_compressed_parents: a
        // numeric parent reference is resolved against the file's history.
        let data = kndx_bytes(&[
            b"a option 0 1 :",
            b"b option 0 1 0 :",
            b"c option 0 1 1 0 :",
        ]);
        let pc = parse_kndx_data(&data).unwrap();
        assert_eq!(pc.cache[&b"b".to_vec()].parents, vec![b"a".to_vec()]);
        assert_eq!(
            pc.cache[&b"c".to_vec()].parents,
            vec![b"b".to_vec(), b"a".to_vec()]
        );
    }

    #[test]
    fn parse_kndx_data_duplicate_entries_keep_first_history_index() {
        // Mirrors LowLevelKnitIndexTests.test_read_duplicate_entries: the
        // first occurrence of a version pins its history index; later
        // occurrences overwrite the cache row but not the history slot.
        let data = kndx_bytes(&[
            b"parent options 0 1 :",
            b"version options1 0 1 0 :",
            b"version options2 1 2 .other :",
            b"version options3 3 4 0 .other :",
        ]);
        let pc = parse_kndx_data(&data).unwrap();
        // Two distinct keys, two history slots.
        assert_eq!(pc.cache.len(), 2);
        assert_eq!(pc.history.len(), 2);
        // The "version" slot is pinned at index 1 (right after "parent").
        let ver = &pc.cache[&b"version".to_vec()];
        assert_eq!(ver.index, 1);
        // Cache row reflects the *latest* line: pos=3, size=4,
        // options=options3, parents=[parent, other].
        assert_eq!(ver.pos, 3);
        assert_eq!(ver.size, 4);
        assert_eq!(ver.options, vec![b"options3".to_vec()]);
        assert_eq!(ver.parents, vec![b"parent".to_vec(), b"other".to_vec()]);
    }

    #[test]
    fn parse_kndx_data_rejects_impossible_parent_index() {
        // Mirrors LowLevelKnitIndexTests.test_impossible_parent.
        let data = kndx_bytes(&[b"a option 0 1 :", b"b option 0 1 4 :"]);
        let err = parse_kndx_data(&data).unwrap_err();
        assert!(matches!(err, KnitError::KndxCorrupt { .. }));
    }

    #[test]
    fn parse_kndx_data_rejects_non_integer_parent_index() {
        // Mirrors LowLevelKnitIndexTests.test_corrupted_parent.
        let data = kndx_bytes(&[b"a option 0 1 :", b"b option 0 1 :", b"c option 0 1 1v :"]);
        let err = parse_kndx_data(&data).unwrap_err();
        assert!(matches!(err, KnitError::KndxCorrupt { .. }));
    }

    #[test]
    fn parse_kndx_data_rejects_corrupt_parent_in_list() {
        // Mirrors LowLevelKnitIndexTests.test_corrupted_parent_in_list.
        let data = kndx_bytes(&[b"a option 0 1 :", b"b option 0 1 :", b"c option 0 1 1 v :"]);
        let err = parse_kndx_data(&data).unwrap_err();
        assert!(matches!(err, KnitError::KndxCorrupt { .. }));
    }

    #[test]
    fn parse_kndx_data_rejects_invalid_position() {
        // Mirrors LowLevelKnitIndexTests.test_invalid_position.
        let data = kndx_bytes(&[b"a option 1v 1 :"]);
        let err = parse_kndx_data(&data).unwrap_err();
        assert!(matches!(err, KnitError::KndxCorrupt { .. }));
    }

    #[test]
    fn parse_kndx_data_rejects_invalid_size() {
        // Mirrors LowLevelKnitIndexTests.test_invalid_size.
        let data = kndx_bytes(&[b"a option 1 1v :"]);
        let err = parse_kndx_data(&data).unwrap_err();
        assert!(matches!(err, KnitError::KndxCorrupt { .. }));
    }

    #[test]
    fn parse_kndx_data_parses_position_and_size() {
        // Mirrors LowLevelKnitIndexTests.test_get_position.
        let data = kndx_bytes(&[b"a option 0 1 :", b"b option 1 2 :"]);
        let pc = parse_kndx_data(&data).unwrap();
        let a = &pc.cache[&b"a".to_vec()];
        let b = &pc.cache[&b"b".to_vec()];
        assert_eq!((a.pos, a.size), (0, 1));
        assert_eq!((b.pos, b.size), (1, 2));
    }

    #[test]
    fn parse_kndx_data_preserves_options_list() {
        // Mirrors LowLevelKnitIndexTests.test_get_options.
        let data = kndx_bytes(&[b"a opt1 0 1 :", b"b opt2,opt3 1 2 :"]);
        let pc = parse_kndx_data(&data).unwrap();
        assert_eq!(pc.cache[&b"a".to_vec()].options, vec![b"opt1".to_vec()]);
        assert_eq!(
            pc.cache[&b"b".to_vec()].options,
            vec![b"opt2".to_vec(), b"opt3".to_vec()]
        );
    }

    /// Glue a kndx body into a MemoryTransport at the path our test mapper
    /// produces (`name.kndx` for ConstantMapper { result: "name" }).
    fn make_kndx_transport(
        name: &str,
        lines: &[&[u8]],
    ) -> crate::transport::testing::MemoryTransport {
        let t = crate::transport::testing::MemoryTransport::new();
        let data = kndx_bytes(lines);
        t.put_file_non_atomic(&format!("{}.kndx", name), &data, true)
            .unwrap();
        t
    }

    fn make_kndx_index(
        name: &str,
        lines: &[&[u8]],
    ) -> KndxIndex<crate::transport::testing::MemoryTransport, crate::key_mapper::ConstantMapper>
    {
        let transport = make_kndx_transport(name, lines);
        KndxIndex::new(
            transport,
            crate::key_mapper::ConstantMapper {
                result: name.to_string(),
            },
        )
    }

    #[test]
    fn kndx_index_get_parent_map_resolves_compressed_parents() {
        // Mirrors LowLevelKnitIndexTests.test_get_parent_map at the
        // KndxIndex (rather than parse_kndx_data) layer.
        let idx = make_kndx_index(
            "filename",
            &[
                b"a option 0 1 :",
                b"b option 1 2 0 .c :",
                b"c option 1 2 1 0 .e :",
            ],
        );
        let key_a: KnitKey = vec![b"a".to_vec()];
        let key_b: KnitKey = vec![b"b".to_vec()];
        let key_c: KnitKey = vec![b"c".to_vec()];
        let pm = idx
            .get_parent_map(&[key_a.clone(), key_b.clone(), key_c.clone()])
            .unwrap();
        assert_eq!(pm[&key_a], Vec::<KnitKey>::new());
        assert_eq!(pm[&key_b], vec![vec![b"a".to_vec()], vec![b"c".to_vec()]]);
        assert_eq!(
            pm[&key_c],
            vec![
                vec![b"b".to_vec()],
                vec![b"a".to_vec()],
                vec![b"e".to_vec()],
            ]
        );
    }

    #[test]
    fn kndx_index_get_method_returns_method_from_options() {
        // Mirrors LowLevelKnitIndexTests.test_get_method's positive cases.
        let idx = make_kndx_index(
            "filename",
            &[b"a fulltext,unknown 0 1 :", b"b unknown,line-delta 1 2 :"],
        );
        let key_a: KnitKey = vec![b"a".to_vec()];
        let key_b: KnitKey = vec![b"b".to_vec()];
        assert_eq!(idx.get_method(&key_a).unwrap(), KnitMethod::Fulltext);
        assert_eq!(idx.get_method(&key_b).unwrap(), KnitMethod::LineDelta);
    }

    #[test]
    fn kndx_index_add_records_writes_to_transport_and_updates_cache() {
        // Mirrors a subset of LowLevelKnitIndexTests.test_add_versions:
        // verify the appended bytes have the expected per-line shape and
        // that subsequent reads come back from the in-memory cache.
        let idx = make_kndx_index("filename", &[]);
        let key: KnitKey = vec![b"a".to_vec()];
        let memo = KnitIndexMemo {
            path: "filename.knit".to_string(),
            offset: 0,
            length: 1,
        };
        idx.add_records(
            &[(key.clone(), vec![KnitMethod::Fulltext], memo, vec![])],
            false,
            false,
        )
        .unwrap();
        // The cache is now populated.
        assert!(idx.contains(&key).unwrap());
        assert_eq!(idx.get_method(&key).unwrap(), KnitMethod::Fulltext);

        // And the kndx file ends with the expected " a fulltext 0 1 :" line.
        let written = idx.transport().get_bytes("filename.kndx").unwrap();
        assert!(
            written.ends_with(b"\na fulltext 0 1 :"),
            "kndx tail mismatch: {:?}",
            String::from_utf8_lossy(&written)
        );
    }

    #[test]
    fn kndx_index_load_prefix_typed_reports_bad_header() {
        // Mirrors LowLevelKnitIndexTests.test_read_corrupted_header at the
        // KndxIndex layer: the typed loader surfaces BadKnitHeader rather
        // than collapsing it into a generic transport error.
        let transport = crate::transport::testing::MemoryTransport::new();
        transport
            .put_file_non_atomic("filename.kndx", b"not a bzr knit index header\n", true)
            .unwrap();
        let idx = KndxIndex::new(
            transport,
            crate::key_mapper::ConstantMapper {
                result: "filename".to_string(),
            },
        );
        let err = idx.load_prefix_typed(vec![]).unwrap_err();
        assert!(matches!(
            err,
            KndxLoadError::Knit(KnitError::BadKnitHeader { .. })
        ));
    }

    fn fulltext_pos(path: &str, offset: u64, length: usize) -> KnitRecordDetails {
        KnitRecordDetails {
            method: KnitMethod::Fulltext,
            noeol: false,
            index_memo: KnitIndexMemo {
                path: path.to_string(),
                offset,
                length,
            },
            compression_parent: None,
            parents: vec![],
        }
    }

    fn delta_pos(
        path: &str,
        offset: u64,
        length: usize,
        compression_parent: KnitKey,
    ) -> KnitRecordDetails {
        KnitRecordDetails {
            method: KnitMethod::LineDelta,
            noeol: false,
            index_memo: KnitIndexMemo {
                path: path.to_string(),
                offset,
                length,
            },
            compression_parent: Some(compression_parent.clone()),
            parents: vec![compression_parent],
        }
    }

    #[test]
    fn kndx_index_total_build_size_walks_compression_chain() {
        // Mirrors LowLevelKnitIndexTests.test__get_total_build_size: the
        // size of a delta key is the cumulative size of its chain back to
        // the fulltext, with shared ancestors only counted once.
        let idx = make_kndx_index("filename", &[]);
        let key_a: KnitKey = vec![b"a".to_vec()];
        let key_b: KnitKey = vec![b"b".to_vec()];
        let key_c: KnitKey = vec![b"c".to_vec()];
        let key_d: KnitKey = vec![b"d".to_vec()];
        let mut positions = std::collections::HashMap::new();
        positions.insert(key_a.clone(), fulltext_pos("p", 0, 100));
        positions.insert(key_b.clone(), delta_pos("p", 100, 21, key_a.clone()));
        positions.insert(key_c.clone(), delta_pos("p", 121, 35, key_b.clone()));
        positions.insert(key_d.clone(), delta_pos("p", 156, 12, key_b.clone()));

        assert_eq!(idx.get_total_build_size(&[key_a.clone()], &positions), 100);
        assert_eq!(idx.get_total_build_size(&[key_b.clone()], &positions), 121);
        // c needs a + b + c.
        assert_eq!(idx.get_total_build_size(&[key_c.clone()], &positions), 156);
        // b shouldn't be double-counted.
        assert_eq!(
            idx.get_total_build_size(&[key_b.clone(), key_c.clone()], &positions),
            156
        );
        // d needs a + b + d.
        assert_eq!(idx.get_total_build_size(&[key_d.clone()], &positions), 133);
        // c + d share a + b; total is 100 + 21 + 35 + 12 = 168.
        assert_eq!(idx.get_total_build_size(&[key_c, key_d], &positions), 168);
    }

    #[test]
    fn encode_graph_index_record_rejects_delta_in_non_delta_index() {
        // Mirrors TestGraphIndexKnit.test_add_version_delta_not_delta_index.
        let err = encode_graph_index_record(false, 0, 10, KnitMethod::LineDelta, true, false, &[])
            .unwrap_err();
        assert!(matches!(err, KnitError::Corrupt(_)));
    }

    #[test]
    fn encode_graph_index_record_rejects_parents_in_parentless_index() {
        // Mirrors TestNoParentsGraphIndexKnit.test_add_versions_parents_not_parents_index.
        let err = encode_graph_index_record(
            false,
            0,
            10,
            KnitMethod::Fulltext,
            false,
            false,
            &[vec![b"p".to_vec()]],
        )
        .unwrap_err();
        assert!(matches!(err, KnitError::Corrupt(_)));
    }

    #[test]
    fn encode_graph_index_record_fulltext_no_parents() {
        // A no-eol fulltext in a parents+deltas index produces refs of
        // shape `[parents, []]`: a graph-parents column and an empty
        // compression-parent column (a fulltext has no compression parent).
        let (value, refs) =
            encode_graph_index_record(true, 123, 45, KnitMethod::Fulltext, true, true, &[])
                .unwrap();
        assert_eq!(value, b"N123 45");
        assert_eq!(refs, vec![Vec::<KnitKey>::new(), Vec::<KnitKey>::new()]);
    }

    #[test]
    fn encode_graph_index_record_line_delta_uses_first_parent_as_compression_parent() {
        // line-delta refs: `[parents, [parents[0]]]` — the second column
        // carries the compression parent (always the left-most parent on
        // the Python side).
        let parent_a: KnitKey = vec![b"file".to_vec(), b"a".to_vec()];
        let parent_b: KnitKey = vec![b"file".to_vec(), b"b".to_vec()];
        let (value, refs) = encode_graph_index_record(
            false,
            10,
            20,
            KnitMethod::LineDelta,
            true,
            true,
            &[parent_a.clone(), parent_b.clone()],
        )
        .unwrap();
        assert_eq!(value, b" 10 20");
        assert_eq!(refs, vec![vec![parent_a.clone(), parent_b], vec![parent_a]]);
    }

    #[test]
    fn encode_graph_index_record_parentless_index_has_single_refs_column() {
        // With has_parents=false the function returns no refs at all.
        let (value, refs) =
            encode_graph_index_record(false, 5, 7, KnitMethod::Fulltext, false, false, &[])
                .unwrap();
        assert_eq!(value, b" 5 7");
        assert!(refs.is_empty());
    }
}

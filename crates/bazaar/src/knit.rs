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

// ============================================================
// In-memory knit content layer
// ============================================================

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
    /// Whether the trailing `\n` on the last line should be stripped on
    /// output. Mirrors the Python `_should_strip_eol` flag.
    fn should_strip_eol(&self) -> bool;
    /// Set the strip-eol flag.
    fn set_should_strip_eol(&mut self, strip: bool);

    /// Apply a plain (origin-stripped) line delta in place.
    ///
    /// Each hunk replaces lines `[offset+start .. offset+end]` with the
    /// hunk's payload, where `offset` accumulates as the running cursor
    /// adjustment from the prior hunks (`offset += start - end + count`).
    /// `new_version_id` is only meaningful for [`PlainKnitContent`],
    /// which records it as its new owning version; annotated content
    /// ignores it because each line carries its own origin already.
    fn apply_delta(&mut self, delta: &[DeltaHunk<Vec<u8>>], new_version_id: &[u8]);

    /// Return just the text lines (without origin annotations). If
    /// `should_strip_eol` is set, the trailing `\n` of the last line is
    /// removed in the returned copy.
    fn text(&self) -> Vec<Vec<u8>>;

    /// Return `(origin, text)` pairs. For [`PlainKnitContent`] the
    /// `origin` is always the content's `version_id`.
    fn annotate(&self) -> Vec<AnnotatedLine>;
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
    fn should_strip_eol(&self) -> bool {
        self.should_strip_eol
    }

    fn set_should_strip_eol(&mut self, strip: bool) {
        self.should_strip_eol = strip;
    }

    fn apply_delta(&mut self, delta: &[DeltaHunk<Vec<u8>>], _new_version_id: &[u8]) {
        // Plain delta: lines are bare bytes, no origins. We can't
        // recover the original origin so callers that hand us a plain
        // delta should be feeding us lines that came from the parent
        // chain via a separate annotation step. The Python original has
        // the same restriction: AnnotatedKnitContent.apply_delta takes
        // a plain delta and just splices the bytes in. We mirror that
        // by attaching an empty origin to each spliced line — callers
        // can re-annotate if they need to.
        let mut offset: isize = 0;
        for hunk in delta {
            let start = (offset + hunk.start as isize) as usize;
            let end = (offset + hunk.end as isize) as usize;
            let new_pairs: Vec<AnnotatedLine> =
                hunk.lines.iter().map(|l| (Vec::new(), l.clone())).collect();
            self.lines.splice(start..end, new_pairs);
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
        self.lines
            .iter()
            .map(|l| (self.version_id.clone(), l.clone()))
            .collect()
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

    /// Parse a delta record's body into the plain (origin-stripped)
    /// hunk shape that [`KnitContent::apply_delta`] consumes.
    fn parse_line_delta(&self, lines: &[&[u8]]) -> Result<Vec<DeltaHunk<Vec<u8>>>, KnitError>;

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

    fn parse_line_delta(&self, lines: &[&[u8]]) -> Result<Vec<DeltaHunk<Vec<u8>>>, KnitError> {
        // Parse with the annotated parser but keep only the text bytes
        // from each hunk line — same as the Python factory's
        // `parse_line_delta(plain=True)` mode.
        parse_line_delta_plain(lines)
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

/// Whether a knit record is a fulltext or a line-delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KnitMethod {
    Fulltext,
    LineDelta,
}

impl KnitMethod {
    /// The historical Python-facing name of this method, used in the
    /// `record_details` tuple returned by `_KnitGraphIndex.get_build_details`.
    pub fn as_str(self) -> &'static str {
        match self {
            KnitMethod::Fulltext => "fulltext",
            KnitMethod::LineDelta => "line-delta",
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn refs<'a>(v: &'a [Vec<u8>]) -> Vec<&'a [u8]> {
        v.iter().map(|l| l.as_slice()).collect()
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
            lines: vec![b"B\n".to_vec(), b"C\n".to_vec()],
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
}

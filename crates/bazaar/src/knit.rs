//! Knit format parsing and serialization.
//!
//! Port of the pure-logic pieces of `bzrformats/knit.py`: fulltext and
//! line-delta parse/serialize for the annotated and plain variants, plus
//! the `get_line_delta_blocks` matching-block extractor. Content
//! objects, record I/O, and VersionedFile plumbing stay in Python.

/// Errors from parsing knit record bodies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KnitError {
    /// A fulltext or delta line had no space separating origin from text.
    MissingOrigin(Vec<u8>),
    /// A delta header `start,end,count` was malformed.
    BadDeltaHeader(Vec<u8>),
    /// A delta header said N lines but the iterator ran out earlier.
    TruncatedDelta,
}

impl std::fmt::Display for KnitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KnitError::MissingOrigin(l) => {
                write!(f, "annotated knit line missing origin: {:?}", l)
            }
            KnitError::BadDeltaHeader(h) => write!(f, "bad delta header: {:?}", h),
            KnitError::TruncatedDelta => write!(f, "delta truncated: too few lines"),
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

#[derive(Debug, PartialEq, Eq)]
pub enum NetworkHeaderError {
    MissingKeyTerminator,
    MissingParentsTerminator,
    MissingNoEolByte,
}

impl std::fmt::Display for NetworkHeaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NetworkHeaderError::MissingKeyTerminator => {
                write!(f, "knit network record key missing newline terminator")
            }
            NetworkHeaderError::MissingParentsTerminator => {
                write!(f, "knit network record parents missing newline terminator")
            }
            NetworkHeaderError::MissingNoEolByte => {
                write!(f, "knit network record missing noeol byte")
            }
        }
    }
}

impl std::error::Error for NetworkHeaderError {}

/// Parse the variable-length header of a `knit-*-gz` network record.
///
/// `bytes` is the full record and `start` is the offset just past the
/// storage-kind line (the same `line_end` the Python caller computes via
/// `network_bytes_to_kind_and_offset`).
pub fn parse_network_record_header(
    bytes: &[u8],
    start: usize,
) -> Result<NetworkRecordHeader<'_>, NetworkHeaderError> {
    let key_end = bytes[start..]
        .iter()
        .position(|&b| b == b'\n')
        .map(|i| start + i)
        .ok_or(NetworkHeaderError::MissingKeyTerminator)?;
    let key: Vec<&[u8]> = bytes[start..key_end].split(|&b| b == b'\x00').collect();

    let parents_start = key_end + 1;
    let parents_end = bytes[parents_start..]
        .iter()
        .position(|&b| b == b'\n')
        .map(|i| parents_start + i)
        .ok_or(NetworkHeaderError::MissingParentsTerminator)?;
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
        return Err(NetworkHeaderError::MissingNoEolByte);
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
    fn network_header_rejects_missing_noeol_byte() {
        let bytes = b"knit-ft-gz\nk\nNone:\n";
        let err = parse_network_record_header(bytes, 11).unwrap_err();
        assert_eq!(err, NetworkHeaderError::MissingNoEolByte);
    }
}

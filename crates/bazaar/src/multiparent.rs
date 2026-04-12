//! Multi-parent diff representation.
//!
//! Port of the pure-logic pieces of `bzrformats/multiparent.py`: the
//! [`MultiParent`] container, its [`Hunk`] variants, and the patch
//! serialization format. Construction from line lists (which depends on
//! patiencediff) and the `VersionedFile` wrappers (which do I/O) remain in
//! Python for now.

/// One hunk of a multi-parent diff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Hunk {
    /// Lines introduced by this text (not present in any parent).
    NewText(Vec<Vec<u8>>),
    /// A reference to a run of lines in one of the parent texts.
    ParentText {
        parent: usize,
        parent_pos: usize,
        child_pos: usize,
        num_lines: usize,
    },
}

/// A multi-parent diff: an ordered sequence of [`Hunk`]s.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MultiParent {
    pub hunks: Vec<Hunk>,
}

/// Error returned when [`MultiParent::from_patch`] fails to parse input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    /// A header line started with an unexpected byte.
    UnexpectedChar(u8),
    /// An `i N` or `c ...` header could not be parsed.
    BadHeader(Vec<u8>),
    /// A NewText header promised more lines than the input contained.
    Truncated,
    /// A `\n` continuation line appeared with no preceding NewText hunk.
    OrphanContinuation,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::UnexpectedChar(c) => write!(f, "unexpected leading byte {:#x}", c),
            ParseError::BadHeader(h) => write!(f, "bad header line: {:?}", h),
            ParseError::Truncated => write!(f, "truncated patch"),
            ParseError::OrphanContinuation => write!(f, "continuation line with no NewText"),
        }
    }
}

impl std::error::Error for ParseError {}

impl MultiParent {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_hunks(hunks: Vec<Hunk>) -> Self {
        Self { hunks }
    }

    /// Total number of lines in the reconstructed text.
    ///
    /// Mirrors Python's `num_lines`: a trailing ParentText carries absolute
    /// positioning, so we scan from the end summing NewText lengths until we
    /// hit one.
    pub fn num_lines(&self) -> usize {
        let mut extra = 0usize;
        for hunk in self.hunks.iter().rev() {
            match hunk {
                Hunk::ParentText {
                    child_pos,
                    num_lines,
                    ..
                } => return child_pos + num_lines + extra,
                Hunk::NewText(lines) => extra += lines.len(),
            }
        }
        extra
    }

    /// True when this diff is effectively a fulltext (one NewText hunk).
    pub fn is_snapshot(&self) -> bool {
        matches!(self.hunks.as_slice(), [Hunk::NewText(_)])
    }

    /// Serialize to the patch wire format, yielding one byte chunk per line.
    pub fn to_patch(&self) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        for hunk in &self.hunks {
            match hunk {
                Hunk::NewText(lines) => {
                    out.push(format!("i {}\n", lines.len()).into_bytes());
                    for line in lines {
                        out.push(line.clone());
                    }
                    out.push(b"\n".to_vec());
                }
                Hunk::ParentText {
                    parent,
                    parent_pos,
                    child_pos,
                    num_lines,
                } => {
                    out.push(
                        format!("c {} {} {} {}\n", parent, parent_pos, child_pos, num_lines)
                            .into_bytes(),
                    );
                }
            }
        }
        out
    }

    /// Length in bytes of the serialized patch.
    pub fn patch_len(&self) -> usize {
        self.to_patch().iter().map(|l| l.len()).sum()
    }

    /// Parse a patch (as a single byte slice) back into a [`MultiParent`].
    pub fn from_patch(text: &[u8]) -> Result<Self, ParseError> {
        Self::from_patch_lines(split_lines(text))
    }

    fn from_patch_lines(lines: Vec<&[u8]>) -> Result<Self, ParseError> {
        let mut hunks: Vec<Hunk> = Vec::new();
        let mut i = 0;
        while i < lines.len() {
            let cur = lines[i];
            i += 1;
            let first = match cur.first().copied() {
                Some(c) => c,
                None => return Err(ParseError::BadHeader(cur.to_vec())),
            };
            match first {
                b'i' => {
                    let n = parse_usize_after_space(cur)?;
                    if i + n > lines.len() {
                        return Err(ParseError::Truncated);
                    }
                    let mut hunk_lines: Vec<Vec<u8>> =
                        lines[i..i + n].iter().map(|s| s.to_vec()).collect();
                    i += n;
                    // Python strips the trailing '\n' from the final inserted
                    // line; `to_patch` emits a bare '\n' separator afterwards,
                    // which round-trips back via the '\n' continuation branch.
                    if let Some(last) = hunk_lines.last_mut() {
                        if last.last() == Some(&b'\n') {
                            last.pop();
                        }
                    }
                    hunks.push(Hunk::NewText(hunk_lines));
                }
                b'\n' => match hunks.last_mut() {
                    Some(Hunk::NewText(lines)) => {
                        if let Some(last) = lines.last_mut() {
                            last.push(b'\n');
                        } else {
                            return Err(ParseError::OrphanContinuation);
                        }
                    }
                    _ => return Err(ParseError::OrphanContinuation),
                },
                b'c' => {
                    let (parent, parent_pos, child_pos, num_lines) = parse_c_header(cur)?;
                    hunks.push(Hunk::ParentText {
                        parent,
                        parent_pos,
                        child_pos,
                        num_lines,
                    });
                }
                other => return Err(ParseError::UnexpectedChar(other)),
            }
        }
        Ok(MultiParent { hunks })
    }

    /// Iterate the hunks alongside their `[start, end)` line ranges.
    ///
    /// Yields `(start, end, kind)` where kind is either the new lines or a
    /// reference tuple `(parent, parent_start, parent_end)`. Mirrors Python's
    /// `range_iterator`.
    pub fn range_iterator(&self) -> Vec<RangeItem<'_>> {
        let mut out = Vec::with_capacity(self.hunks.len());
        let mut start = 0usize;
        for hunk in &self.hunks {
            match hunk {
                Hunk::NewText(lines) => {
                    let end = start + lines.len();
                    out.push(RangeItem {
                        start,
                        end,
                        data: RangeData::New(lines),
                    });
                    start = end;
                }
                Hunk::ParentText {
                    parent,
                    parent_pos,
                    child_pos,
                    num_lines,
                } => {
                    let end = child_pos + num_lines;
                    out.push(RangeItem {
                        start: *child_pos,
                        end,
                        data: RangeData::Parent {
                            parent: *parent,
                            parent_start: *parent_pos,
                            parent_end: parent_pos + num_lines,
                        },
                    });
                    start = end;
                }
            }
        }
        out
    }

    /// Yield matching blocks for a specific parent, terminating with the
    /// conventional `(parent_len, child_len, 0)` sentinel.
    pub fn matching_blocks(&self, parent: usize, parent_len: usize) -> Vec<(usize, usize, usize)> {
        let mut out = Vec::new();
        for hunk in &self.hunks {
            if let Hunk::ParentText {
                parent: p,
                parent_pos,
                child_pos,
                num_lines,
            } = hunk
            {
                if *p == parent {
                    out.push((*parent_pos, *child_pos, *num_lines));
                }
            }
        }
        out.push((parent_len, self.num_lines(), 0));
        out
    }
}

/// Borrowed view of a single entry yielded by [`MultiParent::range_iterator`].
#[derive(Debug, PartialEq, Eq)]
pub struct RangeItem<'a> {
    pub start: usize,
    pub end: usize,
    pub data: RangeData<'a>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum RangeData<'a> {
    New(&'a [Vec<u8>]),
    Parent {
        parent: usize,
        parent_start: usize,
        parent_end: usize,
    },
}

/// Split bytes the same way Python's `BytesIO.readlines()` does: each line
/// keeps its trailing `\n`, except possibly the last.
fn split_lines(data: &[u8]) -> Vec<&[u8]> {
    let mut out = Vec::new();
    let mut start = 0;
    for (i, &b) in data.iter().enumerate() {
        if b == b'\n' {
            out.push(&data[start..=i]);
            start = i + 1;
        }
    }
    if start < data.len() {
        out.push(&data[start..]);
    }
    out
}

fn parse_usize_after_space(line: &[u8]) -> Result<usize, ParseError> {
    let rest = line
        .iter()
        .position(|&b| b == b' ')
        .map(|p| &line[p + 1..])
        .ok_or_else(|| ParseError::BadHeader(line.to_vec()))?;
    let end = rest
        .iter()
        .position(|&b| b == b' ' || b == b'\n')
        .unwrap_or(rest.len());
    std::str::from_utf8(&rest[..end])
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .ok_or_else(|| ParseError::BadHeader(line.to_vec()))
}

fn parse_c_header(line: &[u8]) -> Result<(usize, usize, usize, usize), ParseError> {
    let trimmed = if line.last() == Some(&b'\n') {
        &line[..line.len() - 1]
    } else {
        line
    };
    let s = std::str::from_utf8(trimmed).map_err(|_| ParseError::BadHeader(line.to_vec()))?;
    let mut parts = s.split(' ');
    let tag = parts.next();
    if tag != Some("c") {
        return Err(ParseError::BadHeader(line.to_vec()));
    }
    let mut next_num = || -> Result<usize, ParseError> {
        parts
            .next()
            .and_then(|p| p.parse::<usize>().ok())
            .ok_or_else(|| ParseError::BadHeader(line.to_vec()))
    };
    let parent = next_num()?;
    let parent_pos = next_num()?;
    let child_pos = next_num()?;
    let num_lines = next_num()?;
    if parts.next().is_some() {
        return Err(ParseError::BadHeader(line.to_vec()));
    }
    Ok((parent, parent_pos, child_pos, num_lines))
}

/// Topologically sort `versions` given a `parents` mapping.
///
/// Port of `multiparent._topo_iter`. `parents[v]` is either `Some(parents)`
/// or `None` for a "parentless" sentinel (treated as having no parents).
/// Keys in `parents` not present in `versions` are ignored when counting
/// pending predecessors. Returns versions in an order where every version
/// appears after its parents that are also in the input set.
///
/// Input ordering of `versions` is used as a tiebreaker so the output is
/// deterministic. Duplicate entries in `versions` are emitted only once.
pub fn topo_iter<K>(
    parents: &std::collections::HashMap<K, Option<Vec<K>>>,
    versions: &[K],
) -> Vec<K>
where
    K: std::hash::Hash + Eq + Clone,
{
    let mut version_order: Vec<K> = Vec::with_capacity(versions.len());
    let mut version_set: std::collections::HashSet<K> = std::collections::HashSet::new();
    for v in versions {
        if version_set.insert(v.clone()) {
            version_order.push(v.clone());
        }
    }

    let mut seen: std::collections::HashSet<K> = std::collections::HashSet::new();
    let mut descendants: std::collections::HashMap<K, Vec<K>> = std::collections::HashMap::new();

    let pending_count = |v: &K, seen: &std::collections::HashSet<K>| -> usize {
        match parents.get(v) {
            Some(Some(ps)) => ps
                .iter()
                .filter(|p| version_set.contains(*p) && !seen.contains(*p))
                .count(),
            _ => 0,
        }
    };

    for v in &version_order {
        if let Some(Some(ps)) = parents.get(v) {
            for p in ps {
                descendants.entry(p.clone()).or_default().push(v.clone());
            }
        }
    }

    let mut cur: Vec<K> = version_order
        .iter()
        .filter(|v| pending_count(v, &seen) == 0)
        .cloned()
        .collect();

    let mut out: Vec<K> = Vec::new();
    while !cur.is_empty() {
        let mut next: Vec<K> = Vec::new();
        for v in &cur {
            if seen.contains(v) {
                continue;
            }
            if pending_count(v, &seen) != 0 {
                continue;
            }
            if let Some(ds) = descendants.get(v) {
                next.extend(ds.iter().cloned());
            }
            out.push(v.clone());
            seen.insert(v.clone());
        }
        cur = next;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines(s: &[&[u8]]) -> Vec<Vec<u8>> {
        s.iter().map(|l| l.to_vec()).collect()
    }

    #[test]
    fn new_text_to_patch() {
        let mp = MultiParent::with_hunks(vec![Hunk::NewText(lines(&[b"a\n"]))]);
        assert_eq!(
            mp.to_patch(),
            vec![b"i 1\n".to_vec(), b"a\n".to_vec(), b"\n".to_vec()]
        );
    }

    #[test]
    fn empty_new_text_to_patch() {
        // Mirrors test_multiparent.TestNewText.test_to_patch empty case.
        let mp = MultiParent::with_hunks(vec![Hunk::NewText(vec![])]);
        assert_eq!(mp.to_patch(), vec![b"i 0\n".to_vec(), b"\n".to_vec()]);
    }

    #[test]
    fn new_text_line_without_trailing_newline_to_patch() {
        // Mirrors test_multiparent.TestNewText.test_to_patch `[b"a"]` case —
        // `to_patch` must emit the bare `b"\n"` separator regardless of
        // whether the final payload line itself ends in `\n`.
        let mp = MultiParent::with_hunks(vec![Hunk::NewText(lines(&[b"a"]))]);
        assert_eq!(
            mp.to_patch(),
            vec![b"i 1\n".to_vec(), b"a".to_vec(), b"\n".to_vec()]
        );
    }

    #[test]
    fn mixed_to_patch() {
        let mp = MultiParent::with_hunks(vec![
            Hunk::NewText(lines(&[b"a\n"])),
            Hunk::ParentText {
                parent: 0,
                parent_pos: 1,
                child_pos: 2,
                num_lines: 3,
            },
        ]);
        assert_eq!(
            mp.to_patch(),
            vec![
                b"i 1\n".to_vec(),
                b"a\n".to_vec(),
                b"\n".to_vec(),
                b"c 0 1 2 3\n".to_vec(),
            ]
        );
    }

    #[test]
    fn from_patch_round_trip() {
        let mp = MultiParent::with_hunks(vec![
            Hunk::NewText(lines(&[b"a\n"])),
            Hunk::ParentText {
                parent: 0,
                parent_pos: 1,
                child_pos: 2,
                num_lines: 3,
            },
        ]);
        let parsed = MultiParent::from_patch(b"i 1\na\n\nc 0 1 2 3").unwrap();
        assert_eq!(parsed, mp);
    }

    #[test]
    fn from_patch_without_trailing_separator() {
        let parsed = MultiParent::from_patch(b"i 1\na\nc 0 1 2 3\n").unwrap();
        let expected = MultiParent::with_hunks(vec![
            Hunk::NewText(vec![b"a".to_vec()]),
            Hunk::ParentText {
                parent: 0,
                parent_pos: 1,
                child_pos: 2,
                num_lines: 3,
            },
        ]);
        assert_eq!(parsed, expected);
    }

    #[test]
    fn num_lines_matches_python() {
        let mut mp = MultiParent::with_hunks(vec![Hunk::NewText(lines(&[b"a\n"]))]);
        assert_eq!(mp.num_lines(), 1);
        mp.hunks.push(Hunk::NewText(lines(&[b"b\n", b"c\n"])));
        assert_eq!(mp.num_lines(), 3);
        mp.hunks.push(Hunk::ParentText {
            parent: 0,
            parent_pos: 0,
            child_pos: 3,
            num_lines: 2,
        });
        assert_eq!(mp.num_lines(), 5);
        mp.hunks.push(Hunk::NewText(lines(&[b"f\n", b"g\n"])));
        assert_eq!(mp.num_lines(), 7);
    }

    #[test]
    fn range_iterator_shape() {
        let mp = MultiParent::with_hunks(vec![
            Hunk::ParentText {
                parent: 1,
                parent_pos: 0,
                child_pos: 0,
                num_lines: 4,
            },
            Hunk::ParentText {
                parent: 0,
                parent_pos: 3,
                child_pos: 4,
                num_lines: 1,
            },
            Hunk::NewText(lines(&[b"q\n"])),
        ]);
        let items = mp.range_iterator();
        assert_eq!(items.len(), 3);
        assert_eq!((items[0].start, items[0].end), (0, 4));
        assert_eq!(
            items[0].data,
            RangeData::Parent {
                parent: 1,
                parent_start: 0,
                parent_end: 4,
            }
        );
        assert_eq!((items[1].start, items[1].end), (4, 5));
        assert_eq!(
            items[1].data,
            RangeData::Parent {
                parent: 0,
                parent_start: 3,
                parent_end: 4,
            }
        );
        assert_eq!((items[2].start, items[2].end), (5, 6));
        match items[2].data {
            RangeData::New(ls) => assert_eq!(ls, &[b"q\n".to_vec()][..]),
            _ => panic!("expected New"),
        }
    }

    #[test]
    fn matching_blocks_emits_sentinel() {
        let mp = MultiParent::with_hunks(vec![
            Hunk::ParentText {
                parent: 0,
                parent_pos: 0,
                child_pos: 0,
                num_lines: 1,
            },
            Hunk::NewText(lines(&[b"b\n"])),
            Hunk::ParentText {
                parent: 0,
                parent_pos: 1,
                child_pos: 2,
                num_lines: 3,
            },
        ]);
        assert_eq!(
            mp.matching_blocks(0, 4),
            vec![(0, 0, 1), (1, 2, 3), (4, 5, 0)]
        );
    }

    #[test]
    fn is_snapshot() {
        assert!(MultiParent::with_hunks(vec![Hunk::NewText(lines(&[b"a\n"]))]).is_snapshot());
        assert!(!MultiParent::new().is_snapshot());
        assert!(!MultiParent::with_hunks(vec![
            Hunk::NewText(lines(&[b"a\n"])),
            Hunk::NewText(lines(&[b"b\n"])),
        ])
        .is_snapshot());
        assert!(!MultiParent::with_hunks(vec![Hunk::ParentText {
            parent: 0,
            parent_pos: 0,
            child_pos: 0,
            num_lines: 1,
        }])
        .is_snapshot());
    }

    #[test]
    fn binary_content_round_trip() {
        // From test_binary_content: bytes containing \r, \xff, NUL.
        let lf_split: Vec<Vec<u8>> = vec![
            b"\x00\n".to_vec(),
            b"\x00\r\x01\n".to_vec(),
            b"\x02\r\xff".to_vec(),
        ];
        let mp = MultiParent::with_hunks(vec![Hunk::NewText(lf_split.clone())]);
        let patch: Vec<u8> = mp.to_patch().into_iter().flatten().collect();
        let parsed = MultiParent::from_patch(&patch).unwrap();
        assert_eq!(parsed, mp);
    }

    #[test]
    fn patch_len_matches_to_patch() {
        let mp = MultiParent::with_hunks(vec![
            Hunk::NewText(lines(&[b"hello\n", b"world\n"])),
            Hunk::ParentText {
                parent: 2,
                parent_pos: 10,
                child_pos: 20,
                num_lines: 5,
            },
        ]);
        let concatenated: usize = mp.to_patch().iter().map(|l| l.len()).sum();
        assert_eq!(mp.patch_len(), concatenated);
    }

    #[test]
    fn from_patch_rejects_unexpected_char() {
        assert_eq!(
            MultiParent::from_patch(b"x nonsense\n"),
            Err(ParseError::UnexpectedChar(b'x'))
        );
    }

    fn topo_parents(
        entries: &[(&str, Option<&[&str]>)],
    ) -> std::collections::HashMap<String, Option<Vec<String>>> {
        entries
            .iter()
            .map(|(k, ps)| {
                (
                    (*k).to_string(),
                    ps.map(|ps| ps.iter().map(|p| (*p).to_string()).collect()),
                )
            })
            .collect()
    }

    fn topo_versions(vs: &[&str]) -> Vec<String> {
        vs.iter().map(|v| (*v).to_string()).collect()
    }

    #[test]
    fn topo_iter_linear_chain() {
        // a <- b <- c <- d, fed in insertion order.
        let parents = topo_parents(&[
            ("a", Some(&[])),
            ("b", Some(&["a"])),
            ("c", Some(&["b"])),
            ("d", Some(&["c"])),
        ]);
        let versions = topo_versions(&["a", "b", "c", "d"]);
        assert_eq!(topo_iter(&parents, &versions), versions);
    }

    #[test]
    fn topo_iter_orders_parents_before_children_when_input_is_shuffled() {
        // Same diamond shape, shuffled input. Tiebreakers come from the
        // order in which descendants were registered while walking
        // `version_order`, so the exact sequence is deterministic and
        // matches the Python `_topo_iter` implementation.
        let parents = topo_parents(&[
            ("a", Some(&[])),
            ("b", Some(&["a"])),
            ("c", Some(&["a"])),
            ("d", Some(&["b", "c"])),
        ]);
        let got = topo_iter(&parents, &topo_versions(&["d", "c", "b", "a"]));
        assert_eq!(got, topo_versions(&["a", "c", "b", "d"]));
    }

    #[test]
    fn topo_iter_parentless_sentinel_is_treated_as_root() {
        // A `None` entry (parentless sentinel) is yielded without waiting
        // on anything, mirroring the Python special case.
        let parents = topo_parents(&[("a", None), ("b", Some(&["a"]))]);
        let got = topo_iter(&parents, &topo_versions(&["b", "a"]));
        assert_eq!(got, topo_versions(&["a", "b"]));
    }

    #[test]
    fn topo_iter_ignores_parents_outside_input_set() {
        // If a parent isn't in the version set, it doesn't count as
        // pending — the child can be yielded immediately.
        let parents = topo_parents(&[("x", Some(&["not-in-set"])), ("y", Some(&["x"]))]);
        let got = topo_iter(&parents, &topo_versions(&["x", "y"]));
        assert_eq!(got, topo_versions(&["x", "y"]));
    }

    #[test]
    fn topo_iter_empty_input() {
        let parents: std::collections::HashMap<String, Option<Vec<String>>> =
            std::collections::HashMap::new();
        let got = topo_iter(&parents, &[] as &[String]);
        assert!(got.is_empty());
    }

    #[test]
    fn topo_iter_deduplicates_input() {
        // Duplicate versions in the input list produce a single output
        // entry, matching the "seen" bookkeeping.
        let parents = topo_parents(&[("a", Some(&[])), ("b", Some(&["a"]))]);
        let got = topo_iter(&parents, &topo_versions(&["a", "b", "a", "b"]));
        assert_eq!(got, topo_versions(&["a", "b"]));
    }

    #[test]
    fn topo_iter_diamond() {
        // a -> b, a -> c, b+c -> d
        let parents = topo_parents(&[
            ("a", Some(&[])),
            ("b", Some(&["a"])),
            ("c", Some(&["a"])),
            ("d", Some(&["b", "c"])),
        ]);
        let got = topo_iter(&parents, &topo_versions(&["a", "b", "c", "d"]));
        assert_eq!(got, topo_versions(&["a", "b", "c", "d"]));
    }

    #[test]
    fn from_patch_rejects_truncated_new_text() {
        assert_eq!(
            MultiParent::from_patch(b"i 3\nonly\n"),
            Err(ParseError::Truncated)
        );
    }
}

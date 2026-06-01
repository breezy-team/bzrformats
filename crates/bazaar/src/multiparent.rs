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

/// Error returned when reconstructing a fulltext from a `MultiParent` diff fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconstructError {
    /// A `ParentText` hunk references a parent slot that the version's parent
    /// list does not contain (typically because the caller fed the diff into a
    /// `MultiMemoryVersionedFile` with fewer parents than the diff was built
    /// against).
    ParentIndexOutOfRange {
        /// The parent slot the diff asked for.
        parent_index: usize,
        /// How many parents the version actually has.
        parent_count: usize,
    },
    /// Reconstruction was asked for a version that has no recorded diff.
    UnknownVersion,
}

impl std::fmt::Display for ReconstructError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReconstructError::ParentIndexOutOfRange {
                parent_index,
                parent_count,
            } => write!(
                f,
                "parent index {} out of range (version has {} parents)",
                parent_index, parent_count
            ),
            ReconstructError::UnknownVersion => write!(f, "no diff recorded for requested version"),
        }
    }
}

impl std::error::Error for ReconstructError {}

impl MultiParent {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_hunks(hunks: Vec<Hunk>) -> Self {
        Self { hunks }
    }

    /// Build a [`MultiParent`] from `text` and per-parent matching blocks.
    ///
    /// Mirrors `MultiParent.from_lines` in `bzrformats/multiparent.py`. The
    /// caller computes each parent's `get_matching_blocks()` sequence
    /// (typically via patiencediff) and passes them here; this function owns
    /// the greedy longest-match selection loop.
    ///
    /// Each element of `parent_blocks` is the block list for parent `p`: a
    /// sequence of `(i, j, n)` triples where `i` is the offset in the parent,
    /// `j` the offset in `text`, and `n` the run length. The final sentinel
    /// block `(parent_len, text_len, 0)` may be present or absent — both
    /// shapes are accepted.
    pub fn from_lines_with_blocks(
        text: &[Vec<u8>],
        parent_blocks: &[Vec<(usize, usize, usize)>],
    ) -> Self {
        let mut hunks: Vec<Hunk> = Vec::new();
        let mut new_lines: Vec<Vec<u8>> = Vec::new();
        let mut iters: Vec<std::slice::Iter<'_, (usize, usize, usize)>> =
            parent_blocks.iter().map(|b| b.iter()).collect();
        // cur_block[p] tracks the next candidate block for parent p, or None
        // when the iterator is exhausted.
        let mut cur_block: Vec<Option<(usize, usize, usize)>> =
            iters.iter_mut().map(|it| it.next().copied()).collect();

        let mut cur_line = 0usize;
        while cur_line < text.len() {
            // Best match across parents: the longest ParentText we can anchor
            // at cur_line.
            let mut best: Option<(usize, usize, usize, usize)> = None; // (parent, parent_pos, child_pos, num_lines)
            for (p, slot) in cur_block.iter_mut().enumerate() {
                // Advance past blocks that end at or before cur_line.
                loop {
                    match *slot {
                        Some((_, j, n)) if j + n <= cur_line => {
                            *slot = iters[p].next().copied();
                        }
                        _ => break,
                    }
                }
                let Some((i, j, n)) = *slot else { continue };
                if j > cur_line {
                    continue;
                }
                let offset = cur_line - j;
                let i = i + offset;
                let j = cur_line;
                let n = n - offset;
                if n == 0 {
                    continue;
                }
                if best.is_none_or(|b| n > b.3) {
                    best = Some((p, i, j, n));
                }
            }
            match best {
                None => {
                    new_lines.push(text[cur_line].clone());
                    cur_line += 1;
                }
                Some((parent, parent_pos, child_pos, num_lines)) => {
                    if !new_lines.is_empty() {
                        hunks.push(Hunk::NewText(std::mem::take(&mut new_lines)));
                    }
                    hunks.push(Hunk::ParentText {
                        parent,
                        parent_pos,
                        child_pos,
                        num_lines,
                    });
                    cur_line += num_lines;
                }
            }
        }
        if !new_lines.is_empty() {
            hunks.push(Hunk::NewText(new_lines));
        }
        Self { hunks }
    }

    /// Build a [`MultiParent`] from `text` and its `parents`, computing each
    /// parent's matching-block sequence with patiencediff. `left_blocks` may
    /// be supplied to skip the diff against `parents[0]`.
    ///
    /// Mirrors `MultiParent.from_lines` in `bzrformats/multiparent.py`.
    pub fn from_lines(
        text: &[Vec<u8>],
        parents: &[&[Vec<u8>]],
        left_blocks: Option<Vec<(usize, usize, usize)>>,
    ) -> Self {
        if parents.is_empty() {
            return Self::from_lines_with_blocks(text, &[]);
        }
        let compare = |parent: &[Vec<u8>]| -> Vec<(usize, usize, usize)> {
            patiencediff::SequenceMatcher::new(parent, text)
                .get_matching_blocks()
                .to_vec()
        };
        let mut parent_blocks: Vec<Vec<(usize, usize, usize)>> = Vec::with_capacity(parents.len());
        parent_blocks.push(left_blocks.unwrap_or_else(|| compare(parents[0])));
        for p in &parents[1..] {
            parent_blocks.push(compare(p));
        }
        Self::from_lines_with_blocks(text, &parent_blocks)
    }

    /// Matching `(parent_pos, child_pos, num_lines)` triples between this
    /// diff and `parent` (its index into the parents list), plus a final
    /// sentinel `(parent_len, num_lines, 0)`.
    ///
    /// Mirrors `MultiParent.get_matching_blocks` — used by
    /// `VersionedFiles.add_mpdiffs` to pass the single-parent matching
    /// blocks straight into `add_lines` as a delta-compression hint.
    pub fn get_matching_blocks(
        &self,
        parent: usize,
        parent_len: usize,
    ) -> Vec<(usize, usize, usize)> {
        let mut out: Vec<(usize, usize, usize)> = Vec::new();
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

    /// The length in bytes of the gzip-compressed patch. Mirrors
    /// `MultiParent.zipped_patch_len`.
    pub fn zipped_patch_len(&self) -> usize {
        use std::io::Write;
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        for chunk in self.to_patch() {
            // Writing to an in-memory Vec never fails.
            let _ = enc.write_all(&chunk);
        }
        enc.finish().map(|v| v.len()).unwrap_or(0)
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

/// Gzip-compress `lines` into a single gzip container. Mirrors
/// `multiparent.gzip_string`.
pub fn gzip_string<'a>(lines: impl IntoIterator<Item = &'a [u8]>) -> Vec<u8> {
    use std::io::Write;
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    for line in lines {
        // Writing to an in-memory Vec never fails.
        let _ = enc.write_all(line);
    }
    enc.finish().unwrap_or_default()
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

/// In-memory `BaseVersionedFile`/`MultiMemoryVersionedFile` analogue.
///
/// Holds an mpdiff per version together with its parent keys, and can
/// reconstruct any version's fulltext lines by walking the chain (cached
/// in `_lines`). Mirrors the subset of `BaseVersionedFile` /
/// `MultiMemoryVersionedFile` that `VersionedFiles.add_mpdiffs` exercises:
/// `add_diff`, `add_version`, `has_version`, `get_diff`, `get_line_list`.
///
/// Snapshot bookkeeping, size ranking, build ranking, import_versionedfile
/// and the other helpers from the Python `BaseVersionedFile` are not
/// ported — `add_mpdiffs` doesn't use them, and they'd nearly double the
/// pyo3 surface for no current caller.
pub struct MultiMemoryVersionedFile<K>
where
    K: std::hash::Hash + Eq + Clone,
{
    diffs: std::collections::HashMap<K, MultiParent>,
    parents: std::collections::HashMap<K, Vec<K>>,
    lines_cache: std::collections::HashMap<K, Vec<Vec<u8>>>,
    snapshots: std::collections::HashSet<K>,
    snapshot_interval: Option<usize>,
    max_snapshots: Option<usize>,
    /// Preserves insertion order so `versions()` yields the same sequence
    /// as Python's `iter(self._parents)`, which dicts preserve insertion
    /// order for.
    insert_order: Vec<K>,
}

impl<K> Default for MultiMemoryVersionedFile<K>
where
    K: std::hash::Hash + Eq + Clone,
{
    fn default() -> Self {
        Self::new(Some(25), None)
    }
}

impl<K> MultiMemoryVersionedFile<K>
where
    K: std::hash::Hash + Eq + Clone,
{
    pub fn new(snapshot_interval: Option<usize>, max_snapshots: Option<usize>) -> Self {
        Self {
            diffs: std::collections::HashMap::new(),
            parents: std::collections::HashMap::new(),
            lines_cache: std::collections::HashMap::new(),
            snapshots: std::collections::HashSet::new(),
            snapshot_interval,
            max_snapshots,
            insert_order: Vec::new(),
        }
    }

    pub fn has_version(&self, version: &K) -> bool {
        self.parents.contains_key(version)
    }

    pub fn get_diff(&self, version: &K) -> Option<&MultiParent> {
        self.diffs.get(version)
    }

    pub fn get_parents(&self, version: &K) -> Option<&[K]> {
        self.parents.get(version).map(Vec::as_slice)
    }

    /// Park `diff` against `version_id`, with the given parent keys. No
    /// snapshot decision is made; the lines cache is not touched. Mirrors
    /// `MultiMemoryVersionedFile.add_diff`.
    pub fn add_diff(&mut self, diff: MultiParent, version_id: K, parent_ids: Vec<K>) {
        if !self.parents.contains_key(&version_id) {
            self.insert_order.push(version_id.clone());
        }
        self.diffs.insert(version_id.clone(), diff);
        self.parents.insert(version_id, parent_ids);
    }

    /// Add a version (with fulltext `lines`). Decides whether to record as
    /// a snapshot (`NewText`) or as a multiparent delta. Mirrors
    /// `BaseVersionedFile.add_version`; `force_snapshot=None` means use
    /// `do_snapshot`, `single_parent` controls whether to diff against
    /// only the first parent.
    pub fn add_version(
        &mut self,
        lines: Vec<Vec<u8>>,
        version_id: K,
        parent_ids: Vec<K>,
        force_snapshot: Option<bool>,
        single_parent: bool,
    ) -> Result<(), ReconstructError> {
        let take_snapshot =
            force_snapshot.unwrap_or_else(|| self.do_snapshot(&version_id, &parent_ids));
        let diff = if take_snapshot {
            self.snapshots.insert(version_id.clone());
            MultiParent::with_hunks(vec![Hunk::NewText(lines.clone())])
        } else {
            let parents_slice: &[K] = if single_parent {
                &parent_ids[..parent_ids.len().min(1)]
            } else {
                &parent_ids[..]
            };
            let parent_lines = self.get_line_list_owned(parents_slice)?;
            let parent_refs: Vec<&[Vec<u8>]> = parent_lines.iter().map(Vec::as_slice).collect();
            let d = MultiParent::from_lines(&lines, &parent_refs, None);
            if d.is_snapshot() {
                self.snapshots.insert(version_id.clone());
            }
            d
        };
        self.add_diff(diff, version_id.clone(), parent_ids);
        self.lines_cache.insert(version_id, lines);
        Ok(())
    }

    /// Mirror of `BaseVersionedFile.do_snapshot`: walk back
    /// `snapshot_interval` levels; if the chain reaches a snapshot in that
    /// many steps, no need to record this one.
    pub fn do_snapshot(&self, _version_id: &K, parent_ids: &[K]) -> bool {
        let Some(interval) = self.snapshot_interval else {
            return false;
        };
        if let Some(max) = self.max_snapshots {
            if self.snapshots.len() == max {
                return false;
            }
        }
        if parent_ids.is_empty() {
            return true;
        }
        let mut frontier: Vec<K> = parent_ids.to_vec();
        for _ in 0..interval {
            if frontier.is_empty() {
                return false;
            }
            let current = std::mem::take(&mut frontier);
            for v in current {
                if !self.snapshots.contains(&v) {
                    if let Some(ps) = self.parents.get(&v) {
                        frontier.extend(ps.iter().cloned());
                    }
                }
            }
        }
        true
    }

    /// Get the reconstructed lines for each version in `version_ids`,
    /// caching as we go. Mirrors `BaseVersionedFile.get_line_list`.
    pub fn get_line_list(
        &mut self,
        version_ids: &[K],
    ) -> Result<Vec<Vec<Vec<u8>>>, ReconstructError> {
        version_ids
            .iter()
            .map(|v| self.cache_version(v).map(<[Vec<u8>]>::to_vec))
            .collect()
    }

    fn get_line_list_owned(
        &mut self,
        version_ids: &[K],
    ) -> Result<Vec<Vec<Vec<u8>>>, ReconstructError> {
        self.get_line_list(version_ids)
    }

    /// Reconstruct a version's fulltext (caching the result) and return a
    /// reference into the cache. Returns [`ReconstructError`] if the diff
    /// references a parent index outside the version's parent list.
    pub fn cache_version(&mut self, version_id: &K) -> Result<&[Vec<u8>], ReconstructError> {
        if !self.lines_cache.contains_key(version_id) {
            let length = self
                .diffs
                .get(version_id)
                .map(MultiParent::num_lines)
                .unwrap_or(0);
            let mut lines: Vec<Vec<u8>> = Vec::with_capacity(length);
            self.reconstruct(&mut lines, version_id.clone(), 0, length)?;
            self.lines_cache.insert(version_id.clone(), lines);
        }
        Ok(self
            .lines_cache
            .get(version_id)
            .expect("just inserted above")
            .as_slice())
    }

    /// Append lines for `[req_start, req_end)` of `req_version_id` to `out`.
    ///
    /// Iterative port of `_Reconstructor._reconstruct`: walks the diff
    /// chain backward, splitting a range across hunk boundaries when
    /// necessary. Each ParentText hunk is rewritten as a fresh range
    /// request against the parent and pushed onto a pending stack.
    fn reconstruct(
        &mut self,
        out: &mut Vec<Vec<u8>>,
        req_version_id: K,
        req_start: usize,
        req_end: usize,
    ) -> Result<(), ReconstructError> {
        if req_start == req_end {
            return Ok(());
        }
        let mut pending: Vec<(K, usize, usize)> = vec![(req_version_id, req_start, req_end)];
        while let Some((version_id, req_start, req_end)) = pending.pop() {
            if let Some(cached) = self.lines_cache.get(&version_id) {
                out.extend_from_slice(&cached[req_start..req_end]);
                continue;
            }
            let diff = self
                .diffs
                .get(&version_id)
                .ok_or(ReconstructError::UnknownVersion)?;
            let ranges = diff.range_iterator();
            let mut idx = 0;
            while idx < ranges.len() && ranges[idx].end <= req_start {
                idx += 1;
            }
            if idx == ranges.len() {
                continue;
            }
            let hunk = &ranges[idx];
            let mut req_end = req_end;
            if req_end > hunk.end {
                pending.push((version_id.clone(), hunk.end, req_end));
                req_end = hunk.end;
            }
            match &hunk.data {
                RangeData::New(lines) => {
                    let local_start = req_start - hunk.start;
                    let local_end = req_end - hunk.start;
                    out.extend(lines[local_start..local_end].iter().cloned());
                }
                RangeData::Parent {
                    parent,
                    parent_start,
                    parent_end,
                } => {
                    let parents = self.parents.get(&version_id);
                    let parent_count = parents.map(Vec::len).unwrap_or(0);
                    let parent_key = parents
                        .and_then(|ps| ps.get(*parent))
                        .ok_or(ReconstructError::ParentIndexOutOfRange {
                            parent_index: *parent,
                            parent_count,
                        })?
                        .clone();
                    let new_start = parent_start + req_start - hunk.start;
                    let new_end = parent_end + req_end - hunk.end;
                    pending.push((parent_key, new_start, new_end));
                }
            }
        }
        Ok(())
    }

    pub fn versions(&self) -> impl Iterator<Item = &K> {
        self.insert_order.iter()
    }

    /// Read-only access to the parent map (version -> list of parent keys).
    pub fn parents_map(&self) -> &std::collections::HashMap<K, Vec<K>> {
        &self.parents
    }

    /// Read-only access to the lines cache (version -> reconstructed
    /// fulltext lines). A version only appears here after it has been
    /// reconstructed at least once, or seeded by `add_version`.
    pub fn lines_cache(&self) -> &std::collections::HashMap<K, Vec<Vec<u8>>> {
        &self.lines_cache
    }

    /// Snapshot set (versions stored as `NewText` instead of a delta).
    pub fn snapshots(&self) -> &std::collections::HashSet<K> {
        &self.snapshots
    }

    /// Whether `version` is a recorded snapshot.
    pub fn is_snapshot(&self, version: &K) -> bool {
        self.snapshots.contains(version)
    }

    pub fn clear_cache(&mut self) {
        self.lines_cache.clear();
    }

    /// Replace a version's existing diff with a fulltext snapshot of its
    /// reconstructed lines. Mirrors `BaseVersionedFile.make_snapshot`.
    pub fn make_snapshot(&mut self, version_id: K) -> Result<(), ReconstructError> {
        let lines = self.cache_version(&version_id)?.to_vec();
        let parents = self.parents.get(&version_id).cloned().unwrap_or_default();
        let snap = MultiParent::with_hunks(vec![Hunk::NewText(lines)]);
        self.add_diff(snap, version_id.clone(), parents);
        self.snapshots.insert(version_id);
        Ok(())
    }

    /// Like `BaseVersionedFile.import_diffs`: copy every version's diff +
    /// parent list from `other` into `self` (without recomputing).
    pub fn import_diffs(&mut self, other: &Self) {
        for v in other.versions() {
            if let (Some(d), Some(p)) = (other.get_diff(v), other.get_parents(v)) {
                self.add_diff(d.clone(), v.clone(), p.to_vec());
            }
        }
    }

    /// Versions ranked by `(snapshot_len - delta_len)` ascending — the
    /// negative end is the cheapest to snapshot. Mirrors
    /// `BaseVersionedFile.get_size_ranking`. Snapshot versions are
    /// skipped.
    pub fn get_size_ranking(&mut self) -> Result<Vec<(isize, K)>, ReconstructError> {
        let versions: Vec<K> = self.insert_order.clone();
        let mut out: Vec<(isize, K)> = Vec::new();
        for v in &versions {
            if self.snapshots.contains(v) {
                continue;
            }
            let diff_len = self
                .diffs
                .get(v)
                .map(|d| d.to_patch().iter().map(Vec::len).sum::<usize>())
                .unwrap_or(0);
            let lines = self.cache_version(v)?.to_vec();
            let snap = MultiParent::with_hunks(vec![Hunk::NewText(lines)]);
            let snap_len: usize = snap.to_patch().iter().map(Vec::len).sum();
            out.push((snap_len as isize - diff_len as isize, v.clone()));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    /// Select new snapshots to drop the output size below `num` total
    /// snapshots. Returns the versions to snapshot. Mirrors
    /// `BaseVersionedFile.select_by_size` — picks the last `num` entries
    /// from the size ranking.
    pub fn select_by_size(&mut self, num: usize) -> Result<Vec<K>, ReconstructError> {
        let needed = num.saturating_sub(self.snapshots.len());
        let ranking = self.get_size_ranking()?;
        Ok(ranking
            .into_iter()
            .rev()
            .take(needed)
            .map(|(_, v)| v)
            .collect())
    }

    /// Select which versions to add as snapshots given the chain depth
    /// from each version to its nearest snapshot ancestor. Mirrors
    /// `BaseVersionedFile.select_snapshots`.
    pub fn select_snapshots(&self) -> std::collections::HashSet<K> {
        let interval = self.snapshot_interval.unwrap_or(usize::MAX);
        // Topo-walk via the existing topo_iter helper.
        let parents_map: std::collections::HashMap<K, Option<Vec<K>>> = self
            .parents
            .iter()
            .map(|(k, v)| (k.clone(), Some(v.clone())))
            .collect();
        let order: Vec<K> = topo_iter(&parents_map, &self.insert_order);
        let mut build_ancestors: std::collections::HashMap<K, std::collections::HashSet<K>> =
            std::collections::HashMap::new();
        let mut snapshots: std::collections::HashSet<K> = std::collections::HashSet::new();
        for version_id in &order {
            let parents = self.parents.get(version_id).cloned().unwrap_or_default();
            let mut potential: std::collections::HashSet<K> = parents.iter().cloned().collect();
            if parents.is_empty() {
                snapshots.insert(version_id.clone());
                build_ancestors.insert(version_id.clone(), std::collections::HashSet::new());
            } else {
                for p in &parents {
                    if let Some(set) = build_ancestors.get(p) {
                        potential.extend(set.iter().cloned());
                    }
                }
                if potential.len() > interval {
                    snapshots.insert(version_id.clone());
                    build_ancestors.insert(version_id.clone(), std::collections::HashSet::new());
                } else {
                    build_ancestors.insert(version_id.clone(), potential);
                }
            }
        }
        snapshots
    }

    /// Rank versions by how much their snapshot status reduces overall
    /// build complexity. Mirrors `BaseVersionedFile.get_build_ranking`.
    pub fn get_build_ranking(&self) -> Vec<K> {
        let mut could_avoid: std::collections::HashMap<K, std::collections::HashSet<K>> =
            std::collections::HashMap::new();
        let mut referenced_by: std::collections::HashMap<K, std::collections::HashSet<K>> =
            std::collections::HashMap::new();
        let parents_map: std::collections::HashMap<K, Option<Vec<K>>> = self
            .parents
            .iter()
            .map(|(k, v)| (k.clone(), Some(v.clone())))
            .collect();
        let order: Vec<K> = topo_iter(&parents_map, &self.insert_order);
        for v in &order {
            could_avoid.insert(v.clone(), std::collections::HashSet::new());
            if !self.snapshots.contains(v) {
                let parents = self.parents.get(v).cloned().unwrap_or_default();
                for p in &parents {
                    if let Some(set) = could_avoid.get(p).cloned() {
                        could_avoid.get_mut(v).unwrap().extend(set);
                    }
                }
                let all_known: Vec<K> = self.parents.keys().cloned().collect();
                could_avoid.get_mut(v).unwrap().extend(all_known);
                could_avoid.get_mut(v).unwrap().remove(v);
            }
            let avoid_set = could_avoid.get(v).cloned().unwrap_or_default();
            for avoid_id in avoid_set {
                referenced_by.entry(avoid_id).or_default().insert(v.clone());
            }
        }
        let mut available: Vec<K> = self.insert_order.clone();
        let mut ranking: Vec<K> = Vec::new();
        while !available.is_empty() {
            available.sort_by_key(|x| {
                could_avoid.get(x).map(|s| s.len()).unwrap_or(0)
                    * referenced_by.get(x).map(|s| s.len()).unwrap_or(0)
            });
            let selected = available.pop().expect("non-empty checked above");
            ranking.push(selected.clone());
            let selected_refs = referenced_by.get(&selected).cloned().unwrap_or_default();
            let selected_avoid = could_avoid.get(&selected).cloned().unwrap_or_default();
            for v in &selected_refs {
                if let Some(set) = could_avoid.get_mut(v) {
                    for r in &selected_avoid {
                        set.remove(r);
                    }
                }
            }
            for v in &selected_avoid {
                if let Some(set) = referenced_by.get_mut(v) {
                    for r in &selected_refs {
                        set.remove(r);
                    }
                }
            }
        }
        ranking
    }

    pub fn snapshot_interval(&self) -> Option<usize> {
        self.snapshot_interval
    }

    pub fn max_snapshots(&self) -> Option<usize> {
        self.max_snapshots
    }

    /// The set of version ids currently recorded as snapshots.
    pub fn snapshots_set(&self) -> &std::collections::HashSet<K> {
        &self.snapshots
    }

    /// Record `version_id` as a snapshot without recomputing its diff. Used
    /// when restoring state (e.g. loading a disk index).
    pub fn mark_snapshot(&mut self, version_id: K) {
        self.snapshots.insert(version_id);
    }
}

/// Error from a [`DiskMultiVersionedFile`] operation: either reconstruction
/// failed or the underlying disk I/O failed.
#[derive(Debug)]
pub enum DiskError {
    Reconstruct(ReconstructError),
    Io(std::io::Error),
}

impl std::fmt::Display for DiskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DiskError::Reconstruct(e) => write!(f, "{}", e),
            DiskError::Io(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for DiskError {}

impl From<ReconstructError> for DiskError {
    fn from(e: ReconstructError) -> Self {
        DiskError::Reconstruct(e)
    }
}

impl From<std::io::Error> for DiskError {
    fn from(e: std::io::Error) -> Self {
        DiskError::Io(e)
    }
}

/// Disk-backed multi-parent versioned file, ported from
/// `bzrformats.multiparent.MultiVersionedFile`.
///
/// Diffs are appended to `<filename>.mpknit` as independent gzip members (each
/// prefixed with a `version <id>\n` line) and the parents/snapshots/offsets
/// index is bencoded to `<filename>.mpidx`. An in-memory
/// [`MultiMemoryVersionedFile`] holds the live diffs so reconstruction reuses
/// the shared engine; `load` repopulates it by reading every diff off disk.
pub struct DiskMultiVersionedFile {
    filename: String,
    mem: MultiMemoryVersionedFile<Vec<u8>>,
    /// version id -> (byte offset, byte length) of its gzip member in .mpknit
    diff_offset: std::collections::HashMap<Vec<u8>, (u64, u64)>,
}

impl DiskMultiVersionedFile {
    pub fn new(
        filename: String,
        snapshot_interval: Option<usize>,
        max_snapshots: Option<usize>,
    ) -> Self {
        Self {
            filename,
            mem: MultiMemoryVersionedFile::new(snapshot_interval, max_snapshots),
            diff_offset: std::collections::HashMap::new(),
        }
    }

    fn knit_path(&self) -> String {
        format!("{}.mpknit", self.filename)
    }

    fn idx_path(&self) -> String {
        format!("{}.mpidx", self.filename)
    }

    /// Append `diff` for `version_id` to the .mpknit file as a gzip member and
    /// record its offset. Mirrors `MultiVersionedFile.add_diff`.
    fn write_diff_to_disk(&mut self, diff: &MultiParent, version_id: &[u8]) -> std::io::Result<()> {
        use std::io::{Seek, SeekFrom, Write};
        let mut outfile = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.knit_path())?;
        let start = outfile.seek(SeekFrom::End(0))?;
        {
            let mut enc =
                flate2::write::GzEncoder::new(&mut outfile, flate2::Compression::default());
            enc.write_all(b"version ")?;
            enc.write_all(version_id)?;
            enc.write_all(b"\n")?;
            for chunk in diff.to_patch() {
                enc.write_all(&chunk)?;
            }
            enc.finish()?;
        }
        let end = outfile.seek(SeekFrom::End(0))?;
        self.diff_offset
            .insert(version_id.to_vec(), (start, end - start));
        Ok(())
    }

    /// Add a fulltext version: compute its diff against parents (deciding
    /// snapshots), store it in the in-memory VF and append it to disk.
    pub fn add_version(
        &mut self,
        lines: Vec<Vec<u8>>,
        version_id: Vec<u8>,
        parent_ids: Vec<Vec<u8>>,
        force_snapshot: Option<bool>,
        single_parent: bool,
    ) -> Result<(), DiskError> {
        self.mem.add_version(
            lines,
            version_id.clone(),
            parent_ids,
            force_snapshot,
            single_parent,
        )?;
        let diff = self.mem.get_diff(&version_id).expect("just added").clone();
        self.write_diff_to_disk(&diff, &version_id)?;
        Ok(())
    }

    /// Reconstruct the fulltext line lists for `version_ids`.
    pub fn get_line_list(
        &mut self,
        version_ids: &[Vec<u8>],
    ) -> Result<Vec<Vec<Vec<u8>>>, ReconstructError> {
        self.mem.get_line_list(version_ids)
    }

    /// Read a single diff back from the .mpknit file.
    pub fn read_diff_from_disk(&self, version_id: &[u8]) -> std::io::Result<MultiParent> {
        use std::io::{Read, Seek, SeekFrom};
        let (start, count) = *self
            .diff_offset
            .get(version_id)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "unknown version"))?;
        let mut infile = std::fs::File::open(self.knit_path())?;
        infile.seek(SeekFrom::Start(start))?;
        let mut buf = vec![0u8; count as usize];
        infile.read_exact(&mut buf)?;
        let mut dec = flate2::read::GzDecoder::new(&buf[..]);
        let mut content = Vec::new();
        dec.read_to_end(&mut content)?;
        // Drop the leading `version <id>\n` header line.
        let body = match content.iter().position(|&b| b == b'\n') {
            Some(i) => &content[i + 1..],
            None => &content[..],
        };
        MultiParent::from_patch(body)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
    }

    /// Persist the parents/snapshots/offsets index to the .mpidx file as a
    /// bencoded `(parents, snapshots, diff_offset)` tuple, matching the
    /// `fastbencode` layout the Python implementation wrote.
    pub fn save(&self) -> std::io::Result<()> {
        let data = self.encode_index();
        std::fs::write(self.idx_path(), data)
    }

    /// Load the index from .mpidx and repopulate the in-memory VF by reading
    /// every diff back off the .mpknit file.
    pub fn load(&mut self) -> std::io::Result<()> {
        let data = std::fs::read(self.idx_path())?;
        let (parents, snapshots, diff_offset) = Self::decode_index(&data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        self.diff_offset = diff_offset;
        let mut mem: MultiMemoryVersionedFile<Vec<u8>> =
            MultiMemoryVersionedFile::new(self.mem.snapshot_interval(), self.mem.max_snapshots());
        // Re-add each diff in the on-disk order so reconstruction has them.
        for (version_id, parent_ids) in &parents {
            let diff = self.read_diff_from_disk(version_id)?;
            mem.add_diff(diff, version_id.clone(), parent_ids.clone());
        }
        for snap in snapshots {
            mem.mark_snapshot(snap);
        }
        self.mem = mem;
        Ok(())
    }

    /// Remove the .mpknit and .mpidx files from disk.
    pub fn destroy(&self) -> std::io::Result<()> {
        for path in [self.knit_path(), self.idx_path()] {
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Bencode the `(parents, snapshots, diff_offset)` index.
    ///
    /// `parents` is a dict version_id -> [parent_id, ...]; `snapshots` is a
    /// list of version_ids; `diff_offset` is a dict version_id ->
    /// [start, length]. Dict keys are emitted in sorted order, as bencode
    /// requires.
    fn encode_index(&self) -> Vec<u8> {
        use bendy::encoding::Encoder;
        let mut parents: Vec<(Vec<u8>, Vec<Vec<u8>>)> = self
            .mem
            .parents_map()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        parents.sort_by(|a, b| a.0.cmp(&b.0));
        let mut offsets: Vec<(Vec<u8>, (u64, u64))> = self
            .diff_offset
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        offsets.sort_by(|a, b| a.0.cmp(&b.0));
        let mut snapshots: Vec<Vec<u8>> = self.mem.snapshots_set().iter().cloned().collect();
        snapshots.sort();

        let mut e = Encoder::new();
        e.emit_list(|list| {
            // parents dict
            list.emit_dict(|mut d| {
                for (k, v) in &parents {
                    d.emit_pair_with(k, |e| {
                        e.emit_list(|l| {
                            for p in v {
                                l.emit_bytes(p)?;
                            }
                            Ok(())
                        })
                    })?;
                }
                Ok(())
            })?;
            // snapshots list
            list.emit_list(|l| {
                for s in &snapshots {
                    l.emit_bytes(s)?;
                }
                Ok(())
            })?;
            // diff_offset dict
            list.emit_dict(|mut d| {
                for (k, (start, len)) in &offsets {
                    d.emit_pair_with(k, |e| {
                        e.emit_list(|l| {
                            l.emit_int(*start)?;
                            l.emit_int(*len)?;
                            Ok(())
                        })
                    })?;
                }
                Ok(())
            })?;
            Ok(())
        })
        .expect("bencode index");
        e.get_output().expect("bencode index")
    }

    #[allow(clippy::type_complexity)]
    fn decode_index(
        data: &[u8],
    ) -> Result<
        (
            Vec<(Vec<u8>, Vec<Vec<u8>>)>,
            Vec<Vec<u8>>,
            std::collections::HashMap<Vec<u8>, (u64, u64)>,
        ),
        String,
    > {
        use bendy::decoding::{Decoder, Object};
        let mut decoder = Decoder::new(data);
        let mut top = match decoder.next_object().map_err(|e| e.to_string())? {
            Some(Object::List(l)) => l,
            _ => return Err("index is not a bencode list".to_string()),
        };
        // parents dict
        let mut parents: Vec<(Vec<u8>, Vec<Vec<u8>>)> = Vec::new();
        match top.next_object().map_err(|e| e.to_string())? {
            Some(Object::Dict(mut d)) => {
                while let Some((k, v)) = d.next_pair().map_err(|e| e.to_string())? {
                    let key = k.to_vec();
                    let mut ps = Vec::new();
                    if let Object::List(mut pl) = v {
                        while let Some(p) = pl.next_object().map_err(|e| e.to_string())? {
                            ps.push(bytes_of(p)?);
                        }
                    }
                    parents.push((key, ps));
                }
            }
            _ => return Err("expected parents dict".to_string()),
        }
        // snapshots list
        let mut snapshots = Vec::new();
        match top.next_object().map_err(|e| e.to_string())? {
            Some(Object::List(mut l)) => {
                while let Some(s) = l.next_object().map_err(|e| e.to_string())? {
                    snapshots.push(bytes_of(s)?);
                }
            }
            _ => return Err("expected snapshots list".to_string()),
        }
        // diff_offset dict
        let mut diff_offset = std::collections::HashMap::new();
        match top.next_object().map_err(|e| e.to_string())? {
            Some(Object::Dict(mut d)) => {
                while let Some((k, v)) = d.next_pair().map_err(|e| e.to_string())? {
                    let key = k.to_vec();
                    if let Object::List(mut pair) = v {
                        let start = int_of(pair.next_object().map_err(|e| e.to_string())?)?;
                        let len = int_of(pair.next_object().map_err(|e| e.to_string())?)?;
                        diff_offset.insert(key, (start, len));
                    }
                }
            }
            _ => return Err("expected diff_offset dict".to_string()),
        }
        Ok((parents, snapshots, diff_offset))
    }
}

fn bytes_of(obj: bendy::decoding::Object<'_, '_>) -> Result<Vec<u8>, String> {
    match obj {
        bendy::decoding::Object::Bytes(b) => Ok(b.to_vec()),
        _ => Err("expected bencode bytes".to_string()),
    }
}

fn int_of(obj: Option<bendy::decoding::Object<'_, '_>>) -> Result<u64, String> {
    match obj {
        Some(bendy::decoding::Object::Integer(s)) => s.parse::<u64>().map_err(|e| e.to_string()),
        _ => Err("expected bencode integer".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines(s: &[&[u8]]) -> Vec<Vec<u8>> {
        s.iter().map(|l| l.to_vec()).collect()
    }

    #[test]
    fn disk_vf_save_load_roundtrip() {
        // Mirrors bzrformats test_multiparent.TestMultiVersionedFile.test_save_load.
        let dir = std::env::temp_dir().join(format!("mpvf-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let base = dir.join("foop").to_str().unwrap().to_string();

        let mut vf = DiskMultiVersionedFile::new(base.clone(), Some(25), None);
        vf.add_version(
            lines(&[b"a\n", b"b\n", b"c\n", b"d"]),
            b"a".to_vec(),
            vec![],
            None,
            false,
        )
        .unwrap();
        vf.add_version(
            lines(&[b"a\n", b"e\n", b"d\n"]),
            b"b".to_vec(),
            vec![b"a".to_vec()],
            None,
            false,
        )
        .unwrap();
        vf.save().unwrap();

        let mut newvf = DiskMultiVersionedFile::new(base, Some(25), None);
        newvf.load().unwrap();
        let a = newvf.get_line_list(&[b"a".to_vec()]).unwrap();
        assert_eq!(a[0].concat(), b"a\nb\nc\nd");
        let b = newvf.get_line_list(&[b"b".to_vec()]).unwrap();
        assert_eq!(b[0].concat(), b"a\ne\nd\n");
        newvf.destroy().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
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

    #[test]
    fn from_lines_no_parents_is_single_new_text() {
        let text = lines(&[b"a\n", b"b\n"]);
        let mp = MultiParent::from_lines_with_blocks(&text, &[]);
        assert_eq!(mp.hunks, vec![Hunk::NewText(lines(&[b"a\n", b"b\n"]))]);
    }

    #[test]
    fn from_lines_runs_patiencediff_for_each_parent() {
        // text = parent → single ParentText covering everything.
        let text = lines(&[b"a\n", b"b\n", b"c\n"]);
        let p0 = lines(&[b"a\n", b"b\n", b"c\n"]);
        let parents: Vec<&[Vec<u8>]> = vec![&p0];
        let mp = MultiParent::from_lines(&text, &parents, None);
        assert_eq!(
            mp.hunks,
            vec![Hunk::ParentText {
                parent: 0,
                parent_pos: 0,
                child_pos: 0,
                num_lines: 3,
            }]
        );
    }

    #[test]
    fn from_lines_supplied_left_blocks_skip_left_diff() {
        // Supplied blocks claim a perfect match even though parent doesn't
        // contain text — proves from_lines used them instead of running
        // patiencediff.
        let text = lines(&[b"a\n", b"b\n"]);
        let p0 = lines(&[b"x\n", b"y\n"]);
        let parents: Vec<&[Vec<u8>]> = vec![&p0];
        let mp = MultiParent::from_lines(&text, &parents, Some(vec![(0, 0, 2), (2, 2, 0)]));
        assert_eq!(
            mp.hunks,
            vec![Hunk::ParentText {
                parent: 0,
                parent_pos: 0,
                child_pos: 0,
                num_lines: 2,
            }]
        );
    }

    #[test]
    fn from_lines_single_parent_full_match() {
        // text == parent. One (0,0,2) block plus sentinel.
        let text = lines(&[b"a\n", b"b\n"]);
        let blocks = vec![vec![(0, 0, 2), (2, 2, 0)]];
        let mp = MultiParent::from_lines_with_blocks(&text, &blocks);
        assert_eq!(
            mp.hunks,
            vec![Hunk::ParentText {
                parent: 0,
                parent_pos: 0,
                child_pos: 0,
                num_lines: 2,
            }]
        );
    }

    #[test]
    fn from_lines_prefers_longest_match_across_parents() {
        // text = [a b c d]
        // parent 0 matches [a b] at (0,0,2)
        // parent 1 matches [a b c d] at (0,0,4)
        // The longest match (parent 1) should win.
        let text = lines(&[b"a\n", b"b\n", b"c\n", b"d\n"]);
        let blocks = vec![vec![(0, 0, 2), (2, 4, 0)], vec![(0, 0, 4), (4, 4, 0)]];
        let mp = MultiParent::from_lines_with_blocks(&text, &blocks);
        assert_eq!(
            mp.hunks,
            vec![Hunk::ParentText {
                parent: 1,
                parent_pos: 0,
                child_pos: 0,
                num_lines: 4,
            }]
        );
    }

    #[test]
    fn from_lines_mixes_new_text_and_parent_text() {
        // text = [x a b y]
        // parent 0 matches [a b] at (0,1,2)
        let text = lines(&[b"x\n", b"a\n", b"b\n", b"y\n"]);
        let blocks = vec![vec![(0, 1, 2), (2, 4, 0)]];
        let mp = MultiParent::from_lines_with_blocks(&text, &blocks);
        assert_eq!(
            mp.hunks,
            vec![
                Hunk::NewText(lines(&[b"x\n"])),
                Hunk::ParentText {
                    parent: 0,
                    parent_pos: 0,
                    child_pos: 1,
                    num_lines: 2,
                },
                Hunk::NewText(lines(&[b"y\n"])),
            ]
        );
    }

    #[test]
    fn from_lines_advances_block_offset_when_partial() {
        // text = [a b c]; parent provides (0,0,3) but cur_line might land
        // mid-block if a prior hunk consumed the start. Simulate this by
        // pretending a longer parent matched first.
        // text = [a b c d]
        // parent 0: single block (0,0,4)
        let text = lines(&[b"a\n", b"b\n", b"c\n", b"d\n"]);
        let blocks = vec![vec![(0, 0, 4), (4, 4, 0)]];
        let mp = MultiParent::from_lines_with_blocks(&text, &blocks);
        assert_eq!(
            mp.hunks,
            vec![Hunk::ParentText {
                parent: 0,
                parent_pos: 0,
                child_pos: 0,
                num_lines: 4,
            }]
        );
    }

    #[test]
    fn mpvf_fulltext_roundtrip_via_add_version() {
        // Add a single fulltext (no parents → snapshot), read it back.
        let mut mpvf: MultiMemoryVersionedFile<&'static str> = MultiMemoryVersionedFile::default();
        let text = lines(&[b"a\n", b"b\n"]);
        mpvf.add_version(text.clone(), "v1", vec![], None, false)
            .unwrap();
        assert!(mpvf.has_version(&"v1"));
        mpvf.clear_cache();
        let got = mpvf.get_line_list(&["v1"]).unwrap();
        assert_eq!(got, vec![text]);
    }

    #[test]
    fn mpvf_delta_reconstructs_from_parent() {
        // v1 = [a b c], v2 = [a x c] (replace line 1 with x).
        let mut mpvf: MultiMemoryVersionedFile<&'static str> = MultiMemoryVersionedFile::default();
        let v1 = lines(&[b"a\n", b"b\n", b"c\n"]);
        let v2 = lines(&[b"a\n", b"x\n", b"c\n"]);
        mpvf.add_version(v1.clone(), "v1", vec![], None, false)
            .unwrap();
        mpvf.add_version(v2.clone(), "v2", vec!["v1"], None, false)
            .unwrap();
        // Force reconstruction from chain only.
        mpvf.clear_cache();
        let got = mpvf.get_line_list(&["v2"]).unwrap();
        assert_eq!(got, vec![v2]);
    }

    #[test]
    fn mpvf_add_diff_then_reconstruct_via_get_line_list() {
        // Wire up the diff directly (the path add_mpdiffs uses) and verify
        // get_line_list walks the chain.
        let mut mpvf: MultiMemoryVersionedFile<&'static str> = MultiMemoryVersionedFile::default();
        mpvf.add_version(lines(&[b"x\n", b"y\n"]), "base", vec![], None, false)
            .unwrap();
        // Manually craft a delta that replaces line 0 with "X".
        let diff = MultiParent::with_hunks(vec![
            Hunk::NewText(lines(&[b"X\n"])),
            Hunk::ParentText {
                parent: 0,
                parent_pos: 1,
                child_pos: 1,
                num_lines: 1,
            },
        ]);
        mpvf.add_diff(diff, "child", vec!["base"]);
        // Clear the cache so reconstruct must walk the diff chain.
        mpvf.clear_cache();
        let got = mpvf.get_line_list(&["child"]).unwrap();
        assert_eq!(got, vec![lines(&[b"X\n", b"y\n"])]);
    }

    /// Split a byte string into one `"x\n"` line per byte, mirroring the
    /// Python test helper `add_version`.
    fn char_lines(s: &[u8]) -> Vec<Vec<u8>> {
        s.iter().map(|b| vec![*b, b'\n']).collect()
    }

    /// The 3-version fixture from the Python TestMultiParent.make_vf:
    /// rev-a=abcd, rev-b=acde, rev-c=abef with parents [rev-a, rev-b].
    fn make_two_parent_vf() -> MultiMemoryVersionedFile<&'static str> {
        let mut vf: MultiMemoryVersionedFile<&'static str> = MultiMemoryVersionedFile::default();
        vf.add_version(char_lines(b"abcd"), "rev-a", vec![], None, false)
            .unwrap();
        vf.add_version(char_lines(b"acde"), "rev-b", vec![], None, false)
            .unwrap();
        vf.add_version(
            char_lines(b"abef"),
            "rev-c",
            vec!["rev-a", "rev-b"],
            None,
            false,
        )
        .unwrap();
        vf
    }

    #[test]
    fn mpvf_reconstructs_version_with_two_parents() {
        // rev-c is a diff against both rev-a and rev-b; reconstructing it
        // exercises hunks that reference different parent slots.
        let mut vf = make_two_parent_vf();
        vf.clear_cache();
        let got = vf.get_line_list(&["rev-a", "rev-c"]).unwrap();
        assert_eq!(got[0], char_lines(b"abcd"));
        assert_eq!(got[1], char_lines(b"abef"));
    }

    #[test]
    fn mpvf_get_build_ranking_returns_all_versions() {
        let vf = make_two_parent_vf();
        let ranking: std::collections::HashSet<&str> = vf.get_build_ranking().into_iter().collect();
        let expected: std::collections::HashSet<&str> =
            vec!["rev-a", "rev-b", "rev-c"].into_iter().collect();
        assert_eq!(ranking, expected);
    }

    #[test]
    fn mpvf_get_build_ranking_single_version() {
        let mut vf: MultiMemoryVersionedFile<&'static str> = MultiMemoryVersionedFile::default();
        vf.add_version(char_lines(b"a"), "rev-a", vec![], None, false)
            .unwrap();
        assert_eq!(vf.get_build_ranking(), vec!["rev-a"]);
    }

    #[test]
    fn mpvf_reordered_lines_from_distinct_parent_hunks() {
        // The corner case requiring a cursor restart during reconstruction:
        // rev-e draws one line each from two different hunks of rev-b, in the
        // opposite order to how they appear in rev-b.
        let mut vf: MultiMemoryVersionedFile<&'static str> = MultiMemoryVersionedFile::default();
        vf.add_version(char_lines(b"c"), "rev-a", vec![], None, false)
            .unwrap();
        vf.add_version(char_lines(b"acb"), "rev-b", vec!["rev-a"], None, false)
            .unwrap();
        vf.add_version(char_lines(b"b"), "rev-c", vec!["rev-b"], None, false)
            .unwrap();
        vf.add_version(char_lines(b"a"), "rev-d", vec!["rev-b"], None, false)
            .unwrap();
        vf.add_version(
            char_lines(b"ba"),
            "rev-e",
            vec!["rev-c", "rev-d"],
            None,
            false,
        )
        .unwrap();
        vf.clear_cache();
        let got = vf.get_line_list(&["rev-e"]).unwrap();
        assert_eq!(got[0], char_lines(b"ba"));
    }

    #[test]
    fn mpvf_versions_preserves_insert_order() {
        let mut mpvf: MultiMemoryVersionedFile<&'static str> = MultiMemoryVersionedFile::default();
        mpvf.add_version(vec![], "a", vec![], None, false).unwrap();
        mpvf.add_version(vec![], "b", vec![], None, false).unwrap();
        mpvf.add_version(vec![], "c", vec![], None, false).unwrap();
        let v: Vec<&str> = mpvf.versions().copied().collect();
        assert_eq!(v, vec!["a", "b", "c"]);
    }

    #[test]
    fn mpvf_make_snapshot_replaces_delta_with_fulltext() {
        let mut mpvf: MultiMemoryVersionedFile<&'static str> = MultiMemoryVersionedFile::default();
        let base = lines(&[b"a\n", b"b\n"]);
        let child = lines(&[b"X\n", b"b\n"]);
        mpvf.add_version(base, "base", vec![], None, false).unwrap();
        mpvf.add_version(child.clone(), "child", vec!["base"], None, false)
            .unwrap();
        assert!(!mpvf.is_snapshot(&"child"));
        mpvf.make_snapshot("child").unwrap();
        assert!(mpvf.is_snapshot(&"child"));
        // The stored diff is now a single NewText covering the full child.
        let d = mpvf.get_diff(&"child").unwrap();
        assert!(matches!(d.hunks.as_slice(), [Hunk::NewText(_)]));
    }

    #[test]
    fn mpvf_import_diffs_copies_each_version() {
        let mut src: MultiMemoryVersionedFile<&'static str> = MultiMemoryVersionedFile::default();
        src.add_version(lines(&[b"a\n"]), "v1", vec![], None, false)
            .unwrap();
        src.add_version(lines(&[b"b\n"]), "v2", vec!["v1"], None, false)
            .unwrap();
        let mut dst: MultiMemoryVersionedFile<&'static str> = MultiMemoryVersionedFile::default();
        dst.import_diffs(&src);
        assert!(dst.has_version(&"v1"));
        assert!(dst.has_version(&"v2"));
        // Parents are preserved.
        assert_eq!(dst.get_parents(&"v2"), Some(&["v1"][..]));
    }

    #[test]
    fn mpvf_select_snapshots_picks_chain_breaks() {
        // Build a long chain; with snapshot_interval=2 every third
        // version (counting the root) should be selected.
        let mut mpvf: MultiMemoryVersionedFile<&'static str> =
            MultiMemoryVersionedFile::new(Some(2), None);
        mpvf.add_version(lines(&[b"v1\n"]), "v1", vec![], None, false)
            .unwrap();
        mpvf.add_version(lines(&[b"v2\n"]), "v2", vec!["v1"], None, false)
            .unwrap();
        mpvf.add_version(lines(&[b"v3\n"]), "v3", vec!["v2"], None, false)
            .unwrap();
        mpvf.add_version(lines(&[b"v4\n"]), "v4", vec!["v3"], None, false)
            .unwrap();
        let chosen = mpvf.select_snapshots();
        // v1 has no parents → always a snapshot. After two steps we
        // exceed the interval, so v4 (3 ancestors) is also selected.
        assert!(chosen.contains(&"v1"));
        assert!(chosen.contains(&"v4"));
    }
}

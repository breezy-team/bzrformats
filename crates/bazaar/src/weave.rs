//! Weave storage core algorithms.
//!
//! Port of the pure-logic core of `bzrformats/weave.py` plus the v5 on-disk
//! format reader/writer from `bzrformats/weavefile.py`. A weave is a single
//! flat sequence of [`WeaveEntry`] items: literal lines plus bracketed
//! insertion/deletion instructions. This module implements the annotation
//! walk (`extract`) against that representation, plus [`read_weave_v5`]
//! and [`write_weave_v5`] for the on-disk format. The Python class still
//! owns I/O, parent/name bookkeeping, and the higher-level VersionedFile
//! surface.

/// Magic header for the v5 weave file format.
pub const WEAVE_V5_FORMAT: &[u8] = b"# bzr weave file v5\n";

/// A deserialized weave file: per-version metadata plus the flat weave
/// instruction/line stream.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WeaveFile {
    pub parents: Vec<Vec<usize>>,
    pub sha1s: Vec<Vec<u8>>,
    pub names: Vec<Vec<u8>>,
    pub weave: Vec<WeaveEntry>,
}

/// Compute the sha1 hex digest of the concatenation of `lines`. Mirrors
/// `bzrformats.osutils.sha_strings`, which weave uses to checksum each
/// version's content.
pub fn sha_strings<L: AsRef<[u8]>>(lines: &[L]) -> Vec<u8> {
    use sha1::{Digest, Sha1};
    let mut hasher = Sha1::new();
    for line in lines {
        hasher.update(line.as_ref());
    }
    let digest = hasher.finalize();
    let mut hex = vec![0u8; digest.len() * 2];
    for (i, byte) in digest.iter().enumerate() {
        let high = byte >> 4;
        let low = byte & 0x0f;
        hex[i * 2] = if high < 10 {
            b'0' + high
        } else {
            b'a' + high - 10
        };
        hex[i * 2 + 1] = if low < 10 {
            b'0' + low
        } else {
            b'a' + low - 10
        };
    }
    hex
}

impl WeaveFile {
    /// Look up a version index by name. Mirrors `Weave._lookup` (linear
    /// scan of `_names`).
    pub fn lookup(&self, name: &[u8]) -> Option<usize> {
        self.names.iter().position(|n| n == name)
    }

    /// Compute the sha1 of the lines making up `version` and verify it
    /// against the stored sha1. Mirrors `Weave.get_lines`.
    pub fn get_lines(&self, version: usize) -> Result<Vec<Vec<u8>>, WeaveError> {
        if version >= self.parents.len() {
            return Err(WeaveError::RevisionNotPresent(version));
        }
        let included = inclusions(&self.parents, &[version]);
        let extracted = extract(&self.weave, &included)?;
        let result: Vec<Vec<u8>> = extracted.iter().map(|e| e.text.to_vec()).collect();
        let measured = sha_strings(&result);
        let expected = &self.sha1s[version];
        if &measured != expected {
            return Err(WeaveError::InvalidChecksum {
                version,
                expected: expected.clone(),
                measured,
            });
        }
        Ok(result)
    }

    /// Return `(originating-version-name, line)` pairs for `version`.
    /// Mirrors `Weave.annotate`.
    pub fn annotate(&self, version: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, WeaveError> {
        if version >= self.parents.len() {
            return Err(WeaveError::RevisionNotPresent(version));
        }
        let included = inclusions(&self.parents, &[version]);
        let extracted = extract(&self.weave, &included)?;
        Ok(extracted
            .into_iter()
            .map(|e| (self.names[e.origin].clone(), e.text.to_vec()))
            .collect())
    }

    /// Add a single text on top of the weave.
    ///
    /// Returns the index of the new version. Port of `Weave._add`.
    ///
    /// * `version_id`: symbolic name. If `None`, allocated as `b"sha1:" + sha1`.
    /// * `parents`: direct parent indices.
    /// * `sha1`: precomputed sha1 hex; if `None`, hashed from `lines`.
    /// * `nostore_sha`: if `Some` and equal to the new sha1, returns
    ///   `Err(WeaveError::ExistingContent)` without storing.
    pub fn add(
        &mut self,
        version_id: Option<&[u8]>,
        lines: &[Vec<u8>],
        parents: &[usize],
        sha1: Option<Vec<u8>>,
        nostore_sha: Option<&[u8]>,
    ) -> Result<usize, WeaveError> {
        let sha1 = sha1.unwrap_or_else(|| sha_strings(lines));
        if let Some(no) = nostore_sha {
            if no == sha1.as_slice() {
                return Err(WeaveError::ExistingContent);
            }
        }
        let owned_name: Vec<u8>;
        let version_id: &[u8] = match version_id {
            Some(v) => v,
            None => {
                owned_name = {
                    let mut s = b"sha1:".to_vec();
                    s.extend_from_slice(&sha1);
                    s
                };
                &owned_name
            }
        };

        if let Some(idx) = self.lookup(version_id) {
            return self.check_repeated_add(version_id, parents, &sha1, idx);
        }

        for &p in parents {
            if p >= self.parents.len() {
                return Err(WeaveError::RevisionNotPresent(p));
            }
        }

        let new_version = self.parents.len();
        self.parents.push(parents.to_vec());
        self.sha1s.push(sha1.clone());
        self.names.push(version_id.to_vec());

        if parents.is_empty() {
            // Special case: fresh root. Skip the diff and just append the
            // lines wrapped in a single insertion block.
            if !lines.is_empty() {
                self.weave.push(WeaveEntry::Control {
                    op: Instruction::InsertOpen,
                    version: new_version,
                });
                for line in lines {
                    self.weave.push(WeaveEntry::Line(line.clone()));
                }
                self.weave.push(WeaveEntry::Control {
                    op: Instruction::InsertClose,
                    version: new_version,
                });
            }
            return Ok(new_version);
        }

        if parents.len() == 1 && self.sha1s[parents[0]] == sha1 {
            // Single parent, identical text — no edits to record.
            return Ok(new_version);
        }

        let ancestors = inclusions(&self.parents, parents);
        let extracted = extract(&self.weave, &ancestors)?;

        // basis_lineno[i] = absolute index in self.weave of basis line i.
        // basis_lines[i]  = bytes of basis line i.
        let mut basis_lineno: Vec<usize> = extracted.iter().map(|e| e.lineno).collect();
        let basis_lines: Vec<&[u8]> = extracted.iter().map(|e| e.text).collect();

        // Identical merged text: nothing to record.
        if basis_lines.len() == lines.len()
            && basis_lines
                .iter()
                .zip(lines.iter())
                .all(|(a, b)| *a == b.as_slice())
        {
            return Ok(new_version);
        }

        // Sentinel: a virtual basis line at the end of the weave so the
        // diff can refer to "insert at the end".
        basis_lineno.push(self.weave.len());

        let basis_owned: Vec<Vec<u8>> = basis_lines.iter().map(|s| s.to_vec()).collect();
        let mut sm = patiencediff::SequenceMatcher::new(&basis_owned, lines);
        let opcodes = sm.get_opcodes();

        // `offset` tracks how many entries have been spliced into self.weave
        // since the start of this loop, so the next i1/i2 (which were
        // computed against the *pre-mutation* layout) can be translated to
        // the current layout.
        let mut offset: isize = 0;
        for op in opcodes {
            if matches!(op, patiencediff::Opcode::Equal(_, _, _, _)) {
                continue;
            }
            let i1_basis = op.a_start();
            let i2_basis = op.a_end();
            let j1 = op.b_start();
            let j2 = op.b_end();
            let i1 = basis_lineno[i1_basis];
            let i2 = basis_lineno[i2_basis];

            // Apply deletion bracket first: insert `[` before line i1 and
            // `]` after line i2-1, both in *current* coordinates.
            if i1 != i2 {
                let pos1 = (i1 as isize + offset) as usize;
                self.weave.insert(
                    pos1,
                    WeaveEntry::Control {
                        op: Instruction::DeleteOpen,
                        version: new_version,
                    },
                );
                let pos2 = (i2 as isize + offset + 1) as usize;
                self.weave.insert(
                    pos2,
                    WeaveEntry::Control {
                        op: Instruction::DeleteClose,
                        version: new_version,
                    },
                );
                offset += 2;
            }

            if j1 != j2 {
                // Insert the new lines wrapped in `{`/`}` after the (now
                // bracketed) deletion region.
                let i = (i2 as isize + offset) as usize;
                let mut splice: Vec<WeaveEntry> = Vec::with_capacity(j2 - j1 + 2);
                splice.push(WeaveEntry::Control {
                    op: Instruction::InsertOpen,
                    version: new_version,
                });
                for line in &lines[j1..j2] {
                    splice.push(WeaveEntry::Line(line.clone()));
                }
                splice.push(WeaveEntry::Control {
                    op: Instruction::InsertClose,
                    version: new_version,
                });
                let added = splice.len();
                let tail = self.weave.split_off(i);
                self.weave.extend(splice);
                self.weave.extend(tail);
                offset += added as isize;
            }
        }

        Ok(new_version)
    }

    fn check_repeated_add(
        &self,
        name: &[u8],
        parents: &[usize],
        sha1: &[u8],
        idx: usize,
    ) -> Result<usize, WeaveError> {
        let mut existing = self.parents[idx].clone();
        existing.sort_unstable();
        let mut requested = parents.to_vec();
        requested.sort_unstable();
        if existing != requested || self.sha1s[idx] != sha1 {
            return Err(WeaveError::RevisionAlreadyPresent(name.to_vec()));
        }
        Ok(idx)
    }
}

/// Errors from reading a v5 weave file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WeaveFileError {
    /// The file was empty or its first line wasn't the magic header.
    BadHeader(Vec<u8>),
    /// The file ended mid-record.
    UnexpectedEof,
    /// A header or body line didn't match any known form.
    UnexpectedLine(Vec<u8>),
    /// A numeric field (parent index, instruction version) couldn't be
    /// parsed as a decimal integer.
    InvalidInteger(Vec<u8>),
}

impl std::fmt::Display for WeaveFileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WeaveFileError::BadHeader(l) => write!(f, "invalid weave file header: {:?}", l),
            WeaveFileError::UnexpectedEof => write!(f, "unexpected end of weave file"),
            WeaveFileError::UnexpectedLine(l) => write!(f, "unexpected line {:?}", l),
            WeaveFileError::InvalidInteger(s) => write!(f, "not a valid integer: {:?}", s),
        }
    }
}

impl std::error::Error for WeaveFileError {}

/// Instruction bracket kind in a weave entry stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Instruction {
    /// Open an insertion block introduced by `version`.
    InsertOpen,
    /// Close the most recently opened insertion block. `version` is ignored.
    InsertClose,
    /// Open a deletion block applied by `version`.
    DeleteOpen,
    /// Close a deletion block applied by `version`.
    DeleteClose,
}

/// One entry in a weave: either a literal line or a control instruction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WeaveEntry {
    Line(Vec<u8>),
    Control { op: Instruction, version: usize },
}

/// Errors from walking a malformed weave or from higher-level Weave ops.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WeaveError {
    /// `}` appeared with no matching `{`.
    UnmatchedInsertClose,
    /// `]` appeared for a deletion that wasn't open (in the included set).
    UnmatchedDeleteClose(usize),
    /// Insertion stack non-empty at end of weave.
    UnclosedInsertions(Vec<usize>),
    /// Deletion set non-empty at end of weave.
    UnclosedDeletions(Vec<usize>),
    /// `add` was called with a name that already exists but with parents
    /// or a sha1 that don't match the existing entry.
    RevisionAlreadyPresent(Vec<u8>),
    /// `add` referenced a parent index that doesn't exist.
    RevisionNotPresent(usize),
    /// `add` was called with `nostore_sha` matching the new content's sha1.
    ExistingContent,
    /// On-disk sha1 didn't match the recomputed sha1 for `get_lines`.
    InvalidChecksum {
        version: usize,
        expected: Vec<u8>,
        measured: Vec<u8>,
    },
}

impl std::fmt::Display for WeaveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WeaveError::UnmatchedInsertClose => write!(f, "unmatched '}}' in weave"),
            WeaveError::UnmatchedDeleteClose(v) => {
                write!(f, "unmatched ']' for version {} in weave", v)
            }
            WeaveError::UnclosedInsertions(v) => {
                write!(f, "unclosed insertion blocks at end of weave: {:?}", v)
            }
            WeaveError::UnclosedDeletions(v) => {
                write!(f, "unclosed deletion blocks at end of weave: {:?}", v)
            }
            WeaveError::RevisionAlreadyPresent(name) => {
                write!(f, "revision {:?} already present", name)
            }
            WeaveError::RevisionNotPresent(v) => {
                write!(f, "revision index {} not present", v)
            }
            WeaveError::ExistingContent => write!(f, "content already stored under nostore_sha"),
            WeaveError::InvalidChecksum {
                version,
                expected,
                measured,
            } => write!(
                f,
                "invalid checksum for version {}: expected {:?}, measured {:?}",
                version, expected, measured
            ),
        }
    }
}

impl std::error::Error for WeaveError {}

/// One yielded item from [`extract`]: the originating version index, the
/// absolute line number in the weave, and a borrow of the line bytes.
#[derive(Debug, PartialEq, Eq)]
pub struct ExtractLine<'a> {
    pub origin: usize,
    pub lineno: usize,
    pub text: &'a [u8],
}

/// Walk `weave` yielding lines that are active in the given `included`
/// version set. Mirrors `Weave._extract` in `bzrformats/weave.py`.
///
/// `included` should already contain the transitive closure of
/// ancestors for the versions of interest (see `inclusions`, added in a
/// follow-up). The caller passes indices into the weave's version table.
pub fn extract<'a>(
    weave: &'a [WeaveEntry],
    included: &std::collections::HashSet<usize>,
) -> Result<Vec<ExtractLine<'a>>, WeaveError> {
    let mut istack: Vec<usize> = Vec::new();
    let mut dset: std::collections::HashSet<usize> = std::collections::HashSet::new();
    let mut isactive: Option<bool> = None;
    let mut result = Vec::new();

    for (lineno, entry) in weave.iter().enumerate() {
        match entry {
            WeaveEntry::Control { op, version } => {
                isactive = None;
                match op {
                    Instruction::InsertOpen => istack.push(*version),
                    Instruction::InsertClose => {
                        istack.pop().ok_or(WeaveError::UnmatchedInsertClose)?;
                    }
                    Instruction::DeleteOpen => {
                        if included.contains(version) {
                            dset.insert(*version);
                        }
                    }
                    Instruction::DeleteClose => {
                        if included.contains(version) && !dset.remove(version) {
                            return Err(WeaveError::UnmatchedDeleteClose(*version));
                        }
                    }
                }
            }
            WeaveEntry::Line(text) => {
                let active = match isactive {
                    Some(a) => a,
                    None => {
                        let a = dset.is_empty()
                            && istack.last().is_some_and(|top| included.contains(top));
                        isactive = Some(a);
                        a
                    }
                };
                if active {
                    result.push(ExtractLine {
                        origin: *istack.last().expect("active implies non-empty istack"),
                        lineno,
                        text,
                    });
                }
            }
        }
    }

    if !istack.is_empty() {
        return Err(WeaveError::UnclosedInsertions(istack));
    }
    if !dset.is_empty() {
        let mut v: Vec<usize> = dset.into_iter().collect();
        v.sort_unstable();
        return Err(WeaveError::UnclosedDeletions(v));
    }
    Ok(result)
}

/// Compute the set of ancestor version indices of `versions`, inclusive.
///
/// Mirrors `Weave._inclusions`: starts with the input set and, for each
/// version from `max..=1` that is in the set, unions in its immediate
/// parents from `parents_by_version`. Version 0 is treated as a root and
/// its parent list is never expanded — this matches the Python off-by-one
/// (`range(max(versions), 0, -1)`).
pub fn inclusions(
    parents_by_version: &[Vec<usize>],
    versions: &[usize],
) -> std::collections::HashSet<usize> {
    let mut out = std::collections::HashSet::new();
    if versions.is_empty() {
        return out;
    }
    out.extend(versions.iter().copied());
    let max_v = *versions.iter().max().expect("non-empty");
    for v in (1..=max_v).rev() {
        if out.contains(&v) {
            if let Some(ps) = parents_by_version.get(v) {
                out.extend(ps.iter().copied());
            }
        }
    }
    out
}

/// One yielded item from [`walk_internal`]: the absolute line number, the
/// innermost open insertion version, the set of active deletion versions,
/// and a borrow of the line bytes. Matches `Weave._walk_internal` but with
/// indices rather than resolved names.
#[derive(Debug, PartialEq, Eq)]
pub struct WalkLine<'a> {
    pub lineno: usize,
    pub insert: usize,
    pub deletes: Vec<usize>,
    pub text: &'a [u8],
}

/// Walk `weave` yielding every literal line along with its open-insertion
/// version and the current deletion set. Unlike [`extract`], this doesn't
/// filter on an `included` set — callers decide what to do with each line.
pub fn walk_internal(weave: &[WeaveEntry]) -> Result<Vec<WalkLine<'_>>, WeaveError> {
    let mut istack: Vec<usize> = Vec::new();
    let mut dset: std::collections::BTreeSet<usize> = std::collections::BTreeSet::new();
    let mut result = Vec::new();

    for (lineno, entry) in weave.iter().enumerate() {
        match entry {
            WeaveEntry::Control { op, version } => match op {
                Instruction::InsertOpen => istack.push(*version),
                Instruction::InsertClose => {
                    istack.pop().ok_or(WeaveError::UnmatchedInsertClose)?;
                }
                Instruction::DeleteOpen => {
                    dset.insert(*version);
                }
                Instruction::DeleteClose => {
                    if !dset.remove(version) {
                        return Err(WeaveError::UnmatchedDeleteClose(*version));
                    }
                }
            },
            WeaveEntry::Line(text) => {
                let insert = *istack.last().expect("line outside any insertion block");
                result.push(WalkLine {
                    lineno,
                    insert,
                    deletes: dset.iter().copied().collect(),
                    text,
                });
            }
        }
    }

    if !istack.is_empty() {
        return Err(WeaveError::UnclosedInsertions(istack));
    }
    if !dset.is_empty() {
        return Err(WeaveError::UnclosedDeletions(dset.into_iter().collect()));
    }
    Ok(result)
}

/// Parse a v5 weave file from its raw bytes. Mirrors
/// `bzrformats.weavefile._read_weave_v5`.
pub fn read_weave_v5(data: &[u8]) -> Result<WeaveFile, WeaveFileError> {
    let lines = split_with_newlines(data);
    let mut iter = lines.into_iter();

    let first = iter.next().ok_or(WeaveFileError::UnexpectedEof)?;
    if first != WEAVE_V5_FORMAT {
        return Err(WeaveFileError::BadHeader(first.to_vec()));
    }

    let mut out = WeaveFile::default();

    // Per-version metadata: `i[ parents...]`, `1 sha1`, `n name`, blank.
    loop {
        let line = iter.next().ok_or(WeaveFileError::UnexpectedEof)?;
        if line == b"w\n" {
            break;
        }
        if line.first() == Some(&b'i') {
            // `b"i\n"` is no-parents; `b"i <int>( <int>)*\n"` is a parent list.
            let ps = if line.len() > 2 {
                let trimmed = trim_trailing_newline(&line[2..]);
                let mut result = Vec::new();
                for part in trimmed.split(|&b| b == b' ') {
                    result.push(parse_usize(part)?);
                }
                result
            } else {
                Vec::new()
            };
            out.parents.push(ps);

            let sha1_line = iter.next().ok_or(WeaveFileError::UnexpectedEof)?;
            out.sha1s
                .push(trim_trailing_newline(&sha1_line[2..]).to_vec());

            let name_line = iter.next().ok_or(WeaveFileError::UnexpectedEof)?;
            out.names
                .push(trim_trailing_newline(&name_line[2..]).to_vec());

            // Consume the trailing blank line between records.
            iter.next().ok_or(WeaveFileError::UnexpectedEof)?;
        } else {
            return Err(WeaveFileError::UnexpectedLine(line.to_vec()));
        }
    }

    // Body: weave entries terminated by `W\n`.
    loop {
        let line = iter.next().ok_or(WeaveFileError::UnexpectedEof)?;
        if line == b"W\n" {
            break;
        }
        if line.starts_with(b". ") {
            // Literal line that includes its trailing newline.
            out.weave.push(WeaveEntry::Line(line[2..].to_vec()));
        } else if line.starts_with(b", ") {
            // Literal line that doesn't end in a newline — strip the wrapper.
            out.weave
                .push(WeaveEntry::Line(trim_trailing_newline(&line[2..]).to_vec()));
        } else if line == b"}\n" {
            out.weave.push(WeaveEntry::Control {
                op: Instruction::InsertClose,
                version: 0,
            });
        } else {
            let tag = *line
                .first()
                .ok_or_else(|| WeaveFileError::UnexpectedLine(line.to_vec()))?;
            let op = match tag {
                b'{' => Instruction::InsertOpen,
                b'[' => Instruction::DeleteOpen,
                b']' => Instruction::DeleteClose,
                _ => return Err(WeaveFileError::UnexpectedLine(line.to_vec())),
            };
            // The version number is ASCII digits after `"X "` up to the
            // trailing `\n`.
            if line.len() < 3 || line[1] != b' ' {
                return Err(WeaveFileError::UnexpectedLine(line.to_vec()));
            }
            let version = parse_usize(trim_trailing_newline(&line[2..]))?;
            out.weave.push(WeaveEntry::Control { op, version });
        }
    }

    Ok(out)
}

/// Serialize a [`WeaveFile`] to the v5 on-disk byte format. Mirrors
/// `bzrformats.weavefile.write_weave_v5`.
pub fn write_weave_v5(wf: &WeaveFile) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(WEAVE_V5_FORMAT);

    for version in 0..wf.parents.len() {
        let parents = &wf.parents[version];
        if parents.is_empty() {
            out.extend_from_slice(b"i\n");
        } else {
            out.extend_from_slice(b"i ");
            for (i, &p) in parents.iter().enumerate() {
                if i > 0 {
                    out.push(b' ');
                }
                out.extend_from_slice(p.to_string().as_bytes());
            }
            out.push(b'\n');
        }
        out.extend_from_slice(b"1 ");
        out.extend_from_slice(&wf.sha1s[version]);
        out.push(b'\n');
        out.extend_from_slice(b"n ");
        out.extend_from_slice(&wf.names[version]);
        out.push(b'\n');
        out.push(b'\n');
    }

    out.extend_from_slice(b"w\n");

    for entry in &wf.weave {
        match entry {
            WeaveEntry::Control { op, version } => match op {
                Instruction::InsertClose => out.extend_from_slice(b"}\n"),
                Instruction::InsertOpen => {
                    out.extend_from_slice(b"{ ");
                    out.extend_from_slice(version.to_string().as_bytes());
                    out.push(b'\n');
                }
                Instruction::DeleteOpen => {
                    out.extend_from_slice(b"[ ");
                    out.extend_from_slice(version.to_string().as_bytes());
                    out.push(b'\n');
                }
                Instruction::DeleteClose => {
                    out.extend_from_slice(b"] ");
                    out.extend_from_slice(version.to_string().as_bytes());
                    out.push(b'\n');
                }
            },
            WeaveEntry::Line(line) => {
                if line.is_empty() {
                    out.extend_from_slice(b", \n");
                } else if line.last() == Some(&b'\n') {
                    out.extend_from_slice(b". ");
                    out.extend_from_slice(line);
                } else {
                    out.extend_from_slice(b", ");
                    out.extend_from_slice(line);
                    out.push(b'\n');
                }
            }
        }
    }

    out.extend_from_slice(b"W\n");
    out
}

/// Split `data` on `\n`, keeping the newline at the end of each line except
/// the last. Mirrors Python's `readlines()` semantics.
fn split_with_newlines(data: &[u8]) -> Vec<&[u8]> {
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

fn trim_trailing_newline(line: &[u8]) -> &[u8] {
    if line.last() == Some(&b'\n') {
        &line[..line.len() - 1]
    } else {
        line
    }
}

fn parse_usize(bytes: &[u8]) -> Result<usize, WeaveFileError> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .ok_or_else(|| WeaveFileError::InvalidInteger(bytes.to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn line(s: &[u8]) -> WeaveEntry {
        WeaveEntry::Line(s.to_vec())
    }

    fn ctl(op: Instruction, v: usize) -> WeaveEntry {
        WeaveEntry::Control { op, version: v }
    }

    fn set(xs: &[usize]) -> HashSet<usize> {
        xs.iter().copied().collect()
    }

    /// Simplest weave: a single version 0 inserts three lines.
    #[test]
    fn single_version_extract() {
        let weave = vec![
            ctl(Instruction::InsertOpen, 0),
            line(b"a\n"),
            line(b"b\n"),
            line(b"c\n"),
            ctl(Instruction::InsertClose, 0),
        ];
        let got = extract(&weave, &set(&[0])).unwrap();
        let lines: Vec<&[u8]> = got.iter().map(|e| e.text).collect();
        assert_eq!(lines, vec![b"a\n".as_slice(), b"b\n", b"c\n"]);
        assert!(got.iter().all(|e| e.origin == 0));
    }

    /// An excluded version's lines don't appear even though the weave
    /// still contains them.
    #[test]
    fn excluded_version_filtered() {
        let weave = vec![
            ctl(Instruction::InsertOpen, 0),
            line(b"base\n"),
            ctl(Instruction::InsertClose, 0),
            ctl(Instruction::InsertOpen, 1),
            line(b"only-in-1\n"),
            ctl(Instruction::InsertClose, 1),
        ];
        let got = extract(&weave, &set(&[0])).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].text, b"base\n");
        assert_eq!(got[0].origin, 0);
    }

    /// A version-1 insertion nested inside version-0 keeps the origin
    /// pointing at version 1 (innermost open insertion).
    #[test]
    fn nested_insertion_origin() {
        let weave = vec![
            ctl(Instruction::InsertOpen, 0),
            line(b"top\n"),
            ctl(Instruction::InsertOpen, 1),
            line(b"nested\n"),
            ctl(Instruction::InsertClose, 1),
            line(b"bottom\n"),
            ctl(Instruction::InsertClose, 0),
        ];
        let got = extract(&weave, &set(&[0, 1])).unwrap();
        let pairs: Vec<(usize, &[u8])> = got.iter().map(|e| (e.origin, e.text)).collect();
        assert_eq!(
            pairs,
            vec![(0, b"top\n".as_slice()), (1, b"nested\n"), (0, b"bottom\n"),]
        );
    }

    /// A deletion applied by version 1 suppresses a version-0 line when
    /// version 1 is in the included set.
    #[test]
    fn deletion_suppresses_line() {
        let weave = vec![
            ctl(Instruction::InsertOpen, 0),
            line(b"keep\n"),
            ctl(Instruction::DeleteOpen, 1),
            line(b"gone\n"),
            ctl(Instruction::DeleteClose, 1),
            line(b"also\n"),
            ctl(Instruction::InsertClose, 0),
        ];
        let got_v0 = extract(&weave, &set(&[0])).unwrap();
        assert_eq!(got_v0.len(), 3, "without version 1, delete is inert");
        let got_v01 = extract(&weave, &set(&[0, 1])).unwrap();
        let lines: Vec<&[u8]> = got_v01.iter().map(|e| e.text).collect();
        assert_eq!(lines, vec![b"keep\n".as_slice(), b"also\n"]);
    }

    #[test]
    fn unclosed_insertion_errors() {
        let weave = vec![ctl(Instruction::InsertOpen, 0), line(b"x\n")];
        assert_eq!(
            extract(&weave, &set(&[0])),
            Err(WeaveError::UnclosedInsertions(vec![0]))
        );
    }

    #[test]
    fn unmatched_close_errors() {
        let weave = vec![ctl(Instruction::InsertClose, 0)];
        assert_eq!(
            extract(&weave, &set(&[0])),
            Err(WeaveError::UnmatchedInsertClose)
        );
    }

    /// An inactive insertion's lines aren't emitted even if a deletion
    /// is also open inside them.
    #[test]
    fn inclusions_empty_input() {
        assert!(inclusions(&[vec![]], &[]).is_empty());
    }

    #[test]
    fn inclusions_linear_chain() {
        // 0 <- 1 <- 2 <- 3
        let parents = vec![vec![], vec![0], vec![1], vec![2]];
        let got = inclusions(&parents, &[3]);
        assert_eq!(got, set(&[0, 1, 2, 3]));
    }

    #[test]
    fn inclusions_version_zero_root_is_not_expanded() {
        // Verify the Python off-by-one: version 0's parents slot is
        // never consulted. Put a nonsense sentinel parent there and
        // make sure it doesn't leak into the result.
        let parents = vec![vec![999], vec![0]];
        let got = inclusions(&parents, &[1]);
        assert_eq!(got, set(&[0, 1]));
    }

    #[test]
    fn inclusions_merges_converge() {
        // 0 -- 1 -- 3
        //  \-- 2 --/
        let parents = vec![vec![], vec![0], vec![0], vec![1, 2]];
        let got = inclusions(&parents, &[3]);
        assert_eq!(got, set(&[0, 1, 2, 3]));
    }

    #[test]
    fn walk_internal_reports_deletes() {
        let weave = vec![
            ctl(Instruction::InsertOpen, 0),
            line(b"a\n"),
            ctl(Instruction::DeleteOpen, 1),
            line(b"b\n"),
            ctl(Instruction::DeleteClose, 1),
            ctl(Instruction::InsertClose, 0),
        ];
        let got = walk_internal(&weave).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].text, b"a\n");
        assert_eq!(got[0].insert, 0);
        assert!(got[0].deletes.is_empty());
        assert_eq!(got[1].text, b"b\n");
        assert_eq!(got[1].insert, 0);
        assert_eq!(got[1].deletes, vec![1]);
    }

    #[test]
    fn three_way_merge_extract() {
        // Mirrors test_weave.test_multi_line_merge. The weave shape is
        // captured from a real `Weave` instance (not hand-crafted) so the
        // test exercises the exact nesting of insertions and deletions
        // that `_add` produces for a three-way merge.
        let weave = vec![
            ctl(Instruction::InsertOpen, 0),
            line(b"header"),
            ctl(Instruction::InsertClose, 0),
            ctl(Instruction::InsertOpen, 1),
            line(b""),
            line(b"line from 1"),
            ctl(Instruction::InsertClose, 1),
            ctl(Instruction::InsertOpen, 2),
            ctl(Instruction::DeleteOpen, 3),
            line(b""),
            ctl(Instruction::DeleteClose, 3),
            ctl(Instruction::InsertOpen, 3),
            line(b"fixup line"),
            ctl(Instruction::InsertClose, 3),
            line(b"line from 2"),
            ctl(Instruction::DeleteOpen, 3),
            line(b"more from 2"),
            ctl(Instruction::InsertClose, 2),
            ctl(Instruction::DeleteClose, 3),
        ];
        let got = extract(&weave, &set(&[0, 1, 2, 3])).unwrap();
        let pairs: Vec<(usize, &[u8])> = got.iter().map(|e| (e.origin, e.text)).collect();
        assert_eq!(
            pairs,
            vec![
                (0, b"header".as_slice()),
                (1, b""),
                (1, b"line from 1"),
                (3, b"fixup line"),
                (2, b"line from 2"),
            ]
        );
    }

    #[test]
    fn read_weave_v5_minimal() {
        // One version, no parents, one literal line.
        let mut data = WEAVE_V5_FORMAT.to_vec();
        data.extend_from_slice(b"i\n1 0000000000000000000000000000000000000000\nn text0\n\n");
        data.extend_from_slice(b"w\n");
        data.extend_from_slice(b"{ 0\n. hello\n}\n");
        data.extend_from_slice(b"W\n");

        let wf = read_weave_v5(&data).unwrap();
        assert_eq!(wf.parents, vec![Vec::<usize>::new()]);
        assert_eq!(
            wf.sha1s,
            vec![b"0000000000000000000000000000000000000000".to_vec()]
        );
        assert_eq!(wf.names, vec![b"text0".to_vec()]);
        assert_eq!(
            wf.weave,
            vec![
                WeaveEntry::Control {
                    op: Instruction::InsertOpen,
                    version: 0,
                },
                WeaveEntry::Line(b"hello\n".to_vec()),
                WeaveEntry::Control {
                    op: Instruction::InsertClose,
                    version: 0,
                },
            ]
        );
    }

    #[test]
    fn read_weave_v5_with_parents_and_no_eol_line() {
        // Two versions: the second has parent 0, and the body contains a
        // `", "` line (no trailing newline) plus a deletion bracket.
        let mut data = WEAVE_V5_FORMAT.to_vec();
        data.extend_from_slice(b"i\n1 aaa\nn text0\n\n");
        data.extend_from_slice(b"i 0\n1 bbb\nn text1\n\n");
        data.extend_from_slice(b"w\n");
        data.extend_from_slice(b"{ 0\n. line\n, noeol\n}\n");
        data.extend_from_slice(b"[ 1\n, gone\n] 1\n");
        data.extend_from_slice(b"W\n");

        let wf = read_weave_v5(&data).unwrap();
        assert_eq!(wf.parents, vec![Vec::<usize>::new(), vec![0]]);
        assert_eq!(wf.sha1s, vec![b"aaa".to_vec(), b"bbb".to_vec()]);
        assert_eq!(wf.names, vec![b"text0".to_vec(), b"text1".to_vec()]);
        assert_eq!(
            wf.weave,
            vec![
                WeaveEntry::Control {
                    op: Instruction::InsertOpen,
                    version: 0,
                },
                WeaveEntry::Line(b"line\n".to_vec()),
                WeaveEntry::Line(b"noeol".to_vec()),
                WeaveEntry::Control {
                    op: Instruction::InsertClose,
                    version: 0,
                },
                WeaveEntry::Control {
                    op: Instruction::DeleteOpen,
                    version: 1,
                },
                WeaveEntry::Line(b"gone".to_vec()),
                WeaveEntry::Control {
                    op: Instruction::DeleteClose,
                    version: 1,
                },
            ]
        );
    }

    #[test]
    fn read_weave_v5_multiple_parents_on_one_version() {
        // Version 2 has parents [0, 1].
        let mut data = WEAVE_V5_FORMAT.to_vec();
        data.extend_from_slice(b"i\n1 a\nn v0\n\n");
        data.extend_from_slice(b"i 0\n1 b\nn v1\n\n");
        data.extend_from_slice(b"i 0 1\n1 c\nn v2\n\n");
        data.extend_from_slice(b"w\nW\n");

        let wf = read_weave_v5(&data).unwrap();
        assert_eq!(wf.parents, vec![vec![], vec![0], vec![0, 1]]);
        assert_eq!(wf.weave, Vec::<WeaveEntry>::new());
    }

    #[test]
    fn read_weave_v5_empty_line_roundtrips_to_empty_bytes() {
        // The `", "` form with an empty payload represents an empty line.
        let mut data = WEAVE_V5_FORMAT.to_vec();
        data.extend_from_slice(b"i\n1 a\nn v0\n\n");
        data.extend_from_slice(b"w\n{ 0\n, \n}\nW\n");

        let wf = read_weave_v5(&data).unwrap();
        assert_eq!(
            wf.weave,
            vec![
                WeaveEntry::Control {
                    op: Instruction::InsertOpen,
                    version: 0,
                },
                WeaveEntry::Line(b"".to_vec()),
                WeaveEntry::Control {
                    op: Instruction::InsertClose,
                    version: 0,
                },
            ]
        );
    }

    #[test]
    fn read_weave_v5_rejects_bad_header() {
        let err = read_weave_v5(b"not-a-weave\n").unwrap_err();
        assert!(matches!(err, WeaveFileError::BadHeader(_)));
    }

    #[test]
    fn read_weave_v5_rejects_empty_input() {
        assert_eq!(read_weave_v5(b""), Err(WeaveFileError::UnexpectedEof));
    }

    #[test]
    fn read_weave_v5_rejects_truncated_after_header() {
        let err = read_weave_v5(WEAVE_V5_FORMAT).unwrap_err();
        assert_eq!(err, WeaveFileError::UnexpectedEof);
    }

    fn sample_weave_file() -> WeaveFile {
        WeaveFile {
            parents: vec![vec![], vec![0], vec![0, 1]],
            sha1s: vec![
                b"1111111111111111111111111111111111111111".to_vec(),
                b"2222222222222222222222222222222222222222".to_vec(),
                b"3333333333333333333333333333333333333333".to_vec(),
            ],
            names: vec![b"text0".to_vec(), b"text1".to_vec(), b"merge".to_vec()],
            weave: vec![
                WeaveEntry::Control {
                    op: Instruction::InsertOpen,
                    version: 0,
                },
                WeaveEntry::Line(b"hello\n".to_vec()),
                WeaveEntry::Line(b"no-eol".to_vec()),
                WeaveEntry::Control {
                    op: Instruction::InsertClose,
                    version: 0,
                },
                WeaveEntry::Control {
                    op: Instruction::DeleteOpen,
                    version: 1,
                },
                WeaveEntry::Line(b"".to_vec()),
                WeaveEntry::Control {
                    op: Instruction::DeleteClose,
                    version: 1,
                },
            ],
        }
    }

    #[test]
    fn write_weave_v5_shape() {
        let expected: Vec<u8> = [
            b"# bzr weave file v5\n".as_slice(),
            b"i\n1 1111111111111111111111111111111111111111\nn text0\n\n",
            b"i 0\n1 2222222222222222222222222222222222222222\nn text1\n\n",
            b"i 0 1\n1 3333333333333333333333333333333333333333\nn merge\n\n",
            b"w\n",
            b"{ 0\n. hello\n, no-eol\n}\n",
            b"[ 1\n, \n] 1\n",
            b"W\n",
        ]
        .concat();
        assert_eq!(write_weave_v5(&sample_weave_file()), expected);
    }

    #[test]
    fn weave_file_round_trip() {
        let wf = sample_weave_file();
        let bytes = write_weave_v5(&wf);
        let parsed = read_weave_v5(&bytes).unwrap();
        assert_eq!(parsed, wf);
    }

    #[test]
    fn weave_file_round_trip_minimal() {
        let wf = WeaveFile {
            parents: vec![vec![]],
            sha1s: vec![b"a".to_vec()],
            names: vec![b"v0".to_vec()],
            weave: vec![],
        };
        let bytes = write_weave_v5(&wf);
        assert_eq!(read_weave_v5(&bytes).unwrap(), wf);
    }

    #[test]
    fn weave_file_round_trip_empty_weave_body() {
        // No instructions and no literal lines — just metadata then `w\nW\n`.
        let wf = WeaveFile {
            parents: vec![vec![], vec![0]],
            sha1s: vec![b"x".to_vec(), b"y".to_vec()],
            names: vec![b"a".to_vec(), b"b".to_vec()],
            weave: vec![],
        };
        let bytes = write_weave_v5(&wf);
        assert_eq!(read_weave_v5(&bytes).unwrap(), wf);
    }

    #[test]
    fn walk_internal_unclosed_insertion_errors() {
        let weave = vec![ctl(Instruction::InsertOpen, 0), line(b"x\n")];
        assert_eq!(
            walk_internal(&weave),
            Err(WeaveError::UnclosedInsertions(vec![0]))
        );
    }

    #[test]
    fn inactive_insertion_blocks_lines() {
        let weave = vec![
            ctl(Instruction::InsertOpen, 1),
            line(b"only-in-1\n"),
            ctl(Instruction::InsertClose, 1),
        ];
        let got = extract(&weave, &set(&[0])).unwrap();
        assert!(got.is_empty());
    }

    fn ls(strs: &[&[u8]]) -> Vec<Vec<u8>> {
        strs.iter().map(|s| s.to_vec()).collect()
    }

    /// Mirrors `RepeatedAdd::test_duplicate_add` — adding the same name
    /// twice with matching parents+sha1 returns the same index.
    #[test]
    fn duplicate_add_returns_existing_index() {
        let mut wf = WeaveFile::default();
        let text = ls(&[b"line 1\n", b"line 2\n"]);
        let idx1 = wf.add(Some(b"text0"), &text, &[], None, None).unwrap();
        let idx2 = wf.add(Some(b"text0"), &text, &[], None, None).unwrap();
        assert_eq!(idx1, idx2);
        assert_eq!(wf.parents.len(), 1);
    }

    /// Mirrors `InvalidRepeatedAdd` — same name with different content or
    /// different parents must error.
    #[test]
    fn invalid_repeated_add_errors() {
        let mut wf = WeaveFile::default();
        let text = ls(&[b"line 1\n"]);
        wf.add(Some(b"basis"), &text, &[], None, None).unwrap();
        wf.add(Some(b"text0"), &text, &[], None, None).unwrap();
        // Different content under same name.
        let other = ls(&[b"different\n"]);
        let err = wf.add(Some(b"text0"), &other, &[], None, None).unwrap_err();
        assert_eq!(err, WeaveError::RevisionAlreadyPresent(b"text0".to_vec()));
        // Same content but wrong parents.
        let err = wf.add(Some(b"text0"), &text, &[0], None, None).unwrap_err();
        assert_eq!(err, WeaveError::RevisionAlreadyPresent(b"text0".to_vec()));
    }

    /// Mirrors `InvalidAdd` — referencing a missing parent index errors.
    #[test]
    fn invalid_add_missing_parent_errors() {
        let mut wf = WeaveFile::default();
        let err = wf
            .add(Some(b"text0"), &ls(&[b"new text\n"]), &[69], None, None)
            .unwrap_err();
        assert_eq!(err, WeaveError::RevisionNotPresent(69));
    }

    /// Mirrors `AnnotateOne` — single version annotation reports its own
    /// name as origin for every line.
    #[test]
    fn annotate_one_version() {
        let mut wf = WeaveFile::default();
        let text = ls(&[b"hello\n", b"world\n"]);
        let idx = wf.add(Some(b"text0"), &text, &[], None, None).unwrap();
        let annotated = wf.annotate(idx).unwrap();
        assert_eq!(
            annotated,
            vec![
                (b"text0".to_vec(), b"hello\n".to_vec()),
                (b"text0".to_vec(), b"world\n".to_vec()),
            ]
        );
    }

    /// Mirrors the first half of `InsertLines::runTest` — adding a single
    /// line on top of a parent attributes the new line to the new version
    /// and re-uses the parent's line.
    #[test]
    fn insert_one_line_attribution() {
        let mut wf = WeaveFile::default();
        wf.add(Some(b"text0"), &ls(&[b"line 1\n"]), &[], None, None)
            .unwrap();
        wf.add(
            Some(b"text1"),
            &ls(&[b"line 1\n", b"line 2\n"]),
            &[0],
            None,
            None,
        )
        .unwrap();
        assert_eq!(
            wf.annotate(0).unwrap(),
            vec![(b"text0".to_vec(), b"line 1\n".to_vec())]
        );
        assert_eq!(
            wf.get_lines(1).unwrap(),
            vec![b"line 1\n".to_vec(), b"line 2\n".to_vec()]
        );
        assert_eq!(
            wf.annotate(1).unwrap(),
            vec![
                (b"text0".to_vec(), b"line 1\n".to_vec()),
                (b"text1".to_vec(), b"line 2\n".to_vec()),
            ]
        );
    }

    /// Mirrors the merge half of `InsertLines::runTest` — a 3-way insertion
    /// keeps the parent attributions for shared lines and credits the
    /// new version for the inserted middle line.
    #[test]
    fn insert_lines_merge_attribution() {
        let mut wf = WeaveFile::default();
        wf.add(Some(b"text0"), &ls(&[b"line 1\n"]), &[], None, None)
            .unwrap();
        wf.add(
            Some(b"text1"),
            &ls(&[b"line 1\n", b"line 2\n"]),
            &[0],
            None,
            None,
        )
        .unwrap();
        wf.add(
            Some(b"text3"),
            &ls(&[b"line 1\n", b"middle line\n", b"line 2\n"]),
            &[0, 1],
            None,
            None,
        )
        .unwrap();
        assert_eq!(
            wf.annotate(2).unwrap(),
            vec![
                (b"text0".to_vec(), b"line 1\n".to_vec()),
                (b"text3".to_vec(), b"middle line\n".to_vec()),
                (b"text1".to_vec(), b"line 2\n".to_vec()),
            ]
        );
    }

    /// Mirrors `DeleteLines::runTest` — every derived version round-trips
    /// through `get_lines` after being added with a single parent.
    #[test]
    fn delete_lines_round_trip() {
        let mut wf = WeaveFile::default();
        let base = ls(&[b"one\n", b"two\n", b"three\n", b"four\n"]);
        wf.add(Some(b"text0"), &base, &[], None, None).unwrap();
        let texts: Vec<Vec<Vec<u8>>> = vec![
            ls(&[b"one\n", b"two\n", b"three\n"]),
            ls(&[b"two\n", b"three\n", b"four\n"]),
            ls(&[b"one\n", b"four\n"]),
            ls(&[b"one\n", b"two\n", b"three\n", b"four\n"]),
        ];
        for (i, t) in texts.iter().enumerate() {
            wf.add(
                Some(format!("text{}", i + 1).as_bytes()),
                t,
                &[0],
                None,
                None,
            )
            .unwrap();
        }
        for (i, t) in texts.iter().enumerate() {
            assert_eq!(&wf.get_lines(i + 1).unwrap(), t);
        }
    }

    /// `add` with `nostore_sha` matching the new content errors instead
    /// of inserting.
    #[test]
    fn add_nostore_sha_blocks_storage() {
        let mut wf = WeaveFile::default();
        let text = ls(&[b"line\n"]);
        let sha = sha_strings(&text);
        let err = wf
            .add(Some(b"text0"), &text, &[], None, Some(&sha))
            .unwrap_err();
        assert_eq!(err, WeaveError::ExistingContent);
        assert!(wf.parents.is_empty());
    }

    /// `add` without an explicit name allocates `b"sha1:" + sha1`.
    #[test]
    fn add_anonymous_uses_sha_name() {
        let mut wf = WeaveFile::default();
        let text = ls(&[b"hi\n"]);
        let sha = sha_strings(&text);
        let idx = wf.add(None, &text, &[], None, None).unwrap();
        assert_eq!(wf.names[idx], {
            let mut n = b"sha1:".to_vec();
            n.extend_from_slice(&sha);
            n
        });
    }
}

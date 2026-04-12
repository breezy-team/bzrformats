//! Weave storage core algorithms.
//!
//! Port of the pure-logic core of `bzrformats/weave.py`. A weave is a single
//! flat sequence of [`WeaveEntry`] items: literal lines plus bracketed
//! insertion/deletion instructions. This module implements the annotation
//! walk (`extract`) against that representation. The Python class still
//! owns I/O, parent/name bookkeeping, and the higher-level VersionedFile
//! surface.

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

/// Errors from walking a malformed weave.
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
}

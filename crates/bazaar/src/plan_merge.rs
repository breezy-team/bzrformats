//! Merge plan generation. Mirrors `bzrformats.merge._PlanMergeBase` and
//! `_PlanLCAMerge`.
//!
//! The merge plan is a sequence of `(tag, line)` pairs: each line of each
//! side of the merge is classified as `new-a`/`new-b` (introduced),
//! `killed-a`/`killed-b` (removed), `unchanged` (preserved), or
//! `conflicted-a`/`conflicted-b` (two sides disagree).
//!
//! The legacy Python module exposed three classes:
//!  * `_PlanMergeBase` — base with `get_lines`, `_get_matching_blocks`,
//!    `_unique_lines`, `_iter_plan`, `_subtract_plans`.
//!  * `_PlanMerge` — annotate-based merge (stays in Python; depends on
//!    per-file weave building and annotation walks).
//!  * `_PlanLCAMerge` — LCA-based merge (ported here).
//!
//! This module covers the base bookkeeping plus `_PlanLCAMerge`. The
//! pure-crate types depend only on [`crate::versionedfile::VersionedFiles`]
//! and the patiencediff crate, so callers can drive them with either a
//! native Rust `VersionedFiles` or a pyo3-wrapped Python one.

use crate::knit::KnitError;
use crate::versionedfile::{Key, VersionedFiles};
use std::collections::HashMap;
use std::collections::HashSet;

/// One step of a merge plan: a tag plus the line it applies to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeTag {
    NewA,
    NewB,
    KilledA,
    KilledB,
    Unchanged,
    ConflictedA,
    ConflictedB,
}

impl MergeTag {
    pub fn as_str(&self) -> &'static str {
        match self {
            MergeTag::NewA => "new-a",
            MergeTag::NewB => "new-b",
            MergeTag::KilledA => "killed-a",
            MergeTag::KilledB => "killed-b",
            MergeTag::Unchanged => "unchanged",
            MergeTag::ConflictedA => "conflicted-a",
            MergeTag::ConflictedB => "conflicted-b",
        }
    }

    pub fn from_str(s: &str) -> Option<MergeTag> {
        Some(match s {
            "new-a" => MergeTag::NewA,
            "new-b" => MergeTag::NewB,
            "killed-a" => MergeTag::KilledA,
            "killed-b" => MergeTag::KilledB,
            "unchanged" => MergeTag::Unchanged,
            "conflicted-a" => MergeTag::ConflictedA,
            "conflicted-b" => MergeTag::ConflictedB,
            _ => return None,
        })
    }
}

/// Marker used by Python for "no parent" — the literal byte string `null:`.
pub const NULL_REVISION: &[u8] = b"null:";

fn is_null(rev_id: &[u8]) -> bool {
    rev_id == NULL_REVISION
}

/// Matching block returned by patiencediff: `(i, j, n)` meaning
/// `a[i..i+n] == b[j..j+n]`. The final block is always `(len(a), len(b), 0)`.
pub type MatchingBlock = (usize, usize, usize);

/// Compute matching blocks between two lists of lines using patiencediff.
/// Mirrors `_PlanMergeBase._get_matching_blocks` for the uncached path.
pub fn matching_blocks(left: &[Vec<u8>], right: &[Vec<u8>]) -> Vec<MatchingBlock> {
    let mut sm = patiencediff::SequenceMatcher::new(left, right);
    sm.get_matching_blocks().to_vec()
}

/// Walk `matching_blocks` and partition the line indices into
/// `(unique_left, unique_right)` — the lines that aren't part of any
/// matching block. Mirrors `_PlanMergeBase._unique_lines`.
pub fn unique_lines(blocks: &[MatchingBlock]) -> (Vec<usize>, Vec<usize>) {
    let mut last_i = 0usize;
    let mut last_j = 0usize;
    let mut left = Vec::new();
    let mut right = Vec::new();
    for &(i, j, n) in blocks {
        left.extend(last_i..i);
        right.extend(last_j..j);
        last_i = i + n;
        last_j = j + n;
    }
    (left, right)
}

/// Emit the merge plan from the matching blocks plus the per-side
/// `new`/`killed` line indices. Mirrors `_PlanMergeBase._iter_plan`.
pub fn iter_plan(
    blocks: &[MatchingBlock],
    new_a: &HashSet<usize>,
    killed_b: &HashSet<usize>,
    new_b: &HashSet<usize>,
    killed_a: &HashSet<usize>,
    lines_a: &[Vec<u8>],
    lines_b: &[Vec<u8>],
) -> Vec<(MergeTag, Vec<u8>)> {
    let mut out = Vec::new();
    let mut last_i = 0usize;
    let mut last_j = 0usize;
    for &(i, j, n) in blocks {
        for a_index in last_i..i {
            let tag = if new_a.contains(&a_index) {
                if killed_b.contains(&a_index) {
                    MergeTag::ConflictedA
                } else {
                    MergeTag::NewA
                }
            } else {
                MergeTag::KilledB
            };
            out.push((tag, lines_a[a_index].clone()));
        }
        for b_index in last_j..j {
            let tag = if new_b.contains(&b_index) {
                if killed_a.contains(&b_index) {
                    MergeTag::ConflictedB
                } else {
                    MergeTag::NewB
                }
            } else {
                MergeTag::KilledA
            };
            out.push((tag, lines_b[b_index].clone()));
        }
        for a_index in i..i + n {
            out.push((MergeTag::Unchanged, lines_a[a_index].clone()));
        }
        last_i = i + n;
        last_j = j + n;
    }
    out
}

/// Remove changes from `new_plan` that came from `old_plan`. Mirrors
/// `_PlanMergeBase._subtract_plans`.
///
/// Both inputs are lists of `(tag, line)` pairs; the assumption is that
/// the difference between them is their choice of 'b' text. Lines that
/// match between `old_plan` and `new_plan` and are about the 'b'
/// revision get rewritten (`killed-b` → `unchanged`) or dropped
/// (`new-b`); everything else passes through verbatim.
pub fn subtract_plans(
    old_plan: &[(MergeTag, Vec<u8>)],
    new_plan: &[(MergeTag, Vec<u8>)],
) -> Vec<(MergeTag, Vec<u8>)> {
    // Build the patience-diff lookup over the (tag, line) pairs by hashing
    // their string-encoded form, the way the Python implementation does.
    let old_keys: Vec<(String, Vec<u8>)> = old_plan
        .iter()
        .map(|(t, l)| (t.as_str().to_string(), l.clone()))
        .collect();
    let new_keys: Vec<(String, Vec<u8>)> = new_plan
        .iter()
        .map(|(t, l)| (t.as_str().to_string(), l.clone()))
        .collect();
    let mut sm = patiencediff::SequenceMatcher::new(&old_keys, &new_keys);
    let blocks: Vec<MatchingBlock> = sm.get_matching_blocks().to_vec();
    let mut out = Vec::new();
    let mut last_j = 0usize;
    for (_, j, n) in blocks {
        for jj in last_j..j {
            out.push(new_plan[jj].clone());
        }
        for jj in j..j + n {
            match &new_plan[jj].0 {
                MergeTag::NewB => {
                    // Drop: this line was already on the 'b' side of the
                    // old plan, so it shouldn't appear in the subtracted
                    // result.
                }
                MergeTag::KilledB => {
                    // The line existed in both; mark unchanged.
                    out.push((MergeTag::Unchanged, new_plan[jj].1.clone()));
                }
                _ => out.push(new_plan[jj].clone()),
            }
        }
        last_j = j + n;
    }
    out
}

/// Fetch the fulltext lines for the given revisions. Mirrors
/// `_PlanMergeBase.get_lines`: queries `vf.get_record_stream(keys, ...)`
/// once, returns a `{revision_id_suffix: lines}` map keyed by the *last*
/// segment of each returned key (since callers refer to revisions by
/// bare bytes ids, not full tuple keys).
pub fn get_lines(
    vf: &dyn VersionedFiles,
    key_prefix: &[Vec<u8>],
    revisions: &[Vec<u8>],
) -> Result<HashMap<Vec<u8>, Vec<Vec<u8>>>, KnitError> {
    let keys: Vec<Key> = revisions
        .iter()
        .map(|rev| {
            let mut segs = key_prefix.to_vec();
            segs.push(rev.clone());
            Key::Fixed(segs)
        })
        .collect();
    let stream = vf.get_record_stream(&keys, "unordered", true)?;
    let mut out = HashMap::new();
    for record in stream {
        let record = record?;
        if record.storage_kind() == "absent" {
            return Err(KnitError::RevisionNotPresent(
                record.key().segments().to_vec(),
            ));
        }
        let key = record.key();
        let rev_id = key.version_id().to_vec();
        let lines: Vec<Vec<u8>> = record.to_lines().map(|l| l.into_owned()).collect();
        out.insert(rev_id, lines);
    }
    Ok(out)
}

/// LCA-based merge planner. Mirrors `bzrformats.merge._PlanLCAMerge`.
///
/// `key_prefix` is the prefix that gets prepended to bare revision ids
/// when forming `VersionedFiles` keys (typically `(file_id,)`). `a_rev`
/// and `b_rev` are bare revision ids of the two merge tips. `lcas` is
/// the set of LCAs already computed via vcs-graph; each entry is either
/// the bare bytes `null:` or a bare-bytes revision id (the caller is
/// responsible for stripping the prefix off `vcsgraph::find_lca`'s
/// output).
pub struct PlanLCAMerge<'vf> {
    pub a_rev: Vec<u8>,
    pub b_rev: Vec<u8>,
    pub key_prefix: Vec<Vec<u8>>,
    pub lcas: HashSet<Vec<u8>>,
    pub lines_a: Vec<Vec<u8>>,
    pub lines_b: Vec<Vec<u8>>,
    cached_matching_blocks: HashMap<(Vec<u8>, Vec<u8>), Vec<MatchingBlock>>,
    vf: &'vf dyn VersionedFiles,
}

impl<'vf> PlanLCAMerge<'vf> {
    pub fn new(
        vf: &'vf dyn VersionedFiles,
        a_rev: Vec<u8>,
        b_rev: Vec<u8>,
        key_prefix: Vec<Vec<u8>>,
        lcas: HashSet<Vec<u8>>,
    ) -> Result<Self, KnitError> {
        let tip_lines = get_lines(vf, &key_prefix, &[a_rev.clone(), b_rev.clone()])?;
        let lines_a = tip_lines.get(&a_rev).cloned().unwrap_or_default();
        let lines_b = tip_lines.get(&b_rev).cloned().unwrap_or_default();
        let mut cached_matching_blocks: HashMap<(Vec<u8>, Vec<u8>), Vec<MatchingBlock>> =
            HashMap::new();
        for lca in &lcas {
            let lca_lines = if is_null(lca) {
                Vec::new()
            } else {
                get_lines(vf, &key_prefix, &[lca.clone()])?
                    .remove(lca.as_slice())
                    .unwrap_or_default()
            };
            cached_matching_blocks.insert(
                (a_rev.clone(), lca.clone()),
                matching_blocks(&lines_a, &lca_lines),
            );
            cached_matching_blocks.insert(
                (b_rev.clone(), lca.clone()),
                matching_blocks(&lines_b, &lca_lines),
            );
        }
        Ok(Self {
            a_rev,
            b_rev,
            key_prefix,
            lcas,
            lines_a,
            lines_b,
            cached_matching_blocks,
            vf,
        })
    }

    /// Fetch matching blocks between two revisions, consulting the cache.
    /// Mirrors `_PlanMergeBase._get_matching_blocks`. Falls back to
    /// computing fresh blocks via patiencediff when the cache misses.
    pub fn get_matching_blocks(
        &mut self,
        left: &[u8],
        right: &[u8],
    ) -> Result<Vec<MatchingBlock>, KnitError> {
        if let Some(cached) = self
            .cached_matching_blocks
            .get(&(left.to_vec(), right.to_vec()))
        {
            return Ok(cached.clone());
        }
        let mut need: Vec<Vec<u8>> = Vec::new();
        if left != self.a_rev.as_slice() && left != self.b_rev.as_slice() {
            need.push(left.to_vec());
        }
        if right != self.a_rev.as_slice() && right != self.b_rev.as_slice() {
            need.push(right.to_vec());
        }
        let fetched = if need.is_empty() {
            HashMap::new()
        } else {
            get_lines(self.vf, &self.key_prefix, &need)?
        };
        let left_lines = self.lines_for(left, &fetched);
        let right_lines = self.lines_for(right, &fetched);
        Ok(matching_blocks(&left_lines, &right_lines))
    }

    fn lines_for(&self, rev: &[u8], fetched: &HashMap<Vec<u8>, Vec<Vec<u8>>>) -> Vec<Vec<u8>> {
        if rev == self.a_rev.as_slice() {
            self.lines_a.clone()
        } else if rev == self.b_rev.as_slice() {
            self.lines_b.clone()
        } else if is_null(rev) {
            Vec::new()
        } else {
            fetched.get(rev).cloned().unwrap_or_default()
        }
    }

    /// Determine which lines are `new` versus `killed` relative to the
    /// LCAs. Mirrors `_PlanLCAMerge._determine_status`.
    pub fn determine_status(
        &mut self,
        revision_id: &[u8],
        unique_line_numbers: &HashSet<usize>,
    ) -> Result<(HashSet<usize>, HashSet<usize>), KnitError> {
        let mut new: HashSet<usize> = HashSet::new();
        let mut killed: HashSet<usize> = HashSet::new();
        let lcas: Vec<Vec<u8>> = self.lcas.iter().cloned().collect();
        for lca in &lcas {
            let blocks = self.get_matching_blocks(revision_id, lca)?;
            let (unique_vs_lca, _) = unique_lines(&blocks);
            let unique_vs_lca: HashSet<usize> = unique_vs_lca.into_iter().collect();
            // intersection -> truly new (no LCA had it).
            new.extend(unique_line_numbers.intersection(&unique_vs_lca).copied());
            // difference -> not unique in this LCA, i.e. the LCA had the line.
            killed.extend(unique_line_numbers.difference(&unique_vs_lca).copied());
        }
        Ok((new, killed))
    }

    /// Generate the merge plan. Mirrors `_PlanMergeBase.plan_merge`.
    pub fn plan_merge(&mut self) -> Result<Vec<(MergeTag, Vec<u8>)>, KnitError> {
        let a_rev = self.a_rev.clone();
        let b_rev = self.b_rev.clone();
        let blocks = self.get_matching_blocks(&a_rev, &b_rev)?;
        let (unique_a, unique_b) = unique_lines(&blocks);
        let unique_a_set: HashSet<usize> = unique_a.into_iter().collect();
        let unique_b_set: HashSet<usize> = unique_b.into_iter().collect();
        let (new_a, killed_b) = self.determine_status(&a_rev, &unique_a_set)?;
        let (new_b, killed_a) = self.determine_status(&b_rev, &unique_b_set)?;
        Ok(iter_plan(
            &blocks,
            &new_a,
            &killed_b,
            &new_b,
            &killed_a,
            &self.lines_a,
            &self.lines_b,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(b: &[u8]) -> Vec<u8> {
        b.to_vec()
    }

    #[test]
    fn unique_lines_empty_blocks() {
        let blocks = vec![(0, 0, 0)];
        assert_eq!(
            unique_lines(&blocks),
            (Vec::<usize>::new(), Vec::<usize>::new())
        );
    }

    #[test]
    fn unique_lines_partitions_around_matches() {
        // a = [a b c d], b = [x b c y]; blocks: (1,1,2), (4,4,0)
        let blocks = vec![(1, 1, 2), (4, 4, 0)];
        let (left, right) = unique_lines(&blocks);
        assert_eq!(left, vec![0, 3]);
        assert_eq!(right, vec![0, 3]);
    }

    #[test]
    fn iter_plan_emits_killed_b_for_lines_unique_to_a() {
        let blocks = vec![(1, 0, 2), (3, 2, 0)];
        let lines_a = vec![line(b"a\n"), line(b"b\n"), line(b"c\n")];
        let lines_b = vec![line(b"b\n"), line(b"c\n")];
        let plan = iter_plan(
            &blocks,
            &HashSet::new(),
            &HashSet::new(),
            &HashSet::new(),
            &HashSet::new(),
            &lines_a,
            &lines_b,
        );
        assert_eq!(plan[0].0, MergeTag::KilledB);
        assert_eq!(plan[0].1, b"a\n".to_vec());
    }

    #[test]
    fn subtract_plans_drops_new_b_lines_present_in_old() {
        let old = vec![
            (MergeTag::NewB, line(b"x\n")),
            (MergeTag::Unchanged, line(b"y\n")),
        ];
        let new = vec![
            (MergeTag::NewB, line(b"x\n")),
            (MergeTag::Unchanged, line(b"y\n")),
            (MergeTag::NewA, line(b"z\n")),
        ];
        let out = subtract_plans(&old, &new);
        // The 'new-b x' line is shared with old → dropped. The shared
        // 'unchanged y' line passes through. The fresh 'new-a z' line is
        // unique to new → preserved.
        assert_eq!(
            out,
            vec![
                (MergeTag::Unchanged, line(b"y\n")),
                (MergeTag::NewA, line(b"z\n")),
            ]
        );
    }

    #[test]
    fn subtract_plans_rewrites_killed_b_to_unchanged() {
        let old = vec![(MergeTag::KilledB, line(b"x\n"))];
        let new = vec![(MergeTag::KilledB, line(b"x\n"))];
        let out = subtract_plans(&old, &new);
        assert_eq!(out, vec![(MergeTag::Unchanged, line(b"x\n"))]);
    }
}

//! Merge plan generation. Mirrors `bzrformats.merge`.
//!
//! The merge plan is a sequence of `(tag, line)` pairs: each line of each
//! side of the merge is classified as `new-a`/`new-b` (introduced),
//! `killed-a`/`killed-b` (removed), `unchanged` (preserved),
//! `conflicted-a`/`conflicted-b` (two sides disagree), or one of the weave
//! states (`killed-base`, `killed-both`, `ghost-a`, `ghost-b`,
//! `irrelevant`).
//!
//! The legacy Python module exposed three classes, all ported here:
//!  * `_PlanMergeBase` — base bookkeeping (`get_lines`,
//!    `matching_blocks`, `unique_lines`, `iter_plan`, `subtract_plans`).
//!  * [`PlanMerge`] — annotate-based merge: builds an in-memory weave from
//!    the per-file graph (via `vcs-graph`) and runs `Weave.plan_merge`.
//!  * [`PlanLCAMerge`] — LCA-based merge.
//!
//! The pure-crate types depend only on
//! [`crate::versionedfile::VersionedFiles`], the weave core, the
//! patiencediff crate and `vcs-graph`, so callers can drive them with
//! either a native Rust `VersionedFiles` or a pyo3-wrapped Python one.

use crate::knit::KnitError;
use crate::versionedfile::{Key, VersionedFiles};
use std::collections::HashMap;
use std::collections::HashSet;

/// One step of a merge plan: a tag plus the line it applies to.
///
/// Covers the full vocabulary that `Weave.plan_merge` can emit, since the
/// weave-based `_PlanMerge` path returns those tags verbatim, plus the
/// `conflicted-a`/`conflicted-b` tags the LCA path produces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeTag {
    NewA,
    NewB,
    KilledA,
    KilledB,
    Unchanged,
    ConflictedA,
    ConflictedB,
    KilledBase,
    KilledBoth,
    GhostA,
    GhostB,
    Irrelevant,
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
            MergeTag::KilledBase => "killed-base",
            MergeTag::KilledBoth => "killed-both",
            MergeTag::GhostA => "ghost-a",
            MergeTag::GhostB => "ghost-b",
            MergeTag::Irrelevant => "irrelevant",
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
            "killed-base" => MergeTag::KilledBase,
            "killed-both" => MergeTag::KilledBoth,
            "ghost-a" => MergeTag::GhostA,
            "ghost-b" => MergeTag::GhostB,
            "irrelevant" => MergeTag::Irrelevant,
            _ => return None,
        })
    }
}

impl From<crate::weave::PlanMergeState> for MergeTag {
    fn from(state: crate::weave::PlanMergeState) -> MergeTag {
        use crate::weave::PlanMergeState;
        match state {
            PlanMergeState::KilledBase => MergeTag::KilledBase,
            PlanMergeState::KilledBoth => MergeTag::KilledBoth,
            PlanMergeState::KilledA => MergeTag::KilledA,
            PlanMergeState::KilledB => MergeTag::KilledB,
            PlanMergeState::Unchanged => MergeTag::Unchanged,
            PlanMergeState::NewA => MergeTag::NewA,
            PlanMergeState::NewB => MergeTag::NewB,
            PlanMergeState::GhostA => MergeTag::GhostA,
            PlanMergeState::GhostB => MergeTag::GhostB,
            PlanMergeState::Irrelevant => MergeTag::Irrelevant,
        }
    }
}

/// Marker used by Python for "no parent" — the literal byte string `null:`.
pub const NULL_REVISION: &[u8] = b"null:";

/// Marker used by Python for the synthetic working-tree tip — the literal
/// byte string `current:`. Used as the fake merge-sort tip when building the
/// in-memory weave in [`PlanMerge::build_weave`].
pub const CURRENT_REVISION: &[u8] = b"current:";

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

/// Compute matching blocks between two revisions with no caching. Mirrors
/// `_PlanMergeBase._get_matching_blocks` for the cold-cache path, which is
/// the only path `_PlanMerge` (no tip-line precaching) ever takes from its
/// public `_get_matching_blocks` method.
pub fn matching_blocks_uncached(
    vf: &dyn VersionedFiles,
    key_prefix: &[Vec<u8>],
    left: &[u8],
    right: &[u8],
) -> Result<Vec<MatchingBlock>, KnitError> {
    let mut lines = get_lines(vf, key_prefix, &[left.to_vec(), right.to_vec()])?;
    let left_lines = lines.remove(left).unwrap_or_default();
    let right_lines = lines.remove(right).unwrap_or_default();
    Ok(matching_blocks(&left_lines, &right_lines))
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

/// A [`vcs_graph::ParentsProvider`] over a [`VersionedFiles`]. Mirrors
/// `vcsgraph.graph.Graph(vf)`: parent queries are answered by
/// `vf.get_parent_map`. The backing `VersionedFiles` (in the merge flow this
/// is `_PlanMergeVersionedFile`) is responsible for the NULL substitution
/// that maps parentless keys to `(NULL_REVISION,)`.
struct VfParentsProvider<'a> {
    vf: &'a dyn VersionedFiles,
}

impl vcs_graph::ParentsProvider<Key> for VfParentsProvider<'_> {
    fn get_parent_map(&self, keys: &std::collections::HashSet<Key>) -> vcs_graph::ParentMap<Key> {
        let key_vec: Vec<Key> = keys.iter().cloned().collect();
        let mut out = vcs_graph::ParentMap::new();
        // The trait method is fallible, but `ParentsProvider` is not. A
        // failed lookup here means a backing-store error; mirror Python's
        // behaviour of letting that surface by simply returning what we
        // have (the caller re-raises as RevisionNotPresent later when the
        // text is actually fetched). In practice get_parent_map on the
        // merge VFs does not raise.
        if let Ok(map) = self.vf.get_parent_map(&key_vec) {
            for (k, parents) in map {
                out.insert(k, vcs_graph::Parents::Known(parents));
            }
        }
        out
    }
}

/// The NULL sentinel as a tuple key: `(b"null:",)`. The Python merge VF
/// substitutes parentless keys with `(NULL_REVISION,)`, which round-trips
/// through the trait as this single-segment key.
fn null_key() -> Key {
    Key::Fixed(vec![NULL_REVISION.to_vec()])
}

/// Annotate-based merge planner. Mirrors `bzrformats.merge._PlanMerge`.
///
/// Builds an in-memory weave from the per-file graph between the two tips
/// (back to their recursive LCAs) and runs `Weave.plan_merge` over it.
/// When one tip dominates the other in the per-file graph, short-circuits
/// to tagging every line of the dominating text `new-a`/`new-b`.
pub struct PlanMerge<'vf> {
    pub a_rev: Vec<u8>,
    pub b_rev: Vec<u8>,
    pub key_prefix: Vec<Vec<u8>>,
    a_key: Key,
    b_key: Key,
    head_key: Option<Key>,
    weave: Option<crate::weave::WeaveFile>,
    vf: &'vf dyn VersionedFiles,
}

/// Build a tuple key from a prefix and a bare revision id suffix.
fn make_key(prefix: &[Vec<u8>], suffix: &[u8]) -> Key {
    let mut segs = prefix.to_vec();
    segs.push(suffix.to_vec());
    Key::Fixed(segs)
}

impl<'vf> PlanMerge<'vf> {
    /// Construct the planner, eagerly determining the dominating head (or
    /// building the in-memory weave). Mirrors `_PlanMerge.__init__`.
    pub fn new(
        vf: &'vf dyn VersionedFiles,
        a_rev: Vec<u8>,
        b_rev: Vec<u8>,
        key_prefix: Vec<Vec<u8>>,
    ) -> Result<Self, KnitError> {
        let a_key = make_key(&key_prefix, &a_rev);
        let b_key = make_key(&key_prefix, &b_rev);
        let mut me = PlanMerge {
            a_rev,
            b_rev,
            key_prefix,
            a_key: a_key.clone(),
            b_key: b_key.clone(),
            head_key: None,
            weave: None,
            vf,
        };
        let null = null_key();
        let heads = {
            let provider = VfParentsProvider { vf };
            let graph = vcs_graph::Graph::new(provider);
            graph.heads_with_null([a_key.clone(), b_key.clone()], &null)
        };
        if heads.len() == 1 {
            // One side dominates the other in the per-file graph; we can
            // return its lines directly without comparing texts.
            me.head_key = heads.into_iter().next();
        } else {
            me.build_weave()?;
        }
        Ok(me)
    }

    /// Fetch fulltext lines for `revisions`.
    fn get_lines_map(
        &self,
        revisions: &[Vec<u8>],
    ) -> Result<HashMap<Vec<u8>, Vec<Vec<u8>>>, KnitError> {
        get_lines(self.vf, &self.key_prefix, revisions)
    }

    /// Find all ancestors back to a unique LCA. Mirrors
    /// `_PlanMerge._find_recursive_lcas`. Returns a parent map of tuple keys
    /// (the NULL sentinel never appears as a key in the result).
    fn find_recursive_lcas(&self) -> Result<HashMap<Key, Vec<Key>>, KnitError> {
        let null = null_key();
        let provider = VfParentsProvider { vf: self.vf };
        let graph = vcs_graph::Graph::new(provider);
        let mut cur_ancestors: Vec<Key> = vec![self.a_key.clone(), self.b_key.clone()];
        let mut parent_map: HashMap<Key, Vec<Key>> = HashMap::new();
        loop {
            let mut next_lcas: Vec<Key> = graph
                .find_lca(cur_ancestors.iter().cloned(), &null)
                .into_iter()
                .collect();
            // A plain NULL means "no common ancestor".
            if next_lcas.len() == 1 && next_lcas[0] == null {
                next_lcas.clear();
            }
            // Order each tip's parents by merge order into the tip.
            for rev_key in &cur_ancestors {
                let ordered = graph.find_merge_order(rev_key.clone(), next_lcas.iter().cloned());
                parent_map.insert(rev_key.clone(), ordered);
            }
            match next_lcas.len() {
                0 => break,
                1 => {
                    parent_map.insert(next_lcas[0].clone(), Vec::new());
                    break;
                }
                n if n > 2 => {
                    // More than two LCAs: fall back to grabbing all nodes
                    // between here and the unique LCA.
                    let mut cur_lcas = next_lcas.clone();
                    while cur_lcas.len() > 1 {
                        cur_lcas = graph
                            .find_lca(cur_lcas.iter().cloned(), &null)
                            .into_iter()
                            .collect();
                    }
                    // No common base, or a plain NULL: prefer None, which
                    // doesn't confuse the interesting-texts gathering.
                    let unique_lca = if cur_lcas.is_empty() || cur_lcas[0] == null {
                        None
                    } else {
                        Some(cur_lcas[0].clone())
                    };
                    let extra =
                        self.find_unique_parents(&graph, &next_lcas, unique_lca.as_ref())?;
                    parent_map.extend(extra);
                    break;
                }
                _ => {
                    cur_ancestors = next_lcas;
                }
            }
        }
        Ok(parent_map)
    }

    /// Find ancestors of `tip_keys` that aren't ancestors of `base_key`.
    /// Mirrors `_PlanMerge._find_unique_parents`.
    fn find_unique_parents(
        &self,
        graph: &vcs_graph::Graph<Key, VfParentsProvider<'_>>,
        tip_keys: &[Key],
        base_key: Option<&Key>,
    ) -> Result<HashMap<Key, Vec<Key>>, KnitError> {
        let null = null_key();
        let mut parent_map: HashMap<Key, Vec<Key>> = HashMap::new();
        match base_key {
            None => {
                for (k, parents) in graph.iter_ancestry(tip_keys.iter().cloned()) {
                    if k == null {
                        continue;
                    }
                    let ps = match parents {
                        vcs_graph::Parents::Known(ps) => ps,
                        vcs_graph::Parents::Ghost => Vec::new(),
                    };
                    parent_map.insert(k, ps);
                }
            }
            Some(base_key) => {
                let mut interesting: HashSet<Key> = HashSet::new();
                for tip in tip_keys {
                    interesting
                        .extend(graph.find_unique_ancestors(tip.clone(), [base_key.clone()]));
                }
                let pm = graph.get_parent_map(interesting.iter().cloned());
                for (k, parents) in pm.iter() {
                    let ps = match parents {
                        vcs_graph::Parents::Known(ps) => ps.clone(),
                        vcs_graph::Parents::Ghost => Vec::new(),
                    };
                    parent_map.insert(k.clone(), ps);
                }
                parent_map.insert(base_key.clone(), Vec::new());
            }
        }
        let (mut culled, mut child_map, mut tails) = remove_external_references(&parent_map);
        // Remove all tails but base_key.
        if let Some(base_key) = base_key {
            tails.retain(|t| t != base_key);
            prune_tails(&mut culled, &mut child_map, tails);
        }
        // Collapse uninteresting linear regions.
        let mut pm = vcs_graph::ParentMap::new();
        for (k, parents) in &culled {
            pm.insert(k.clone(), vcs_graph::Parents::Known(parents.clone()));
        }
        let collapsed = vcs_graph::collapse_linear_regions(&pm);
        let mut out: HashMap<Key, Vec<Key>> = HashMap::new();
        for (k, parents) in collapsed.iter() {
            let ps = match parents {
                vcs_graph::Parents::Known(ps) => ps.clone(),
                vcs_graph::Parents::Ghost => Vec::new(),
            };
            out.insert(k.clone(), ps);
        }
        Ok(out)
    }

    /// Build the in-memory weave from the recursive-LCA parent map. Mirrors
    /// `_PlanMerge._build_weave`.
    fn build_weave(&mut self) -> Result<(), KnitError> {
        let mut parent_map = self.find_recursive_lcas()?;
        // Gather the texts we need: every key in the parent map plus the
        // two tips.
        let mut all_revision_keys: HashSet<Key> = parent_map.keys().cloned().collect();
        all_revision_keys.insert(self.a_key.clone());
        all_revision_keys.insert(self.b_key.clone());
        let revision_ids: Vec<Vec<u8>> = all_revision_keys
            .iter()
            .map(|k| k.version_id().to_vec())
            .collect();
        let all_texts = self.get_lines_map(&revision_ids)?;

        // Add a synthetic tip so left-hand parents insert before right-hand
        // ones, then merge_sort and add in reverse order.
        let tip_key = make_key(&self.key_prefix, CURRENT_REVISION);
        parent_map.insert(
            tip_key.clone(),
            vec![self.a_key.clone(), self.b_key.clone()],
        );

        let graph_map: HashMap<Key, Vec<Key>> = parent_map.clone();
        let sorted = vcs_graph::tsort::merge_sort(graph_map, Some(tip_key.clone()), None, false)
            .map_err(|e| KnitError::Corrupt(format!("merge_sort failed: {:?}", e)))?;

        let mut weave = crate::weave::WeaveFile::default();
        for (_seq, key, _depth, _revno, _eom) in sorted.into_iter().rev() {
            if key == tip_key {
                continue;
            }
            let parent_keys = parent_map.get(&key).cloned().unwrap_or_default();
            let revision_id = key.version_id().to_vec();
            let parent_ids: Vec<Vec<u8>> = parent_keys
                .iter()
                .map(|k| k.version_id().to_vec())
                .collect();
            let parent_refs: Vec<&[u8]> = parent_ids.iter().map(|p| p.as_slice()).collect();
            let lines = all_texts.get(&revision_id).cloned().unwrap_or_default();
            weave
                .add_lines(&revision_id, &parent_refs, &lines, None, None)
                .map_err(weave_err_to_knit)?;
        }
        self.weave = Some(weave);
        Ok(())
    }

    /// Generate the merge plan. Mirrors `_PlanMerge.plan_merge`.
    pub fn plan_merge(&mut self) -> Result<Vec<(MergeTag, Vec<u8>)>, KnitError> {
        if let Some(head_key) = self.head_key.clone() {
            let (tag, head_rev) = if head_key == self.a_key {
                (MergeTag::NewA, self.a_rev.clone())
            } else {
                if head_key != self.b_key {
                    return Err(KnitError::Corrupt(format!(
                        "There was an invalid head: {:?} != {:?}",
                        self.b_key, head_key
                    )));
                }
                (MergeTag::NewB, self.b_rev.clone())
            };
            let lines = self
                .get_lines_map(std::slice::from_ref(&head_rev))?
                .remove(&head_rev)
                .unwrap_or_default();
            return Ok(lines.into_iter().map(|l| (tag.clone(), l)).collect());
        }
        let weave = self
            .weave
            .as_ref()
            .expect("weave built when no dominating head");
        let plan = weave
            .plan_merge(&self.a_rev, &self.b_rev)
            .map_err(weave_err_to_knit)?;
        Ok(plan
            .into_iter()
            .map(|(state, line)| (MergeTag::from(state), line))
            .collect())
    }
}

/// Map a `WeaveError` to a `KnitError` so `PlanMerge` can carry a single
/// error type. Missing texts become `RevisionNotPresent`.
fn weave_err_to_knit(err: crate::weave::WeaveError) -> KnitError {
    use crate::weave::WeaveError;
    match err {
        WeaveError::RevisionNotPresentByName(name) => KnitError::RevisionNotPresent(vec![name]),
        WeaveError::RevisionNotPresent(idx) => {
            KnitError::Corrupt(format!("weave revision index not present: {}", idx))
        }
        other => KnitError::Corrupt(format!("weave error: {:?}", other)),
    }
}

/// Result of [`remove_external_references`]: the culled parent map, the
/// `{parent: [children]}` child map, and the list of tail nodes.
pub type CulledGraph<K> = (HashMap<K, Vec<K>>, HashMap<K, Vec<K>>, Vec<K>);

/// Remove references that point outside `parent_map`. Mirrors
/// `_PlanMerge._remove_external_references`. Generic over the key type so it
/// can be unit-tested with plain keys.
///
/// Returns `(filtered_parent_map, child_map, tails)`:
/// * `filtered_parent_map` is `parent_map` with external parents dropped,
/// * `child_map` is `{parent: [children]}`,
/// * `tails` are nodes with no parents inside the map.
///
/// Insertion order of `parent_map` is preserved by walking the keys in their
/// stored order; callers that need deterministic ordering should pass an
/// ordered map. The pyo3 binding implements the Python-visible version
/// directly over `dict` to keep arbitrary key types.
pub fn remove_external_references<K: Clone + Eq + std::hash::Hash>(
    parent_map: &HashMap<K, Vec<K>>,
) -> CulledGraph<K> {
    let mut filtered: HashMap<K, Vec<K>> = HashMap::new();
    let mut child_map: HashMap<K, Vec<K>> = HashMap::new();
    let mut tails: Vec<K> = Vec::new();
    for (key, parent_keys) in parent_map {
        let culled: Vec<K> = parent_keys
            .iter()
            .filter(|p| parent_map.contains_key(*p))
            .cloned()
            .collect();
        if culled.is_empty() {
            tails.push(key.clone());
        }
        for parent_key in &culled {
            child_map
                .entry(parent_key.clone())
                .or_default()
                .push(key.clone());
        }
        child_map.entry(key.clone()).or_default();
        filtered.insert(key.clone(), culled);
    }
    (filtered, child_map, tails)
}

/// Remove tails from the parent map until no more children have zero parents.
/// Mirrors `_PlanMerge._prune_tails`. Mutates `parent_map` and `child_map` in
/// place. Generic over the key type.
pub fn prune_tails<K: Clone + Eq + std::hash::Hash>(
    parent_map: &mut HashMap<K, Vec<K>>,
    child_map: &mut HashMap<K, Vec<K>>,
    mut tails_to_remove: Vec<K>,
) {
    while let Some(next) = tails_to_remove.pop() {
        parent_map.remove(&next);
        let children = child_map.remove(&next).unwrap_or_default();
        for child in children {
            if let Some(child_parents) = parent_map.get_mut(&child) {
                if let Some(pos) = child_parents.iter().position(|p| *p == next) {
                    child_parents.remove(pos);
                }
                if child_parents.is_empty() {
                    tails_to_remove.push(child);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::versionedfile::VirtualVersionedFiles;
    use std::sync::Arc;

    fn line(b: &[u8]) -> Vec<u8> {
        b.to_vec()
    }

    /// Split a byte string into one `"x\n"` line per byte (the Python merge
    /// tests build revision texts the same way).
    fn char_lines(s: &[u8]) -> Vec<Vec<u8>> {
        s.iter().map(|b| vec![*b, b'\n']).collect()
    }

    /// Build a VirtualVersionedFiles over an in-memory `{rev: (parents, text)}`
    /// graph, keyed by single-byte revision ids (empty key prefix).
    #[allow(clippy::type_complexity)]
    fn make_vf(
        revs: Vec<(&[u8], Vec<&[u8]>, &[u8])>,
    ) -> VirtualVersionedFiles<
        impl Fn(&[Vec<u8>]) -> Result<HashMap<Vec<u8>, Vec<Vec<u8>>>, KnitError> + Send + Sync,
        impl Fn(&[u8]) -> Result<Option<Vec<Vec<u8>>>, KnitError> + Send + Sync,
    > {
        let mut parents: HashMap<Vec<u8>, Vec<Vec<u8>>> = HashMap::new();
        let mut lines: HashMap<Vec<u8>, Vec<Vec<u8>>> = HashMap::new();
        for (rev, ps, text) in revs {
            parents.insert(rev.to_vec(), ps.iter().map(|p| p.to_vec()).collect());
            lines.insert(rev.to_vec(), char_lines(text));
        }
        let parents = Arc::new(parents);
        let lines = Arc::new(lines);
        let pclone = parents.clone();
        VirtualVersionedFiles::new(
            move |keys: &[Vec<u8>]| {
                Ok(keys
                    .iter()
                    .filter_map(|k| pclone.get(k).map(|p| (k.clone(), p.clone())))
                    .collect())
            },
            move |key: &[u8]| Ok(lines.get(key).cloned()),
        )
    }

    fn plan_strings(plan: Vec<(MergeTag, Vec<u8>)>) -> Vec<(String, Vec<u8>)> {
        plan.into_iter()
            .map(|(tag, l)| (tag.as_str().to_string(), l))
            .collect()
    }

    #[test]
    fn plan_merge_three_way() {
        // Port of test_merge.test_plan_merge: A=abc, B(A)=acehg, C(A)=fabg.
        let vf = make_vf(vec![
            (b"A", vec![], b"abc"),
            (b"B", vec![b"A"], b"acehg"),
            (b"C", vec![b"A"], b"fabg"),
        ]);
        let mut pm = PlanMerge::new(&vf, b"B".to_vec(), b"C".to_vec(), vec![]).unwrap();
        let plan = plan_strings(pm.plan_merge().unwrap());
        assert_eq!(
            plan,
            vec![
                ("new-b".to_string(), b"f\n".to_vec()),
                ("unchanged".to_string(), b"a\n".to_vec()),
                ("killed-a".to_string(), b"b\n".to_vec()),
                ("killed-b".to_string(), b"c\n".to_vec()),
                ("new-a".to_string(), b"e\n".to_vec()),
                ("new-a".to_string(), b"h\n".to_vec()),
                ("new-a".to_string(), b"g\n".to_vec()),
                ("new-b".to_string(), b"g\n".to_vec()),
            ]
        );
    }

    #[test]
    fn plan_merge_no_common_ancestor() {
        // Two disjoint roots: every line is tagged new-a or new-b.
        let vf = make_vf(vec![(b"A", vec![], b"ab"), (b"B", vec![], b"cd")]);
        let mut pm = PlanMerge::new(&vf, b"A".to_vec(), b"B".to_vec(), vec![]).unwrap();
        let plan = plan_strings(pm.plan_merge().unwrap());
        // All of A's lines are new-a and all of B's lines are new-b.
        assert!(plan.iter().all(|(t, _)| t == "new-a" || t == "new-b"));
        assert_eq!(plan.iter().filter(|(t, _)| t == "new-a").count(), 2,);
        assert_eq!(plan.iter().filter(|(t, _)| t == "new-b").count(), 2,);
    }

    #[test]
    fn plan_merge_dominating_head_shortcuts() {
        // B descends from A: B dominates, so merging A and B returns B's lines
        // tagged new-b without a weave comparison.
        let vf = make_vf(vec![(b"A", vec![], b"ab"), (b"B", vec![b"A"], b"abc")]);
        let mut pm = PlanMerge::new(&vf, b"A".to_vec(), b"B".to_vec(), vec![]).unwrap();
        let plan = plan_strings(pm.plan_merge().unwrap());
        assert_eq!(
            plan,
            vec![
                ("new-b".to_string(), b"a\n".to_vec()),
                ("new-b".to_string(), b"b\n".to_vec()),
                ("new-b".to_string(), b"c\n".to_vec()),
            ]
        );
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

    fn pm(pairs: &[(i32, &[i32])]) -> HashMap<i32, Vec<i32>> {
        pairs.iter().map(|(k, ps)| (*k, ps.to_vec())).collect()
    }

    fn assert_remove_external_references(
        expected_filtered: &[(i32, &[i32])],
        expected_child: &[(i32, &[i32])],
        expected_tails: &[i32],
        input: &[(i32, &[i32])],
    ) {
        let (filtered, mut child_map, mut tails) = remove_external_references(&pm(input));
        assert_eq!(filtered, pm(expected_filtered));
        // The child lists' ordering is not strictly defined (the Rust core
        // walks the parent map in HashMap order); compare them as sets.
        for children in child_map.values_mut() {
            children.sort_unstable();
        }
        let mut want_child = pm(expected_child);
        for children in want_child.values_mut() {
            children.sort_unstable();
        }
        assert_eq!(child_map, want_child);
        tails.sort_unstable();
        let mut want = expected_tails.to_vec();
        want.sort_unstable();
        assert_eq!(tails, want);
    }

    #[test]
    fn remove_external_references_cases() {
        // Nothing to remove.
        assert_remove_external_references(
            &[(3, &[2]), (2, &[1]), (1, &[])],
            &[(1, &[2]), (2, &[3]), (3, &[])],
            &[1],
            &[(3, &[2]), (2, &[1]), (1, &[])],
        );
        // The reverse direction.
        assert_remove_external_references(
            &[(1, &[2]), (2, &[3]), (3, &[])],
            &[(3, &[2]), (2, &[1]), (1, &[])],
            &[3],
            &[(1, &[2]), (2, &[3]), (3, &[])],
        );
        // Extra (external) references get dropped.
        assert_remove_external_references(
            &[(3, &[2]), (2, &[1]), (1, &[])],
            &[(1, &[2]), (2, &[3]), (3, &[])],
            &[1],
            &[(3, &[2, 4]), (2, &[1, 5]), (1, &[6])],
        );
        // Multiple tails.
        assert_remove_external_references(
            &[(4, &[2, 3]), (3, &[]), (2, &[1]), (1, &[])],
            &[(1, &[2]), (2, &[4]), (3, &[4]), (4, &[])],
            &[1, 3],
            &[(4, &[2, 3]), (3, &[5]), (2, &[1]), (1, &[6])],
        );
        // Multiple children.
        assert_remove_external_references(
            &[(1, &[3]), (2, &[3, 4]), (3, &[]), (4, &[])],
            &[(1, &[]), (2, &[]), (3, &[1, 2]), (4, &[2])],
            &[3, 4],
            &[(1, &[3]), (2, &[3, 4]), (3, &[5]), (4, &[])],
        );
    }

    fn assert_prune_tails(expected: &[(i32, &[i32])], tails: &[i32], input: &[(i32, &[i32])]) {
        let mut parent_map = pm(input);
        // Build child_map the same way the test helper does in Python.
        let mut child_map: HashMap<i32, Vec<i32>> = HashMap::new();
        for (key, parent_keys) in &parent_map {
            child_map.entry(*key).or_default();
            for pkey in parent_keys {
                child_map.entry(*pkey).or_default().push(*key);
            }
        }
        prune_tails(&mut parent_map, &mut child_map, tails.to_vec());
        assert_eq!(parent_map, pm(expected));
    }

    #[test]
    fn prune_tails_cases() {
        // Nothing requested to prune.
        assert_prune_tails(
            &[(1, &[]), (2, &[]), (3, &[])],
            &[],
            &[(1, &[]), (2, &[]), (3, &[])],
        );
        // Prune a single entry.
        assert_prune_tails(&[(1, &[]), (3, &[])], &[2], &[(1, &[]), (2, &[]), (3, &[])]);
        // Prune a chain.
        assert_prune_tails(&[(1, &[])], &[3], &[(1, &[]), (2, &[3]), (3, &[])]);
        // Prune a chain with a diamond.
        assert_prune_tails(
            &[(1, &[])],
            &[5],
            &[(1, &[]), (2, &[3, 4]), (3, &[5]), (4, &[5]), (5, &[])],
        );
        // Prune a partial chain.
        assert_prune_tails(
            &[(1, &[6]), (6, &[])],
            &[5],
            &[
                (1, &[2, 6]),
                (2, &[3, 4]),
                (3, &[5]),
                (4, &[5]),
                (5, &[]),
                (6, &[]),
            ],
        );
        // Prune a chain with multiple tips, pulling out intermediates.
        assert_prune_tails(
            &[(1, &[3]), (3, &[])],
            &[4, 5],
            &[(1, &[2, 3]), (2, &[4, 5]), (3, &[]), (4, &[]), (5, &[])],
        );
        assert_prune_tails(
            &[(1, &[3]), (3, &[])],
            &[5, 4],
            &[(1, &[2, 3]), (2, &[4, 5]), (3, &[]), (4, &[]), (5, &[])],
        );
    }
}

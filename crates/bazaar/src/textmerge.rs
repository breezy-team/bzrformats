//! Text merge functionality.
//!
//! Port of `bzrformats/textmerge.py`. Provides structured two-way merge using
//! the patiencediff algorithm. Each merge yields a sequence of `Group`s; an
//! `Unchanged` group is a region common to both inputs, while a `Conflict`
//! group holds the diverging lines from each side.

use patiencediff::SequenceMatcher;

pub const A_MARKER: &[u8] = b"<<<<<<< \n";
pub const B_MARKER: &[u8] = b">>>>>>> \n";
pub const SPLIT_MARKER: &[u8] = b"=======\n";

/// One region of a structured merge result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Group {
    Unchanged(Vec<Vec<u8>>),
    Conflict { a: Vec<Vec<u8>>, b: Vec<Vec<u8>> },
}

impl Group {
    pub fn is_conflict(&self) -> bool {
        matches!(self, Group::Conflict { .. })
    }

    fn is_useful(&self) -> bool {
        match self {
            Group::Unchanged(lines) => !lines.is_empty(),
            Group::Conflict { a, b } => !a.is_empty() || !b.is_empty(),
        }
    }
}

/// Two-way merge of `lines_a` and `lines_b`.
///
/// Common regions are reported as [`Group::Unchanged`]; diverging regions as
/// [`Group::Conflict`].
pub fn merge2(lines_a: &[Vec<u8>], lines_b: &[Vec<u8>]) -> Vec<Group> {
    let mut sm = SequenceMatcher::new(lines_a, lines_b);
    let mut out = Vec::new();
    let mut pos_a = 0;
    let mut pos_b = 0;
    for &(ai, bi, l) in sm.get_matching_blocks() {
        let group = Group::Conflict {
            a: lines_a[pos_a..ai].to_vec(),
            b: lines_b[pos_b..bi].to_vec(),
        };
        if group.is_useful() {
            out.push(group);
        }
        let unchanged = Group::Unchanged(lines_a[ai..ai + l].to_vec());
        if unchanged.is_useful() {
            out.push(unchanged);
        }
        pos_a = ai + l;
        pos_b = bi + l;
    }
    out
}

/// Re-run a two-way merge over the conflicted regions of an existing merge,
/// shrinking each conflict region to its minimal diverging core.
///
/// This may split one conflict into several smaller ones but never introduces
/// new conflicts.
pub fn reprocess_struct(struct_iter: impl IntoIterator<Item = Group>) -> Vec<Group> {
    let mut out = Vec::new();
    for group in struct_iter {
        match group {
            Group::Unchanged(_) => out.push(group),
            Group::Conflict { a, b } => {
                for sub in merge2(&a, &b) {
                    out.push(sub);
                }
            }
        }
    }
    out
}

/// Render a structured merge result to a flat line stream, inserting conflict
/// markers around [`Group::Conflict`] regions.
///
/// Returns `(lines, had_conflicts)`.
pub fn struct_to_lines(
    groups: &[Group],
    a_marker: &[u8],
    b_marker: &[u8],
    split_marker: &[u8],
) -> (Vec<Vec<u8>>, bool) {
    let mut lines = Vec::new();
    let mut conflicts = false;
    for group in groups {
        match group {
            Group::Unchanged(g) => lines.extend(g.iter().cloned()),
            Group::Conflict { a, b } => {
                conflicts = true;
                lines.push(a_marker.to_vec());
                lines.extend(a.iter().cloned());
                lines.push(split_marker.to_vec());
                lines.extend(b.iter().cloned());
                lines.push(b_marker.to_vec());
            }
        }
    }
    (lines, conflicts)
}

/// Plan-merge state, mirroring `bzrformats.versionedfile` plan strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanState {
    Unchanged,
    KilledA,
    KilledB,
    NewA,
    NewB,
    ConflictedA,
    ConflictedB,
    KilledBoth,
    Irrelevant,
    GhostA,
    GhostB,
    KilledBase,
}

impl PlanState {
    pub fn from_str(s: &str) -> Option<PlanState> {
        Some(match s {
            "unchanged" => PlanState::Unchanged,
            "killed-a" => PlanState::KilledA,
            "killed-b" => PlanState::KilledB,
            "new-a" => PlanState::NewA,
            "new-b" => PlanState::NewB,
            "conflicted-a" => PlanState::ConflictedA,
            "conflicted-b" => PlanState::ConflictedB,
            "killed-both" => PlanState::KilledBoth,
            "irrelevant" => PlanState::Irrelevant,
            "ghost-a" => PlanState::GhostA,
            "ghost-b" => PlanState::GhostB,
            "killed-base" => PlanState::KilledBase,
            _ => return None,
        })
    }
}

/// One emitted region from `merge_struct_from_plan`. Lines are referenced by
/// their index in the original plan so callers can preserve the source line
/// objects byte-for-byte.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanGroup {
    /// Single resolved chunk: emit these line indices unchanged.
    Single(Vec<usize>),
    /// Conflict region: side-A vs side-B line indices.
    Conflict { a: Vec<usize>, b: Vec<usize> },
}

/// Translate a weave merge plan to structured merge groups.
///
/// `states` is the per-line plan state; `lines` carries the corresponding
/// line bytes (used for content equality when collapsing same-line "fake"
/// conflicts) and to suppress single-line `unchanged` groups for empty lines.
/// Output groups carry indices back into the original plan so callers can
/// preserve byte identity for the lines.
#[allow(unused_assignments)]
pub fn merge_struct_from_plan<L: AsRef<[u8]>>(states: &[PlanState], lines: &[L]) -> Vec<PlanGroup> {
    assert_eq!(states.len(), lines.len());
    let mut out = Vec::new();
    let mut lines_a: Vec<usize> = Vec::new();
    let mut lines_b: Vec<usize> = Vec::new();
    let mut ch_a = false;
    let mut ch_b = false;

    let line_bytes = |i: usize| lines[i].as_ref();
    let same_content = |a: &[usize], b: &[usize]| -> bool {
        a.len() == b.len()
            && a.iter()
                .zip(b.iter())
                .all(|(&i, &j)| line_bytes(i) == line_bytes(j))
    };

    macro_rules! flush {
        () => {
            if lines_a.is_empty() && lines_b.is_empty() {
                // nothing
            } else if ch_a && !ch_b {
                out.push(PlanGroup::Single(std::mem::take(&mut lines_a)));
            } else if ch_b && !ch_a {
                out.push(PlanGroup::Single(std::mem::take(&mut lines_b)));
            } else if same_content(&lines_a, &lines_b) {
                out.push(PlanGroup::Single(std::mem::take(&mut lines_a)));
            } else {
                out.push(PlanGroup::Conflict {
                    a: std::mem::take(&mut lines_a),
                    b: std::mem::take(&mut lines_b),
                });
            }
            lines_a.clear();
            lines_b.clear();
            ch_a = false;
            ch_b = false;
        };
    }

    for (idx, state) in states.iter().enumerate() {
        if *state == PlanState::Unchanged {
            flush!();
            if !line_bytes(idx).is_empty() {
                out.push(PlanGroup::Single(vec![idx]));
            }
            continue;
        }
        match state {
            PlanState::KilledA => {
                ch_a = true;
                lines_b.push(idx);
            }
            PlanState::KilledB => {
                ch_b = true;
                lines_a.push(idx);
            }
            PlanState::NewA => {
                ch_a = true;
                lines_a.push(idx);
            }
            PlanState::NewB => {
                ch_b = true;
                lines_b.push(idx);
            }
            PlanState::ConflictedA => {
                ch_a = true;
                ch_b = true;
                lines_a.push(idx);
            }
            PlanState::ConflictedB => {
                ch_a = true;
                ch_b = true;
                lines_b.push(idx);
            }
            PlanState::KilledBoth => {
                ch_a = true;
                ch_b = true;
            }
            PlanState::Irrelevant
            | PlanState::GhostA
            | PlanState::GhostB
            | PlanState::KilledBase => {}
            PlanState::Unchanged => unreachable!(),
        }
    }
    flush!();
    out
}

/// Reconstruct a BASE text from a weave merge plan.
///
/// Returns the indices (into `states`) of the lines that belong to BASE:
/// `unchanged`, `killed-a`, `killed-b` and `killed-both` states.
pub fn base_indices_from_plan(states: &[PlanState]) -> Vec<usize> {
    let mut out = Vec::new();
    for (idx, state) in states.iter().enumerate() {
        match state {
            PlanState::Unchanged
            | PlanState::KilledA
            | PlanState::KilledB
            | PlanState::KilledBoth => out.push(idx),
            _ => {}
        }
    }
    out
}

/// Convenience: produce merged lines plus a conflict flag from two inputs.
pub fn merge_lines(
    lines_a: &[Vec<u8>],
    lines_b: &[Vec<u8>],
    reprocess: bool,
    a_marker: &[u8],
    b_marker: &[u8],
    split_marker: &[u8],
) -> (Vec<Vec<u8>>, bool) {
    let mut groups = merge2(lines_a, lines_b);
    if reprocess {
        groups = reprocess_struct(groups);
    }
    struct_to_lines(&groups, a_marker, b_marker, split_marker)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines(s: &str) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let mut current = Vec::new();
        for &b in s.as_bytes() {
            current.push(b);
            if b == b'\n' {
                out.push(std::mem::take(&mut current));
            }
        }
        if !current.is_empty() {
            out.push(current);
        }
        out
    }

    #[test]
    fn agreed() {
        let l = lines("a\nb\nc\nd\ne\nf\n");
        let (merged, conflicts) = merge_lines(&l, &l, false, A_MARKER, B_MARKER, SPLIT_MARKER);
        assert_eq!(merged, l);
        assert!(!conflicts);
    }

    #[test]
    fn conflict() {
        let a = lines("a\nb\nc\nd\ne\nf\ng\nh\n");
        let b = lines("z\nb\nx\nd\ne\ne\nf\ng\ny\n");
        let expected = "<\na\n=\nz\n>\nb\n<\nc\n=\nx\n>\nd\ne\n<\n=\ne\n>\nf\ng\n<\nh\n=\ny\n>\n";
        let (merged, conflicts) = merge_lines(&a, &b, false, b"<\n", b">\n", b"=\n");
        let joined: Vec<u8> = merged.into_iter().flatten().collect();
        assert_eq!(joined, expected.as_bytes());
        assert!(conflicts);

        let (merged_rp, conflicts_rp) = merge_lines(&a, &b, true, b"<\n", b">\n", b"=\n");
        let joined_rp: Vec<u8> = merged_rp.into_iter().flatten().collect();
        assert_eq!(joined_rp, expected.as_bytes());
        assert!(conflicts_rp);
    }

    #[test]
    fn plan_merge_unchanged_runs() {
        let states = vec![
            PlanState::Unchanged,
            PlanState::Unchanged,
            PlanState::Unchanged,
        ];
        let lines: Vec<&[u8]> = vec![b"a", b"b", b"c"];
        let groups = merge_struct_from_plan(&states, &lines);
        assert_eq!(
            groups,
            vec![
                PlanGroup::Single(vec![0]),
                PlanGroup::Single(vec![1]),
                PlanGroup::Single(vec![2]),
            ]
        );
    }

    #[test]
    fn plan_merge_killed_a_then_unchanged() {
        // killed-a sets ch_a but pushes to lines_b. The Python original
        // yields the empty (lines_a,) chunk on flush; downstream iter_useful
        // discards it.
        let states = vec![PlanState::KilledA, PlanState::Unchanged];
        let lines: Vec<&[u8]> = vec![b"x", b"y"];
        let groups = merge_struct_from_plan(&states, &lines);
        assert_eq!(
            groups,
            vec![PlanGroup::Single(vec![]), PlanGroup::Single(vec![1])]
        );
    }

    #[test]
    fn plan_merge_new_a_then_unchanged() {
        // new-a is the symmetric case that does carry the line through.
        let states = vec![PlanState::NewA, PlanState::Unchanged];
        let lines: Vec<&[u8]> = vec![b"x", b"y"];
        let groups = merge_struct_from_plan(&states, &lines);
        assert_eq!(
            groups,
            vec![PlanGroup::Single(vec![0]), PlanGroup::Single(vec![1])]
        );
    }

    #[test]
    fn plan_merge_two_sided_conflict() {
        // new-a then new-b without intervening unchanged -> conflict
        let states = vec![PlanState::NewA, PlanState::NewB];
        let lines: Vec<&[u8]> = vec![b"x", b"y"];
        let groups = merge_struct_from_plan(&states, &lines);
        assert_eq!(
            groups,
            vec![PlanGroup::Conflict {
                a: vec![0],
                b: vec![1]
            }]
        );
    }

    #[test]
    fn plan_merge_same_line_on_both_sides_collapses() {
        // new-a and new-b inserting the *same* content collapse to a single
        // chunk, not a conflict — content equality, not index equality.
        let states = vec![PlanState::NewA, PlanState::NewB];
        let lines: Vec<&[u8]> = vec![b"xxx\n", b"xxx\n"];
        let groups = merge_struct_from_plan(&states, &lines);
        assert_eq!(groups, vec![PlanGroup::Single(vec![0])]);
    }

    #[test]
    fn plan_merge_killed_both_is_a_change() {
        // killed-both with no surviving lines -> drops nothing
        let states = vec![
            PlanState::Unchanged,
            PlanState::KilledBoth,
            PlanState::Unchanged,
        ];
        let lines: Vec<&[u8]> = vec![b"a", b"b", b"c"];
        let groups = merge_struct_from_plan(&states, &lines);
        assert_eq!(
            groups,
            vec![PlanGroup::Single(vec![0]), PlanGroup::Single(vec![2]),]
        );
    }

    #[test]
    fn plan_merge_skips_empty_unchanged_line() {
        let states = vec![PlanState::Unchanged, PlanState::Unchanged];
        let lines: Vec<&[u8]> = vec![b"", b"x"];
        let groups = merge_struct_from_plan(&states, &lines);
        assert_eq!(groups, vec![PlanGroup::Single(vec![1])]);
    }

    #[test]
    fn base_indices_only_includes_base_states() {
        let states = vec![
            PlanState::Unchanged,
            PlanState::KilledA,
            PlanState::NewA,
            PlanState::KilledB,
            PlanState::ConflictedA,
            PlanState::KilledBoth,
            PlanState::GhostA,
        ];
        assert_eq!(base_indices_from_plan(&states), vec![0, 1, 3, 5]);
    }

    #[test]
    fn reprocess_splits_conflicts() {
        let input = vec![
            Group::Conflict {
                a: vec![b"a".to_vec()],
                b: vec![b"b".to_vec()],
            },
            Group::Unchanged(vec![b"c".to_vec()]),
            Group::Conflict {
                a: vec![b"d".to_vec(), b"e".to_vec(), b"f".to_vec()],
                b: vec![b"g".to_vec(), b"e".to_vec(), b"h".to_vec()],
            },
            Group::Unchanged(vec![b"i".to_vec()]),
        ];
        let expected = vec![
            Group::Conflict {
                a: vec![b"a".to_vec()],
                b: vec![b"b".to_vec()],
            },
            Group::Unchanged(vec![b"c".to_vec()]),
            Group::Conflict {
                a: vec![b"d".to_vec()],
                b: vec![b"g".to_vec()],
            },
            Group::Unchanged(vec![b"e".to_vec()]),
            Group::Conflict {
                a: vec![b"f".to_vec()],
                b: vec![b"h".to_vec()],
            },
            Group::Unchanged(vec![b"i".to_vec()]),
        ];
        assert_eq!(reprocess_struct(input), expected);
    }
}

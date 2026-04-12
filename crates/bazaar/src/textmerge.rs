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

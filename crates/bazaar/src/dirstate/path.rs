//! Path ordering and bisection helpers shared between the bisect
//! routines and the dirblock sort.
//!
//! Python's dirstate treats a path as a sequence of components
//! (split on `/`), with all entries in a directory preceding any
//! entries in its subdirectories. The `lt_by_dirs` and
//! `lt_path_by_dirblock` functions expose that ordering, and
//! `bisect_path_{left,right}` mirror the `bisect` module's usual
//! behaviour under the dirblock ordering.

use std::cmp::Ordering;
use std::path::Path;

pub fn lt_by_dirs(path1: &Path, path2: &Path) -> bool {
    let path1_parts = path1.components();
    let path2_parts = path2.components();
    let mut path1_parts_iter = path1_parts;
    let mut path2_parts_iter = path2_parts;

    loop {
        match (path1_parts_iter.next(), path2_parts_iter.next()) {
            (None, None) => return false,
            (None, Some(_)) => return true,
            (Some(_), None) => return false,
            (Some(part1), Some(part2)) => match part1.cmp(&part2) {
                Ordering::Equal => continue,
                Ordering::Less => return true,
                Ordering::Greater => return false,
            },
        }
    }
}

pub fn lt_path_by_dirblock(path1: &Path, path2: &Path) -> bool {
    let key1 = (path1.parent(), path1.file_name());
    let key2 = (path2.parent(), path2.file_name());

    key1 < key2
}

pub fn bisect_path_left(paths: &[&Path], path: &Path) -> usize {
    let mut hi = paths.len();
    let mut lo = 0;
    while lo < hi {
        let mid = (lo + hi) / 2;
        let cur = paths[mid];
        if lt_path_by_dirblock(cur, path) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

pub fn bisect_path_right(paths: &[&Path], path: &Path) -> usize {
    let mut hi = paths.len();
    let mut lo = 0;
    while lo < hi {
        let mid = (lo + hi) / 2;
        let cur = paths[mid];
        if lt_path_by_dirblock(path, cur) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    lo
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> &Path {
        Path::new(s)
    }

    /// Python's assertCmpByDirs(expected, a, b) with expected in {-1, 0, 1}.
    fn assert_cmp(expected: i32, a: &str, b: &str) {
        let (pa, pb) = (p(a), p(b));
        match expected {
            0 => {
                assert_eq!(a, b);
                assert!(!lt_by_dirs(pa, pb));
                assert!(!lt_by_dirs(pb, pa));
            }
            v if v > 0 => {
                assert!(!lt_by_dirs(pa, pb));
                assert!(lt_by_dirs(pb, pa));
            }
            _ => {
                assert!(lt_by_dirs(pa, pb));
                assert!(!lt_by_dirs(pb, pa));
            }
        }
    }

    #[test]
    fn lt_by_dirs_cmp_empty() {
        assert_cmp(0, "", "");
        assert_cmp(1, "a", "");
        assert_cmp(1, "abcdef", "");
        assert_cmp(1, "test/ing/a/path/", "");
    }

    #[test]
    fn lt_by_dirs_cmp_same_str() {
        for s in ["a", "ab", "abc", "a/b", "a/b/c/d/e"] {
            assert_cmp(0, s, s);
        }
    }

    #[test]
    fn lt_by_dirs_simple_paths() {
        assert_cmp(-1, "a", "b");
        assert_cmp(-1, "aa", "ab");
        assert_cmp(-1, "ab", "bb");
        assert_cmp(-1, "a/a", "a/b");
        assert_cmp(-1, "a/b", "b/b");
        assert_cmp(-1, "a/a/a", "a/a/b");
    }

    #[test]
    fn lt_by_dirs_tricky_paths() {
        assert_cmp(1, "ab/cd/ef", "ab/cc/ef");
        assert_cmp(1, "ab/cd/ef", "ab/c/ef");
        assert_cmp(-1, "ab/cd/ef", "ab/cd-ef");
        assert_cmp(-1, "ab/cd", "ab/cd-");
        assert_cmp(-1, "ab/cd", "ab-cd");
    }

    #[test]
    fn lt_by_dirs_non_ascii() {
        assert_cmp(-1, "\u{b5}", "\u{e5}");
        assert_cmp(-1, "a", "\u{e5}");
        assert_cmp(-1, "b", "\u{b5}");
        assert_cmp(-1, "a/b", "a/\u{e5}");
        assert_cmp(-1, "b/a", "b/\u{b5}");
    }

    #[test]
    fn lt_path_by_dirblock_simple_sorted_list() {
        let paths: Vec<&Path> = vec![p(""), p("a"), p("ab"), p("abc"), p("a/b/c"), p("b/d/e")];
        for (i, a) in paths.iter().enumerate() {
            for (j, b) in paths.iter().enumerate() {
                assert_eq!(
                    lt_path_by_dirblock(a, b),
                    i < j,
                    "lt_path_by_dirblock({:?}, {:?}) mismatched i={} j={}",
                    a,
                    b,
                    i,
                    j,
                );
            }
        }
    }

    #[test]
    fn bisect_path_left_simple_list() {
        let paths: Vec<&Path> = vec![p(""), p("a"), p("b"), p("c"), p("d")];
        for (i, path) in paths.iter().enumerate() {
            assert_eq!(bisect_path_left(&paths, path), i);
        }
        assert_eq!(bisect_path_left(&paths, p("_")), 1);
        assert_eq!(bisect_path_left(&paths, p("aa")), 2);
        assert_eq!(bisect_path_left(&paths, p("bb")), 3);
        assert_eq!(bisect_path_left(&paths, p("dd")), 5);
    }

    #[test]
    fn bisect_path_right_after_equal_entry() {
        let paths: Vec<&Path> = vec![p(""), p("a"), p("b"), p("c"), p("d")];
        for (i, path) in paths.iter().enumerate() {
            assert_eq!(bisect_path_right(&paths, path), i + 1);
        }
    }
}

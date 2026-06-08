//! Pack maintenance for pack repositories: the `pack()` / `autopack()`
//! operations and the pack-distribution arithmetic that decides when
//! autopack should fire.
//!
//! The arithmetic is a direct port of breezy's `RepositoryPackCollection`
//! (`breezy/bzr/pack_repo.py`): `max_pack_count`, `pack_distribution` and
//! `plan_autopack_combinations`. These are pure functions over a repository's
//! pack list (each pack summarised by its revision count), kept separate from
//! the I/O of `pack()` so they can be unit-tested in isolation.

/// The largest number of packs a repository with `total_revisions` revisions
/// is allowed before autopack repacks the excess.
///
/// breezy's rule (`_max_pack_count`): one pack for an empty repository,
/// otherwise the sum of the decimal digits of the revision count (so 1234
/// allows 1+2+3+4 = 10 packs).
pub fn max_pack_count(total_revisions: u64) -> usize {
    if total_revisions == 0 {
        return 1;
    }
    total_revisions
        .to_string()
        .bytes()
        .map(|b| (b - b'0') as usize)
        .sum()
}

/// The target distribution of pack sizes for `total_revisions` revisions, as a
/// list of revision-count buckets, largest first.
///
/// breezy's `pack_distribution`: read the decimal digits least-significant
/// first; a digit `d` at place value `10^e` contributes `d` buckets of size
/// `10^e`. So 1234 -> `[1000, 100, 100, 10, 10, 10, 1, 1, 1, 1]`. An empty
/// repository yields `[0]`.
pub fn pack_distribution(total_revisions: u64) -> Vec<u64> {
    if total_revisions == 0 {
        return vec![0];
    }
    let digits = total_revisions.to_string();
    let mut buckets = Vec::new();
    // Iterate digits least-significant first, tracking the place value.
    for (i, ch) in digits.bytes().rev().enumerate() {
        let count = (ch - b'0') as usize;
        let value = 10u64.pow(i as u32);
        for _ in 0..count {
            buckets.push(value);
        }
    }
    buckets.reverse();
    buckets
}

/// Decide which packs to combine, given each pack's revision count.
///
/// Returns the list of pack indices (into `pack_revision_counts`) to repack
/// into a single new pack, or an empty list when nothing should be done.
///
/// Mirrors breezy's `plan_autopack_combinations`: if the repository already has
/// no more packs than the target distribution has buckets, there is nothing to
/// do. Otherwise the packs are considered largest first; a pack big enough to
/// fill the next distribution bucket on its own is left alone, and the smaller
/// packs are gathered until the remaining buckets are accounted for. Everything
/// gathered is flattened into one combine operation. A plan that would only
/// move a single pack is suppressed (repacking one pack into one pack is
/// pointless).
pub fn plan_autopack_combinations(pack_revision_counts: &[u64]) -> Vec<usize> {
    let distribution = {
        let total: u64 = pack_revision_counts.iter().sum();
        pack_distribution(total)
    };
    if pack_revision_counts.len() <= distribution.len() {
        return Vec::new();
    }

    // Sort pack indices by revision count, largest first. (Ties keep input
    // order, which is irrelevant since every selected pack ends up in one
    // combined operation.)
    let mut order: Vec<usize> = (0..pack_revision_counts.len()).collect();
    order.sort_by(|&a, &b| pack_revision_counts[b].cmp(&pack_revision_counts[a]));

    // Port of breezy's loop, which mutates a working copy of the distribution.
    let mut dist: std::collections::VecDeque<i64> =
        distribution.iter().map(|&v| v as i64).collect();
    let mut selected = Vec::new();
    let mut pending_op_revs: i64 = 0;
    for &pack in &order {
        let mut rev_count = pack_revision_counts[pack] as i64;
        if dist.front().is_some_and(|&head| rev_count >= head) {
            // Already packed better than this bucket: consume buckets equal to
            // its size, shrinking a partially-filled final bucket.
            while rev_count > 0 {
                let head = *dist.front().expect("distribution exhausted");
                rev_count -= head;
                if rev_count >= 0 {
                    dist.pop_front();
                } else {
                    *dist.front_mut().unwrap() = -rev_count;
                }
            }
        } else {
            // Add this pack to the current output operation.
            pending_op_revs += rev_count;
            selected.push(pack);
            if dist.front().is_some_and(|&head| pending_op_revs >= head) {
                dist.pop_front();
                pending_op_revs = 0;
            }
        }
    }

    // Repacking a single pack into a single pack achieves nothing.
    if selected.len() < 2 {
        return Vec::new();
    }
    selected
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_pack_count_is_digit_sum() {
        assert_eq!(max_pack_count(0), 1);
        assert_eq!(max_pack_count(1), 1);
        assert_eq!(max_pack_count(9), 9);
        assert_eq!(max_pack_count(10), 1);
        assert_eq!(max_pack_count(1234), 10);
        assert_eq!(max_pack_count(1000000), 1);
    }

    #[test]
    fn pack_distribution_powers_of_ten() {
        assert_eq!(pack_distribution(0), vec![0]);
        assert_eq!(pack_distribution(1), vec![1]);
        assert_eq!(pack_distribution(9), vec![1, 1, 1, 1, 1, 1, 1, 1, 1]);
        assert_eq!(pack_distribution(10), vec![10]);
        assert_eq!(
            pack_distribution(1234),
            vec![1000, 100, 100, 10, 10, 10, 1, 1, 1, 1]
        );
    }

    #[test]
    fn plan_does_nothing_when_within_distribution() {
        // 1000 revisions in 1 pack: distribution is [1000], 1 pack <= 1 bucket.
        assert!(plan_autopack_combinations(&[1000]).is_empty());
        // Two packs summing to 11 revisions: distribution(11) = [10, 1], len 2,
        // 2 packs <= 2 buckets -> nothing to do.
        assert!(plan_autopack_combinations(&[10, 1]).is_empty());
    }

    #[test]
    fn plan_combines_many_small_packs() {
        // Five single-revision packs: total 5, distribution(5) = [1,1,1,1,1]
        // (5 buckets), 5 packs <= 5 buckets -> nothing.
        assert!(plan_autopack_combinations(&[1, 1, 1, 1, 1]).is_empty());
        // Six single-revision packs: total 6, distribution = six 1-buckets (6),
        // 6 <= 6 -> still nothing.
        assert!(plan_autopack_combinations(&[1, 1, 1, 1, 1, 1]).is_empty());
        // Eleven single-revision packs: total 11, distribution(11) = [10, 1]
        // (2 buckets); 11 packs > 2 -> combine. The first bucket (10) gathers
        // ten 1-packs; the eleventh exactly fills the trailing 1-bucket on its
        // own, so it is left alone (matching breezy: 10 packs selected).
        let plan = plan_autopack_combinations(&[1; 11]);
        assert_eq!(plan.len(), 10);
    }

    #[test]
    fn plan_leaves_a_large_pack_alone() {
        // One big pack (1000 revs) plus three tiny ones: total 1003,
        // distribution(1003) = [1000, 1, 1, 1] (4 buckets); 4 packs <= 4 -> no
        // repack.
        assert!(plan_autopack_combinations(&[1000, 1, 1, 1]).is_empty());
        // Add a fifth tiny pack: total 1004, distribution = [1000,1,1,1,1]
        // (5 buckets); 5 packs <= 5 -> still nothing.
        assert!(plan_autopack_combinations(&[1000, 1, 1, 1, 1]).is_empty());
    }
}

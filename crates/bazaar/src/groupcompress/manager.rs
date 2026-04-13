//! Pure-logic heuristics from `_LazyGroupContentManager`.
//!
//! These functions decide whether a groupcompress block needs repacking and
//! whether it is "well utilized" enough to leave alone. The corresponding
//! Python lives in `bzrformats.groupcompress._LazyGroupContentManager`.

/// Result of [`check_rebuild_action`]: what to do with the block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebuildAction {
    /// The block is dense enough to keep as-is.
    Keep,
    /// The referenced bytes are packed at the front, just trim the tail.
    Trim,
    /// The referenced bytes are scattered, rebuild the block from scratch.
    Rebuild,
}

/// Decide whether a block should be repacked given the byte ranges actually
/// referenced by its factories and the total uncompressed content length.
///
/// Returns `(action, last_byte_used, total_bytes_used)`. Mirrors Python's
/// `_LazyGroupContentManager._check_rebuild_action`.
pub fn check_rebuild_action(
    factories: &[(usize, usize)],
    content_length: usize,
) -> (RebuildAction, usize, usize) {
    let mut total_bytes_used = 0;
    let mut last_byte_used = 0;
    for &(start, end) in factories {
        total_bytes_used += end - start;
        if last_byte_used < end {
            last_byte_used = end;
        }
    }
    if total_bytes_used * 2 >= content_length {
        return (RebuildAction::Keep, last_byte_used, total_bytes_used);
    }
    if total_bytes_used * 2 > last_byte_used {
        return (RebuildAction::Trim, last_byte_used, total_bytes_used);
    }
    (RebuildAction::Rebuild, last_byte_used, total_bytes_used)
}

/// Tunables for [`check_is_well_utilized`].
///
/// These mirror the class attributes on Python's `_LazyGroupContentManager`.
#[derive(Debug, Clone, Copy)]
pub struct WellUtilizedSettings {
    /// `_max_cut_fraction`: the smallest acceptable used-fraction of the block.
    pub max_cut_fraction: f64,
    /// `_full_enough_block_size`: blocks at or above this size are considered
    /// full regardless of content mix.
    pub full_enough_block_size: usize,
    /// `_full_enough_mixed_block_size`: blocks with mixed file-id content are
    /// considered full at this smaller threshold.
    pub full_enough_mixed_block_size: usize,
}

impl Default for WellUtilizedSettings {
    fn default() -> Self {
        Self {
            max_cut_fraction: 0.75,
            full_enough_block_size: 3 * 1024 * 1024,
            full_enough_mixed_block_size: 2 * 768 * 1024,
        }
    }
}

/// Decide whether a block is "well utilized" enough to leave intact during
/// pack-on-the-fly. Mirrors Python's `_LazyGroupContentManager.check_is_well_utilized`.
///
/// `factories` provides the `(start, end)` byte range and the file-id prefix
/// (everything but the last segment of the key tuple) for each record.
pub fn check_is_well_utilized<P: PartialEq>(
    factories: &[((usize, usize), P)],
    content_length: usize,
    settings: &WellUtilizedSettings,
) -> bool {
    if factories.len() == 1 {
        // A block of length 1 could always be improved by combining with
        // adjacent groups; the Python heuristic refuses to leave it alone.
        return false;
    }
    let positions: Vec<(usize, usize)> = factories.iter().map(|(p, _)| *p).collect();
    let (_action, _last, total_bytes_used) = check_rebuild_action(&positions, content_length);
    if (total_bytes_used as f64) < (content_length as f64) * settings.max_cut_fraction {
        return false;
    }
    if content_length >= settings.full_enough_block_size {
        return true;
    }
    // Mixed-prefix content gets a lower threshold.
    let mut common_prefix: Option<&P> = None;
    for (_, prefix) in factories {
        match common_prefix {
            None => common_prefix = Some(prefix),
            Some(cp) if cp != prefix => {
                return content_length >= settings.full_enough_mixed_block_size;
            }
            _ => {}
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keep_when_more_than_half_is_used() {
        let (action, last, total) = check_rebuild_action(&[(0, 60)], 100);
        assert_eq!(action, RebuildAction::Keep);
        assert_eq!(last, 60);
        assert_eq!(total, 60);
    }

    #[test]
    fn trim_when_used_bytes_are_at_the_front() {
        // 30 of 100 used, all at the front (last_byte = 30, total*2 > last).
        let (action, last, total) = check_rebuild_action(&[(0, 30)], 100);
        assert_eq!(action, RebuildAction::Trim);
        assert_eq!(last, 30);
        assert_eq!(total, 30);
    }

    #[test]
    fn rebuild_when_used_bytes_are_scattered() {
        // 10 of 100 used right at the end → not at the front.
        let (action, last, total) = check_rebuild_action(&[(90, 100)], 100);
        assert_eq!(action, RebuildAction::Rebuild);
        assert_eq!(last, 100);
        assert_eq!(total, 10);
    }

    #[test]
    fn keep_at_exactly_half() {
        // Exactly half: total*2 == content_length triggers Keep.
        let (action, _, _) = check_rebuild_action(&[(0, 50)], 100);
        assert_eq!(action, RebuildAction::Keep);
    }

    fn pos(start: usize, end: usize) -> ((usize, usize), &'static [u8]) {
        ((start, end), b"file-id".as_slice())
    }

    #[test]
    fn well_utilized_single_factory_is_never_well_utilized() {
        let factories = vec![pos(0, 100)];
        assert!(!check_is_well_utilized(
            &factories,
            100,
            &WellUtilizedSettings::default()
        ));
    }

    #[test]
    fn well_utilized_below_max_cut_fraction_is_not_well_utilized() {
        // 50% used, default cutoff 75% → not well utilized.
        let factories = vec![pos(0, 25), pos(25, 50)];
        assert!(!check_is_well_utilized(
            &factories,
            100,
            &WellUtilizedSettings::default()
        ));
    }

    #[test]
    fn well_utilized_full_enough_block_is_well_utilized() {
        // Block size is at the full_enough threshold; content fully used.
        let size = WellUtilizedSettings::default().full_enough_block_size;
        let factories = vec![pos(0, size / 2), pos(size / 2, size)];
        assert!(check_is_well_utilized(
            &factories,
            size,
            &WellUtilizedSettings::default()
        ));
    }

    #[test]
    fn well_utilized_mixed_content_uses_lower_threshold() {
        let settings = WellUtilizedSettings::default();
        let size = settings.full_enough_mixed_block_size;
        // Two factories with different file-id prefixes.
        let factories: Vec<((usize, usize), &[u8])> =
            vec![((0, size / 2), b"file-a"), ((size / 2, size), b"file-b")];
        assert!(check_is_well_utilized(&factories, size, &settings));
    }

    #[test]
    fn well_utilized_same_prefix_below_full_enough_is_not_well_utilized() {
        // Just under the single-prefix `full_enough` threshold: even though the
        // block is fully used, we expect the heuristic to return false because
        // it's still below the full size and not mixed.
        let settings = WellUtilizedSettings::default();
        let size = settings.full_enough_mixed_block_size; // < full_enough_block_size
        let factories = vec![pos(0, size / 2), pos(size / 2, size)];
        assert!(!check_is_well_utilized(&factories, size, &settings));
    }
}

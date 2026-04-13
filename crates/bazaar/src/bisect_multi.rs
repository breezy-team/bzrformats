//! Bisection lookup for multiple keys over a byte-addressable content.
//!
//! Port of `bzrformats.bisect_multi.bisect_multi_bytes`. The algorithm is an
//! amortised binary search that walks every key in parallel: each round halves
//! a shared `delta` and advances or retreats each still-pending key by that
//! much. The callback decides, per (location, key) pair, whether the key lies
//! earlier, later, is absent, or has been located. Found keys are emitted as
//! `(key, value)` tuples; absent keys are silently dropped.

/// Outcome of looking at a single `(location, key)` probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BisectStatus<V> {
    /// Key is earlier in the content; retreat by `delta`.
    Earlier,
    /// Key is later in the content; advance by `delta`.
    Later,
    /// Key is not present; drop it from the search.
    Absent,
    /// Key has been located; yield this value to the caller.
    Found(V),
}

/// Perform parallel bisection lookups.
///
/// `content_lookup` receives the current round's probes and must return one
/// `BisectStatus` per probe in the same order. The first probe for every key
/// is at `size / 2`; subsequent rounds halve the step until it hits 1 and
/// then stay there.
pub fn bisect_multi_bytes<K, V, F>(mut content_lookup: F, size: usize, keys: Vec<K>) -> Vec<(K, V)>
where
    F: FnMut(Vec<(usize, K)>) -> Vec<((usize, K), BisectStatus<V>)>,
{
    let mut result = Vec::new();
    let mut delta = size / 2;
    let mut search_keys: Vec<(usize, K)> = keys.into_iter().map(|k| (delta, k)).collect();
    while !search_keys.is_empty() {
        let search_results = content_lookup(std::mem::take(&mut search_keys));
        if delta > 1 {
            delta /= 2;
        }
        for ((location, key), status) in search_results {
            match status {
                BisectStatus::Earlier => {
                    search_keys.push((location.saturating_sub(delta), key));
                }
                BisectStatus::Later => {
                    search_keys.push((location + delta, key));
                }
                BisectStatus::Absent => {}
                BisectStatus::Found(v) => {
                    result.push((key, v));
                }
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    fn run<V, F>(
        size: usize,
        keys: &[&'static str],
        mut inner: F,
    ) -> (Vec<(&'static str, V)>, Vec<Vec<(usize, &'static str)>>)
    where
        V: Clone,
        F: FnMut((usize, &'static str)) -> BisectStatus<V>,
    {
        let calls: RefCell<Vec<Vec<(usize, &'static str)>>> = RefCell::new(Vec::new());
        let results = bisect_multi_bytes(
            |probes| {
                calls.borrow_mut().push(probes.clone());
                probes
                    .into_iter()
                    .map(|(loc, key)| {
                        let status = inner((loc, key));
                        ((loc, key), status)
                    })
                    .collect()
            },
            size,
            keys.to_vec(),
        );
        (results, calls.into_inner())
    }

    #[test]
    fn lookup_no_keys_no_calls() {
        let (results, calls) = run::<(), _>(100, &[], |_| BisectStatus::Absent);
        assert!(results.is_empty());
        assert!(calls.is_empty());
    }

    #[test]
    fn lookup_missing_key_no_content() {
        let (results, calls) = run::<(), _>(0, &["foo", "bar"], |_| BisectStatus::Absent);
        assert!(results.is_empty());
        assert_eq!(calls, vec![vec![(0, "foo"), (0, "bar")]]);
    }

    #[test]
    fn lookup_missing_key_before_all_others_zero_length() {
        let (results, calls) = run::<(), _>(0, &["foo", "bar"], |(loc, _)| {
            if loc == 0 {
                BisectStatus::Absent
            } else {
                BisectStatus::Earlier
            }
        });
        assert!(results.is_empty());
        assert_eq!(calls, vec![vec![(0, "foo"), (0, "bar")]]);
    }

    #[test]
    fn lookup_missing_key_before_all_others_length_2() {
        let (results, calls) = run::<(), _>(2, &["foo", "bar"], |(loc, _)| {
            if loc == 0 {
                BisectStatus::Absent
            } else {
                BisectStatus::Earlier
            }
        });
        assert!(results.is_empty());
        assert_eq!(
            calls,
            vec![vec![(1, "foo"), (1, "bar")], vec![(0, "foo"), (0, "bar")],]
        );
    }

    #[test]
    fn lookup_missing_key_before_all_others_big() {
        // Mirrors the 200MB-ish test from Python.
        let size = 268_435_456 - 1;
        let (results, calls) = run::<(), _>(size, &["foo", "bar"], |(loc, _)| {
            if loc == 0 {
                BisectStatus::Absent
            } else {
                BisectStatus::Earlier
            }
        });
        assert!(results.is_empty());
        let expected_offsets: &[usize] = &[
            134_217_727,
            67_108_864,
            33_554_433,
            16_777_218,
            8_388_611,
            4_194_308,
            2_097_157,
            1_048_582,
            524_295,
            262_152,
            131_081,
            65_546,
            32_779,
            16_396,
            8_205,
            4_110,
            2_063,
            1_040,
            529,
            274,
            147,
            84,
            53,
            38,
            31,
            28,
            27,
            26,
            25,
            24,
            23,
            22,
            21,
            20,
            19,
            18,
            17,
            16,
            15,
            14,
            13,
            12,
            11,
            10,
            9,
            8,
            7,
            6,
            5,
            4,
            3,
            2,
            1,
            0,
        ];
        let expected: Vec<Vec<(usize, &'static str)>> = expected_offsets
            .iter()
            .map(|&o| vec![(o, "foo"), (o, "bar")])
            .collect();
        assert_eq!(calls, expected);
    }

    #[test]
    fn lookup_missing_key_after_all_others_zero_length() {
        let (results, calls) = run::<(), _>(0, &["foo", "bar"], |(loc, _)| {
            if loc == 0 {
                BisectStatus::Absent
            } else {
                BisectStatus::Later
            }
        });
        assert!(results.is_empty());
        assert_eq!(calls, vec![vec![(0, "foo"), (0, "bar")]]);
    }

    #[test]
    fn lookup_missing_key_after_all_others_length_3() {
        let end = 2usize;
        let (results, calls) = run::<(), _>(3, &["foo", "bar"], |(loc, _)| {
            if loc == end {
                BisectStatus::Absent
            } else {
                BisectStatus::Later
            }
        });
        assert!(results.is_empty());
        assert_eq!(
            calls,
            vec![vec![(1, "foo"), (1, "bar")], vec![(2, "foo"), (2, "bar")],]
        );
    }

    #[test]
    fn lookup_missing_key_when_a_key_is_missing_continues() {
        let (results, calls) = run::<(), _>(2, &["foo", "bar"], |(loc, key)| {
            if key == "foo" || loc == 0 {
                BisectStatus::Absent
            } else {
                BisectStatus::Earlier
            }
        });
        assert!(results.is_empty());
        assert_eq!(calls, vec![vec![(1, "foo"), (1, "bar")], vec![(0, "bar")],]);
    }

    #[test]
    fn found_keys_returned_other_searches_continue() {
        let (results, calls) = run::<&'static str, _>(4, &["foo", "bar"], |(loc, key)| {
            if (loc, key) == (1, "bar") {
                BisectStatus::Found("bar-result")
            } else if loc == 0 {
                BisectStatus::Absent
            } else {
                BisectStatus::Earlier
            }
        });
        assert_eq!(results, vec![("bar", "bar-result")]);
        assert_eq!(
            calls,
            vec![
                vec![(2, "foo"), (2, "bar")],
                vec![(1, "foo"), (1, "bar")],
                vec![(0, "foo")],
            ]
        );
    }

    #[test]
    fn searches_different_keys_in_different_directions() {
        let (results, calls) = run::<(), _>(4, &["foo", "bar"], |(loc, key)| {
            if key == "bar" {
                if loc == 1 {
                    BisectStatus::Absent
                } else {
                    BisectStatus::Earlier
                }
            } else if loc == 3 {
                BisectStatus::Absent
            } else {
                BisectStatus::Later
            }
        });
        assert!(results.is_empty());
        assert_eq!(
            calls,
            vec![vec![(2, "foo"), (2, "bar")], vec![(3, "foo"), (1, "bar")],]
        );
    }

    #[test]
    fn change_direction_in_single_key_search() {
        let (results, calls) = run::<(), _>(8, &["foo", "bar"], |(loc, _)| {
            if loc == 5 {
                BisectStatus::Absent
            } else if loc > 5 {
                BisectStatus::Earlier
            } else {
                BisectStatus::Later
            }
        });
        assert!(results.is_empty());
        assert_eq!(
            calls,
            vec![
                vec![(4, "foo"), (4, "bar")],
                vec![(6, "foo"), (6, "bar")],
                vec![(5, "foo"), (5, "bar")],
            ]
        );
    }
}

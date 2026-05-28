//! Pack repository helpers.
//!
//! The Python `bzrformats.pack_repo` module is mostly orchestration over
//! Python objects (transports, graph indices, container writers, config
//! stacks). This module carries the pieces of it that are pure data and
//! algorithm, so the pyo3 layer can stay a thin shell:
//!
//! - [`index_definition`] / [`IndexKind`] — the `(extension, offset)`
//!   table that maps an index type to its on-disk suffix and its slot in
//!   the `index_sizes` array.
//! - [`index_name`] — compute the disk file name of an index for a given
//!   pack name.
//! - [`group_retrieval_requests`] — the same-index grouping that
//!   `_DirectPackAccess.get_raw_records` does before issuing reads.
//! - [`split_raw_records`] — the offset slicing that
//!   `_DirectPackAccess.add_raw_records` does to chop one concatenated
//!   buffer into per-record byte ranges.
//! - [`reload_decision`] / [`ReloadDecision`] — the branch in
//!   `_DirectPackAccess.reload_or_raise` that decides whether a reload
//!   recovered the situation or the original error must be re-raised.

/// The five index kinds a pack carries. Each maps to a file extension and
/// a fixed position in the `index_sizes` array, matching the Python
/// `Pack.index_definitions` dict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    Chk,
    Revision,
    Inventory,
    Text,
    Signature,
}

impl IndexKind {
    /// Parse the lowercase type name used by the Python API
    /// (`"chk"`, `"revision"`, `"inventory"`, `"text"`, `"signature"`).
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "chk" => Some(IndexKind::Chk),
            "revision" => Some(IndexKind::Revision),
            "inventory" => Some(IndexKind::Inventory),
            "text" => Some(IndexKind::Text),
            "signature" => Some(IndexKind::Signature),
            _ => None,
        }
    }

    /// The lowercase type name, the inverse of [`IndexKind::from_name`].
    pub fn as_name(self) -> &'static str {
        match self {
            IndexKind::Chk => "chk",
            IndexKind::Revision => "revision",
            IndexKind::Inventory => "inventory",
            IndexKind::Text => "text",
            IndexKind::Signature => "signature",
        }
    }
}

/// The `(file extension, index_sizes offset)` pair for an index kind,
/// matching one row of the Python `Pack.index_definitions` dict.
pub fn index_definition(kind: IndexKind) -> (&'static str, usize) {
    match kind {
        IndexKind::Chk => (".cix", 4),
        IndexKind::Revision => (".rix", 0),
        IndexKind::Inventory => (".iix", 1),
        IndexKind::Text => (".tix", 2),
        IndexKind::Signature => (".six", 3),
    }
}

/// The file extension for an index kind (e.g. `".rix"` for revisions).
pub fn index_extension(kind: IndexKind) -> &'static str {
    index_definition(kind).0
}

/// The position of an index kind in the `index_sizes` array.
pub fn index_offset(kind: IndexKind) -> usize {
    index_definition(kind).1
}

/// The disk file name of an index: the pack `name` followed by the
/// kind's extension. Mirrors `Pack.index_name`.
pub fn index_name(kind: IndexKind, name: &str) -> String {
    format!("{}{}", name, index_extension(kind))
}

/// One contiguous group of `(offset, length)` reads against a single
/// index, as produced by grouping a flat retrieval request by index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrievalGroup<I> {
    /// The index every read in this group targets.
    pub index: I,
    /// The `(offset, length)` reads, in request order.
    pub reads: Vec<(u64, u64)>,
}

/// Group a flat list of `(index, offset, length)` retrieval memos into
/// runs of consecutive same-index requests.
///
/// This reproduces the first pass of `_DirectPackAccess.get_raw_records`:
/// it does *not* sort or deduplicate, it only coalesces neighbours that
/// share an index, so that one readv can serve a run. A request that
/// returns to an earlier index after visiting another starts a fresh
/// group.
pub fn group_retrieval_requests<I, T>(memos: T) -> Vec<RetrievalGroup<I>>
where
    I: PartialEq,
    T: IntoIterator<Item = (I, u64, u64)>,
{
    let mut groups: Vec<RetrievalGroup<I>> = Vec::new();
    for (index, offset, length) in memos {
        match groups.last_mut() {
            Some(last) if last.index == index => last.reads.push((offset, length)),
            _ => groups.push(RetrievalGroup {
                index,
                reads: vec![(offset, length)],
            }),
        }
    }
    groups
}

/// The byte range `[start, start + length)` of one record inside a
/// concatenated buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawRecordSlice {
    pub start: usize,
    pub length: usize,
}

/// Slice a concatenated raw-data buffer into per-record ranges given the
/// record sizes, mirroring the offset walk in
/// `_DirectPackAccess.add_raw_records`.
///
/// Returns an error if the sizes do not add up to the buffer length, the
/// condition that would otherwise make the Python code read past the end
/// (yielding short slices) or leave trailing data unaccounted for.
pub fn split_raw_records(
    sizes: &[usize],
    data_len: usize,
) -> Result<Vec<RawRecordSlice>, PackRepoError> {
    let mut slices = Vec::with_capacity(sizes.len());
    let mut offset = 0usize;
    for &size in sizes {
        let end = offset
            .checked_add(size)
            .ok_or(PackRepoError::SizeOverflow)?;
        if end > data_len {
            return Err(PackRepoError::SizeMismatch {
                total: end,
                data_len,
            });
        }
        slices.push(RawRecordSlice {
            start: offset,
            length: size,
        });
        offset = end;
    }
    if offset != data_len {
        return Err(PackRepoError::SizeMismatch {
            total: offset,
            data_len,
        });
    }
    Ok(slices)
}

/// Errors from the pure pack_repo helpers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PackRepoError {
    /// The record sizes did not sum to the buffer length.
    SizeMismatch { total: usize, data_len: usize },
    /// Summing the record sizes overflowed `usize`.
    SizeOverflow,
}

impl std::fmt::Display for PackRepoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PackRepoError::SizeMismatch { total, data_len } => write!(
                f,
                "raw record sizes sum to {} but buffer is {} bytes",
                total, data_len
            ),
            PackRepoError::SizeOverflow => write!(f, "raw record sizes overflowed"),
        }
    }
}

impl std::error::Error for PackRepoError {}

/// The outcome of `_DirectPackAccess.reload_or_raise`'s decision logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReloadDecision {
    /// The reload recovered (or was assumed to via a prior reload); the
    /// caller should retry the operation.
    Retry,
    /// No recovery is possible; the caller must re-raise the original
    /// error that triggered the retry.
    Raise,
}

/// Decide whether to retry or re-raise after a `RetryWithNewPacks`,
/// matching the branch structure of `_DirectPackAccess.reload_or_raise`.
///
/// - `has_reload_func` is whether a reload function is configured.
/// - `reload_changed` is the boolean the reload function returned (only
///   meaningful when `has_reload_func` is true): true if it reported the
///   packs changed, false if it reported nothing changed.
/// - `reload_occurred` is the flag carried by the `RetryWithNewPacks`
///   exception: true if an earlier reload already happened (so a
///   no-change report is tolerated as an in-memory cache miss), false if
///   this is the first reload (so a no-change report is a hard error).
pub fn reload_decision(
    has_reload_func: bool,
    reload_changed: bool,
    reload_occurred: bool,
) -> ReloadDecision {
    if !has_reload_func {
        return ReloadDecision::Raise;
    }
    if !reload_changed && !reload_occurred {
        // We expected to find changes but the reload found none, and no
        // earlier reload can explain the miss: this is fatal.
        return ReloadDecision::Raise;
    }
    ReloadDecision::Retry
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_definitions_match_python() {
        assert_eq!(index_definition(IndexKind::Chk), (".cix", 4));
        assert_eq!(index_definition(IndexKind::Revision), (".rix", 0));
        assert_eq!(index_definition(IndexKind::Inventory), (".iix", 1));
        assert_eq!(index_definition(IndexKind::Text), (".tix", 2));
        assert_eq!(index_definition(IndexKind::Signature), (".six", 3));
    }

    #[test]
    fn index_name_appends_extension() {
        assert_eq!(index_name(IndexKind::Revision, "abc"), "abc.rix");
        assert_eq!(index_name(IndexKind::Chk, "abc"), "abc.cix");
    }

    #[test]
    fn kind_name_round_trips() {
        for kind in [
            IndexKind::Chk,
            IndexKind::Revision,
            IndexKind::Inventory,
            IndexKind::Text,
            IndexKind::Signature,
        ] {
            assert_eq!(IndexKind::from_name(kind.as_name()), Some(kind));
        }
        assert_eq!(IndexKind::from_name("bogus"), None);
    }

    #[test]
    fn grouping_coalesces_consecutive_same_index() {
        let memos = vec![(0u32, 0u64, 10u64), (0, 10, 5), (1, 0, 3), (0, 20, 4)];
        let groups = group_retrieval_requests(memos);
        assert_eq!(
            groups,
            vec![
                RetrievalGroup {
                    index: 0,
                    reads: vec![(0, 10), (10, 5)]
                },
                RetrievalGroup {
                    index: 1,
                    reads: vec![(0, 3)]
                },
                RetrievalGroup {
                    index: 0,
                    reads: vec![(20, 4)]
                },
            ]
        );
    }

    #[test]
    fn grouping_empty_yields_no_groups() {
        let groups = group_retrieval_requests(Vec::<(u32, u64, u64)>::new());
        assert!(groups.is_empty());
    }

    #[test]
    fn split_raw_records_exact() {
        let slices = split_raw_records(&[10, 2, 5], 17).unwrap();
        assert_eq!(
            slices,
            vec![
                RawRecordSlice {
                    start: 0,
                    length: 10
                },
                RawRecordSlice {
                    start: 10,
                    length: 2
                },
                RawRecordSlice {
                    start: 12,
                    length: 5
                },
            ]
        );
    }

    #[test]
    fn split_raw_records_too_short_is_error() {
        assert_eq!(
            split_raw_records(&[10, 5], 12),
            Err(PackRepoError::SizeMismatch {
                total: 15,
                data_len: 12
            })
        );
    }

    #[test]
    fn split_raw_records_trailing_data_is_error() {
        assert_eq!(
            split_raw_records(&[10], 12),
            Err(PackRepoError::SizeMismatch {
                total: 10,
                data_len: 12
            })
        );
    }

    #[test]
    fn reload_decision_table() {
        // No reload func: always raise.
        assert_eq!(reload_decision(false, false, false), ReloadDecision::Raise);
        assert_eq!(reload_decision(false, true, true), ReloadDecision::Raise);
        // Reload reports a change: retry regardless of reload_occurred.
        assert_eq!(reload_decision(true, true, false), ReloadDecision::Retry);
        assert_eq!(reload_decision(true, true, true), ReloadDecision::Retry);
        // Reload reports no change, first reload: hard error.
        assert_eq!(reload_decision(true, false, false), ReloadDecision::Raise);
        // Reload reports no change, but an earlier reload happened: retry
        // (treat as in-memory cache miss).
        assert_eq!(reload_decision(true, false, true), ReloadDecision::Retry);
    }
}

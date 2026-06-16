//! Pure-logic core of `GroupCompressVersionedFiles`.
//!
//! This module ports the orchestration that the Python
//! `bzrformats.groupcompress.GroupCompressVersionedFiles` class performs on
//! top of the already-Rust groupcompress block/compressor/manager code. The
//! pyo3 layer (`crates/bazaar-py/src/groupcompress.rs`) wraps a Python index
//! and access object and drives these helpers.

use crate::groupcompress::block::GroupCompressBlock;
use crate::knit::FileRef;

/// A versioned-file key: a tuple of byte segments, the last being the
/// version id. Groupcompress shares the keyspace type with knit.
pub type GcKey = crate::versionedfile::Key;

/// Number of bytes a fetch batch accumulates before it is flushed.
///
/// Mirrors `bzrformats.groupcompress.BATCH_SIZE`.
pub const BATCH_SIZE: u64 = 1 << 16;

/// Default cap on the bytes a `GroupCompressor` indexes for delta matching.
///
/// Mirrors `GroupCompressVersionedFiles._DEFAULT_MAX_BYTES_TO_INDEX`.
pub const DEFAULT_MAX_BYTES_TO_INDEX: usize = 1024 * 1024;

/// Identifies a single groupcompress block within a store, plus the byte
/// range it occupies.
///
/// This is the cache key for the block cache and the unit `_get_blocks`
/// fetches. Mirrors the Python `read_memo = index_memo[0:3]` triple
/// `(index, start, stop)`. `index` is abstracted via [`FileRef`] so the
/// pure crate does not depend on the Python graph-index object.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReadMemo<F: FileRef = String> {
    /// Identifies which backing index/shard the block lives in.
    pub index: F,
    /// Byte offset of the block's start in the backing file.
    pub start: u64,
    /// Byte offset one past the block's end.
    pub stop: u64,
}

impl<F: FileRef> ReadMemo<F> {
    pub fn new(index: F, start: u64, stop: u64) -> Self {
        ReadMemo { index, start, stop }
    }

    /// The on-disk byte length of the block.
    pub fn byte_length(&self) -> u64 {
        self.stop.saturating_sub(self.start)
    }
}

/// Locates a single record: which block it lives in (`read_memo`) and the
/// `[entry_start, entry_end)` slice of the decompressed block that holds it.
///
/// Mirrors the Python `index_memo = (index, start, stop, basis_end,
/// delta_end)` 5-tuple; the trailing pair becomes [`Self::entry_start`] /
/// [`Self::entry_end`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexMemo<F: FileRef = String> {
    /// The block this record lives in.
    pub read_memo: ReadMemo<F>,
    /// Offset of the record inside the decompressed block.
    pub entry_start: u64,
    /// Offset one past the record inside the decompressed block.
    pub entry_end: u64,
}

impl<F: FileRef> IndexMemo<F> {
    pub fn new(read_memo: ReadMemo<F>, entry_start: u64, entry_end: u64) -> Self {
        IndexMemo {
            read_memo,
            entry_start,
            entry_end,
        }
    }
}

/// A fetched groupcompress block paired with the memo it was fetched for.
///
/// `_get_blocks` yields these in the order the read-memos were requested.
pub struct FetchedBlock<F: FileRef = String> {
    pub read_memo: ReadMemo<F>,
    pub block: GroupCompressBlock,
}

/// What a [`GcIndex`] knows about one stored key.
///
/// Mirrors the Python `_GCGraphIndex.get_build_details` result (a
/// `GCBuildDetails`): where the record's bytes live (`index_memo`) and the
/// key's graph parents. groupcompress records are always whole entries
/// inside a block, so there is no separate compression-parent field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcBuildDetails<F: FileRef = String> {
    /// Locates the record: its block and slice within the block.
    pub index_memo: IndexMemo<F>,
    /// The key's graph parents, or `None` if the index stores no graph.
    pub parents: Option<Vec<GcKey>>,
}

/// The index half of a groupcompress store.
///
/// Resolves keys to build details and graph parents, and accepts new
/// records. The pyo3 layer implements this by wrapping a Python
/// `_GCGraphIndex`; pure-Rust callers implement it directly. Mirrors the
/// `KnitIndex` trait.
pub trait GcIndex {
    /// Identifies which backing block-file a record lives in.
    type F: FileRef;

    /// Build details for `keys`. Missing keys are absent from the result.
    fn get_build_details(
        &self,
        keys: &[GcKey],
    ) -> Result<std::collections::HashMap<GcKey, GcBuildDetails<Self::F>>, crate::knit::KnitError>;

    /// Graph parents for `keys`. Missing keys are absent from the result.
    fn get_parent_map(
        &self,
        keys: &[GcKey],
    ) -> Result<std::collections::HashMap<GcKey, Vec<GcKey>>, crate::knit::KnitError>;

    /// All keys present in this index.
    fn keys(&self) -> Result<Vec<GcKey>, crate::knit::KnitError>;

    /// Whether this index stores graph parents.
    fn has_graph(&self) -> bool;

    /// Assert that a write is permitted, erroring otherwise.
    fn check_write_ok(&self) -> Result<(), crate::knit::KnitError>;

    /// Add records to the index.
    ///
    /// Each record is `(key, index_memo, parents)`. `random_id` skips the
    /// duplicate check.
    fn add_records(
        &self,
        records: &[(GcKey, IndexMemo<Self::F>, Option<Vec<GcKey>>)],
        random_id: bool,
    ) -> Result<(), crate::knit::KnitError>;
}

/// The raw-data half of a groupcompress store.
///
/// Fetches and appends whole compressed blocks. Mirrors `KnitAccess`.
pub trait GcAccess {
    /// Identifies which backing block-file a block lives in.
    type F: FileRef;

    /// Fetch the raw (compressed) bytes for each read-memo, in order.
    fn get_raw_records(
        &self,
        memos: &[ReadMemo<Self::F>],
    ) -> Result<Vec<Vec<u8>>, crate::knit::KnitError>;

    /// Append a compressed block and return the read-memo locating it.
    fn add_raw_record(
        &self,
        size: usize,
        chunks: Vec<Vec<u8>>,
    ) -> Result<ReadMemo<Self::F>, crate::knit::KnitError>;

    /// Call the reload hook after a stale-pack error, or re-raise it.
    ///
    /// Returns `Ok(())` if the caller should retry, `Err` if unrecoverable.
    fn reload_or_raise(&self, err: crate::knit::KnitError) -> Result<(), crate::knit::KnitError> {
        Err(err)
    }
}

/// A shared, mutable reference to a decoded block.
///
/// `Arc<Mutex<..>>` rather than `Rc<RefCell<..>>` so the cache and the store
/// are `Send + Sync`-compatible — the pyo3 layer can hold the store in a
/// regular `#[pyclass]` without resorting to `unsendable`.
pub type SharedBlock = std::sync::Arc<std::sync::Mutex<GroupCompressBlock>>;

/// The decoded-block cache the store consults before fetching.
///
/// Decoupled into a trait so the pyo3 layer can back it with the Python
/// `LRUSizeCache` (size-bounded) while pure-Rust callers use a plain map.
pub trait BlockCache<F: FileRef>: Send + Sync {
    /// Fetch a cached block by read-memo.
    fn get(&self, memo: &ReadMemo<F>) -> Option<SharedBlock>;

    /// Store a freshly decoded block.
    fn insert(&self, memo: ReadMemo<F>, block: SharedBlock);

    /// Whether a block is in the cache.
    fn contains(&self, memo: &ReadMemo<F>) -> bool {
        self.get(memo).is_some()
    }

    /// Drop every cached block.
    fn clear(&self);

    /// Number of cached blocks.
    fn len(&self) -> usize;

    /// Whether the cache currently holds nothing.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Unbounded in-memory `BlockCache` for pure-Rust callers.
pub struct MapBlockCache<F: FileRef> {
    inner: std::sync::Mutex<std::collections::HashMap<ReadMemo<F>, SharedBlock>>,
}

impl<F: FileRef> Default for MapBlockCache<F> {
    fn default() -> Self {
        MapBlockCache {
            inner: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }
}

impl<F: FileRef> BlockCache<F> for MapBlockCache<F> {
    fn get(&self, memo: &ReadMemo<F>) -> Option<SharedBlock> {
        self.inner.lock().unwrap().get(memo).cloned()
    }
    fn insert(&self, memo: ReadMemo<F>, block: SharedBlock) {
        self.inner.lock().unwrap().insert(memo, block);
    }
    fn clear(&self) {
        self.inner.lock().unwrap().clear();
    }
    fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }
}

/// Given the read-memos a batch wants and which of them are already
/// cached, return the de-duplicated, order-preserving list of memos that
/// still need to be fetched.
///
/// Mirrors the partitioning loop in `GroupCompressVersionedFiles._get_blocks`:
/// a memo that is cached is skipped, and a memo already queued for fetch is
/// not queued twice. The first-seen request order is preserved so the
/// fetched raw records line up with the consume order. `is_cached` lets the
/// caller plug in whatever block cache it holds.
pub fn memos_to_fetch<F: FileRef>(
    read_memos: &[ReadMemo<F>],
    is_cached: impl Fn(&ReadMemo<F>) -> bool,
) -> Vec<ReadMemo<F>> {
    let mut out: Vec<ReadMemo<F>> = Vec::new();
    let mut seen: std::collections::HashSet<ReadMemo<F>> = std::collections::HashSet::new();
    for memo in read_memos {
        if is_cached(memo) {
            continue;
        }
        if seen.insert(memo.clone()) {
            out.push(memo.clone());
        }
    }
    out
}

/// Which store a record-stream key is served from.
///
/// The Python code carries the `GroupCompressVersionedFiles` object itself
/// (`self`) or a fallback VF object. The pure crate cannot hold Python
/// objects, so a fallback is identified by its index into the ordered
/// fallback list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// This versioned-files store (the Python `self`).
    Local,
    /// The fallback at this index in the immediate-fallback list.
    Fallback(usize),
}

/// Group an ordered key sequence into `(source, [keys])` runs.
///
/// `source_of` maps each key to its [`Source`]; consecutive keys from the
/// same source are collected into one run. Mirrors the "Now group by
/// source" loops shared by the three Python ordering helpers.
fn group_by_source(
    keys: impl IntoIterator<Item = GcKey>,
    source_of: impl Fn(&GcKey) -> Source,
) -> Vec<(Source, Vec<GcKey>)> {
    let mut runs: Vec<(Source, Vec<GcKey>)> = Vec::new();
    for key in keys {
        let source = source_of(&key);
        match runs.last_mut() {
            Some((s, run)) if *s == source => run.push(key),
            _ => runs.push((source, vec![key])),
        }
    }
    runs
}

/// Order keys topologically (or in groupcompress order) and group by source.
///
/// Mirrors `GroupCompressVersionedFiles._get_ordered_source_keys`. `ordering`
/// is `"topological"` or `"groupcompress"`; any key absent from
/// `key_to_source` is served locally.
pub fn ordered_source_keys(
    ordering: &str,
    parent_map: &[(GcKey, Vec<GcKey>)],
    key_to_source: &std::collections::HashMap<GcKey, Source>,
) -> Vec<(Source, Vec<GcKey>)> {
    let raw: Vec<(Vec<Vec<u8>>, Vec<Vec<Vec<u8>>>)> = parent_map
        .iter()
        .map(|(k, ps)| {
            (
                k.segments().to_vec(),
                ps.iter().map(|p| p.segments().to_vec()).collect(),
            )
        })
        .collect();
    let present: Vec<Vec<Vec<u8>>> = if ordering == "topological" {
        let mut sorter = vcs_graph::tsort::TopoSorter::new(raw.into_iter());
        sorter
            .sorted()
            .expect("groupcompress parent_map should not contain cycles")
    } else {
        crate::groupcompress::sort::sort_gc_optimal(raw)
    };
    let keys = present.into_iter().map(GcKey::fixed);
    group_by_source(keys, |k| {
        key_to_source.get(k).copied().unwrap_or(Source::Local)
    })
}

/// Keep the caller's requested order, grouping by source and dropping keys
/// that are absent from every store.
///
/// Mirrors `GroupCompressVersionedFiles._get_as_requested_source_keys`. A
/// key present in `locations` or `unadded` is local; otherwise its
/// `key_to_source` entry is used; a key in none of them is skipped.
pub fn as_requested_source_keys(
    orig_keys: &[GcKey],
    locations: &std::collections::HashSet<GcKey>,
    unadded: &std::collections::HashSet<GcKey>,
    key_to_source: &std::collections::HashMap<GcKey, Source>,
) -> Vec<(Source, Vec<GcKey>)> {
    let present: Vec<GcKey> = orig_keys
        .iter()
        .filter(|k| {
            locations.contains(*k) || unadded.contains(*k) || key_to_source.contains_key(*k)
        })
        .cloned()
        .collect();
    group_by_source(present, |k| {
        if locations.contains(k) || unadded.contains(k) {
            Source::Local
        } else {
            key_to_source.get(k).copied().unwrap_or(Source::Local)
        }
    })
}

/// Accumulates keys into a fetch batch, tracking the read-memos that batch
/// touches and a running byte estimate.
///
/// Ports the state that `_BatchingBlockFetcher.add_key` maintains. The
/// block cache is a Python `LRUSizeCache`, so the cache lookup is passed in
/// as a predicate; the actual fetch happens later in the pyo3 layer using
/// [`Self::memos_to_get`].
#[derive(Debug, Default)]
pub struct BatchAccumulator<F: FileRef = String> {
    keys: Vec<GcKey>,
    /// Read-memos seen in this batch, in first-seen order.
    batch_memos: Vec<ReadMemo<F>>,
    /// Read-memos in this batch that were not cached and must be fetched.
    memos_to_get: Vec<ReadMemo<F>>,
    total_bytes: u64,
}

impl<F: FileRef> BatchAccumulator<F> {
    pub fn new() -> Self {
        BatchAccumulator {
            keys: Vec::new(),
            batch_memos: Vec::new(),
            memos_to_get: Vec::new(),
            total_bytes: 0,
        }
    }

    /// Add a key to the current batch and return the running byte estimate.
    ///
    /// `read_memo` is the key's block memo (`index_memo[0:3]`). `is_cached`
    /// reports whether that block is already in the Python block cache.
    /// Mirrors `_BatchingBlockFetcher.add_key`: a memo already in the batch
    /// is not re-counted; a new uncached memo is queued for fetch and its
    /// `stop` offset is added to the estimate (Python adds `read_memo[2]`,
    /// the absolute stop, not the byte length — preserved here so the
    /// `BATCH_SIZE` threshold behaves identically).
    pub fn add_key(
        &mut self,
        key: GcKey,
        read_memo: ReadMemo<F>,
        is_cached: impl Fn(&ReadMemo<F>) -> bool,
    ) -> u64 {
        self.keys.push(key);
        if self.batch_memos.contains(&read_memo) {
            return self.total_bytes;
        }
        if !is_cached(&read_memo) {
            self.total_bytes += read_memo.stop;
            self.memos_to_get.push(read_memo.clone());
        }
        self.batch_memos.push(read_memo);
        self.total_bytes
    }

    /// Keys added to this batch, in insertion order.
    pub fn keys(&self) -> &[GcKey] {
        &self.keys
    }

    /// Uncached read-memos this batch must fetch, in first-seen order.
    pub fn memos_to_get(&self) -> &[ReadMemo<F>] {
        &self.memos_to_get
    }

    /// Running byte estimate for the batch.
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Clear all batch state, ready for the next batch.
    pub fn reset(&mut self) {
        self.keys.clear();
        self.batch_memos.clear();
        self.memos_to_get.clear();
        self.total_bytes = 0;
    }
}

/// Order keys for I/O efficiency: in-memory (unadded) keys first, then
/// located keys grouped by the block they live in, then fallback runs.
///
/// Mirrors `GroupCompressVersionedFiles._get_io_ordered_source_keys`.
/// `located_keys` is the located keys in the caller's order; each must have
/// an entry in `locations`. They are stably sorted by their block index so
/// keys in one group stay together while keeping their relative order, as
/// Python's `sorted(locations, key=get_group)` does over an insertion-
/// ordered dict. `fallback_runs` is the already-grouped `(source, keys)`
/// list for keys served by fallbacks.
pub fn io_ordered_source_keys<F: FileRef>(
    located_keys: &[GcKey],
    locations: &std::collections::HashMap<GcKey, IndexMemo<F>>,
    unadded: &[GcKey],
    fallback_runs: Vec<(Source, Vec<GcKey>)>,
) -> Vec<(Source, Vec<GcKey>)> {
    let mut local: Vec<GcKey> = unadded.to_vec();
    let mut located: Vec<GcKey> = located_keys.to_vec();
    // Python sorts located keys by the group object alone (index_memo[0]);
    // the sort is stable, so keys within one group keep their relative order.
    located.sort_by(|a, b| {
        locations[a]
            .read_memo
            .index
            .cmp(&locations[b].read_memo.index)
    });
    local.extend(located);
    let mut runs = vec![(Source::Local, local)];
    runs.extend(fallback_runs);
    runs
}

/// A groupcompress versioned-file store.
///
/// The pure-Rust equivalent of Python's `GroupCompressVersionedFiles`,
/// generic over a [`GcIndex`] and [`GcAccess`] exactly as
/// [`crate::knit::KnitVersionedFiles`] is over `KnitIndex` / `KnitAccess`.
/// A pure-Rust caller implements those two traits; the pyo3 layer wraps a
/// Python index / access object.
pub struct GroupCompressVersionedFiles<I, A, C = MapBlockCache<<I as GcIndex>::F>>
where
    I: GcIndex,
    A: GcAccess<F = I::F>,
    C: BlockCache<I::F>,
{
    index: I,
    access: A,
    /// Whether to delta-compress; carried for parity with the Python store.
    delta: bool,
    /// Decoded-block cache. Decoupled via [`BlockCache`] so the pyo3 layer
    /// can back it with the Python `LRUSizeCache` (size-bounded) while
    /// pure-Rust callers get an unbounded [`MapBlockCache`] by default.
    block_cache: C,
    /// Fallback stores consulted for keys absent from this one.
    fallbacks: Vec<Box<dyn crate::versionedfile::VersionedFiles>>,
}

impl<I, A> GroupCompressVersionedFiles<I, A, MapBlockCache<I::F>>
where
    I: GcIndex,
    A: GcAccess<F = I::F>,
{
    /// Create a store with the default in-memory block cache.
    pub fn new(index: I, access: A, delta: bool) -> Self {
        GroupCompressVersionedFiles::with_cache(index, access, delta, MapBlockCache::default())
    }
}

impl<I, A, C> GroupCompressVersionedFiles<I, A, C>
where
    I: GcIndex,
    A: GcAccess<F = I::F>,
    C: BlockCache<I::F>,
{
    /// Create a store with a caller-supplied block cache.
    pub fn with_cache(index: I, access: A, delta: bool, block_cache: C) -> Self {
        GroupCompressVersionedFiles {
            index,
            access,
            delta,
            block_cache,
            fallbacks: Vec::new(),
        }
    }

    /// The block cache.
    pub fn block_cache(&self) -> &C {
        &self.block_cache
    }

    /// Add a fallback store for keys not present in this one.
    ///
    /// Mirrors `GroupCompressVersionedFiles.add_fallback_versioned_files`.
    pub fn add_fallback_versioned_files(
        &mut self,
        fallback: Box<dyn crate::versionedfile::VersionedFiles>,
    ) {
        self.fallbacks.push(fallback);
    }

    /// The backing index.
    pub fn index(&self) -> &I {
        &self.index
    }

    /// The backing access object.
    pub fn access(&self) -> &A {
        &self.access
    }

    /// Whether this store delta-compresses.
    pub fn delta(&self) -> bool {
        self.delta
    }

    /// Fetch decoded blocks for `read_memos`, in request order.
    ///
    /// Mirrors `GroupCompressVersionedFiles._get_blocks`: cached blocks are
    /// reused, uncached read-memos are de-duplicated and fetched in one
    /// `get_raw_records` call, then decoded and cached.
    #[allow(clippy::type_complexity)]
    pub fn get_blocks(
        &self,
        read_memos: &[ReadMemo<I::F>],
    ) -> Result<Vec<(ReadMemo<I::F>, SharedBlock)>, crate::knit::KnitError> {
        let to_fetch = memos_to_fetch(read_memos, |m| self.block_cache.contains(m));
        let raw = self.access.get_raw_records(&to_fetch)?;
        if raw.len() != to_fetch.len() {
            return Err(crate::knit::KnitError::Corrupt(
                "get_raw_records returned the wrong number of records".to_string(),
            ));
        }
        // Decode and cache the freshly fetched blocks.
        for (memo, zdata) in to_fetch.iter().zip(raw) {
            let block = GroupCompressBlock::from_bytes(&zdata[..])
                .map_err(|e| crate::knit::KnitError::Corrupt(e.to_string()))?;
            self.block_cache.insert(
                memo.clone(),
                std::sync::Arc::new(std::sync::Mutex::new(block)),
            );
        }
        // Now every requested read-memo is in the cache; yield in order.
        let mut out = Vec::with_capacity(read_memos.len());
        for memo in read_memos {
            let block = self.block_cache.get(memo).ok_or_else(|| {
                crate::knit::KnitError::Corrupt("block missing after fetch".to_string())
            })?;
            out.push((memo.clone(), block));
        }
        Ok(out)
    }

    /// Graph parents for `keys`; absent keys are omitted.
    ///
    /// Mirrors `GroupCompressVersionedFiles.get_parent_map`: the local
    /// index is consulted first, then each fallback in turn for keys still
    /// missing.
    pub fn get_parent_map(
        &self,
        keys: &[GcKey],
    ) -> Result<std::collections::HashMap<GcKey, Vec<GcKey>>, crate::knit::KnitError> {
        let mut result = self.index.get_parent_map(keys)?;
        let mut missing: Vec<GcKey> = keys
            .iter()
            .filter(|k| !result.contains_key(*k))
            .cloned()
            .collect();
        for fb in &self.fallbacks {
            if missing.is_empty() {
                break;
            }
            let found = fb.get_parent_map(&missing)?;
            missing.retain(|k| !found.contains_key(k));
            result.extend(found);
        }
        Ok(result)
    }

    /// All keys present in this store or any fallback.
    pub fn keys(&self) -> Result<Vec<GcKey>, crate::knit::KnitError> {
        let mut seen: std::collections::HashSet<GcKey> = self.index.keys()?.into_iter().collect();
        for fb in &self.fallbacks {
            for k in fb.keys()? {
                seen.insert(k);
            }
        }
        Ok(seen.into_iter().collect())
    }

    /// Get a stream of records for `keys`.
    ///
    /// Mirrors `GroupCompressVersionedFiles.get_record_stream` for a store
    /// with no fallbacks: locate the keys, order them per `ordering`, fetch
    /// the blocks they live in, and extract each record into a
    /// `ChunkedContentFactory`. Keys absent from the index yield an
    /// `AbsentContentFactory`. Records are returned eagerly as a `Vec`, as
    /// the knit pure store does.
    ///
    /// `ordering` is `"unordered"`, `"topological"`, `"groupcompress"`, or
    /// `"as-requested"`.
    pub fn get_record_stream(
        &self,
        keys: &[GcKey],
        ordering: &str,
    ) -> Result<Vec<Box<dyn crate::versionedfile::ContentFactory>>, crate::knit::KnitError> {
        let locations = self.index.get_build_details(keys)?;
        let mut out: Vec<Box<dyn crate::versionedfile::ContentFactory>> = Vec::new();
        // Keys not in the local index are tried against fallbacks; only keys
        // absent from every store yield an AbsentContentFactory.
        let mut nonlocal: Vec<GcKey> = keys
            .iter()
            .filter(|k| !locations.contains_key(*k))
            .cloned()
            .collect();
        for fb in &self.fallbacks {
            if nonlocal.is_empty() {
                break;
            }
            let fb_records = fb.get_record_stream(&nonlocal, ordering, true)?;
            let mut still_missing: Vec<GcKey> = Vec::new();
            let mut found: std::collections::HashSet<GcKey> = std::collections::HashSet::new();
            for rec in fb_records {
                let rec = rec?;
                if rec.storage_kind() == "absent" {
                    continue;
                }
                found.insert(rec.key());
                out.push(rec);
            }
            for k in nonlocal {
                if !found.contains(&k) {
                    still_missing.push(k);
                }
            }
            nonlocal = still_missing;
        }
        for key in nonlocal {
            out.push(Box::new(crate::versionedfile::AbsentContentFactory::new(
                key,
            )));
        }
        // Order the located keys.
        let located: Vec<GcKey> = keys
            .iter()
            .filter(|k| locations.contains_key(*k))
            .cloned()
            .collect();
        let ordered: Vec<GcKey> = match ordering {
            "topological" | "groupcompress" => {
                let parent_map: Vec<(GcKey, Vec<GcKey>)> = located
                    .iter()
                    .map(|k| (k.clone(), locations[k].parents.clone().unwrap_or_default()))
                    .collect();
                let empty = std::collections::HashMap::new();
                ordered_source_keys(ordering, &parent_map, &empty)
                    .into_iter()
                    .flat_map(|(_, ks)| ks)
                    .collect()
            }
            "as-requested" => located,
            // "unordered" and anything else: I/O order, grouped by block.
            _ => {
                let mut io = located;
                io.sort_by(|a, b| {
                    let ka = &locations[a].index_memo;
                    let kb = &locations[b].index_memo;
                    (
                        &ka.read_memo.index,
                        ka.read_memo.start,
                        ka.read_memo.stop,
                        ka.entry_start,
                        ka.entry_end,
                    )
                        .cmp(&(
                            &kb.read_memo.index,
                            kb.read_memo.start,
                            kb.read_memo.stop,
                            kb.entry_start,
                            kb.entry_end,
                        ))
                });
                io
            }
        };
        // Fetch every block the ordered keys touch, then extract records.
        let read_memos: Vec<ReadMemo<I::F>> = ordered
            .iter()
            .map(|k| locations[k].index_memo.read_memo.clone())
            .collect();
        let blocks = self.get_blocks(&read_memos)?;
        let block_of: std::collections::HashMap<ReadMemo<I::F>, SharedBlock> =
            blocks.into_iter().collect();
        for key in &ordered {
            let memo = &locations[key].index_memo;
            let block = block_of.get(&memo.read_memo).ok_or_else(|| {
                crate::knit::KnitError::Corrupt("block missing for located key".to_string())
            })?;
            let chunks = block
                .lock()
                .unwrap()
                .extract(memo.entry_start as usize, memo.entry_end as usize)
                .map_err(|e| crate::knit::KnitError::Corrupt(format!("{:?}", e)))?;
            out.push(Box::new(crate::versionedfile::ChunkedContentFactory::new(
                None,
                key.clone(),
                locations[key].parents.clone(),
                chunks,
            )));
        }
        Ok(out)
    }

    /// Insert a stream of records into this store.
    ///
    /// Mirrors `GroupCompressVersionedFiles._insert_record_stream` for the
    /// ordinary (non-block-reuse) path: each record's text is compressed
    /// into a `RabinGroupCompressor`; when the start-new-block heuristic
    /// fires the current block is flushed (written via `GcAccess` and
    /// indexed via `GcIndex`) and a fresh compressor started. Returns the
    /// `(sha1, length)` of each inserted record.
    ///
    /// `nostore_sha`, when set on a record's content, causes
    /// `KnitError::ExistingContent` if the text is already stored.
    pub fn insert_record_stream(
        &self,
        stream: impl IntoIterator<Item = Box<dyn crate::versionedfile::ContentFactory>>,
        random_id: bool,
    ) -> Result<Vec<(Vec<u8>, usize)>, crate::knit::KnitError> {
        use crate::groupcompress::compressor::{GroupCompressor, RabinGroupCompressor};

        self.index.check_write_ok()?;
        let mut results: Vec<(Vec<u8>, usize)> = Vec::new();
        let mut compressor = RabinGroupCompressor::new(None);
        // Records compressed into the block not yet flushed: their key, the
        // (entry_start, entry_end) within the block, and graph parents.
        let mut pending: Vec<(GcKey, usize, usize, Option<Vec<GcKey>>)> = Vec::new();
        let mut inserted: std::collections::HashSet<GcKey> = std::collections::HashSet::new();

        let mut last_prefix: Option<Vec<u8>> = None;
        let mut max_fulltext_len: usize = 0;
        let mut max_fulltext_prefix: Option<Vec<u8>> = None;

        for record in stream {
            let key = record.key();
            if record.storage_kind() == "absent" {
                return Err(crate::knit::KnitError::RevisionNotPresent(
                    key.segments().to_vec(),
                ));
            }
            if random_id && !inserted.insert(key.clone()) {
                // Same key offered twice under random_id: skip the dup.
                continue;
            }
            // The record's text, as chunks.
            let chunks: Vec<Vec<u8>> = record.to_chunks().map(|c| c.into_owned()).collect();
            let chunks_len: usize = record
                .size()
                .unwrap_or_else(|| chunks.iter().map(|c| c.len()).sum());
            let chunk_refs: Vec<&[u8]> = chunks.iter().map(|c| c.as_slice()).collect();

            // The prefix is the key's first segment for multi-segment keys.
            let prefix: Option<Vec<u8>> = if key.segments().len() > 1 {
                Some(key.segments()[0].clone())
            } else {
                None
            };
            let soft = prefix.is_some() && prefix == last_prefix;
            if max_fulltext_len < chunks_len {
                max_fulltext_len = chunks_len;
                max_fulltext_prefix = prefix.clone();
            }
            let (mut sha1, mut start_point, mut end_point) = compress_record(
                &mut compressor,
                &key,
                &chunk_refs,
                chunks_len,
                record.sha1(),
                soft,
            )?;
            // Start-new-block heuristic (mirrors the Python conditions).
            let same_prefix = prefix == max_fulltext_prefix;
            let start_new_block = if same_prefix && end_point < 2 * max_fulltext_len {
                false
            } else if end_point > 4 * 1024 * 1024 {
                true
            } else {
                prefix.is_some() && prefix != last_prefix && end_point > 2 * 1024 * 1024
            };
            last_prefix = prefix;
            if start_new_block {
                let (content_chunks, content_len) = compressor.flush_without_last();
                self.flush_block(content_chunks, content_len, &mut pending, random_id)?;
                compressor = RabinGroupCompressor::new(None);
                max_fulltext_len = chunks_len;
                let recompressed = compress_record(
                    &mut compressor,
                    &key,
                    &chunk_refs,
                    chunks_len,
                    record.sha1(),
                    false,
                )?;
                sha1 = recompressed.0;
                start_point = recompressed.1;
                end_point = recompressed.2;
            }
            // A content-addressed key (None version id) gets the sha1 filled in.
            let stored_key = if key.version_id().is_empty() {
                GcKey::from_prefix_and_suffix(key.prefix(), {
                    let mut seg = b"sha1:".to_vec();
                    seg.extend_from_slice(&sha1);
                    seg
                })
            } else {
                key.clone()
            };
            results.push((sha1, chunks_len));
            pending.push((stored_key, start_point, end_point, record.parents()));
        }
        if !pending.is_empty() {
            let (content_chunks, content_len) = compressor.flush();
            self.flush_block(content_chunks, content_len, &mut pending, random_id)?;
        }
        Ok(results)
    }

    /// Wrap the compressor's flushed content into a block, write it via the
    /// access object, and index every pending record against it.
    ///
    /// Mirrors the `flush` closure inside `_insert_record_stream`.
    fn flush_block(
        &self,
        content_chunks: Vec<Vec<u8>>,
        content_len: usize,
        pending: &mut Vec<(GcKey, usize, usize, Option<Vec<GcKey>>)>,
        random_id: bool,
    ) -> Result<(), crate::knit::KnitError> {
        let mut block = GroupCompressBlock::new();
        block.set_chunked_content(&content_chunks, content_len);
        let on_disk = block.to_bytes();
        let size = on_disk.len();
        let read_memo = self.access.add_raw_record(size, vec![on_disk])?;
        let records: Vec<(GcKey, IndexMemo<I::F>, Option<Vec<GcKey>>)> = pending
            .drain(..)
            .map(|(key, start, end, parents)| {
                (
                    key,
                    IndexMemo::new(read_memo.clone(), start as u64, end as u64),
                    parents,
                )
            })
            .collect();
        self.index.add_records(&records, random_id)?;
        Ok(())
    }

    /// Add one text given as a list of lines.
    ///
    /// Mirrors `GroupCompressVersionedFiles.add_lines` / `add_content`:
    /// wraps the lines in a content factory and inserts it. Returns the
    /// text's `(sha1, length)`.
    pub fn add_lines(
        &self,
        key: GcKey,
        parents: Option<Vec<GcKey>>,
        lines: Vec<Vec<u8>>,
    ) -> Result<(Vec<u8>, usize), crate::knit::KnitError> {
        // _check_add: a version id with embedded whitespace is rejected.
        let version_id = key.version_id();
        if version_id
            .iter()
            .any(|b| matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c))
        {
            return Err(crate::knit::KnitError::Corrupt(format!(
                "invalid revision id: {:?}",
                version_id
            )));
        }
        let sha1 = crate::weave::sha_strings(&lines);
        let factory: Box<dyn crate::versionedfile::ContentFactory> = Box::new(
            crate::versionedfile::ChunkedContentFactory::new(Some(sha1), key, parents, lines),
        );
        let mut inserted = self.insert_record_stream(std::iter::once(factory), false)?;
        inserted
            .pop()
            .ok_or_else(|| crate::knit::KnitError::Corrupt("add_lines inserted nothing".into()))
    }

    /// SHA-1 of every requested key; absent keys are omitted.
    ///
    /// Mirrors `GroupCompressVersionedFiles.get_sha1s`.
    pub fn get_sha1s(
        &self,
        keys: &[GcKey],
    ) -> Result<std::collections::HashMap<GcKey, Vec<u8>>, crate::knit::KnitError> {
        let mut out = std::collections::HashMap::new();
        for record in self.get_record_stream(keys, "unordered")? {
            if record.storage_kind() == "absent" {
                continue;
            }
            let digest = match record.sha1() {
                Some(s) => s,
                None => {
                    let chunks: Vec<Vec<u8>> = record.to_chunks().map(|c| c.into_owned()).collect();
                    crate::weave::sha_strings(&chunks)
                }
            };
            out.insert(record.key(), digest);
        }
        Ok(out)
    }

    /// Keys of missing compression parents.
    ///
    /// Mirrors `get_missing_compression_parent_keys`: groupcompress cannot
    /// reference texts outside the group, so this is always empty.
    pub fn get_missing_compression_parent_keys(&self) -> Vec<GcKey> {
        Vec::new()
    }

    /// Drop the decoded-block cache.
    ///
    /// Mirrors `GroupCompressVersionedFiles.clear_cache`.
    pub fn clear_cache(&self) {
        self.block_cache.clear();
    }

    /// Walk the lines of every requested key.
    ///
    /// Mirrors `iter_lines_added_or_present_in_keys`: each key's text is
    /// read and split into lines, returned as `(line, key)` pairs. A key
    /// absent from every store is an error.
    /// Yield `(line, key)` for every line added by or present in
    /// `keys`. The record stream is fetched up front (the index lookup
    /// is inherently batched), but each record is only decoded into
    /// lines when the iterator reaches it. Yields `Err` for an absent
    /// key or a line that fails to decode.
    pub fn iter_lines_added_or_present_in_keys(
        &self,
        keys: &[GcKey],
    ) -> Result<
        impl Iterator<Item = Result<(Vec<u8>, GcKey), crate::knit::KnitError>>,
        crate::knit::KnitError,
    > {
        let records = self.get_record_stream(keys, "unordered")?;
        Ok(records.into_iter().flat_map(|record| {
            if record.storage_kind() == "absent" {
                let err =
                    crate::knit::KnitError::RevisionNotPresent(record.key().segments().to_vec());
                return vec![Err(err)].into_iter();
            }
            let key = record.key();
            let chunks: Vec<Vec<u8>> = record.to_chunks().map(|c| c.into_owned()).collect();
            crate::osutils::chunks_to_lines(chunks.into_iter().map(Ok::<_, std::io::Error>))
                .map(|line| {
                    line.map(|l| (l.into_owned(), key.clone()))
                        .map_err(|e| crate::knit::KnitError::Corrupt(e.to_string()))
                })
                .collect::<Vec<_>>()
                .into_iter()
        }))
    }

    /// Check the store reads back: every key's text is fetched and decoded.
    ///
    /// Mirrors `GroupCompressVersionedFiles.check` with `keys=None`.
    pub fn check(&self) -> Result<(), crate::knit::KnitError> {
        let all_keys = self.keys()?;
        for record in self.get_record_stream(&all_keys, "unordered")? {
            if record.storage_kind() == "absent" {
                return Err(crate::knit::KnitError::RevisionNotPresent(
                    record.key().segments().to_vec(),
                ));
            }
            // Force decoding of the content.
            let _ = record.to_fulltext();
        }
        Ok(())
    }
}

/// `GroupCompressVersionedFiles` is a full [`VersionedFiles`] backend, so
/// it can be used anywhere the trait is expected (as a CHK store for
/// `CHKInventory`, as a fallback store, etc.). The trait methods delegate
/// to the inherent ones, adapting the few signatures that differ (the
/// trait borrows keys/lines and boxes the record stream).
impl<I, A, C> crate::versionedfile::VersionedFiles for GroupCompressVersionedFiles<I, A, C>
where
    I: GcIndex + Send + Sync,
    A: GcAccess<F = I::F> + Send + Sync,
    C: BlockCache<I::F> + Send + Sync,
{
    fn get_parent_map(
        &self,
        keys: &[GcKey],
    ) -> Result<std::collections::HashMap<GcKey, Vec<GcKey>>, crate::knit::KnitError> {
        GroupCompressVersionedFiles::get_parent_map(self, keys)
    }

    fn get_record_stream(
        &self,
        keys: &[GcKey],
        ordering: &str,
        _include_delta_closure: bool,
    ) -> Result<
        Box<
            dyn Iterator<
                Item = Result<
                    Box<dyn crate::versionedfile::ContentFactory>,
                    crate::knit::KnitError,
                >,
            >,
        >,
        crate::knit::KnitError,
    > {
        let records = GroupCompressVersionedFiles::get_record_stream(self, keys, ordering)?;
        Ok(Box::new(records.into_iter().map(Ok)))
    }

    fn get_sha1s(
        &self,
        keys: &[GcKey],
    ) -> Result<std::collections::HashMap<GcKey, Vec<u8>>, crate::knit::KnitError> {
        GroupCompressVersionedFiles::get_sha1s(self, keys)
    }

    fn keys(&self) -> Result<Vec<GcKey>, crate::knit::KnitError> {
        GroupCompressVersionedFiles::keys(self)
    }

    fn add_lines(
        &self,
        key: &GcKey,
        parents: Option<&[GcKey]>,
        lines: &[Vec<u8>],
    ) -> Result<(Vec<u8>, usize), crate::knit::KnitError> {
        GroupCompressVersionedFiles::add_lines(
            self,
            key.clone(),
            parents.map(|p| p.to_vec()),
            lines.to_vec(),
        )
    }

    fn insert_record_stream(
        &self,
        stream: Box<dyn Iterator<Item = Box<dyn crate::versionedfile::ContentFactory>>>,
    ) -> Result<(), crate::knit::KnitError> {
        GroupCompressVersionedFiles::insert_record_stream(self, stream, false)?;
        Ok(())
    }

    fn iter_lines_added_or_present_in_keys(
        &self,
        keys: &[GcKey],
    ) -> Result<Vec<(Vec<u8>, GcKey)>, crate::knit::KnitError> {
        GroupCompressVersionedFiles::iter_lines_added_or_present_in_keys(self, keys)?
            .collect::<Result<Vec<_>, _>>()
    }

    fn annotate(&self, _key: &GcKey) -> Result<Vec<(GcKey, Vec<u8>)>, crate::knit::KnitError> {
        Err(crate::knit::KnitError::NotImplemented(
            "groupcompress annotate",
        ))
    }

    fn get_missing_compression_parent_keys(&self) -> Result<Vec<GcKey>, crate::knit::KnitError> {
        Ok(GroupCompressVersionedFiles::get_missing_compression_parent_keys(self))
    }

    fn clear_cache(&self) {
        GroupCompressVersionedFiles::clear_cache(self)
    }

    fn check(&self) -> Result<(), crate::knit::KnitError> {
        GroupCompressVersionedFiles::check(self)
    }
}

/// Compress one record into `compressor`, mapping the result to plain
/// types and `nostore_sha` rejection to `KnitError::ExistingContent`.
fn compress_record(
    compressor: &mut crate::groupcompress::compressor::RabinGroupCompressor,
    key: &GcKey,
    chunks: &[&[u8]],
    length: usize,
    expected_sha: Option<Vec<u8>>,
    soft: bool,
) -> Result<(Vec<u8>, usize, usize), crate::knit::KnitError> {
    use crate::groupcompress::compressor::GroupCompressor;
    let expected = expected_sha
        .map(|s| String::from_utf8(s))
        .transpose()
        .map_err(|e| crate::knit::KnitError::Corrupt(e.to_string()))?;
    match compressor.compress(key, chunks, length, expected, None, Some(soft)) {
        Ok((sha, start, end, _kind)) => Ok((sha.into_bytes(), start, end)),
        Err(crate::versionedfile::Error::ExistingContent(_)) => {
            Err(crate::knit::KnitError::ExistingContent(Vec::new()))
        }
        Err(e) => Err(crate::knit::KnitError::Corrupt(e.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_memo_byte_length_is_stop_minus_start() {
        let memo = ReadMemo::new("idx".to_string(), 100, 350);
        assert_eq!(memo.byte_length(), 250);
    }

    #[test]
    fn read_memo_byte_length_saturates_when_stop_below_start() {
        // A corrupt memo with stop < start yields 0 rather than underflowing.
        let memo = ReadMemo::new("idx".to_string(), 500, 100);
        assert_eq!(memo.byte_length(), 0);
    }

    #[test]
    fn read_memos_compare_by_all_three_fields() {
        let base = ReadMemo::new("a".to_string(), 0, 10);
        assert_eq!(base, ReadMemo::new("a".to_string(), 0, 10));
        assert_ne!(base, ReadMemo::new("b".to_string(), 0, 10));
        assert_ne!(base, ReadMemo::new("a".to_string(), 1, 10));
        assert_ne!(base, ReadMemo::new("a".to_string(), 0, 11));
    }

    #[test]
    fn index_memo_carries_read_memo_and_entry_range() {
        let rm = ReadMemo::new("idx".to_string(), 0, 1000);
        let im = IndexMemo::new(rm.clone(), 40, 120);
        assert_eq!(im.read_memo, rm);
        assert_eq!(im.entry_start, 40);
        assert_eq!(im.entry_end, 120);
    }

    fn memo(idx: &str, start: u64) -> ReadMemo<String> {
        ReadMemo::new(idx.to_string(), start, start + 10)
    }

    #[test]
    fn memos_to_fetch_skips_cached_and_preserves_order() {
        let req = vec![memo("a", 0), memo("b", 0), memo("c", 0)];
        let cached = [memo("b", 0)];
        let out = memos_to_fetch(&req, |m| cached.contains(m));
        assert_eq!(out, vec![memo("a", 0), memo("c", 0)]);
    }

    #[test]
    fn memos_to_fetch_dedups_repeated_memos() {
        // The same block requested twice is fetched once, in first-seen order.
        let req = vec![memo("a", 0), memo("b", 0), memo("a", 0), memo("c", 0)];
        let out = memos_to_fetch(&req, |_| false);
        assert_eq!(out, vec![memo("a", 0), memo("b", 0), memo("c", 0)]);
    }

    #[test]
    fn memos_to_fetch_empty_when_all_cached() {
        let req = vec![memo("a", 0), memo("b", 0)];
        let out = memos_to_fetch(&req, |_| true);
        assert!(out.is_empty());
    }

    fn gckey(id: &[u8]) -> GcKey {
        GcKey::fixed(vec![id.to_vec()])
    }

    #[test]
    fn ordered_source_keys_topological_groups_by_source() {
        // Chain a -> b -> c; b is served by fallback 0, the rest locally.
        let a = gckey(b"a");
        let b = gckey(b"b");
        let c = gckey(b"c");
        let parent_map = vec![
            (a.clone(), vec![]),
            (b.clone(), vec![a.clone()]),
            (c.clone(), vec![b.clone()]),
        ];
        let mut k2s = std::collections::HashMap::new();
        k2s.insert(b.clone(), Source::Fallback(0));
        let runs = ordered_source_keys("topological", &parent_map, &k2s);
        assert_eq!(
            runs,
            vec![
                (Source::Local, vec![a]),
                (Source::Fallback(0), vec![b]),
                (Source::Local, vec![c]),
            ]
        );
    }

    #[test]
    fn as_requested_source_keys_keeps_order_and_drops_absent() {
        let a = gckey(b"a");
        let b = gckey(b"b");
        let absent = gckey(b"absent");
        let f = gckey(b"f");
        let locations: std::collections::HashSet<GcKey> = vec![a.clone()].into_iter().collect();
        let unadded: std::collections::HashSet<GcKey> = vec![b.clone()].into_iter().collect();
        let mut k2s = std::collections::HashMap::new();
        k2s.insert(f.clone(), Source::Fallback(1));
        let runs = as_requested_source_keys(
            &[a.clone(), absent, b.clone(), f.clone()],
            &locations,
            &unadded,
            &k2s,
        );
        // `absent` is dropped; a and b are both local and merge into one run.
        assert_eq!(
            runs,
            vec![(Source::Local, vec![a, b]), (Source::Fallback(1), vec![f])]
        );
    }

    #[test]
    fn io_ordered_source_keys_unadded_first_then_grouped_then_fallbacks() {
        let u = gckey(b"u");
        let x = gckey(b"x");
        let y = gckey(b"y");
        let f = gckey(b"f");
        let mut locations = std::collections::HashMap::new();
        // x in block "g2", y in block "g1" — sort pulls y ahead of x.
        locations.insert(
            x.clone(),
            IndexMemo::new(ReadMemo::new("g2".to_string(), 0, 10), 0, 5),
        );
        locations.insert(
            y.clone(),
            IndexMemo::new(ReadMemo::new("g1".to_string(), 0, 10), 0, 5),
        );
        let runs = io_ordered_source_keys(
            &[x.clone(), y.clone()],
            &locations,
            &[u.clone()],
            vec![(Source::Fallback(0), vec![f.clone()])],
        );
        assert_eq!(
            runs,
            vec![
                (Source::Local, vec![u, y, x]),
                (Source::Fallback(0), vec![f]),
            ]
        );
    }

    #[test]
    fn batch_accumulator_queues_uncached_memos_and_counts_stop() {
        let mut acc: BatchAccumulator<String> = BatchAccumulator::new();
        // Two keys in distinct uncached blocks.
        let t1 = acc.add_key(gckey(b"k1"), ReadMemo::new("g1".into(), 0, 30), |_| false);
        assert_eq!(t1, 30); // running estimate adds `stop`
        let t2 = acc.add_key(gckey(b"k2"), ReadMemo::new("g2".into(), 0, 50), |_| false);
        assert_eq!(t2, 80);
        assert_eq!(acc.keys(), &[gckey(b"k1"), gckey(b"k2")]);
        assert_eq!(
            acc.memos_to_get(),
            &[
                ReadMemo::new("g1".into(), 0, 30),
                ReadMemo::new("g2".into(), 0, 50),
            ]
        );
    }

    #[test]
    fn batch_accumulator_does_not_recount_repeated_memo() {
        let mut acc: BatchAccumulator<String> = BatchAccumulator::new();
        let block = ReadMemo::new("g1".to_string(), 0, 30);
        acc.add_key(gckey(b"k1"), block.clone(), |_| false);
        // A second key in the same block adds the key but not the bytes.
        let total = acc.add_key(gckey(b"k2"), block.clone(), |_| false);
        assert_eq!(total, 30);
        assert_eq!(acc.keys().len(), 2);
        assert_eq!(acc.memos_to_get(), &[block]);
    }

    #[test]
    fn batch_accumulator_skips_fetch_for_cached_memo() {
        let mut acc: BatchAccumulator<String> = BatchAccumulator::new();
        let block = ReadMemo::new("g1".to_string(), 0, 30);
        // Cached blocks are not queued and not counted.
        let total = acc.add_key(gckey(b"k1"), block, |_| true);
        assert_eq!(total, 0);
        assert!(acc.memos_to_get().is_empty());
        assert_eq!(acc.keys().len(), 1);
    }

    #[test]
    fn batch_accumulator_reset_clears_state() {
        let mut acc: BatchAccumulator<String> = BatchAccumulator::new();
        acc.add_key(gckey(b"k1"), ReadMemo::new("g1".into(), 0, 30), |_| false);
        acc.reset();
        assert!(acc.keys().is_empty());
        assert!(acc.memos_to_get().is_empty());
        assert_eq!(acc.total_bytes(), 0);
    }

    /// Serialize a one-record fulltext block to its on-disk bytes.
    fn block_bytes(body: &[u8]) -> Vec<u8> {
        let mut b = GroupCompressBlock::new();
        b.set_content(body);
        b.to_bytes()
    }

    /// A `GcAccess` that serves canned block bytes and counts fetches.
    struct MockAccess {
        blocks: std::collections::HashMap<ReadMemo<String>, Vec<u8>>,
        fetched: std::cell::RefCell<Vec<ReadMemo<String>>>,
    }

    impl GcAccess for MockAccess {
        type F = String;
        fn get_raw_records(
            &self,
            memos: &[ReadMemo<String>],
        ) -> Result<Vec<Vec<u8>>, crate::knit::KnitError> {
            self.fetched.borrow_mut().extend_from_slice(memos);
            memos
                .iter()
                .map(|m| {
                    self.blocks
                        .get(m)
                        .cloned()
                        .ok_or_else(|| crate::knit::KnitError::Corrupt("no such memo".into()))
                })
                .collect()
        }
        fn add_raw_record(
            &self,
            _size: usize,
            _chunks: Vec<Vec<u8>>,
        ) -> Result<ReadMemo<String>, crate::knit::KnitError> {
            unimplemented!("not needed for get_blocks tests")
        }
    }

    /// A `GcIndex` stub; `get_blocks` does not touch the index.
    struct UnusedIndex;
    impl GcIndex for UnusedIndex {
        type F = String;
        fn get_build_details(
            &self,
            _keys: &[GcKey],
        ) -> Result<std::collections::HashMap<GcKey, GcBuildDetails<String>>, crate::knit::KnitError>
        {
            unimplemented!()
        }
        fn get_parent_map(
            &self,
            _keys: &[GcKey],
        ) -> Result<std::collections::HashMap<GcKey, Vec<GcKey>>, crate::knit::KnitError> {
            unimplemented!()
        }
        fn keys(&self) -> Result<Vec<GcKey>, crate::knit::KnitError> {
            unimplemented!()
        }
        fn has_graph(&self) -> bool {
            true
        }
        fn check_write_ok(&self) -> Result<(), crate::knit::KnitError> {
            unimplemented!()
        }
        fn add_records(
            &self,
            _records: &[(GcKey, IndexMemo<String>, Option<Vec<GcKey>>)],
            _random_id: bool,
        ) -> Result<(), crate::knit::KnitError> {
            unimplemented!()
        }
    }

    #[test]
    fn get_blocks_fetches_decodes_and_caches() {
        let m1 = ReadMemo::new("g".to_string(), 0, 10);
        let m2 = ReadMemo::new("g".to_string(), 10, 20);
        let mut blocks = std::collections::HashMap::new();
        blocks.insert(m1.clone(), block_bytes(b"first block body\n"));
        blocks.insert(m2.clone(), block_bytes(b"second block body\n"));
        let access = MockAccess {
            blocks,
            fetched: std::cell::RefCell::new(Vec::new()),
        };
        let vf = GroupCompressVersionedFiles::new(UnusedIndex, access, true);

        // First call fetches both, in request order.
        let out = vf.get_blocks(&[m1.clone(), m2.clone()]).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].0, m1);
        assert_eq!(out[1].0, m2);
        assert_eq!(vf.access().fetched.borrow().len(), 2);

        // Second call is served entirely from the cache: no new fetch.
        let out2 = vf.get_blocks(&[m1.clone(), m2.clone()]).unwrap();
        assert_eq!(out2.len(), 2);
        assert_eq!(vf.access().fetched.borrow().len(), 2);
    }

    #[test]
    fn get_blocks_dedups_repeated_memos_in_one_call() {
        let m = ReadMemo::new("g".to_string(), 0, 10);
        let mut blocks = std::collections::HashMap::new();
        blocks.insert(m.clone(), block_bytes(b"body\n"));
        let access = MockAccess {
            blocks,
            fetched: std::cell::RefCell::new(Vec::new()),
        };
        let vf = GroupCompressVersionedFiles::new(UnusedIndex, access, true);
        // The same block requested twice is fetched once, yielded twice.
        let out = vf.get_blocks(&[m.clone(), m.clone()]).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(vf.access().fetched.borrow().len(), 1);
    }

    /// A `GcIndex` that serves a fixed build-details map.
    struct MapIndex {
        details: std::collections::HashMap<GcKey, GcBuildDetails<String>>,
    }
    impl GcIndex for MapIndex {
        type F = String;
        fn get_build_details(
            &self,
            keys: &[GcKey],
        ) -> Result<std::collections::HashMap<GcKey, GcBuildDetails<String>>, crate::knit::KnitError>
        {
            Ok(keys
                .iter()
                .filter_map(|k| self.details.get(k).map(|d| (k.clone(), d.clone())))
                .collect())
        }
        fn get_parent_map(
            &self,
            keys: &[GcKey],
        ) -> Result<std::collections::HashMap<GcKey, Vec<GcKey>>, crate::knit::KnitError> {
            Ok(keys
                .iter()
                .filter_map(|k| {
                    self.details
                        .get(k)
                        .map(|d| (k.clone(), d.parents.clone().unwrap_or_default()))
                })
                .collect())
        }
        fn keys(&self) -> Result<Vec<GcKey>, crate::knit::KnitError> {
            Ok(self.details.keys().cloned().collect())
        }
        fn has_graph(&self) -> bool {
            true
        }
        fn check_write_ok(&self) -> Result<(), crate::knit::KnitError> {
            Ok(())
        }
        fn add_records(
            &self,
            _records: &[(GcKey, IndexMemo<String>, Option<Vec<GcKey>>)],
            _random_id: bool,
        ) -> Result<(), crate::knit::KnitError> {
            unimplemented!()
        }
    }

    #[test]
    fn get_record_stream_extracts_records_from_a_block() {
        use crate::groupcompress::compressor::{GroupCompressor, RabinGroupCompressor};

        // Compress two records into one block, capturing their positions.
        let mut gc = RabinGroupCompressor::new(None);
        let body_a = b"record a text line one\nrecord a text line two\n";
        let body_b = b"record b text line one\nrecord b text line two\n";
        let (_sha, a_start, a_end, _k) = gc
            .compress(
                &GcKey::fixed(vec![b"a".to_vec()]),
                &[body_a.as_slice()],
                body_a.len(),
                None,
                None,
                None,
            )
            .unwrap();
        let (_sha, b_start, b_end, _k) = gc
            .compress(
                &GcKey::fixed(vec![b"b".to_vec()]),
                &[body_b.as_slice()],
                body_b.len(),
                None,
                None,
                None,
            )
            .unwrap();
        // flush() returns the raw concatenated content; wrap it in a block.
        let (content_chunks, content_len) = gc.flush();
        let mut gcb = GroupCompressBlock::new();
        gcb.set_chunked_content(&content_chunks, content_len);
        let block: Vec<u8> = gcb.to_bytes();

        let rm = ReadMemo::new("g".to_string(), 0, block.len() as u64);
        let key_a = gckey(b"a");
        let key_b = gckey(b"b");
        let mut details = std::collections::HashMap::new();
        details.insert(
            key_a.clone(),
            GcBuildDetails {
                index_memo: IndexMemo::new(rm.clone(), a_start as u64, a_end as u64),
                parents: Some(vec![]),
            },
        );
        details.insert(
            key_b.clone(),
            GcBuildDetails {
                index_memo: IndexMemo::new(rm.clone(), b_start as u64, b_end as u64),
                parents: Some(vec![key_a.clone()]),
            },
        );
        let mut blocks = std::collections::HashMap::new();
        blocks.insert(rm, block);
        let access = MockAccess {
            blocks,
            fetched: std::cell::RefCell::new(Vec::new()),
        };
        let vf = GroupCompressVersionedFiles::new(MapIndex { details }, access, true);

        let records = vf
            .get_record_stream(&[key_a.clone(), key_b.clone()], "as-requested")
            .unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].key(), key_a);
        assert_eq!(records[0].to_fulltext().as_ref(), body_a.as_slice());
        assert_eq!(records[1].key(), key_b);
        assert_eq!(records[1].to_fulltext().as_ref(), body_b.as_slice());
        assert_eq!(records[1].parents(), Some(vec![key_a]));
    }

    #[test]
    fn get_record_stream_yields_absent_for_missing_keys() {
        let access = MockAccess {
            blocks: std::collections::HashMap::new(),
            fetched: std::cell::RefCell::new(Vec::new()),
        };
        let vf = GroupCompressVersionedFiles::new(
            MapIndex {
                details: std::collections::HashMap::new(),
            },
            access,
            true,
        );
        let records = vf
            .get_record_stream(&[gckey(b"nope")], "unordered")
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].storage_kind(), "absent");
        assert_eq!(records[0].key(), gckey(b"nope"));
    }

    /// A writable in-memory groupcompress store shared by a `MemIndex` and
    /// a `MemAccess`, so an insert/read round-trip can be exercised purely.
    #[derive(Default)]
    struct MemStore {
        /// Appended blocks, in write order; the read-memo index is the
        /// block's position in this vec, rendered as a string.
        blocks: Vec<Vec<u8>>,
        details: std::collections::HashMap<GcKey, GcBuildDetails<String>>,
    }

    #[derive(Clone)]
    struct MemIndex(std::rc::Rc<std::cell::RefCell<MemStore>>);
    #[derive(Clone)]
    struct MemAccess(std::rc::Rc<std::cell::RefCell<MemStore>>);

    impl GcIndex for MemIndex {
        type F = String;
        fn get_build_details(
            &self,
            keys: &[GcKey],
        ) -> Result<std::collections::HashMap<GcKey, GcBuildDetails<String>>, crate::knit::KnitError>
        {
            let store = self.0.borrow();
            Ok(keys
                .iter()
                .filter_map(|k| store.details.get(k).map(|d| (k.clone(), d.clone())))
                .collect())
        }
        fn get_parent_map(
            &self,
            keys: &[GcKey],
        ) -> Result<std::collections::HashMap<GcKey, Vec<GcKey>>, crate::knit::KnitError> {
            let store = self.0.borrow();
            Ok(keys
                .iter()
                .filter_map(|k| {
                    store
                        .details
                        .get(k)
                        .map(|d| (k.clone(), d.parents.clone().unwrap_or_default()))
                })
                .collect())
        }
        fn keys(&self) -> Result<Vec<GcKey>, crate::knit::KnitError> {
            Ok(self.0.borrow().details.keys().cloned().collect())
        }
        fn has_graph(&self) -> bool {
            true
        }
        fn check_write_ok(&self) -> Result<(), crate::knit::KnitError> {
            Ok(())
        }
        fn add_records(
            &self,
            records: &[(GcKey, IndexMemo<String>, Option<Vec<GcKey>>)],
            _random_id: bool,
        ) -> Result<(), crate::knit::KnitError> {
            let mut store = self.0.borrow_mut();
            for (key, memo, parents) in records {
                store.details.insert(
                    key.clone(),
                    GcBuildDetails {
                        index_memo: memo.clone(),
                        parents: parents.clone(),
                    },
                );
            }
            Ok(())
        }
    }

    impl GcAccess for MemAccess {
        type F = String;
        fn get_raw_records(
            &self,
            memos: &[ReadMemo<String>],
        ) -> Result<Vec<Vec<u8>>, crate::knit::KnitError> {
            let store = self.0.borrow();
            memos
                .iter()
                .map(|m| {
                    let idx: usize = m
                        .index
                        .parse()
                        .map_err(|_| crate::knit::KnitError::Corrupt("bad block index".into()))?;
                    store
                        .blocks
                        .get(idx)
                        .cloned()
                        .ok_or_else(|| crate::knit::KnitError::Corrupt("no such block".into()))
                })
                .collect()
        }
        fn add_raw_record(
            &self,
            _size: usize,
            chunks: Vec<Vec<u8>>,
        ) -> Result<ReadMemo<String>, crate::knit::KnitError> {
            let mut store = self.0.borrow_mut();
            let idx = store.blocks.len();
            let bytes: Vec<u8> = chunks.concat();
            let len = bytes.len() as u64;
            store.blocks.push(bytes);
            Ok(ReadMemo::new(idx.to_string(), 0, len))
        }
    }

    #[test]
    fn insert_record_stream_then_get_record_stream_round_trips() {
        use crate::versionedfile::{ChunkedContentFactory, ContentFactory};

        let store = std::rc::Rc::new(std::cell::RefCell::new(MemStore::default()));
        let vf = GroupCompressVersionedFiles::new(
            MemIndex(store.clone()),
            MemAccess(store.clone()),
            true,
        );

        let key_a = gckey(b"a");
        let key_b = gckey(b"b");
        let text_a = b"the quick brown fox\njumps over\n".to_vec();
        let text_b = b"the lazy dog\nsleeps all day\n".to_vec();
        let stream: Vec<Box<dyn ContentFactory>> = vec![
            Box::new(ChunkedContentFactory::new(
                None,
                key_a.clone(),
                Some(vec![]),
                vec![text_a.clone()],
            )),
            Box::new(ChunkedContentFactory::new(
                None,
                key_b.clone(),
                Some(vec![key_a.clone()]),
                vec![text_b.clone()],
            )),
        ];
        let inserted = vf.insert_record_stream(stream, false).unwrap();
        assert_eq!(inserted.len(), 2);

        // Read the records back: the text must round-trip exactly.
        let records = vf
            .get_record_stream(&[key_a.clone(), key_b.clone()], "as-requested")
            .unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].key(), key_a);
        assert_eq!(records[0].to_fulltext().as_ref(), text_a.as_slice());
        assert_eq!(records[1].key(), key_b);
        assert_eq!(records[1].to_fulltext().as_ref(), text_b.as_slice());
        assert_eq!(records[1].parents(), Some(vec![key_a]));
    }

    #[test]
    fn add_lines_then_get_sha1s_and_iter_lines() {
        let store = std::rc::Rc::new(std::cell::RefCell::new(MemStore::default()));
        let vf = GroupCompressVersionedFiles::new(
            MemIndex(store.clone()),
            MemAccess(store.clone()),
            true,
        );
        let key = gckey(b"v1");
        let lines = vec![b"alpha line\n".to_vec(), b"beta line\n".to_vec()];
        let (sha1, length) = vf
            .add_lines(key.clone(), Some(vec![]), lines.clone())
            .unwrap();
        assert_eq!(sha1, crate::weave::sha_strings(&lines));
        assert_eq!(length, lines.iter().map(|l| l.len()).sum::<usize>());

        // get_sha1s returns the same digest.
        let sha1s = vf.get_sha1s(&[key.clone()]).unwrap();
        assert_eq!(sha1s.get(&key), Some(&sha1));

        // iter_lines yields each line paired with the key.
        let iter_lines = vf
            .iter_lines_added_or_present_in_keys(&[key.clone()])
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            iter_lines,
            vec![
                (b"alpha line\n".to_vec(), key.clone()),
                (b"beta line\n".to_vec(), key.clone()),
            ]
        );

        // check passes over a sound store.
        vf.check().unwrap();
    }
}

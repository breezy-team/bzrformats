use crate::inventory::Entry as InventoryEntry;
use crate::FileId;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
#[cfg(test)]
use std::fs::Metadata;
#[cfg(all(unix, test))]
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;

mod sha1;
pub use sha1::{DefaultSHA1Provider, SHA1Provider};

mod pack_stat;
pub use pack_stat::{pack_stat, pack_stat_metadata, stat_to_minikind};

mod path;
pub use path::{bisect_path_left, bisect_path_right, lt_by_dirs, lt_path_by_dirblock};

mod header;
pub use header::{
    fields_per_entry, get_ghosts_line, get_output_lines, get_parents_line, read_header, Header,
    HeaderError, BISECT_PAGE_SIZE, HEADER_FORMAT_2, HEADER_FORMAT_3,
};

mod kind;
pub use kind::{Kind, OptionKindExt};

mod entry;
pub use entry::{Dirblock, Entry, EntryKey, LockState, MemoryState, TreeData, YesNo};

mod id_index;
pub use id_index::{inv_entry_to_details, IdIndex};

mod iter_changes;
use iter_changes::IterPhase;
pub use iter_changes::{
    DirstateChange, IterChangesIter, ProcessEntryError, ProcessEntryState, ProcessPathInfo,
};

fn null_parent_details() -> TreeData {
    TreeData {
        minikind: Kind::Absent,
        fingerprint: Vec::new(),
        size: 0,
        executable: false,
        packed_stat: Vec::new(),
    }
}

fn join_path(dirname: &[u8], basename: &[u8]) -> Vec<u8> {
    if dirname.is_empty() {
        basename.to_vec()
    } else {
        let mut p = dirname.to_vec();
        p.push(b'/');
        p.extend_from_slice(basename);
        p
    }
}

/// Is `candidate` inside `parent` (or equal to it)?  Mirrors
/// `osutils.is_inside`: `parent` is the prefix directory, `candidate`
/// is the potentially-nested path.
fn is_inside(parent: &[u8], candidate: &[u8]) -> bool {
    if parent == candidate {
        return true;
    }
    if parent.is_empty() {
        return true;
    }
    candidate.len() > parent.len()
        && candidate.starts_with(parent)
        && candidate[parent.len()] == b'/'
}

#[allow(clippy::too_many_arguments)]
fn resolve_parent_id(
    dirblocks: &[Dirblock],
    old_dirname: &[u8],
    old_basename: &[u8],
    entry_file_id: &[u8],
    source_index: usize,
    old_dirname_to_file_id: &std::collections::HashMap<Vec<u8>, Vec<u8>>,
    last_source_parent: &mut Option<(Vec<u8>, Option<Vec<u8>>)>,
) -> Option<Vec<u8>> {
    if !old_basename.is_empty()
        && last_source_parent
            .as_ref()
            .map(|(d, _)| d.as_slice() == old_dirname)
            .unwrap_or(false)
    {
        return last_source_parent.as_ref().and_then(|(_, id)| id.clone());
    }
    let cached = old_dirname_to_file_id.get(old_dirname).cloned();
    let pid_raw = match cached {
        Some(v) => Some(v),
        None => {
            let (pdir, pbase) = split_path_utf8(old_dirname);
            let bei = get_block_entry_index(dirblocks, pdir, pbase, source_index);
            if bei.path_present {
                Some(
                    dirblocks[bei.block_index].entries[bei.entry_index]
                        .key
                        .file_id
                        .clone(),
                )
            } else {
                None
            }
        }
    };
    match pid_raw {
        Some(v) if v == entry_file_id => None,
        Some(v) => {
            *last_source_parent = Some((old_dirname.to_vec(), Some(v.clone())));
            Some(v)
        }
        None => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn resolve_target_parent_id(
    dirblocks: &[Dirblock],
    new_dirname: &[u8],
    new_basename: &[u8],
    entry_file_id: &[u8],
    target_index: usize,
    new_dirname_to_file_id: &std::collections::HashMap<Vec<u8>, Vec<u8>>,
    last_target_parent: &mut Option<(Vec<u8>, Option<Vec<u8>>)>,
) -> Result<Option<Vec<u8>>, ProcessEntryError> {
    if !new_basename.is_empty()
        && last_target_parent
            .as_ref()
            .map(|(d, _)| d.as_slice() == new_dirname)
            .unwrap_or(false)
    {
        return Ok(last_target_parent.as_ref().and_then(|(_, id)| id.clone()));
    }
    let cached = new_dirname_to_file_id.get(new_dirname).cloned();
    let pid_raw = match cached {
        Some(v) => Some(v),
        None => {
            let (pdir, pbase) = split_path_utf8(new_dirname);
            let bei = get_block_entry_index(dirblocks, pdir, pbase, target_index);
            if bei.path_present {
                Some(
                    dirblocks[bei.block_index].entries[bei.entry_index]
                        .key
                        .file_id
                        .clone(),
                )
            } else {
                return Err(ProcessEntryError::Internal(format!(
                    "Could not find target parent in wt: {:?}",
                    new_dirname
                )));
            }
        }
    };
    match pid_raw {
        Some(v) if v == entry_file_id => Ok(None),
        Some(v) => {
            *last_target_parent = Some((new_dirname.to_vec(), Some(v.clone())));
            Ok(Some(v))
        }
        None => Ok(None),
    }
}

/// Return the last path component (utf8 bytes) of `path`.  Matches
/// `osutils.splitpath(path)[-1]` — the basename of a path.
fn splitpath_last(path: &[u8]) -> Vec<u8> {
    match path.iter().rposition(|&b| b == b'/') {
        Some(i) => path[i + 1..].to_vec(),
        None => path.to_vec(),
    }
}

/// Build a `ProcessPathInfo` for `path_utf8`, or `None` when the path
/// does not exist on disk.  Mirrors Python's `_path_info` helper on
/// `ProcessEntryPython`.
fn compute_path_info(
    pstate: &ProcessEntryState,
    transport: &dyn Transport,
    path_utf8: &[u8],
) -> Result<Option<ProcessPathInfo>, ProcessEntryError> {
    let abspath = join_path(&pstate.root_abspath, path_utf8);
    let stat = match transport.lstat(&abspath) {
        Ok(s) => s,
        Err(_) => return Ok(None),
    };
    let mut kind = if stat.is_file() {
        Some(osutils::Kind::File)
    } else if stat.is_dir() {
        Some(osutils::Kind::Directory)
    } else if stat.is_symlink() {
        Some(osutils::Kind::Symlink)
    } else {
        None
    };
    // The tree root itself is never a tree-reference (mirrors Python's
    // `_directory_may_be_tree_reference`: `return relpath and ...`).
    if kind == Some(osutils::Kind::Directory)
        && pstate.supports_tree_reference
        && !path_utf8.is_empty()
    {
        let is_ref = transport.is_tree_reference_dir(&abspath).map_err(|e| {
            ProcessEntryError::Internal(format!(
                "is_tree_reference_dir({}): {}",
                String::from_utf8_lossy(&abspath),
                e
            ))
        })?;
        if is_ref {
            kind = Some(osutils::Kind::TreeReference);
        }
    }
    Ok(Some(ProcessPathInfo {
        abspath,
        kind,
        stat,
    }))
}

/// Update `seen_ids` + `search_specific_file_parents` from a
/// just-emitted `DirstateChange`.  Mirrors Python's
/// `_gather_result_for_consistency`.
fn gather_result_for_consistency(pstate: &mut ProcessEntryState, change: &DirstateChange) {
    if !pstate.partial || change.file_id.is_empty() {
        return;
    }
    pstate.seen_ids.insert(change.file_id.clone());
    if let Some(ref new_path) = change.new_path {
        if !new_path.is_empty() {
            // Queue every ancestor directory, plus the root.
            let mut path = new_path.clone();
            while let Some(i) = path.iter().rposition(|&b| b == b'/') {
                path.truncate(i);
                pstate.search_specific_file_parents.insert(path.clone());
            }
            pstate.search_specific_file_parents.insert(Vec::new());
        }
    }
}

mod transport;
pub use transport::{DirEntryInfo, StatInfo, Transport, TransportError};

mod walker;
pub use walker::{WalkDirsUtf8, WalkedDir};

mod parser;
pub use parser::{dirblocks_to_entry_lines, entry_to_line, parse_dirblocks, DirblocksError};

/// In-memory `DirState`, the Rust counterpart to `bzrformats.dirstate.DirState`.
///
/// This commit introduces the struct and a constructor mirroring Python's
/// `__init__`. Behaviour (reading, writing, entry lookup, change processing)
/// is added in follow-up commits; for now the struct is a passive container
/// so later ports have a stable place to hang methods.
pub struct DirState {
    /// Path to the dirstate file on disk (Python's `_filename`).
    pub filename: PathBuf,
    /// Provider used to compute sha1s and stat+sha1 tuples for working-tree
    /// files. Boxed so callers can swap in an alternate implementation for
    /// testing, matching Python's `_sha1_provider` attribute.
    pub sha1_provider: Box<dyn SHA1Provider + Send + Sync>,
    /// State of the header (`NotInMemory` until `_read_header` runs).
    pub header_state: MemoryState,
    /// State of the per-row dirblock data.
    pub dirblock_state: MemoryState,
    /// If an error was detected while updating the dirstate we refuse to
    /// write it back. Mirrors Python's `_changes_aborted` flag.
    pub changes_aborted: bool,
    /// The in-memory dirblocks, sorted by dirname. Python stores this as
    /// `[(dirname, [entry, ...])]` in `_dirblocks`.
    pub dirblocks: Vec<Dirblock>,
    /// Ghost parent revision ids: parents that are referenced but not
    /// present locally.
    pub ghosts: Vec<Vec<u8>>,
    /// Parent revision ids for the current tree, in order. The first entry
    /// is the current parent; subsequent entries are merged parents.
    pub parents: Vec<Vec<u8>>,
    /// Offset in `filename` where the header ends and the dirblock text
    /// begins, populated after the header has been parsed.
    pub end_of_header: Option<u64>,
    /// Cutoff mtime/ctime for trusting cached sha1s. `None` until
    /// `_sha_cutoff_time` has been computed for the current `now`.
    pub cutoff_time: Option<i64>,
    /// Declared entry count from the header, or `None` before the header is
    /// read. Used to validate the dirblock parse.
    pub num_entries: usize,
    /// Current read/write lock state.
    pub lock_state: Option<LockState>,
    /// Set of keys whose hash is known to have changed since load. Used by
    /// `_mark_modified` to decide whether a save is worthwhile.
    pub known_hash_changes: HashSet<EntryKey>,
    /// Below this many hash-only changes a save is skipped.
    /// `-1` means *never* save hash changes; `0` means always save them.
    pub worth_saving_limit: i64,
    /// Call `fdatasync` after writing the state file if true.
    pub fdatasync: bool,
    /// Trust the filesystem's executable bit when building tree data.
    pub use_filesystem_for_exec: bool,
    /// Bisect chunk size when reading the state file in pages; mirrors
    /// `_bisect_page_size`.
    pub bisect_page_size: usize,
    /// Lazily-populated index of `file_id → [(dirname, basename, file_id)]`.
    /// `None` until [`DirState::get_or_build_id_index`] is called, at
    /// which point it is rebuilt from the current `dirblocks`.
    /// Invalidate by setting to `None` whenever dirblocks change.
    pub id_index: Option<IdIndex>,
    /// Lazily-populated index of `packed_stat → sha1` for every file
    /// entry in tree 0. `None` until [`DirState::get_or_build_packed_stat_index`]
    /// is called, mirroring Python's `_packed_stat_index` attribute.
    /// Invalidate by setting to `None` whenever tree-0 entries change.
    pub packed_stat_index: Option<HashMap<Vec<u8>, Vec<u8>>>,
}

impl DirState {
    /// Create a new, empty `DirState` object.
    ///
    /// The returned state has no data loaded from disk — `header_state` and
    /// `dirblock_state` are both `NotInMemory`. Call a future `load` method
    /// to populate it. This mirrors the Python constructor at
    /// `bzrformats/dirstate.py` `DirState.__init__`.
    pub fn new<P: Into<PathBuf>>(
        path: P,
        sha1_provider: Box<dyn SHA1Provider + Send + Sync>,
        worth_saving_limit: i64,
        use_filesystem_for_exec: bool,
        fdatasync: bool,
    ) -> Self {
        DirState {
            filename: path.into(),
            sha1_provider,
            header_state: MemoryState::NotInMemory,
            dirblock_state: MemoryState::NotInMemory,
            changes_aborted: false,
            dirblocks: Vec::new(),
            ghosts: Vec::new(),
            parents: Vec::new(),
            end_of_header: None,
            cutoff_time: None,
            num_entries: 0,
            lock_state: None,
            known_hash_changes: HashSet::new(),
            worth_saving_limit,
            fdatasync,
            use_filesystem_for_exec,
            bisect_page_size: BISECT_PAGE_SIZE,
            id_index: None,
            packed_stat_index: None,
        }
    }

    /// Yield a reference to every entry across every dirblock, in
    /// dirblock order. Mirrors Python's `_iter_entries` in the simple
    /// case (without the implicit `_read_dirblocks_if_needed` —
    /// callers are expected to have populated `dirblocks` already).
    pub fn iter_entries(&self) -> impl Iterator<Item = &Entry> {
        self.dirblocks.iter().flat_map(|b| b.entries.iter())
    }

    /// Build an [`IdIndex`] from the current dirblocks. Pure — no
    /// cache interaction; callers that want Python's cached behaviour
    /// should use [`DirState::get_or_build_id_index`] instead.
    pub fn build_id_index(&self) -> IdIndex {
        let mut idx = IdIndex::new();
        for entry in self.iter_entries() {
            let file_id = FileId::from(&entry.key.file_id);
            idx.add((
                entry.key.dirname.as_slice(),
                entry.key.basename.as_slice(),
                &file_id,
            ));
        }
        idx
    }

    /// Return a reference to the cached [`IdIndex`], rebuilding it
    /// from `self.dirblocks` on first call after the cache was last
    /// invalidated. Mirrors Python's `DirState._get_id_index`.
    ///
    /// The cache lives in `self.id_index`; any code that mutates
    /// `self.dirblocks` must set `self.id_index = None` afterwards to
    /// force a rebuild on the next access.
    pub fn get_or_build_id_index(&mut self) -> &IdIndex {
        if self.id_index.is_none() {
            self.id_index = Some(self.build_id_index());
        }
        self.id_index.as_ref().unwrap()
    }

    /// Rebuild the `packed_stat → sha1` map from every tree-0 file
    /// entry. Pure — no cache interaction.
    pub fn build_packed_stat_index(&self) -> HashMap<Vec<u8>, Vec<u8>> {
        let mut index: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        for entry in self.iter_entries() {
            let tree0 = match entry.trees.first() {
                Some(t) => t,
                None => continue,
            };
            if tree0.minikind == Kind::File {
                // Python stores the mapping keyed by the packed_stat
                // and with the fingerprint (the sha1) as the value.
                index.insert(tree0.packed_stat.clone(), tree0.fingerprint.clone());
            }
        }
        index
    }

    /// Return a reference to the cached `packed_stat → sha1` map,
    /// rebuilding it on first call after the cache was last
    /// invalidated. Mirrors Python's `DirState._get_packed_stat_index`.
    ///
    /// The cache lives in `self.packed_stat_index`; any code that
    /// mutates tree-0 file entries must set `self.packed_stat_index =
    /// None` afterwards to force a rebuild on the next access.
    pub fn get_or_build_packed_stat_index(&mut self) -> &HashMap<Vec<u8>, Vec<u8>> {
        if self.packed_stat_index.is_none() {
            self.packed_stat_index = Some(self.build_packed_stat_index());
        }
        self.packed_stat_index.as_ref().unwrap()
    }

    /// Parse the header of the dirstate file from `data` and populate the
    /// in-memory fields that Python's `_read_header` would populate.
    ///
    /// `data` must contain the full dirstate file contents (or at minimum
    /// enough bytes to cover the header); this mirrors Python's
    /// `state_file.readline()` loop operating on a buffered file. On
    /// success the `parents`, `ghosts`, `num_entries`, and `end_of_header`
    /// fields are set and `header_state` transitions to
    /// `InMemoryUnmodified`.
    pub fn read_header(&mut self, data: &[u8]) -> Result<(), HeaderError> {
        let header = read_header(data)?;
        self.parents = header.parents;
        self.ghosts = header.ghosts;
        self.num_entries = header.num_entries;
        self.end_of_header = Some(header.end_of_header as u64);
        self.header_state = MemoryState::InMemoryUnmodified;
        Ok(())
    }

    /// Split `self.dirblocks[0]` — which the parser fills with *both* root
    /// entries and contents-of-root entries — into the two sentinel
    /// blocks Python's `_read_dirblocks` / `_split_root_dirblock_into_contents`
    /// produces: block 0 holds entries whose basename is empty (the root
    /// itself and any parent-tree variants), and block 1 holds the rest.
    ///
    /// Returns an error if the layout does not match the expected
    /// post-parse shape (fewer than two blocks, or block 1 is not the
    /// empty sentinel).
    pub fn split_root_dirblock_into_contents(&mut self) -> Result<(), SplitRootError> {
        split_root_dirblock_into_contents(&mut self.dirblocks)
    }

    /// Locate the block for a given key. Mirrors
    /// `DirState._find_block_index_from_key`, without the
    /// `_last_block_index` / `_split_path_cache` memoisation layers
    /// (those live on the Python object and are a follow-up port).
    pub fn find_block_index_from_key(&self, key: &EntryKey) -> (usize, bool) {
        find_block_index_from_key(&self.dirblocks, key)
    }

    /// Locate the entry index for a key within a block. Mirrors
    /// `DirState._find_entry_index`, in the simpler uncached form.
    pub fn find_entry_index(&self, key: &EntryKey, block: &[Entry]) -> (usize, bool) {
        find_entry_index(key, block)
    }

    /// Look up a `(dirname, basename)` path in the given tree. Mirrors
    /// `DirState._get_block_entry_index`.
    pub fn get_block_entry_index(
        &self,
        dirname: &[u8],
        basename: &[u8],
        tree_index: usize,
    ) -> BlockEntryIndex {
        get_block_entry_index(&self.dirblocks, dirname, basename, tree_index)
    }

    /// Serialise the in-memory state to the byte chunks that make up the
    /// on-disk file. Mirrors Python's `DirState.get_lines` for the
    /// common "we have in-memory data to write" branch; it does not
    /// handle the fast-path shortcut that re-reads an unmodified file
    /// from disk (that shortcut belongs on the soon-to-be-ported
    /// `save` method).
    pub fn get_lines(&self) -> Vec<Vec<u8>> {
        let parents_refs: Vec<&[u8]> = self.parents.iter().map(|p| p.as_slice()).collect();
        let ghosts_refs: Vec<&[u8]> = self.ghosts.iter().map(|g| g.as_slice()).collect();
        let parents_line = get_parents_line(&parents_refs);
        let ghosts_line = get_ghosts_line(&ghosts_refs);

        let entry_lines = dirblocks_to_entry_lines(&self.dirblocks);

        // Build the owned-backing-store buffer, then borrow slices into
        // it when calling `get_output_lines`.
        let mut owned: Vec<Vec<u8>> = Vec::with_capacity(2 + entry_lines.len());
        owned.push(parents_line);
        owned.push(ghosts_line);
        owned.extend(entry_lines);
        let borrowed: Vec<&[u8]> = owned.iter().map(|l| l.as_slice()).collect();
        get_output_lines(borrowed)
    }

    /// Mark the dirstate as modified. Mirrors Python's
    /// `DirState._mark_modified`.
    ///
    /// If `hash_changed_entries` is non-empty, only the hash cache is
    /// affected: the provided entry keys are added to
    /// `known_hash_changes` and the `dirblock_state` transitions from
    /// `NotInMemory`/`InMemoryUnmodified` into `InMemoryHashModified`
    /// (a full `InMemoryModified` state takes precedence and is not
    /// downgraded).
    ///
    /// If `hash_changed_entries` is empty the whole dirblock state is
    /// considered dirty: `dirblock_state` becomes `InMemoryModified`
    /// regardless of its previous value. `header_modified` is an
    /// orthogonal flag that promotes `header_state` to
    /// `InMemoryModified` as well.
    pub fn mark_modified(&mut self, hash_changed_entries: &[EntryKey], header_modified: bool) {
        if !hash_changed_entries.is_empty() {
            for key in hash_changed_entries {
                self.known_hash_changes.insert(key.clone());
            }
            if matches!(
                self.dirblock_state,
                MemoryState::NotInMemory | MemoryState::InMemoryUnmodified
            ) {
                self.dirblock_state = MemoryState::InMemoryHashModified;
            }
        } else {
            self.dirblock_state = MemoryState::InMemoryModified;
        }
        if header_modified {
            self.header_state = MemoryState::InMemoryModified;
        }
    }

    /// Mark the dirstate as unmodified — both header and dirblock state
    /// return to `InMemoryUnmodified` and the hash-change set is
    /// cleared. Mirrors Python's `DirState._mark_unmodified`.
    pub fn mark_unmodified(&mut self) {
        self.header_state = MemoryState::InMemoryUnmodified;
        self.dirblock_state = MemoryState::InMemoryUnmodified;
        self.known_hash_changes.clear();
    }

    /// Replace the entire in-memory state with `parent_ids` and
    /// `dirblocks`, marking both the header and the dirblock data
    /// fully modified. Mirrors Python's `DirState._set_data`: the
    /// caller owns any sort/shape invariants on `dirblocks`; this
    /// method does not validate them.
    ///
    /// Any cached `id_index` is invalidated. Python's
    /// `_packed_stat_index` has no equivalent on the Rust struct yet
    /// and is therefore not touched here.
    pub fn set_data(&mut self, parent_ids: Vec<Vec<u8>>, dirblocks: Vec<Dirblock>) {
        self.dirblocks = dirblocks;
        self.mark_modified(&[], true);
        self.parents = parent_ids;
        self.id_index = None;
        self.packed_stat_index = None;
    }

    /// Overwrite the tree-0 slot of the entry at `key` with the given
    /// details. Returns an error if `key` is not present; otherwise
    /// does no other bookkeeping — no id_index changes, no cross-ref
    /// rewrites, no state bump. This is the narrow primitive the
    /// `py_update_entry` hash-refresh path needs: callers that want
    /// structural changes should use [`DirState::update_minimal`] or
    /// [`DirState::add`].
    pub fn set_tree0(&mut self, key: &EntryKey, details: TreeData) -> Result<(), MakeAbsentError> {
        let (block_index, block_present) = find_block_index_from_key(&self.dirblocks, key);
        if !block_present {
            return Err(MakeAbsentError::BlockNotFound { key: key.clone() });
        }
        let (entry_index, entry_present) =
            find_entry_index(key, &self.dirblocks[block_index].entries);
        if !entry_present {
            return Err(MakeAbsentError::EntryNotFound { key: key.clone() });
        }
        self.dirblocks[block_index].entries[entry_index].trees[0] = details;
        self.packed_stat_index = None;
        Ok(())
    }

    /// Return the live tree-0 minikind for `key`, or `None` when no
    /// entry with that key is present. Used by callers that need to
    /// refresh a stale snapshot against current dirblock contents
    /// (notably `set_state_from_inventory`'s zipper-merge loop, which
    /// used to rely on Python-side tuple aliasing to observe mid-loop
    /// rewrites).
    pub fn tree0_minikind(&self, key: &EntryKey) -> Option<Kind> {
        let (block_index, block_present) = find_block_index_from_key(&self.dirblocks, key);
        if !block_present {
            return None;
        }
        let (entry_index, entry_present) =
            find_entry_index(key, &self.dirblocks[block_index].entries);
        if !entry_present {
            return None;
        }
        self.dirblocks[block_index].entries[entry_index]
            .trees
            .first()
            .map(|t| t.minikind)
    }

    /// Record an observed sha1 for `key`'s tree-0 row when the file's
    /// stat falls in the cacheable window.  Mirrors Python's
    /// `DirState._observed_sha1`: silently ignores non-file kinds and
    /// files whose mtime/ctime land after the cutoff.
    ///
    /// Takes the stat fields unpacked so callers can feed in whichever
    /// shape they already have (Python's `os.stat_result`, Rust's
    /// [`Metadata`], synthetic fixture data).
    /// Record the observed sha1 for the entry at `key` and return the
    /// new tree-0 `TreeData` so callers that hold a mirror of the
    /// entry row (e.g. Python tuple) can write it back in place
    /// without a second lookup.
    ///
    /// Returns `Ok(None)` when no update happened — non-regular-file,
    /// or the stat falls inside the uncacheable window.
    #[allow(clippy::too_many_arguments)]
    pub fn observed_sha1(
        &mut self,
        key: &EntryKey,
        sha1: &[u8],
        st_mode: u32,
        st_size: u64,
        st_mtime: i64,
        st_ctime: i64,
        st_dev: u64,
        st_ino: u64,
    ) -> Result<Option<TreeData>, UpdateEntryError> {
        use std::time::{SystemTime, UNIX_EPOCH};

        // S_IFREG (0o100000) after masking with S_IFMT.
        if (st_mode & 0o170000) != 0o100000 {
            return Ok(None);
        }

        let now_secs: i64 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let cutoff: i64 = self.cutoff_time.unwrap_or_else(|| {
            let c = now_secs - 3;
            self.cutoff_time = Some(c);
            c
        });

        if st_mtime >= cutoff || st_ctime >= cutoff {
            return Ok(None);
        }

        let (block_index, block_present) = find_block_index_from_key(&self.dirblocks, key);
        if !block_present {
            return Err(UpdateEntryError::EntryNotFound);
        }
        let (entry_index, entry_present) =
            find_entry_index(key, &self.dirblocks[block_index].entries);
        if !entry_present {
            return Err(UpdateEntryError::EntryNotFound);
        }
        let executable = self.dirblocks[block_index].entries[entry_index].trees[0].executable;
        let packed_stat = pack_stat(
            st_size,
            st_mtime as u64,
            st_ctime as u64,
            st_dev,
            st_ino,
            st_mode,
        )
        .into_bytes();
        let new_tree0 = TreeData {
            minikind: Kind::File,
            fingerprint: sha1.to_vec(),
            size: st_size,
            executable,
            packed_stat,
        };
        self.dirblocks[block_index].entries[entry_index].trees[0] = new_tree0.clone();
        self.packed_stat_index = None;
        self.mark_modified(std::slice::from_ref(key), false);
        Ok(Some(new_tree0))
    }

    /// Refresh the tree-0 slot of `key` from the filesystem.  Mirrors
    /// Python's `py_update_entry`: if the stat hasn't changed since
    /// the last time we saved, re-use the cached link-or-sha1;
    /// otherwise read the file (or symlink) and rewrite the tree-0
    /// slot.  Returns the sha1 hex or symlink target, or `None` when
    /// the on-disk kind is not supported (e.g. block/char devices),
    /// when the row is a directory and the cached stat matches
    /// (nothing to report), or when we skip the sha because the
    /// Compare one dirstate entry against what's on disk (or nothing,
    /// if the path is absent in the target) and yield a
    /// [`DirstateChange`] describing any differences.  Ports Python's
    /// `ProcessEntryPython._process_entry`.
    ///
    /// Returns `(None, None)` when the entry is uninteresting (no row
    /// in either side of the comparison), `(None, Some(false))` when
    /// both sides match and `pstate.include_unchanged` is off,
    /// `(Some(change), Some(true))` for a real change, and
    /// `(Some(change), Some(false))` for an unchanged-but-included
    /// report.
    pub fn process_entry(
        &mut self,
        pstate: &mut ProcessEntryState,
        entry_key: &EntryKey,
        entry_trees: &[TreeData],
        path_info: Option<&ProcessPathInfo>,
        transport: &dyn Transport,
    ) -> Result<(Option<DirstateChange>, Option<bool>), ProcessEntryError> {
        let source_details: TreeData = if let Some(idx) = pstate.source_index {
            entry_trees
                .get(idx)
                .cloned()
                .unwrap_or_else(null_parent_details)
        } else {
            null_parent_details()
        };
        let target_idx = pstate.target_index;
        let mut target_details: TreeData = entry_trees
            .get(target_idx)
            .cloned()
            .unwrap_or_else(null_parent_details);
        let mut target_minikind = target_details.minikind;

        // Step 1: if on disk and versioned in the target, refresh
        // via update_entry (which may flip minikind e.g. d → t).
        let mut link_or_sha1: Option<Vec<u8>> = None;
        if let Some(info) = path_info {
            if target_minikind.is_fdlt() {
                if target_idx != 0 {
                    return Err(ProcessEntryError::Internal(
                        "update_entry requires target_index == 0".into(),
                    ));
                }
                link_or_sha1 = self
                    .update_entry(entry_key, &info.abspath, &info.stat, transport)
                    .map_err(|e| ProcessEntryError::Internal(format!("update_entry: {}", e)))?;
                let (bi, _) = find_block_index_from_key(&self.dirblocks, entry_key);
                let (ei, _) = find_entry_index(entry_key, &self.dirblocks[bi].entries);
                target_details = self.dirblocks[bi].entries[ei].trees[target_idx].clone();
                target_minikind = target_details.minikind;
            }
        }

        let file_id = entry_key.file_id.clone();
        let mut source_minikind = source_details.minikind;
        let mut source_details_mut = source_details.clone();

        if source_minikind.is_fdltr() && target_minikind.is_fdlt() {
            let old_dirname: Vec<u8>;
            let old_basename: Vec<u8>;
            let mut old_path: Option<Vec<u8>>;
            let mut path: Option<Vec<u8>>;

            if source_minikind == Kind::Relocated {
                let src_path = source_details_mut.fingerprint.clone();
                let already_inside = pstate
                    .searched_specific_files
                    .iter()
                    .any(|p| is_inside(p.as_slice(), &src_path));
                if !already_inside {
                    pstate.search_specific_files.insert(src_path.clone());
                }
                old_path = Some(src_path.clone());
                let (od, ob) = split_path_utf8(&src_path);
                old_dirname = od.to_vec();
                old_basename = ob.to_vec();
                path = Some(join_path(&entry_key.dirname, &entry_key.basename));

                let src_idx = pstate.source_index.ok_or_else(|| {
                    ProcessEntryError::Internal("relocation with no source_index".into())
                })?;
                let bei =
                    get_block_entry_index(&self.dirblocks, &old_dirname, &old_basename, src_idx);
                let src = if bei.path_present {
                    self.dirblocks[bei.block_index].entries[bei.entry_index]
                        .trees
                        .get(src_idx)
                        .cloned()
                } else {
                    None
                };
                let src = src.ok_or_else(|| {
                    ProcessEntryError::DirstateCorrupt(format!(
                        "entry '{}/{}' is considered renamed from {:?} but source does not exist",
                        String::from_utf8_lossy(&entry_key.dirname),
                        String::from_utf8_lossy(&entry_key.basename),
                        src_path,
                    ))
                })?;
                source_details_mut = src;
                source_minikind = source_details_mut.minikind;
            } else {
                old_dirname = entry_key.dirname.clone();
                old_basename = entry_key.basename.clone();
                old_path = None;
                path = None;
            }

            let (content_change, target_kind, target_exec) = if let Some(info) = path_info {
                // Walker reports `kind = None` for fifo / socket /
                // block / char device — kinds dirstate can't track
                // and that we must not try to sha1 (opening a fifo
                // for reading blocks).  Surface
                // `BadFileKindError` to callers; mirrors how the
                // original Python dirstate fails out via
                // `entry_factory[kind]` lookup.
                let target_kind = info.kind.ok_or_else(|| ProcessEntryError::BadFileKind {
                    path: info.abspath.clone(),
                    mode: info.stat.mode,
                })?;
                match target_kind {
                    osutils::Kind::Directory => {
                        if path.is_none() {
                            let p = join_path(&old_dirname, &old_basename);
                            path = Some(p.clone());
                            old_path = Some(p);
                        }
                        if let Some(p) = path.as_ref() {
                            pstate
                                .new_dirname_to_file_id
                                .insert(p.clone(), file_id.clone());
                        }
                        (
                            source_minikind != Kind::Directory,
                            Some(osutils::Kind::Directory),
                            false,
                        )
                    }
                    osutils::Kind::File => {
                        let cc = if source_minikind != Kind::File {
                            true
                        } else {
                            if link_or_sha1.is_none() {
                                let path_buf = bytes_to_path(&info.abspath);
                                let sha = self.sha1_provider.sha1(&path_buf).map_err(|e| {
                                    ProcessEntryError::Internal(format!("sha1: {}", e))
                                })?;
                                let sha_bytes = sha.as_bytes().to_vec();
                                let _ = self.observed_sha1(
                                    entry_key,
                                    &sha_bytes,
                                    info.stat.mode,
                                    info.stat.size,
                                    info.stat.mtime,
                                    info.stat.ctime,
                                    info.stat.dev,
                                    info.stat.ino,
                                );
                                link_or_sha1 = Some(sha_bytes);
                            }
                            link_or_sha1.as_deref()
                                != Some(source_details_mut.fingerprint.as_slice())
                        };
                        let te = if self.use_filesystem_for_exec {
                            (info.stat.mode & 0o100) != 0
                        } else {
                            target_details.executable
                        };
                        (cc, Some(osutils::Kind::File), te)
                    }
                    osutils::Kind::Symlink => {
                        let cc = if source_minikind != Kind::Symlink {
                            true
                        } else {
                            link_or_sha1.as_deref()
                                != Some(source_details_mut.fingerprint.as_slice())
                        };
                        (cc, Some(osutils::Kind::Symlink), false)
                    }
                    osutils::Kind::TreeReference => (
                        source_minikind != Kind::TreeReference,
                        Some(osutils::Kind::TreeReference),
                        false,
                    ),
                }
            } else {
                (true, None, false)
            };

            if source_minikind == Kind::Directory {
                if path.is_none() {
                    let p = join_path(&old_dirname, &old_basename);
                    path = Some(p.clone());
                    old_path = Some(p);
                }
                if let Some(op) = old_path.as_ref() {
                    pstate
                        .old_dirname_to_file_id
                        .insert(op.clone(), file_id.clone());
                }
            }

            let source_parent_id = resolve_parent_id(
                &self.dirblocks,
                &old_dirname,
                &old_basename,
                &entry_key.file_id,
                pstate.source_index.unwrap_or(0),
                &pstate.old_dirname_to_file_id,
                &mut pstate.last_source_parent,
            );
            let target_parent_id = resolve_target_parent_id(
                &self.dirblocks,
                &entry_key.dirname,
                &entry_key.basename,
                &entry_key.file_id,
                target_idx,
                &pstate.new_dirname_to_file_id,
                &mut pstate.last_target_parent,
            )?;

            let source_exec = source_details_mut.executable;
            let changed = content_change
                || source_parent_id != target_parent_id
                || old_basename != entry_key.basename
                || source_exec != target_exec;

            if !changed && !pstate.include_unchanged {
                return Ok((None, Some(false)));
            }

            let (old_path_out, path_out) = match old_path {
                Some(ref op) => (op.clone(), path.clone().unwrap_or_else(|| op.clone())),
                None => {
                    let p = join_path(&old_dirname, &old_basename);
                    (p.clone(), p)
                }
            };

            return Ok((
                Some(DirstateChange {
                    file_id: entry_key.file_id.clone(),
                    old_path: Some(old_path_out),
                    new_path: Some(path_out),
                    content_change,
                    old_versioned: true,
                    new_versioned: true,
                    source_parent_id,
                    target_parent_id,
                    old_basename: Some(old_basename),
                    new_basename: Some(entry_key.basename.clone()),
                    source_kind: source_minikind.to_osutils_kind(),
                    target_kind,
                    source_exec: Some(source_exec),
                    target_exec: Some(target_exec),
                }),
                Some(changed),
            ));
        }

        if source_minikind == Kind::Absent && target_minikind.is_fdlt() {
            let path = join_path(&entry_key.dirname, &entry_key.basename);
            let (parent_dir, parent_base) = split_path_utf8(&entry_key.dirname);
            let parent_bei =
                get_block_entry_index(&self.dirblocks, parent_dir, parent_base, target_idx);
            let parent_id: Option<Vec<u8>> = if parent_bei.path_present {
                let pid = self.dirblocks[parent_bei.block_index].entries[parent_bei.entry_index]
                    .key
                    .file_id
                    .clone();
                (pid != entry_key.file_id).then_some(pid)
            } else {
                None
            };
            if let Some(info) = path_info {
                let te = if self.use_filesystem_for_exec {
                    (info.stat.mode & 0o170000 == 0o100000) && (info.stat.mode & 0o100) != 0
                } else {
                    target_details.executable
                };
                return Ok((
                    Some(DirstateChange {
                        file_id: entry_key.file_id.clone(),
                        old_path: None,
                        new_path: Some(path),
                        content_change: true,
                        old_versioned: false,
                        new_versioned: true,
                        source_parent_id: None,
                        target_parent_id: parent_id,
                        old_basename: None,
                        new_basename: Some(entry_key.basename.clone()),
                        source_kind: None,
                        target_kind: info.kind,
                        source_exec: None,
                        target_exec: Some(te),
                    }),
                    Some(true),
                ));
            } else {
                return Ok((
                    Some(DirstateChange {
                        file_id: entry_key.file_id.clone(),
                        old_path: None,
                        new_path: Some(path),
                        content_change: false,
                        old_versioned: false,
                        new_versioned: true,
                        source_parent_id: None,
                        target_parent_id: parent_id,
                        old_basename: None,
                        new_basename: Some(entry_key.basename.clone()),
                        source_kind: None,
                        target_kind: None,
                        source_exec: None,
                        target_exec: Some(false),
                    }),
                    Some(true),
                ));
            }
        }

        if source_minikind.is_fdlt() && target_minikind == Kind::Absent {
            let old_path = join_path(&entry_key.dirname, &entry_key.basename);
            let src_idx = pstate.source_index.unwrap_or(0);
            let (pdir, pbase) = split_path_utf8(&entry_key.dirname);
            let parent_bei = get_block_entry_index(&self.dirblocks, pdir, pbase, src_idx);
            let parent_id: Option<Vec<u8>> = if parent_bei.path_present {
                let pid = self.dirblocks[parent_bei.block_index].entries[parent_bei.entry_index]
                    .key
                    .file_id
                    .clone();
                (pid != entry_key.file_id).then_some(pid)
            } else {
                None
            };
            return Ok((
                Some(DirstateChange {
                    file_id: entry_key.file_id.clone(),
                    old_path: Some(old_path),
                    new_path: None,
                    content_change: true,
                    old_versioned: true,
                    new_versioned: false,
                    source_parent_id: parent_id,
                    target_parent_id: None,
                    old_basename: Some(entry_key.basename.clone()),
                    new_basename: None,
                    source_kind: source_minikind.to_osutils_kind(),
                    target_kind: None,
                    source_exec: Some(source_details_mut.executable),
                    target_exec: None,
                }),
                Some(true),
            ));
        }

        if source_minikind.is_fdlt() && target_minikind == Kind::Relocated {
            let tpath = target_details.fingerprint.clone();
            let already_inside = pstate
                .searched_specific_files
                .iter()
                .any(|p| is_inside(p.as_slice(), &tpath));
            if !already_inside {
                pstate.search_specific_files.insert(tpath);
            }
            return Ok((None, None));
        }

        if source_minikind.is_absent_or_relocated() && target_minikind.is_absent_or_relocated() {
            return Ok((None, None));
        }

        Err(ProcessEntryError::Internal(format!(
            "don't know how to compare source_minikind={:?}, target_minikind={:?}",
            source_minikind, target_minikind
        )))
    }

    /// Advance the lazy iter_changes state machine and return the
    /// next change to yield, or `Ok(None)` when the walk is done.
    /// Mirrors Python's `ProcessEntryPython.iter_changes` generator:
    /// call repeatedly to get one change at a time.
    ///
    /// Each call may emit 0 or more changes; leftover changes are
    /// buffered on `iter.pending` so subsequent calls drain them
    /// before resuming the walk.
    pub fn iter_changes_next(
        &mut self,
        iter: &mut IterChangesIter,
        pstate: &mut ProcessEntryState,
        transport: &dyn Transport,
    ) -> Result<Option<DirstateChange>, ProcessEntryError> {
        loop {
            if let Some(change) = iter.pending.pop_front() {
                return Ok(Some(change));
            }
            match iter.phase {
                IterPhase::PickRoot => {
                    let next_root = pstate.search_specific_files.iter().next().cloned();
                    match next_root {
                        Some(root) => {
                            pstate.search_specific_files.remove(&root);
                            pstate.searched_specific_files.insert(root.clone());
                            let abspath = join_path(&pstate.root_abspath, &root);
                            iter.current_root = Some((root, abspath));
                            iter.root_processed = false;
                            iter.walker = None;
                            iter.block_index = 0;
                            iter.staged_walker_block = None;
                            iter.phase = IterPhase::ProcessRoot;
                        }
                        None => {
                            iter.phase = IterPhase::DrainParents;
                        }
                    }
                }
                IterPhase::ProcessRoot => {
                    let (current_root, root_abspath) = iter
                        .current_root
                        .as_ref()
                        .expect("current_root set")
                        .clone();

                    let root_stat = match transport.lstat(&root_abspath) {
                        Ok(s) => Some(s),
                        Err(TransportError::NotFound(_)) => None,
                        Err(e) => {
                            return Err(ProcessEntryError::Internal(format!(
                                "lstat({}): {}",
                                String::from_utf8_lossy(&root_abspath),
                                e
                            )))
                        }
                    };
                    let root_path_info = match root_stat {
                        None => None,
                        Some(stat) => {
                            let mut kind = if stat.is_file() {
                                Some(osutils::Kind::File)
                            } else if stat.is_dir() {
                                Some(osutils::Kind::Directory)
                            } else if stat.is_symlink() {
                                Some(osutils::Kind::Symlink)
                            } else {
                                None
                            };
                            // The tree root itself is never a tree-reference.
                            if kind == Some(osutils::Kind::Directory)
                                && pstate.supports_tree_reference
                                && !current_root.is_empty()
                            {
                                let is_ref = transport
                                    .is_tree_reference_dir(&root_abspath)
                                    .map_err(|e| {
                                        ProcessEntryError::Internal(format!(
                                            "is_tree_reference_dir({}): {}",
                                            String::from_utf8_lossy(&root_abspath),
                                            e
                                        ))
                                    })?;
                                if is_ref {
                                    kind = Some(osutils::Kind::TreeReference);
                                }
                            }
                            Some(ProcessPathInfo {
                                abspath: root_abspath.clone(),
                                kind,
                                stat,
                            })
                        }
                    };

                    let root_entries_owned: Vec<(EntryKey, Vec<TreeData>)> = self
                        .entries_for_path(&current_root)
                        .into_iter()
                        .map(|e| (e.key.clone(), e.trees.clone()))
                        .collect();
                    if root_entries_owned.is_empty() && root_path_info.is_none() {
                        iter.phase = IterPhase::PickRoot;
                        continue;
                    }
                    let mut path_handled = false;
                    for (ek, trees) in &root_entries_owned {
                        let (change, changed) = self.process_entry(
                            pstate,
                            ek,
                            trees,
                            root_path_info.as_ref(),
                            transport,
                        )?;
                        if changed.is_some() {
                            path_handled = true;
                            if changed == Some(true) {
                                if let Some(ref c) = change {
                                    gather_result_for_consistency(pstate, c);
                                }
                            }
                            if changed == Some(true) || pstate.include_unchanged {
                                if let Some(c) = change {
                                    iter.pending.push_back(c);
                                }
                            }
                        }
                    }
                    if pstate.want_unversioned && !path_handled {
                        if let Some(ref info) = root_path_info {
                            let new_executable =
                                info.stat.is_file() && (info.stat.mode & 0o100) != 0;
                            let basename = splitpath_last(&current_root);
                            iter.pending.push_back(DirstateChange {
                                file_id: Vec::new(),
                                old_path: None,
                                new_path: Some(current_root.clone()),
                                content_change: true,
                                old_versioned: false,
                                new_versioned: false,
                                source_parent_id: None,
                                target_parent_id: None,
                                old_basename: None,
                                new_basename: Some(basename),
                                source_kind: None,
                                target_kind: info.kind,
                                source_exec: None,
                                target_exec: Some(new_executable),
                            });
                        }
                    }

                    // Decide whether to seed the on-disk walker.  We
                    // only walk the filesystem when the root exists on
                    // disk and is a plain directory; tree-references,
                    // regular files, symlinks, and missing paths all
                    // skip the walker.  Mirrors Python's catching of
                    // `ENOENT/ENOTDIR/EINVAL` from the first
                    // `_walkdirs_utf8` step.
                    //
                    // The dirblock side of the walk still runs even if
                    // the disk side is absent: a deleted directory
                    // whose children remain in the source dirblocks
                    // (e.g. ``specific_files=["b"]`` when ``b`` and
                    // ``b/c`` were both removed) needs them reported.
                    let walk_disk = root_path_info
                        .as_ref()
                        .map(|p| p.kind == Some(osutils::Kind::Directory))
                        .unwrap_or(false);
                    let initial_key = EntryKey {
                        dirname: current_root.clone(),
                        basename: Vec::new(),
                        file_id: Vec::new(),
                    };
                    let (mut bi_check, _) =
                        find_block_index_from_key(&self.dirblocks, &initial_key);
                    if bi_check == 0 {
                        bi_check = 1;
                    }
                    let has_dirblocks = self
                        .dirblocks
                        .get(bi_check)
                        .map(|b| is_inside(&current_root, &b.dirname))
                        .unwrap_or(false);
                    if !walk_disk && !has_dirblocks {
                        iter.phase = IterPhase::PickRoot;
                        continue;
                    }

                    // Seed the subtree walker (disk-side only when
                    // the root actually exists as a directory).
                    iter.walker = if walk_disk {
                        Some(WalkDirsUtf8::new(&root_abspath, &current_root))
                    } else {
                        None
                    };
                    iter.block_index = bi_check;
                    iter.staged_walker_block = None;
                    iter.merge_entry_index = 0;
                    iter.merge_path_index = 0;
                    iter.merge_path_handled = false;
                    iter.merge_advance_entry = true;
                    iter.merge_advance_path = true;
                    iter.phase = IterPhase::WalkSubtree;
                }
                IterPhase::WalkSubtree => {
                    self.iter_changes_step_walk(iter, pstate, transport)?;
                }
                IterPhase::DrainParents => {
                    self.iter_changes_step_parents(iter, pstate, transport)?;
                }
                IterPhase::Done => return Ok(None),
            }
            // Any changes just queued become the next yielded value
            // via the top-of-loop drain.
        }
    }

    /// Advance one step of the `WalkSubtree` phase.  Exactly one
    /// walker block + dirblock pair gets merged per call; if we
    /// exhaust both under the current root, transition back to
    /// `PickRoot`.
    fn iter_changes_step_walk(
        &mut self,
        iter: &mut IterChangesIter,
        pstate: &mut ProcessEntryState,
        transport: &dyn Transport,
    ) -> Result<(), ProcessEntryError> {
        let current_root = iter
            .current_root
            .as_ref()
            .expect("current_root set while walking")
            .0
            .clone();

        // Pull the next walker block if we haven't cached one. The
        // walker is only seeded when the root exists on disk; for a
        // pure dirblock-only walk (e.g. a deleted specific-file dir
        // whose source-side children still need reporting) `walker`
        // is `None` and `staged_walker_block` stays `None`.
        if iter.staged_walker_block.is_none() && iter.walker.is_some() {
            let walker = iter.walker.as_mut().expect("walker initialised");
            let mut captured: Option<(Vec<u8>, Vec<u8>, Vec<DirEntryInfo>)> = None;
            let supports_ref = pstate.supports_tree_reference;
            let mut tref_err: Option<(Vec<u8>, TransportError)> = None;
            let progressed = walker
                .next_dir(transport, |rel, abs, entries| {
                    if rel.is_empty() {
                        entries.retain(|e| e.basename.as_slice() != b".bzr");
                    }
                    if supports_ref {
                        for e in entries.iter_mut() {
                            if e.kind != Some(osutils::Kind::Directory) {
                                continue;
                            }
                            match transport.is_tree_reference_dir(&e.abspath) {
                                Ok(true) => e.kind = Some(osutils::Kind::TreeReference),
                                Ok(false) => {}
                                Err(err) => {
                                    if tref_err.is_none() {
                                        tref_err = Some((e.abspath.clone(), err));
                                    }
                                }
                            }
                        }
                    }
                    captured = Some((rel.to_vec(), abs.to_vec(), entries.clone()));
                })
                .map_err(|e| ProcessEntryError::Internal(format!("walkdirs: {}", e)))?;
            if let Some((path, err)) = tref_err {
                return Err(ProcessEntryError::Internal(format!(
                    "is_tree_reference_dir({}): {}",
                    String::from_utf8_lossy(&path),
                    err
                )));
            }
            iter.staged_walker_block = if progressed { captured } else { None };
        }

        let block_info = self
            .dirblocks
            .get(iter.block_index)
            .filter(|b| is_inside(&current_root, &b.dirname))
            .map(|b| (b.dirname.clone(), b.entries.clone()));

        // Both exhausted → this root is done; back to PickRoot.
        if iter.staged_walker_block.is_none() && block_info.is_none() {
            iter.phase = IterPhase::PickRoot;
            return Ok(());
        }

        // Resolve mis-aligned walker vs block: whichever is "earlier"
        // gets consumed first.  This mirrors the Python _lt_by_dirs
        // dispatch at the top of the merge loop.
        if let (Some((walker_rel, _, walker_entries)), Some((block_dirname, _))) =
            (iter.staged_walker_block.as_ref(), block_info.as_ref())
        {
            if walker_rel.as_slice() != block_dirname.as_slice() {
                if cmp_by_dirs_bytes(walker_rel, block_dirname).is_lt() {
                    // Walker has an unversioned directory the
                    // dirstate doesn't know about.  Emit records
                    // (if want_unversioned) and prune its subdirs
                    // from the walker's recursion.
                    if pstate.want_unversioned {
                        for pi in walker_entries.iter() {
                            let new_executable = pi.stat.is_file() && (pi.stat.mode & 0o100) != 0;
                            let path = if walker_rel.is_empty() {
                                pi.basename.clone()
                            } else {
                                let mut p = walker_rel.clone();
                                p.push(b'/');
                                p.extend_from_slice(&pi.basename);
                                p
                            };
                            iter.pending.push_back(DirstateChange {
                                file_id: Vec::new(),
                                old_path: None,
                                new_path: Some(path),
                                content_change: true,
                                old_versioned: false,
                                new_versioned: false,
                                source_parent_id: None,
                                target_parent_id: None,
                                old_basename: None,
                                new_basename: Some(pi.basename.clone()),
                                source_kind: None,
                                target_kind: pi.kind,
                                source_exec: None,
                                target_exec: Some(new_executable),
                            });
                        }
                    }
                    // Don't descend into unversioned directories.
                    if let Some(walker) = iter.walker.as_mut() {
                        walker.pending_subdirs.clear();
                    }
                    iter.staged_walker_block = None;
                    return Ok(());
                } else {
                    // Dirstate knows about a block the walker didn't
                    // visit (directory removed from disk).  Emit
                    // removals for every live entry.
                    let (_, block_entries) = block_info.unwrap();
                    for entry in &block_entries {
                        let (change, changed) =
                            self.process_entry(pstate, &entry.key, &entry.trees, None, transport)?;
                        if changed.is_some() {
                            if changed == Some(true) {
                                if let Some(ref c) = change {
                                    gather_result_for_consistency(pstate, c);
                                }
                            }
                            if changed == Some(true) || pstate.include_unchanged {
                                if let Some(c) = change {
                                    iter.pending.push_back(c);
                                }
                            }
                        }
                    }
                    iter.block_index += 1;
                    return Ok(());
                }
            }
        }

        // --- Aligned merge: same dirname on both sides (or one side empty) ---
        let (_block_dirname, block_entries) = block_info.unwrap_or((Vec::new(), Vec::new()));
        let walker_rel = iter
            .staged_walker_block
            .as_ref()
            .map(|(rel, _, _)| rel.clone())
            .unwrap_or_default();
        let walker_entries = iter
            .staged_walker_block
            .as_ref()
            .map(|(_, _, entries)| entries.clone())
            .unwrap_or_default();

        // Drain the inner merge loop one step at a time.  Unlike
        // Python's tight `while current_entry or current_path_info`
        // loop, we run the entire merge here — it's bounded by the
        // dir contents and typically small.  Results queue onto
        // iter.pending and surface one per outer next_change call.
        let mut entry_index = iter.merge_entry_index;
        let mut path_index = iter.merge_path_index;
        let mut path_handled = iter.merge_path_handled;
        let mut advance_entry = iter.merge_advance_entry;
        let mut advance_path = iter.merge_advance_path;
        let mut walker_local = walker_entries.clone();
        loop {
            let current_entry = block_entries.get(entry_index).cloned();
            let current_path_info = if path_index < walker_local.len() {
                Some(walker_local[path_index].clone())
            } else {
                None
            };
            if current_entry.is_none() && current_path_info.is_none() {
                break;
            }
            if current_entry.is_none() {
                // handled by want_unversioned below
            } else if current_path_info.is_none() {
                let ce = current_entry.as_ref().unwrap();
                let (change, changed) =
                    self.process_entry(pstate, &ce.key, &ce.trees, None, transport)?;
                if changed.is_some() {
                    if changed == Some(true) {
                        if let Some(ref c) = change {
                            gather_result_for_consistency(pstate, c);
                        }
                    }
                    if changed == Some(true) || pstate.include_unchanged {
                        if let Some(c) = change {
                            iter.pending.push_back(c);
                        }
                    }
                }
            } else {
                let ce = current_entry.as_ref().unwrap();
                let pi = current_path_info.as_ref().unwrap();
                let target0 = ce.trees.get(pstate.target_index).map(|t| t.minikind);
                let mismatch = ce.key.basename != pi.basename
                    || matches!(target0, Some(Kind::Absent) | Some(Kind::Relocated));
                if mismatch {
                    if pi.basename.as_slice() < ce.key.basename.as_slice() {
                        advance_entry = false;
                    } else {
                        let path_info_absent: Option<&ProcessPathInfo> = None;
                        let (change, changed) = self.process_entry(
                            pstate,
                            &ce.key,
                            &ce.trees,
                            path_info_absent,
                            transport,
                        )?;
                        if changed.is_some() {
                            if changed == Some(true) {
                                if let Some(ref c) = change {
                                    gather_result_for_consistency(pstate, c);
                                }
                            }
                            if changed == Some(true) || pstate.include_unchanged {
                                if let Some(c) = change {
                                    iter.pending.push_back(c);
                                }
                            }
                        }
                        advance_path = false;
                    }
                } else {
                    let pi_rs = ProcessPathInfo {
                        abspath: pi.abspath.clone(),
                        kind: pi.kind,
                        stat: pi.stat,
                    };
                    let (change, changed) =
                        self.process_entry(pstate, &ce.key, &ce.trees, Some(&pi_rs), transport)?;
                    if changed.is_some() {
                        path_handled = true;
                        if changed == Some(true) {
                            if let Some(ref c) = change {
                                gather_result_for_consistency(pstate, c);
                            }
                        }
                        if changed == Some(true) || pstate.include_unchanged {
                            if let Some(c) = change {
                                iter.pending.push_back(c);
                            }
                        }
                    }
                }
            }

            if advance_entry && current_entry.is_some() {
                entry_index += 1;
            } else {
                advance_entry = true;
            }
            if advance_path && current_path_info.is_some() {
                if !path_handled {
                    if pstate.want_unversioned {
                        let pi = current_path_info.as_ref().unwrap();
                        let new_executable = pi.stat.is_file() && (pi.stat.mode & 0o100) != 0;
                        let path = if walker_rel.is_empty() {
                            pi.basename.clone()
                        } else {
                            let mut p = walker_rel.clone();
                            p.push(b'/');
                            p.extend_from_slice(&pi.basename);
                            p
                        };
                        iter.pending.push_back(DirstateChange {
                            file_id: Vec::new(),
                            old_path: None,
                            new_path: Some(path),
                            content_change: true,
                            old_versioned: false,
                            new_versioned: false,
                            source_parent_id: None,
                            target_parent_id: None,
                            old_basename: None,
                            new_basename: Some(pi.basename.clone()),
                            source_kind: None,
                            target_kind: pi.kind,
                            source_exec: None,
                            target_exec: Some(new_executable),
                        });
                    }
                    let pi = current_path_info.as_ref().unwrap();
                    if pi.kind == Some(osutils::Kind::Directory) {
                        let child_rel = if walker_rel.is_empty() {
                            pi.basename.clone()
                        } else {
                            let mut p = walker_rel.clone();
                            p.push(b'/');
                            p.extend_from_slice(&pi.basename);
                            p
                        };
                        if let Some(walker) = iter.walker.as_mut() {
                            walker.pending_subdirs.retain(|(rel, _)| rel != &child_rel);
                        }
                    }
                }
                let pi = current_path_info.as_ref().unwrap();
                if pi.kind == Some(osutils::Kind::TreeReference) {
                    let child_rel = if walker_rel.is_empty() {
                        pi.basename.clone()
                    } else {
                        let mut p = walker_rel.clone();
                        p.push(b'/');
                        p.extend_from_slice(&pi.basename);
                        p
                    };
                    if let Some(walker) = iter.walker.as_mut() {
                        walker.pending_subdirs.retain(|(rel, _)| rel != &child_rel);
                    }
                }
                path_index += 1;
                path_handled = false;
                let _ = &mut walker_local;
            } else {
                advance_path = true;
            }
        }

        iter.merge_entry_index = entry_index;
        iter.merge_path_index = path_index;
        iter.merge_path_handled = path_handled;
        iter.merge_advance_entry = advance_entry;
        iter.merge_advance_path = advance_path;
        iter.block_index += 1;
        iter.staged_walker_block = None;
        // Reset merge cursors for the next block.
        iter.merge_entry_index = 0;
        iter.merge_path_index = 0;
        iter.merge_path_handled = false;
        iter.merge_advance_entry = true;
        iter.merge_advance_path = true;
        Ok(())
    }

    /// Advance one step of the `DrainParents` phase — equivalent to
    /// Python's `_iter_specific_file_parents`.
    fn iter_changes_step_parents(
        &mut self,
        iter: &mut IterChangesIter,
        pstate: &mut ProcessEntryState,
        transport: &dyn Transport,
    ) -> Result<(), ProcessEntryError> {
        let next = pstate.search_specific_file_parents.iter().next().cloned();
        let path_utf8 = match next {
            Some(p) => p,
            None => {
                iter.phase = IterPhase::Done;
                return Ok(());
            }
        };
        pstate.search_specific_file_parents.remove(&path_utf8);
        if pstate
            .searched_specific_files
            .iter()
            .any(|p| is_inside(p.as_slice(), &path_utf8))
        {
            return Ok(());
        }
        if pstate.searched_exact_paths.contains(&path_utf8) {
            return Ok(());
        }
        let path_entries: Vec<(EntryKey, Vec<TreeData>)> = self
            .entries_for_path(&path_utf8)
            .into_iter()
            .map(|e| (e.key.clone(), e.trees.clone()))
            .collect();
        let mut selected: Vec<(EntryKey, Vec<TreeData>)> = Vec::new();
        let mut found_item = false;
        for (ek, trees) in &path_entries {
            let target = trees.get(pstate.target_index).map(|t| t.minikind);
            let source = pstate
                .source_index
                .and_then(|i| trees.get(i))
                .map(|t| t.minikind);
            if !matches!(target, Some(Kind::Absent) | Some(Kind::Relocated)) {
                found_item = true;
                selected.push((ek.clone(), trees.clone()));
            } else if pstate.source_index.is_some()
                && !matches!(source, Some(Kind::Absent) | Some(Kind::Relocated))
            {
                found_item = true;
                if target == Some(Kind::Absent) {
                    selected.push((ek.clone(), trees.clone()));
                } else {
                    let target_path = trees[pstate.target_index].fingerprint.clone();
                    pstate.search_specific_file_parents.insert(target_path);
                }
            }
        }
        if !found_item {
            return Err(ProcessEntryError::Internal(format!(
                "Missing entry for specific path parent {:?}",
                path_utf8
            )));
        }
        let path_info = compute_path_info(pstate, transport, &path_utf8)?;
        for (ek, trees) in &selected {
            if pstate.seen_ids.contains(&ek.file_id) {
                continue;
            }
            let (change, changed) =
                self.process_entry(pstate, ek, trees, path_info.as_ref(), transport)?;
            if changed.is_none() {
                return Err(ProcessEntryError::Internal(format!(
                    "entry<->path mismatch for specific path {:?}",
                    path_utf8
                )));
            }
            if changed == Some(true) {
                if let Some(ref c) = change {
                    gather_result_for_consistency(pstate, c);
                    if c.source_kind == Some(osutils::Kind::Directory)
                        && c.target_kind != Some(osutils::Kind::Directory)
                    {
                        let entry_path = match pstate.source_index {
                            Some(si)
                                if trees.get(si).map(|t| t.minikind) == Some(Kind::Relocated) =>
                            {
                                trees[si].fingerprint.clone()
                            }
                            _ => path_utf8.clone(),
                        };
                        let initial_key = EntryKey {
                            dirname: entry_path.clone(),
                            basename: Vec::new(),
                            file_id: Vec::new(),
                        };
                        let (mut block_index, _) =
                            find_block_index_from_key(&self.dirblocks, &initial_key);
                        if block_index == 0 {
                            block_index += 1;
                        }
                        if block_index < self.dirblocks.len() {
                            let block = &self.dirblocks[block_index];
                            if is_inside(&entry_path, &block.dirname) {
                                for child in &block.entries {
                                    let source_mk = pstate
                                        .source_index
                                        .and_then(|i| child.trees.get(i))
                                        .map(|t| t.minikind);
                                    if matches!(
                                        source_mk,
                                        Some(Kind::Absent) | Some(Kind::Relocated)
                                    ) {
                                        continue;
                                    }
                                    let child_path =
                                        join_path(&child.key.dirname, &child.key.basename);
                                    pstate.search_specific_file_parents.insert(child_path);
                                }
                            }
                        }
                    }
                }
            }
            if changed == Some(true) || pstate.include_unchanged {
                if let Some(c) = change {
                    iter.pending.push_back(c);
                }
            }
        }
        pstate.searched_exact_paths.insert(path_utf8);
        let _ = iter.parents_drain_started;
        Ok(())
    }

    /// Refresh the tree-0 slot of `key` from the filesystem.  Mirrors
    /// Python's `py_update_entry`:
    ///
    /// Arguments are (key, abspath, stat, transport) — see the doc
    /// comment on [`StatInfo`] for the stat fields, and the
    /// [`Transport`] trait for read_link semantics.
    pub fn update_entry(
        &mut self,
        key: &EntryKey,
        abspath: &[u8],
        stat: &StatInfo,
        transport: &dyn Transport,
    ) -> Result<Option<Vec<u8>>, UpdateEntryError> {
        use std::time::{SystemTime, UNIX_EPOCH};

        // 1. Derive minikind from st_mode.  Non-file/dir/symlink kinds
        //    are silently skipped (Python returns None via the
        //    KeyError branch).
        let mut minikind: Kind = if stat.is_file() {
            Kind::File
        } else if stat.is_dir() {
            Kind::Directory
        } else if stat.is_symlink() {
            Kind::Symlink
        } else {
            return Ok(None);
        };

        let packed_stat = pack_stat(
            stat.size,
            stat.mtime as u64,
            stat.ctime as u64,
            stat.dev,
            stat.ino,
            stat.mode,
        )
        .into_bytes();

        // 2. Fetch the saved tree-0 row (need a clone, we'll mutate it).
        let (block_index, block_present) = find_block_index_from_key(&self.dirblocks, key);
        if !block_present {
            return Err(UpdateEntryError::EntryNotFound);
        }
        let (entry_index, entry_present) =
            find_entry_index(key, &self.dirblocks[block_index].entries);
        if !entry_present {
            return Err(UpdateEntryError::EntryNotFound);
        }
        let entry_len = self.dirblocks[block_index].entries[entry_index].trees.len();
        let tree1_minikind: Option<Kind> = self.dirblocks[block_index].entries[entry_index]
            .trees
            .get(1)
            .map(|t| t.minikind);
        let saved = self.dirblocks[block_index].entries[entry_index].trees[0].clone();

        // 3. A directory row that used to be a tree-reference keeps
        //    its 't' minikind even when the filesystem kind is plain
        //    directory (matches Python's special case).
        if minikind == Kind::Directory && saved.minikind == Kind::TreeReference {
            minikind = Kind::TreeReference;
        }

        // 4. Cache-hit path: same kind + same stat + same size → return
        //    saved link/sha1 without further I/O.
        if minikind == saved.minikind && packed_stat == saved.packed_stat {
            if minikind == Kind::Directory {
                return Ok(None);
            }
            if saved.size == stat.size {
                return Ok(Some(saved.fingerprint.clone()));
            }
        }

        // 5. Cache miss — rewrite the row.
        let now_secs: i64 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let cutoff: i64 = self.cutoff_time.unwrap_or_else(|| {
            let c = now_secs - 3;
            self.cutoff_time = Some(c);
            c
        });

        let stat_is_cacheable = stat.mtime < cutoff && stat.ctime < cutoff;

        let mut result: Option<Vec<u8>> = None;
        let mut worth_saving = true;
        let mut became_directory = false;

        // Tree-references don't get a tree-0 rewrite: the Python
        // implementation's if/elif chain has no arm for b't', so the
        // saved row is left intact and only mark_modified runs.
        if minikind == Kind::TreeReference {
            self.mark_modified(std::slice::from_ref(key), false);
            return Ok(None);
        }

        let new_tree0 = match minikind {
            Kind::File => {
                let executable = if self.use_filesystem_for_exec {
                    (stat.mode & 0o100) != 0
                } else {
                    saved.executable
                };
                if stat_is_cacheable && entry_len > 1 && tree1_minikind != Some(Kind::Absent) {
                    // SHA1Provider remains a pluggable indirection for
                    // content hashing (content filters).  Callers can
                    // install a provider that reads through their own
                    // layer; DefaultSHA1Provider is a thin wrapper
                    // over `sha_file_by_name`.
                    let path_buf = bytes_to_path(abspath);
                    let sha1 = self
                        .sha1_provider
                        .sha1(&path_buf)
                        .map_err(UpdateEntryError::Io)?;
                    result = Some(sha1.as_bytes().to_vec());
                    TreeData {
                        minikind: Kind::File,
                        fingerprint: sha1.into_bytes(),
                        size: stat.size,
                        executable,
                        packed_stat,
                    }
                } else {
                    worth_saving = false;
                    TreeData {
                        minikind: Kind::File,
                        fingerprint: Vec::new(),
                        size: stat.size,
                        executable,
                        packed_stat: b"x".repeat(32),
                    }
                }
            }
            Kind::Directory => {
                if saved.minikind != Kind::Directory {
                    became_directory = true;
                } else {
                    worth_saving = false;
                }
                TreeData {
                    minikind: Kind::Directory,
                    fingerprint: Vec::new(),
                    size: 0,
                    executable: false,
                    packed_stat,
                }
            }
            Kind::Symlink => {
                if saved.minikind == Kind::Symlink {
                    worth_saving = false;
                }
                let target_bytes = transport.read_link(abspath).map_err(|e| match e {
                    TransportError::Io { kind, message } => {
                        UpdateEntryError::Io(std::io::Error::new(kind, message))
                    }
                    TransportError::NotFound(p) => {
                        UpdateEntryError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, p))
                    }
                    other => UpdateEntryError::Other(other.to_string()),
                })?;
                result = Some(target_bytes.clone());
                if stat_is_cacheable {
                    TreeData {
                        minikind: Kind::Symlink,
                        fingerprint: target_bytes,
                        size: stat.size,
                        executable: false,
                        packed_stat,
                    }
                } else {
                    TreeData {
                        minikind: Kind::Symlink,
                        fingerprint: Vec::new(),
                        size: stat.size,
                        executable: false,
                        packed_stat: b"x".repeat(32),
                    }
                }
            }
            Kind::Absent | Kind::Relocated | Kind::TreeReference => {
                // TreeReference short-circuited above; Absent/Relocated
                // never flow through `is_file()/is_dir()/is_symlink()`.
                return Err(UpdateEntryError::UnexpectedKind(minikind));
            }
        };

        self.dirblocks[block_index].entries[entry_index].trees[0] = new_tree0;
        self.packed_stat_index = None;

        if became_directory {
            // A former file/symlink is now a directory; ensure the
            // child dirblock exists.
            let (dirname_parent, basename_parent) = (key.dirname.clone(), key.basename.clone());
            let parent_bei =
                get_block_entry_index(&self.dirblocks, &dirname_parent, &basename_parent, 0);
            if parent_bei.path_present {
                let mut subdir = dirname_parent.clone();
                if !subdir.is_empty() {
                    subdir.push(b'/');
                }
                subdir.extend_from_slice(&basename_parent);
                self.ensure_block(
                    parent_bei.block_index as isize,
                    parent_bei.entry_index as isize,
                    &subdir,
                )
                .map_err(|e| UpdateEntryError::Other(format!("ensure_block: {:?}", e)))?;
            }
        }

        if worth_saving {
            self.mark_modified(std::slice::from_ref(key), false);
        }

        Ok(result)
    }

    /// Append a `NULL_PARENT_DETAILS` row to every entry's tree slot
    /// list. Mirrors Python's inline loop in `update_basis_by_delta`:
    /// when the current dirstate has no parents and a new parent is
    /// being introduced, each row needs space for the new parent's
    /// tree-1 slot before `update_basis_by_delta` can fill it in.
    pub fn bootstrap_new_parent_slot(&mut self) {
        for block in self.dirblocks.iter_mut() {
            for entry in block.entries.iter_mut() {
                entry.trees.push(TreeData {
                    minikind: Kind::Absent,
                    fingerprint: Vec::new(),
                    size: 0,
                    executable: false,
                    packed_stat: Vec::new(),
                });
            }
        }
    }

    /// Forget all in-memory state, returning the object to the same
    /// shape a freshly constructed [`DirState`] has before any load.
    /// Mirrors Python's `DirState._wipe_state`.
    ///
    /// Python additionally clears `_split_path_cache`; that field has
    /// no equivalent on the Rust struct yet (the still un-ported
    /// memoisation layer on `_find_block_index_from_key`), so this
    /// function resets what it can and leaves a note for the future
    /// port to extend.
    pub fn wipe_state(&mut self) {
        self.header_state = MemoryState::NotInMemory;
        self.dirblock_state = MemoryState::NotInMemory;
        self.changes_aborted = false;
        self.parents.clear();
        self.ghosts.clear();
        self.dirblocks.clear();
        self.id_index = None;
        self.packed_stat_index = None;
        self.end_of_header = None;
        self.cutoff_time = None;
    }

    /// Whether the current in-memory state is worth persisting. Mirrors
    /// `DirState._worth_saving`: full-dirblock or header modifications
    /// always save; hash-only changes save only once they exceed
    /// `worth_saving_limit`, and `-1` disables hash-only saves entirely.
    pub fn worth_saving(&self) -> bool {
        if matches!(self.header_state, MemoryState::InMemoryModified)
            || matches!(self.dirblock_state, MemoryState::InMemoryModified)
        {
            return true;
        }
        if matches!(self.dirblock_state, MemoryState::InMemoryHashModified) {
            if self.worth_saving_limit == -1 {
                return false;
            }
            if self.known_hash_changes.len() as i64 >= self.worth_saving_limit {
                return true;
            }
        }
        false
    }

    /// Persist the in-memory state through `transport`, assuming a
    /// write lock is already held. This is the post-lock-upgrade core
    /// of Python's `DirState.save`: honours `changes_aborted` and
    /// `worth_saving` as early-return gates, serialises `get_lines()`
    /// via `write_all`, optionally `fdatasync`s, and finishes with
    /// `mark_unmodified`.
    ///
    /// The caller owns the read→write lock-upgrade dance that Python's
    /// `save` performs via `temporary_write_lock` — the `Transport`
    /// trait deliberately does not model it, because lock-upgrade
    /// semantics belong to the Python `LockToken` plumbing rather than
    /// to dirstate. A caller that wants the full Python behaviour
    /// performs the upgrade, calls `save_to`, then restores the read
    /// lock.
    ///
    /// Returns `Ok(true)` if the state was actually written, `Ok(false)`
    /// if an early-return gate prevented the write, and `Err` if the
    /// transport is not write-locked or any `write_all`/`fdatasync`
    /// call failed.
    pub fn save_to<T: Transport + ?Sized>(
        &mut self,
        transport: &mut T,
    ) -> Result<bool, TransportError> {
        if self.changes_aborted {
            return Ok(false);
        }
        if !self.worth_saving() {
            return Ok(false);
        }
        if transport.lock_state() != Some(LockState::Write) {
            return Err(TransportError::Other(
                "save_to requires a write lock".to_string(),
            ));
        }
        let mut buf: Vec<u8> = Vec::new();
        for line in self.get_lines() {
            buf.extend_from_slice(&line);
        }
        transport.write_all(&buf)?;
        if self.fdatasync {
            transport.fdatasync()?;
        }
        self.mark_unmodified();
        Ok(true)
    }

    /// Number of parent entries present in each dirstate record row.
    /// Mirrors Python's `DirState._num_present_parents` — total
    /// parents minus ghost parents.
    pub fn num_present_parents(&self) -> usize {
        self.parents.len().saturating_sub(self.ghosts.len())
    }

    /// Replace the entire tree-0 state with the rows produced by
    /// walking `new_inv.iter_entries_by_dir()`. Mirrors Python's
    /// `DirState.set_state_from_inventory`: zips the existing dirstate
    /// entries (in iteration order) against the incoming inventory
    /// entries, calling [`DirState::update_minimal`] and
    /// [`DirState::make_absent`] to drive the dirstate into the new
    /// shape.
    ///
    /// Each element of `new_entries` is a pre-sorted tuple
    /// `(path_utf8, file_id, minikind, fingerprint, executable)`. The
    /// caller is expected to have built it from
    /// `iter_entries_by_dir`, which yields paths in the order the
    /// dirstate needs. `fingerprint` is normally empty for non
    /// tree-reference entries; the tree-reference case carries the
    /// `reference_revision` bytes.
    pub fn set_state_from_inventory(
        &mut self,
        new_entries: Vec<(Vec<u8>, Vec<u8>, Kind, Vec<u8>, bool)>,
    ) -> Result<(), BasisApplyError> {
        fn cmp_by_dirs(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
            let mut ai = a.split(|&c| c == b'/');
            let mut bi = b.split(|&c| c == b'/');
            loop {
                match (ai.next(), bi.next()) {
                    (None, None) => return std::cmp::Ordering::Equal,
                    (None, Some(_)) => return std::cmp::Ordering::Less,
                    (Some(_), None) => return std::cmp::Ordering::Greater,
                    (Some(x), Some(y)) => match x.cmp(y) {
                        std::cmp::Ordering::Equal => continue,
                        other => return other,
                    },
                }
            }
        }

        // Snapshot the current tree-0 entries in dirstate iteration order,
        // mirroring Python's `list(self._iter_entries())` call.
        let old_entries: Vec<Entry> = self
            .dirblocks
            .iter()
            .flat_map(|block| block.entries.iter().cloned())
            .collect();

        let mut old_iter = old_entries.into_iter();
        let mut new_iter = new_entries.into_iter();
        let mut current_old: Option<Entry> = old_iter.next();
        let mut current_new: Option<(Vec<u8>, Vec<u8>, Kind, Vec<u8>, bool)> = new_iter.next();

        while current_new.is_some() || current_old.is_some() {
            // Skip dead old rows: the live tree-0 minikind may differ
            // from the snapshot because prior update_minimal calls in
            // this loop could have rewritten it.
            if let Some(ref old) = current_old {
                if self.tree0_minikind(&old.key).is_not_live() {
                    current_old = old_iter.next();
                    continue;
                }
            }

            // Materialise the new-entry split.
            let new_split = current_new.as_ref().map(|(path, file_id, mk, fp, ex)| {
                let (dn, bn) = split_path_utf8(path);
                let new_key = EntryKey {
                    dirname: dn.to_vec(),
                    basename: bn.to_vec(),
                    file_id: file_id.clone(),
                };
                (path.clone(), new_key, *mk, fp.clone(), *ex)
            });

            match (current_old.as_ref(), new_split.as_ref()) {
                (None, Some((path, key, mk, fp, ex))) => {
                    // Old is finished; insert the new entry.
                    let tree0 = TreeData {
                        minikind: *mk,
                        fingerprint: fp.clone(),
                        size: 0,
                        executable: *ex,
                        packed_stat: b"x".repeat(32),
                    };
                    self.update_minimal(key.clone(), tree0, Some(path), true)?;
                    current_new = new_iter.next();
                }
                (Some(old), None) => {
                    // New is finished; make the old entry absent.
                    let key = old.key.clone();
                    // Swallow EntryNotFound — a prior update_minimal
                    // may have pruned the row already.
                    if self.tree0_minikind(&key).is_some() {
                        self.make_absent(&key)
                            .map_err(|e| BasisApplyError::Internal {
                                reason: format!("make_absent: {}", e),
                            })?;
                    }
                    current_old = old_iter.next();
                }
                (Some(old), Some((path, key, mk, fp, ex))) => {
                    if *key == old.key {
                        // Same key; update in place if exec/minikind changed.
                        let old_t0 = &old.trees[0];
                        if old_t0.executable != *ex || old_t0.minikind != *mk {
                            let tree0 = TreeData {
                                minikind: *mk,
                                fingerprint: fp.clone(),
                                size: 0,
                                executable: *ex,
                                packed_stat: b"x".repeat(32),
                            };
                            self.update_minimal(key.clone(), tree0, Some(path), true)?;
                        }
                        current_old = old_iter.next();
                        current_new = new_iter.next();
                    } else {
                        let new_before_old = match cmp_by_dirs(&key.dirname, &old.key.dirname) {
                            std::cmp::Ordering::Less => true,
                            std::cmp::Ordering::Greater => false,
                            std::cmp::Ordering::Equal => {
                                (key.basename.as_slice(), key.file_id.as_slice())
                                    < (old.key.basename.as_slice(), old.key.file_id.as_slice())
                            }
                        };
                        if new_before_old {
                            let tree0 = TreeData {
                                minikind: *mk,
                                fingerprint: fp.clone(),
                                size: 0,
                                executable: *ex,
                                packed_stat: b"x".repeat(32),
                            };
                            self.update_minimal(key.clone(), tree0, Some(path), true)?;
                            current_new = new_iter.next();
                        } else {
                            let okey = old.key.clone();
                            if self.tree0_minikind(&okey).is_some() {
                                self.make_absent(&okey)
                                    .map_err(|e| BasisApplyError::Internal {
                                        reason: format!("make_absent: {}", e),
                                    })?;
                            }
                            current_old = old_iter.next();
                        }
                    }
                }
                (None, None) => unreachable!(),
            }
        }
        self.mark_modified(&[], false);
        self.id_index = None;
        Ok(())
    }

    /// Replace the parent trees. Mirrors Python's
    /// `DirState.set_parent_trees`.
    ///
    /// `trees` gives the revision-id of every parent (including
    /// ghosts) in order. `ghosts` is the list of revision-ids that
    /// are ghosts — must be a subset of `trees`. `parent_tree_entries`
    /// is one list per *non-ghost* parent tree, in the same order as
    /// non-ghost parents appear in `trees`; each list is the result of
    /// walking that tree via `iter_entries_by_dir` and mapping each
    /// entry to `(path_utf8, file_id, minikind, fingerprint, size,
    /// executable, tree_data)` (i.e. path/file_id plus the 5-tuple
    /// returned by [`inv_entry_to_details`]).
    ///
    /// The method rebuilds the full dirblocks layout from: (a) the
    /// current tree-0 rows already in `self.dirblocks` (non-absent,
    /// non-relocated), and (b) the per-parent-tree entry lists.
    /// Cross-tree relocation pointers are emitted in both the
    /// vertical and horizontal axes, matching the legacy matrix
    /// construction. Ghost parents occupy a tree slot but contribute
    /// no entries — their slot is always `NULL_PARENT_DETAILS`.
    pub fn set_parent_trees(
        &mut self,
        trees: Vec<Vec<u8>>,
        ghosts: Vec<Vec<u8>>,
        parent_tree_entries: Vec<Vec<(Vec<u8>, Vec<u8>, TreeData)>>,
    ) -> Result<(), EntriesToStateError> {
        let non_ghost_count = parent_tree_entries.len();
        // All parent slots, including ghosts: each entry has
        // `1 + non_ghost_count` tree slots.
        let parent_count = non_ghost_count;

        let mut by_path: std::collections::HashMap<EntryKey, Vec<TreeData>> =
            std::collections::HashMap::new();
        let mut id_index = IdIndex::new();

        // Step 1: seed with existing tree-0 entries.
        for block in self.dirblocks.iter() {
            for entry in block.entries.iter() {
                let mk = match entry.trees.first().map(|t| t.minikind) {
                    Some(k) => k,
                    None => continue,
                };
                if mk.is_absent_or_relocated() {
                    continue;
                }
                let mut row = Vec::with_capacity(1 + parent_count);
                row.push(entry.trees[0].clone());
                for _ in 0..parent_count {
                    row.push(TreeData {
                        minikind: Kind::Absent,
                        fingerprint: Vec::new(),
                        size: 0,
                        executable: false,
                        packed_stat: Vec::new(),
                    });
                }
                id_index.add((
                    entry.key.dirname.as_slice(),
                    entry.key.basename.as_slice(),
                    &FileId::from(&entry.key.file_id),
                ));
                by_path.insert(entry.key.clone(), row);
            }
        }

        // Step 2: fold each non-ghost parent tree into the matrix.
        for (index, tree_entries) in parent_tree_entries.into_iter().enumerate() {
            let tree_index = index + 1;
            let new_location_suffix_len = parent_count - tree_index;
            for (path_utf8, file_id, details) in tree_entries {
                let (dirname, basename) = split_path_utf8(&path_utf8);
                let new_entry_key = EntryKey {
                    dirname: dirname.to_vec(),
                    basename: basename.to_vec(),
                    file_id: file_id.clone(),
                };

                let fid = FileId::from(&file_id);
                let entry_keys: Vec<(Vec<u8>, Vec<u8>, FileId)> = id_index.get(&fid);

                // Vertical axis: every other path for this file_id in
                // this tree gets a relocation pointer back to path_utf8.
                for (e_dir, e_base, _e_fid) in &entry_keys {
                    let ek = EntryKey {
                        dirname: e_dir.clone(),
                        basename: e_base.clone(),
                        file_id: file_id.clone(),
                    };
                    if ek == new_entry_key {
                        continue;
                    }
                    if let Some(row) = by_path.get_mut(&ek) {
                        row[tree_index] = TreeData {
                            minikind: Kind::Relocated,
                            fingerprint: path_utf8.clone(),
                            size: 0,
                            executable: false,
                            packed_stat: Vec::new(),
                        };
                    }
                }

                // By-path consistency: insert into existing row or
                // create a new one with relocation pointers for the
                // earlier tree indexes.
                let has_key = entry_keys.iter().any(|(d, b, _)| {
                    d.as_slice() == new_entry_key.dirname.as_slice()
                        && b.as_slice() == new_entry_key.basename.as_slice()
                });
                if has_key {
                    by_path.get_mut(&new_entry_key).unwrap()[tree_index] = details;
                } else {
                    let mut new_details: Vec<TreeData> = Vec::with_capacity(1 + parent_count);
                    for lookup_index in 0..tree_index {
                        if entry_keys.is_empty() {
                            new_details.push(TreeData {
                                minikind: Kind::Absent,
                                fingerprint: Vec::new(),
                                size: 0,
                                executable: false,
                                packed_stat: Vec::new(),
                            });
                        } else {
                            let a_key = &entry_keys[0];
                            let ak = EntryKey {
                                dirname: a_key.0.clone(),
                                basename: a_key.1.clone(),
                                file_id: file_id.clone(),
                            };
                            let look = &by_path[&ak][lookup_index];
                            if look.minikind == Kind::Relocated || look.minikind == Kind::Absent {
                                new_details.push(look.clone());
                            } else {
                                let mut real_path = a_key.0.clone();
                                if !real_path.is_empty() {
                                    real_path.push(b'/');
                                }
                                real_path.extend_from_slice(&a_key.1);
                                new_details.push(TreeData {
                                    minikind: Kind::Relocated,
                                    fingerprint: real_path,
                                    size: 0,
                                    executable: false,
                                    packed_stat: Vec::new(),
                                });
                            }
                        }
                    }
                    new_details.push(details);
                    for _ in 0..new_location_suffix_len {
                        new_details.push(TreeData {
                            minikind: Kind::Absent,
                            fingerprint: Vec::new(),
                            size: 0,
                            executable: false,
                            packed_stat: Vec::new(),
                        });
                    }
                    by_path.insert(new_entry_key.clone(), new_details);
                    id_index.add((
                        new_entry_key.dirname.as_slice(),
                        new_entry_key.basename.as_slice(),
                        &fid,
                    ));
                }
            }
        }

        // Step 3: materialise the sorted entry list.
        let mut new_entries: Vec<Entry> = by_path
            .into_iter()
            .map(|(key, trees)| Entry { key, trees })
            .collect();
        Self::sort_entries(&mut new_entries);
        self.entries_to_current_state(new_entries)?;
        self.parents = trees;
        self.ghosts = ghosts;
        self.mark_modified(&[], true);
        self.id_index = Some(id_index);
        self.packed_stat_index = None;
        Ok(())
    }

    /// Rebuild `self.dirblocks` from a pre-sorted, flat list of
    /// entries. Mirrors Python's `DirState._entries_to_current_state`.
    ///
    /// `new_entries` must start with the root row (dirname and
    /// basename both empty); otherwise
    /// [`EntriesToStateError::MissingRootRow`] is returned. The
    /// resulting layout contains the two sentinel empty-dirname blocks
    /// followed by one block per distinct subdirectory, then fed
    /// through [`DirState::split_root_dirblock_into_contents`] to
    /// separate the root row from the root-contents rows.
    ///
    /// This function does not re-sort entries — callers that hand in a
    /// sorted list skip the cost, and Python's comment calls this out
    /// explicitly.
    pub fn entries_to_current_state(
        &mut self,
        new_entries: Vec<Entry>,
    ) -> Result<(), EntriesToStateError> {
        let first = new_entries.first().ok_or(EntriesToStateError::Empty)?;
        if !first.key.dirname.is_empty() || !first.key.basename.is_empty() {
            return Err(EntriesToStateError::MissingRootRow {
                key: first.key.clone(),
            });
        }

        let mut dirblocks: Vec<Dirblock> = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: Vec::new(),
            },
            Dirblock {
                dirname: Vec::new(),
                entries: Vec::new(),
            },
        ];
        // Root-group index: all entries with dirname == b"" are
        // appended to dirblocks[0]; `split_root_dirblock_into_contents`
        // later splits them into the true root and the contents-of-root.
        let mut current_idx: usize = 0;
        let mut current_dirname: Vec<u8> = Vec::new();
        for entry in new_entries {
            if entry.key.dirname != current_dirname {
                current_dirname = entry.key.dirname.clone();
                dirblocks.push(Dirblock {
                    dirname: current_dirname.clone(),
                    entries: Vec::new(),
                });
                current_idx = dirblocks.len() - 1;
            }
            dirblocks[current_idx].entries.push(entry);
        }
        self.dirblocks = dirblocks;
        self.id_index = None;
        self.packed_stat_index = None;
        split_root_dirblock_into_contents(&mut self.dirblocks)
            .map_err(EntriesToStateError::SplitFailed)?;
        Ok(())
    }

    /// Ensure a block for `dirname` exists in `self.dirblocks`, creating
    /// it if necessary. Mirrors Python's `DirState._ensure_block`.
    ///
    /// `parent_block_index` and `parent_row_index` identify the entry
    /// whose directory is being ensured. The root row is special-cased:
    /// `(parent_block_index=0, parent_row_index=0, dirname=b"")`
    /// shortcuts to block index 1 — the sentinel contents-of-root
    /// block produced by `split_root_dirblock_into_contents`.
    ///
    /// On success returns the index of the block for `dirname`. On
    /// failure — the dirname does not end with the basename stored at
    /// the given parent coordinates — returns
    /// [`EnsureBlockError::BadDirname`] to match Python's
    /// `AssertionError("bad dirname ...")`.
    pub fn ensure_block(
        &mut self,
        parent_block_index: isize,
        parent_row_index: isize,
        dirname: &[u8],
    ) -> Result<usize, EnsureBlockError> {
        // Root shortcut: block 0 row 0 with an empty dirname is always
        // followed by the empty sentinel at block 1.
        if dirname.is_empty() && parent_row_index == 0 && parent_block_index == 0 {
            return Ok(1);
        }
        // Python's assertion: dirname must end with the parent entry's
        // basename.  The Python source guards the lookup with
        // `(parent_block_index == -1 and parent_block_index == -1 and
        //   dirname == b"")` — the duplicate `parent_block_index`
        // appears to be a typo for `parent_row_index`, but the duplicate
        // collapses to a single check anyway, so the actually-observable
        // condition is `parent_block_index == -1 && dirname.is_empty()`.
        // We preserve the observable behaviour without carrying the
        // typo forward.
        let sentinel_shortcut = parent_block_index == -1 && dirname.is_empty();
        if !sentinel_shortcut {
            let parent_basename = self
                .dirblocks
                .get(parent_block_index as usize)
                .and_then(|b| b.entries.get(parent_row_index as usize))
                .map(|e| e.key.basename.as_slice())
                .ok_or_else(|| EnsureBlockError::BadDirname(dirname.to_vec()))?;
            if !dirname.ends_with(parent_basename) {
                return Err(EnsureBlockError::BadDirname(dirname.to_vec()));
            }
        }
        let lookup_key = EntryKey {
            dirname: dirname.to_vec(),
            basename: Vec::new(),
            file_id: Vec::new(),
        };
        let (block_index, present) = find_block_index_from_key(&self.dirblocks, &lookup_key);
        if !present {
            self.dirblocks.insert(
                block_index,
                Dirblock {
                    dirname: dirname.to_vec(),
                    entries: Vec::new(),
                },
            );
        }
        Ok(block_index)
    }

    /// Discard any parent trees beyond the first. Mirrors Python's
    /// `DirState._discard_merge_parents`.
    ///
    /// After this function returns the dirstate contains either 1 or
    /// 2 trees per row: current + first parent, or just current if
    /// the first parent was a ghost (Python keeps the parent slot but
    /// replaces its tree data with a `NULL_PARENT_DETAILS` placeholder
    /// so every row still has two tree slots). Entries whose tree-0
    /// and tree-1 minikinds both fall into the "dead pattern" set
    /// `{(a,r), (a,a), (r,r), (r,a)}` — i.e. absent or relocated in
    /// both the current tree and the first parent — are removed from
    /// their dirblock entirely.
    ///
    /// The header is marked modified so the change survives a save.
    /// This invalidates the cached `id_index`; callers must not hold
    /// a reference to the old one across this call.
    pub fn discard_merge_parents(&mut self) {
        if self.parents.is_empty() {
            return;
        }

        let first_parent_is_ghost = self.ghosts.contains(&self.parents[0]);

        for block in self.dirblocks.iter_mut() {
            let mut surviving: Vec<Entry> = Vec::with_capacity(block.entries.len());
            for entry in block.entries.drain(..) {
                let tree0_kind = entry.trees.first().map(|t| t.minikind);
                let tree1_kind = entry.trees.get(1).map(|t| t.minikind);
                // `is_dead` when both tree-0 and tree-1 are
                // absent-or-relocated (the four `(a|r, a|r)` patterns
                // Python's loop calls dead).
                let is_dead = matches!(
                    (tree0_kind, tree1_kind),
                    (Some(a), Some(b))
                        if a.is_absent_or_relocated() && b.is_absent_or_relocated()
                );
                if is_dead {
                    continue;
                }
                let mut new_entry = entry;
                if first_parent_is_ghost {
                    // Replace trees beyond index 0 with a single
                    // NULL_PARENT_DETAILS row so every entry still
                    // has exactly two tree slots after the discard.
                    new_entry.trees.truncate(1);
                    new_entry.trees.push(TreeData {
                        minikind: Kind::Absent,
                        fingerprint: Vec::new(),
                        size: 0,
                        executable: false,
                        packed_stat: Vec::new(),
                    });
                } else {
                    // Keep only trees 0 and 1.
                    new_entry.trees.truncate(2);
                }
                surviving.push(new_entry);
            }
            block.entries = surviving;
        }

        self.ghosts.clear();
        let first_parent = self.parents[0].clone();
        self.parents = vec![first_parent];
        self.id_index = None;
        self.packed_stat_index = None;
        self.mark_modified(&[], true);
    }

    /// Mark `key` as absent for tree 0, following Python's
    /// `DirState._make_absent`.
    ///
    /// Behaviour:
    /// 1. Scan trees 1.. of the entry at `key`. For each non-absent,
    ///    non-relocated row, remember `key` as still-referenced; for
    ///    each relocated row, remember the relocation target's key
    ///    (same file_id, new dirname/basename).
    /// 2. If `key` is not still-referenced by any remaining tree,
    ///    remove its entry row from the block and drop `key` from the
    ///    id index.
    /// 3. For every remaining-key, set its tree-0 slot to
    ///    `NULL_PARENT_DETAILS`. Assert that the slot isn't already
    ///    absent (mirroring Python's `bad row` assertion).
    /// 4. Mark the dirstate modified.
    ///
    /// Returns `true` when the entry row was removed in step (2),
    /// matching Python's `last_reference` return.
    pub fn make_absent(&mut self, key: &EntryKey) -> Result<bool, MakeAbsentError> {
        // Locate the entry we're making absent.
        let (block_index, block_present) = find_block_index_from_key(&self.dirblocks, key);
        if !block_present {
            return Err(MakeAbsentError::BlockNotFound { key: key.clone() });
        }
        let (entry_index, entry_present) =
            find_entry_index(key, &self.dirblocks[block_index].entries);
        if !entry_present {
            return Err(MakeAbsentError::EntryNotFound { key: key.clone() });
        }

        // Collect remaining references across trees 1..N. Python scans
        // `current_old[1][1:]`, i.e. every tree slot except tree 0.
        let mut remaining_keys: Vec<EntryKey> = Vec::new();
        {
            let entry = &self.dirblocks[block_index].entries[entry_index];
            for tree in entry.trees.iter().skip(1) {
                match tree.minikind {
                    // Python's branches treat 'a' as "not present at any
                    // path" and everything else except 'r' as "still at
                    // the original key".
                    Kind::Absent => {}
                    Kind::Relocated => {
                        // Relocated row: fingerprint holds the target
                        // path, file_id stays the same.
                        let (dirname, basename) = split_path_utf8(&tree.fingerprint);
                        remaining_keys.push(EntryKey {
                            dirname: dirname.to_vec(),
                            basename: basename.to_vec(),
                            file_id: key.file_id.clone(),
                        });
                    }
                    Kind::File | Kind::Directory | Kind::Symlink | Kind::TreeReference => {
                        remaining_keys.push(key.clone());
                    }
                }
            }
        }

        // The same `key` can be pushed multiple times when an entry
        // has several parent-tree slots that all happen to be 'f' (or
        // 'd' / 'l' / 't'). Each such slot maps to "still at the
        // original key", so the tree-0 update only needs to happen
        // once per distinct key — Python achieves this implicitly by
        // working through a dict.
        remaining_keys.sort_by(|a, b| {
            a.dirname
                .cmp(&b.dirname)
                .then_with(|| a.basename.cmp(&b.basename))
                .then_with(|| a.file_id.cmp(&b.file_id))
        });
        remaining_keys.dedup();

        let last_reference = !remaining_keys.iter().any(|k| k == key);

        if last_reference {
            // Remove the entry row entirely.
            self.dirblocks[block_index].entries.remove(entry_index);
            if let Some(id_index) = self.id_index.as_mut() {
                let fid = FileId::from(&key.file_id);
                id_index.remove((key.dirname.as_slice(), key.basename.as_slice(), &fid));
            }
        }

        // Update every remaining-key's tree 0 slot to NULL_PARENT_DETAILS.
        for update_key in &remaining_keys {
            let (ub, ub_present) = find_block_index_from_key(&self.dirblocks, update_key);
            if !ub_present {
                return Err(MakeAbsentError::UpdateBlockNotFound {
                    key: update_key.clone(),
                });
            }
            let (ue, ue_present) = find_entry_index(update_key, &self.dirblocks[ub].entries);
            if !ue_present {
                return Err(MakeAbsentError::UpdateEntryNotFound {
                    key: update_key.clone(),
                });
            }
            let tree0 = self.dirblocks[ub].entries[ue]
                .trees
                .first_mut()
                .ok_or_else(|| MakeAbsentError::BadRow {
                    key: update_key.clone(),
                })?;
            if tree0.minikind == Kind::Absent {
                return Err(MakeAbsentError::BadRow {
                    key: update_key.clone(),
                });
            }
            *tree0 = TreeData {
                minikind: Kind::Absent,
                fingerprint: Vec::new(),
                size: 0,
                executable: false,
                packed_stat: Vec::new(),
            };
        }

        // Tree-0 mutations invalidate the packed_stat_index.
        self.packed_stat_index = None;
        self.mark_modified(&[], false);
        Ok(last_reference)
    }

    /// Apply a sequence of "adds" to tree 1, mirroring Python's
    /// `DirState._update_basis_apply_adds`. `adds` is a flat list of
    /// per-entry records produced by `update_basis_by_delta`: each
    /// describes a new entry to insert (or, when `real_add` is false,
    /// the add half of a split rename). The caller is responsible for
    /// collecting and translating Python inventory entries into
    /// [`BasisAdd`] records — this function only touches dirblocks.
    ///
    /// Sorts `adds` in-place by `new_path` to match Python's
    /// `adds.sort(key=lambda x: x[1])`. The resulting lexicographic
    /// order ensures every parent dirblock is visited before its
    /// children.
    ///
    /// Invariants that produce an `InconsistentDelta` error — mirroring
    /// Python's `_raise_invalid` — are carried as
    /// [`BasisApplyError::Invalid`] values so the pyo3 layer can wrap
    /// them in the Python `InconsistentDelta` exception. Assertions
    /// about internal state that should never happen (such as
    /// `_find_entry_index` missing a key the linear scan locates) are
    /// reported as [`BasisApplyError::Internal`].
    ///
    /// Side effects:
    /// - may call [`DirState::ensure_block`] to materialise a dirblock
    ///   for a missing parent directory;
    /// - mutates tree-1 slots of existing entries;
    /// - inserts new entries with `[NULL_PARENT_DETAILS, new_details]`;
    /// - converts cross-directory renames to tree-0 relocation rows
    ///   when the new tree-1 entry's tree-0 slot is absent but the
    ///   file_id exists at a different path in tree 0;
    /// - ensures a child dirblock exists for directory-kind adds;
    /// - invalidates `id_index` and `packed_stat_index` caches.
    pub fn update_basis_apply_adds(
        &mut self,
        adds: &mut Vec<BasisAdd>,
    ) -> Result<(), BasisApplyError> {
        // Sort lexographically by new_path so parents are processed
        // before children.
        adds.sort_by(|a, b| a.new_path.cmp(&b.new_path));

        for add in adds.iter() {
            let (dirname_raw, basename_raw) = split_path_utf8(&add.new_path);
            let dirname = dirname_raw.to_vec();
            let basename = basename_raw.to_vec();
            let entry_key = EntryKey {
                dirname: dirname.clone(),
                basename: basename.clone(),
                file_id: add.file_id.clone(),
            };

            let (mut block_index, mut present) =
                find_block_index_from_key(&self.dirblocks, &entry_key);
            if !present {
                // The target dirblock is missing; look up the parent
                // in tree 1 and ensure a child block for `dirname`.
                let (parent_dir_raw, parent_base_raw) = split_path_utf8(&dirname);
                let bei =
                    get_block_entry_index(&self.dirblocks, parent_dir_raw, parent_base_raw, 1);
                if !bei.path_present {
                    return Err(BasisApplyError::Invalid {
                        path: add.new_path.clone(),
                        file_id: add.file_id.clone(),
                        reason: "Unable to find block for this record. Was the parent added?"
                            .to_string(),
                    });
                }
                self.ensure_block(bei.block_index as isize, bei.entry_index as isize, &dirname)
                    .map_err(|e| BasisApplyError::Invalid {
                        path: add.new_path.clone(),
                        file_id: add.file_id.clone(),
                        reason: format!("{:?}", e),
                    })?;
                // ensure_block may have inserted a new block at or
                // before the original `block_index`, shifting us.
                let (new_block_index, new_present) =
                    find_block_index_from_key(&self.dirblocks, &entry_key);
                block_index = new_block_index;
                present = new_present;
                // ensure_block must have created the dirblock for
                // `dirname`; `present` here refers to the dirblock,
                // not the entry inside it.
                debug_assert!(present);
            }
            let _ = present;

            let (entry_index, entry_present) =
                find_entry_index(&entry_key, &self.dirblocks[block_index].entries);

            if add.real_add && add.old_path.is_some() {
                return Err(BasisApplyError::Invalid {
                    path: add.new_path.clone(),
                    file_id: add.file_id.clone(),
                    reason: format!(
                        "considered a real add but still had old_path at {:?}",
                        add.old_path.as_ref().unwrap()
                    ),
                });
            }

            if entry_present {
                // Update the existing entry's tree 1 slot.
                let entry = &mut self.dirblocks[block_index].entries[entry_index];
                match entry.trees.get(1).map(|t| t.minikind) {
                    None | Some(Kind::Absent) => {
                        if entry.trees.len() >= 2 {
                            entry.trees[1] = add.new_details.clone();
                        } else {
                            entry.trees.push(add.new_details.clone());
                        }
                    }
                    Some(Kind::Relocated) => {
                        return Err(BasisApplyError::NotImplemented {
                            reason: "basis entry is a relocation".to_string(),
                        });
                    }
                    Some(_) => {
                        return Err(BasisApplyError::Invalid {
                            path: add.new_path.clone(),
                            file_id: add.file_id.clone(),
                            reason:
                                "An entry was marked as a new add but the basis target already existed"
                                    .to_string(),
                        });
                    }
                }
            } else {
                // The exact key is not present; scan the two
                // neighbouring positions for same-path-different-id
                // conflicts (Python only checks `entry_index - 1`
                // and `entry_index`).
                let block_len = self.dirblocks[block_index].entries.len();
                let start = entry_index.saturating_sub(1);
                let end = entry_index + 1;
                for maybe_index in start..end {
                    if maybe_index >= block_len {
                        continue;
                    }
                    let maybe = &self.dirblocks[block_index].entries[maybe_index];
                    if maybe.key.dirname != dirname || maybe.key.basename != basename {
                        continue;
                    }
                    if maybe.key.file_id == add.file_id {
                        return Err(BasisApplyError::Internal {
                            reason: format!(
                                "find_entry_index did not find a key match but walking the data did, for ({:?}, {:?}, {:?})",
                                dirname, basename, add.file_id
                            ),
                        });
                    }
                    if maybe.trees.get(1).map(|t| t.minikind).is_live() {
                        return Err(BasisApplyError::Invalid {
                            path: add.new_path.clone(),
                            file_id: add.file_id.clone(),
                            reason: format!(
                                "we have an add record for path, but the path is already present with another file_id {:?}",
                                maybe.key.file_id
                            ),
                        });
                    }
                }

                // Insert the new entry with NULL_PARENT_DETAILS for
                // tree 0 and `new_details` for tree 1.
                let new_entry = Entry {
                    key: entry_key.clone(),
                    trees: vec![
                        TreeData {
                            minikind: Kind::Absent,
                            fingerprint: Vec::new(),
                            size: 0,
                            executable: false,
                            packed_stat: Vec::new(),
                        },
                        add.new_details.clone(),
                    ],
                };
                self.dirblocks[block_index]
                    .entries
                    .insert(entry_index, new_entry);
            }

            // Cross-tree check: if the (possibly just-inserted) entry's
            // tree 0 slot is absent, look up the file_id in tree 0
            // elsewhere and, if found, rewrite both sides into
            // relocation rows.
            let active_kind = self.dirblocks[block_index].entries[entry_index]
                .trees
                .first()
                .map(|t| t.minikind);

            if active_kind == Some(Kind::Absent) {
                // Look up file_id via id_index; collect candidate
                // (block, entry) coordinates before mutating, to
                // keep the borrow checker happy.
                let fid = FileId::from(&add.file_id);
                let candidate_keys = self.get_or_build_id_index().get(&fid);

                let mut relocation: Option<(usize, usize, Vec<u8>)> = None;
                for key_tuple in candidate_keys {
                    let (k_dirname, k_basename, _k_file_id) = key_tuple;
                    let bei = get_block_entry_index(&self.dirblocks, &k_dirname, &k_basename, 0);
                    if !bei.path_present {
                        continue;
                    }
                    let candidate = &self.dirblocks[bei.block_index].entries[bei.entry_index];
                    if candidate.key.file_id != add.file_id {
                        continue;
                    }
                    if candidate.trees.first().map(|t| t.minikind).is_not_live() {
                        return Err(BasisApplyError::Invalid {
                            path: add.new_path.clone(),
                            file_id: add.file_id.clone(),
                            reason: "We found a tree0 entry that doesnt make sense".to_string(),
                        });
                    }
                    let active_dir = candidate.key.dirname.clone();
                    let active_name = candidate.key.basename.clone();
                    let active_path = if active_dir.is_empty() {
                        active_name.clone()
                    } else {
                        let mut p = active_dir.clone();
                        p.push(b'/');
                        p.extend_from_slice(&active_name);
                        p
                    };
                    relocation = Some((bei.block_index, bei.entry_index, active_path));
                    break;
                }

                if let Some((other_block, other_entry, active_path)) = relocation {
                    // Update the other entry's tree 1 slot to point
                    // at the new path.
                    {
                        let other = &mut self.dirblocks[other_block].entries[other_entry];
                        let new_tree1 = TreeData {
                            minikind: Kind::Relocated,
                            fingerprint: add.new_path.clone(),
                            size: 0,
                            executable: false,
                            packed_stat: Vec::new(),
                        };
                        if other.trees.len() >= 2 {
                            other.trees[1] = new_tree1;
                        } else {
                            other.trees.push(new_tree1);
                        }
                    }
                    // Update the new entry's tree 0 slot to point at
                    // the other path.
                    {
                        let e = &mut self.dirblocks[block_index].entries[entry_index];
                        e.trees[0] = TreeData {
                            minikind: Kind::Relocated,
                            fingerprint: active_path,
                            size: 0,
                            executable: false,
                            packed_stat: Vec::new(),
                        };
                    }
                }
            } else if active_kind == Some(Kind::Relocated) {
                return Err(BasisApplyError::NotImplemented {
                    reason: "active entry is a relocation".to_string(),
                });
            }

            // If the new entry is a directory, ensure a child dirblock
            // for its path exists.
            if add.new_details.minikind == Kind::Directory {
                // Use the (possibly-shifted) block_index + entry_index
                // as the parent coordinates for the child dirblock.
                self.ensure_block(block_index as isize, entry_index as isize, &add.new_path)
                    .map_err(|e| BasisApplyError::Invalid {
                        path: add.new_path.clone(),
                        file_id: add.file_id.clone(),
                        reason: format!("{:?}", e),
                    })?;
            }
        }

        self.id_index = None;
        self.packed_stat_index = None;
        Ok(())
    }

    /// Check that every `(dirname_utf8, file_id)` pair in `parents`
    /// exists in `tree_index` at the given path with the given id
    /// *and* is a directory. Mirrors Python's
    /// `DirState._after_delta_check_parents`.
    ///
    /// Returns [`BasisApplyError::Invalid`] on the first parent that
    /// is missing (`"This parent is not present."`) or not a
    /// directory (`"This parent is not a directory."`).
    pub fn after_delta_check_parents(
        &mut self,
        parents: &[(Vec<u8>, Vec<u8>)],
        tree_index: usize,
    ) -> Result<(), BasisApplyError> {
        for (dirname_utf8, file_id) in parents {
            let (d, b) = split_path_utf8(dirname_utf8);
            let bei = get_block_entry_index(&self.dirblocks, d, b, tree_index);
            if !bei.path_present {
                return Err(BasisApplyError::Invalid {
                    path: dirname_utf8.clone(),
                    file_id: file_id.clone(),
                    reason: "This parent is not present.".to_string(),
                });
            }
            let entry = &self.dirblocks[bei.block_index].entries[bei.entry_index];
            if entry.key.file_id != *file_id {
                return Err(BasisApplyError::Invalid {
                    path: dirname_utf8.clone(),
                    file_id: file_id.clone(),
                    reason: "This parent is not present.".to_string(),
                });
            }
            if entry.trees.get(tree_index).map(|t| t.minikind) != Some(Kind::Directory) {
                return Err(BasisApplyError::Invalid {
                    path: dirname_utf8.clone(),
                    file_id: file_id.clone(),
                    reason: "This parent is not a directory.".to_string(),
                });
            }
        }
        Ok(())
    }

    /// Verify that none of `new_ids` is already present at a live
    /// entry in `tree_index`. Mirrors Python's
    /// `DirState._check_delta_ids_absent` — used by both
    /// `update_by_delta` and `update_basis_by_delta` to guard against
    /// a delta that resurrects an already-present file id.
    ///
    /// On a conflict, returns [`BasisApplyError::Invalid`] carrying
    /// the first offending path / file id.
    pub fn check_delta_ids_absent(
        &mut self,
        new_ids: &[Vec<u8>],
        tree_index: usize,
    ) -> Result<(), BasisApplyError> {
        if new_ids.is_empty() {
            return Ok(());
        }
        let _ = self.get_or_build_id_index();
        for file_id in new_ids {
            let fid = FileId::from(file_id);
            let candidates = self.id_index.as_ref().unwrap().get(&fid);
            for (dn, bn, _) in candidates {
                let bei = get_block_entry_index(&self.dirblocks, &dn, &bn, tree_index);
                if !bei.path_present {
                    continue;
                }
                let entry = &self.dirblocks[bei.block_index].entries[bei.entry_index];
                if entry.key.file_id != *file_id {
                    continue;
                }
                let mut path = dn.clone();
                if !path.is_empty() {
                    path.push(b'/');
                }
                path.extend_from_slice(&bn);
                return Err(BasisApplyError::Invalid {
                    path,
                    file_id: file_id.clone(),
                    reason: "This file_id is new in the delta but already present in the target"
                        .to_string(),
                });
            }
        }
        Ok(())
    }

    /// Update a single entry in tree 0 — either insert a new row or
    /// replace its tree-0 details. Mirrors Python's
    /// `DirState.update_minimal`.
    ///
    /// # Parameters
    /// - `key`: `(dirname, basename, file_id)` identifying the entry.
    /// - `tree0_details`: replacement data for the tree-0 slot
    ///   (the `new_details` tuple Python builds from minikind,
    ///   fingerprint, size, executable, packed_stat).
    /// - `path_utf8`: `dirname + "/" + basename` without the leading
    ///   slash, or `b""` for the root; used when building relocation
    ///   pointers. Required whenever the method takes the
    ///   cross-reference branch.
    /// - `fullscan`: when true, skip the conflicting-entry check
    ///   that `set_state_from_inventory` disables for bulk loads.
    ///
    /// Returns `Ok(())` on success, or
    /// [`BasisApplyError::Invalid`] / [`BasisApplyError::Internal`]
    /// for user-visible delta conflicts and internal invariant
    /// violations (matching Python's `_raise_invalid` /
    /// `AssertionError` / "no path").
    pub fn update_minimal(
        &mut self,
        key: EntryKey,
        tree0_details: TreeData,
        path_utf8: Option<&[u8]>,
        fullscan: bool,
    ) -> Result<(), BasisApplyError> {
        // Ensure the block for `key.dirname` exists. Python's
        // `_find_block` performs a `_find_block_index_from_key`
        // lookup then — when the block is missing and the caller
        // does not pass `add_if_missing=True` — verifies the parent
        // directory is versioned in tree 0, raising
        // `NotVersionedError` otherwise.
        let (_block_index, block_present) = find_block_index_from_key(&self.dirblocks, &key);
        if !block_present {
            // Python's parent-check: osutils.split(key.dirname) and
            // require the result to be a present path in tree 0.
            let (parent_dir, parent_base) = split_path_utf8(&key.dirname);
            let parent_bei = get_block_entry_index(&self.dirblocks, parent_dir, parent_base, 0);
            if !parent_bei.path_present {
                let mut path = key.dirname.clone();
                if !path.is_empty() {
                    path.push(b'/');
                }
                path.extend_from_slice(&key.basename);
                return Err(BasisApplyError::NotVersioned { path });
            }
            self.ensure_block(
                parent_bei.block_index as isize,
                parent_bei.entry_index as isize,
                &key.dirname,
            )
            .map_err(|e| BasisApplyError::Internal {
                reason: format!("ensure_block failed: {:?}", e),
            })?;
        }
        let (block_index, _) = find_block_index_from_key(&self.dirblocks, &key);

        // Find the insertion point within the block.
        let (mut entry_index, present) =
            find_entry_index(&key, &self.dirblocks[block_index].entries);

        // Pre-populate the id_index cache once.
        let _ = self.get_or_build_id_index();

        if !present {
            // Non-fullscan conflict check: walk forward from the
            // basename-only match position and ensure no existing
            // entry occupies the same (dirname, basename) with a
            // live tree-0 row.
            if !fullscan {
                let prefix_key = EntryKey {
                    dirname: key.dirname.clone(),
                    basename: key.basename.clone(),
                    file_id: Vec::new(),
                };
                let (mut low_index, _) =
                    find_entry_index(&prefix_key, &self.dirblocks[block_index].entries);
                while low_index < self.dirblocks[block_index].entries.len() {
                    let candidate = &self.dirblocks[block_index].entries[low_index];
                    if candidate.key.dirname == key.dirname
                        && candidate.key.basename == key.basename
                    {
                        if candidate.trees.first().map(|t| t.minikind).is_live() {
                            let mut path = key.dirname.clone();
                            if !path.is_empty() {
                                path.push(b'/');
                            }
                            path.extend_from_slice(&key.basename);
                            return Err(BasisApplyError::Invalid {
                                path,
                                file_id: key.file_id.clone(),
                                reason: format!(
                                    "Attempt to add item at path already occupied by id {:?}",
                                    candidate.key.file_id
                                ),
                            });
                        }
                        low_index += 1;
                    } else {
                        break;
                    }
                }
            }

            // Existing keys for this file_id across the id_index.
            let fid = FileId::from(&key.file_id);
            let existing_keys: Vec<(Vec<u8>, Vec<u8>, FileId)> =
                self.id_index.as_ref().unwrap().get(&fid);

            let new_trees: Vec<TreeData> = if existing_keys.is_empty() {
                // Simple case: a new file id, no parents to link.
                let mut trees = vec![tree0_details.clone()];
                for _ in 0..self.num_present_parents() {
                    trees.push(TreeData {
                        minikind: Kind::Absent,
                        fingerprint: Vec::new(),
                        size: 0,
                        executable: false,
                        packed_stat: Vec::new(),
                    });
                }
                trees
            } else {
                // Cross-reference case: rewrite other rows to point
                // at this new entry, then assemble parent details
                // by cloning from existing rows or synthesising
                // relocation pointers.
                let path_bytes = path_utf8.ok_or_else(|| BasisApplyError::Internal {
                    reason: "update_minimal: no path".to_string(),
                })?;

                // Convert each existing key's tree-0 slot to a
                // relocation pointer to `path_utf8`. Python also
                // drops entries that become entirely dead
                // afterwards via `_maybe_remove_row`.
                let mut removed_before_target = 0usize;
                let keys_snapshot: Vec<(Vec<u8>, Vec<u8>, FileId)> = existing_keys.clone();
                for other_tuple in &keys_snapshot {
                    let (odirname, obasename, _ofid) = other_tuple;
                    let other_key = EntryKey {
                        dirname: odirname.clone(),
                        basename: obasename.clone(),
                        file_id: key.file_id.clone(),
                    };
                    let (ob_idx, ob_present) =
                        find_block_index_from_key(&self.dirblocks, &other_key);
                    if !ob_present {
                        return Err(BasisApplyError::Internal {
                            reason: format!("could not find block for {:?}", other_key),
                        });
                    }
                    let (oe_idx, oe_present) =
                        find_entry_index(&other_key, &self.dirblocks[ob_idx].entries);
                    if !oe_present {
                        return Err(BasisApplyError::Internal {
                            reason: format!(
                                "update_minimal: could not find other entry for {:?}",
                                other_key
                            ),
                        });
                    }

                    self.dirblocks[ob_idx].entries[oe_idx].trees[0] = TreeData {
                        minikind: Kind::Relocated,
                        fingerprint: path_bytes.to_vec(),
                        size: 0,
                        executable: false,
                        packed_stat: Vec::new(),
                    };

                    let all_dead = self.dirblocks[ob_idx].entries[oe_idx]
                        .trees
                        .iter()
                        .all(|t| t.minikind == Kind::Absent || t.minikind == Kind::Relocated);
                    if all_dead {
                        let removed_key = self.dirblocks[ob_idx].entries[oe_idx].key.clone();
                        self.dirblocks[ob_idx].entries.remove(oe_idx);
                        if let Some(idx) = self.id_index.as_mut() {
                            let rfid = FileId::from(&removed_key.file_id);
                            idx.remove((
                                removed_key.dirname.as_slice(),
                                removed_key.basename.as_slice(),
                                &rfid,
                            ));
                        }
                        if ob_idx == block_index && oe_idx < entry_index {
                            removed_before_target += 1;
                        }
                    }
                }
                entry_index = entry_index.saturating_sub(removed_before_target);

                let mut trees = vec![tree0_details.clone()];
                let num_parents = self.num_present_parents();
                if num_parents > 0 {
                    // Python grabs `list(existing_keys)[0]` before
                    // the removals, so the first key in the
                    // snapshot is the authoritative source for
                    // parent-tree details.
                    let (odirname, obasename, _ofid) = keys_snapshot[0].clone();
                    let other_key = EntryKey {
                        dirname: odirname.clone(),
                        basename: obasename.clone(),
                        file_id: key.file_id.clone(),
                    };
                    let (ub_idx, ub_present) =
                        find_block_index_from_key(&self.dirblocks, &other_key);
                    if !ub_present {
                        return Err(BasisApplyError::Internal {
                            reason: format!("could not find block for {:?}", other_key),
                        });
                    }
                    let (ue_idx, ue_present) =
                        find_entry_index(&other_key, &self.dirblocks[ub_idx].entries);
                    if !ue_present {
                        return Err(BasisApplyError::Internal {
                            reason: format!(
                                "update_minimal: could not find entry for {:?}",
                                other_key
                            ),
                        });
                    }
                    for lookup_index in 1..=num_parents {
                        let source_tree = self.dirblocks[ub_idx].entries[ue_idx]
                            .trees
                            .get(lookup_index)
                            .cloned();
                        match source_tree {
                            Some(ref t)
                                if t.minikind == Kind::Absent || t.minikind == Kind::Relocated =>
                            {
                                trees.push(t.clone());
                            }
                            Some(_) => {
                                let mut ptr = odirname.clone();
                                if !ptr.is_empty() {
                                    ptr.push(b'/');
                                }
                                ptr.extend_from_slice(&obasename);
                                trees.push(TreeData {
                                    minikind: Kind::Relocated,
                                    fingerprint: ptr,
                                    size: 0,
                                    executable: false,
                                    packed_stat: Vec::new(),
                                });
                            }
                            None => {
                                trees.push(TreeData {
                                    minikind: Kind::Absent,
                                    fingerprint: Vec::new(),
                                    size: 0,
                                    executable: false,
                                    packed_stat: Vec::new(),
                                });
                            }
                        }
                    }
                }
                trees
            };

            // Insert the new entry at `entry_index`, then extend
            // the id_index.
            let new_entry = Entry {
                key: key.clone(),
                trees: new_trees,
            };
            self.dirblocks[block_index]
                .entries
                .insert(entry_index, new_entry);
            if let Some(idx) = self.id_index.as_mut() {
                idx.add((
                    key.dirname.as_slice(),
                    key.basename.as_slice(),
                    &FileId::from(&key.file_id),
                ));
            }
        } else {
            // Update the tree-0 slot of the existing entry in place.
            self.dirblocks[block_index].entries[entry_index].trees[0] = tree0_details.clone();

            let path_bytes = path_utf8.ok_or_else(|| BasisApplyError::Internal {
                reason: "update_minimal: no path".to_string(),
            })?;

            // Cross-reference maintenance: every other entry that
            // shares this file_id (as recorded in the id_index)
            // must be turned into a relocation pointer to
            // `path_utf8`.
            let fid = FileId::from(&key.file_id);
            let existing_keys: Vec<(Vec<u8>, Vec<u8>, FileId)> =
                self.id_index.as_ref().unwrap().get(&fid);
            if !existing_keys
                .iter()
                .any(|(d, b, _)| d == &key.dirname && b == &key.basename)
            {
                return Err(BasisApplyError::Internal {
                    reason: format!(
                        "We found the entry in the blocks, but the key is not in the id_index. key: {:?}, existing_keys: {:?}",
                        key, existing_keys
                    ),
                });
            }

            for (odirname, obasename, _ofid) in &existing_keys {
                if odirname == &key.dirname && obasename == &key.basename {
                    continue;
                }
                let other_key = EntryKey {
                    dirname: odirname.clone(),
                    basename: obasename.clone(),
                    file_id: key.file_id.clone(),
                };
                let (ob_idx, ob_present) = find_block_index_from_key(&self.dirblocks, &other_key);
                if !ob_present {
                    return Err(BasisApplyError::Internal {
                        reason: format!("not present: {:?}", other_key),
                    });
                }
                let (oe_idx, oe_present) =
                    find_entry_index(&other_key, &self.dirblocks[ob_idx].entries);
                if !oe_present {
                    return Err(BasisApplyError::Internal {
                        reason: format!("not present: {:?}", other_key),
                    });
                }
                self.dirblocks[ob_idx].entries[oe_idx].trees[0] = TreeData {
                    minikind: Kind::Relocated,
                    fingerprint: path_bytes.to_vec(),
                    size: 0,
                    executable: false,
                    packed_stat: Vec::new(),
                };
            }
        }

        // If the new entry is a directory, ensure a child block
        // exists for its path.
        if tree0_details.minikind == Kind::Directory {
            let mut subdir_name = key.dirname.clone();
            if !subdir_name.is_empty() {
                subdir_name.push(b'/');
            }
            subdir_name.extend_from_slice(&key.basename);
            let subdir_key = EntryKey {
                dirname: subdir_name,
                basename: Vec::new(),
                file_id: Vec::new(),
            };
            let (sb_idx, sb_present) = find_block_index_from_key(&self.dirblocks, &subdir_key);
            if !sb_present {
                self.dirblocks.insert(
                    sb_idx,
                    Dirblock {
                        dirname: subdir_key.dirname.clone(),
                        entries: Vec::new(),
                    },
                );
            }
        }

        self.mark_modified(&[], false);
        self.packed_stat_index = None;
        Ok(())
    }

    /// High-level entry point mirroring Python's `DirState.add` from
    /// the top: takes a `path` string (any of `""`, `"foo"`,
    /// `"foo/bar"`), normalises the basename, validates `.`/`..`,
    /// packs the stat, and dispatches to [`DirState::add`].
    ///
    /// Returns `AddError::InvalidNormalization` when NFC would point
    /// at an inaccessible path, `AddError::InvalidEntryName` when the
    /// basename is `.` or `..`.  Other failures bubble up from
    /// [`DirState::add`].
    pub fn add_path(
        &mut self,
        path: &str,
        file_id: &[u8],
        kind: osutils::Kind,
        stat: Option<StatInfo>,
        fingerprint: &[u8],
    ) -> Result<(), AddError> {
        // Split the str-path into (dirname, basename).  Python uses
        // `os.path.split` which splits on the last `/`.
        let (dirname_s, basename_s) = match path.rfind('/') {
            Some(idx) => (&path[..idx], &path[idx + 1..]),
            None => ("", path),
        };

        // NFC-normalise the basename.  Inaccessible-after-normalisation
        // is a hard error on Linux; on macOS the filesystem is the one
        // doing the normalisation so the result is always accessible.
        let basename_norm =
            match osutils::path::normalized_filename(std::path::Path::new(basename_s)) {
                Some((norm, accessible)) => {
                    if norm.as_os_str() != std::ffi::OsStr::new(basename_s) && !accessible {
                        return Err(AddError::InvalidNormalization {
                            path: path.to_string(),
                        });
                    }
                    norm.to_string_lossy().into_owned()
                }
                None => basename_s.to_string(),
            };

        if basename_norm == "." || basename_norm == ".." {
            return Err(AddError::InvalidEntryName {
                name: path.to_string(),
            });
        }

        // Rejoin using the (possibly renormalised) basename, then
        // strip leading/trailing `/` and take the utf8 bytes.  This
        // matches the `(dirname + "/" + basename).strip("/").encode("utf8")`
        // pass Python does before the utf8 split.
        let mut rejoined = String::with_capacity(dirname_s.len() + 1 + basename_norm.len());
        rejoined.push_str(dirname_s);
        rejoined.push('/');
        rejoined.push_str(&basename_norm);
        let utf8path = rejoined.trim_matches('/').as_bytes().to_vec();

        let (dirname_b, basename_b): (&[u8], &[u8]) =
            match utf8path.iter().rposition(|&b| b == b'/') {
                Some(idx) => (&utf8path[..idx], &utf8path[idx + 1..]),
                None => (b"".as_slice(), utf8path.as_slice()),
            };

        let (size, packed_stat_owned) = match stat {
            None => (0u64, vec![b'x'; 32]),
            Some(st) => {
                let packed = pack_stat(
                    st.size,
                    st.mtime as u64,
                    st.ctime as u64,
                    st.dev,
                    st.ino,
                    st.mode,
                );
                (st.size, packed.into_bytes())
            }
        };

        self.add(
            &utf8path,
            dirname_b,
            basename_b,
            file_id,
            kind,
            size,
            &packed_stat_owned,
            fingerprint,
        )
    }

    /// Add a new tracked entry. Mirrors Python's `DirState.add` after
    /// path normalisation: the caller is responsible for handing in
    /// `utf8path` with its `dirname`/`basename` split already done, and
    /// for supplying the packed_stat bytes (use `pack_stat` on the
    /// `os.lstat` result, or `None` to substitute `NULLSTAT`).
    ///
    /// `kind` is the filesystem kind; ``osutils::Kind`` already
    /// constrains it to the four valid variants.
    ///
    /// The method performs the same duplicate-id detection Python does:
    /// if `file_id` is already tracked at a live (non-absent) path it
    /// returns `AddError::DuplicateFileId`. If the file_id existed
    /// previously at a different path marked absent, that old row is
    /// rewritten as a relocation pointer to the new path via
    /// [`DirState::update_minimal`], matching Python's `rename_from`
    /// fix-up. In that case the resulting entry's parent-tree slot 0
    /// stores a relocation row pointing back at the old path, so
    /// history-aware tooling can still resolve the id.
    ///
    /// The target dirblock is created (`ensure_block`) if missing, and a
    /// child block is ensured when the new entry is a directory — both
    /// matching Python's post-insert `_ensure_block` call.
    #[allow(clippy::too_many_arguments)]
    pub fn add(
        &mut self,
        utf8path: &[u8],
        dirname: &[u8],
        basename: &[u8],
        file_id: &[u8],
        kind: osutils::Kind,
        size: u64,
        packed_stat: &[u8],
        fingerprint: &[u8],
    ) -> Result<(), AddError> {
        // Pre-flight: does this file_id already live somewhere?
        // Python calls `_get_entry(0, fileid_utf8=file_id,
        // include_deleted=True)` and branches on the result.
        self.get_or_build_id_index();
        let fid = FileId::from(&file_id.to_vec());
        let candidates = self.id_index.as_ref().unwrap().get(&fid);

        let mut rename_from: Option<(Vec<u8>, Vec<u8>)> = None;
        for (cand_dir, cand_base, _cfid) in candidates {
            let cand_key = EntryKey {
                dirname: cand_dir.clone(),
                basename: cand_base.clone(),
                file_id: file_id.to_vec(),
            };
            let (cb_idx, cb_present) = find_block_index_from_key(&self.dirblocks, &cand_key);
            if !cb_present {
                continue;
            }
            let (ce_idx, ce_present) = find_entry_index(&cand_key, &self.dirblocks[cb_idx].entries);
            if !ce_present {
                continue;
            }
            let entry = &self.dirblocks[cb_idx].entries[ce_idx];
            let tree0_kind = match entry.trees.first().map(|t| t.minikind) {
                Some(k) => k,
                None => continue,
            };
            match tree0_kind {
                Kind::Absent => {
                    if cand_dir.as_slice() != dirname || cand_base.as_slice() != basename {
                        rename_from = Some((cand_dir.clone(), cand_base.clone()));
                    }
                    break;
                }
                Kind::Relocated => {
                    // The candidate row is a relocation pointer; keep
                    // searching — the real home is elsewhere.
                    continue;
                }
                other => {
                    let path = if cand_dir.is_empty() {
                        cand_base.clone()
                    } else {
                        let mut p = cand_dir.clone();
                        p.push(b'/');
                        p.extend_from_slice(&cand_base);
                        p
                    };
                    let path_str = String::from_utf8_lossy(&path);
                    let kind_str = other
                        .to_osutils_kind()
                        .expect("absent/relocated handled above")
                        .as_str();
                    return Err(AddError::DuplicateFileId {
                        file_id: file_id.to_vec(),
                        info: format!("{}:{}", kind_str, path_str),
                    });
                }
            }
        }

        // Rename fix-up: the id used to live at rename_from but was
        // marked absent. Python calls update_minimal to turn the old
        // row into a relocation pointer to the new path.
        if let Some((old_dir, old_base)) = rename_from.as_ref() {
            let old_key = EntryKey {
                dirname: old_dir.clone(),
                basename: old_base.clone(),
                file_id: file_id.to_vec(),
            };
            let reloc_details = TreeData {
                minikind: Kind::Relocated,
                fingerprint: utf8path.to_vec(),
                size: 0,
                executable: false,
                packed_stat: Vec::new(),
            };
            self.update_minimal(old_key, reloc_details, Some(b""), false)
                .map_err(|e| AddError::Internal {
                    reason: format!("rename-from update_minimal: {}", e),
                })?;
        }

        // Find the block that should receive the new entry.
        let first_key = EntryKey {
            dirname: dirname.to_vec(),
            basename: basename.to_vec(),
            file_id: Vec::new(),
        };
        let (mut block_index, block_present) =
            find_block_index_from_key(&self.dirblocks, &first_key);
        if block_present {
            // A block exists; walk entries at this basename and ensure
            // none is live in tree 0.
            let (mut entry_index, _) =
                find_entry_index(&first_key, &self.dirblocks[block_index].entries);
            let block = &self.dirblocks[block_index].entries;
            while entry_index < block.len()
                && block[entry_index].key.dirname == dirname
                && block[entry_index].key.basename == basename
            {
                if block[entry_index]
                    .trees
                    .first()
                    .map(|t| t.minikind)
                    .is_live()
                {
                    let mut path = dirname.to_vec();
                    if !path.is_empty() {
                        path.push(b'/');
                    }
                    path.extend_from_slice(basename);
                    return Err(AddError::AlreadyAdded { path });
                }
                entry_index += 1;
            }
        } else {
            // Python: look up the parent directory; if absent, raise
            // NotVersionedError. Otherwise ensure_block.
            let (parent_dir, parent_base) = split_path_utf8(dirname);
            let pbei = get_block_entry_index(&self.dirblocks, parent_dir, parent_base, 0);
            if !pbei.path_present {
                let mut path = dirname.to_vec();
                if !path.is_empty() {
                    path.push(b'/');
                }
                path.extend_from_slice(basename);
                return Err(AddError::NotVersioned { path });
            }
            self.ensure_block(
                pbei.block_index as isize,
                pbei.entry_index as isize,
                dirname,
            )
            .map_err(|e| AddError::Internal {
                reason: format!("ensure_block failed: {:?}", e),
            })?;
            let (new_block_index, _) = find_block_index_from_key(&self.dirblocks, &first_key);
            block_index = new_block_index;
        }

        // Build the tree-0 details. Python treats directories specially:
        // their fingerprint and size are always empty / zero, even if
        // the caller passes a value.
        let minikind: Kind = kind.into();
        let tree0 = match kind {
            osutils::Kind::Directory => TreeData {
                minikind,
                fingerprint: Vec::new(),
                size: 0,
                executable: false,
                packed_stat: packed_stat.to_vec(),
            },
            osutils::Kind::TreeReference => TreeData {
                minikind,
                fingerprint: fingerprint.to_vec(),
                size: 0,
                executable: false,
                packed_stat: packed_stat.to_vec(),
            },
            osutils::Kind::File | osutils::Kind::Symlink => TreeData {
                minikind,
                fingerprint: fingerprint.to_vec(),
                size,
                executable: false,
                packed_stat: packed_stat.to_vec(),
            },
        };

        // Empty parent info: NULL_PARENT_DETAILS per present parent.
        let num_present = self.num_present_parents();
        let mut parent_info: Vec<TreeData> = (0..num_present)
            .map(|_| TreeData {
                minikind: Kind::Absent,
                fingerprint: Vec::new(),
                size: 0,
                executable: false,
                packed_stat: Vec::new(),
            })
            .collect();
        if let Some((old_dir, old_base)) = rename_from {
            // Replace parent_info[0] with a relocation pointer to the
            // old path. Matches Python's
            // `parent_info[0] = (b"r", old_path_utf8, 0, False, b"")`.
            let old_path_utf8 = if old_dir.is_empty() {
                old_base
            } else {
                let mut p = old_dir.clone();
                p.push(b'/');
                p.extend_from_slice(&old_base);
                p
            };
            if let Some(p0) = parent_info.get_mut(0) {
                *p0 = TreeData {
                    minikind: Kind::Relocated,
                    fingerprint: old_path_utf8,
                    size: 0,
                    executable: false,
                    packed_stat: Vec::new(),
                };
            }
        }

        let mut trees = vec![tree0];
        trees.extend(parent_info);

        let entry_key = EntryKey {
            dirname: dirname.to_vec(),
            basename: basename.to_vec(),
            file_id: file_id.to_vec(),
        };
        let (entry_index, present) =
            find_entry_index(&entry_key, &self.dirblocks[block_index].entries);
        if !present {
            self.dirblocks[block_index].entries.insert(
                entry_index,
                Entry {
                    key: entry_key.clone(),
                    trees,
                },
            );
            if let Some(idx) = self.id_index.as_mut() {
                idx.add((dirname, basename, &FileId::from(&file_id.to_vec())));
            }
        } else {
            let existing = &mut self.dirblocks[block_index].entries[entry_index];
            let current_t0 = existing.trees.first().map(|t| t.minikind);
            if current_t0 != Some(Kind::Absent) {
                return Err(AddError::AlreadyAddedAssertion {
                    basename: basename.to_vec(),
                    file_id: file_id.to_vec(),
                });
            }
            // Overwrite tree-0 only; leave parent slots alone.
            existing.trees[0] = trees.into_iter().next().unwrap();
        }

        if kind == osutils::Kind::Directory {
            // Python: _ensure_block(block_index, entry_index, utf8path).
            // We need to pass coordinates of the entry we just inserted
            // / overwrote. Re-find it since insertion may have shifted.
            let (eb, _) = find_block_index_from_key(&self.dirblocks, &entry_key);
            let (ei, _) = find_entry_index(&entry_key, &self.dirblocks[eb].entries);
            self.ensure_block(eb as isize, ei as isize, utf8path)
                .map_err(|e| AddError::Internal {
                    reason: format!("child ensure_block failed: {:?}", e),
                })?;
        }

        self.mark_modified(&[], false);
        Ok(())
    }

    /// Change the file id of the root path. Mirrors Python's
    /// `DirState.set_path_id`, which only supports `path=b""`.
    ///
    /// Python's original implementation called `_make_absent` on the
    /// old root entry (which mutated the shared tree-0 slot to
    /// NULL_PARENT_DETAILS when parent trees kept the entry alive)
    /// and then called `update_minimal` with
    /// `packed_stat=entry[1][0][4]`. The packed_stat observed by
    /// `update_minimal` therefore depended on whether the mutation
    /// had reset it: empty bytes when parents held the entry alive,
    /// the original stat otherwise. This port reproduces that rule
    /// explicitly.
    pub fn set_path_id(&mut self, path: &[u8], new_id: &[u8]) -> Result<(), SetPathIdError> {
        if !path.is_empty() {
            return Err(SetPathIdError::NonRootPath);
        }

        // Locate the current root entry in tree 0. Python's
        // `_get_entry(0, path_utf8=b"")` lookup.
        let bei = get_block_entry_index(&self.dirblocks, b"", b"", 0);
        if !bei.path_present {
            // Root entry must exist; if it does not, the dirstate is
            // malformed — report it rather than silently no-op.
            return Err(SetPathIdError::Internal {
                reason: "root entry missing".to_string(),
            });
        }
        let entry = &self.dirblocks[bei.block_index].entries[bei.entry_index];
        if entry.key.file_id == new_id {
            return Ok(());
        }

        // Capture the data we need before make_absent mutates state.
        let old_key = entry.key.clone();
        let original_packed_stat = entry
            .trees
            .first()
            .map(|t| t.packed_stat.clone())
            .unwrap_or_default();
        // If any parent tree kept the entry alive (minikind not in
        // {a, r}), the legacy code's make_absent-in-place mutation
        // reset packed_stat to empty bytes; update_minimal then stored
        // NULLSTAT in the new row. Preserve that observable behaviour.
        let parents_keep_entry = entry
            .trees
            .iter()
            .skip(1)
            .any(|t| t.minikind != Kind::Absent && t.minikind != Kind::Relocated);
        let packed_stat = if parents_keep_entry {
            Vec::new()
        } else {
            original_packed_stat
        };

        self.make_absent(&old_key)
            .map_err(|e| SetPathIdError::Internal {
                reason: format!("make_absent: {}", e),
            })?;

        let new_key = EntryKey {
            dirname: Vec::new(),
            basename: Vec::new(),
            file_id: new_id.to_vec(),
        };
        let tree0 = TreeData {
            minikind: Kind::Directory,
            fingerprint: Vec::new(),
            size: 0,
            executable: false,
            packed_stat,
        };
        self.update_minimal(new_key, tree0, Some(b""), false)
            .map_err(|e| SetPathIdError::Internal {
                reason: format!("update_minimal: {}", e),
            })?;

        self.mark_modified(&[], false);
        Ok(())
    }

    /// Apply a sequence of "removals" to tree 0, mirroring Python's
    /// `DirState._apply_removals`. Each record is a
    /// `(file_id, path)` tuple; the method sorts them in reverse
    /// path order (so deeper paths are removed first), locates the
    /// entry in tree 0, asserts it is present with the expected
    /// file_id, and calls [`DirState::make_absent`].
    ///
    /// After each removal the directory block that used to hold the
    /// removed entry's children is scanned for live tree-0 rows —
    /// any surviving row flags an inconsistent delta, matching
    /// Python's "file id was deleted but its children were not
    /// deleted" guard.
    pub fn apply_removals(
        &mut self,
        removals: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(), BasisApplyError> {
        // Sort by path in reverse so nested children come out before
        // their parents — matches Python's
        // `sorted(removals, reverse=True, key=operator.itemgetter(1))`.
        let mut sorted: Vec<&(Vec<u8>, Vec<u8>)> = removals.iter().collect();
        sorted.sort_by(|a, b| b.1.cmp(&a.1));

        for (file_id, path) in sorted {
            let (dirname, basename) = split_path_utf8(path);
            let bei = get_block_entry_index(&self.dirblocks, dirname, basename, 0);
            if !bei.path_present {
                return Err(BasisApplyError::Invalid {
                    path: path.clone(),
                    file_id: file_id.clone(),
                    reason: "Wrong path for old path.".to_string(),
                });
            }
            let entry_file_id = self.dirblocks[bei.block_index].entries[bei.entry_index]
                .key
                .file_id
                .clone();
            if entry_file_id != *file_id {
                return Err(BasisApplyError::Invalid {
                    path: path.clone(),
                    file_id: file_id.clone(),
                    reason: format!(
                        "Attempt to remove path has wrong id - found {:?}.",
                        entry_file_id
                    ),
                });
            }
            let target_key = self.dirblocks[bei.block_index].entries[bei.entry_index]
                .key
                .clone();
            self.make_absent(&target_key)
                .map_err(|e| BasisApplyError::Invalid {
                    path: path.clone(),
                    file_id: file_id.clone(),
                    reason: format!("{:?}", e),
                })?;

            // After-removal integrity check: if a dirblock for
            // `path` still exists in tree 0, none of its rows may
            // be live.
            let child_bei = get_block_entry_index(&self.dirblocks, path, b"", 0);
            if child_bei.dir_present {
                let block = &self.dirblocks[child_bei.block_index];
                for child in &block.entries {
                    if child.trees.first().map(|t| t.minikind).is_live() {
                        return Err(BasisApplyError::Invalid {
                            path: path.clone(),
                            file_id: file_id.clone(),
                            reason: "The file id was deleted but its children were not deleted."
                                .to_string(),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    /// Mirrors Python's `DirState._validate`. Walks the dirblocks
    /// and cross-references tree state invariants: root-block
    /// sentinel, dirblock ordering, per-block entry ordering,
    /// per-tree id→path consistency (absent / relocation /
    /// file-or-dir rules), parent-entry presence, and id_index
    /// back-references when the cache is populated.
    ///
    /// Returns `Ok(())` when all invariants hold, or a
    /// [`ValidateError`] describing the first violation — which the
    /// pyo3 layer turns into `AssertionError` to match Python.
    pub fn validate(&self) -> Result<(), ValidateError> {
        if !self.dirblocks.is_empty() && !self.dirblocks[0].dirname.is_empty() {
            return Err(ValidateError(
                "dirblocks don't start with root block".into(),
            ));
        }
        if self.dirblocks.len() > 1 && !self.dirblocks[1].dirname.is_empty() {
            return Err(ValidateError("dirblocks missing root directory".into()));
        }
        // dirblock names after the root pair must be in sorted
        // component order. Python does
        // `[d[0].split(b"/") for d in self._dirblocks[1:]]`.
        let dir_names: Vec<Vec<&[u8]>> = self
            .dirblocks
            .iter()
            .skip(1)
            .map(|d| d.dirname.split(|&b| b == b'/').collect())
            .collect();
        let mut sorted_dir_names = dir_names.clone();
        sorted_dir_names.sort();
        if dir_names != sorted_dir_names {
            return Err(ValidateError("dir names are not in sorted order".into()));
        }
        for dirblock in &self.dirblocks {
            for entry in &dirblock.entries {
                if dirblock.dirname != entry.key.dirname {
                    return Err(ValidateError(format!(
                        "entry key dirname {} doesn't match block directory name {}",
                        String::from_utf8_lossy(&entry.key.dirname),
                        String::from_utf8_lossy(&dirblock.dirname)
                    )));
                }
            }
            let key_tuple =
                |k: &EntryKey| (k.dirname.clone(), k.basename.clone(), k.file_id.clone());
            if !dirblock
                .entries
                .windows(2)
                .all(|w| key_tuple(&w[0].key) <= key_tuple(&w[1].key))
            {
                return Err(ValidateError(format!(
                    "dirblock for {:?} is not sorted",
                    dirblock.dirname
                )));
            }
        }

        // Per-tree id→path map. Each slot is
        // Option<(previous_path, previous_loc)> matching Python's
        // tuple: previous_path == None means "seen as absent",
        // otherwise it's the canonical path (for a live row) or the
        // relocation target (for a relocation row).
        type IdMap = std::collections::HashMap<Vec<u8>, (Option<Vec<u8>>, Vec<u8>)>;
        let tree_count = 1 + self.num_present_parents();
        let mut id_path_maps: Vec<IdMap> = (0..tree_count).map(|_| IdMap::new()).collect();
        for entry in self.iter_entries() {
            let file_id = &entry.key.file_id;
            let mut this_path = entry.key.dirname.clone();
            if !this_path.is_empty() {
                this_path.push(b'/');
            }
            this_path.extend_from_slice(&entry.key.basename);
            if entry.trees.len() != tree_count {
                return Err(ValidateError(format!(
                    "wrong number of entry details for {:?}, expected {}",
                    entry.key, tree_count
                )));
            }
            let mut absent_positions = 0usize;
            for (tree_index, tree_state) in entry.trees.iter().enumerate() {
                let minikind = tree_state.minikind;
                if minikind == Kind::Absent || minikind == Kind::Relocated {
                    absent_positions += 1;
                }
                if let Some((previous_path, previous_loc)) =
                    id_path_maps[tree_index].get(file_id.as_slice()).cloned()
                {
                    if minikind == Kind::Absent {
                        if previous_path.is_some() {
                            return Err(ValidateError(format!(
                                "file {} absent but previously present",
                                String::from_utf8_lossy(file_id)
                            )));
                        }
                    } else if minikind == Kind::Relocated {
                        let target = tree_state.fingerprint.clone();
                        if previous_path.as_deref() != Some(target.as_slice()) {
                            return Err(ValidateError(format!(
                                "relocation {} inconsistent with previous {:?}",
                                String::from_utf8_lossy(file_id),
                                previous_path.as_deref().map(String::from_utf8_lossy)
                            )));
                        }
                    } else {
                        if previous_path.as_deref() != Some(this_path.as_slice()) {
                            return Err(ValidateError(format!(
                                "entry {:?} inconsistent with previous path {:?} at {:?}",
                                entry.key, previous_path, previous_loc
                            )));
                        }
                        self.check_valid_parent(tree_index, &entry.key, &this_path)?;
                    }
                } else {
                    match minikind {
                        Kind::Absent => {
                            id_path_maps[tree_index]
                                .insert(file_id.to_vec(), (None, this_path.clone()));
                        }
                        Kind::Relocated => {
                            id_path_maps[tree_index].insert(
                                file_id.to_vec(),
                                (Some(tree_state.fingerprint.clone()), this_path.clone()),
                            );
                        }
                        Kind::File | Kind::Directory | Kind::Symlink | Kind::TreeReference => {
                            id_path_maps[tree_index].insert(
                                file_id.to_vec(),
                                (Some(this_path.clone()), this_path.clone()),
                            );
                            self.check_valid_parent(tree_index, &entry.key, &this_path)?;
                        }
                    }
                }
            }
            if absent_positions == tree_count {
                return Err(ValidateError(format!(
                    "entry {:?} has no data for any tree",
                    entry.key
                )));
            }
        }

        // id_index back-reference check, if the cache is built.
        if let Some(id_index) = &self.id_index {
            for (dirname, basename, file_id) in id_index.iter_all() {
                let lookup_key = EntryKey {
                    dirname: dirname.clone(),
                    basename: basename.clone(),
                    file_id: file_id.as_bytes().to_vec(),
                };
                let (block_index, present) =
                    find_block_index_from_key(&self.dirblocks, &lookup_key);
                if !present {
                    return Err(ValidateError(format!(
                        "missing block for entry key: {:?}",
                        lookup_key
                    )));
                }
                let (_, entry_present) =
                    find_entry_index(&lookup_key, &self.dirblocks[block_index].entries);
                if !entry_present {
                    return Err(ValidateError(format!(
                        "missing entry for key: {:?}",
                        lookup_key
                    )));
                }
            }
        }
        Ok(())
    }

    /// Helper for [`DirState::validate`] — mirrors Python's nested
    /// `check_valid_parent`. Verifies the containing directory
    /// entry exists and is marked as a directory in `tree_index`.
    /// The root row (empty dirname + empty basename) has no parent.
    fn check_valid_parent(
        &self,
        tree_index: usize,
        key: &EntryKey,
        this_path: &[u8],
    ) -> Result<(), ValidateError> {
        if key.dirname.is_empty() && key.basename.is_empty() {
            return Ok(());
        }
        let parent = self
            .get_entry_by_path(tree_index, &key.dirname)
            .ok_or_else(|| {
                ValidateError(format!(
                    "no parent entry for {:?} in tree {}",
                    this_path, tree_index
                ))
            })?;
        let parent_minikind = parent.trees.get(tree_index).map(|t| t.minikind);
        if parent_minikind != Some(Kind::Directory) {
            return Err(ValidateError(format!(
                "parent entry for {:?} is not a directory",
                this_path
            )));
        }
        Ok(())
    }

    /// Rebase the basis tree onto `new_revid`. Mirrors Python's
    /// `DirState.update_basis_by_delta` — the sibling of
    /// [`DirState::update_by_delta`] that rebases the basis tree.
    ///
    /// This encapsulates the full Python entrypoint:
    ///   1. `discard_merge_parents()` to drop all parents past the first.
    ///   2. Ghost-check: returns [`BasisApplyError::NotImplemented`]
    ///      when any ghost parent remains, matching Python's
    ///      `NotImplementedError`.
    ///   3. When the dirstate has no parents, extend every entry's
    ///      tree list with a `NULL_PARENT_DETAILS` row and append
    ///      `new_revid` to `parents`.
    ///   4. Replace `parents[0]` with `new_revid`.
    ///   5. Apply the pre-flattened, pre-sorted delta.
    ///   6. Mark modified and clear id_index.
    /// High-level entry point taking a native
    /// [`crate::inventory_delta::InventoryDelta`] directly — does the
    /// per-row file_id validation + inv_entry flattening Python's
    /// shim used to do before calling into Rust, then dispatches to
    /// [`DirState::update_basis_by_delta`].
    pub fn update_basis_by_delta_from_inventory_delta(
        &mut self,
        delta: &crate::inventory_delta::InventoryDelta,
        new_revid: Vec<u8>,
    ) -> Result<(), BasisApplyError> {
        let mut flat: Vec<FlatBasisDeltaEntry> = Vec::with_capacity(delta.len());
        for row in delta.iter() {
            let file_id_bytes = row.file_id.as_bytes().to_vec();

            if let Some(ref entry) = row.new_entry {
                if entry.file_id().as_bytes() != row.file_id.as_bytes() {
                    let new_path_bytes = row.new_path.as_deref().unwrap_or("").as_bytes().to_vec();
                    return Err(BasisApplyError::MismatchedEntryFileId {
                        new_path: new_path_bytes,
                        file_id: file_id_bytes,
                        entry_debug: format!("{:?}", entry),
                    });
                }
            }

            let (np_bytes, parent_id): (Option<Vec<u8>>, Option<Vec<u8>>) =
                match row.new_path.as_deref() {
                    None => (None, None),
                    Some(p) => {
                        let entry = row.new_entry.as_ref().ok_or_else(|| {
                            BasisApplyError::NewPathWithoutEntry {
                                new_path: p.as_bytes().to_vec(),
                                file_id: file_id_bytes.clone(),
                            }
                        })?;
                        let pid = entry.parent_id().map(|fid| fid.as_bytes().to_vec());
                        (Some(p.as_bytes().to_vec()), pid)
                    }
                };
            let op_bytes: Option<Vec<u8>> = row.old_path.as_deref().map(|p| p.as_bytes().to_vec());
            let details = row.new_entry.as_ref().map(|e| inv_entry_to_details(e));

            flat.push(FlatBasisDeltaEntry {
                old_path: op_bytes,
                new_path: np_bytes,
                file_id: file_id_bytes,
                parent_id,
                details,
            });
        }
        self.update_basis_by_delta(flat, new_revid)
    }

    pub fn update_basis_by_delta(
        &mut self,
        entries: Vec<FlatBasisDeltaEntry>,
        new_revid: Vec<u8>,
    ) -> Result<(), BasisApplyError> {
        self.discard_merge_parents();
        if !self.ghosts.is_empty() {
            return Err(BasisApplyError::NotImplemented {
                reason: "update_basis_by_delta with ghost parents".to_string(),
            });
        }
        if self.parents.is_empty() {
            self.bootstrap_new_parent_slot();
            self.parents.push(new_revid.clone());
        }
        self.parents[0] = new_revid;
        let result = self.update_basis_by_delta_inner(entries);
        if result.is_ok() {
            self.mark_modified(&[], true);
            self.id_index = None;
        }
        result
    }

    fn update_basis_by_delta_inner(
        &mut self,
        entries: Vec<FlatBasisDeltaEntry>,
    ) -> Result<(), BasisApplyError> {
        use std::collections::BTreeSet;

        let mut adds: Vec<BasisAdd> = Vec::new();
        let mut changes: Vec<(Vec<u8>, Vec<u8>, Vec<u8>, TreeData)> = Vec::new();
        let mut deletes: Vec<(Vec<u8>, Option<Vec<u8>>, Vec<u8>, bool)> = Vec::new();
        let mut parents_set: BTreeSet<(Vec<u8>, Vec<u8>)> = BTreeSet::new();
        let mut new_ids: Vec<Vec<u8>> = Vec::new();

        let details_to_tree_data = |d: &(Kind, Vec<u8>, u64, bool, Vec<u8>)| TreeData {
            minikind: d.0,
            fingerprint: d.1.clone(),
            size: d.2,
            executable: d.3,
            packed_stat: d.4.clone(),
        };

        for entry in entries {
            let FlatBasisDeltaEntry {
                old_path,
                new_path,
                file_id,
                parent_id,
                details,
            } = entry;
            if let Some(ref np) = new_path {
                let (dirname_utf8, basename_utf8) = split_path_utf8(np);
                if !basename_utf8.is_empty() {
                    let pid = parent_id.clone().unwrap_or_default();
                    parents_set.insert((dirname_utf8.to_vec(), pid));
                }
            }
            match (old_path.clone(), new_path.clone()) {
                (None, Some(np)) => {
                    let details = details.as_ref().expect("add must have details");
                    adds.push(BasisAdd {
                        old_path: None,
                        new_path: np,
                        file_id: file_id.clone(),
                        new_details: details_to_tree_data(details),
                        real_add: true,
                    });
                    new_ids.push(file_id);
                }
                (Some(op), None) => {
                    deletes.push((op, None, file_id, true));
                }
                (Some(op), Some(np)) if op.is_empty() && np.is_empty() => {
                    let details = details.as_ref().expect("change must have details");
                    changes.push((op, np, file_id, details_to_tree_data(details)));
                }
                (Some(op), Some(np)) => {
                    // Drain pending deletes before walking tree-1
                    // children of old_path — otherwise we'd see
                    // stale rows.
                    self.update_basis_apply_deletes(&deletes)?;
                    deletes.clear();
                    let details = details.as_ref().expect("rename must have details");
                    adds.push(BasisAdd {
                        old_path: Some(op.clone()),
                        new_path: np.clone(),
                        file_id: file_id.clone(),
                        new_details: details_to_tree_data(details),
                        real_add: false,
                    });
                    // Walk children of old_path in tree 1 in
                    // reverse (Python does `reversed(list(...))`)
                    // so deeper paths come out first.
                    let mut children = self.iter_child_entries(1, &op);
                    children.reverse();
                    for child in children {
                        let child_dirname = child.key.dirname.clone();
                        let child_basename = child.key.basename.clone();
                        let child_fid = child.key.file_id.clone();
                        let mut source_path = child_dirname.clone();
                        if !source_path.is_empty() {
                            source_path.push(b'/');
                        }
                        source_path.extend_from_slice(&child_basename);
                        let target_path = if !np.is_empty() {
                            let suffix = &source_path[op.len()..];
                            let mut t = np.clone();
                            t.extend_from_slice(suffix);
                            t
                        } else {
                            if op.is_empty() {
                                return Err(BasisApplyError::Internal {
                                    reason: "cannot rename directory to itself".to_string(),
                                });
                            }
                            source_path[op.len() + 1..].to_vec()
                        };
                        let child_tree1 = child.trees.get(1).cloned().unwrap_or(TreeData {
                            minikind: Kind::Absent,
                            fingerprint: Vec::new(),
                            size: 0,
                            executable: false,
                            packed_stat: Vec::new(),
                        });
                        adds.push(BasisAdd {
                            old_path: None,
                            new_path: target_path.clone(),
                            file_id: child_fid.clone(),
                            new_details: child_tree1,
                            real_add: false,
                        });
                        deletes.push((source_path, Some(target_path), child_fid, false));
                    }
                    deletes.push((op, Some(np), file_id, false));
                }
                (None, None) => {
                    return Err(BasisApplyError::Internal {
                        reason: "delta row with neither old_path nor new_path".to_string(),
                    });
                }
            }
        }

        self.check_delta_ids_absent(&new_ids, 1)?;
        self.update_basis_apply_deletes(&deletes)?;
        self.update_basis_apply_adds(&mut adds)?;
        self.update_basis_apply_changes(&changes)?;
        let parents_vec: Vec<(Vec<u8>, Vec<u8>)> = parents_set.into_iter().collect();
        self.after_delta_check_parents(&parents_vec, 1)?;
        Ok(())
    }

    /// Apply a pre-flattened inventory delta to tree 0. Mirrors
    /// Python's `DirState.update_by_delta` — the workhorse for
    /// `apply_inventory_delta` in dirstate-based trees.
    ///
    /// Each `entries` element is the Python-side extraction of one
    /// delta row: `(old_path, new_path, file_id, parent_id,
    /// minikind, executable, fingerprint)`. The Python caller is
    /// responsible for delta `.check()`/`.sort()` and for looking up
    /// `inv_entry.parent_id` / kind → minikind / `reference_revision`
    /// before calling this method.
    ///
    /// This function:
    /// 1. validates no repeated file_id,
    /// 2. accumulates `removals`, `insertions`, `new_ids`, `parents`,
    /// 3. expands each rename into delete+add pairs for all
    ///    descendant entries by walking [`DirState::iter_child_entries`],
    /// 4. calls `check_delta_ids_absent`, `apply_removals`,
    ///    `apply_insertions`, and `after_delta_check_parents` in
    ///    order — matching Python's try/except block exactly.
    /// High-level entry point taking a native
    /// [`crate::inventory_delta::InventoryDelta`] directly — does the
    /// per-row flattening Python's shim used to do and dispatches to
    /// [`DirState::update_by_delta`].
    pub fn update_by_delta_from_inventory_delta(
        &mut self,
        delta: &crate::inventory_delta::InventoryDelta,
    ) -> Result<(), BasisApplyError> {
        let mut flat: Vec<FlatDeltaEntry> = Vec::with_capacity(delta.len());
        for row in delta.iter() {
            let file_id_bytes = row.file_id.as_bytes().to_vec();
            let op_bytes: Option<Vec<u8>> = row.old_path.as_deref().map(|p| p.as_bytes().to_vec());
            let (np_bytes, parent_id, minikind, executable, fingerprint): (
                Option<Vec<u8>>,
                Option<Vec<u8>>,
                Kind,
                bool,
                Vec<u8>,
            ) = match row.new_path.as_deref() {
                None => (None, None, Kind::Absent, false, Vec::new()),
                Some(p) => {
                    let entry = row.new_entry.as_ref().ok_or_else(|| {
                        BasisApplyError::NewPathWithoutEntry {
                            new_path: p.as_bytes().to_vec(),
                            file_id: file_id_bytes.clone(),
                        }
                    })?;
                    let pid = entry.parent_id().map(|fid| fid.as_bytes().to_vec());
                    let details = inv_entry_to_details(entry);
                    let mk = details.0;
                    let fp = if mk == Kind::TreeReference {
                        details.1
                    } else {
                        Vec::new()
                    };
                    let ex = details.3;
                    (Some(p.as_bytes().to_vec()), pid, mk, ex, fp)
                }
            };
            flat.push(FlatDeltaEntry {
                old_path: op_bytes,
                new_path: np_bytes,
                file_id: file_id_bytes,
                parent_id,
                minikind,
                executable,
                fingerprint,
            });
        }
        self.update_by_delta(flat)
    }

    pub fn update_by_delta(&mut self, entries: Vec<FlatDeltaEntry>) -> Result<(), BasisApplyError> {
        use std::collections::{BTreeSet, HashMap};

        let mut insertions: HashMap<Vec<u8>, (EntryKey, Kind, bool, Vec<u8>, Vec<u8>)> =
            HashMap::new();
        let mut removals: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let mut parents_set: BTreeSet<(Vec<u8>, Vec<u8>)> = BTreeSet::new();
        let mut new_ids: Vec<Vec<u8>> = Vec::new();

        for entry in entries {
            let FlatDeltaEntry {
                old_path,
                new_path,
                file_id,
                parent_id,
                minikind,
                executable,
                fingerprint,
            } = entry;
            if insertions.contains_key(&file_id) || removals.contains_key(&file_id) {
                let path = old_path
                    .clone()
                    .or_else(|| new_path.clone())
                    .unwrap_or_default();
                return Err(BasisApplyError::Invalid {
                    path,
                    file_id,
                    reason: "repeated file_id".to_string(),
                });
            }
            if let Some(ref op) = old_path {
                removals.insert(file_id.clone(), op.clone());
            } else {
                new_ids.push(file_id.clone());
            }
            if let Some(ref np) = new_path {
                let (dirname_utf8, basename) = split_path_utf8(np);
                if !basename.is_empty() {
                    let pid = parent_id.clone().unwrap_or_default();
                    parents_set.insert((dirname_utf8.to_vec(), pid));
                }
                let key = EntryKey {
                    dirname: dirname_utf8.to_vec(),
                    basename: basename.to_vec(),
                    file_id: file_id.clone(),
                };
                insertions.insert(
                    file_id.clone(),
                    (key, minikind, executable, fingerprint.clone(), np.clone()),
                );
            }
            // Transform renames into delete+add pairs for all children.
            if let (Some(ref op), Some(ref np)) = (&old_path, &new_path) {
                let children = self.iter_child_entries(0, op);
                for child in children {
                    let child_id = child.key.file_id.clone();
                    if insertions.contains_key(&child_id) || removals.contains_key(&child_id) {
                        continue;
                    }
                    let child_dirname = child.key.dirname.clone();
                    let child_basename = child.key.basename.clone();
                    let child_tree0 = child.trees.first();
                    let child_minikind = child_tree0.map(|t| t.minikind).unwrap_or(Kind::Absent);
                    let child_fingerprint = child_tree0
                        .map(|t| t.fingerprint.clone())
                        .unwrap_or_default();
                    let child_executable = child_tree0.map(|t| t.executable).unwrap_or(false);
                    let mut old_child_path = child_dirname.clone();
                    if !old_child_path.is_empty() {
                        old_child_path.push(b'/');
                    }
                    old_child_path.extend_from_slice(&child_basename);
                    removals.insert(child_id.clone(), old_child_path);
                    // new_child_dirname = new_path + child_dirname[len(old_path):]
                    let suffix = &child_dirname[op.len()..];
                    let mut new_child_dirname = np.clone();
                    new_child_dirname.extend_from_slice(suffix);
                    let mut new_child_path = new_child_dirname.clone();
                    if !new_child_path.is_empty() {
                        new_child_path.push(b'/');
                    }
                    new_child_path.extend_from_slice(&child_basename);
                    let key = EntryKey {
                        dirname: new_child_dirname,
                        basename: child_basename,
                        file_id: child_id.clone(),
                    };
                    insertions.insert(
                        child_id,
                        (
                            key,
                            child_minikind,
                            child_executable,
                            child_fingerprint,
                            new_child_path,
                        ),
                    );
                }
            }
        }

        self.check_delta_ids_absent(&new_ids, 0)?;
        let removals_vec: Vec<(Vec<u8>, Vec<u8>)> = removals
            .into_iter()
            .map(|(fid, path)| (fid, path))
            .collect();
        self.apply_removals(&removals_vec)?;
        let insertions_vec: Vec<(EntryKey, Kind, bool, Vec<u8>, Vec<u8>)> =
            insertions.into_values().collect();
        self.apply_insertions(insertions_vec)?;
        let parents_vec: Vec<(Vec<u8>, Vec<u8>)> = parents_set.into_iter().collect();
        self.after_delta_check_parents(&parents_vec, 0)?;
        Ok(())
    }

    /// Apply a sequence of "insertions" to tree 0. Mirrors Python's
    /// `DirState._apply_insertions`: sort the adds and, for each,
    /// call [`DirState::update_minimal`]. A `NotVersioned` error
    /// from `update_minimal` is reshaped into `Invalid` with reason
    /// `"Missing parent"`, matching Python's
    /// `except NotVersionedError: self._raise_invalid(..., "Missing parent")`.
    pub fn apply_insertions(
        &mut self,
        adds: Vec<(EntryKey, Kind, bool, Vec<u8>, Vec<u8>)>,
    ) -> Result<(), BasisApplyError> {
        let mut sorted = adds;
        sorted.sort_by(|a, b| {
            a.0.dirname
                .cmp(&b.0.dirname)
                .then_with(|| a.0.basename.cmp(&b.0.basename))
                .then_with(|| a.0.file_id.cmp(&b.0.file_id))
        });
        for (key, minikind, executable, fingerprint, path_utf8) in sorted {
            let file_id = key.file_id.clone();
            let tree0_details = TreeData {
                minikind,
                fingerprint,
                size: 0,
                executable,
                packed_stat: b"x".repeat(32),
            };
            match self.update_minimal(key, tree0_details, Some(&path_utf8), false) {
                Ok(()) => {}
                Err(BasisApplyError::NotVersioned { .. }) => {
                    return Err(BasisApplyError::Invalid {
                        path: path_utf8,
                        file_id,
                        reason: "Missing parent".to_string(),
                    });
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Apply a sequence of "changes" to tree 1. Mirrors Python's
    /// `DirState._update_basis_apply_changes`. Each change updates
    /// the tree-1 slot of an existing entry whose file_id matches
    /// at the new path. The entry must already exist and be live
    /// (tree-1 minikind not absent/relocated); otherwise the caller
    /// sees `BasisApplyError::Invalid`.
    ///
    /// Invalidates id_index and packed_stat_index caches.
    pub fn update_basis_apply_changes(
        &mut self,
        changes: &[(Vec<u8>, Vec<u8>, Vec<u8>, TreeData)],
    ) -> Result<(), BasisApplyError> {
        for (_old_path, new_path, file_id, new_details) in changes {
            let (dirname, basename) = split_path_utf8(new_path);
            let bei = get_block_entry_index(&self.dirblocks, dirname, basename, 1);
            if !bei.path_present {
                return Err(BasisApplyError::Invalid {
                    path: new_path.clone(),
                    file_id: file_id.clone(),
                    reason: "changed entry considered not present".to_string(),
                });
            }
            let entry = &mut self.dirblocks[bei.block_index].entries[bei.entry_index];
            if entry.key.file_id != *file_id {
                return Err(BasisApplyError::Invalid {
                    path: new_path.clone(),
                    file_id: file_id.clone(),
                    reason: "changed entry considered not present".to_string(),
                });
            }
            if entry.trees.get(1).map(|t| t.minikind).is_not_live() {
                return Err(BasisApplyError::Invalid {
                    path: new_path.clone(),
                    file_id: file_id.clone(),
                    reason: "changed entry considered not present".to_string(),
                });
            }
            if entry.trees.len() >= 2 {
                entry.trees[1] = new_details.clone();
            } else {
                entry.trees.push(new_details.clone());
            }
        }
        self.id_index = None;
        self.packed_stat_index = None;
        Ok(())
    }

    /// Apply a sequence of "deletes" to tree 1. Mirrors Python's
    /// `DirState._update_basis_apply_deletes`. Each delete either
    /// removes an entry row entirely (when the active tree is also
    /// absent/relocated) or sets its tree-1 slot to NULL_PARENT_DETAILS
    /// so the file id survives in the active tree. The post-delete
    /// dirblock integrity check walks child blocks to ensure no live
    /// rows were left behind; that check follows Python exactly.
    ///
    /// Each tuple is `(old_path, Option<new_path>, file_id, real_delete)`
    /// where `real_delete` must equal `new_path.is_none()` — otherwise
    /// the caller sees `BasisApplyError::Invalid("bad delete delta")`.
    ///
    /// Invalidates id_index and packed_stat_index caches.
    pub fn update_basis_apply_deletes(
        &mut self,
        deletes: &[(Vec<u8>, Option<Vec<u8>>, Vec<u8>, bool)],
    ) -> Result<(), BasisApplyError> {
        for (old_path, new_path, file_id, real_delete) in deletes {
            if *real_delete != new_path.is_none() {
                return Err(BasisApplyError::Invalid {
                    path: old_path.clone(),
                    file_id: file_id.clone(),
                    reason: "bad delete delta".to_string(),
                });
            }

            let (dirname, basename) = split_path_utf8(old_path);
            let bei = get_block_entry_index(&self.dirblocks, dirname, basename, 1);
            if !bei.path_present {
                return Err(BasisApplyError::Invalid {
                    path: old_path.clone(),
                    file_id: file_id.clone(),
                    reason: "basis tree does not contain removed entry".to_string(),
                });
            }
            let (active_kind, old_kind, entry_file_id): (Option<Kind>, Option<Kind>, Vec<u8>) = {
                let entry = &self.dirblocks[bei.block_index].entries[bei.entry_index];
                (
                    entry.trees.first().map(|t| t.minikind),
                    entry.trees.get(1).map(|t| t.minikind),
                    entry.key.file_id.clone(),
                )
            };
            if entry_file_id != *file_id {
                return Err(BasisApplyError::Invalid {
                    path: old_path.clone(),
                    file_id: file_id.clone(),
                    reason: "mismatched file_id in tree 1".to_string(),
                });
            }

            // The dirblock whose children are then scanned for
            // live-row leaks. `None` when no follow-up check is
            // needed.
            let mut dir_block_index: Option<usize> = None;

            if active_kind.is_not_live() {
                if active_kind == Some(Kind::Relocated) {
                    // Follow the tree-0 relocation pointer and
                    // clear the target's tree-1 slot.
                    let active_path = self.dirblocks[bei.block_index].entries[bei.entry_index]
                        .trees[0]
                        .fingerprint
                        .clone();
                    let (adirname, abasename) = split_path_utf8(&active_path);
                    let abei = get_block_entry_index(&self.dirblocks, adirname, abasename, 0);
                    if !abei.path_present {
                        return Err(BasisApplyError::Invalid {
                            path: old_path.clone(),
                            file_id: file_id.clone(),
                            reason: "Dirstate did not have matching rename entries".to_string(),
                        });
                    }
                    let (a_t0, a_t1): (Option<Kind>, Option<Kind>) = {
                        let ae = &self.dirblocks[abei.block_index].entries[abei.entry_index];
                        (
                            ae.trees.first().map(|t| t.minikind),
                            ae.trees.get(1).map(|t| t.minikind),
                        )
                    };
                    if a_t1 != Some(Kind::Relocated) {
                        return Err(BasisApplyError::Invalid {
                            path: old_path.clone(),
                            file_id: file_id.clone(),
                            reason: "Dirstate did not have matching rename entries".to_string(),
                        });
                    }
                    if !matches!(a_t0, Some(k) if k.is_fdlt()) {
                        return Err(BasisApplyError::Invalid {
                            path: old_path.clone(),
                            file_id: file_id.clone(),
                            reason: "Dirstate had a rename pointing at an inactive tree0"
                                .to_string(),
                        });
                    }
                    let ae = &mut self.dirblocks[abei.block_index].entries[abei.entry_index];
                    let null = TreeData {
                        minikind: Kind::Absent,
                        fingerprint: Vec::new(),
                        size: 0,
                        executable: false,
                        packed_stat: Vec::new(),
                    };
                    if ae.trees.len() >= 2 {
                        ae.trees[1] = null;
                    } else {
                        ae.trees.push(null);
                    }
                }

                self.dirblocks[bei.block_index]
                    .entries
                    .remove(bei.entry_index);

                if old_kind == Some(Kind::Directory) {
                    let dirblock_key = EntryKey {
                        dirname: old_path.clone(),
                        basename: Vec::new(),
                        file_id: Vec::new(),
                    };
                    let (db_index, db_present) =
                        find_block_index_from_key(&self.dirblocks, &dirblock_key);
                    if db_present {
                        if self.dirblocks[db_index].entries.is_empty() {
                            self.dirblocks.remove(db_index);
                        } else {
                            dir_block_index = Some(db_index);
                        }
                    }
                }
            } else {
                let entry = &mut self.dirblocks[bei.block_index].entries[bei.entry_index];
                let null = TreeData {
                    minikind: Kind::Absent,
                    fingerprint: Vec::new(),
                    size: 0,
                    executable: false,
                    packed_stat: Vec::new(),
                };
                if entry.trees.len() >= 2 {
                    entry.trees[1] = null;
                } else {
                    entry.trees.push(null);
                }

                let child_bei = get_block_entry_index(&self.dirblocks, old_path, b"", 1);
                if child_bei.dir_present {
                    dir_block_index = Some(child_bei.block_index);
                }
            }

            if let Some(db_index) = dir_block_index {
                let block = &self.dirblocks[db_index];
                for child in &block.entries {
                    if child.trees.get(1).map(|t| t.minikind).is_live() {
                        return Err(BasisApplyError::Invalid {
                            path: old_path.clone(),
                            file_id: file_id.clone(),
                            reason: "The file id was deleted but its children were not deleted."
                                .to_string(),
                        });
                    }
                }
            }
        }

        self.id_index = None;
        self.packed_stat_index = None;
        Ok(())
    }

    /// Look up the dirstate entry for `file_id` in `tree_index`,
    /// following any relocation chain the entries describe. Mirrors
    /// the `fileid_utf8` branch of Python's `DirState._get_entry`.
    ///
    /// If `include_deleted` is true, an entry whose tree data is
    /// absent (`Kind::Absent`) is returned rather than hidden.
    /// Returns [`GetEntryResult::NotFound`] if no key for `file_id`
    /// exists in the id index, or [`GetEntryResult::Entry`] with the
    /// located entry key on success.  Unknown minikinds are impossible
    /// by construction — [`Kind`] only parses the six valid codes.
    ///
    /// The result is returned as an owned [`EntryKey`] rather than a
    /// borrow because the caller may need to keep `self` borrowable
    /// for other lookups; callers that need the full entry can
    /// re-fetch it via [`DirState::find_block_index_from_key`] and
    /// [`DirState::find_entry_index`].
    pub fn get_entry_by_file_id(
        &mut self,
        tree_index: usize,
        file_id: &[u8],
        include_deleted: bool,
    ) -> GetEntryResult {
        // Copy out the candidate keys so we can drop the borrow on
        // `self.id_index` and mutate other state during the scan.
        let candidates = {
            let idx = self.get_or_build_id_index();
            idx.get(&FileId::from(&file_id.to_vec()))
        };
        if candidates.is_empty() {
            return GetEntryResult::NotFound;
        }

        // Follow relocation chains until we hit a live entry, an
        // absent entry, or run out of candidate keys. Bounded by the
        // number of relocation hops the dirstate actually contains;
        // the `visited` set guards against pathological cycles.
        let mut current: Vec<EntryKey> = candidates
            .into_iter()
            .map(|(d, b, f)| EntryKey {
                dirname: d,
                basename: b,
                file_id: f.as_bytes().to_vec(),
            })
            .collect();
        let mut visited: HashSet<EntryKey> = HashSet::new();

        loop {
            let mut relocation_target: Option<Vec<u8>> = None;
            for key in &current {
                if !visited.insert(key.clone()) {
                    continue;
                }
                let (block_index, present) = find_block_index_from_key(&self.dirblocks, key);
                // "strange, probably indicates an out of date id index" —
                // Python's comment: silently skip stale entries.
                if !present {
                    continue;
                }
                let block = &self.dirblocks[block_index].entries;
                let (entry_index, entry_present) = find_entry_index(key, block);
                if !entry_present {
                    continue;
                }
                let entry = &block[entry_index];
                let Some(tree) = entry.trees.get(tree_index) else {
                    continue;
                };
                match tree.minikind {
                    k if k.is_fdlt() => {
                        return GetEntryResult::Entry(entry.key.clone());
                    }
                    Kind::Absent => {
                        if include_deleted {
                            return GetEntryResult::Entry(entry.key.clone());
                        }
                        return GetEntryResult::NotFound;
                    }
                    Kind::Relocated => {
                        // Follow the relocation by recursing via the
                        // `real_path` fingerprint.
                        relocation_target = Some(tree.fingerprint.clone());
                        break;
                    }
                    _ => unreachable!(),
                }
            }
            match relocation_target {
                Some(real_path) => {
                    // The relocation target is a path — Python just
                    // recurses with the same fileid_utf8 and the new
                    // path, walking the id index again. We mirror that
                    // by filtering the candidate set down to keys that
                    // match the (dirname, basename) split of the real
                    // path, leaving the file_id constraint in place.
                    let (dirname, basename) = split_path_utf8(&real_path);
                    let all = self
                        .get_or_build_id_index()
                        .get(&FileId::from(&file_id.to_vec()));
                    current = all
                        .into_iter()
                        .filter(|(d, b, _)| d == dirname && b == basename)
                        .map(|(d, b, f)| EntryKey {
                            dirname: d,
                            basename: b,
                            file_id: f.as_bytes().to_vec(),
                        })
                        .collect();
                    if current.is_empty() {
                        return GetEntryResult::NotFound;
                    }
                }
                None => return GetEntryResult::NotFound,
            }
        }
    }

    /// Remove `entries[index]` from `entries` (and drop it from
    /// `id_index`) if none of its trees hold a live record — i.e.
    /// every tree column is `b'a'` (absent) or `b'r'` (relocation).
    /// Mirrors Python's `DirState._maybe_remove_row`.
    ///
    /// Returns `true` if the row was removed, `false` otherwise.
    pub fn maybe_remove_row(
        entries: &mut Vec<Entry>,
        index: usize,
        id_index: &mut IdIndex,
    ) -> bool {
        let entry = &entries[index];
        let present_in_row = entry
            .trees
            .iter()
            .any(|t| t.minikind != Kind::Absent && t.minikind != Kind::Relocated);
        if present_in_row {
            return false;
        }
        let file_id = FileId::from(&entry.key.file_id);
        id_index.remove((
            entry.key.dirname.as_slice(),
            entry.key.basename.as_slice(),
            &file_id,
        ));
        entries.remove(index);
        true
    }

    /// Sort `entries` into canonical dirblock order. Mirrors Python's
    /// `DirState._sort_entries`: the sort key is
    /// `(dirname.split(b"/"), basename, file_id)`, which matches the
    /// order `_entries_to_current_state` expects before writing.
    ///
    /// The Python version caches `dirname → split` because real-world
    /// calls re-sort ~10× more entries than distinct directories;
    /// Rust's `sort_by_cached_key` gets the same amortisation
    /// automatically.
    pub fn sort_entries(entries: &mut [Entry]) {
        entries.sort_by_cached_key(|e| {
            (
                e.key
                    .dirname
                    .split(|&b| b == b'/')
                    .map(|s| s.to_vec())
                    .collect::<Vec<Vec<u8>>>(),
                e.key.basename.clone(),
                e.key.file_id.clone(),
            )
        });
    }

    /// Return references to every dirstate entry whose key `(dirname,
    /// basename)` matches `path_utf8`, across all file ids. Mirrors
    /// Python's `DirState._entries_for_path`: a path can be represented
    /// by multiple rows when the same location held different file ids
    /// in different parent trees, so the lookup walks the block
    /// starting at the first matching entry and stops at the first
    /// non-match. Returns an empty list when no block exists for the
    /// parent directory.
    pub fn entries_for_path(&self, path_utf8: &[u8]) -> Vec<&Entry> {
        let (dirname, basename) = split_path_utf8(path_utf8);
        let key = EntryKey {
            dirname: dirname.to_vec(),
            basename: basename.to_vec(),
            file_id: Vec::new(),
        };
        let (block_index, present) = self.find_block_index_from_key(&key);
        if !present {
            return Vec::new();
        }
        let block = &self.dirblocks[block_index].entries;
        let (mut entry_index, _) = self.find_entry_index(&key, block);
        let mut result = Vec::new();
        while entry_index < block.len() {
            let candidate = &block[entry_index];
            if candidate.key.dirname != key.dirname || candidate.key.basename != key.basename {
                break;
            }
            result.push(candidate);
            entry_index += 1;
        }
        result
    }

    /// Look up the dirstate entry at `path_utf8` in `tree_index` and
    /// return a reference to it, or `None` if the path is not present
    /// in that tree. Mirrors the `path_utf8` branch of Python's
    /// `DirState._get_entry` (the file-id fallback is a follow-up port
    /// once `_get_id_index` exists in Rust).
    ///
    /// `path_utf8` is split on the last `/` into a `(dirname, basename)`
    /// pair matching `osutils.split`, then fed through
    /// [`DirState::get_block_entry_index`]. The result points at a
    /// live (non-absent, non-relocated) entry only when `path_present`
    /// is true; otherwise `None` is returned.
    pub fn get_entry_by_path(&self, tree_index: usize, path_utf8: &[u8]) -> Option<&Entry> {
        let (dirname, basename) = split_path_utf8(path_utf8);
        let bei = self.get_block_entry_index(dirname, basename, tree_index);
        if !bei.path_present {
            return None;
        }
        self.dirblocks
            .get(bei.block_index)
            .and_then(|b| b.entries.get(bei.entry_index))
    }

    /// Walk the subtree rooted at `path_utf8` and return every live
    /// entry (kind not in `b'a'`/`b'r'`) in `tree_index`, in the order
    /// Python's `DirState._iter_child_entries` yields them.
    ///
    /// The walk is breadth-first: all immediate children of `path_utf8`
    /// first, then all children of those (grouped by whichever parent
    /// they were enqueued from). Directory entries whose tree data says
    /// they're directories (`b'd'`) are recursed into; absent and
    /// relocated entries are filtered out of the output but do not
    /// suppress the recursion into other entries.
    ///
    /// An empty `path_utf8` walks the top of the tree. Asking for the
    /// children of a non-directory returns an empty vector.
    pub fn iter_child_entries(&self, tree_index: usize, path_utf8: &[u8]) -> Vec<Entry> {
        let mut out: Vec<Entry> = Vec::new();
        let mut next_pending: Vec<Vec<u8>> = vec![path_utf8.to_vec()];
        while !next_pending.is_empty() {
            let pending = std::mem::take(&mut next_pending);
            for path in pending {
                let lookup_key = EntryKey {
                    dirname: path.clone(),
                    basename: Vec::new(),
                    file_id: Vec::new(),
                };
                let (mut block_index, present) =
                    find_block_index_from_key(&self.dirblocks, &lookup_key);
                // Python treats block_index 0 as a special case: the
                // caller asked for the root, and the first real block
                // with root entries lives at index 1. If there are no
                // other blocks we're done.
                if block_index == 0 {
                    block_index = 1;
                    if self.dirblocks.len() == 1 {
                        return out;
                    }
                } else if !present {
                    // children of a non-directory asked for.
                    continue;
                }
                if block_index >= self.dirblocks.len() {
                    continue;
                }
                let block = &self.dirblocks[block_index];
                for entry in &block.entries {
                    let kind = entry
                        .trees
                        .get(tree_index)
                        .map(|t| t.minikind)
                        .unwrap_or(Kind::Absent);
                    if !kind.is_absent_or_relocated() {
                        out.push(entry.clone());
                    }
                    if kind == Kind::Directory {
                        // Build `dirname/basename` for the recursion.
                        let next_path = if entry.key.dirname.is_empty() {
                            entry.key.basename.clone()
                        } else {
                            let mut p = entry.key.dirname.clone();
                            p.push(b'/');
                            p.extend_from_slice(&entry.key.basename);
                            p
                        };
                        next_pending.push(next_path);
                    }
                }
            }
        }
        out
    }

    /// Bisect the on-disk dirstate for rows at the given paths.
    /// Mirrors Python's `DirState._bisect`.
    ///
    /// `read_range(offset, len)` must return the bytes at `[offset,
    /// offset+len)` from the dirstate file. `file_size` is the full
    /// file length (used to bound the initial bisect window). The
    /// caller must have already loaded the header (so
    /// `end_of_header` and `num_present_parents()` are populated)
    /// and must hold a read or write lock on the file.
    ///
    /// Returns a map from `path_utf8` → list of entries at that path
    /// (an entry is the usual `(key, [tree_data, ...])` shape).
    /// Missing paths do not appear in the map.
    pub fn bisect<F>(
        &self,
        paths: Vec<Vec<u8>>,
        file_size: u64,
        mut read_range: F,
    ) -> Result<std::collections::HashMap<Vec<u8>, Vec<Entry>>, BisectError>
    where
        F: FnMut(u64, usize) -> Result<Vec<u8>, BisectError>,
    {
        bisect_bytes(
            self.end_of_header.unwrap_or(0),
            file_size,
            self.num_present_parents(),
            paths,
            BisectMode::Paths,
            &mut read_range,
        )
    }

    /// Bisect the on-disk dirstate for every entry whose dirname is
    /// in `dir_list`. Mirrors Python's `DirState._bisect_dirblocks`.
    pub fn bisect_dirblocks<F>(
        &self,
        dir_list: Vec<Vec<u8>>,
        file_size: u64,
        mut read_range: F,
    ) -> Result<std::collections::HashMap<Vec<u8>, Vec<Entry>>, BisectError>
    where
        F: FnMut(u64, usize) -> Result<Vec<u8>, BisectError>,
    {
        bisect_bytes(
            self.end_of_header.unwrap_or(0),
            file_size,
            self.num_present_parents(),
            dir_list,
            BisectMode::Dirnames,
            &mut read_range,
        )
    }

    /// Recursive variant of `bisect`: for every path in `paths` find
    /// the row and, if it is a directory, recursively bisect for its
    /// children. Renames are followed via the fingerprint pointer.
    /// Mirrors `DirState._bisect_recursive`.
    ///
    /// Returns a map from `(dirname, basename, file_id)` → list of
    /// tree-data rows.
    #[allow(clippy::type_complexity)]
    pub fn bisect_recursive<F>(
        &self,
        paths: Vec<Vec<u8>>,
        file_size: u64,
        mut read_range: F,
    ) -> Result<std::collections::HashMap<(Vec<u8>, Vec<u8>, Vec<u8>), Vec<TreeData>>, BisectError>
    where
        F: FnMut(u64, usize) -> Result<Vec<u8>, BisectError>,
    {
        use std::collections::{HashMap, HashSet};
        let mut found: HashMap<(Vec<u8>, Vec<u8>, Vec<u8>), Vec<TreeData>> = HashMap::new();
        let mut found_dir_names: HashSet<(Vec<u8>, Vec<u8>)> = HashSet::new();
        let mut processed_dirs: HashSet<Vec<u8>> = HashSet::new();

        // Seed: run bisect() on the initial path list.
        let mut newly_found = bisect_bytes(
            self.end_of_header.unwrap_or(0),
            file_size,
            self.num_present_parents(),
            paths,
            BisectMode::Paths,
            &mut read_range,
        )?;

        while !newly_found.is_empty() {
            let mut pending_dirs: Vec<Vec<u8>> = Vec::new();
            let mut paths_to_search: Vec<Vec<u8>> = Vec::new();
            for entries in newly_found.values() {
                for entry in entries {
                    let key = (
                        entry.key.dirname.clone(),
                        entry.key.basename.clone(),
                        entry.key.file_id.clone(),
                    );
                    found.insert(key.clone(), entry.trees.clone());
                    found_dir_names.insert((entry.key.dirname.clone(), entry.key.basename.clone()));
                    let mut is_dir = false;
                    for tree_info in &entry.trees {
                        match tree_info.minikind {
                            Kind::Directory => {
                                if is_dir {
                                    continue;
                                }
                                is_dir = true;
                                let mut path = entry.key.dirname.clone();
                                if !path.is_empty() {
                                    path.push(b'/');
                                }
                                path.extend_from_slice(&entry.key.basename);
                                if !processed_dirs.contains(&path) {
                                    pending_dirs.push(path);
                                }
                            }
                            Kind::Relocated => {
                                let (dn, _bn) = split_path_utf8(&tree_info.fingerprint);
                                if pending_dirs.iter().any(|p| p == dn) {
                                    continue;
                                }
                                let dn_vec = dn.to_vec();
                                let (rdn, rbn) = split_path_utf8(&tree_info.fingerprint);
                                if !found_dir_names.contains(&(rdn.to_vec(), rbn.to_vec())) {
                                    paths_to_search.push(tree_info.fingerprint.clone());
                                    let _ = dn_vec; // silence warning
                                }
                            }
                            Kind::Absent | Kind::File | Kind::Symlink | Kind::TreeReference => {}
                        }
                    }
                }
            }
            paths_to_search.sort();
            paths_to_search.dedup();
            pending_dirs.sort();
            pending_dirs.dedup();

            newly_found = bisect_bytes(
                self.end_of_header.unwrap_or(0),
                file_size,
                self.num_present_parents(),
                paths_to_search,
                BisectMode::Paths,
                &mut read_range,
            )?;
            let dir_results = bisect_bytes(
                self.end_of_header.unwrap_or(0),
                file_size,
                self.num_present_parents(),
                pending_dirs.clone(),
                BisectMode::Dirnames,
                &mut read_range,
            )?;
            for (k, v) in dir_results {
                newly_found.insert(k, v);
            }
            for d in pending_dirs {
                processed_dirs.insert(d);
            }
        }

        Ok(found)
    }
}

mod bisect;
pub use bisect::BisectError;
use bisect::{bisect_bytes, cmp_by_dirs_bytes, BisectMode};

// The bisect implementation (~300 lines) lives in ``bisect.rs``.
// Every call site below that refers to ``bisect_bytes`` /
// ``BisectMode`` / ``cmp_by_dirs_bytes`` picks them up through the
// module-local ``use`` above.

mod errors;
pub use errors::{
    AddError, BasisAdd, BasisApplyError, EnsureBlockError, EntriesToStateError,
    FlatBasisDeltaEntry, FlatDeltaEntry, MakeAbsentError, SetPathIdError, SplitRootError,
    UpdateEntryError, ValidateError,
};

/// Seconds-since-epoch from a [`Metadata::modified`] reading.  Returns
/// 0 when the platform does not carry the information.
/// Convert a byte-encoded filesystem path into a `PathBuf`.  On unix
/// this is a zero-copy `OsString::from_vec`; on other platforms we
/// fall back to utf8 decoding.  Callers that hold a `&[u8]` from the
/// Transport contract use this to talk to `SHA1Provider::sha1` which
/// still takes a `&Path`.
fn bytes_to_path(bytes: &[u8]) -> PathBuf {
    #[cfg(unix)]
    {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;
        PathBuf::from(OsString::from_vec(bytes.to_vec()))
    }
    #[cfg(not(unix))]
    {
        PathBuf::from(String::from_utf8_lossy(bytes).into_owned())
    }
}

#[cfg(test)]
fn metadata_mtime_secs(m: &Metadata) -> i64 {
    m.modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Seconds-since-epoch from the filesystem's "changed" timestamp.  On
/// Unix we read `st_ctime` directly; on other platforms we fall back
/// to `created()` which is the closest analogue.
#[cfg(test)]
fn metadata_ctime_secs(m: &Metadata) -> i64 {
    #[cfg(unix)]
    {
        m.ctime()
    }
    #[cfg(not(unix))]
    {
        m.created()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }
}

/// Pure-function version of [`DirState::split_root_dirblock_into_contents`].
/// Exposed so callers that are still building a `Vec<Dirblock>` outside of
/// a full `DirState` (e.g. the pyo3 shim) can reuse the same logic.
/// Split a NUL-free dirstate `dirname` on `/` into its path components.
/// Mirrors the `split_object` helper inside the Python and pyo3
/// implementations of `bisect_dirblock`; the comparison is then
/// lexicographic-by-component rather than lexicographic-by-byte, which is
/// the ordering dirblocks use on disk.
fn split_dirname(dirname: &[u8]) -> Vec<&[u8]> {
    dirname.split(|&b| b == b'/').collect()
}

/// Split `path` on the last `/` into a `(dirname, basename)` pair,
/// matching `bzrformats.osutils.split`. Paths with no `/` map to
/// `(b"", path)`; `b""` itself maps to `(b"", b"")`.
fn split_path_utf8(path: &[u8]) -> (&[u8], &[u8]) {
    match path.iter().rposition(|&b| b == b'/') {
        Some(i) => (&path[..i], &path[i + 1..]),
        None => (b"".as_slice(), path),
    }
}

/// Find the insertion position for a directory name within `dirblocks`,
/// using component-wise comparison on the dirname. Mirrors the pyo3
/// `bisect_dirblock` function in `crates/bazaar-py/src/dirstate.rs` but
/// operates on a plain `&[Dirblock]` slice rather than Python objects.
///
/// `lo` defaults to 0 (Python's default is 1, which callers pass
/// explicitly to skip the sentinel root block); we require the caller to
/// be explicit to avoid hiding the sentinel-skipping convention.
pub fn bisect_dirblock(dirblocks: &[Dirblock], dirname: &[u8], lo: usize, hi: usize) -> usize {
    let target = split_dirname(dirname);
    let mut lo = lo;
    let mut hi = hi;
    while lo < hi {
        let mid = (lo + hi) / 2;
        let cur = split_dirname(&dirblocks[mid].dirname);
        if cur < target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// Find the block index containing the key's `(dirname, basename)` —
/// pure-Rust counterpart of `DirState._find_block_index_from_key`. The
/// second tuple element is `true` when the returned index actually points
/// at a block whose dirname equals `key.dirname` (i.e. the block exists),
/// and `false` when the index is the position at which a block for that
/// dirname *would* be inserted.
///
/// This function does not consult or update the `last_block_index` cache
/// Python maintains; callers that want the cache should use
/// [`DirState::find_block_index_from_key`] instead.
pub fn find_block_index_from_key(dirblocks: &[Dirblock], key: &EntryKey) -> (usize, bool) {
    // Python's fast path: `(b"", b"")` always lives in block 0.
    if key.dirname.is_empty() && key.basename.is_empty() {
        return (0, true);
    }
    // Skip the first sentinel block (index 0); `_right`-style bisect
    // over the rest matches Python's `bisect_dirblock(..., 1, ...)` call.
    let block_index = bisect_dirblock(dirblocks, &key.dirname, 1, dirblocks.len());
    let present = block_index < dirblocks.len() && dirblocks[block_index].dirname == key.dirname;
    (block_index, present)
}

/// Compare `(dirname, basename, file_id)` keys in the tuple order Python
/// uses when Python's `bisect.bisect_left(block, (key, []))` walks
/// entries. The `file_id` is the third tuple element so the ordering here
/// matches Python's native tuple comparison.
fn entry_key_cmp(a: &EntryKey, b: &EntryKey) -> Ordering {
    match a.dirname.cmp(&b.dirname) {
        Ordering::Equal => match a.basename.cmp(&b.basename) {
            Ordering::Equal => a.file_id.cmp(&b.file_id),
            other => other,
        },
        other => other,
    }
}

/// Find the entry index for `key` within `block`. Returns the insertion
/// index and whether an exact match was found. Mirrors
/// `DirState._find_entry_index` in the simpler "no cache" form —
/// Python's version also consults `self._last_entry_index` as a
/// one-slot cache, but the caching layer is additive and lives on the
/// `DirState` method wrapper.
pub fn find_entry_index(key: &EntryKey, block: &[Entry]) -> (usize, bool) {
    // bisect_left over entry keys.
    let mut lo = 0;
    let mut hi = block.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        match entry_key_cmp(&block[mid].key, key) {
            Ordering::Less => lo = mid + 1,
            _ => hi = mid,
        }
    }
    let present = lo < block.len() && block[lo].key == *key;
    (lo, present)
}

/// Result of [`DirState::get_entry_by_file_id`]. Mirrors the
/// `(entry, None)` / `None` return pattern Python uses for
/// `DirState._get_entry`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GetEntryResult {
    /// No entry for the requested file_id exists in the given tree.
    NotFound,
    /// The located entry's key. The full entry can be re-fetched via
    /// [`DirState::find_block_index_from_key`] +
    /// [`DirState::find_entry_index`] if the caller needs the trees.
    Entry(EntryKey),
}

/// Result of [`get_block_entry_index`]: the four-tuple Python returns,
/// giving coordinates of where a `(dirname, basename)` pair lives — or
/// should be inserted — in the dirblocks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockEntryIndex {
    /// Block index within `dirblocks`.
    pub block_index: usize,
    /// Entry index within the block at `block_index`.
    pub entry_index: usize,
    /// `true` when the directory (i.e. a block with the target dirname)
    /// exists anywhere in the dirstate.
    pub dir_present: bool,
    /// `true` when the specific `(dirname, basename)` exists in
    /// `tree_index` with a non-absent / non-relocated entry.
    pub path_present: bool,
}

/// Pure-Rust counterpart to `DirState._get_block_entry_index`.
///
/// Walks the block for `(dirname, basename)` to find the first entry in
/// `tree_index` whose minikind is neither `b'a'` (absent) nor `b'r'`
/// (relocated). Callers use this both for membership tests and for
/// computing the insertion point when adding new entries.
pub fn get_block_entry_index(
    dirblocks: &[Dirblock],
    dirname: &[u8],
    basename: &[u8],
    tree_index: usize,
) -> BlockEntryIndex {
    let key = EntryKey {
        dirname: dirname.to_vec(),
        basename: basename.to_vec(),
        file_id: Vec::new(),
    };
    let (block_index, dir_present) = find_block_index_from_key(dirblocks, &key);
    if !dir_present {
        return BlockEntryIndex {
            block_index,
            entry_index: 0,
            dir_present: false,
            path_present: false,
        };
    }
    let block = &dirblocks[block_index].entries;
    let (mut entry_index, _) = find_entry_index(&key, block);
    // Linear scan over the contiguous run of entries sharing the same
    // (dirname, basename), skipping absent/relocated variants for the
    // requested tree. Mirrors the Python loop at dirstate.py:2254.
    while entry_index < block.len()
        && block[entry_index].key.dirname == key.dirname
        && block[entry_index].key.basename == key.basename
    {
        if let Some(tree) = block[entry_index].trees.get(tree_index) {
            if tree.minikind != Kind::Absent && tree.minikind != Kind::Relocated {
                return BlockEntryIndex {
                    block_index,
                    entry_index,
                    dir_present: true,
                    path_present: true,
                };
            }
        }
        entry_index += 1;
    }
    BlockEntryIndex {
        block_index,
        entry_index,
        dir_present: true,
        path_present: false,
    }
}

pub fn split_root_dirblock_into_contents(dirblocks: &mut [Dirblock]) -> Result<(), SplitRootError> {
    if dirblocks.len() < 2 {
        return Err(SplitRootError::MissingSentinels);
    }
    // Python: `if self._dirblocks[1] != (b"", []): raise ValueError(...)`.
    // The second sentinel is always empty after parse_dirblocks; anything
    // else means the caller already mutated the layout.
    if !dirblocks[1].dirname.is_empty() || !dirblocks[1].entries.is_empty() {
        return Err(SplitRootError::BadSecondSentinel {
            dirname: dirblocks[1].dirname.clone(),
            entry_count: dirblocks[1].entries.len(),
        });
    }

    let block_zero = std::mem::take(&mut dirblocks[0].entries);
    let (root_entries, contents_of_root): (Vec<Entry>, Vec<Entry>) = block_zero
        .into_iter()
        .partition(|entry| entry.key.basename.is_empty());
    dirblocks[0].entries = root_entries;
    dirblocks[1].entries = contents_of_root;
    Ok(())
}

#[cfg(test)]
mod tests;

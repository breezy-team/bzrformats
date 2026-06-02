//! Reading a 2a (groupcompress + CHK) pack repository.
//!
//! Layout under `.bzr/repository/` (see [`crate::bzrdir`]):
//! `pack-names` lists the packs and the byte sizes of each pack's five
//! indices; `packs/<name>.pack` holds groupcompress blocks; and
//! `indices/<name>.{rix,iix,tix,six,cix}` map keys to byte ranges in the
//! `.pack`.
//!
//! The heavy lifting (block fetch, decompression, delta reconstruction,
//! record extraction) is already implemented by
//! [`GroupCompressVersionedFiles`](crate::groupcompress::gcvf::GroupCompressVersionedFiles).
//! This module supplies the two backends it needs — a [`GcIndex`] over the
//! per-suffix btree indices and a [`GcAccess`] that reads raw bytes from
//! the `.pack` files — and wires them up for the revision, inventory,
//! text and chk stores.

use std::collections::HashMap;

use crate::btree_graph_index::BTreeGraphIndex;
use crate::groupcompress::gcvf::{
    GcAccess, GcBuildDetails, GcIndex, GroupCompressVersionedFiles, IndexMemo, ReadMemo,
};
use crate::knit::KnitError;
use crate::pack_repo::{index_extension, IndexKind};
use crate::transport::{Transport, TransportError};
use crate::versionedfile::Key;

/// The pack name is used as the groupcompress `FileRef`, identifying which
/// `.pack` file a block lives in.
type PackName = String;

/// Errors from reading a 2a repository.
#[derive(Debug)]
pub enum RepositoryError {
    /// A required object was not found in the repository.
    NoSuchRevision(Vec<u8>),
    /// An index value or record could not be parsed.
    Corrupt(String),
    /// An underlying transport error.
    Transport(TransportError),
    /// An error from the groupcompress layer.
    Knit(KnitError),
    /// An index file could not be read.
    Index(crate::btree_graph_index::IndexError),
}

impl std::fmt::Display for RepositoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RepositoryError::NoSuchRevision(r) => {
                write!(f, "no such revision: {}", String::from_utf8_lossy(r))
            }
            RepositoryError::Corrupt(m) => write!(f, "corrupt repository data: {m}"),
            RepositoryError::Transport(e) => write!(f, "transport error: {e}"),
            RepositoryError::Knit(e) => write!(f, "groupcompress error: {e}"),
            RepositoryError::Index(e) => write!(f, "index error: {e}"),
        }
    }
}

impl std::error::Error for RepositoryError {}

impl From<TransportError> for RepositoryError {
    fn from(e: TransportError) -> Self {
        RepositoryError::Transport(e)
    }
}

impl From<KnitError> for RepositoryError {
    fn from(e: KnitError) -> Self {
        RepositoryError::Knit(e)
    }
}

impl From<crate::btree_graph_index::IndexError> for RepositoryError {
    fn from(e: crate::btree_graph_index::IndexError) -> Self {
        RepositoryError::Index(e)
    }
}

/// Parse an index entry value (`b"start length [basis_end delta_end]"`)
/// into the `(start, length, entry_start, entry_end)` a groupcompress
/// record needs. When the basis/delta pair is absent the whole block is
/// the record.
fn parse_index_value(value: &[u8]) -> Result<(u64, u64, u64, u64), RepositoryError> {
    let text = std::str::from_utf8(value)
        .map_err(|_| RepositoryError::Corrupt("index value not utf-8".to_string()))?;
    let parts: Vec<&str> = text.split(' ').collect();
    let parse = |s: &str| -> Result<u64, RepositoryError> {
        s.parse::<u64>()
            .map_err(|_| RepositoryError::Corrupt(format!("bad integer in index value: {text:?}")))
    };
    match parts.as_slice() {
        [start, length] => {
            let start = parse(start)?;
            let length = parse(length)?;
            Ok((start, length, 0, 0))
        }
        [start, length, basis_end, delta_end] => Ok((
            parse(start)?,
            parse(length)?,
            parse(basis_end)?,
            parse(delta_end)?,
        )),
        _ => Err(RepositoryError::Corrupt(format!(
            "unexpected index value shape: {text:?}"
        ))),
    }
}

/// A [`GcIndex`] built from the per-pack btree indices of one kind
/// (revisions, inventories, texts or chk), merged across all packs.
///
/// Each key resolves to the pack it lives in plus the record's location
/// inside that pack's groupcompress data.
struct PackGcIndex {
    /// key -> (build details, graph parents).
    entries: HashMap<Key, GcBuildDetails<PackName>>,
    /// Whether the underlying index stores graph parents.
    has_graph: bool,
}

impl PackGcIndex {
    /// Build the combined index for `kind` across `packs`, reading each
    /// pack's index file via `transport` (rooted at `.bzr/repository`).
    fn load(
        transport: &dyn Transport,
        packs: &[PackName],
        kind: IndexKind,
    ) -> Result<Self, RepositoryError> {
        let ext = index_extension(kind);
        let mut entries: HashMap<Key, GcBuildDetails<PackName>> = HashMap::new();
        let mut has_graph = false;
        for pack in packs {
            let name = format!("indices/{pack}{ext}");
            let index = BTreeGraphIndex::open(transport, &name)?;
            if index.node_ref_lists() > 0 {
                has_graph = true;
            }
            for (key, value, refs) in index.iter_all_entries() {
                let (start, length, basis_end, delta_end) = parse_index_value(value)?;
                let read_memo = ReadMemo::new(pack.clone(), start, start + length);
                let index_memo = IndexMemo::new(read_memo, basis_end, delta_end);
                let parents = if index.node_ref_lists() > 0 {
                    let first = refs.first().cloned().unwrap_or_default();
                    Some(first.into_iter().map(Key::fixed).collect())
                } else {
                    None
                };
                entries.insert(
                    Key::fixed(key.clone()),
                    GcBuildDetails {
                        index_memo,
                        parents,
                    },
                );
            }
        }
        Ok(PackGcIndex { entries, has_graph })
    }
}

impl GcIndex for PackGcIndex {
    type F = PackName;

    fn get_build_details(
        &self,
        keys: &[Key],
    ) -> Result<HashMap<Key, GcBuildDetails<Self::F>>, KnitError> {
        let mut out = HashMap::new();
        for key in keys {
            if let Some(details) = self.entries.get(key) {
                out.insert(key.clone(), details.clone());
            }
        }
        Ok(out)
    }

    fn get_parent_map(&self, keys: &[Key]) -> Result<HashMap<Key, Vec<Key>>, KnitError> {
        let mut out = HashMap::new();
        for key in keys {
            if let Some(details) = self.entries.get(key) {
                if let Some(parents) = &details.parents {
                    out.insert(key.clone(), parents.clone());
                }
            }
        }
        Ok(out)
    }

    fn keys(&self) -> Result<Vec<Key>, KnitError> {
        Ok(self.entries.keys().cloned().collect())
    }

    fn has_graph(&self) -> bool {
        self.has_graph
    }

    fn check_write_ok(&self) -> Result<(), KnitError> {
        Err(KnitError::Corrupt("read-only index".to_string()))
    }

    fn add_records(
        &self,
        _records: &[(Key, IndexMemo<Self::F>, Option<Vec<Key>>)],
        _random_id: bool,
    ) -> Result<(), KnitError> {
        Err(KnitError::Corrupt("read-only index".to_string()))
    }
}

pub use crate::transport::SharedTransport;

/// A [`GcAccess`] that reads raw groupcompress block bytes from the
/// `.pack` files of the repository.
struct PackGcAccess {
    transport: SharedTransport,
    /// Cache of whole pack files, keyed by pack name. The packs a single
    /// repository produces are small enough to hold in memory; this avoids
    /// re-reading the file for every record.
    cache: std::sync::Mutex<HashMap<PackName, std::sync::Arc<Vec<u8>>>>,
}

impl PackGcAccess {
    fn new(transport: SharedTransport) -> Self {
        PackGcAccess {
            transport,
            cache: std::sync::Mutex::new(HashMap::new()),
        }
    }

    fn pack_bytes(&self, pack: &str) -> Result<std::sync::Arc<Vec<u8>>, KnitError> {
        if let Some(bytes) = self.cache.lock().unwrap().get(pack) {
            return Ok(bytes.clone());
        }
        let path = format!("packs/{pack}.pack");
        let bytes = self
            .transport
            .get_bytes(&path)
            .map_err(|e| KnitError::Corrupt(format!("reading {path}: {e}")))?;
        let arc = std::sync::Arc::new(bytes);
        self.cache
            .lock()
            .unwrap()
            .insert(pack.to_string(), arc.clone());
        Ok(arc)
    }
}

impl GcAccess for PackGcAccess {
    type F = PackName;

    fn get_raw_records(&self, memos: &[ReadMemo<Self::F>]) -> Result<Vec<Vec<u8>>, KnitError> {
        let mut out = Vec::with_capacity(memos.len());
        for memo in memos {
            let bytes = self.pack_bytes(&memo.index)?;
            let start = memo.start as usize;
            let stop = memo.stop as usize;
            if stop > bytes.len() || start > stop {
                return Err(KnitError::Corrupt(format!(
                    "record range {start}..{stop} outside pack {} (len {})",
                    memo.index,
                    bytes.len()
                )));
            }
            // The index range covers a whole container Bytes record
            // (`B<len>\n<names>\n<body>`); the groupcompress block is the
            // record body.
            let body = crate::pack::read_bytes_record_body(&bytes[start..stop])
                .map_err(|e| KnitError::Corrupt(format!("reading pack record: {e}")))?;
            out.push(body);
        }
        Ok(out)
    }

    fn add_raw_record(
        &self,
        _size: usize,
        _chunks: Vec<Vec<u8>>,
    ) -> Result<ReadMemo<Self::F>, KnitError> {
        Err(KnitError::Corrupt("read-only access".to_string()))
    }
}

/// A groupcompress store for one kind of object in the repository.
type Store = GroupCompressVersionedFiles<PackGcIndex, PackGcAccess>;

/// Build the groupcompress store for one index kind across all packs.
fn build_store(
    transport: &SharedTransport,
    packs: &[PackName],
    kind: IndexKind,
) -> Result<Store, RepositoryError> {
    let index = PackGcIndex::load(transport.as_ref(), packs, kind)?;
    let access = PackGcAccess::new(transport.clone());
    Ok(GroupCompressVersionedFiles::new(index, access, false))
}

/// The CHK byte store as a trait object, so it can be shared with the
/// `CHKInventory`s it materializes without leaking the concrete store
/// type into the public API.
type SharedChkStore = std::sync::Arc<dyn crate::versionedfile::VersionedFiles + Send + Sync>;

/// A 2a pack repository.
///
/// Reading is available immediately after [`open`](Self::open). Writing
/// follows breezy's write-group lifecycle on the same object:
/// [`start_write_group`](Self::start_write_group), then `add_*`, then
/// [`commit_write_group`](Self::commit_write_group). The new-pack machinery
/// is private (the [`WriteGroup`]); there is no separate writer type.
pub struct Pack2aRepository {
    transport: SharedTransport,
    revisions: Store,
    inventories: Store,
    texts: Store,
    /// The CHK byte store, shared with the `CHKInventory`s it materializes.
    chk_bytes: SharedChkStore,
    /// The in-progress write group, if one is open.
    write_group: Option<super::pack_2a_writer::WriteGroup>,
}

impl Pack2aRepository {
    /// Open the repository whose `.bzr/repository` directory is rooted at
    /// `transport`.
    pub fn open(transport: SharedTransport) -> Result<Self, RepositoryError> {
        let packs = read_pack_names(transport.as_ref())?;
        let revisions = build_store(&transport, &packs, IndexKind::Revision)?;
        let inventories = build_store(&transport, &packs, IndexKind::Inventory)?;
        let texts = build_store(&transport, &packs, IndexKind::Text)?;
        let chk_bytes: SharedChkStore =
            std::sync::Arc::new(build_store(&transport, &packs, IndexKind::Chk)?);
        Ok(Pack2aRepository {
            transport,
            revisions,
            inventories,
            texts,
            chk_bytes,
            write_group: None,
        })
    }

    /// Create an empty 2a repository at `transport` (rooted at the
    /// `.bzr/repository` directory), then open it.
    ///
    /// Writes the `format` marker, an empty `pack-names`, and the
    /// `indices/` and `packs/` directories.
    pub fn create(transport: SharedTransport) -> Result<Self, RepositoryError> {
        transport.mkdir("indices")?;
        transport.mkdir("packs")?;
        transport.put_bytes(
            "format",
            b"Bazaar repository format 2a (needs bzr 1.16 or later)\n",
        )?;
        // An empty pack-names index: no packs yet.
        let empty = crate::btree_builder::BTreeBuilder::new(0, 1)
            .finish()
            .map_err(|e| RepositoryError::Corrupt(format!("empty pack-names: {e:?}")))?;
        transport.put_bytes("pack-names", &empty)?;
        Self::open(transport)
    }

    /// The list of pack names, read fresh from `pack-names`.
    #[allow(dead_code)]
    fn pack_names(&self) -> Result<Vec<PackName>, RepositoryError> {
        read_pack_names(self.transport.as_ref())
    }

    /// All revision ids stored in this repository.
    pub fn all_revision_ids(&self) -> Result<Vec<Vec<u8>>, RepositoryError> {
        let mut ids: Vec<Vec<u8>> = self
            .revisions
            .keys()?
            .into_iter()
            .filter_map(|k| k.segments().first().cloned())
            .collect();
        ids.sort();
        Ok(ids)
    }

    /// Read and parse a revision by id.
    pub fn get_revision(
        &self,
        revision_id: &[u8],
    ) -> Result<crate::revision::Revision, RepositoryError> {
        use crate::serializer::RevisionSerializer;
        let key = Key::fixed(vec![revision_id.to_vec()]);
        let mut stream = self.revisions.get_record_stream(&[key], "unordered")?;
        let record = stream
            .pop()
            .ok_or_else(|| RepositoryError::NoSuchRevision(revision_id.to_vec()))?;
        // An absent record yields an AbsentContentFactory whose fulltext is
        // empty and whose storage kind is "absent".
        if record.storage_kind() == "absent" {
            return Err(RepositoryError::NoSuchRevision(revision_id.to_vec()));
        }
        let bytes = record.to_fulltext().into_owned();
        crate::bencode_serializer::BEncodeRevisionSerializer1
            .read_revision_from_string(&bytes)
            .map_err(|e| RepositoryError::Corrupt(format!("revision parse: {e:?}")))
    }

    /// Read the CHK inventory for a revision.
    ///
    /// Reads the serialised `CHKInventory` header from the inventories
    /// store, then materializes it by walking the CHK maps through the
    /// shared chk-bytes store.
    pub fn get_inventory(
        &self,
        revision_id: &[u8],
    ) -> Result<
        crate::chk_inventory::CHKInventory<dyn crate::versionedfile::VersionedFiles + Send + Sync>,
        RepositoryError,
    > {
        let key = Key::fixed(vec![revision_id.to_vec()]);
        let mut stream = self.inventories.get_record_stream(&[key], "unordered")?;
        let record = stream
            .pop()
            .ok_or_else(|| RepositoryError::NoSuchRevision(revision_id.to_vec()))?;
        if record.storage_kind() == "absent" {
            return Err(RepositoryError::NoSuchRevision(revision_id.to_vec()));
        }
        let lines: Vec<Vec<u8>> = record.to_lines().map(|l| l.into_owned()).collect();
        let cache: std::sync::Arc<dyn crate::chk_map::PageCache> =
            std::sync::Arc::new(crate::chk_map::InMemoryPageCache::new());
        let rev_id = crate::RevisionId::from(revision_id);
        crate::chk_inventory::CHKInventory::deserialise(
            self.chk_bytes.clone(),
            cache,
            &lines,
            &rev_id,
        )
        .map_err(|e| RepositoryError::Corrupt(format!("inventory deserialise: {e:?}")))
    }

    /// Read the full text of a versioned file at a given revision.
    ///
    /// Texts are keyed by `(file_id, revision)` — the revision that last
    /// modified the file, as recorded in its inventory entry (not
    /// necessarily the revision being inspected).
    pub fn get_file_text(
        &self,
        file_id: &[u8],
        revision: &[u8],
    ) -> Result<Vec<u8>, RepositoryError> {
        let key = Key::fixed(vec![file_id.to_vec(), revision.to_vec()]);
        let mut stream = self.texts.get_record_stream(&[key], "unordered")?;
        let record = stream.pop().ok_or_else(|| {
            RepositoryError::Corrupt(format!(
                "no text for ({}, {})",
                String::from_utf8_lossy(file_id),
                String::from_utf8_lossy(revision)
            ))
        })?;
        if record.storage_kind() == "absent" {
            return Err(RepositoryError::Corrupt(format!(
                "no text for ({}, {})",
                String::from_utf8_lossy(file_id),
                String::from_utf8_lossy(revision)
            )));
        }
        Ok(record.to_fulltext().into_owned())
    }

    /// Open a write group: subsequent `add_*` calls accumulate into a new
    /// pack, made durable by [`commit_write_group`](Self::commit_write_group).
    /// Errors if a write group is already open.
    pub fn start_write_group(&mut self) -> Result<(), RepositoryError> {
        if self.write_group.is_some() {
            return Err(RepositoryError::Corrupt(
                "a write group is already open".to_string(),
            ));
        }
        let pack_name = new_pack_name();
        self.write_group = Some(super::pack_2a_writer::WriteGroup::new(&pack_name)?);
        Ok(())
    }

    fn write_group_mut(&mut self) -> Result<&super::pack_2a_writer::WriteGroup, RepositoryError> {
        self.write_group
            .as_ref()
            .ok_or_else(|| RepositoryError::Corrupt("no write group is open".to_string()))
    }

    /// Add a revision (already serialised to bencode bytes) to the open
    /// write group.
    pub fn add_revision(
        &mut self,
        revision_id: &[u8],
        parents: &[Vec<u8>],
        bytes: &[u8],
    ) -> Result<(), RepositoryError> {
        self.write_group_mut()?
            .add_revision(revision_id, parents, bytes)
    }

    /// Build a CHK inventory from `entries` and add it to the open write
    /// group, returning the inventory sha1 to record on the revision.
    pub fn add_inventory_from_entries(
        &mut self,
        revision_id: &[u8],
        parents: &[Vec<u8>],
        root_id: &[u8],
        entries: &[crate::inventory::Entry],
    ) -> Result<Vec<u8>, RepositoryError> {
        self.write_group_mut()?
            .add_inventory_from_entries(revision_id, parents, root_id, entries)
    }

    /// Add a file text (keyed by `(file_id, revision)`) to the open write
    /// group.
    pub fn add_text(
        &mut self,
        file_id: &[u8],
        revision: &[u8],
        parents: &[(Vec<u8>, Vec<u8>)],
        bytes: &[u8],
    ) -> Result<(), RepositoryError> {
        self.write_group_mut()?
            .add_text(file_id, revision, parents, bytes)
    }

    /// Flush the open write group: write its pack, indices and an updated
    /// `pack-names`. After this, re-open the repository to read the newly
    /// committed data (the in-memory read stores are not refreshed).
    pub fn commit_write_group(&mut self) -> Result<(), RepositoryError> {
        let group = self
            .write_group
            .take()
            .ok_or_else(|| RepositoryError::Corrupt("no write group is open".to_string()))?;
        let existing = read_pack_names_with_values(self.transport.as_ref())?;
        group.finish(self.transport.as_ref(), &existing)?;
        Ok(())
    }
}

/// Generate a fresh 32-hex-character pack name.
///
/// brz derives the name from a hash of the pack contents; on disk any
/// unique 32-hex token is valid, so a random one suffices.
fn new_pack_name() -> String {
    crate::osutils::rand_chars(32)
        .chars()
        .map(|ch| char::from_digit((ch as u32) % 16, 16).unwrap())
        .collect()
}

/// Read `pack-names`, returning each `(pack_name, value_bytes)` pair.
fn read_pack_names_with_values(
    transport: &dyn Transport,
) -> Result<Vec<(String, Vec<u8>)>, RepositoryError> {
    let index = BTreeGraphIndex::open(transport, "pack-names")?;
    let mut out = Vec::new();
    for (key, value, _refs) in index.iter_all_entries() {
        if let Some(name) = key.first() {
            out.push((String::from_utf8_lossy(name).into_owned(), value.clone()));
        }
    }
    Ok(out)
}

/// Read `pack-names` and return the pack names in it.
fn read_pack_names(transport: &dyn Transport) -> Result<Vec<PackName>, RepositoryError> {
    let index = BTreeGraphIndex::open(transport, "pack-names")?;
    let mut names = Vec::new();
    for (key, _value, _refs) in index.iter_all_entries() {
        if let Some(name) = key.first() {
            names.push(String::from_utf8_lossy(name).into_owned());
        }
    }
    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serializer::RevisionSerializer;
    use crate::transport::LocalTransport;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_revision(
        id: &[u8],
        parents: Vec<&[u8]>,
        message: &str,
        inv_sha1: Option<Vec<u8>>,
    ) -> crate::revision::Revision {
        crate::revision::Revision::new(
            crate::RevisionId::from(id),
            parents.into_iter().map(crate::RevisionId::from).collect(),
            Some("Test User <test@example.com>".to_string()),
            message.to_string(),
            HashMap::new(),
            inv_sha1,
            1577880000.0,
            Some(0),
        )
    }

    fn temp_repo() -> (tempfile::TempDir, SharedTransport) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("repository");
        std::fs::create_dir_all(&path).unwrap();
        let t: SharedTransport = Arc::new(LocalTransport::new(&path));
        (dir, t)
    }

    #[test]
    fn revision_and_text_write_round_trip() {
        let ser = crate::bencode_serializer::BEncodeRevisionSerializer1;
        let (_d, t) = temp_repo();
        let mut repo = Pack2aRepository::create(t.clone()).unwrap();
        repo.start_write_group().unwrap();
        let r1 = ser
            .write_revision_to_string(&make_revision(b"rev-1", vec![], "first", None))
            .unwrap();
        let r2 = ser
            .write_revision_to_string(&make_revision(b"rev-2", vec![b"rev-1"], "second", None))
            .unwrap();
        repo.add_revision(b"rev-1", &[], &r1).unwrap();
        repo.add_revision(b"rev-2", &[b"rev-1".to_vec()], &r2)
            .unwrap();
        repo.add_text(b"file-1", b"rev-1", &[], b"hello world\n")
            .unwrap();
        repo.commit_write_group().unwrap();

        // Re-open to read the committed data.
        let repo = Pack2aRepository::open(t).unwrap();
        let mut ids = repo.all_revision_ids().unwrap();
        ids.sort();
        assert_eq!(ids, vec![b"rev-1".to_vec(), b"rev-2".to_vec()]);
        assert_eq!(repo.get_revision(b"rev-2").unwrap().message, "second");
        assert_eq!(
            repo.get_file_text(b"file-1", b"rev-1").unwrap(),
            b"hello world\n"
        );
    }

    #[test]
    fn chk_inventory_write_round_trip() {
        use crate::inventory::Entry;
        use crate::FileId;
        let ser = crate::bencode_serializer::BEncodeRevisionSerializer1;
        let (_d, t) = temp_repo();
        let mut repo = Pack2aRepository::create(t.clone()).unwrap();
        repo.start_write_group().unwrap();

        let rev = b"rev-1";
        let root_id = crate::inventory::ROOT_ID;
        // One file under the root.
        let text = b"hello\n";
        let sha1 = crate::weave::sha_strings(&[&text[..]]);
        repo.add_text(b"file-1", rev, &[], text).unwrap();
        let entries = vec![
            // The root directory must be present in the inventory.
            Entry::root(
                FileId::from(root_id),
                Some(crate::RevisionId::from(&rev[..])),
            ),
            Entry::file(
                FileId::from(&b"file-1"[..]),
                "a.txt".to_string(),
                FileId::from(root_id),
                Some(crate::RevisionId::from(&rev[..])),
                Some(sha1.clone()),
                Some(text.len() as u64),
                Some(false),
                None,
            ),
        ];
        let inv_sha1 = repo
            .add_inventory_from_entries(rev, &[], root_id, &entries)
            .unwrap();
        let rbytes = ser
            .write_revision_to_string(&make_revision(rev, vec![], "commit", Some(inv_sha1)))
            .unwrap();
        repo.add_revision(rev, &[], &rbytes).unwrap();
        repo.commit_write_group().unwrap();

        // Re-open and materialize the inventory.
        let repo = Pack2aRepository::open(t).unwrap();
        let inv = repo.get_inventory(rev).unwrap();
        let entries: Vec<_> = inv.entries().unwrap();
        let paths: Vec<String> = entries.iter().map(|(p, _)| p.clone()).collect();
        assert_eq!(paths, vec!["a.txt".to_string()]);
        assert_eq!(
            repo.get_file_text(b"file-1", rev).unwrap(),
            b"hello\n".to_vec()
        );
    }
}

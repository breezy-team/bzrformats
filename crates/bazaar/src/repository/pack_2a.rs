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

/// A [`GcAccess`] that reads raw groupcompress block bytes from the
/// `.pack` files of the repository.
struct PackGcAccess<'t> {
    transport: &'t dyn Transport,
    /// Cache of whole pack files, keyed by pack name. The packs a single
    /// repository produces are small enough to hold in memory; this avoids
    /// re-reading the file for every record.
    cache: std::sync::Mutex<HashMap<PackName, std::sync::Arc<Vec<u8>>>>,
}

impl<'t> PackGcAccess<'t> {
    fn new(transport: &'t dyn Transport) -> Self {
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

impl GcAccess for PackGcAccess<'_> {
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
type Store<'t> = GroupCompressVersionedFiles<PackGcIndex, PackGcAccess<'t>>;

/// A read-only view of a 2a pack repository.
pub struct Pack2aRepository<'t> {
    revisions: Store<'t>,
}

impl<'t> Pack2aRepository<'t> {
    /// Open the repository whose `.bzr/repository` directory is rooted at
    /// `transport`.
    pub fn open(transport: &'t dyn Transport) -> Result<Self, RepositoryError> {
        let packs = read_pack_names(transport)?;
        let rev_index = PackGcIndex::load(transport, &packs, IndexKind::Revision)?;
        let rev_access = PackGcAccess::new(transport);
        let revisions = GroupCompressVersionedFiles::new(rev_index, rev_access, false);
        Ok(Pack2aRepository { revisions })
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

//! The write-group machinery for a 2a (groupcompress + CHK) pack
//! repository.
//!
//! This is the private implementation behind the write methods on
//! [`Pack2aRepository`](super::Pack2aRepository): callers open or create a
//! repository and write through `start_write_group` / `add_*` /
//! `commit_write_group`, mirroring breezy. [`WriteGroup`] is the in-progress
//! new pack that backs that lifecycle; it is not a public type.
//!
//! It groupcompress-compresses records into a single new `.pack`, builds
//! the five per-pack btree indices, and writes `pack-names`. The
//! compression and block framing are done by the existing
//! [`GroupCompressVersionedFiles::insert_record_stream`], driven through
//! writable [`GcAccess`]/[`GcIndex`] backends defined here:
//!
//! - [`PackWritingAccess`] appends each groupcompress block to a shared
//!   container writer (one `.pack` for all object kinds) and reports where
//!   it landed.
//! - `PackWritingIndex` collects `(key, location, parents)` for one object
//!   kind, later serialised into that kind's btree index.
//!
//! All object kinds share one `.pack` but each has its own index, matching
//! the on-disk layout the reader expects.

use std::sync::{Arc, Mutex};

use crate::btree_builder::BTreeBuilder;
use crate::groupcompress::gcvf::{
    GcAccess, GcBuildDetails, GcIndex, GroupCompressVersionedFiles, IndexMemo, ReadMemo,
};
use crate::knit::KnitError;
use crate::pack::ContainerWriter;
use crate::pack_repo::{index_extension, IndexKind};
use crate::transport::Transport;
use crate::versionedfile::Key;

use super::pack_2a::RepositoryError;

/// The pack name (used as the groupcompress `FileRef`).
type PackName = String;

/// Which of a write group's stores a repack copy targets.
#[derive(Clone, Copy)]
pub(super) enum RepackTarget {
    Revisions,
    Inventories,
    Texts,
    Signatures,
    Chk,
}

/// The growing `.pack` container, shared by every object kind's store.
struct SharedPack {
    writer: ContainerWriter<Vec<u8>>,
}

/// A writable [`GcAccess`] that appends groupcompress blocks to a shared
/// `.pack` container writer.
#[derive(Clone)]
struct PackWritingAccess {
    pack_name: PackName,
    pack: Arc<Mutex<SharedPack>>,
}

impl GcAccess for PackWritingAccess {
    type F = PackName;

    fn get_raw_records(&self, memos: &[ReadMemo<Self::F>]) -> Result<Vec<Vec<u8>>, KnitError> {
        // Read records back from the in-memory pack buffer being written, so
        // a delta-based inventory build can read CHK pages it just wrote.
        let pack = self.pack.lock().unwrap();
        let bytes = pack.writer.get_ref();
        let mut out = Vec::with_capacity(memos.len());
        for memo in memos {
            let start = memo.start as usize;
            let stop = memo.stop as usize;
            if stop > bytes.len() || start > stop {
                return Err(KnitError::Corrupt(format!(
                    "record range {start}..{stop} outside pack buffer (len {})",
                    bytes.len()
                )));
            }
            let body = crate::pack::read_bytes_record_body(&bytes[start..stop])
                .map_err(|e| KnitError::Corrupt(format!("reading pack record: {e}")))?;
            out.push(body);
        }
        Ok(out)
    }

    fn add_raw_record(
        &self,
        _size: usize,
        chunks: Vec<Vec<u8>>,
    ) -> Result<ReadMemo<Self::F>, KnitError> {
        let body_len: usize = chunks.iter().map(|c| c.len()).sum();
        let refs: Vec<&[u8]> = chunks.iter().map(|c| c.as_slice()).collect();
        let mut pack = self.pack.lock().unwrap();
        let (start, length) = pack
            .writer
            .add_bytes_record(&refs, body_len, &[])
            .map_err(|e| KnitError::Corrupt(format!("writing pack record: {e}")))?;
        Ok(ReadMemo::new(self.pack_name.clone(), start, start + length))
    }
}

/// One collected index entry: a key, where its record landed, and its
/// graph parents (when the index tracks a graph).
type IndexRecord = (Key, IndexMemo<PackName>, Option<Vec<Key>>);

/// A writable [`GcIndex`] that collects the index entries for one object
/// kind. The graph (whether parents are tracked) is fixed at construction
/// so the resulting index has the right `node_ref_lists`.
struct PackWritingIndex {
    has_graph: bool,
    records: Mutex<Vec<IndexRecord>>,
}

impl PackWritingIndex {
    fn new(has_graph: bool) -> Self {
        PackWritingIndex {
            has_graph,
            records: Mutex::new(Vec::new()),
        }
    }

    /// Drain the collected index entries (called once at flush time).
    fn take_records(&self) -> Vec<IndexRecord> {
        std::mem::take(&mut self.records.lock().unwrap())
    }
}

impl GcIndex for PackWritingIndex {
    type F = PackName;

    fn get_build_details(
        &self,
        keys: &[Key],
    ) -> Result<std::collections::HashMap<Key, GcBuildDetails<Self::F>>, KnitError> {
        // Records written earlier in this write group are readable: a
        // delta-based inventory build reads back CHK pages it just wrote.
        let records = self.records.lock().unwrap();
        let mut out = std::collections::HashMap::new();
        for key in keys {
            if let Some((_, memo, parents)) = records.iter().rev().find(|(k, _, _)| k == key) {
                out.insert(
                    key.clone(),
                    GcBuildDetails {
                        index_memo: memo.clone(),
                        parents: parents.clone(),
                    },
                );
            }
        }
        Ok(out)
    }

    fn get_parent_map(
        &self,
        keys: &[Key],
    ) -> Result<std::collections::HashMap<Key, Vec<Key>>, KnitError> {
        let records = self.records.lock().unwrap();
        let mut out = std::collections::HashMap::new();
        for key in keys {
            if let Some((_, _, Some(parents))) = records.iter().rev().find(|(k, _, _)| k == key) {
                out.insert(key.clone(), parents.clone());
            }
        }
        Ok(out)
    }

    fn keys(&self) -> Result<Vec<Key>, KnitError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .map(|(k, _, _)| k.clone())
            .collect())
    }

    fn has_graph(&self) -> bool {
        self.has_graph
    }

    fn check_write_ok(&self) -> Result<(), KnitError> {
        Ok(())
    }

    fn add_records(&self, records: &[IndexRecord], _random_id: bool) -> Result<(), KnitError> {
        self.records.lock().unwrap().extend_from_slice(records);
        Ok(())
    }
}

/// One object-kind store being written: the groupcompress VF plus the
/// index kind it serialises to.
type WriteStore = GroupCompressVersionedFiles<PackWritingIndex, PackWritingAccess>;

/// A writer that accumulates objects into one new pack and flushes the
/// pack, its indices and `pack-names` to a transport.
///
/// Construct with [`new`](Self::new), add objects through the per-kind
/// `add_*` helpers, then call [`finish`](Self::finish).
pub(super) struct WriteGroup {
    pack: Arc<Mutex<SharedPack>>,
    revisions: WriteStore,
    inventories: WriteStore,
    texts: WriteStore,
    signatures: WriteStore,
    /// `Arc`-wrapped so it can be handed to `CHKInventory::from_inventory`,
    /// which writes CHK pages through it as the inventory is built.
    chk_bytes: Arc<WriteStore>,
}

impl WriteGroup {
    /// Start writing a new pack named `pack_name` (a 32-char hex string).
    ///
    /// `chk_fallback` is the repository's existing CHK store: registering it
    /// as a fallback on the new pack's CHK store lets a delta-based
    /// inventory write (`create_by_apply_delta`) read unchanged pages from
    /// the old packs while writing only the changed pages into the new one.
    pub(super) fn new(
        pack_name: &str,
        chk_fallback: Option<Arc<dyn crate::versionedfile::VersionedFiles + Send + Sync>>,
    ) -> Result<Self, RepositoryError> {
        let mut writer = ContainerWriter::new(Vec::new());
        writer
            .begin()
            .map_err(|e| RepositoryError::Corrupt(format!("pack begin: {e}")))?;
        let pack = Arc::new(Mutex::new(SharedPack { writer }));

        let make = |has_graph: bool| -> WriteStore {
            let access = PackWritingAccess {
                pack_name: pack_name.to_string(),
                pack: pack.clone(),
            };
            GroupCompressVersionedFiles::new(PackWritingIndex::new(has_graph), access, false)
        };

        // Revisions, inventories and texts carry a parent graph; the
        // signatures and chk stores do not. Build all stores before moving
        // `pack` into the struct, so the borrowing closure is done first.
        let revisions = make(true);
        let inventories = make(true);
        let texts = make(true);
        let signatures = make(false);
        let mut chk_store = make(false);
        if let Some(fallback) = chk_fallback {
            chk_store.add_fallback_versioned_files(Box::new(fallback));
        }
        let chk_bytes = Arc::new(chk_store);

        Ok(WriteGroup {
            pack,
            revisions,
            inventories,
            texts,
            signatures,
            chk_bytes,
        })
    }

    /// Add a signature text for `revision_id` (the clearsigned testament).
    pub(super) fn add_signature(
        &self,
        revision_id: &[u8],
        signature: &[u8],
    ) -> Result<(), RepositoryError> {
        let key = Key::fixed(vec![revision_id.to_vec()]);
        self.signatures
            .add_lines(key, Some(Vec::new()), split_lines(signature))?;
        Ok(())
    }

    /// Add a revision record (already serialised to bencode bytes).
    pub(super) fn add_revision(
        &self,
        revision_id: &[u8],
        parents: &[Vec<u8>],
        bytes: &[u8],
    ) -> Result<(), RepositoryError> {
        let key = Key::fixed(vec![revision_id.to_vec()]);
        let parent_keys: Vec<Key> = parents
            .iter()
            .map(|p| Key::fixed(vec![p.clone()]))
            .collect();
        self.revisions
            .add_lines(key, Some(parent_keys), split_lines(bytes))?;
        Ok(())
    }

    /// Add an inventory record (the serialised CHKInventory header).
    pub(super) fn add_inventory(
        &self,
        revision_id: &[u8],
        parents: &[Vec<u8>],
        bytes: &[u8],
    ) -> Result<(), RepositoryError> {
        let key = Key::fixed(vec![revision_id.to_vec()]);
        let parent_keys: Vec<Key> = parents
            .iter()
            .map(|p| Key::fixed(vec![p.clone()]))
            .collect();
        self.inventories
            .add_lines(key, Some(parent_keys), split_lines(bytes))?;
        Ok(())
    }

    /// Build a CHK inventory from `entries`, write its CHK pages, and add
    /// its serialised header as the inventory record for `revision_id`.
    ///
    /// Returns the sha1 of the serialised inventory, which the revision
    /// record records as `inventory_sha1`. `entries` must include every
    /// versioned object except the root (the root is identified by
    /// `root_id`). The 2a format parameters (`hash-255-way`, big pages)
    /// are applied.
    pub(super) fn add_inventory_from_entries(
        &self,
        revision_id: &[u8],
        parents: &[Vec<u8>],
        root_id: &[u8],
        entries: &[crate::inventory::Entry],
    ) -> Result<Vec<u8>, RepositoryError> {
        // Build the CHK inventory, writing its pages through the chk store.
        let cache: std::sync::Arc<dyn crate::chk_map::PageCache> =
            std::sync::Arc::new(crate::chk_map::InMemoryPageCache::new());
        let inv = crate::chk_inventory::CHKInventory::from_inventory(
            self.chk_bytes.clone(),
            cache,
            crate::RevisionId::from(revision_id),
            crate::FileId::from(root_id),
            entries,
            65536,
            b"hash-255-way".to_vec(),
        )
        .map_err(|e| RepositoryError::Corrupt(format!("building chk inventory: {e:?}")))?;
        let lines = inv
            .to_lines()
            .map_err(|e| RepositoryError::Corrupt(format!("serialising chk inventory: {e:?}")))?;
        let inv_bytes: Vec<u8> = lines.concat();
        let sha1 = crate::weave::sha_strings(&lines);
        self.add_inventory(revision_id, parents, &inv_bytes)?;
        Ok(sha1)
    }

    /// Build the new inventory by applying `delta` to the basis inventory
    /// (whose serialised header is `basis_lines`), writing only the changed
    /// CHK pages into this write group and referencing unchanged pages in
    /// the existing packs via the fallback store. Adds the new inventory's
    /// header for `new_revision_id` and returns its sha1.
    pub(super) fn add_inventory_by_delta(
        &self,
        basis_revision_id: &[u8],
        basis_lines: &[Vec<u8>],
        delta: &crate::inventory_delta::InventoryDelta,
        new_revision_id: &[u8],
        parents: &[Vec<u8>],
    ) -> Result<Vec<u8>, RepositoryError> {
        let cache: std::sync::Arc<dyn crate::chk_map::PageCache> =
            std::sync::Arc::new(crate::chk_map::InMemoryPageCache::new());
        // Deserialise the basis against this write group's CHK store, which
        // falls back to the existing packs for pages it does not hold.
        let basis = crate::chk_inventory::CHKInventory::deserialise(
            self.chk_bytes.clone(),
            cache,
            basis_lines,
            &crate::RevisionId::from(basis_revision_id),
        )
        .map_err(|e| RepositoryError::Corrupt(format!("basis inventory deserialise: {e:?}")))?;
        let new_inv = basis
            .create_by_apply_delta(delta, crate::RevisionId::from(new_revision_id), true)
            .map_err(|e| RepositoryError::Corrupt(format!("apply inventory delta: {e:?}")))?;
        let lines = new_inv
            .to_lines()
            .map_err(|e| RepositoryError::Corrupt(format!("serialising chk inventory: {e:?}")))?;
        let inv_bytes: Vec<u8> = lines.concat();
        let sha1 = crate::weave::sha_strings(&lines);
        self.add_inventory(new_revision_id, parents, &inv_bytes)?;
        Ok(sha1)
    }

    /// Add a file text, keyed by `(file_id, revision)`.
    pub(super) fn add_text(
        &self,
        file_id: &[u8],
        revision: &[u8],
        parents: &[(Vec<u8>, Vec<u8>)],
        bytes: &[u8],
    ) -> Result<(), RepositoryError> {
        let key = Key::fixed(vec![file_id.to_vec(), revision.to_vec()]);
        let parent_keys: Vec<Key> = parents
            .iter()
            .map(|(f, r)| Key::fixed(vec![f.clone(), r.clone()]))
            .collect();
        self.texts
            .add_lines(key, Some(parent_keys), split_lines(bytes))?;
        Ok(())
    }

    /// Copy every record from a source store into one of this write group's
    /// stores, preserving keys and parents.
    ///
    /// This is the per-kind copy step of a repack: source records are pulled as
    /// fulltext (via `get_record_stream`) and re-added, which recompresses them
    /// into the new pack's groupcompress blocks (the `reuse_blocks=False`
    /// behaviour brz's packer uses). `target` selects which store to add to.
    pub(super) fn copy_store(
        &self,
        source: &dyn crate::versionedfile::VersionedFiles,
        target: RepackTarget,
    ) -> Result<(), RepositoryError> {
        let mut keys = source.keys()?;
        // Stable order so repacked packs are reproducible.
        keys.sort_by(|a, b| a.segments().cmp(b.segments()));
        self.copy_store_keys(source, target, &keys)
    }

    /// Copy just `keys` from a source store into one of this write group's
    /// stores, preserving keys and parents. Used by the same-format streaming
    /// fetch to copy exactly the records belonging to the fetched revisions.
    pub(super) fn copy_store_keys(
        &self,
        source: &dyn crate::versionedfile::VersionedFiles,
        target: RepackTarget,
        keys: &[crate::versionedfile::Key],
    ) -> Result<(), RepositoryError> {
        let store = match target {
            RepackTarget::Revisions => &self.revisions,
            RepackTarget::Inventories => &self.inventories,
            RepackTarget::Texts => &self.texts,
            RepackTarget::Signatures => &self.signatures,
            RepackTarget::Chk => self.chk_bytes.as_ref(),
        };
        for record in source.get_record_stream(keys, "unordered", false)? {
            let record = record?;
            if record.storage_kind() == "absent" {
                continue;
            }
            let key = record.key();
            let parents = record.parents();
            let lines: Vec<Vec<u8>> = record.to_lines().map(|l| l.into_owned()).collect();
            store.add_lines(key, parents, lines)?;
        }
        Ok(())
    }

    /// Add raw CHK page bytes under their content key into the chk store. The
    /// streaming fetch hands pages straight through (already content-addressed),
    /// rather than rebuilding the CHK maps.
    pub(super) fn add_chk_page(
        &self,
        page_key: &[u8],
        page_bytes: Vec<u8>,
    ) -> Result<(), RepositoryError> {
        let key = Key::fixed(vec![page_key.to_vec()]);
        let lines = split_lines(&page_bytes);
        self.chk_bytes.add_lines(key, None, lines)?;
        Ok(())
    }

    /// Flush this write group to `transport` (rooted at `.bzr/repository`):
    /// write the new `.pack`, its five indices, and an updated `pack-names`
    /// that lists `existing_packs` plus the new one.
    ///
    /// Returns the new pack's `(name, pack-names value bytes)` so the caller
    /// can track it. Does nothing and returns `None` when the group is empty
    /// (no records added).
    pub(super) fn finish(
        self,
        transport: &dyn Transport,
        existing_packs: &[(String, Vec<u8>)],
    ) -> Result<Option<(String, Vec<u8>)>, RepositoryError> {
        // Build each index from its store's collected records.
        let rix = serialise_index(&self.revisions, 1)?;
        let iix = serialise_index(&self.inventories, 1)?;
        let tix = serialise_index(&self.texts, 2)?;
        let six = serialise_index(&self.signatures, 1)?;
        let cix = serialise_index(self.chk_bytes.as_ref(), 1)?;

        // Close the container and grab the pack bytes.
        let pack_bytes = {
            let mut pack = self.pack.lock().unwrap();
            pack.writer
                .end()
                .map_err(|e| RepositoryError::Corrupt(format!("pack end: {e}")))?;
            std::mem::take(pack.writer.get_mut())
        };

        // Name the finished pack by the md5 of its content, as brz does. The
        // write group's internal `pack_name` was only a token used while
        // collecting records (the index values store offsets, not the name).
        let pack_name = md5_hex(&pack_bytes);

        transport.put_bytes(&format!("packs/{pack_name}.pack"), &pack_bytes, None)?;
        let write_index = |ext: &str, bytes: &[u8]| -> Result<usize, RepositoryError> {
            let name = format!("indices/{pack_name}{ext}");
            transport.put_bytes(&name, bytes, None)?;
            Ok(bytes.len())
        };
        // Order in pack-names value: rix iix tix six cix.
        let sizes = [
            write_index(index_extension(IndexKind::Revision), &rix)?,
            write_index(index_extension(IndexKind::Inventory), &iix)?,
            write_index(index_extension(IndexKind::Text), &tix)?,
            write_index(index_extension(IndexKind::Signature), &six)?,
            write_index(index_extension(IndexKind::Chk), &cix)?,
        ];
        let new_value = sizes
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join(" ")
            .into_bytes();

        // pack-names: a btree index mapping (pack_name,) -> the five sizes,
        // for every existing pack plus the new one.
        let mut names = BTreeBuilder::new(0, 1);
        for (name, value) in existing_packs {
            names
                .add_node(vec![name.clone().into_bytes()], value.clone(), vec![])
                .map_err(|e| RepositoryError::Corrupt(format!("pack-names node: {e:?}")))?;
        }
        names
            .add_node(
                vec![pack_name.clone().into_bytes()],
                new_value.clone(),
                vec![],
            )
            .map_err(|e| RepositoryError::Corrupt(format!("pack-names node: {e:?}")))?;
        let names_bytes = names
            .finish()
            .map_err(|e| RepositoryError::Corrupt(format!("pack-names finish: {e:?}")))?;
        transport.put_bytes("pack-names", &names_bytes, None)?;

        Ok(Some((pack_name, new_value)))
    }
}

/// The lowercase-hex md5 digest of `bytes`, the form brz names a pack by.
fn md5_hex(bytes: &[u8]) -> String {
    use md5::{Digest, Md5};
    let digest = Md5::digest(bytes);
    let mut s = String::with_capacity(32);
    for b in digest {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Split a byte buffer into lines the way the versioned-file layer
/// expects: each line keeps its trailing `\n`, and a final unterminated
/// segment is kept as-is.
fn split_lines(bytes: &[u8]) -> Vec<Vec<u8>> {
    let mut lines = Vec::new();
    let mut start = 0;
    for (i, b) in bytes.iter().enumerate() {
        if *b == b'\n' {
            lines.push(bytes[start..=i].to_vec());
            start = i + 1;
        }
    }
    if start < bytes.len() {
        lines.push(bytes[start..].to_vec());
    }
    lines
}

/// Serialise a write store's collected index entries into a btree index.
///
/// `key_elements` is the number of segments in this kind's keys (1 for
/// revisions/inventories/chk, 2 for texts). A graph store writes one
/// reference list of parents; a graphless store writes none.
fn serialise_index(store: &WriteStore, key_elements: usize) -> Result<Vec<u8>, RepositoryError> {
    let has_graph = store.index().has_graph();
    let ref_lists = if has_graph { 1 } else { 0 };
    let mut builder = BTreeBuilder::new(ref_lists, key_elements);
    let records = store.index().take_records();
    // Collapse duplicate keys (keeping the first). Content-addressed CHK pages
    // are commonly added more than once within a write group -- e.g. a fetch
    // copying several revisions whose inventories share pages -- and a page
    // added twice is byte-identical, so the first index entry is correct.
    let mut seen: std::collections::HashSet<Vec<Vec<u8>>> = std::collections::HashSet::new();
    for (key, memo, parents) in records {
        if !seen.insert(key.segments().to_vec()) {
            continue;
        }
        let value = format!(
            "{} {} {} {}",
            memo.read_memo.start,
            memo.read_memo.byte_length(),
            memo.entry_start,
            memo.entry_end
        )
        .into_bytes();
        let references: Vec<Vec<Vec<Vec<u8>>>> = if has_graph {
            vec![parents
                .unwrap_or_default()
                .into_iter()
                .map(|k| k.segments().to_vec())
                .collect()]
        } else {
            vec![]
        };
        builder
            .add_node(key.segments().to_vec(), value, references)
            .map_err(|e| RepositoryError::Corrupt(format!("index node: {e:?}")))?;
    }
    builder
        .finish()
        .map_err(|e| RepositoryError::Corrupt(format!("index finish: {e:?}")))
}

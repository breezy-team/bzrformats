//! Reading and writing a knit-pack repository (the pre-2a pack formats).
//!
//! Knit-pack repositories share the pack/btree-index layout of 2a but store
//! **knit** records (gzip fulltext or line-delta) instead of groupcompress
//! blocks, and **XML** inventories instead of CHK. There are four indices
//! per pack (`.rix`/`.iix`/`.tix`/`.six`) and no `.cix`.
//!
//! The structure mirrors [`super::pack_2a`]: implement [`KnitIndex`] over
//! the per-pack btree indices and [`KnitAccess`] over the `.pack` files,
//! then let [`KnitVersionedFiles`] reconstruct each record (following delta
//! chains). The XML serializer chosen for the format (xml5/6/7) parses the
//! revision and inventory bytes.

use std::collections::HashMap;

use crate::btree_graph_index::BTreeGraphIndex;
use crate::knit::{
    parse_knit_index_value, KnitAccess, KnitError, KnitIndex, KnitIndexMemo, KnitKey, KnitMethod,
    KnitPlainFactory, KnitRecordDetails, KnitVersionedFiles,
};
use crate::pack_repo::{index_extension, IndexKind};
use crate::transport::{SharedTransport, Transport};

use super::format::{InventorySerializerKind, RepositoryFormat, StorageKind};
use super::pack_2a::RepositoryError;

/// The pack name is used as the knit `FileRef`.
type PackName = String;

/// A [`KnitIndex`] built from the per-pack btree indices of one kind,
/// merged across all packs.
struct PackKnitIndex {
    /// key -> record details (method, noeol, location, parents).
    entries: HashMap<KnitKey, KnitRecordDetails<PackName>>,
    has_graph: bool,
}

impl PackKnitIndex {
    /// Build the combined index for `kind` across `packs`.
    ///
    /// `key_segments` is the number of key elements (1 for revisions and
    /// inventories, 2 for texts). The btree value is `<flag><pos> <size>`;
    /// the second reference list (when present and non-empty) names the
    /// compression parent and marks the record as a line-delta.
    fn load(
        transport: &dyn Transport,
        packs: &[PackName],
        kind: IndexKind,
    ) -> Result<Self, RepositoryError> {
        let ext = index_extension(kind);
        let mut entries: HashMap<KnitKey, KnitRecordDetails<PackName>> = HashMap::new();
        let mut has_graph = false;
        for pack in packs {
            let name = format!("indices/{pack}{ext}");
            let index = BTreeGraphIndex::open(transport, &name)?;
            if index.node_ref_lists() > 0 {
                has_graph = true;
            }
            for (key, value, refs) in index.iter_all_entries() {
                let parsed = parse_knit_index_value(value)
                    .map_err(|e| RepositoryError::Corrupt(format!("knit index value: {e}")))?;
                let parents: Vec<KnitKey> = refs.first().cloned().unwrap_or_default();
                // A non-empty second reference list (the compression parent)
                // means this record is a line-delta against it.
                let compression_parent: Option<KnitKey> =
                    refs.get(1).and_then(|cp| cp.first().cloned());
                let method = if compression_parent.is_some() {
                    KnitMethod::LineDelta
                } else {
                    KnitMethod::Fulltext
                };
                entries.insert(
                    key.clone(),
                    KnitRecordDetails {
                        method,
                        noeol: parsed.noeol,
                        index_memo: KnitIndexMemo {
                            file_ref: pack.clone(),
                            offset: parsed.pos,
                            length: parsed.size as usize,
                        },
                        compression_parent,
                        parents,
                    },
                );
            }
        }
        Ok(PackKnitIndex { entries, has_graph })
    }
}

impl KnitIndex for PackKnitIndex {
    type F = PackName;

    fn get_build_details(
        &self,
        keys: &[KnitKey],
    ) -> Result<HashMap<KnitKey, KnitRecordDetails<Self::F>>, KnitError> {
        let mut out = HashMap::new();
        for key in keys {
            if let Some(d) = self.entries.get(key) {
                out.insert(key.clone(), d.clone());
            }
        }
        Ok(out)
    }

    fn keys(&self) -> Result<Vec<KnitKey>, KnitError> {
        Ok(self.entries.keys().cloned().collect())
    }

    fn get_parent_map(
        &self,
        keys: &[KnitKey],
    ) -> Result<HashMap<KnitKey, Vec<KnitKey>>, KnitError> {
        let mut out = HashMap::new();
        for key in keys {
            if let Some(d) = self.entries.get(key) {
                out.insert(key.clone(), d.parents.clone());
            }
        }
        Ok(out)
    }

    fn get_method(&self, key: &KnitKey) -> Result<KnitMethod, KnitError> {
        self.entries
            .get(key)
            .map(|d| d.method)
            .ok_or_else(|| KnitError::RevisionNotPresent(key.clone()))
    }

    fn get_total_build_size(
        &self,
        keys: &[KnitKey],
        positions: &HashMap<KnitKey, KnitRecordDetails<Self::F>>,
    ) -> usize {
        keys.iter()
            .filter_map(|k| positions.get(k))
            .map(|d| d.index_memo.length)
            .sum()
    }

    fn sort_keys_by_io(
        &self,
        keys: &mut [KnitKey],
        positions: &HashMap<KnitKey, KnitRecordDetails<Self::F>>,
    ) {
        keys.sort_by(|a, b| {
            let ka = positions
                .get(a)
                .map(|d| (&d.index_memo.file_ref, d.index_memo.offset));
            let kb = positions
                .get(b)
                .map(|d| (&d.index_memo.file_ref, d.index_memo.offset));
            ka.cmp(&kb)
        });
    }

    fn has_graph(&self) -> bool {
        self.has_graph
    }

    fn contains(&self, key: &KnitKey) -> Result<bool, KnitError> {
        Ok(self.entries.contains_key(key))
    }

    fn get_missing_compression_parents(&self) -> Result<Vec<KnitKey>, KnitError> {
        Ok(Vec::new())
    }

    fn check_write_ok(&self) -> Result<(), KnitError> {
        Err(KnitError::Corrupt("read-only index".to_string()))
    }

    fn add_records(
        &self,
        _records: &[(
            KnitKey,
            Vec<KnitMethod>,
            KnitIndexMemo<Self::F>,
            Vec<KnitKey>,
        )],
        _random_id: bool,
        _missing_compression_parents: bool,
    ) -> Result<(), KnitError> {
        Err(KnitError::Corrupt("read-only index".to_string()))
    }
}

/// A [`KnitAccess`] that reads raw knit records from the `.pack` files.
struct PackKnitAccess {
    transport: SharedTransport,
    cache: std::sync::Mutex<HashMap<PackName, std::sync::Arc<Vec<u8>>>>,
}

impl PackKnitAccess {
    fn new(transport: SharedTransport) -> Self {
        PackKnitAccess {
            transport,
            cache: std::sync::Mutex::new(HashMap::new()),
        }
    }

    fn pack_bytes(&self, pack: &str) -> Result<std::sync::Arc<Vec<u8>>, KnitError> {
        if let Some(b) = self.cache.lock().unwrap().get(pack) {
            return Ok(b.clone());
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

    fn record_body(&self, memo: &KnitIndexMemo<PackName>) -> Result<Vec<u8>, KnitError> {
        let bytes = self.pack_bytes(&memo.file_ref)?;
        let start = memo.offset as usize;
        let stop = start + memo.length;
        if stop > bytes.len() {
            return Err(KnitError::Corrupt(format!(
                "record range {start}..{stop} outside pack {} (len {})",
                memo.file_ref,
                bytes.len()
            )));
        }
        // The index range covers a whole container Bytes record; the knit
        // record (gzip) is the record body.
        crate::pack::read_bytes_record_body(&bytes[start..stop])
            .map_err(|e| KnitError::Corrupt(format!("reading pack record: {e}")))
    }
}

impl KnitAccess for PackKnitAccess {
    type F = PackName;

    fn get_raw_record(&self, memo: &KnitIndexMemo<Self::F>) -> Result<Vec<u8>, KnitError> {
        self.record_body(memo)
    }

    fn get_raw_records(&self, memos: &[KnitIndexMemo<Self::F>]) -> Result<Vec<Vec<u8>>, KnitError> {
        memos.iter().map(|m| self.record_body(m)).collect()
    }

    fn add_raw_record(
        &self,
        _key: &KnitKey,
        _size: usize,
        _chunks: Vec<Vec<u8>>,
    ) -> Result<KnitIndexMemo<Self::F>, KnitError> {
        Err(KnitError::Corrupt("read-only access".to_string()))
    }

    fn flush(&self) -> Result<(), KnitError> {
        Ok(())
    }

    fn reload_or_raise(&self, err: KnitError) -> Result<(), KnitError> {
        Err(err)
    }
}

/// A knit store for one kind of object in the repository.
type Store = KnitVersionedFiles<PackKnitIndex, PackKnitAccess, KnitPlainFactory>;

fn build_store(
    transport: &SharedTransport,
    packs: &[PackName],
    kind: IndexKind,
) -> Result<Store, RepositoryError> {
    let index = PackKnitIndex::load(transport.as_ref(), packs, kind)?;
    let access = PackKnitAccess::new(transport.clone());
    // max_delta_chain of 200 mirrors breezy's pack repositories.
    Ok(KnitVersionedFiles::new(
        index,
        access,
        KnitPlainFactory,
        200,
    ))
}

/// A knit-pack repository.
///
/// Reading is available after [`open`](Self::open); writing follows the
/// breezy write-group lifecycle ([`start_write_group`](Self::start_write_group),
/// `add_*`, [`commit_write_group`](Self::commit_write_group)).
pub struct KnitPackRepository {
    format: &'static RepositoryFormat,
    transport: SharedTransport,
    revisions: Store,
    inventories: Store,
    texts: Store,
    write_group: Option<WriteGroup>,
}

impl KnitPackRepository {
    /// Open the knit-pack repository whose `.bzr/repository` directory is
    /// rooted at `transport`.
    pub fn open(transport: SharedTransport) -> Result<Self, RepositoryError> {
        let format = check_format(transport.as_ref())?;
        let packs = read_pack_names(transport.as_ref())?;
        Ok(KnitPackRepository {
            format,
            revisions: build_store(&transport, &packs, IndexKind::Revision)?,
            inventories: build_store(&transport, &packs, IndexKind::Inventory)?,
            texts: build_store(&transport, &packs, IndexKind::Text)?,
            transport,
            write_group: None,
        })
    }

    /// Create an empty knit-pack repository of `format` at `transport` and
    /// open it. `format` must be a `KnitPack` storage format.
    pub fn create(
        transport: SharedTransport,
        format: &'static RepositoryFormat,
    ) -> Result<Self, RepositoryError> {
        if format.storage != StorageKind::KnitPack {
            return Err(RepositoryError::UnsupportedFormat(
                format.get_format_description(),
            ));
        }
        transport.mkdir("")?;
        transport.mkdir("indices")?;
        transport.mkdir("packs")?;
        transport.put_bytes("format", format.format_string())?;
        let empty = crate::btree_builder::BTreeBuilder::new(0, 1)
            .finish()
            .map_err(|e| RepositoryError::Corrupt(format!("empty pack-names: {e:?}")))?;
        transport.put_bytes("pack-names", &empty)?;
        Self::open(transport)
    }

    /// Open a write group.
    pub fn start_write_group(&mut self) -> Result<(), RepositoryError> {
        if self.write_group.is_some() {
            return Err(RepositoryError::Corrupt(
                "a write group is already open".to_string(),
            ));
        }
        self.write_group = Some(WriteGroup::new(&new_pack_name())?);
        Ok(())
    }

    fn group(&self) -> Result<&WriteGroup, RepositoryError> {
        self.write_group
            .as_ref()
            .ok_or_else(|| RepositoryError::Corrupt("no write group is open".to_string()))
    }

    /// Add a revision, serialised to XML (v5).
    pub fn add_revision(
        &mut self,
        revision: &crate::revision::Revision,
        parents: &[Vec<u8>],
    ) -> Result<(), RepositoryError> {
        use crate::serializer::RevisionSerializer;
        let bytes = crate::xml_serializer::XMLRevisionSerializer5
            .write_revision_to_string(revision)
            .map_err(|e| RepositoryError::Corrupt(format!("write revision: {e:?}")))?;
        let key: KnitKey = vec![revision.revision_id.as_bytes().to_vec()];
        let parent_keys: Vec<KnitKey> = parents.iter().map(|p| vec![p.clone()]).collect();
        self.group()?
            .revisions
            .add_lines(key, parent_keys, split_lines(&bytes), false)
            .map_err(|e| RepositoryError::Corrupt(format!("add revision: {e}")))?;
        Ok(())
    }

    /// Add an inventory, given its already-serialised XML bytes.
    pub fn add_inventory_xml(
        &mut self,
        revision_id: &[u8],
        parents: &[Vec<u8>],
        xml: &[u8],
    ) -> Result<(), RepositoryError> {
        let key: KnitKey = vec![revision_id.to_vec()];
        let parent_keys: Vec<KnitKey> = parents.iter().map(|p| vec![p.clone()]).collect();
        self.group()?
            .inventories
            .add_lines(key, parent_keys, split_lines(xml), false)
            .map_err(|e| RepositoryError::Corrupt(format!("add inventory: {e}")))?;
        Ok(())
    }

    /// Add a file text, keyed by `(file_id, revision)`.
    pub fn add_text(
        &mut self,
        file_id: &[u8],
        revision: &[u8],
        parents: &[(Vec<u8>, Vec<u8>)],
        bytes: &[u8],
    ) -> Result<(), RepositoryError> {
        let key: KnitKey = vec![file_id.to_vec(), revision.to_vec()];
        let parent_keys: Vec<KnitKey> = parents
            .iter()
            .map(|(f, r)| vec![f.clone(), r.clone()])
            .collect();
        self.group()?
            .texts
            .add_lines(key, parent_keys, split_lines(bytes), false)
            .map_err(|e| RepositoryError::Corrupt(format!("add text: {e}")))?;
        Ok(())
    }

    /// Flush the open write group.
    pub fn commit_write_group(&mut self) -> Result<(), RepositoryError> {
        let group = self
            .write_group
            .take()
            .ok_or_else(|| RepositoryError::Corrupt("no write group is open".to_string()))?;
        let existing = read_pack_names_with_values(self.transport.as_ref())?;
        group.finish(self.transport.as_ref(), &existing)?;
        Ok(())
    }

    /// All revision ids in this repository, sorted.
    pub fn all_revision_ids(&self) -> Result<Vec<Vec<u8>>, RepositoryError> {
        let mut ids: Vec<Vec<u8>> = self
            .revisions
            .keys()?
            .into_iter()
            .filter_map(|k| k.into_iter().next())
            .collect();
        ids.sort();
        Ok(ids)
    }

    /// Read and parse a revision by id (XML, serializer v5).
    pub fn get_revision(
        &self,
        revision_id: &[u8],
    ) -> Result<crate::revision::Revision, RepositoryError> {
        use crate::serializer::RevisionSerializer;
        let key: KnitKey = vec![revision_id.to_vec()];
        let bytes = self
            .revisions
            .get_text(&key)
            .map_err(|_| RepositoryError::NoSuchRevision(revision_id.to_vec()))?;
        crate::xml_serializer::XMLRevisionSerializer5
            .read_revision_from_string(&bytes)
            .map_err(|e| RepositoryError::Corrupt(format!("revision parse: {e:?}")))
    }

    /// Read the file text for `(file_id, revision)`.
    pub fn get_file_text(
        &self,
        file_id: &[u8],
        revision: &[u8],
    ) -> Result<Vec<u8>, RepositoryError> {
        let key: KnitKey = vec![file_id.to_vec(), revision.to_vec()];
        self.texts
            .get_text(&key)
            .map_err(|e| RepositoryError::Corrupt(format!("text {e}")))
    }

    /// Read the raw serialised inventory XML for a revision.
    pub fn get_inventory_xml(&self, revision_id: &[u8]) -> Result<Vec<u8>, RepositoryError> {
        let key: KnitKey = vec![revision_id.to_vec()];
        self.inventories
            .get_text(&key)
            .map_err(|e| RepositoryError::Corrupt(format!("inventory {e}")))
    }

    /// The format this repository was opened as.
    pub fn format(&self) -> &'static RepositoryFormat {
        self.format
    }

    /// The inventory serializer for this repository's format.
    fn inventory_serializer(&self) -> &'static dyn crate::serializer::InventorySerializer {
        match self.format.inventory_serializer {
            InventorySerializerKind::Xml5 => &crate::xml_serializer::XMLInventorySerializer5,
            InventorySerializerKind::Xml6 => &crate::xml_serializer::XMLInventorySerializer6,
            InventorySerializerKind::Xml7 => &crate::xml_serializer::XMLInventorySerializer7,
            // Knit-pack never uses CHK; fall back to xml5 for completeness.
            InventorySerializerKind::Chk255BigPage => {
                &crate::xml_serializer::XMLInventorySerializer5
            }
        }
    }

    /// Read the inventory for a revision as an in-memory
    /// [`MutableInventory`](crate::inventory::MutableInventory) (parsed from
    /// the format's XML serializer). The same type the 2a reader returns.
    pub fn get_inventory(
        &self,
        revision_id: &[u8],
    ) -> Result<crate::inventory::MutableInventory, RepositoryError> {
        let xml = self.get_inventory_xml(revision_id)?;
        let lines: Vec<Vec<u8>> = split_lines(&xml);
        let line_refs: Vec<&[u8]> = lines.iter().map(|l| l.as_slice()).collect();
        self.inventory_serializer()
            .read_inventory_from_lines(&line_refs, Some(crate::RevisionId::from(revision_id)))
            .map_err(|e| RepositoryError::Corrupt(format!("inventory parse: {e:?}")))
    }

    /// Build an inventory from `entries` (the root entry first, then its
    /// descendants in parent-before-child order), serialise it to XML with
    /// the format's serializer, add it to the open write group, and return
    /// the serialised inventory's sha1 to record on the revision.
    pub fn add_inventory_from_entries(
        &mut self,
        revision_id: &[u8],
        parents: &[Vec<u8>],
        _root_id: &[u8],
        entries: &[crate::inventory::Entry],
    ) -> Result<Vec<u8>, RepositoryError> {
        let mut inv = crate::inventory::MutableInventory::new();
        inv.revision_id = Some(crate::RevisionId::from(revision_id));
        for entry in entries {
            inv.add(entry.clone())
                .map_err(|e| RepositoryError::Corrupt(format!("build inventory: {e:?}")))?;
        }
        let lines = self
            .inventory_serializer()
            .write_inventory_to_lines(&inv, false)
            .map_err(|e| RepositoryError::Corrupt(format!("serialise inventory: {e:?}")))?;
        let line_refs: Vec<&[u8]> = lines.iter().map(|l| l.as_slice()).collect();
        let sha1 = crate::weave::sha_strings(&line_refs);
        let xml: Vec<u8> = lines.concat();
        self.add_inventory_xml(revision_id, parents, &xml)?;
        Ok(sha1)
    }
}

impl super::Repository for KnitPackRepository {
    fn format(&self) -> &'static RepositoryFormat {
        KnitPackRepository::format(self)
    }

    fn all_revision_ids(&self) -> Result<Vec<Vec<u8>>, RepositoryError> {
        KnitPackRepository::all_revision_ids(self)
    }

    fn get_revision(
        &self,
        revision_id: &[u8],
    ) -> Result<crate::revision::Revision, RepositoryError> {
        KnitPackRepository::get_revision(self, revision_id)
    }

    fn get_inventory(
        &self,
        revision_id: &[u8],
    ) -> Result<Box<dyn crate::inventory::Inventory>, RepositoryError> {
        Ok(Box::new(KnitPackRepository::get_inventory(
            self,
            revision_id,
        )?))
    }

    fn get_file_text(&self, file_id: &[u8], revision: &[u8]) -> Result<Vec<u8>, RepositoryError> {
        KnitPackRepository::get_file_text(self, file_id, revision)
    }

    fn start_write_group(&mut self) -> Result<(), RepositoryError> {
        KnitPackRepository::start_write_group(self)
    }

    fn add_revision(
        &mut self,
        revision: &crate::revision::Revision,
        parents: &[Vec<u8>],
    ) -> Result<(), RepositoryError> {
        KnitPackRepository::add_revision(self, revision, parents)
    }

    fn add_inventory_from_entries(
        &mut self,
        revision_id: &[u8],
        parents: &[Vec<u8>],
        root_id: &[u8],
        entries: &[crate::inventory::Entry],
    ) -> Result<Vec<u8>, RepositoryError> {
        KnitPackRepository::add_inventory_from_entries(self, revision_id, parents, root_id, entries)
    }

    fn add_text(
        &mut self,
        file_id: &[u8],
        revision: &[u8],
        parents: &[(Vec<u8>, Vec<u8>)],
        bytes: &[u8],
    ) -> Result<(), RepositoryError> {
        KnitPackRepository::add_text(self, file_id, revision, parents, bytes)
    }

    fn commit_write_group(&mut self) -> Result<(), RepositoryError> {
        KnitPackRepository::commit_write_group(self)
    }
}

/// Verify the `format` marker is a supported knit-pack format.
fn check_format(transport: &dyn Transport) -> Result<&'static RepositoryFormat, RepositoryError> {
    let marker = transport.get_bytes("format")?;
    let format = super::format::find_format(&marker)
        .ok_or_else(|| RepositoryError::UnknownFormat(marker.clone()))?;
    if format.storage != StorageKind::KnitPack {
        return Err(RepositoryError::UnsupportedFormat(
            format.get_format_description(),
        ));
    }
    Ok(format)
}

/// Generate a fresh 32-hex-character pack name.
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

/// Split a byte buffer into lines, each keeping its trailing newline.
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

use std::sync::{Arc, Mutex};

use crate::knit::{encode_graph_index_record, KnitMethod as KM};
use crate::pack::ContainerWriter;

/// One collected knit index record.
type KnitWriteRecord = (
    KnitKey,
    Vec<KnitMethod>,
    KnitIndexMemo<PackName>,
    Vec<KnitKey>,
);

/// A writable [`KnitIndex`] that collects records for one object kind.
///
/// `has_deltas` distinguishes the texts/inventories indices (which carry a
/// compression-parent reference list) from the revisions index (parents
/// only); `has_parents` is always true for the kinds we write.
struct KnitWriteIndex {
    has_deltas: bool,
    records: Mutex<Vec<KnitWriteRecord>>,
}

impl KnitWriteIndex {
    fn new(has_deltas: bool) -> Self {
        KnitWriteIndex {
            has_deltas,
            records: Mutex::new(Vec::new()),
        }
    }

    fn take_records(&self) -> Vec<KnitWriteRecord> {
        std::mem::take(&mut self.records.lock().unwrap())
    }
}

impl KnitIndex for KnitWriteIndex {
    type F = PackName;

    fn get_build_details(
        &self,
        _keys: &[KnitKey],
    ) -> Result<HashMap<KnitKey, KnitRecordDetails<Self::F>>, KnitError> {
        Ok(HashMap::new())
    }

    fn keys(&self) -> Result<Vec<KnitKey>, KnitError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .map(|(k, _, _, _)| k.clone())
            .collect())
    }

    fn get_parent_map(
        &self,
        _keys: &[KnitKey],
    ) -> Result<HashMap<KnitKey, Vec<KnitKey>>, KnitError> {
        Ok(HashMap::new())
    }

    fn get_method(&self, key: &KnitKey) -> Result<KnitMethod, KnitError> {
        Err(KnitError::RevisionNotPresent(key.clone()))
    }

    fn get_total_build_size(
        &self,
        _keys: &[KnitKey],
        _positions: &HashMap<KnitKey, KnitRecordDetails<Self::F>>,
    ) -> usize {
        0
    }

    fn sort_keys_by_io(
        &self,
        _keys: &mut [KnitKey],
        _positions: &HashMap<KnitKey, KnitRecordDetails<Self::F>>,
    ) {
    }

    fn has_graph(&self) -> bool {
        true
    }

    fn contains(&self, _key: &KnitKey) -> Result<bool, KnitError> {
        Ok(false)
    }

    fn get_missing_compression_parents(&self) -> Result<Vec<KnitKey>, KnitError> {
        Ok(Vec::new())
    }

    fn check_write_ok(&self) -> Result<(), KnitError> {
        Ok(())
    }

    fn add_records(
        &self,
        records: &[KnitWriteRecord],
        _random_id: bool,
        _missing_compression_parents: bool,
    ) -> Result<(), KnitError> {
        self.records.lock().unwrap().extend_from_slice(records);
        Ok(())
    }
}

/// A writable [`KnitAccess`] that appends knit records to a shared pack.
#[derive(Clone)]
struct KnitWriteAccess {
    pack_name: PackName,
    pack: Arc<Mutex<ContainerWriter<Vec<u8>>>>,
}

impl KnitAccess for KnitWriteAccess {
    type F = PackName;

    fn get_raw_record(&self, _memo: &KnitIndexMemo<Self::F>) -> Result<Vec<u8>, KnitError> {
        Err(KnitError::Corrupt("write-only access".to_string()))
    }

    fn get_raw_records(
        &self,
        _memos: &[KnitIndexMemo<Self::F>],
    ) -> Result<Vec<Vec<u8>>, KnitError> {
        Err(KnitError::Corrupt("write-only access".to_string()))
    }

    fn add_raw_record(
        &self,
        _key: &KnitKey,
        size: usize,
        data: Vec<Vec<u8>>,
    ) -> Result<KnitIndexMemo<Self::F>, KnitError> {
        let refs: Vec<&[u8]> = data.iter().map(|c| c.as_slice()).collect();
        let mut pack = self.pack.lock().unwrap();
        let (start, length) = pack
            .add_bytes_record(&refs, size, &[])
            .map_err(|e| KnitError::Corrupt(format!("writing pack record: {e}")))?;
        Ok(KnitIndexMemo {
            file_ref: self.pack_name.clone(),
            offset: start,
            length: length as usize,
        })
    }

    fn flush(&self) -> Result<(), KnitError> {
        Ok(())
    }

    fn reload_or_raise(&self, err: KnitError) -> Result<(), KnitError> {
        Err(err)
    }
}

type WriteStore = KnitVersionedFiles<KnitWriteIndex, KnitWriteAccess, KnitPlainFactory>;

/// The in-progress new pack for a knit-pack write group.
struct WriteGroup {
    pack_name: PackName,
    pack: Arc<Mutex<ContainerWriter<Vec<u8>>>>,
    revisions: WriteStore,
    inventories: WriteStore,
    texts: WriteStore,
}

impl WriteGroup {
    fn new(pack_name: &str) -> Result<Self, RepositoryError> {
        let mut writer = ContainerWriter::new(Vec::new());
        writer
            .begin()
            .map_err(|e| RepositoryError::Corrupt(format!("pack begin: {e}")))?;
        let pack = Arc::new(Mutex::new(writer));
        let make = |has_deltas: bool| -> WriteStore {
            let access = KnitWriteAccess {
                pack_name: pack_name.to_string(),
                pack: pack.clone(),
            };
            // max_delta_chain 0 -> always fulltext (the write side does not
            // delta-compress; readers handle both).
            KnitVersionedFiles::new(KnitWriteIndex::new(has_deltas), access, KnitPlainFactory, 0)
        };
        let revisions = make(false);
        let inventories = make(true);
        let texts = make(true);
        Ok(WriteGroup {
            pack_name: pack_name.to_string(),
            pack,
            revisions,
            inventories,
            texts,
        })
    }

    /// Flush the pack, its four indices and an updated `pack-names`.
    fn finish(
        self,
        transport: &dyn Transport,
        existing: &[(String, Vec<u8>)],
    ) -> Result<(), RepositoryError> {
        let WriteGroup {
            pack_name,
            pack,
            revisions,
            inventories,
            texts,
        } = self;
        let rix = serialise_index(revisions.index, 1)?;
        let iix = serialise_index(inventories.index, 1)?;
        let tix = serialise_index(texts.index, 2)?;
        // Knit-pack has no chk index; the signatures index is empty.
        let six = crate::btree_builder::BTreeBuilder::new(1, 1)
            .finish()
            .map_err(|e| RepositoryError::Corrupt(format!("empty six: {e:?}")))?;

        let pack_bytes = {
            let mut writer = pack.lock().unwrap();
            writer
                .end()
                .map_err(|e| RepositoryError::Corrupt(format!("pack end: {e}")))?;
            std::mem::take(writer.get_mut())
        };
        transport.put_bytes(&format!("packs/{}.pack", pack_name), &pack_bytes)?;

        let write_index = |ext: &str, bytes: &[u8]| -> Result<usize, RepositoryError> {
            transport.put_bytes(&format!("indices/{}{ext}", pack_name), bytes)?;
            Ok(bytes.len())
        };
        // Knit-pack pack-names order: rix iix tix six (no cix).
        let sizes = [
            write_index(index_extension(IndexKind::Revision), &rix)?,
            write_index(index_extension(IndexKind::Inventory), &iix)?,
            write_index(index_extension(IndexKind::Text), &tix)?,
            write_index(index_extension(IndexKind::Signature), &six)?,
        ];
        let new_value = sizes
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join(" ")
            .into_bytes();

        let mut names = crate::btree_builder::BTreeBuilder::new(0, 1);
        for (name, value) in existing {
            names
                .add_node(vec![name.clone().into_bytes()], value.clone(), vec![])
                .map_err(|e| RepositoryError::Corrupt(format!("pack-names node: {e:?}")))?;
        }
        names
            .add_node(vec![pack_name.clone().into_bytes()], new_value, vec![])
            .map_err(|e| RepositoryError::Corrupt(format!("pack-names node: {e:?}")))?;
        let names_bytes = names
            .finish()
            .map_err(|e| RepositoryError::Corrupt(format!("pack-names finish: {e:?}")))?;
        transport.put_bytes("pack-names", &names_bytes)?;
        Ok(())
    }
}

/// Serialise a write index's collected records into a btree index.
fn serialise_index(index: KnitWriteIndex, key_elements: usize) -> Result<Vec<u8>, RepositoryError> {
    let has_deltas = index.has_deltas;
    let ref_lists = if has_deltas { 2 } else { 1 };
    let mut builder = crate::btree_builder::BTreeBuilder::new(ref_lists, key_elements);
    for (key, options, memo, parents) in index.take_records() {
        let noeol = options.contains(&KM::NoEol);
        let method = if options.contains(&KM::LineDelta) {
            KM::LineDelta
        } else {
            KM::Fulltext
        };
        let (value, node_refs) = encode_graph_index_record(
            noeol,
            memo.offset,
            memo.length as u64,
            method,
            true,
            has_deltas,
            &parents,
        )
        .map_err(|e| RepositoryError::Corrupt(format!("encode index: {e}")))?;
        builder
            .add_node(key, value, node_refs)
            .map_err(|e| RepositoryError::Corrupt(format!("index node: {e:?}")))?;
    }
    builder
        .finish()
        .map_err(|e| RepositoryError::Corrupt(format!("index finish: {e:?}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::LocalTransport;
    use std::sync::Arc;

    fn knitpack6() -> &'static RepositoryFormat {
        super::super::format::find_format(b"Bazaar RepositoryFormatKnitPack6 (bzr 1.9)\n").unwrap()
    }

    fn temp() -> (tempfile::TempDir, SharedTransport) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("repository");
        std::fs::create_dir_all(&path).unwrap();
        (dir, Arc::new(LocalTransport::new(&path)))
    }

    #[test]
    fn write_then_read_round_trip() {
        let (_d, t) = temp();
        let mut repo = KnitPackRepository::create(t.clone(), knitpack6()).unwrap();
        repo.start_write_group().unwrap();
        let r1 = crate::revision::Revision::new(
            crate::RevisionId::from(&b"rev-1"[..]),
            vec![],
            Some("T <t@e>".into()),
            "first".into(),
            std::collections::HashMap::new(),
            None,
            1577880000.0,
            Some(0),
        );
        let r2 = crate::revision::Revision::new(
            crate::RevisionId::from(&b"rev-2"[..]),
            vec![crate::RevisionId::from(&b"rev-1"[..])],
            Some("T <t@e>".into()),
            "second".into(),
            std::collections::HashMap::new(),
            None,
            1577966400.0,
            Some(0),
        );
        repo.add_revision(&r1, &[]).unwrap();
        repo.add_revision(&r2, &[b"rev-1".to_vec()]).unwrap();
        repo.add_text(b"file-1", b"rev-1", &[], b"hello\n").unwrap();
        repo.add_text(
            b"file-1",
            b"rev-2",
            &[(b"file-1".to_vec(), b"rev-1".to_vec())],
            b"hello\ngoodbye\n",
        )
        .unwrap();
        repo.commit_write_group().unwrap();

        let repo = KnitPackRepository::open(t).unwrap();
        let mut ids = repo.all_revision_ids().unwrap();
        ids.sort();
        assert_eq!(ids, vec![b"rev-1".to_vec(), b"rev-2".to_vec()]);
        assert_eq!(repo.get_revision(b"rev-1").unwrap().message, "first");
        let got2 = repo.get_revision(b"rev-2").unwrap();
        assert_eq!(got2.message, "second");
        assert_eq!(
            got2.parent_ids
                .iter()
                .map(|p| p.as_bytes().to_vec())
                .collect::<Vec<_>>(),
            vec![b"rev-1".to_vec()]
        );
        assert_eq!(repo.get_file_text(b"file-1", b"rev-1").unwrap(), b"hello\n");
        assert_eq!(
            repo.get_file_text(b"file-1", b"rev-2").unwrap(),
            b"hello\ngoodbye\n"
        );
    }

    #[test]
    fn create_rejects_non_knitpack_format() {
        let (_d, t) = temp();
        let fmt = super::super::format::find_format(
            b"Bazaar repository format 2a (needs bzr 1.16 or later)\n",
        )
        .unwrap();
        assert!(KnitPackRepository::create(t, fmt).is_err());
    }
}

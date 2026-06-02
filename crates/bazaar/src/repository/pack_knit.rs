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

/// A read-only view of a knit-pack repository.
pub struct KnitPackRepository {
    format: &'static RepositoryFormat,
    revisions: Store,
    inventories: Store,
    texts: Store,
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
        })
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

    /// Parse the inventory for a revision into entries `(path, Entry)`.
    pub fn get_inventory(
        &self,
        revision_id: &[u8],
    ) -> Result<Vec<(String, crate::inventory::Entry)>, RepositoryError> {
        let xml = self.get_inventory_xml(revision_id)?;
        let lines: Vec<Vec<u8>> = split_lines(&xml);
        let line_refs: Vec<&[u8]> = lines.iter().map(|l| l.as_slice()).collect();
        let inv = self
            .inventory_serializer()
            .read_inventory_from_lines(&line_refs, Some(crate::RevisionId::from(revision_id)))
            .map_err(|e| RepositoryError::Corrupt(format!("inventory parse: {e:?}")))?;
        let mut out = Vec::new();
        for (path, entry) in inv.iter_entries(None) {
            if path.is_empty() {
                continue;
            }
            out.push((path, entry.clone()));
        }
        Ok(out)
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

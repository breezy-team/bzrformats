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

use crate::knit::{
    parse_knit_index_value, KnitAccess, KnitError, KnitIndex, KnitIndexMemo, KnitKey, KnitMethod,
    KnitPlainFactory, KnitRecordDetails, KnitVersionedFiles,
};
use crate::pack_repo::{index_extension, IndexKind};
use crate::transport::{SharedTransport, Transport, TransportError};

use super::format::RepositoryFormat;
use super::pack_2a::RepositoryError;
use super::unkey_knit_parent_map;
use crate::declare_repository_format;
use crate::xml_serializer::{
    XMLInventorySerializer5, XMLInventorySerializer6, XMLInventorySerializer7,
    XMLRevisionSerializer5,
};

declare_repository_format! {
    FORMAT_KNIT_PACK_1 {
        format_string: b"Bazaar pack repository format 1 (needs bzr 0.92)\n",
        description: "Pack repository format 1",
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer5,
        open: open_knit_pack,
        create: create_knit_pack,
        supported: true,
        uses_btree_index: false,
    }
}

declare_repository_format! {
    FORMAT_KNIT_PACK_3 {
        format_string: b"Bazaar pack repository format 1 with subtree support (needs bzr 0.92)\n",
        description: "Pack repository format 1 with subtree support",
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer7,
        open: open_knit_pack,
        create: create_knit_pack,
        rich_root_data: true,
        supports_tree_reference: true,
        supported: true,
        uses_btree_index: false,
    }
}

declare_repository_format! {
    FORMAT_KNIT_PACK_4 {
        format_string: b"Bazaar pack repository format 1 with rich root (needs bzr 1.0)\n",
        description: "Pack repository format 1 with rich root",
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer6,
        open: open_knit_pack,
        create: create_knit_pack,
        rich_root_data: true,
        supported: true,
        uses_btree_index: false,
    }
}

declare_repository_format! {
    FORMAT_KNIT_PACK_5 {
        format_string: b"Bazaar RepositoryFormatKnitPack5 (bzr 1.6)\n",
        description: "Pack repository format 5 (stackable)",
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer5,
        open: open_knit_pack,
        create: create_knit_pack,
        supports_external_lookups: true,
        supported: true,
        uses_btree_index: false,
    }
}

declare_repository_format! {
    FORMAT_KNIT_PACK_5_RICH_ROOT {
        format_string: b"Bazaar RepositoryFormatKnitPack5RichRoot (bzr 1.6.1)\n",
        description: "Pack repository format 5 with rich root (stackable)",
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer6,
        open: open_knit_pack,
        create: create_knit_pack,
        rich_root_data: true,
        supports_external_lookups: true,
        supported: true,
        uses_btree_index: false,
    }
}

declare_repository_format! {
    FORMAT_KNIT_PACK_5_RICH_ROOT_BROKEN {
        format_string: b"Bazaar RepositoryFormatKnitPack5RichRoot (bzr 1.6)\n",
        description: "Pack repository format 5 with rich root (broken)",
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer6,
        open: open_knit_pack,
        create: create_knit_pack,
        rich_root_data: true,
        supports_external_lookups: true,
        deprecated: true,
        uses_btree_index: false,
    }
}

declare_repository_format! {
    FORMAT_KNIT_PACK_6 {
        format_string: b"Bazaar RepositoryFormatKnitPack6 (bzr 1.9)\n",
        description: "Pack repository format 6 (btree indexes, stackable)",
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer5,
        open: open_knit_pack,
        create: create_knit_pack,
        supports_external_lookups: true,
        supported: true,
    }
}

declare_repository_format! {
    FORMAT_KNIT_PACK_6_RICH_ROOT {
        format_string: b"Bazaar RepositoryFormatKnitPack6RichRoot (bzr 1.9)\n",
        description: "Pack repository format 6 with rich root (btree, stackable)",
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer6,
        open: open_knit_pack,
        create: create_knit_pack,
        rich_root_data: true,
        supports_external_lookups: true,
        supported: true,
    }
}

/// The pack name is used as the knit `FileRef`.
type PackName = String;

/// Which of a write group's stores a repack copy targets. Knit-pack has no
/// chk store (unlike 2a).
#[derive(Clone, Copy)]
enum RepackTarget {
    Revisions,
    Inventories,
    Texts,
    Signatures,
}

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
            let index = super::pack_index::PackIndex::open(transport, &name)?;
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
    signatures: Store,
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
            signatures: build_store(&transport, &packs, IndexKind::Signature)?,
            transport,
            write_group: None,
        })
    }

    /// Create an empty knit-pack repository of `format` at `transport` and
    /// open it. `format` must be a knit-pack format.
    pub fn create(
        transport: SharedTransport,
        format: &'static RepositoryFormat,
    ) -> Result<Self, RepositoryError> {
        if !std::ptr::fn_addr_eq(format.open, open_knit_pack as super::format::OpenFn) {
            return Err(RepositoryError::UnsupportedFormat(
                format.get_format_description(),
            ));
        }
        transport.mkdir("")?;
        transport.mkdir("indices")?;
        transport.mkdir("packs")?;
        transport.put_bytes("format", format.format_string(), None)?;
        let empty = super::pack_index::IndexBuilder::new(format.uses_btree_index, 0, 1)
            .finish()
            .map_err(|e| RepositoryError::Corrupt(format!("empty pack-names: {e}")))?;
        transport.put_bytes("pack-names", &empty, None)?;
        Self::open(transport)
    }

    /// Open a write group.
    pub fn start_write_group(&mut self) -> Result<(), RepositoryError> {
        if self.write_group.is_some() {
            return Err(RepositoryError::Corrupt(
                "a write group is already open".to_string(),
            ));
        }
        self.write_group = Some(WriteGroup::new(
            &new_pack_name(),
            self.format.uses_btree_index,
        )?);
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

    /// Add a signature text for `revision_id` (the clearsigned testament) to
    /// the open write group.
    pub fn add_signature(
        &mut self,
        revision_id: &[u8],
        signature: &[u8],
    ) -> Result<(), RepositoryError> {
        let key: KnitKey = vec![revision_id.to_vec()];
        self.group()?
            .signatures
            .add_lines(key, Vec::new(), split_lines(signature), false)
            .map_err(|e| RepositoryError::Corrupt(format!("add signature: {e}")))?;
        Ok(())
    }

    /// The signature text stored for `revision_id`, or `None` if unsigned.
    pub fn get_signature_text(
        &self,
        revision_id: &[u8],
    ) -> Result<Option<Vec<u8>>, RepositoryError> {
        let key: KnitKey = vec![revision_id.to_vec()];
        match self.signatures.get_text(&key) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(crate::knit::KnitError::RevisionNotPresent(_)) => Ok(None),
            Err(e) => Err(RepositoryError::Corrupt(format!("signature {e}"))),
        }
    }

    /// Flush the open write group.
    pub fn commit_write_group(&mut self) -> Result<(), RepositoryError> {
        let group = self
            .write_group
            .take()
            .ok_or_else(|| RepositoryError::Corrupt("no write group is open".to_string()))?;
        let existing = read_pack_names_with_values(self.transport.as_ref())?;
        group.finish(self.transport.as_ref(), &existing)?;
        // Autopack if the repository has accumulated too many packs, as brz
        // does on commit_write_group.
        self.autopack()?;
        Ok(())
    }

    /// Stream the `missing` revisions from another knit-pack repository into
    /// this one, copying raw records (revisions, inventories, texts,
    /// signatures) without decoding and re-encoding them.
    ///
    /// This is the same-format fast path for [`crate::repository::fetch`]:
    /// both sides store knit records and XML inventories, so records copy
    /// through verbatim. Unlike 2a there is no CHK page store. `missing` must
    /// be in topological order and already filtered to revisions absent here.
    ///
    /// Requires no open write group.
    pub fn stream_fetch_from(
        &mut self,
        source: &KnitPackRepository,
        missing: &[Vec<u8>],
    ) -> Result<(), RepositoryError> {
        if self.write_group.is_some() {
            return Err(RepositoryError::Corrupt(
                "cannot fetch with an open write group".to_string(),
            ));
        }
        if missing.is_empty() {
            return Ok(());
        }

        // Per-revision stores key by [revid].
        let rev_keys: Vec<KnitKey> = missing.iter().map(|r| vec![r.clone()]).collect();

        // Texts key by [file_id, revid]; collect the keys each fetched
        // revision introduces from its inventory.
        let mut text_keys: Vec<KnitKey> = Vec::new();
        for rev in missing {
            let inv = source.get_inventory(rev)?;
            for (_, entry) in inv.entries() {
                if entry.revision().map(|r| r.as_bytes()) == Some(rev.as_slice()) {
                    text_keys.push(vec![entry.file_id().as_bytes().to_vec(), rev.clone()]);
                }
            }
        }

        self.start_write_group()?;
        let group = self.write_group.as_ref().expect("just opened");
        group.copy_store_keys(&source.revisions, RepackTarget::Revisions, &rev_keys)?;
        group.copy_store_keys(&source.inventories, RepackTarget::Inventories, &rev_keys)?;
        group.copy_store_keys(&source.signatures, RepackTarget::Signatures, &rev_keys)?;
        group.copy_store_keys(&source.texts, RepackTarget::Texts, &text_keys)?;
        self.commit_write_group()?;
        Ok(())
    }

    /// Combine all packs in this repository into a single new pack.
    ///
    /// Re-streams every record (revisions, inventories, texts, signatures) into
    /// one fresh pack, rewrites `pack-names` to reference only it, and moves the
    /// old packs and their indices into `obsolete_packs/`. A single-pack
    /// repository is left untouched. Requires no open write group.
    pub fn pack(&mut self) -> Result<(), RepositoryError> {
        if self.write_group.is_some() {
            return Err(RepositoryError::Corrupt(
                "cannot pack with an open write group".to_string(),
            ));
        }
        let old_packs = read_pack_names(self.transport.as_ref())?;
        if old_packs.len() <= 1 {
            return Ok(());
        }
        self.repack(&old_packs, &[])
    }

    /// Repack the smallest packs when the repository has too many, per the
    /// pack-distribution heuristic. Returns whether a repack happened.
    pub fn autopack(&mut self) -> Result<bool, RepositoryError> {
        if self.write_group.is_some() {
            return Err(RepositoryError::Corrupt(
                "cannot autopack with an open write group".to_string(),
            ));
        }
        let all_packs = read_pack_names(self.transport.as_ref())?;
        if all_packs.len() <= 1 {
            return Ok(false);
        }
        // Revision count per pack, from each pack's revision index.
        let mut counts = Vec::with_capacity(all_packs.len());
        for name in &all_packs {
            let ext = index_extension(IndexKind::Revision);
            let index = super::pack_index::PackIndex::open(
                self.transport.as_ref(),
                &format!("indices/{name}{ext}"),
            )?;
            counts.push(index.iter_all_entries().count() as u64);
        }
        let selected = super::pack_collection::plan_autopack_combinations(&counts);
        if selected.is_empty() {
            return Ok(false);
        }
        let to_combine: Vec<PackName> = selected.iter().map(|&i| all_packs[i].clone()).collect();
        let survivors: Vec<(PackName, Vec<u8>)> = {
            let with_values = read_pack_names_with_values(self.transport.as_ref())?;
            let combine: std::collections::HashSet<&PackName> = to_combine.iter().collect();
            with_values
                .into_iter()
                .filter(|(n, _)| !combine.contains(n))
                .collect()
        };
        self.repack(&to_combine, &survivors)?;
        Ok(true)
    }

    /// Combine `to_combine` into one new pack, rewrite `pack-names` to list
    /// `survivors` plus the new pack, and obsolete the combined packs.
    fn repack(
        &mut self,
        to_combine: &[PackName],
        survivors: &[(PackName, Vec<u8>)],
    ) -> Result<(), RepositoryError> {
        let revisions = build_store(&self.transport, to_combine, IndexKind::Revision)?;
        let inventories = build_store(&self.transport, to_combine, IndexKind::Inventory)?;
        let texts = build_store(&self.transport, to_combine, IndexKind::Text)?;
        let signatures = build_store(&self.transport, to_combine, IndexKind::Signature)?;

        let group = WriteGroup::new(&new_pack_name(), self.format.uses_btree_index)?;
        // Copy order matches brz's KnitPacker: revisions, inventories, texts,
        // signatures (knit-pack has no chk store).
        group.copy_store(&revisions, RepackTarget::Revisions)?;
        group.copy_store(&inventories, RepackTarget::Inventories)?;
        group.copy_store(&texts, RepackTarget::Texts)?;
        group.copy_store(&signatures, RepackTarget::Signatures)?;
        group.finish(self.transport.as_ref(), survivors)?;

        self.obsolete_packs(to_combine)?;
        Ok(())
    }

    /// Move `packs` (their `.pack` files and the four index suffixes) into
    /// `obsolete_packs/`, creating it if needed.
    fn obsolete_packs(&self, packs: &[PackName]) -> Result<(), RepositoryError> {
        let _ = self.transport.mkdir("obsolete_packs");
        for name in packs {
            self.move_to_obsolete(&format!("packs/{name}.pack"), &format!("{name}.pack"))?;
            for kind in [
                IndexKind::Revision,
                IndexKind::Inventory,
                IndexKind::Text,
                IndexKind::Signature,
            ] {
                let ext = index_extension(kind);
                self.move_to_obsolete(&format!("indices/{name}{ext}"), &format!("{name}{ext}"))?;
            }
        }
        Ok(())
    }

    fn move_to_obsolete(&self, from: &str, basename: &str) -> Result<(), RepositoryError> {
        match self
            .transport
            .rename(from, &format!("obsolete_packs/{basename}"))
        {
            Ok(()) | Err(TransportError::NoSuchFile(_)) => Ok(()),
            Err(e) => Err(e.into()),
        }
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

    /// The stored parent ids of each of `revision_ids` (present ones only),
    /// read from the revision knit's index.
    pub fn get_parent_map(
        &self,
        revision_ids: &[Vec<u8>],
    ) -> Result<std::collections::HashMap<Vec<u8>, Vec<Vec<u8>>>, RepositoryError> {
        let keys: Vec<KnitKey> = revision_ids.iter().map(|r| vec![r.clone()]).collect();
        let raw = self
            .revisions
            .get_parent_map(&keys)
            .map_err(RepositoryError::Knit)?;
        Ok(unkey_knit_parent_map(raw))
    }

    /// Read and parse a revision by id (XML, serializer v5).
    pub fn get_revision(
        &self,
        revision_id: &[u8],
    ) -> Result<crate::revision::Revision, RepositoryError> {
        use crate::serializer::RevisionSerializer;
        let key: KnitKey = vec![revision_id.to_vec()];
        let bytes = self.revisions.get_text(&key).map_err(|e| match e {
            crate::knit::KnitError::RevisionNotPresent(_) => {
                RepositoryError::NoSuchRevision(revision_id.to_vec())
            }
            other => RepositoryError::Corrupt(format!("revision {other}")),
        })?;
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
        self.format.inventory_serializer
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

    /// Add the inventory for `new_revision_id` by applying `delta` to the
    /// basis inventory. Knit-pack stores whole-text XML inventories, so the
    /// basis is materialised, the delta applied, and the result serialised
    /// in full (there are no shared pages to preserve).
    pub fn add_inventory_by_delta(
        &mut self,
        basis_revision_id: &[u8],
        delta: &crate::inventory_delta::InventoryDelta,
        new_revision_id: &[u8],
        parents: &[Vec<u8>],
    ) -> Result<Vec<u8>, RepositoryError> {
        // For a first commit the basis is the empty inventory; the delta is
        // all-adds and includes the tree root, so applying it yields the
        // full inventory.
        let basis = if basis_revision_id == crate::branch::NULL_REVISION {
            crate::inventory::MutableInventory::new()
        } else {
            self.get_inventory(basis_revision_id)?
        };
        let new_inv = basis
            .create_by_apply_delta(delta, crate::RevisionId::from(new_revision_id))
            .map_err(|e| RepositoryError::Corrupt(format!("apply inventory delta: {e:?}")))?;
        let lines = self
            .inventory_serializer()
            .write_inventory_to_lines(&new_inv, false)
            .map_err(|e| RepositoryError::Corrupt(format!("serialise inventory: {e:?}")))?;
        let line_refs: Vec<&[u8]> = lines.iter().map(|l| l.as_slice()).collect();
        let sha1 = crate::weave::sha_strings(&line_refs);
        let xml: Vec<u8> = lines.concat();
        self.add_inventory_xml(new_revision_id, parents, &xml)?;
        Ok(sha1)
    }
}

impl super::Repository for KnitPackRepository {
    fn format(&self) -> &'static RepositoryFormat {
        KnitPackRepository::format(self)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    /// Fast path for knit-pack-to-knit-pack fetch: if `source` is also a
    /// knit-pack repository, stream raw records; otherwise decline so the
    /// generic rebuild runs.
    fn try_fetch_from(
        &mut self,
        source: &dyn super::Repository,
        revision_ids: &[Vec<u8>],
    ) -> Result<bool, RepositoryError> {
        match source.as_any().downcast_ref::<KnitPackRepository>() {
            // Only stream when the two knit-pack formats share an inventory
            // serializer (xml5/6/7) and rich-root setting; otherwise the copied
            // XML inventories would not match the target format, so fall back to
            // the generic rebuild.
            Some(src)
                if src.format().inventory_serializer.format_num()
                    == self.format().inventory_serializer.format_num()
                    && src.format().rich_root_data == self.format().rich_root_data =>
            {
                self.stream_fetch_from(src, revision_ids)?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn all_revision_ids(&self) -> Result<Vec<Vec<u8>>, RepositoryError> {
        KnitPackRepository::all_revision_ids(self)
    }

    fn get_parent_map(
        &self,
        revision_ids: &[Vec<u8>],
    ) -> Result<std::collections::HashMap<Vec<u8>, Vec<Vec<u8>>>, RepositoryError> {
        KnitPackRepository::get_parent_map(self, revision_ids)
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

    fn add_inventory_by_delta(
        &mut self,
        basis_revision_id: &[u8],
        delta: &crate::inventory_delta::InventoryDelta,
        new_revision_id: &[u8],
        parents: &[Vec<u8>],
    ) -> Result<Vec<u8>, RepositoryError> {
        KnitPackRepository::add_inventory_by_delta(
            self,
            basis_revision_id,
            delta,
            new_revision_id,
            parents,
        )
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

    fn add_signature_text(
        &mut self,
        revision_id: &[u8],
        signature: &[u8],
    ) -> Result<(), RepositoryError> {
        KnitPackRepository::add_signature(self, revision_id, signature)
    }

    fn get_signature_text(&self, revision_id: &[u8]) -> Result<Option<Vec<u8>>, RepositoryError> {
        KnitPackRepository::get_signature_text(self, revision_id)
    }

    fn commit_write_group(&mut self) -> Result<(), RepositoryError> {
        KnitPackRepository::commit_write_group(self)
    }

    fn pack(&mut self) -> Result<(), RepositoryError> {
        KnitPackRepository::pack(self)
    }

    fn autopack(&mut self) -> Result<bool, RepositoryError> {
        KnitPackRepository::autopack(self)
    }
}

/// Open the repository at `transport` as a knit-pack repository. The
/// [`OpenFn`](super::format::OpenFn) carried by every knit-pack
/// [`RepositoryFormat`].
pub fn open_knit_pack(
    transport: SharedTransport,
) -> Result<Box<dyn super::Repository>, RepositoryError> {
    Ok(Box::new(KnitPackRepository::open(transport)?))
}

/// Create an empty knit-pack repository of `format` at `transport`. The
/// [`CreateFn`](super::format::CreateFn) carried by every knit-pack
/// [`RepositoryFormat`].
pub fn create_knit_pack(
    format: &'static RepositoryFormat,
    transport: SharedTransport,
) -> Result<Box<dyn super::Repository>, RepositoryError> {
    Ok(Box::new(KnitPackRepository::create(transport, format)?))
}

/// Verify the `format` marker is a knit-pack format.
fn check_format(transport: &dyn Transport) -> Result<&'static RepositoryFormat, RepositoryError> {
    let marker = transport.get_bytes("format")?;
    let format = super::format::find_format(&marker)
        .ok_or_else(|| RepositoryError::UnknownFormat(marker.clone()))?;
    if !std::ptr::fn_addr_eq(format.open, open_knit_pack as super::format::OpenFn) {
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
    let index = super::pack_index::PackIndex::open(transport, "pack-names")?;
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
    let index = super::pack_index::PackIndex::open(transport, "pack-names")?;
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
    signatures: WriteStore,
    texts: WriteStore,
    /// Whether to write B+Tree indices (1.9+) or format-1 GraphIndex (0.92,
    /// 1.6).
    uses_btree: bool,
}

impl WriteGroup {
    fn new(pack_name: &str, uses_btree: bool) -> Result<Self, RepositoryError> {
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
        // Signatures, like revisions, are keyed by revision id with no deltas.
        let signatures = make(false);
        let texts = make(true);
        Ok(WriteGroup {
            pack_name: pack_name.to_string(),
            pack,
            revisions,
            inventories,
            signatures,
            texts,
            uses_btree,
        })
    }

    /// Copy every record from a source store into one of this write group's
    /// stores, preserving keys and parents. The source records are pulled as
    /// fulltext and re-added, recompressing them into the new pack.
    fn copy_store(&self, source: &Store, target: RepackTarget) -> Result<(), RepositoryError> {
        let mut keys = source.keys()?;
        keys.sort();
        self.copy_store_keys(source, target, &keys)
    }

    /// Copy just `keys` from a source store into one of this write group's
    /// stores, preserving keys and parents. Used by the same-format streaming
    /// fetch to copy exactly the records belonging to the fetched revisions.
    fn copy_store_keys(
        &self,
        source: &Store,
        target: RepackTarget,
        keys: &[KnitKey],
    ) -> Result<(), RepositoryError> {
        use crate::versionedfile::ContentFactory;
        let store = match target {
            RepackTarget::Revisions => &self.revisions,
            RepackTarget::Inventories => &self.inventories,
            RepackTarget::Texts => &self.texts,
            RepackTarget::Signatures => &self.signatures,
        };
        for record in source.get_record_stream(keys, "unordered", true)? {
            if record.storage_kind() == "absent" {
                continue;
            }
            let key = record.key.clone();
            let parents = record.parents.clone().unwrap_or_default();
            let lines: Vec<Vec<u8>> = record.to_lines().map(|l| l.into_owned()).collect();
            store.add_lines(key, parents, lines, true)?;
        }
        Ok(())
    }

    /// Flush the pack, its four indices and an updated `pack-names`. Returns
    /// the new pack's name (its content md5).
    fn finish(
        self,
        transport: &dyn Transport,
        existing: &[(String, Vec<u8>)],
    ) -> Result<String, RepositoryError> {
        let WriteGroup {
            pack_name: _,
            pack,
            revisions,
            inventories,
            signatures,
            texts,
            uses_btree,
        } = self;
        let rix = serialise_index(revisions.index, 1, uses_btree)?;
        let iix = serialise_index(inventories.index, 1, uses_btree)?;
        let tix = serialise_index(texts.index, 2, uses_btree)?;
        // Signatures are keyed by revision id (1 element); knit-pack has no
        // chk index.
        let six = serialise_index(signatures.index, 1, uses_btree)?;

        let pack_bytes = {
            let mut writer = pack.lock().unwrap();
            writer
                .end()
                .map_err(|e| RepositoryError::Corrupt(format!("pack end: {e}")))?;
            std::mem::take(writer.get_mut())
        };
        // Name the finished pack by the md5 of its content, as brz does (the
        // write group's token was only used while collecting records; index
        // values store offsets, not the pack name).
        let pack_name = md5_hex(&pack_bytes);
        transport.put_bytes(&format!("packs/{pack_name}.pack"), &pack_bytes, None)?;

        let write_index = |ext: &str, bytes: &[u8]| -> Result<usize, RepositoryError> {
            transport.put_bytes(&format!("indices/{pack_name}{ext}"), bytes, None)?;
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

        let mut names = super::pack_index::IndexBuilder::new(uses_btree, 0, 1);
        for (name, value) in existing {
            names
                .add_node(vec![name.clone().into_bytes()], value.clone(), vec![])
                .map_err(|e| RepositoryError::Corrupt(format!("pack-names node: {e}")))?;
        }
        names
            .add_node(vec![pack_name.clone().into_bytes()], new_value, vec![])
            .map_err(|e| RepositoryError::Corrupt(format!("pack-names node: {e}")))?;
        let names_bytes = names
            .finish()
            .map_err(|e| RepositoryError::Corrupt(format!("pack-names finish: {e}")))?;
        transport.put_bytes("pack-names", &names_bytes, None)?;
        Ok(pack_name)
    }
}

/// The lowercase-hex md5 digest of `bytes`, the form brz names a pack by.
fn md5_hex(bytes: &[u8]) -> String {
    use md5::{Digest, Md5};
    Md5::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Serialise a write index's collected records into a pack index of the
/// format's index type (btree for 1.9+, format-1 GraphIndex for 0.92/1.6).
fn serialise_index(
    index: KnitWriteIndex,
    key_elements: usize,
    uses_btree: bool,
) -> Result<Vec<u8>, RepositoryError> {
    let has_deltas = index.has_deltas;
    let ref_lists = if has_deltas { 2 } else { 1 };
    let mut builder = super::pack_index::IndexBuilder::new(uses_btree, ref_lists, key_elements);
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
            .map_err(|e| RepositoryError::Corrupt(format!("index node: {e}")))?;
    }
    builder
        .finish()
        .map_err(|e| RepositoryError::Corrupt(format!("index finish: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::LocalTransport;
    use std::sync::Arc;

    fn temp() -> (tempfile::TempDir, SharedTransport) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("repository");
        std::fs::create_dir_all(&path).unwrap();
        (dir, Arc::new(LocalTransport::new(&path)))
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

    fn knitpack6() -> &'static RepositoryFormat {
        super::super::format::find_format(b"Bazaar RepositoryFormatKnitPack6 (bzr 1.9)\n").unwrap()
    }

    fn make_revision(id: &[u8]) -> crate::revision::Revision {
        crate::revision::Revision::new(
            crate::RevisionId::from(id),
            vec![],
            Some("T <t@e>".to_string()),
            "m".to_string(),
            std::collections::HashMap::new(),
            None,
            1577880000.0,
            Some(0),
        )
    }

    /// Commit one revision (root-only inventory + one file text) in its own
    /// write group, producing one pack.
    fn commit_one(repo: &mut KnitPackRepository, rev: &[u8]) {
        repo.start_write_group().unwrap();
        let root = crate::inventory::ROOT_ID;
        let entries = vec![
            crate::inventory::Entry::root(
                crate::FileId::from(root),
                Some(crate::RevisionId::from(rev)),
            ),
            crate::inventory::Entry::file(
                crate::FileId::from(&b"file-1"[..]),
                "a.txt".into(),
                crate::FileId::from(root),
                Some(crate::RevisionId::from(rev)),
                Some(crate::weave::sha_strings(&[b"hello\n"])),
                Some(6),
                Some(false),
                None,
            ),
        ];
        let inv_sha = repo
            .add_inventory_from_entries(rev, &[], root, &entries)
            .unwrap();
        let mut r = make_revision(rev);
        r.inventory_sha1 = Some(inv_sha);
        repo.add_revision(&r, &[]).unwrap();
        repo.add_text(b"file-1", rev, &[], b"hello\n").unwrap();
        repo.commit_write_group().unwrap();
    }

    /// pack() combines knit-pack packs into one, obsoletes the old packs, and
    /// keeps all data readable.
    #[test]
    fn pack_combines_packs() {
        let (_d, t) = temp();
        let mut repo = KnitPackRepository::create(t.clone(), knitpack6()).unwrap();
        commit_one(&mut repo, b"rev-1");
        commit_one(&mut repo, b"rev-2");
        commit_one(&mut repo, b"rev-3");

        let before = read_pack_names(t.as_ref()).unwrap();
        assert_eq!(before.len(), 3);

        repo.pack().unwrap();

        let after = read_pack_names(t.as_ref()).unwrap();
        assert_eq!(after.len(), 1);
        assert!(!before.contains(&after[0]));
        for name in &before {
            assert!(t.has(&format!("obsolete_packs/{name}.pack")).unwrap());
            assert!(!t.has(&format!("packs/{name}.pack")).unwrap());
        }

        // Everything reads back through the new pack.
        let repo = KnitPackRepository::open(t).unwrap();
        let mut ids = repo.all_revision_ids().unwrap();
        ids.sort();
        assert_eq!(
            ids,
            vec![b"rev-1".to_vec(), b"rev-2".to_vec(), b"rev-3".to_vec()]
        );
        for rev in [&b"rev-1"[..], b"rev-2", b"rev-3"] {
            assert_eq!(repo.get_revision(rev).unwrap().message, "m");
            assert_eq!(repo.get_file_text(b"file-1", rev).unwrap(), b"hello\n");
        }
    }

    /// A committed knit-pack pack is named by the md5 of its content.
    #[test]
    fn pack_name_is_content_md5() {
        let (_d, t) = temp();
        let mut repo = KnitPackRepository::create(t.clone(), knitpack6()).unwrap();
        commit_one(&mut repo, b"rev-1");
        let names = read_pack_names(t.as_ref()).unwrap();
        assert_eq!(names.len(), 1);
        let pack_bytes = t.get_bytes(&format!("packs/{}.pack", names[0])).unwrap();
        assert_eq!(names[0], md5_hex(&pack_bytes));
    }

    /// Committing many packs triggers autopack, bounding the pack count.
    #[test]
    fn autopack_bounds_pack_count_on_commit() {
        let (_d, t) = temp();
        let mut repo = KnitPackRepository::create(t.clone(), knitpack6()).unwrap();
        for i in 0..12u32 {
            let rev = format!("rev-{i}");
            commit_one(&mut repo, rev.as_bytes());
        }
        let names = read_pack_names(t.as_ref()).unwrap();
        assert!(
            names.len() < 12,
            "autopack should consolidate, got {}",
            names.len()
        );

        let repo = KnitPackRepository::open(t).unwrap();
        assert_eq!(repo.all_revision_ids().unwrap().len(), 12);
        for i in 0..12u32 {
            let rev = format!("rev-{i}");
            assert_eq!(
                repo.get_file_text(b"file-1", rev.as_bytes()).unwrap(),
                b"hello\n"
            );
        }
    }
}

//! The non-pack knit repository ("Bazaar-NG Knit Repository Format 1").
//!
//! Unlike the pack formats, this stores each object kind as a standalone
//! knit: `revisions.{knit,kndx}` and `inventory.{knit,kndx}` (one knit each,
//! via a [`ConstantMapper`](crate::key_mapper::ConstantMapper)) and the file
//! texts as per-file knits under `knits/<hash>/<file_id>.{knit,kndx}` (via a
//! [`HashPrefixMapper`](crate::key_mapper::HashPrefixMapper)). Writes append
//! to the knit and its kndx index immediately, so there is no pack-style
//! write group.
//!
//! The heavy lifting is the crate's knit primitives: [`KndxIndex`] over the
//! `.kndx` files and [`KnitKeyAccess`] over the `.knit` files, composed by
//! [`KnitVersionedFiles`]. XML (v5) serialises the revisions and inventories.

use crate::key_mapper::{ConstantMapper, HashPrefixMapper};
use crate::knit::{
    KndxIndex, KnitAnnotateFactory, KnitFactory, KnitKey, KnitKeyAccess, KnitPlainFactory,
    KnitVersionedFiles,
};
use crate::transport::SharedTransport;

use super::format::RepositoryFormat;
use super::pack_2a::RepositoryError;
use crate::declare_repository_format;
use crate::xml_serializer::{
    XMLInventorySerializer5, XMLInventorySerializer6, XMLInventorySerializer7,
    XMLRevisionSerializer5,
};

declare_repository_format! {
    FORMAT_KNIT_1 {
        format_string: b"Bazaar-NG Knit Repository Format 1",
        description: "Knit repository format 1",
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer5,
        open: open_knit,
        create: create_knit,
        supported: true,
        deprecated: true,
    }
}

declare_repository_format! {
    FORMAT_KNIT_3 {
        format_string: b"Bazaar Knit Repository Format 3 (bzr 0.15)\n",
        description: "Knit repository format 3 (rich root, subtrees)",
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer7,
        open: open_knit,
        create: create_knit,
        rich_root_data: true,
        supports_tree_reference: true,
        supported: true,
        deprecated: true,
    }
}

declare_repository_format! {
    FORMAT_KNIT_4 {
        format_string: b"Bazaar Knit Repository Format 4 (bzr 1.0)\n",
        description: "Knit repository format 4 (rich root)",
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer6,
        open: open_knit,
        create: create_knit,
        rich_root_data: true,
        supported: true,
        deprecated: true,
    }
}

/// A knit store of one object kind, keyed by a mapper and parsed by a
/// factory. Revisions and inventories use the plain factory; file texts use
/// the annotated factory (brz annotates per-file knits).
type KnitStore<M, F> =
    KnitVersionedFiles<KndxIndex<SharedTransport, M>, KnitKeyAccess<SharedTransport, M>, F>;

fn make_store<M, F>(transport: &SharedTransport, mapper: M, factory: F) -> KnitStore<M, F>
where
    M: crate::key_mapper::Mapper + Clone,
    F: KnitFactory,
{
    let index = KndxIndex::new(transport.clone(), mapper.clone());
    let access = KnitKeyAccess::new(transport.clone(), mapper);
    // max_delta_chain 200 matches brz's knit default; the writer here always
    // appends fulltext, and the reader follows any deltas brz wrote.
    KnitVersionedFiles::new(index, access, factory, 200)
}

/// A non-pack knit repository, accessed through a transport rooted at
/// `.bzr/repository`.
pub struct KnitRepository {
    format: &'static RepositoryFormat,
    revisions: KnitStore<ConstantMapper, KnitPlainFactory>,
    inventories: KnitStore<ConstantMapper, KnitPlainFactory>,
    signatures: KnitStore<ConstantMapper, KnitPlainFactory>,
    texts: KnitStore<HashPrefixMapper, KnitAnnotateFactory>,
}

impl KnitRepository {
    /// Open the knit repository whose `.bzr/repository` directory is rooted
    /// at `transport`.
    pub fn open(transport: SharedTransport) -> Result<Self, RepositoryError> {
        let format = check_format(transport.as_ref())?;
        let revisions = make_store(
            &transport,
            ConstantMapper {
                result: "revisions".into(),
            },
            KnitPlainFactory,
        );
        let inventories = make_store(
            &transport,
            ConstantMapper {
                result: "inventory".into(),
            },
            KnitPlainFactory,
        );
        let signatures = make_store(
            &transport,
            ConstantMapper {
                result: "signatures".into(),
            },
            KnitPlainFactory,
        );
        // The revisions and inventory stores must have their one prefix loaded
        // before keys/reads see anything (the kndx index is otherwise lazy).
        // The signatures store is loaded lazily on first access: most repos
        // are unsigned, and eager-loading would create an empty signatures.kndx
        // that brz never writes.
        for store in [&revisions, &inventories] {
            store
                .index()
                .load_prefix_typed(Vec::new())
                .map_err(|e| RepositoryError::Corrupt(format!("load kndx: {e:?}")))?;
        }
        // File texts live under knits/<hash>/<file_id>.{knit,kndx}.
        let knits = transport
            .subtransport("knits")
            .map_err(|e| RepositoryError::Corrupt(format!("knits subtransport: {e}")))?;
        Ok(KnitRepository {
            format,
            revisions,
            inventories,
            signatures,
            texts: make_store(&knits, HashPrefixMapper, KnitAnnotateFactory),
        })
    }

    /// Create an empty knit repository of `format` at `transport` and open
    /// it. The stores create their files lazily on first write, so only the
    /// `format` marker and `knits/` directory are written here.
    pub fn create(
        transport: SharedTransport,
        format: &'static RepositoryFormat,
    ) -> Result<Self, RepositoryError> {
        if !std::ptr::fn_addr_eq(format.open, open_knit as super::format::OpenFn) {
            return Err(RepositoryError::UnsupportedFormat(
                format.get_format_description(),
            ));
        }
        transport.mkdir("")?;
        transport.mkdir("knits")?;
        transport.put_bytes("format", format.format_string(), None)?;
        Self::open(transport)
    }

    /// The format this repository was opened as.
    pub fn format(&self) -> &'static RepositoryFormat {
        self.format
    }

    fn inventory_serializer(&self) -> &'static dyn crate::serializer::InventorySerializer {
        self.format.inventory_serializer
    }

    /// All revision ids, sorted.
    pub fn all_revision_ids(&self) -> Result<Vec<Vec<u8>>, RepositoryError> {
        let mut ids: Vec<Vec<u8>> = self
            .revisions
            .keys()
            .map_err(|e| RepositoryError::Corrupt(format!("revision keys: {e}")))?
            .into_iter()
            .filter_map(|k| k.into_iter().next())
            .collect();
        ids.sort();
        Ok(ids)
    }

    /// The stored parent ids of each of `revision_ids` (present ones only),
    /// read from the revisions kndx index.
    pub fn get_parent_map(
        &self,
        revision_ids: &[Vec<u8>],
    ) -> Result<std::collections::HashMap<Vec<u8>, Vec<Vec<u8>>>, RepositoryError> {
        let keys: Vec<crate::knit::KnitKey> =
            revision_ids.iter().map(|r| vec![r.clone()]).collect();
        let raw = self
            .revisions
            .get_parent_map(&keys)
            .map_err(|e| RepositoryError::Corrupt(format!("parent map: {e}")))?;
        Ok(super::unkey_knit_parent_map(raw))
    }

    /// Read and parse a revision (XML, serializer v5).
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

    /// Read the inventory for a revision as an in-memory
    /// [`MutableInventory`](crate::inventory::MutableInventory).
    pub fn get_inventory(
        &self,
        revision_id: &[u8],
    ) -> Result<crate::inventory::MutableInventory, RepositoryError> {
        let key: KnitKey = vec![revision_id.to_vec()];
        let xml = self
            .inventories
            .get_text(&key)
            .map_err(|e| RepositoryError::Corrupt(format!("inventory: {e}")))?;
        let lines: Vec<Vec<u8>> = split_lines(&xml);
        let line_refs: Vec<&[u8]> = lines.iter().map(|l| l.as_slice()).collect();
        self.inventory_serializer()
            .read_inventory_from_lines(&line_refs, Some(crate::RevisionId::from(revision_id)))
            .map_err(|e| RepositoryError::Corrupt(format!("inventory parse: {e:?}")))
    }

    /// Read the file text for `(file_id, revision)`.
    pub fn get_file_text(
        &self,
        file_id: &[u8],
        revision: &[u8],
    ) -> Result<Vec<u8>, RepositoryError> {
        // The per-file text knit is loaded lazily by its file_id prefix.
        self.texts
            .index()
            .load_prefix_typed(vec![file_id.to_vec()])
            .map_err(|e| RepositoryError::Corrupt(format!("load text kndx: {e:?}")))?;
        let key: KnitKey = vec![file_id.to_vec(), revision.to_vec()];
        self.texts
            .get_text(&key)
            .map_err(|e| RepositoryError::Corrupt(format!("text: {e}")))
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
        self.revisions
            .add_lines(key, parent_keys, split_lines(&bytes), false)
            .map_err(|e| RepositoryError::Corrupt(format!("add revision: {e}")))?;
        Ok(())
    }

    /// Add a signature text for `revision_id` (the clearsigned testament) to
    /// the `signatures` knit.
    pub fn add_signature(
        &mut self,
        revision_id: &[u8],
        signature: &[u8],
    ) -> Result<(), RepositoryError> {
        let key: KnitKey = vec![revision_id.to_vec()];
        self.signatures
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
            Err(e) => Err(RepositoryError::Corrupt(format!("signature: {e}"))),
        }
    }

    /// Build the inventory from `entries`, serialise it to XML, and add it.
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
        self.store_inventory(revision_id, parents, &inv)
    }

    /// Build the inventory for `new_revision_id` by applying `delta` to the
    /// basis inventory, then serialise and store it.
    pub fn add_inventory_by_delta(
        &mut self,
        basis_revision_id: &[u8],
        delta: &crate::inventory_delta::InventoryDelta,
        new_revision_id: &[u8],
        parents: &[Vec<u8>],
    ) -> Result<Vec<u8>, RepositoryError> {
        let basis = if basis_revision_id == crate::branch::NULL_REVISION {
            crate::inventory::MutableInventory::new()
        } else {
            self.get_inventory(basis_revision_id)?
        };
        let new_inv = basis
            .create_by_apply_delta(delta, crate::RevisionId::from(new_revision_id))
            .map_err(|e| RepositoryError::Corrupt(format!("apply inventory delta: {e:?}")))?;
        self.store_inventory(new_revision_id, parents, &new_inv)
    }

    fn store_inventory(
        &mut self,
        revision_id: &[u8],
        parents: &[Vec<u8>],
        inv: &crate::inventory::MutableInventory,
    ) -> Result<Vec<u8>, RepositoryError> {
        let lines = self
            .inventory_serializer()
            .write_inventory_to_lines(inv, false)
            .map_err(|e| RepositoryError::Corrupt(format!("serialise inventory: {e:?}")))?;
        let line_refs: Vec<&[u8]> = lines.iter().map(|l| l.as_slice()).collect();
        let sha1 = crate::weave::sha_strings(&line_refs);
        let key: KnitKey = vec![revision_id.to_vec()];
        let parent_keys: Vec<KnitKey> = parents.iter().map(|p| vec![p.clone()]).collect();
        self.inventories
            .add_lines(key, parent_keys, lines, false)
            .map_err(|e| RepositoryError::Corrupt(format!("add inventory: {e}")))?;
        Ok(sha1)
    }

    /// Add a file text, keyed by `(file_id, revision)`.
    pub fn add_text(
        &mut self,
        file_id: &[u8],
        revision: &[u8],
        parents: &[(Vec<u8>, Vec<u8>)],
        bytes: &[u8],
    ) -> Result<(), RepositoryError> {
        self.texts
            .index()
            .load_prefix_typed(vec![file_id.to_vec()])
            .map_err(|e| RepositoryError::Corrupt(format!("load text kndx: {e:?}")))?;
        let key: KnitKey = vec![file_id.to_vec(), revision.to_vec()];
        let parent_keys: Vec<KnitKey> = parents
            .iter()
            .map(|(f, r)| vec![f.clone(), r.clone()])
            .collect();
        self.texts
            .add_lines(key, parent_keys, split_lines(bytes), false)
            .map_err(|e| RepositoryError::Corrupt(format!("add text: {e}")))?;
        Ok(())
    }
}

impl super::Repository for KnitRepository {
    fn format(&self) -> &'static RepositoryFormat {
        KnitRepository::format(self)
    }

    fn all_revision_ids(&self) -> Result<Vec<Vec<u8>>, RepositoryError> {
        KnitRepository::all_revision_ids(self)
    }

    fn get_parent_map(
        &self,
        revision_ids: &[Vec<u8>],
    ) -> Result<std::collections::HashMap<Vec<u8>, Vec<Vec<u8>>>, RepositoryError> {
        KnitRepository::get_parent_map(self, revision_ids)
    }

    fn get_revision(
        &self,
        revision_id: &[u8],
    ) -> Result<crate::revision::Revision, RepositoryError> {
        KnitRepository::get_revision(self, revision_id)
    }

    fn get_inventory(
        &self,
        revision_id: &[u8],
    ) -> Result<Box<dyn crate::inventory::Inventory>, RepositoryError> {
        Ok(Box::new(KnitRepository::get_inventory(self, revision_id)?))
    }

    fn get_file_text(&self, file_id: &[u8], revision: &[u8]) -> Result<Vec<u8>, RepositoryError> {
        KnitRepository::get_file_text(self, file_id, revision)
    }

    fn start_write_group(&mut self) -> Result<(), RepositoryError> {
        // Knit writes append immediately; there is no write group.
        Ok(())
    }

    fn add_revision(
        &mut self,
        revision: &crate::revision::Revision,
        parents: &[Vec<u8>],
    ) -> Result<(), RepositoryError> {
        KnitRepository::add_revision(self, revision, parents)
    }

    fn add_inventory_from_entries(
        &mut self,
        revision_id: &[u8],
        parents: &[Vec<u8>],
        root_id: &[u8],
        entries: &[crate::inventory::Entry],
    ) -> Result<Vec<u8>, RepositoryError> {
        KnitRepository::add_inventory_from_entries(self, revision_id, parents, root_id, entries)
    }

    fn add_inventory_by_delta(
        &mut self,
        basis_revision_id: &[u8],
        delta: &crate::inventory_delta::InventoryDelta,
        new_revision_id: &[u8],
        parents: &[Vec<u8>],
    ) -> Result<Vec<u8>, RepositoryError> {
        KnitRepository::add_inventory_by_delta(
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
        KnitRepository::add_text(self, file_id, revision, parents, bytes)
    }

    fn add_signature_text(
        &mut self,
        revision_id: &[u8],
        signature: &[u8],
    ) -> Result<(), RepositoryError> {
        KnitRepository::add_signature(self, revision_id, signature)
    }

    fn get_signature_text(&self, revision_id: &[u8]) -> Result<Option<Vec<u8>>, RepositoryError> {
        KnitRepository::get_signature_text(self, revision_id)
    }

    fn commit_write_group(&mut self) -> Result<(), RepositoryError> {
        Ok(())
    }
}

/// Verify the `format` marker is a supported non-pack knit format.
fn check_format(
    transport: &dyn crate::transport::Transport,
) -> Result<&'static RepositoryFormat, RepositoryError> {
    let marker = transport.get_bytes("format")?;
    let format = super::format::find_format(&marker)
        .ok_or_else(|| RepositoryError::UnknownFormat(marker.clone()))?;
    if !std::ptr::fn_addr_eq(format.open, open_knit as super::format::OpenFn) {
        return Err(RepositoryError::UnsupportedFormat(
            format.get_format_description(),
        ));
    }
    Ok(format)
}

/// Open the repository at `transport` as a non-pack knit repository. The
/// [`OpenFn`](super::format::OpenFn) carried by every knit
/// [`RepositoryFormat`].
pub fn open_knit(
    transport: SharedTransport,
) -> Result<Box<dyn super::Repository>, RepositoryError> {
    Ok(Box::new(KnitRepository::open(transport)?))
}

/// Create an empty non-pack knit repository of `format` at `transport`. The
/// [`CreateFn`](super::format::CreateFn) carried by every knit
/// [`RepositoryFormat`].
pub fn create_knit(
    format: &'static RepositoryFormat,
    transport: SharedTransport,
) -> Result<Box<dyn super::Repository>, RepositoryError> {
    Ok(Box::new(KnitRepository::create(transport, format)?))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::LocalTransport;
    use std::sync::Arc;

    fn temp() -> (tempfile::TempDir, SharedTransport) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("repository");
        (dir, Arc::new(LocalTransport::new(&path)))
    }

    #[test]
    fn create_rejects_non_knit_format() {
        let (_d, t) = temp();
        let fmt = super::super::format::find_format(
            b"Bazaar repository format 2a (needs bzr 1.16 or later)\n",
        )
        .unwrap();
        assert!(KnitRepository::create(t, fmt).is_err());
    }
}

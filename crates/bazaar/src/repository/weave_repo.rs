//! The all-in-one weave repository ("Bazaar-NG branch, format 6", bzr 0.8).
//!
//! Unlike the metadir formats, a weave repository has no `.bzr/repository`
//! directory: its stores live directly under `.bzr`. This reader is rooted
//! there and reads:
//!
//! - `inventory.weave` -- a single weave holding every revision's inventory
//!   XML (keyed by revision id), via a constant path.
//! - `weaves/<hash>/<file_id>.weave` -- one weave per file, hash-prefixed.
//! - `revision-store/<hash>/<revid>` -- the revision XML texts (uncompressed
//!   for format 6), hash-prefixed. Signature texts share the store with a
//!   `.sig` suffix.
//!
//! Revisions and inventories serialise with XML v5. Writes append to the
//! weaves and the revision-store immediately, so there is no write group.

use crate::key_mapper::{hash_prefix_map, hash_prefix_unmap};
use crate::transport::{SharedTransport, Transport, TransportError};
use crate::weave::{read_weave_v5, write_weave_v5, WeaveFile};

use super::format::RepositoryFormat;
use super::pack_2a::RepositoryError;
use crate::declare_repository_format;
use crate::xml_serializer::{XMLInventorySerializer5, XMLRevisionSerializer5};

declare_repository_format! {
    FORMAT_WEAVE_6 {
        format_string: b"Bazaar-NG branch, format 6\n",
        description: "Weave repository format 6 (all-in-one)",
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer5,
        // No `open`/`create`: the all-in-one weave repository has no
        // `.bzr/repository/format` marker, so it is reached through BzrDir's
        // all-in-one path rather than the metadir dispatcher.
        all_in_one: true,
        supported: true,
        deprecated: true,
    }
}

/// An all-in-one weave repository, accessed through a transport rooted at
/// `.bzr` (the control directory itself).
pub struct WeaveRepository {
    format: &'static RepositoryFormat,
    transport: SharedTransport,
}

impl WeaveRepository {
    /// Open the weave repository whose stores live directly under `transport`
    /// (rooted at `.bzr`). `format` is the recognised weave format.
    pub fn open(
        transport: SharedTransport,
        format: &'static RepositoryFormat,
    ) -> Result<Self, RepositoryError> {
        if !format.is_all_in_one() {
            return Err(RepositoryError::UnsupportedFormat(
                format.get_format_description(),
            ));
        }
        Ok(WeaveRepository { format, transport })
    }

    /// Create an empty all-in-one weave repository scaffold under `transport`
    /// (rooted at `.bzr`) and open it. Writes the `weaves/` and
    /// `revision-store/` directories and an empty `inventory.weave`. The
    /// branch and working-tree files are the control directory's job, not the
    /// repository's.
    pub fn create(
        transport: SharedTransport,
        format: &'static RepositoryFormat,
    ) -> Result<Self, RepositoryError> {
        if !format.is_all_in_one() {
            return Err(RepositoryError::UnsupportedFormat(
                format.get_format_description(),
            ));
        }
        transport.mkdir("weaves")?;
        transport.mkdir("revision-store")?;
        transport.put_bytes(
            "inventory.weave",
            &write_weave_v5(&WeaveFile::default()),
            None,
        )?;
        Self::open(transport, format)
    }

    /// The format this repository was opened as.
    pub fn format(&self) -> &'static RepositoryFormat {
        self.format
    }

    /// Read and parse the inventory weave.
    fn inventory_weave(&self) -> Result<WeaveFile, RepositoryError> {
        let data = self.transport.get_bytes("inventory.weave")?;
        read_weave_v5(&data)
            .map_err(|e| RepositoryError::Corrupt(format!("inventory.weave: {e:?}")))
    }

    /// Read the lines of `revision_id`'s text from a weave, by version name.
    fn weave_text(weave: &WeaveFile, revision_id: &[u8]) -> Result<Vec<u8>, RepositoryError> {
        let idx = weave
            .lookup(revision_id)
            .ok_or_else(|| RepositoryError::NoSuchRevision(revision_id.to_vec()))?;
        let lines = weave
            .get_lines(idx)
            .map_err(|e| RepositoryError::Corrupt(format!("weave get_lines: {e:?}")))?;
        Ok(lines.concat())
    }

    /// The relative path of a revision (or signature) text in the
    /// revision-store: `revision-store/<hash>/<revid><suffix>`.
    fn revision_store_path(revision_id: &[u8], suffix: &str) -> String {
        format!("revision-store/{}{}", hash_prefix_map(revision_id), suffix)
    }

    /// All revision ids, sorted. Read from the revision-store: every file that
    /// is not a signature (`.sig`), with any `.gz` suffix stripped.
    pub fn all_revision_ids(&self) -> Result<Vec<Vec<u8>>, RepositoryError> {
        let sub = self.transport.subtransport("revision-store")?;
        let mut ids = Vec::new();
        for rel in sub.iter_files_recursive()? {
            let rel = rel.strip_suffix(".gz").unwrap_or(&rel);
            if rel.ends_with(".sig") {
                continue;
            }
            ids.push(hash_prefix_unmap(rel));
        }
        ids.sort();
        Ok(ids)
    }

    /// Read and parse a revision (XML, serializer v5) from the revision-store.
    pub fn get_revision(
        &self,
        revision_id: &[u8],
    ) -> Result<crate::revision::Revision, RepositoryError> {
        use crate::serializer::RevisionSerializer;
        let bytes = self.read_revision_store_text(revision_id)?;
        crate::xml_serializer::XMLRevisionSerializer5
            .read_revision_from_string(&bytes)
            .map_err(|e| RepositoryError::Corrupt(format!("revision parse: {e:?}")))
    }

    /// Read a revision text, trying the uncompressed path then the `.gz` one.
    fn read_revision_store_text(&self, revision_id: &[u8]) -> Result<Vec<u8>, RepositoryError> {
        let plain = Self::revision_store_path(revision_id, "");
        match self.transport.get_bytes(&plain) {
            Ok(b) => Ok(b),
            Err(TransportError::NoSuchFile(_)) => {
                let gz = Self::revision_store_path(revision_id, ".gz");
                match self.transport.get_bytes(&gz) {
                    Ok(b) => gunzip(&b)
                        .map_err(|e| RepositoryError::Corrupt(format!("gunzip revision: {e}"))),
                    Err(TransportError::NoSuchFile(_)) => {
                        Err(RepositoryError::NoSuchRevision(revision_id.to_vec()))
                    }
                    Err(e) => Err(e.into()),
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Read the inventory for a revision from the inventory weave.
    pub fn get_inventory(
        &self,
        revision_id: &[u8],
    ) -> Result<crate::inventory::MutableInventory, RepositoryError> {
        use crate::serializer::InventorySerializer;
        let weave = self.inventory_weave()?;
        let xml = Self::weave_text(&weave, revision_id)?;
        crate::xml_serializer::XMLInventorySerializer5
            .read_inventory_from_lines(
                &[xml.as_slice()],
                Some(crate::RevisionId::from(revision_id)),
            )
            .map_err(|e| RepositoryError::Corrupt(format!("inventory parse: {e:?}")))
    }

    /// Read the file text for `(file_id, revision)` from the file's weave.
    pub fn get_file_text(
        &self,
        file_id: &[u8],
        revision: &[u8],
    ) -> Result<Vec<u8>, RepositoryError> {
        let path = format!("weaves/{}.weave", hash_prefix_map(file_id));
        let data = self.transport.get_bytes(&path)?;
        let weave =
            read_weave_v5(&data).map_err(|e| RepositoryError::Corrupt(format!("{path}: {e:?}")))?;
        Self::weave_text(&weave, revision)
    }

    /// The signature text stored for `revision_id`, or `None` if unsigned.
    pub fn get_signature_text(
        &self,
        revision_id: &[u8],
    ) -> Result<Option<Vec<u8>>, RepositoryError> {
        let path = Self::revision_store_path(revision_id, ".sig");
        match self.transport.get_bytes(&path) {
            Ok(b) => Ok(Some(b)),
            Err(TransportError::NoSuchFile(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Create the parent (bucket) directory of a hash-prefixed path before
    /// writing to it. `hash_prefix_map` paths embed a `<bucket>/` subdir that
    /// `put_bytes` won't create on its own. `mkdir` is idempotent.
    fn ensure_parent_dir(&self, path: &str) -> Result<(), RepositoryError> {
        if let Some((dir, _)) = path.rsplit_once('/') {
            self.transport.mkdir(dir)?;
        }
        Ok(())
    }

    /// Read a weave file, returning an empty weave if it doesn't exist yet.
    fn read_or_empty_weave(&self, path: &str) -> Result<WeaveFile, RepositoryError> {
        match self.transport.get_bytes(path) {
            Ok(data) => {
                read_weave_v5(&data).map_err(|e| RepositoryError::Corrupt(format!("{path}: {e:?}")))
            }
            Err(TransportError::NoSuchFile(_)) => Ok(WeaveFile::default()),
            Err(e) => Err(e.into()),
        }
    }

    /// Append a version named `version_id` (parents named `parents`) holding
    /// `lines` to the weave at `path`, writing it back.
    fn weave_add_version(
        &self,
        path: &str,
        version_id: &[u8],
        parents: &[&[u8]],
        lines: &[Vec<u8>],
    ) -> Result<(), RepositoryError> {
        let mut weave = self.read_or_empty_weave(path)?;
        weave
            .add_lines(version_id, parents, lines, None, None)
            .map_err(|e| RepositoryError::Corrupt(format!("{path}: add_lines: {e}")))?;
        self.ensure_parent_dir(path)?;
        self.transport
            .put_bytes(path, &write_weave_v5(&weave), None)?;
        Ok(())
    }

    /// Add a revision, serialised to XML (v5), to the revision-store.
    fn add_revision(
        &mut self,
        revision: &crate::revision::Revision,
        _parents: &[Vec<u8>],
    ) -> Result<(), RepositoryError> {
        use crate::serializer::RevisionSerializer;
        let bytes = crate::xml_serializer::XMLRevisionSerializer5
            .write_revision_to_string(revision)
            .map_err(|e| RepositoryError::Corrupt(format!("write revision: {e:?}")))?;
        let path = Self::revision_store_path(revision.revision_id.as_bytes(), "");
        self.ensure_parent_dir(&path)?;
        self.transport.put_bytes(&path, &bytes, None)?;
        Ok(())
    }

    /// Serialise `inv` (committed form, entries carry revisions) and add it as
    /// a version to `inventory.weave` keyed by `revision_id`. Returns the
    /// inventory text's sha1.
    fn store_inventory(
        &mut self,
        revision_id: &[u8],
        parents: &[Vec<u8>],
        inv: &crate::inventory::MutableInventory,
    ) -> Result<Vec<u8>, RepositoryError> {
        use crate::serializer::InventorySerializer;
        let lines = crate::xml_serializer::XMLInventorySerializer5
            .write_inventory_to_lines(inv, false)
            .map_err(|e| RepositoryError::Corrupt(format!("serialise inventory: {e:?}")))?;
        let line_refs: Vec<&[u8]> = lines.iter().map(|l| l.as_slice()).collect();
        let sha1 = crate::weave::sha_strings(&line_refs);
        let parent_refs: Vec<&[u8]> = parents.iter().map(|p| p.as_slice()).collect();
        self.weave_add_version("inventory.weave", revision_id, &parent_refs, &lines)?;
        Ok(sha1)
    }

    /// Build the inventory from `entries`, serialise it, and store it.
    fn add_inventory_from_entries(
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
    fn add_inventory_by_delta(
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

    /// Add a file text to the file's weave, keyed by the revision id. The
    /// weave version parents are the revids from the `(file_id, revid)`
    /// parent pairs.
    fn add_text(
        &mut self,
        file_id: &[u8],
        revision: &[u8],
        parents: &[(Vec<u8>, Vec<u8>)],
        bytes: &[u8],
    ) -> Result<(), RepositoryError> {
        let path = format!("weaves/{}.weave", hash_prefix_map(file_id));
        // The file weave's version parents are the parent revids; borrow them
        // directly rather than cloning the (file_id, revid) pairs apart.
        let parent_refs: Vec<&[u8]> = parents.iter().map(|(_, r)| r.as_slice()).collect();
        self.weave_add_version(&path, revision, &parent_refs, &split_lines(bytes))
    }

    /// Store a signature text for `revision_id` in the revision-store.
    fn add_signature_text(
        &mut self,
        revision_id: &[u8],
        signature: &[u8],
    ) -> Result<(), RepositoryError> {
        let path = Self::revision_store_path(revision_id, ".sig");
        self.ensure_parent_dir(&path)?;
        self.transport.put_bytes(&path, signature, None)?;
        Ok(())
    }
}

impl super::Repository for WeaveRepository {
    fn format(&self) -> &'static RepositoryFormat {
        WeaveRepository::format(self)
    }

    fn all_revision_ids(&self) -> Result<Vec<Vec<u8>>, RepositoryError> {
        WeaveRepository::all_revision_ids(self)
    }

    fn get_revision(
        &self,
        revision_id: &[u8],
    ) -> Result<crate::revision::Revision, RepositoryError> {
        WeaveRepository::get_revision(self, revision_id)
    }

    fn get_inventory(
        &self,
        revision_id: &[u8],
    ) -> Result<Box<dyn crate::inventory::Inventory>, RepositoryError> {
        Ok(Box::new(WeaveRepository::get_inventory(self, revision_id)?))
    }

    fn get_file_text(&self, file_id: &[u8], revision: &[u8]) -> Result<Vec<u8>, RepositoryError> {
        WeaveRepository::get_file_text(self, file_id, revision)
    }

    fn start_write_group(&mut self) -> Result<(), RepositoryError> {
        // Weave writes append immediately; there is no write group.
        Ok(())
    }

    fn add_revision(
        &mut self,
        revision: &crate::revision::Revision,
        parents: &[Vec<u8>],
    ) -> Result<(), RepositoryError> {
        WeaveRepository::add_revision(self, revision, parents)
    }

    fn add_inventory_from_entries(
        &mut self,
        revision_id: &[u8],
        parents: &[Vec<u8>],
        root_id: &[u8],
        entries: &[crate::inventory::Entry],
    ) -> Result<Vec<u8>, RepositoryError> {
        WeaveRepository::add_inventory_from_entries(self, revision_id, parents, root_id, entries)
    }

    fn add_inventory_by_delta(
        &mut self,
        basis_revision_id: &[u8],
        delta: &crate::inventory_delta::InventoryDelta,
        new_revision_id: &[u8],
        parents: &[Vec<u8>],
    ) -> Result<Vec<u8>, RepositoryError> {
        WeaveRepository::add_inventory_by_delta(
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
        WeaveRepository::add_text(self, file_id, revision, parents, bytes)
    }

    fn add_signature_text(
        &mut self,
        revision_id: &[u8],
        signature: &[u8],
    ) -> Result<(), RepositoryError> {
        WeaveRepository::add_signature_text(self, revision_id, signature)
    }

    fn get_signature_text(&self, revision_id: &[u8]) -> Result<Option<Vec<u8>>, RepositoryError> {
        WeaveRepository::get_signature_text(self, revision_id)
    }

    fn commit_write_group(&mut self) -> Result<(), RepositoryError> {
        Ok(())
    }
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

/// Decompress a gzip stream. The revision-store is uncompressed for format 6
/// but may be `.gz` in older variants, so the reader handles both.
fn gunzip(data: &[u8]) -> std::io::Result<Vec<u8>> {
    use std::io::Read;
    let mut out = Vec::new();
    flate2::read::GzDecoder::new(data).read_to_end(&mut out)?;
    Ok(out)
}

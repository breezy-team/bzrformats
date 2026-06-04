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
//! Revisions and inventories serialise with XML v5. This reader is read-only;
//! the write methods return [`RepositoryError::UnsupportedFormat`].

use crate::key_mapper::{hash_prefix_map, hash_prefix_unmap};
use crate::transport::{SharedTransport, Transport, TransportError};
use crate::weave::{read_weave_v5, WeaveFile};

use super::format::RepositoryFormat;
use super::pack_2a::RepositoryError;

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
        Err(RepositoryError::UnsupportedFormat(
            "writing weave repositories",
        ))
    }

    fn add_revision(
        &mut self,
        _revision: &crate::revision::Revision,
        _parents: &[Vec<u8>],
    ) -> Result<(), RepositoryError> {
        Err(RepositoryError::UnsupportedFormat(
            "writing weave repositories",
        ))
    }

    fn add_inventory_from_entries(
        &mut self,
        _revision_id: &[u8],
        _parents: &[Vec<u8>],
        _root_id: &[u8],
        _entries: &[crate::inventory::Entry],
    ) -> Result<Vec<u8>, RepositoryError> {
        Err(RepositoryError::UnsupportedFormat(
            "writing weave repositories",
        ))
    }

    fn add_inventory_by_delta(
        &mut self,
        _basis_revision_id: &[u8],
        _delta: &crate::inventory_delta::InventoryDelta,
        _new_revision_id: &[u8],
        _parents: &[Vec<u8>],
    ) -> Result<Vec<u8>, RepositoryError> {
        Err(RepositoryError::UnsupportedFormat(
            "writing weave repositories",
        ))
    }

    fn add_text(
        &mut self,
        _file_id: &[u8],
        _revision: &[u8],
        _parents: &[(Vec<u8>, Vec<u8>)],
        _bytes: &[u8],
    ) -> Result<(), RepositoryError> {
        Err(RepositoryError::UnsupportedFormat(
            "writing weave repositories",
        ))
    }

    fn add_signature_text(
        &mut self,
        _revision_id: &[u8],
        _signature: &[u8],
    ) -> Result<(), RepositoryError> {
        Err(RepositoryError::UnsupportedFormat(
            "writing weave repositories",
        ))
    }

    fn get_signature_text(&self, revision_id: &[u8]) -> Result<Option<Vec<u8>>, RepositoryError> {
        WeaveRepository::get_signature_text(self, revision_id)
    }

    fn commit_write_group(&mut self) -> Result<(), RepositoryError> {
        Err(RepositoryError::UnsupportedFormat(
            "writing weave repositories",
        ))
    }
}

/// Decompress a gzip stream. The revision-store is uncompressed for format 6
/// but may be `.gz` in older variants, so the reader handles both.
fn gunzip(data: &[u8]) -> std::io::Result<Vec<u8>> {
    use std::io::Read;
    let mut out = Vec::new();
    flate2::read::GzDecoder::new(data).read_to_end(&mut out)?;
    Ok(out)
}

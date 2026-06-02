//! Repository access: format metadata/registry plus the pack readers.
//!
//! The two reader families ([`Pack2aRepository`] groupcompress/CHK and
//! [`KnitPackRepository`] knit/XML) implement the [`Repository`] trait,
//! which exposes the common read and write operations. `get_inventory`
//! returns a `Box<dyn Inventory>`, so each repository keeps its own natural
//! inventory representation — 2a a lazy CHK inventory, knit-pack an
//! in-memory one — behind the box, without converting one into the other.

pub mod format;
mod formats;
mod pack_2a;
mod pack_2a_writer;
mod pack_knit;

pub use format::{
    all_formats, find_format, InventorySerializerKind, RepositoryFormat, RevisionSerializerKind,
    StorageKind,
};
pub use pack_2a::{Pack2aRepository, RepositoryError, SharedTransport};
pub use pack_knit::KnitPackRepository;

use crate::inventory::Inventory;

/// The common read interface to a bzr repository.
///
/// Object-safe: `get_inventory` returns `Box<dyn Inventory>`, so a repository
/// can be held as `Box<dyn Repository>` while each format keeps its own
/// inventory representation (a lazy CHK inventory for 2a, an in-memory one
/// for knit-pack) behind the box — no conversion between them.
pub trait Repository: Send + Sync {
    /// The format this repository was opened as.
    fn format(&self) -> &'static RepositoryFormat;

    /// All revision ids in this repository, sorted.
    fn all_revision_ids(&self) -> Result<Vec<Vec<u8>>, RepositoryError>;

    /// Read and parse a revision by id.
    fn get_revision(
        &self,
        revision_id: &[u8],
    ) -> Result<crate::revision::Revision, RepositoryError>;

    /// Read the inventory for a revision.
    fn get_inventory(&self, revision_id: &[u8]) -> Result<Box<dyn Inventory>, RepositoryError>;

    /// Read the full text of a versioned file at a given revision.
    fn get_file_text(&self, file_id: &[u8], revision: &[u8]) -> Result<Vec<u8>, RepositoryError>;

    /// Open a write group: a batch of additions flushed atomically by
    /// [`Repository::commit_write_group`].
    fn start_write_group(&mut self) -> Result<(), RepositoryError>;

    /// Add a revision to the open write group, serialising it with the
    /// format's own revision serializer (bencode for 2a, XML for knit-pack).
    fn add_revision(
        &mut self,
        revision: &crate::revision::Revision,
        parents: &[Vec<u8>],
    ) -> Result<(), RepositoryError>;

    /// Build the inventory for a revision from `entries` and add it to the
    /// open write group, returning the inventory sha1 to record on the
    /// revision. Each format stores the inventory in its own representation
    /// (a CHK inventory for 2a, serialised XML for knit-pack).
    fn add_inventory_from_entries(
        &mut self,
        revision_id: &[u8],
        parents: &[Vec<u8>],
        root_id: &[u8],
        entries: &[crate::inventory::Entry],
    ) -> Result<Vec<u8>, RepositoryError>;

    /// Add a file text (keyed by `(file_id, revision)`) to the open write
    /// group.
    fn add_text(
        &mut self,
        file_id: &[u8],
        revision: &[u8],
        parents: &[(Vec<u8>, Vec<u8>)],
        bytes: &[u8],
    ) -> Result<(), RepositoryError>;

    /// Flush the open write group, committing its additions.
    fn commit_write_group(&mut self) -> Result<(), RepositoryError>;
}

/// Open the repository at `transport` (rooted at `.bzr/repository`),
/// dispatching to the right reader based on the registered format's storage
/// family. Returns an abstract [`Repository`].
pub fn open(transport: SharedTransport) -> Result<Box<dyn Repository>, RepositoryError> {
    let marker = transport.get_bytes("format")?;
    let format =
        find_format(&marker).ok_or_else(|| RepositoryError::UnknownFormat(marker.clone()))?;
    match format.storage {
        StorageKind::GroupCompress => Ok(Box::new(Pack2aRepository::open(transport)?)),
        StorageKind::KnitPack => Ok(Box::new(KnitPackRepository::open(transport)?)),
        StorageKind::Knit => Err(RepositoryError::UnsupportedFormat(
            format.get_format_description(),
        )),
    }
}

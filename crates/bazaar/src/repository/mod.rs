//! Repository access: format metadata/registry plus the pack readers.
//!
//! The two reader families ([`Pack2aRepository`] groupcompress/CHK and
//! [`KnitPackRepository`] knit/XML) implement the [`Repository`] trait,
//! which exposes the common read and write operations. `get_inventory`
//! returns a `Box<dyn Inventory>`, so each repository keeps its own natural
//! inventory representation — 2a a lazy CHK inventory, knit-pack an
//! in-memory one — behind the box, without converting one into the other.

mod commit;
pub mod format;
mod formats;
mod pack_2a;
mod pack_2a_writer;
mod pack_index;
mod pack_knit;
mod tree;

pub use commit::CommitBuilder;
pub use format::{all_formats, find_format, RepositoryFormat};
pub use pack_2a::{Pack2aRepository, RepositoryError, SharedTransport};
pub use pack_knit::KnitPackRepository;
pub use tree::RevisionTree;

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

    /// A read-only view of the tree at `revision_id`: its inventory paired
    /// with the revision id. This is the basis a commit builds its
    /// inventory delta against.
    fn revision_tree(&self, revision_id: &[u8]) -> Result<RevisionTree, RepositoryError> {
        if revision_id == crate::branch::NULL_REVISION {
            // The null revision is the empty tree (the basis of a first
            // commit); there is no stored inventory for it.
            let empty = crate::inventory::MutableInventory::new();
            return Ok(RevisionTree::new(revision_id.to_vec(), Box::new(empty)));
        }
        let inventory = self.get_inventory(revision_id)?;
        Ok(RevisionTree::new(revision_id.to_vec(), inventory))
    }

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

    /// Build the inventory for `new_revision_id` by applying `delta` to the
    /// already-committed `basis_revision_id` inventory, adding it to the open
    /// write group and returning its sha1. Formats that can share storage
    /// (2a's CHK inventory) write only the changed pages; others fall back to
    /// re-serialising the whole inventory.
    fn add_inventory_by_delta(
        &mut self,
        basis_revision_id: &[u8],
        delta: &crate::inventory_delta::InventoryDelta,
        new_revision_id: &[u8],
        parents: &[Vec<u8>],
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

    /// Add a signature text for `revision_id` to the open write group.
    fn add_signature_text(
        &mut self,
        revision_id: &[u8],
        signature: &[u8],
    ) -> Result<(), RepositoryError>;

    /// The signature text stored for `revision_id`, or `None` if unsigned.
    fn get_signature_text(&self, revision_id: &[u8]) -> Result<Option<Vec<u8>>, RepositoryError>;

    /// Flush the open write group, committing its additions.
    fn commit_write_group(&mut self) -> Result<(), RepositoryError>;
}

impl dyn Repository + '_ {
    /// Start an incremental commit against the given parents (the first is
    /// the basis the changes are recorded against; an empty list means a
    /// first commit against the null revision). The repository must already
    /// have an open write group.
    pub fn get_commit_builder(
        &mut self,
        parents: Vec<Vec<u8>>,
        new_revision_id: Vec<u8>,
        committer: String,
        timestamp: u64,
        timezone: i32,
    ) -> CommitBuilder<'_> {
        CommitBuilder::new(
            self,
            parents,
            new_revision_id,
            committer,
            timestamp,
            timezone,
        )
    }
}

/// Open the repository at `transport` (rooted at `.bzr/repository`),
/// dispatching to the right reader through the registered format's `open`
/// function. Returns an abstract [`Repository`].
pub fn open(transport: SharedTransport) -> Result<Box<dyn Repository>, RepositoryError> {
    let marker = transport.get_bytes("format")?;
    let format =
        find_format(&marker).ok_or_else(|| RepositoryError::UnknownFormat(marker.clone()))?;
    (format.open)(transport)
}

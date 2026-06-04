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
#[cfg(feature = "knit")]
mod knit_repo;
mod pack_2a;
mod pack_2a_writer;
#[cfg(feature = "knitpack")]
mod pack_index;
#[cfg(feature = "knitpack")]
mod pack_knit;
mod tree;
#[cfg(feature = "weave")]
mod weave_repo;

pub use commit::CommitBuilder;
pub use format::{all_formats, find_format, RepositoryFormat};
#[cfg(feature = "knit")]
pub use knit_repo::KnitRepository;
pub use pack_2a::{Pack2aRepository, RepositoryError, SharedTransport};
#[cfg(feature = "knitpack")]
pub use pack_knit::KnitPackRepository;
pub use tree::RevisionTree;
#[cfg(feature = "weave")]
pub use weave_repo::WeaveRepository;

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

    /// The stored parent ids of each of `revision_ids`, as a map. Revision ids
    /// not present in the repository are omitted from the result. The parents
    /// come straight from the revision store's index (no body deserialisation),
    /// which is the raw graph data callers build a revision graph from.
    fn get_parent_map(
        &self,
        revision_ids: &[Vec<u8>],
    ) -> Result<std::collections::HashMap<Vec<u8>, Vec<Vec<u8>>>, RepositoryError>;

    /// Whether `revision_id` is present in this repository. Defaults to a
    /// single-key [`get_parent_map`](Repository::get_parent_map) lookup;
    /// backends may override with a cheaper index probe.
    fn has_revision(&self, revision_id: &[u8]) -> Result<bool, RepositoryError> {
        Ok(self
            .get_parent_map(std::slice::from_ref(&revision_id.to_vec()))?
            .contains_key(revision_id))
    }

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

    /// Read the full text of the file at tree-relative `path` in `revision`,
    /// resolving the path to a file id through that revision's inventory.
    /// Errors with [`RepositoryError::NoSuchRevision`] if `path` is not in the
    /// tree (reusing the closest variant for "not found").
    fn get_file_text_at_path(
        &self,
        path: &str,
        revision: &[u8],
    ) -> Result<Vec<u8>, RepositoryError> {
        let tree = self.revision_tree(revision)?;
        let file_id = tree
            .path2id(path)
            .ok_or_else(|| RepositoryError::NoSuchRevision(path.as_bytes().to_vec()))?;
        self.get_file_text(file_id.as_bytes(), revision)
    }

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

/// Convert a knit-keyed parent map (`KnitKey -> [KnitKey]`, where each key's
/// first element is the revision id) to the revision-id-keyed map the
/// [`Repository::get_parent_map`] interface returns. Shared by the knit-pack
/// and non-pack knit backends, which both key by `KnitKey`.
pub(crate) fn unkey_knit_parent_map(
    raw: std::collections::HashMap<crate::knit::KnitKey, Vec<crate::knit::KnitKey>>,
) -> std::collections::HashMap<Vec<u8>, Vec<Vec<u8>>> {
    let mut out = std::collections::HashMap::with_capacity(raw.len());
    for (key, parents) in raw {
        if let Some(revid) = key.into_iter().next() {
            let parent_ids = parents
                .into_iter()
                .filter_map(|p| p.into_iter().next())
                .collect();
            out.insert(revid, parent_ids);
        }
    }
    out
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::LocalTransport;
    use std::sync::Arc;

    /// One repository format under test: a label, a closure that creates a
    /// fresh repository over a transport, a closure that re-opens it, and
    /// whether the format can store signatures.
    struct Scenario {
        label: &'static str,
        create: fn(SharedTransport) -> Box<dyn Repository>,
        reopen: fn(SharedTransport) -> Box<dyn Repository>,
        signs: bool,
    }

    fn knitpack6() -> &'static RepositoryFormat {
        find_format(b"Bazaar RepositoryFormatKnitPack6 (bzr 1.9)\n").unwrap()
    }
    fn knit1() -> &'static RepositoryFormat {
        find_format(b"Bazaar-NG Knit Repository Format 1").unwrap()
    }
    fn weave6() -> &'static RepositoryFormat {
        find_format(b"Bazaar-NG branch, format 6\n").unwrap()
    }

    /// Every repository backend that implements the write side, each wrapped
    /// so the shared round-trip runs against `Box<dyn Repository>`.
    fn scenarios() -> Vec<Scenario> {
        vec![
            Scenario {
                label: "2a",
                create: |t| Box::new(Pack2aRepository::create(t).unwrap()),
                reopen: |t| Box::new(Pack2aRepository::open(t).unwrap()),
                signs: true,
            },
            Scenario {
                label: "knit-pack",
                create: |t| Box::new(KnitPackRepository::create(t, knitpack6()).unwrap()),
                reopen: |t| Box::new(KnitPackRepository::open(t).unwrap()),
                // knit-pack signature writing is an unimplemented TODO.
                signs: false,
            },
            Scenario {
                label: "knit",
                create: |t| Box::new(KnitRepository::create(t, knit1()).unwrap()),
                reopen: |t| Box::new(KnitRepository::open(t).unwrap()),
                signs: false,
            },
            Scenario {
                label: "weave",
                create: |t| Box::new(WeaveRepository::create(t, weave6()).unwrap()),
                reopen: |t| Box::new(WeaveRepository::open(t, weave6()).unwrap()),
                signs: true,
            },
        ]
    }

    fn revision(id: &[u8], parents: Vec<&[u8]>, message: &str) -> crate::revision::Revision {
        crate::revision::Revision::new(
            crate::RevisionId::from(id),
            parents.into_iter().map(crate::RevisionId::from).collect(),
            Some("T <t@e>".to_string()),
            message.to_string(),
            std::collections::HashMap::new(),
            None,
            1577880000.0,
            Some(0),
        )
    }

    fn entries(rev: &[u8]) -> Vec<crate::inventory::Entry> {
        use crate::FileId;
        let root = crate::inventory::ROOT_ID;
        vec![
            crate::inventory::Entry::root(FileId::from(root), Some(crate::RevisionId::from(rev))),
            crate::inventory::Entry::file(
                FileId::from(&b"file-1"[..]),
                "a.txt".into(),
                FileId::from(root),
                Some(crate::RevisionId::from(rev)),
                Some(crate::weave::sha_strings(&[b"hello\n"])),
                Some(6),
                Some(false),
                None,
            ),
        ]
    }

    /// Two revisions, a file text with a per-file parent, an inventory, and a
    /// signature (where supported) round-trip through every write-capable
    /// repository backend. Replaces the per-backend copies of this test.
    #[test]
    fn revision_text_inventory_signature_round_trip() {
        for s in scenarios() {
            let dir = tempfile::tempdir().unwrap();
            let t: SharedTransport = Arc::new(LocalTransport::new(dir.path()));
            let mut repo = (s.create)(t.clone());

            repo.start_write_group().unwrap();
            repo.add_revision(&revision(b"rev-1", vec![], "first"), &[])
                .unwrap();
            repo.add_inventory_from_entries(
                b"rev-1",
                &[],
                crate::inventory::ROOT_ID,
                &entries(b"rev-1"),
            )
            .unwrap();
            repo.add_text(b"file-1", b"rev-1", &[], b"hello\n").unwrap();
            repo.add_text(
                b"file-1",
                b"rev-2",
                &[(b"file-1".to_vec(), b"rev-1".to_vec())],
                b"hello\ngoodbye\n",
            )
            .unwrap();
            repo.add_revision(
                &revision(b"rev-2", vec![b"rev-1"], "second"),
                &[b"rev-1".to_vec()],
            )
            .unwrap();
            if s.signs {
                repo.add_signature_text(b"rev-1", b"-----SIG-----\nsigned\n")
                    .unwrap();
            }
            repo.commit_write_group().unwrap();

            let repo = (s.reopen)(t);
            let mut ids = repo.all_revision_ids().unwrap();
            ids.sort();
            assert_eq!(
                ids,
                vec![b"rev-1".to_vec(), b"rev-2".to_vec()],
                "{}",
                s.label
            );

            // get_parent_map returns the stored parents; a missing revision is
            // omitted. has_revision reflects presence.
            let pm = repo
                .get_parent_map(&[b"rev-1".to_vec(), b"rev-2".to_vec(), b"nope".to_vec()])
                .unwrap();
            assert_eq!(pm.get(&b"rev-1".to_vec()), Some(&vec![]), "{}", s.label);
            assert_eq!(
                pm.get(&b"rev-2".to_vec()),
                Some(&vec![b"rev-1".to_vec()]),
                "{}",
                s.label
            );
            assert!(!pm.contains_key(&b"nope".to_vec()), "{}", s.label);
            assert!(repo.has_revision(b"rev-2").unwrap(), "{}", s.label);
            assert!(!repo.has_revision(b"nope").unwrap(), "{}", s.label);
            assert_eq!(
                repo.get_revision(b"rev-1").unwrap().message,
                "first",
                "{}",
                s.label
            );
            let got2 = repo.get_revision(b"rev-2").unwrap();
            assert_eq!(got2.message, "second", "{}", s.label);
            assert_eq!(
                got2.parent_ids
                    .iter()
                    .map(|p| p.as_bytes().to_vec())
                    .collect::<Vec<_>>(),
                vec![b"rev-1".to_vec()],
                "{}",
                s.label
            );
            assert_eq!(
                repo.get_file_text(b"file-1", b"rev-1").unwrap(),
                b"hello\n",
                "{}",
                s.label
            );
            assert_eq!(
                repo.get_file_text(b"file-1", b"rev-2").unwrap(),
                b"hello\ngoodbye\n",
                "{}",
                s.label
            );
            let inv = repo.get_inventory(b"rev-1").unwrap();
            let paths: Vec<String> = inv.entries().unwrap().into_iter().map(|(p, _)| p).collect();
            assert_eq!(paths, vec!["a.txt".to_string()], "{}", s.label);

            // RevisionTree path lookups: path2id, iter_entries, and reading a
            // file by path.
            let tree = repo.revision_tree(b"rev-1").unwrap();
            assert_eq!(
                tree.path2id("a.txt").map(|f| f.as_bytes().to_vec()),
                Some(b"file-1".to_vec()),
                "{}",
                s.label
            );
            assert!(tree.path2id("nope").is_none(), "{}", s.label);
            let tree_paths: Vec<String> = tree.iter_entries().into_iter().map(|(p, _)| p).collect();
            assert_eq!(tree_paths, vec!["a.txt".to_string()], "{}", s.label);
            assert_eq!(
                repo.get_file_text_at_path("a.txt", b"rev-1").unwrap(),
                b"hello\n",
                "{}",
                s.label
            );

            // A stored signature reads back; an unsigned revision returns None.
            let expected = if s.signs {
                Some(b"-----SIG-----\nsigned\n".to_vec())
            } else {
                None
            };
            assert_eq!(
                repo.get_signature_text(b"rev-1").unwrap(),
                expected,
                "{}",
                s.label
            );
            assert_eq!(
                repo.get_signature_text(b"rev-2").unwrap(),
                None,
                "{}",
                s.label
            );
        }
    }
}

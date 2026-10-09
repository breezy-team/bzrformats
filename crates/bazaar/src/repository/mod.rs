//! Repository access: format metadata/registry plus the pack readers.
//!
//! The two reader families ([`Pack2aRepository`] groupcompress/CHK and
//! [`KnitPackRepository`] knit/XML) implement the [`Repository`] trait,
//! which exposes the common read and write operations. `get_inventory`
//! returns a `Box<dyn Inventory>`, so each repository keeps its own natural
//! inventory representation — 2a a lazy CHK inventory, knit-pack an
//! in-memory one — behind the box, without converting one into the other.

mod check;
mod commit;
mod fetch;
pub mod format;
#[cfg(feature = "knit")]
mod knit_repo;
mod pack_2a;
mod pack_2a_writer;
mod pack_collection;
mod pack_index;
#[cfg(feature = "knitpack")]
mod pack_knit;
mod tree;
#[cfg(feature = "weave")]
mod weave_repo;

pub use check::{check, CheckResult};
pub use commit::CommitBuilder;
pub use fetch::fetch;

/// The outcome of [`Repository::reconcile`]: what the reconcile dropped.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReconcileResult {
    /// Number of stored inventories that were unreachable and discarded.
    pub garbage_inventories: usize,
    /// Whether the repository's storage was regenerated (a new pack written).
    pub repacked: bool,
}
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
use crate::lockable_files::{Lockable, LockableExt as _};

/// The common read interface to a bzr repository.
///
/// Object-safe: `get_inventory` returns `Box<dyn Inventory>`, so a repository
/// can be held as `Box<dyn Repository>` while each format keeps its own
/// inventory representation (a lazy CHK inventory for 2a, an in-memory one
/// for knit-pack) behind the box — no conversion between them.
pub trait Repository: Lockable<Error = RepositoryError> + Send + Sync {
    /// The format this repository was opened as.
    fn format(&self) -> &'static RepositoryFormat;

    /// The repository's lock: a count of read and write locks, with a lock
    /// directory for the formats that take one.
    fn lock(&self) -> &crate::lockable_files::LockableFiles;

    /// Whether a write group is open.
    fn is_in_write_group(&self) -> bool {
        false
    }

    /// Reread what other processes may have changed while the repository
    /// was unlocked; called on the first lock.
    fn refresh_data(&mut self) -> Result<(), RepositoryError> {
        Ok(())
    }

    /// Lock the repository for writing, with `token` taking over a held lock
    /// directory and `waiter` deciding what to do while someone else holds
    /// it.
    fn lock_write_with_token(
        &mut self,
        token: Option<&crate::lockable_files::LockToken>,
        waiter: &mut dyn crate::lockable_files::LockWaiter,
    ) -> Result<crate::lockable_files::WriteLocked, RepositoryError> {
        let first = !self.lock().is_locked();
        let locked = self
            .lock()
            .lock_write(token, waiter)
            .map_err(RepositoryError::Locking)?;
        if first {
            if let Err(e) = self.refresh_data() {
                self.lock().unlock().map_err(RepositoryError::Locking)?;
                return Err(e);
            }
        }
        Ok(locked)
    }

    /// Break the repository's lock directory if someone else holds it and
    /// `confirm` agrees (see [`LockableFiles::break_lock`]).
    ///
    /// [`LockableFiles::break_lock`]: crate::lockable_files::LockableFiles::break_lock
    fn break_lock(
        &self,
        confirm: &mut dyn FnMut(Option<&crate::lockdir::LockHeldInfo>) -> bool,
    ) -> Result<(), RepositoryError> {
        self.lock()
            .break_lock(confirm)
            .map(drop)
            .map_err(RepositoryError::Locking)
    }

    /// Whether the repository is locked for writing.
    fn is_write_locked(&self) -> bool {
        self.lock().lock_mode() == Some(crate::lockable_files::LockMode::Write)
    }

    /// Downcast support, so a backend's format-specific fast path can recover a
    /// same-format `source` (e.g. another `Pack2aRepository`). Each backend
    /// returns `self`; a downcast to a different concrete type fails and the
    /// caller uses the generic path. Used by [`try_fetch_from`](Repository::try_fetch_from)
    /// implementations, not by the generic fetcher.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Try to copy `revision_ids` (topologically ordered, already filtered to
    /// revisions absent here) from `source` into this repository using a
    /// format-specific fast path, opening and committing the write group
    /// itself.
    ///
    /// Returns `Ok(true)` if the fast path applied and the revisions were
    /// copied, `Ok(false)` if no fast path is available for this
    /// source/target pair (the caller then uses the generic rebuild). The
    /// default has no fast path. This is where per-format streaming lives, so
    /// [`crate::repository::fetch`] needs no knowledge of concrete formats.
    fn try_fetch_from(
        &mut self,
        _source: &dyn Repository,
        _revision_ids: &[Vec<u8>],
    ) -> Result<bool, RepositoryError> {
        Ok(false)
    }

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
            return Ok(RevisionTree::new(
                crate::RevisionId::from(revision_id),
                Box::new(empty),
            ));
        }
        let inventory = self.get_inventory(revision_id)?;
        Ok(RevisionTree::new(
            crate::RevisionId::from(revision_id),
            inventory,
        ))
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

    /// Commit the open write group's additions.
    ///
    /// Returns the names of the packs written, which can be passed to `pack`
    /// as a hint, or `None` for formats without packs.
    fn commit_write_group(&mut self) -> Result<Option<Vec<String>>, RepositoryError>;

    /// Abort the open write group, dropping what it added where the format
    /// can.
    fn abort_write_group(&mut self) -> Result<(), RepositoryError>;

    /// Suspend the open write group, returning the tokens that resume it
    /// with [`Repository::resume_write_group`]. Formats that cannot suspend
    /// write groups refuse.
    fn suspend_write_group(&mut self) -> Result<Vec<String>, RepositoryError> {
        Err(RepositoryError::UnsuspendableWriteGroup)
    }

    /// Open a write group holding the suspended write group `tokens`.
    /// Formats that cannot suspend write groups refuse.
    fn resume_write_group(&mut self, _tokens: &[String]) -> Result<(), RepositoryError> {
        if !self.is_write_locked() {
            return Err(RepositoryError::NotWriteLocked);
        }
        Err(RepositoryError::UnsuspendableWriteGroup)
    }

    /// Combine the repository's packs into a single pack.
    ///
    /// The default is a no-op (formats without packs have nothing to combine);
    /// the pack backends override it.
    fn pack(&mut self) -> Result<(), RepositoryError> {
        Ok(())
    }

    /// Repack the smallest packs if the repository has accumulated too many,
    /// per the pack-distribution heuristic. Returns whether a repack happened.
    ///
    /// The default is a no-op returning `false`; the pack backends override it.
    fn autopack(&mut self) -> Result<bool, RepositoryError> {
        Ok(false)
    }

    /// Check the integrity of this repository, returning a report of any
    /// inconsistencies (see [`CheckResult`]). Format-neutral: it cross-checks
    /// the data every format exposes through this trait.
    fn check(&self) -> Result<CheckResult, RepositoryError> {
        check::check(self)
    }

    /// Reconcile this repository: regenerate its storage keeping only the data
    /// reachable from its revisions, discarding garbage (e.g. inventories or
    /// texts left behind by an interrupted operation), and report what was
    /// dropped (see [`ReconcileResult`]).
    ///
    /// The default does nothing (formats without packs have no garbage to
    /// collect); the pack backends override it.
    fn reconcile(&mut self) -> Result<ReconcileResult, RepositoryError> {
        Ok(ReconcileResult::default())
    }

    /// Add a fallback repository consulted for objects this one lacks.
    ///
    /// This is how a stacked branch wires its base repository in: reads that
    /// miss in this repository are retried against the fallback chain, in
    /// order. The default returns [`RepositoryError::UnsupportedFormat`]; only
    /// [`StackedRepository`] (which a stacked-branch open wraps the primary in)
    /// supports it.
    fn add_fallback_repository(
        &mut self,
        _fallback: Box<dyn Repository>,
    ) -> Result<(), RepositoryError> {
        Err(RepositoryError::UnsupportedFormat(
            "repository does not support fallbacks",
        ))
    }

    /// Verify the stored GPG signature of `revision_id` against `certs`.
    ///
    /// Mirrors breezy's `verify_revision_signature`: an unsigned revision is
    /// [`VerificationResult::NotSigned`](crate::gpg::VerificationResult::NotSigned);
    /// otherwise the stored clearsigned text is verified and its plaintext is
    /// compared byte-for-byte against the revision's V1 testament short text.
    /// A plaintext that does not match the testament forces
    /// [`VerificationResult::NotValid`](crate::gpg::VerificationResult::NotValid),
    /// even for a cryptographically good signature.
    #[cfg(feature = "gpg")]
    fn verify_revision_signature(
        &self,
        revision_id: &[u8],
        certs: &[sequoia_openpgp::Cert],
    ) -> Result<crate::gpg::VerificationResult, RepositoryError> {
        use crate::gpg::VerificationResult;
        let Some(signature) = self.get_signature_text(revision_id)? else {
            return Ok(VerificationResult::NotSigned);
        };
        let expected = testament_short_text_for_revision(self, revision_id)?;
        let verification = crate::gpg::verify_clearsigned(&signature, certs);
        if verification.plaintext.as_deref() != Some(expected.as_slice()) {
            return Ok(VerificationResult::NotValid);
        }
        Ok(verification.result)
    }

    /// Like [`verify_revision_signature`](Repository::verify_revision_signature)
    /// but taking a keyring as raw public-key blobs (ASCII-armored or binary),
    /// so callers that do not depend on the OpenPGP crate (e.g. the Python
    /// bindings) can pass keys through as bytes.
    #[cfg(feature = "gpg")]
    fn verify_revision_signature_bytes(
        &self,
        revision_id: &[u8],
        keyring: &[Vec<u8>],
    ) -> Result<crate::gpg::VerificationResult, RepositoryError> {
        let certs = crate::gpg::parse_keyring(keyring)
            .map_err(|e| RepositoryError::Corrupt(format!("keyring: {e}")))?;
        self.verify_revision_signature(revision_id, &certs)
    }
}

/// Build the V1 testament short text for a stored revision, the plaintext a
/// valid signature must reproduce.
///
/// Assembles the V1 testament from the revision and its tree (the inventory
/// entries, root excluded, in `iter_entries` order) and returns
/// `as_short_text(V1)`.
#[cfg(feature = "gpg")]
fn testament_short_text_for_revision(
    repo: &(impl Repository + ?Sized),
    revision_id: &[u8],
) -> Result<Vec<u8>, RepositoryError> {
    use crate::testament::{EntryKind as TKind, Testament, TestamentEntry, TestamentFormat};

    let revision = repo.get_revision(revision_id)?;
    let tree = repo.revision_tree(revision_id)?;

    let mut entries = Vec::new();
    for (path, entry) in tree.iter_entries() {
        // The V1 testament omits the root entry.
        if path.is_empty() || path == "." {
            continue;
        }
        let (kind, content) = match entry.kind() {
            crate::osutils::Kind::File => (
                TKind::File,
                entry.text_sha1().map(|s| s.to_vec()).unwrap_or_default(),
            ),
            crate::osutils::Kind::Directory => (TKind::Directory, Vec::new()),
            crate::osutils::Kind::Symlink => (
                TKind::Symlink,
                entry
                    .symlink_target()
                    .map(|t| t.as_bytes().to_vec())
                    .unwrap_or_default(),
            ),
            crate::osutils::Kind::TreeReference => (TKind::TreeReference, Vec::new()),
        };
        entries.push(TestamentEntry {
            path,
            kind,
            file_id: entry.file_id().as_bytes().to_vec(),
            content,
            revision: entry
                .revision()
                .map(|r| r.as_bytes().to_vec())
                .unwrap_or_default(),
            executable: entry.executable(),
        });
    }

    let testament = Testament {
        revision_id: revision_id.to_vec(),
        committer: revision.committer.clone().unwrap_or_default(),
        timestamp: revision.timestamp as i64,
        timezone: revision.timezone.unwrap_or(0),
        message: revision.message.clone(),
        parent_ids: revision
            .parent_ids
            .iter()
            .map(|p| p.as_bytes().to_vec())
            .collect(),
        revprops: revision
            .properties
            .iter()
            .map(|(k, v)| (k.clone(), String::from_utf8_lossy(v).into_owned()))
            .collect(),
        entries,
    };
    testament
        .as_short_text(TestamentFormat::V1)
        .map_err(|e| RepositoryError::Corrupt(format!("testament: {e}")))
}

/// A repository that consults a chain of fallback repositories for objects its
/// primary store lacks.
///
/// This is the data-path half of branch stacking: the primary is the stacked
/// branch's own (thin) repository, and the fallbacks are the repositories of
/// the branches it is stacked on. Reads try the primary first, then each
/// fallback in order; writes go only to the primary. Mirrors breezy, where a
/// `Repository` keeps a list of `_fallback_repositories` and
/// `add_fallback_repository` appends to it.
pub struct StackedRepository {
    primary: Box<dyn Repository>,
    fallbacks: Vec<Box<dyn Repository>>,
}

/// Whether `e` signals that an object is simply absent (so a fallback should be
/// consulted) rather than a hard error.
fn is_not_present(e: &RepositoryError) -> bool {
    matches!(
        e,
        RepositoryError::NoSuchRevision(_) | RepositoryError::NoSuchFileText { .. }
    )
}

impl StackedRepository {
    /// Wrap `primary`, with no fallbacks yet.
    pub fn new(primary: Box<dyn Repository>) -> Self {
        StackedRepository {
            primary,
            fallbacks: Vec::new(),
        }
    }

    /// Try `f` on the primary, then each fallback in order, returning the first
    /// success. A "not present" error ([`RepositoryError::NoSuchRevision`] or
    /// [`RepositoryError::NoSuchFileText`]) is treated as "not here, try the
    /// next"; any other error propagates immediately. If every repository
    /// misses, the primary's not-present error is returned.
    fn first_present<T>(
        &self,
        mut f: impl FnMut(&dyn Repository) -> Result<T, RepositoryError>,
    ) -> Result<T, RepositoryError> {
        match f(self.primary.as_ref()) {
            Err(e) if is_not_present(&e) => {
                for fallback in &self.fallbacks {
                    match f(fallback.as_ref()) {
                        Err(e) if is_not_present(&e) => continue,
                        other => return other,
                    }
                }
                Err(e)
            }
            other => other,
        }
    }
}

impl StackedRepository {
    /// Read-lock the fallbacks, releasing the primary again on failure.
    fn lock_fallbacks(&mut self) -> Result<(), RepositoryError> {
        for i in 0..self.fallbacks.len() {
            if let Err(e) = self.fallbacks[i].lock_read() {
                for fallback in &mut self.fallbacks[..i] {
                    fallback.unlock()?;
                }
                self.primary.unlock()?;
                return Err(e);
            }
        }
        Ok(())
    }
}

impl Lockable for StackedRepository {
    type Error = RepositoryError;

    /// The fallbacks are read-locked with the first lock.
    fn lock_read(&mut self) -> Result<(), RepositoryError> {
        let first = !self.primary.lock().is_locked();
        self.primary.lock_read()?;
        if first {
            self.lock_fallbacks()?;
        }
        Ok(())
    }

    fn lock_write(
        &mut self,
        waiter: &mut dyn crate::lockable_files::LockWaiter,
    ) -> Result<crate::lockable_files::WriteLocked, RepositoryError> {
        self.lock_write_with_token(None, waiter)
    }

    /// The fallbacks are released with the last lock.
    fn unlock(&mut self) -> Result<Option<crate::lockable_files::LockToken>, RepositoryError> {
        let released = self.primary.unlock()?;
        if !self.primary.lock().is_locked() {
            for fallback in &mut self.fallbacks {
                fallback.unlock()?;
            }
        }
        Ok(released)
    }
}

impl Repository for StackedRepository {
    fn lock(&self) -> &crate::lockable_files::LockableFiles {
        self.primary.lock()
    }

    fn is_in_write_group(&self) -> bool {
        self.primary.is_in_write_group()
    }

    fn refresh_data(&mut self) -> Result<(), RepositoryError> {
        self.primary.refresh_data()
    }

    /// The fallbacks are read-locked with the first lock.
    fn lock_write_with_token(
        &mut self,
        token: Option<&crate::lockable_files::LockToken>,
        waiter: &mut dyn crate::lockable_files::LockWaiter,
    ) -> Result<crate::lockable_files::WriteLocked, RepositoryError> {
        let first = !self.primary.lock().is_locked();
        let locked = self.primary.lock_write_with_token(token, waiter)?;
        if first {
            self.lock_fallbacks()?;
        }
        Ok(locked)
    }

    fn format(&self) -> &'static RepositoryFormat {
        self.primary.format()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn all_revision_ids(&self) -> Result<Vec<Vec<u8>>, RepositoryError> {
        // breezy's all_revision_ids is the repository's own revisions only; a
        // stacked repository does not enumerate its fallbacks' revisions.
        self.primary.all_revision_ids()
    }

    fn get_parent_map(
        &self,
        revision_ids: &[Vec<u8>],
    ) -> Result<std::collections::HashMap<Vec<u8>, Vec<Vec<u8>>>, RepositoryError> {
        let mut map = self.primary.get_parent_map(revision_ids)?;
        // Fill in any ids the primary did not know from the fallbacks.
        let mut missing: Vec<Vec<u8>> = revision_ids
            .iter()
            .filter(|id| !map.contains_key(*id))
            .cloned()
            .collect();
        for fallback in &self.fallbacks {
            if missing.is_empty() {
                break;
            }
            let found = fallback.get_parent_map(&missing)?;
            missing.retain(|id| !found.contains_key(id));
            map.extend(found);
        }
        Ok(map)
    }

    fn has_revision(&self, revision_id: &[u8]) -> Result<bool, RepositoryError> {
        if self.primary.has_revision(revision_id)? {
            return Ok(true);
        }
        for fallback in &self.fallbacks {
            if fallback.has_revision(revision_id)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn get_revision(
        &self,
        revision_id: &[u8],
    ) -> Result<crate::revision::Revision, RepositoryError> {
        self.first_present(|r| r.get_revision(revision_id))
    }

    fn get_inventory(&self, revision_id: &[u8]) -> Result<Box<dyn Inventory>, RepositoryError> {
        self.first_present(|r| r.get_inventory(revision_id))
    }

    fn get_file_text(&self, file_id: &[u8], revision: &[u8]) -> Result<Vec<u8>, RepositoryError> {
        self.first_present(|r| r.get_file_text(file_id, revision))
    }

    fn start_write_group(&mut self) -> Result<(), RepositoryError> {
        self.primary.start_write_group()
    }

    fn add_revision(
        &mut self,
        revision: &crate::revision::Revision,
        parents: &[Vec<u8>],
    ) -> Result<(), RepositoryError> {
        self.primary.add_revision(revision, parents)
    }

    fn add_inventory_from_entries(
        &mut self,
        revision_id: &[u8],
        parents: &[Vec<u8>],
        root_id: &[u8],
        entries: &[crate::inventory::Entry],
    ) -> Result<Vec<u8>, RepositoryError> {
        self.primary
            .add_inventory_from_entries(revision_id, parents, root_id, entries)
    }

    fn add_inventory_by_delta(
        &mut self,
        basis_revision_id: &[u8],
        delta: &crate::inventory_delta::InventoryDelta,
        new_revision_id: &[u8],
        parents: &[Vec<u8>],
    ) -> Result<Vec<u8>, RepositoryError> {
        self.primary
            .add_inventory_by_delta(basis_revision_id, delta, new_revision_id, parents)
    }

    fn add_text(
        &mut self,
        file_id: &[u8],
        revision: &[u8],
        parents: &[(Vec<u8>, Vec<u8>)],
        bytes: &[u8],
    ) -> Result<(), RepositoryError> {
        self.primary.add_text(file_id, revision, parents, bytes)
    }

    fn add_signature_text(
        &mut self,
        revision_id: &[u8],
        signature: &[u8],
    ) -> Result<(), RepositoryError> {
        self.primary.add_signature_text(revision_id, signature)
    }

    fn get_signature_text(&self, revision_id: &[u8]) -> Result<Option<Vec<u8>>, RepositoryError> {
        match self.primary.get_signature_text(revision_id)? {
            Some(sig) => Ok(Some(sig)),
            None => {
                for fallback in &self.fallbacks {
                    if let Some(sig) = fallback.get_signature_text(revision_id)? {
                        return Ok(Some(sig));
                    }
                }
                Ok(None)
            }
        }
    }

    fn commit_write_group(&mut self) -> Result<Option<Vec<String>>, RepositoryError> {
        self.primary.commit_write_group()
    }

    fn abort_write_group(&mut self) -> Result<(), RepositoryError> {
        self.primary.abort_write_group()
    }

    fn suspend_write_group(&mut self) -> Result<Vec<String>, RepositoryError> {
        self.primary.suspend_write_group()
    }

    fn resume_write_group(&mut self, tokens: &[String]) -> Result<(), RepositoryError> {
        self.primary.resume_write_group(tokens)
    }

    fn add_fallback_repository(
        &mut self,
        fallback: Box<dyn Repository>,
    ) -> Result<(), RepositoryError> {
        self.fallbacks.push(fallback);
        Ok(())
    }
}

/// Convert a knit-keyed parent map (`KnitKey -> [KnitKey]`, where each key's
/// first element is the revision id) to the revision-id-keyed map the
/// [`Repository::get_parent_map`] interface returns. Shared by the knit-pack
/// and non-pack knit backends, which both key by `KnitKey`.
#[cfg(any(feature = "knit", feature = "knitpack"))]
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

/// Read-lock `repository`, rereading its data with the first lock: how a
/// repository's [`Lockable::lock_read`] works unless it locks differently.
pub fn lock_read<R: Repository + ?Sized>(repository: &mut R) -> Result<(), RepositoryError> {
    let first = !repository.lock().is_locked();
    repository
        .lock()
        .lock_read()
        .map_err(RepositoryError::Locking)?;
    if first {
        if let Err(e) = repository.refresh_data() {
            repository
                .lock()
                .unlock()
                .map_err(RepositoryError::Locking)?;
            return Err(e);
        }
    }
    Ok(())
}

/// Release one lock on `repository`: how a repository's
/// [`Lockable::unlock`] works unless it locks differently. Releasing the
/// last write lock with a write group open aborts the write group, releases
/// the lock and is an error.
pub fn unlock<R: Repository + ?Sized>(
    repository: &mut R,
) -> Result<Option<crate::lockable_files::LockToken>, RepositoryError> {
    let lock = repository.lock();
    let group_left_open = lock.lock_count() == 1
        && lock.lock_mode() == Some(crate::lockable_files::LockMode::Write)
        && repository.is_in_write_group();
    let aborted = if group_left_open {
        repository.abort_write_group()
    } else {
        Ok(())
    };
    let released = repository
        .lock()
        .unlock()
        .map_err(RepositoryError::Locking)?;
    aborted?;
    if group_left_open {
        return Err(RepositoryError::WriteGroupOpen { released });
    }
    Ok(released)
}

/// Run `f` on `repository` in a write group: the write group is committed
/// if `f` succeeds and aborted if it fails. Returns `f`'s result and the commit's pack hint.
pub fn with_write_group<T: Repository + ?Sized, R>(
    repository: &mut T,
    f: impl FnOnce(&mut T) -> Result<R, RepositoryError>,
) -> Result<(R, Option<Vec<String>>), RepositoryError> {
    repository.start_write_group()?;
    match f(repository) {
        Ok(r) => Ok((r, repository.commit_write_group()?)),
        Err(e) => {
            // Report f's failure rather than a failure to clean up after it.
            if let Err(abort) = repository.abort_write_group() {
                log::warn!("failed to abort write group: {abort}");
            }
            Err(e)
        }
    }
}

/// Run `f` on `repository` under a write lock: a write lock the caller
/// holds is counted, and otherwise one is taken for `f` without waiting for
/// another holder.
pub fn with_write_lock<T: Repository + ?Sized, R>(
    repository: &mut T,
    f: impl FnOnce(&mut T) -> Result<R, RepositoryError>,
) -> Result<R, RepositoryError> {
    let mut locked = repository.write_locked()?;
    let result = f(&mut locked);
    let unlocked = locked.unlock();
    result.and_then(|r| unlocked.map(|_| r))
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

    #[cfg(feature = "knitpack")]
    fn knitpack6() -> &'static RepositoryFormat {
        find_format(b"Bazaar RepositoryFormatKnitPack6 (bzr 1.9)\n").unwrap()
    }
    #[cfg(feature = "knit")]
    fn knit1() -> &'static RepositoryFormat {
        find_format(b"Bazaar-NG Knit Repository Format 1").unwrap()
    }
    #[cfg(feature = "weave")]
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
            #[cfg(feature = "knitpack")]
            Scenario {
                label: "knit-pack",
                create: |t| Box::new(KnitPackRepository::create(t, knitpack6()).unwrap()),
                reopen: |t| Box::new(KnitPackRepository::open(t).unwrap()),
                signs: true,
            },
            #[cfg(feature = "knit")]
            Scenario {
                label: "knit",
                create: |t| Box::new(KnitRepository::create(t, knit1()).unwrap()),
                reopen: |t| Box::new(KnitRepository::open(t).unwrap()),
                signs: true,
            },
            #[cfg(feature = "weave")]
            Scenario {
                label: "weave",
                create: |t| Box::new(WeaveRepository::create(t, weave6()).unwrap()),
                reopen: |t| {
                    let os_lock = crate::lockable_files::TransportLock::new(
                        SharedTransport::clone(&t),
                        crate::lockable_files::BRANCH_LOCK,
                    );
                    Box::new(WeaveRepository::open(t, weave6(), os_lock).unwrap())
                },
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
    /// A committed write group is readable through the repository object
    /// that wrote it, without reopening the repository.
    #[test]
    fn committed_data_is_readable_through_the_writer() {
        for s in scenarios() {
            let dir = tempfile::tempdir().unwrap();
            let t: SharedTransport = Arc::new(LocalTransport::new(dir.path()));
            let mut repo = (s.create)(t);
            {
                let mut repo = repo.write_locked().unwrap();
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
                repo.commit_write_group().unwrap();
                assert!(repo.has_revision(b"rev-1").unwrap(), "{}", s.label);
                repo.unlock().unwrap();
            }
            assert!(repo.has_revision(b"rev-1").unwrap(), "{}", s.label);
            {
                let repo = repo.read_locked().unwrap();
                assert!(repo.has_revision(b"rev-1").unwrap(), "{}", s.label);
                repo.unlock().unwrap();
            }

            // Packing moves the old packs aside; the data stays readable.
            {
                let mut repo = repo.write_locked().unwrap();
                repo.start_write_group().unwrap();
                repo.add_revision(
                    &revision(b"rev-2", vec![b"rev-1"], "second"),
                    &[b"rev-1".to_vec()],
                )
                .unwrap();
                repo.commit_write_group().unwrap();
                repo.unlock().unwrap();
            }
            repo.pack().unwrap();
            for rev in [&b"rev-1"[..], b"rev-2"] {
                assert_eq!(
                    rev,
                    repo.get_revision(rev).unwrap().revision_id.as_bytes(),
                    "{}",
                    s.label
                );
            }
        }
    }

    /// Whether the scenario's format suspends write groups (the pack
    /// formats), as `require_suspendable_write_groups` checks.
    fn suspends(s: &Scenario) -> bool {
        matches!(s.label, "2a" | "knit-pack")
    }

    /// A fresh write-locked repository of scenario `s`, with its transport.
    fn write_locked(s: &Scenario) -> (tempfile::TempDir, SharedTransport, Box<dyn Repository>) {
        let dir = tempfile::tempdir().unwrap();
        let t: SharedTransport = Arc::new(LocalTransport::new(dir.path()));
        let mut repo = (s.create)(t.clone());
        repo.lock_write(&mut crate::lockable_files::NoWait).unwrap();
        (dir, t, repo)
    }

    /// Re-open the repository at `t`, write-locked.
    fn reopen_locked(s: &Scenario, t: &SharedTransport) -> Box<dyn Repository> {
        let mut repo = (s.reopen)(t.clone());
        repo.lock_write(&mut crate::lockable_files::NoWait).unwrap();
        repo
    }

    fn has_text(repo: &dyn Repository, file_id: &[u8], revision: &[u8]) -> bool {
        match repo.get_file_text(file_id, revision) {
            Ok(_) => true,
            Err(RepositoryError::NoSuchFileText { .. }) => false,
            Err(e) => panic!("{}", format!("reading ({file_id:?}, {revision:?}): {e}")),
        }
    }

    #[test]
    fn missing_text_is_no_such_file_text() {
        for s in scenarios() {
            let dir = tempfile::tempdir().unwrap();
            let t: SharedTransport = Arc::new(LocalTransport::new(dir.path()));
            let repo = (s.create)(t);
            assert!(
                matches!(
                    repo.get_file_text(b"file-id", b"revid"),
                    Err(RepositoryError::NoSuchFileText { .. })
                ),
                "{}",
                s.label
            );
        }
    }

    #[test]
    fn write_groups_follow_the_lock() {
        for s in scenarios() {
            let dir = tempfile::tempdir().unwrap();
            let t: SharedTransport = Arc::new(LocalTransport::new(dir.path()));
            let mut repo = (s.create)(t);
            assert!(
                matches!(
                    repo.start_write_group(),
                    Err(RepositoryError::NotWriteLocked)
                ),
                "{}",
                s.label
            );
            repo.lock_read().unwrap();
            assert!(
                matches!(
                    repo.start_write_group(),
                    Err(RepositoryError::NotWriteLocked)
                ),
                "{}",
                s.label
            );
            repo.unlock().unwrap();
            repo.lock_write(&mut crate::lockable_files::NoWait).unwrap();
            assert!(!repo.is_in_write_group(), "{}", s.label);
            repo.start_write_group().unwrap();
            assert!(repo.is_in_write_group(), "{}", s.label);
            assert!(
                matches!(
                    repo.start_write_group(),
                    Err(RepositoryError::AlreadyInWriteGroup)
                ),
                "{}",
                s.label
            );
            repo.commit_write_group().unwrap();
            assert!(!repo.is_in_write_group(), "{}", s.label);
            assert!(
                matches!(
                    repo.abort_write_group(),
                    Err(RepositoryError::NotInWriteGroup)
                ),
                "{}",
                s.label
            );
            repo.start_write_group().unwrap();
            repo.abort_write_group().unwrap();
            assert!(!repo.is_in_write_group(), "{}", s.label);
            repo.unlock().unwrap();
        }
    }

    /// Unlocking in a write group aborts it, releases the lock, and
    /// reports the unlock as an error.
    #[test]
    fn unlock_in_write_group_aborts_it() {
        for s in scenarios() {
            let (_dir, t, mut repo) = write_locked(&s);
            repo.start_write_group().unwrap();
            repo.add_text(b"file-id", b"revid", &[], b"lines\n")
                .unwrap();
            assert!(
                matches!(repo.unlock(), Err(RepositoryError::WriteGroupOpen { .. })),
                "{}",
                s.label
            );
            assert!(!repo.lock().is_locked(), "{}", s.label);
            assert!(!repo.is_in_write_group(), "{}", s.label);
            if suspends(&s) {
                assert!(!has_text((s.reopen)(t).as_ref(), b"file-id", b"revid"));
            }
        }
    }

    #[test]
    fn abort_drops_what_a_pack_write_group_added() {
        for s in scenarios().into_iter().filter(suspends) {
            let (_dir, t, mut repo) = write_locked(&s);
            repo.start_write_group().unwrap();
            repo.add_text(b"file-id", b"revid", &[], b"lines\n")
                .unwrap();
            repo.abort_write_group().unwrap();
            assert!(
                !has_text(repo.as_ref(), b"file-id", b"revid"),
                "{}",
                s.label
            );
            assert!(!has_text((s.reopen)(t).as_ref(), b"file-id", b"revid"));
        }
    }

    #[test]
    fn formats_without_packs_cannot_suspend() {
        for s in scenarios().into_iter().filter(|s| !suspends(s)) {
            let (_dir, _t, mut repo) = write_locked(&s);
            repo.start_write_group().unwrap();
            assert!(
                matches!(
                    repo.suspend_write_group(),
                    Err(RepositoryError::UnsuspendableWriteGroup)
                ),
                "{}",
                s.label
            );
            // Refusing leaves the write group open.
            assert!(repo.is_in_write_group(), "{}", s.label);
            repo.abort_write_group().unwrap();
            assert!(
                matches!(
                    repo.resume_write_group(&[]),
                    Err(RepositoryError::UnsuspendableWriteGroup)
                ),
                "{}",
                s.label
            );
        }
    }

    #[test]
    fn suspend_then_resume_and_commit() {
        for s in scenarios().into_iter().filter(suspends) {
            let (_dir, t, mut repo) = write_locked(&s);
            repo.start_write_group().unwrap();
            repo.add_text(b"file-id", b"revid", &[], b"lines\n")
                .unwrap();
            let tokens = repo.suspend_write_group().unwrap();
            assert_eq!(1, tokens.len(), "{}", s.label);
            assert!(!repo.is_in_write_group(), "{}", s.label);
            // test_read_after_suspend_fails
            assert!(
                !has_text(repo.as_ref(), b"file-id", b"revid"),
                "{}",
                s.label
            );

            let mut same = reopen_locked(&s, &t);
            same.resume_write_group(&tokens).unwrap();
            assert!(same.is_in_write_group(), "{}", s.label);
            assert_eq!(
                b"lines\n".to_vec(),
                same.get_file_text(b"file-id", b"revid").unwrap(),
                "{}",
                s.label
            );
            same.add_text(
                b"file-id",
                b"second-revid",
                &[(b"file-id".to_vec(), b"revid".to_vec())],
                b"more lines\n",
            )
            .unwrap();
            same.commit_write_group().unwrap();
            assert_eq!(
                b"lines\n".to_vec(),
                same.get_file_text(b"file-id", b"revid").unwrap(),
                "{}",
                s.label
            );
            assert_eq!(
                b"more lines\n".to_vec(),
                same.get_file_text(b"file-id", b"second-revid").unwrap(),
                "{}",
                s.label
            );
            let reopened = (s.reopen)(t.clone());
            assert!(
                has_text(reopened.as_ref(), b"file-id", b"revid"),
                "{}",
                s.label
            );
            // A committed write group cannot be resumed again.
            assert!(
                matches!(
                    same.resume_write_group(&tokens),
                    Err(RepositoryError::UnresumableWriteGroup { .. })
                ),
                "{}",
                s.label
            );
        }
    }

    #[test]
    fn resumed_write_groups_suspend_with_their_tokens() {
        for s in scenarios().into_iter().filter(suspends) {
            let (_dir, t, mut repo) = write_locked(&s);
            repo.start_write_group().unwrap();
            repo.add_text(b"file-id", b"revid", &[], b"lines\n")
                .unwrap();
            let tokens = repo.suspend_write_group().unwrap();

            // test_no_op_suspend_resume
            let mut same = reopen_locked(&s, &t);
            same.resume_write_group(&tokens).unwrap();
            assert_eq!(tokens, same.suspend_write_group().unwrap(), "{}", s.label);
            // test_read_after_second_suspend_fails
            assert!(
                !has_text(same.as_ref(), b"file-id", b"revid"),
                "{}",
                s.label
            );

            // test_multiple_resume_write_group
            let mut same = reopen_locked(&s, &t);
            same.resume_write_group(&tokens).unwrap();
            same.add_text(
                b"file-id",
                b"second-revid",
                &[(b"file-id".to_vec(), b"revid".to_vec())],
                b"more lines\n",
            )
            .unwrap();
            let new_tokens = same.suspend_write_group().unwrap();
            assert_eq!(2, new_tokens.len(), "{}", s.label);
            assert_eq!(tokens[0], new_tokens[0], "{}", s.label);
            let mut same = reopen_locked(&s, &t);
            same.resume_write_group(&new_tokens).unwrap();
            assert!(has_text(same.as_ref(), b"file-id", b"revid"), "{}", s.label);
            assert!(
                has_text(same.as_ref(), b"file-id", b"second-revid"),
                "{}",
                s.label
            );
            same.abort_write_group().unwrap();
        }
    }

    #[test]
    fn aborting_a_resumed_write_group_discards_it() {
        for s in scenarios().into_iter().filter(suspends) {
            let (_dir, t, mut repo) = write_locked(&s);
            repo.start_write_group().unwrap();
            repo.add_text(b"file-id", b"revid", &[], b"lines\n")
                .unwrap();
            let tokens = repo.suspend_write_group().unwrap();
            let mut same = reopen_locked(&s, &t);
            same.resume_write_group(&tokens).unwrap();
            same.abort_write_group().unwrap();
            // test_read_after_resume_abort_fails
            assert!(
                !has_text(same.as_ref(), b"file-id", b"revid"),
                "{}",
                s.label
            );
            // test_cannot_resume_aborted_write_group
            let mut same = reopen_locked(&s, &t);
            assert!(
                matches!(
                    same.resume_write_group(&tokens),
                    Err(RepositoryError::UnresumableWriteGroup { .. })
                ),
                "{}",
                s.label
            );
            assert!(!same.is_in_write_group(), "{}", s.label);
        }
    }

    #[test]
    fn empty_write_groups_suspend_to_no_tokens() {
        for s in scenarios().into_iter().filter(suspends) {
            let (_dir, _t, mut repo) = write_locked(&s);
            repo.start_write_group().unwrap();
            assert_eq!(
                Vec::<String>::new(),
                repo.suspend_write_group().unwrap(),
                "{}",
                s.label
            );
            repo.resume_write_group(&[]).unwrap();
            assert!(repo.is_in_write_group(), "{}", s.label);
            repo.abort_write_group().unwrap();
        }
    }

    #[test]
    fn malformed_tokens_are_unresumable() {
        for s in scenarios().into_iter().filter(suspends) {
            let (_dir, _t, mut repo) = write_locked(&s);
            match repo.resume_write_group(&["../pack-names".to_string()]) {
                Err(RepositoryError::UnresumableWriteGroup { tokens, reason }) => {
                    assert_eq!(vec!["../pack-names".to_string()], tokens);
                    assert_eq!("Malformed write group token", reason);
                }
                other => panic!("{}: {other:?}", s.label),
            }
        }
    }

    /// The sorted names in directory `dir` of the repository at `t`.
    fn list_sorted(t: &SharedTransport, dir: &str) -> Vec<String> {
        let mut names = t.list_dir(dir).unwrap();
        names.sort();
        names
    }

    #[test]
    fn missing_suspended_packs_are_unresumable() {
        for s in scenarios().into_iter().filter(suspends) {
            let (_dir, _t, mut repo) = write_locked(&s);
            let token = "0".repeat(32);
            match repo.resume_write_group(std::slice::from_ref(&token)) {
                Err(RepositoryError::UnresumableWriteGroup { tokens, reason }) => {
                    assert_eq!(vec![token.clone()], tokens, "{}", s.label);
                    assert_eq!(format!("No such file: upload/{token}.pack"), reason);
                }
                other => panic!("{}: {other:?}", s.label),
            }
            assert!(!repo.is_in_write_group(), "{}", s.label);
        }
    }

    #[test]
    fn resuming_in_a_write_group_is_an_error() {
        for s in scenarios().into_iter().filter(suspends) {
            let (_dir, _t, mut repo) = write_locked(&s);
            repo.start_write_group().unwrap();
            assert!(
                matches!(
                    repo.resume_write_group(&[]),
                    Err(RepositoryError::AlreadyInWriteGroup)
                ),
                "{}",
                s.label
            );
            assert!(repo.is_in_write_group(), "{}", s.label);
            repo.abort_write_group().unwrap();
        }
    }

    #[test]
    fn new_pack_repositories_have_upload_and_obsolete_packs() {
        for s in scenarios().into_iter().filter(suspends) {
            let (_dir, t, _repo) = write_locked(&s);
            assert_eq!(
                Vec::<String>::new(),
                list_sorted(&t, "upload"),
                "{}",
                s.label
            );
            assert_eq!(
                Vec::<String>::new(),
                list_sorted(&t, "obsolete_packs"),
                "{}",
                s.label
            );
        }
    }

    #[test]
    fn commit_returns_the_packs_written() {
        for s in scenarios() {
            let (_dir, t, mut repo) = write_locked(&s);
            repo.start_write_group().unwrap();
            repo.add_text(b"file-id", b"revid", &[], b"lines\n")
                .unwrap();
            let hint = repo.commit_write_group().unwrap();
            if !suspends(&s) {
                assert_eq!(None, hint, "{}", s.label);
                continue;
            }
            let hint = hint.unwrap();
            assert_eq!(1, hint.len(), "{}", s.label);
            assert_eq!(
                vec![format!("{}.pack", hint[0])],
                list_sorted(&t, "packs"),
                "{}",
                s.label
            );
        }
    }

    #[test]
    fn empty_write_groups_commit_no_pack() {
        for s in scenarios().into_iter().filter(suspends) {
            let (_dir, t, mut repo) = write_locked(&s);
            repo.start_write_group().unwrap();
            assert_eq!(
                Some(vec![]),
                repo.commit_write_group().unwrap(),
                "{}",
                s.label
            );
            assert_eq!(
                Vec::<String>::new(),
                list_sorted(&t, "packs"),
                "{}",
                s.label
            );
        }
    }

    /// Committing a resumed write group moves its pack out of the upload
    /// directory and returns it in the hint.
    #[test]
    fn committing_a_resumed_write_group_moves_its_packs() {
        for s in scenarios().into_iter().filter(suspends) {
            let (_dir, t, mut repo) = write_locked(&s);
            repo.start_write_group().unwrap();
            repo.add_text(b"file-id", b"revid", &[], b"lines\n")
                .unwrap();
            let tokens = repo.suspend_write_group().unwrap();
            assert!(
                list_sorted(&t, "upload").contains(&format!("{}.pack", tokens[0])),
                "{}",
                s.label
            );
            assert_eq!(
                Vec::<String>::new(),
                list_sorted(&t, "packs"),
                "{}",
                s.label
            );

            let mut same = reopen_locked(&s, &t);
            same.resume_write_group(&tokens).unwrap();
            assert_eq!(Some(tokens.clone()), same.commit_write_group().unwrap());
            assert_eq!(
                Vec::<String>::new(),
                list_sorted(&t, "upload"),
                "{}",
                s.label
            );
            assert_eq!(
                vec![format!("{}.pack", tokens[0])],
                list_sorted(&t, "packs"),
                "{}",
                s.label
            );
        }
    }

    #[test]
    fn aborting_a_resumed_write_group_deletes_its_uploads() {
        for s in scenarios().into_iter().filter(suspends) {
            let (_dir, t, mut repo) = write_locked(&s);
            repo.start_write_group().unwrap();
            repo.add_text(b"file-id", b"revid", &[], b"lines\n")
                .unwrap();
            let tokens = repo.suspend_write_group().unwrap();
            let mut same = reopen_locked(&s, &t);
            same.resume_write_group(&tokens).unwrap();
            same.abort_write_group().unwrap();
            assert_eq!(
                Vec::<String>::new(),
                list_sorted(&t, "upload"),
                "{}",
                s.label
            );
            assert_eq!(
                Vec::<String>::new(),
                list_sorted(&t, "packs"),
                "{}",
                s.label
            );
        }
    }

    /// Formats that append immediately keep what an aborted write group
    /// added.
    #[test]
    fn abort_keeps_what_an_unsuspendable_write_group_added() {
        for s in scenarios().into_iter().filter(|s| !suspends(s)) {
            let (_dir, t, mut repo) = write_locked(&s);
            repo.start_write_group().unwrap();
            repo.add_text(b"file-id", b"revid", &[], b"lines\n")
                .unwrap();
            repo.abort_write_group().unwrap();
            assert!(!repo.is_in_write_group(), "{}", s.label);
            repo.unlock().unwrap();
            assert!(
                has_text((s.reopen)(t).as_ref(), b"file-id", b"revid"),
                "{}",
                s.label
            );
        }
    }

    #[test]
    fn with_write_group_commits_on_success() {
        for s in scenarios() {
            let (_dir, t, mut repo) = write_locked(&s);
            let (value, hint) = with_write_group(repo.as_mut(), |repo| {
                assert!(repo.is_in_write_group());
                repo.add_text(b"file-id", b"revid", &[], b"lines\n")?;
                Ok(42)
            })
            .unwrap();
            assert_eq!(42, value, "{}", s.label);
            assert_eq!(suspends(&s), hint.is_some(), "{}", s.label);
            assert!(!repo.is_in_write_group(), "{}", s.label);
            repo.unlock().unwrap();
            assert!(
                has_text((s.reopen)(t).as_ref(), b"file-id", b"revid"),
                "{}",
                s.label
            );
        }
    }

    #[test]
    fn with_write_group_aborts_on_failure() {
        for s in scenarios() {
            let (_dir, t, mut repo) = write_locked(&s);
            let result = with_write_group(repo.as_mut(), |repo| -> Result<(), _> {
                repo.add_text(b"file-id", b"revid", &[], b"lines\n")?;
                Err(RepositoryError::Corrupt("failed".to_string()))
            });
            match result {
                Err(RepositoryError::Corrupt(msg)) => assert_eq!("failed", msg, "{}", s.label),
                other => panic!("{}: {other:?}", s.label),
            }
            assert!(!repo.is_in_write_group(), "{}", s.label);
            repo.unlock().unwrap();
            assert_eq!(
                !suspends(&s),
                has_text((s.reopen)(t).as_ref(), b"file-id", b"revid"),
                "{}",
                s.label
            );
        }
    }

    #[test]
    fn revision_text_inventory_signature_round_trip() {
        for s in scenarios() {
            let dir = tempfile::tempdir().unwrap();
            let t: SharedTransport = Arc::new(LocalTransport::new(dir.path()));
            let mut repo = (s.create)(t.clone());

            let mut repo = repo.write_locked().unwrap();
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
            repo.unlock().unwrap();

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

    /// Build a 2a repository with a single revision, file text and inventory.
    fn make_2a_with_rev(dir: &std::path::Path, rev: &[u8]) -> Box<dyn Repository> {
        let t: SharedTransport = Arc::new(LocalTransport::new(dir));
        let mut repo =
            Box::new(Pack2aRepository::create(t.clone()).unwrap()) as Box<dyn Repository>;
        let mut repo = repo.write_locked().unwrap();
        repo.start_write_group().unwrap();
        repo.add_revision(&revision(rev, vec![], "msg"), &[])
            .unwrap();
        repo.add_inventory_from_entries(rev, &[], crate::inventory::ROOT_ID, &entries(rev))
            .unwrap();
        repo.add_text(b"file-1", rev, &[], b"hello\n").unwrap();
        repo.commit_write_group().unwrap();
        repo.unlock().unwrap();
        Box::new(Pack2aRepository::open(t).unwrap())
    }

    /// A stacked repository resolves revisions, inventories and file texts that
    /// live only in a fallback, while its own all_revision_ids stays primary.
    #[test]
    fn stacked_repository_reads_through_fallback() {
        let base_dir = tempfile::tempdir().unwrap();
        let base = make_2a_with_rev(base_dir.path(), b"rev-base");

        // An empty primary repository.
        let top_dir = tempfile::tempdir().unwrap();
        let top_t: SharedTransport = Arc::new(LocalTransport::new(top_dir.path()));
        let primary = Box::new(Pack2aRepository::create(top_t).unwrap()) as Box<dyn Repository>;

        let mut stacked = StackedRepository::new(primary);
        assert!(!stacked.has_revision(b"rev-base").unwrap());
        stacked.add_fallback_repository(base).unwrap();

        // The fallback's revision is now visible through the stack.
        assert!(stacked.has_revision(b"rev-base").unwrap());
        assert_eq!(stacked.get_revision(b"rev-base").unwrap().message, "msg");
        assert_eq!(
            stacked.get_file_text(b"file-1", b"rev-base").unwrap(),
            b"hello\n"
        );
        let pm = stacked.get_parent_map(&[b"rev-base".to_vec()]).unwrap();
        assert_eq!(pm.get(&b"rev-base".to_vec()), Some(&vec![]));

        // all_revision_ids reflects only the (empty) primary, not the fallback.
        assert!(stacked.all_revision_ids().unwrap().is_empty());

        // A genuinely absent revision still errors.
        assert!(matches!(
            stacked.get_revision(b"rev-missing"),
            Err(RepositoryError::NoSuchRevision(_))
        ));
    }

    /// A plain backend reports that it does not support fallbacks.
    #[test]
    fn plain_repository_rejects_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let t: SharedTransport = Arc::new(LocalTransport::new(dir.path()));
        let mut repo = Pack2aRepository::create(t).unwrap();
        let other_dir = tempfile::tempdir().unwrap();
        let other = make_2a_with_rev(other_dir.path(), b"rev-x");
        assert!(matches!(
            repo.add_fallback_repository(other),
            Err(RepositoryError::UnsupportedFormat(_))
        ));
    }

    #[cfg(feature = "gpg")]
    fn gen_signing_cert() -> (sequoia_openpgp::Cert, Vec<u8>) {
        use sequoia_openpgp::cert::CertBuilder;
        use sequoia_openpgp::serialize::Serialize;
        let (cert, _) = CertBuilder::new().add_signing_subkey().generate().unwrap();
        let mut tsk = Vec::new();
        cert.as_tsk().serialize(&mut tsk).unwrap();
        (cert, tsk)
    }

    /// A revision signed over its own V1 testament verifies as valid, and an
    /// unsigned revision reports NotSigned.
    #[cfg(feature = "gpg")]
    #[test]
    fn verify_revision_signature_valid_and_unsigned() {
        use crate::gpg::VerificationResult;

        let dir = tempfile::tempdir().unwrap();
        let t: SharedTransport = Arc::new(LocalTransport::new(dir.path()));
        let repo = make_2a_with_rev_at(&t, b"rev-1");

        // No signature yet.
        assert_eq!(
            repo.verify_revision_signature(b"rev-1", &[]).unwrap(),
            VerificationResult::NotSigned
        );

        // Sign the revision's own testament short text and store it.
        let (cert, tsk) = gen_signing_cert();
        let testament = testament_short_text_for_revision(repo.as_ref(), b"rev-1").unwrap();
        let signature = crate::gpg::clearsign(&testament, &tsk).unwrap();
        let mut repo = repo;
        let mut repo = repo.write_locked().unwrap();
        repo.start_write_group().unwrap();
        repo.add_signature_text(b"rev-1", &signature).unwrap();
        repo.commit_write_group().unwrap();
        repo.unlock().unwrap();
        // Reopen so the freshly written signature pack is visible to reads.
        let repo = Box::new(Pack2aRepository::open(t.clone()).unwrap()) as Box<dyn Repository>;

        assert_eq!(
            repo.verify_revision_signature(b"rev-1", std::slice::from_ref(&cert))
                .unwrap(),
            VerificationResult::Valid
        );
    }

    /// A signature over the wrong content fails the testament byte-compare even
    /// though it is cryptographically good.
    #[cfg(feature = "gpg")]
    #[test]
    fn verify_revision_signature_wrong_testament_is_not_valid() {
        use crate::gpg::VerificationResult;

        let dir = tempfile::tempdir().unwrap();
        let t: SharedTransport = Arc::new(LocalTransport::new(dir.path()));
        let mut repo = make_2a_with_rev_at(&t, b"rev-1");

        let (cert, tsk) = gen_signing_cert();
        // Sign something that is NOT the revision's testament.
        let signature = crate::gpg::clearsign(b"not the testament\n", &tsk).unwrap();
        let mut repo = repo.write_locked().unwrap();
        repo.start_write_group().unwrap();
        repo.add_signature_text(b"rev-1", &signature).unwrap();
        repo.commit_write_group().unwrap();
        repo.unlock().unwrap();
        let repo = Box::new(Pack2aRepository::open(t.clone()).unwrap()) as Box<dyn Repository>;

        assert_eq!(
            repo.verify_revision_signature(b"rev-1", std::slice::from_ref(&cert))
                .unwrap(),
            VerificationResult::NotValid
        );
    }

    /// Build a 2a repository with a single committed revision at `t`, returned
    /// open for read/write (so a signature can be added).
    #[cfg(feature = "gpg")]
    fn make_2a_with_rev_at(t: &SharedTransport, rev: &[u8]) -> Box<dyn Repository> {
        let mut repo =
            Box::new(Pack2aRepository::create(t.clone()).unwrap()) as Box<dyn Repository>;
        let mut repo = repo.write_locked().unwrap();
        repo.start_write_group().unwrap();
        repo.add_revision(&revision(rev, vec![], "msg"), &[])
            .unwrap();
        repo.add_inventory_from_entries(rev, &[], crate::inventory::ROOT_ID, &entries(rev))
            .unwrap();
        repo.add_text(b"file-1", rev, &[], b"hello\n").unwrap();
        repo.commit_write_group().unwrap();
        repo.unlock().unwrap();
        Box::new(Pack2aRepository::open(t.clone()).unwrap())
    }
}

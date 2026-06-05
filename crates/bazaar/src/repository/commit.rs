//! Building a commit incrementally.
//!
//! A [`CommitBuilder`] records a new revision as a delta against a basis,
//! mirroring breezy's `CommitBuilder`: feed it the changes from a working
//! tree ([`record_iter_changes`](CommitBuilder::record_iter_changes)),
//! [`finish_inventory`](CommitBuilder::finish_inventory) to build the new
//! inventory by applying that delta to the basis, then
//! [`commit`](CommitBuilder::commit) to write the revision.
//!
//! Only changed or new entries get a new per-file text and are recorded at
//! the new revision; unchanged entries are carried over at their basis
//! revision (the inventory delta simply omits them). This keeps both the
//! texts written and the CHK pages rewritten proportional to the change,
//! not the tree size.

use std::collections::HashMap;

use crate::inventory::Entry;
use crate::inventory_delta::{InventoryDelta, InventoryDeltaEntry};
use crate::repository::{Repository, RepositoryError};
use crate::workingtree::{EntryKind, WorkingTreeChange};
use crate::{FileId, RevisionId};

/// Accumulates a single-parent commit against a basis revision.
///
/// Construct via [`Repository::get_commit_builder`]; the repository must
/// already have an open write group.
pub struct CommitBuilder<'a> {
    repository: &'a mut dyn Repository,
    parents: Vec<Vec<u8>>,
    new_revision_id: Vec<u8>,
    committer: String,
    timestamp: u64,
    timezone: i32,
    /// Revision properties to record on the revision.
    properties: HashMap<String, Vec<u8>>,
    /// The inventory delta recorded so far.
    delta: Vec<InventoryDeltaEntry>,
    /// The new inventory's sha1, set by `finish_inventory`.
    inventory_sha1: Option<Vec<u8>>,
}

impl<'a> CommitBuilder<'a> {
    pub(super) fn new(
        repository: &'a mut dyn Repository,
        parents: Vec<Vec<u8>>,
        new_revision_id: Vec<u8>,
        committer: String,
        timestamp: u64,
        timezone: i32,
    ) -> Self {
        CommitBuilder {
            repository,
            parents,
            new_revision_id,
            committer,
            timestamp,
            timezone,
            properties: HashMap::new(),
            delta: Vec::new(),
            inventory_sha1: None,
        }
    }

    /// Set the revision properties recorded on the commit.
    pub fn with_properties(mut self, properties: HashMap<String, Vec<u8>>) -> Self {
        self.properties = properties;
        self
    }

    /// Whether the recorded delta represents a real change.
    ///
    /// Mirrors breezy's `_any_changes`: a delta against a non-null basis is
    /// a change if it touches anything; against the null revision (a first
    /// commit) the root entry alone does not count, so more than one delta
    /// entry is required. A commit with pending merges is always considered
    /// a change.
    pub fn any_changes(&self) -> bool {
        if self.parents.len() > 1 {
            return true;
        }
        let basis_is_null = self.basis_revision_id() == crate::branch::NULL_REVISION;
        if basis_is_null {
            self.delta.len() > 1
        } else {
            !self.delta.is_empty()
        }
    }

    /// The basis revision the delta is recorded against (the first parent,
    /// or the null revision for a first commit).
    fn basis_revision_id(&self) -> Vec<u8> {
        self.parents
            .first()
            .cloned()
            .unwrap_or_else(|| crate::branch::NULL_REVISION.to_vec())
    }

    /// Record the working-tree changes against `basis_revision_id`, writing
    /// a new text for each content change and building the inventory delta.
    ///
    /// `get_file_text` reads a working-tree file's current bytes by path; it
    /// is only called for content-changed or newly-added files.
    pub fn record_iter_changes<F>(
        &mut self,
        changes: &[WorkingTreeChange],
        mut get_file_text: F,
    ) -> Result<(), RepositoryError>
    where
        F: FnMut(&str) -> Result<Vec<u8>, RepositoryError>,
    {
        let new_rev = RevisionId::from(self.new_revision_id.as_slice());
        for change in changes {
            match (&change.old_path, &change.new_path) {
                // Removed: delete from the inventory, no text.
                (old @ Some(_), None) => {
                    self.delta.push(InventoryDeltaEntry {
                        old_path: old.clone(),
                        new_path: None,
                        file_id: FileId::from(change.file_id.as_slice()),
                        new_entry: None,
                    });
                }
                // Added, moved, or modified: build the new entry. A content
                // change (or a new entry) records a new text and the new
                // revision; a pure move/metadata change carries the basis
                // revision over.
                (_, Some(new_path)) => {
                    let kind = change.new_kind.ok_or_else(|| {
                        RepositoryError::Corrupt(format!(
                            "change for {new_path} has a new path but no kind"
                        ))
                    })?;
                    let carried_over = !change.content_change && change.basis_revision.is_some();
                    let entry_revision = if carried_over {
                        RevisionId::from(change.basis_revision.as_deref().unwrap())
                    } else {
                        new_rev.clone()
                    };
                    let entry =
                        self.build_entry(change, kind, &entry_revision, &mut get_file_text)?;
                    self.delta.push(InventoryDeltaEntry {
                        old_path: change.old_path.clone(),
                        new_path: Some(new_path.clone()),
                        file_id: FileId::from(change.file_id.as_slice()),
                        new_entry: Some(entry),
                    });
                }
                // No path on either side: nothing to record.
                (None, None) => {}
            }
        }
        Ok(())
    }

    /// Build the new inventory entry for a change, writing its text to the
    /// repository when the content changed (or the entry is new).
    fn build_entry<F>(
        &mut self,
        change: &WorkingTreeChange,
        kind: EntryKind,
        revision: &RevisionId,
        get_file_text: &mut F,
    ) -> Result<Entry, RepositoryError>
    where
        F: FnMut(&str) -> Result<Vec<u8>, RepositoryError>,
    {
        let file_id = FileId::from(change.file_id.as_slice());
        let name = change.new_name.clone().unwrap_or_default();
        let new_path = change.new_path.as_deref().unwrap_or("");
        let writes_text = change.content_change || change.basis_revision.is_none();

        // The tree root (empty path, no parent): record its empty text and a
        // root inventory entry.
        if new_path.is_empty() && change.new_parent_id.is_none() {
            if writes_text {
                self.repository
                    .add_text(&change.file_id, &self.new_revision_id, &[], b"")?;
            }
            return Ok(Entry::root(file_id, Some(revision.clone())));
        }

        let parent_id = FileId::from(
            change
                .new_parent_id
                .as_deref()
                .unwrap_or(crate::inventory::ROOT_ID),
        );

        // The per-file text parents, as (file_id, parent_revision) keys.
        let text_parents: Vec<(Vec<u8>, Vec<u8>)> = change
            .text_parents
            .iter()
            .map(|rev| (change.file_id.clone(), rev.clone()))
            .collect();

        match kind {
            EntryKind::File => {
                let content = get_file_text(new_path)?;
                let sha1 = crate::weave::sha_strings(&[content.as_slice()]);
                let size = content.len() as u64;
                if writes_text {
                    self.repository.add_text(
                        &change.file_id,
                        &self.new_revision_id,
                        &text_parents,
                        &content,
                    )?;
                }
                Ok(Entry::file(
                    file_id,
                    name,
                    parent_id,
                    Some(revision.clone()),
                    Some(sha1),
                    Some(size),
                    Some(change.new_executable),
                    None,
                ))
            }
            EntryKind::Directory => {
                if writes_text {
                    self.repository.add_text(
                        &change.file_id,
                        &self.new_revision_id,
                        &text_parents,
                        b"",
                    )?;
                }
                Ok(Entry::directory(
                    file_id,
                    name,
                    parent_id,
                    Some(revision.clone()),
                ))
            }
            EntryKind::Symlink => {
                if writes_text {
                    self.repository.add_text(
                        &change.file_id,
                        &self.new_revision_id,
                        &text_parents,
                        b"",
                    )?;
                }
                // The symlink target is read from the working tree's file
                // content path; record it on the entry.
                let target = String::from_utf8_lossy(&get_file_text(new_path)?).into_owned();
                Ok(Entry::link(
                    file_id,
                    name,
                    parent_id,
                    Some(revision.clone()),
                    Some(target),
                ))
            }
            EntryKind::TreeReference => Err(RepositoryError::Corrupt(
                "tree references are not supported in commit".to_string(),
            )),
        }
    }

    /// Build the new inventory by applying the recorded delta to the basis,
    /// and record its sha1. Returns the inventory sha1.
    pub fn finish_inventory(&mut self) -> Result<Vec<u8>, RepositoryError> {
        let delta = InventoryDelta(std::mem::take(&mut self.delta));
        let basis = self.basis_revision_id();
        let sha1 = self.repository.add_inventory_by_delta(
            &basis,
            &delta,
            &self.new_revision_id,
            &self.parents,
        )?;
        self.inventory_sha1 = Some(sha1.clone());
        Ok(sha1)
    }

    /// Write the revision record and return its id. `finish_inventory` must
    /// have been called first.
    pub fn commit(&mut self, message: &str) -> Result<Vec<u8>, RepositoryError> {
        let inventory_sha1 = self.inventory_sha1.clone().ok_or_else(|| {
            RepositoryError::Corrupt("commit() called before finish_inventory()".to_string())
        })?;
        let revision = crate::revision::Revision::new(
            RevisionId::from(self.new_revision_id.as_slice()),
            self.parents
                .iter()
                .map(|p| RevisionId::from(p.as_slice()))
                .collect(),
            Some(self.committer.clone()),
            message.to_string(),
            self.properties.clone(),
            Some(inventory_sha1),
            self.timestamp as f64,
            Some(self.timezone),
        );
        self.repository.add_revision(&revision, &self.parents)?;
        Ok(self.new_revision_id.clone())
    }
}

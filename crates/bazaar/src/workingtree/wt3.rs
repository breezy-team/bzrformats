//! The pre-dirstate working tree (format 3).
//!
//! Format 3 is the working-tree layout used by the weave and non-pack knit
//! eras: the tracked set lives in an XML (v5) working inventory rather than a
//! dirstate, with the basis revision and merge parents in separate files. The
//! exact on-disk paths come from a [`Wt3Layout`] -- the knit format keeps them
//! under `.bzr/checkout/`, the weave all-in-one format directly under `.bzr/`.
//!
//! Built only with the `weave` or `knit` feature (see the gate on this module
//! in the parent).

use crate::transport::{SharedTransport, TransportError};

use super::{
    basename, build_committed_entries, change_selected, compute_changes, sign_commit,
    CommitOptions, EntryKind, LiveEntries, LiveEntry, VersionedEntry, WorkingTree,
    WorkingTreeChange, WorkingTreeError,
};

/// How a pre-dirstate working tree records its basis revision.
#[derive(Clone, Copy)]
enum Wt3Basis {
    /// A dedicated `last-revision` file holding the basis revid (knit format
    /// 3); the path is the file. Cleared by writing an empty file.
    LastRevisionFile(&'static str),
    /// The branch's `revision-history` file, whose last line is the basis
    /// (the weave all-in-one layout). The working tree does not write it --
    /// the branch advances it -- so the tree only reads it.
    RevisionHistory(&'static str),
}

/// The on-disk paths of a pre-dirstate working tree. The knit format-3 tree
/// keeps its files under `.bzr/checkout/`; the weave all-in-one tree keeps
/// them directly under `.bzr/`.
#[derive(Clone, Copy)]
struct Wt3Layout {
    inventory: &'static str,
    pending_merges: &'static str,
    basis: Wt3Basis,
}

/// The knit format-3 layout: files under `.bzr/checkout/`, basis in a
/// dedicated `last-revision` file.
const WT3_CHECKOUT_LAYOUT: Wt3Layout = Wt3Layout {
    inventory: ".bzr/checkout/inventory",
    pending_merges: ".bzr/checkout/pending-merges",
    basis: Wt3Basis::LastRevisionFile(".bzr/checkout/last-revision"),
};

/// The weave all-in-one layout: files directly under `.bzr/`, basis taken
/// from the branch's `.bzr/revision-history`.
#[cfg(feature = "weave")]
const WT3_ALL_IN_ONE_LAYOUT: Wt3Layout = Wt3Layout {
    inventory: ".bzr/inventory",
    pending_merges: ".bzr/pending-merges",
    basis: Wt3Basis::RevisionHistory(".bzr/revision-history"),
};

/// The pre-dirstate working tree, accessed through a transport rooted at the
/// tree root (the directory containing `.bzr`).
///
/// Unlike the dirstate tree, the tracked set lives in an XML (v5) working
/// inventory (whose entries carry no revision, matching the working state),
/// the basis is a single revision id, and extra merge parents are lines of a
/// pending-merges file. On-disk file content is read from the tree itself.
/// The exact paths and basis storage come from the [`Wt3Layout`]: knit format
/// 3 keeps them under `.bzr/checkout/`, the weave all-in-one format under
/// `.bzr/` with the basis in the branch's `revision-history`.
///
/// TODO: the `basis-inventory-cache` and `stat-cache` files brz also keeps
/// are not read or maintained here; they are an optimization, not required
/// for correctness.
pub struct WorkingTree3 {
    transport: SharedTransport,
    inventory: crate::inventory::MutableInventory,
    layout: Wt3Layout,
}

impl WorkingTree3 {
    /// Open the knit format-3 working tree reachable through `transport`
    /// (rooted at the directory that contains `.bzr`), parsing its working
    /// inventory.
    pub fn open(transport: SharedTransport) -> Result<Self, WorkingTreeError> {
        Self::open_with_layout(transport, WT3_CHECKOUT_LAYOUT)
    }

    /// Open the weave all-in-one working tree, whose files live directly under
    /// `.bzr` and whose basis is the branch's `revision-history`.
    #[cfg(feature = "weave")]
    pub fn open_all_in_one(transport: SharedTransport) -> Result<Self, WorkingTreeError> {
        Self::open_with_layout(transport, WT3_ALL_IN_ONE_LAYOUT)
    }

    fn open_with_layout(
        transport: SharedTransport,
        layout: Wt3Layout,
    ) -> Result<Self, WorkingTreeError> {
        let inventory = Self::read_inventory(&transport, &layout)?;
        Ok(WorkingTree3 {
            transport,
            inventory,
            layout,
        })
    }

    fn read_inventory(
        transport: &SharedTransport,
        layout: &Wt3Layout,
    ) -> Result<crate::inventory::MutableInventory, WorkingTreeError> {
        use crate::serializer::InventorySerializer;
        let bytes = match transport.get_bytes(layout.inventory) {
            Ok(b) => b,
            Err(TransportError::NoSuchFile(_)) => {
                // No working inventory yet: an empty tree with just the root.
                let mut inv = crate::inventory::MutableInventory::new();
                inv.add(crate::inventory::Entry::root(
                    crate::FileId::from(crate::inventory::ROOT_ID),
                    None,
                ))
                .map_err(|e| WorkingTreeError::Commit(format!("init inventory: {e:?}")))?;
                return Ok(inv);
            }
            Err(e) => return Err(e.into()),
        };
        // read_inventory_from_lines concatenates its inputs before parsing,
        // so the whole file can be passed as a single chunk.
        crate::xml_serializer::XMLInventorySerializer5
            .read_inventory_from_lines(&[bytes.as_slice()], None)
            .map_err(|e| WorkingTreeError::Commit(format!("parse working inventory: {e:?}")))
    }

    /// Persist the working inventory to `.bzr/checkout/inventory` in the
    /// revision-less working form brz writes.
    fn save_inventory(&self) -> Result<(), WorkingTreeError> {
        use crate::serializer::InventorySerializer;
        let lines = crate::xml_serializer::XMLInventorySerializer5
            .write_inventory_to_lines(&self.inventory, true)
            .map_err(|e| WorkingTreeError::Commit(format!("serialise inventory: {e:?}")))?;
        let mut content = Vec::new();
        for line in lines {
            content.extend_from_slice(&line);
        }
        self.transport
            .put_bytes(self.layout.inventory, &content, None)?;
        Ok(())
    }

    /// The file id of the directory containing `path`, for re-parenting an
    /// added or moved entry. Returns the tree root id for a top-level path.
    fn parent_id_for(&self, path: &str) -> Result<Vec<u8>, WorkingTreeError> {
        match path.rsplit_once('/') {
            None => Ok(self.root_id()),
            Some((dir, _)) => self
                .inventory
                .path2id(dir)
                .map(|id| id.as_bytes().to_vec())
                .ok_or_else(|| WorkingTreeError::NotVersioned(dir.to_string())),
        }
    }

    fn root_id(&self) -> Vec<u8> {
        self.inventory
            .root()
            .map(|r| r.file_id().as_bytes().to_vec())
            .unwrap_or_else(|| crate::inventory::ROOT_ID.to_vec())
    }
}

impl WorkingTree for WorkingTree3 {
    fn basis_revision(&self) -> Option<Vec<u8>> {
        let bytes = match self.layout.basis {
            Wt3Basis::LastRevisionFile(path) => self.transport.get_bytes(path).ok()?,
            Wt3Basis::RevisionHistory(path) => {
                // The basis is the last line of revision-history.
                let history = self.transport.get_bytes(path).ok()?;
                history
                    .rsplit(|&b| b == b'\n')
                    .find(|l| !l.is_empty())
                    .map(|l| l.to_vec())?
            }
        };
        if !bytes.is_empty() && bytes != crate::branch::NULL_REVISION {
            Some(bytes)
        } else {
            None
        }
    }

    fn parent_ids(&self) -> Vec<Vec<u8>> {
        let mut parents = Vec::new();
        if let Some(basis) = self.basis_revision() {
            parents.push(basis);
        }
        if let Ok(bytes) = self.transport.get_bytes(self.layout.pending_merges) {
            for line in bytes.split(|&b| b == b'\n') {
                if !line.is_empty() && line != crate::branch::NULL_REVISION {
                    parents.push(line.to_vec());
                }
            }
        }
        parents
    }

    fn add_pending_merge(&mut self, revision_id: &[u8]) -> Result<(), WorkingTreeError> {
        let mut existing = match self.transport.get_bytes(self.layout.pending_merges) {
            Ok(b) => b,
            Err(TransportError::NoSuchFile(_)) => Vec::new(),
            Err(e) => return Err(e.into()),
        };
        let already = existing
            .split(|&b| b == b'\n')
            .any(|line| line == revision_id);
        if already {
            return Ok(());
        }
        if !existing.is_empty() && !existing.ends_with(b"\n") {
            existing.push(b'\n');
        }
        existing.extend_from_slice(revision_id);
        existing.push(b'\n');
        self.transport
            .put_bytes(self.layout.pending_merges, &existing, None)?;
        Ok(())
    }

    fn list_files(&self) -> Vec<VersionedEntry> {
        self.inventory
            .entries()
            .into_iter()
            .filter_map(|(path, entry)| {
                EntryKind::from_inventory_kind(entry.kind()).map(|kind| VersionedEntry {
                    path,
                    file_id: entry.file_id().as_bytes().to_vec(),
                    kind,
                })
            })
            .collect()
    }

    fn path2id(&self, path: &str) -> Option<Vec<u8>> {
        let path = path.trim_matches('/');
        if path.is_empty() {
            return Some(self.root_id());
        }
        self.inventory
            .path2id(path)
            .map(|id| id.as_bytes().to_vec())
    }

    fn get_file_text(&self, path: &str) -> Result<Vec<u8>, WorkingTreeError> {
        Ok(self.transport.get_bytes(path)?)
    }

    fn unknowns(&self) -> Result<Vec<String>, WorkingTreeError> {
        let versioned: std::collections::HashSet<String> = self
            .inventory
            .entries()
            .into_iter()
            .map(|(p, _)| p)
            .collect();
        let mut unknowns = Vec::new();
        for rel in self.transport.iter_files_recursive()? {
            if rel.starts_with(".bzr/") || rel == ".bzr" {
                continue;
            }
            if !versioned.contains(&rel) {
                unknowns.push(rel);
            }
        }
        unknowns.sort();
        Ok(unknowns)
    }

    fn iter_changes(
        &self,
        basis: &crate::repository::RevisionTree,
    ) -> Result<Vec<WorkingTreeChange>, WorkingTreeError> {
        self.iter_changes_with_parents(basis, &[])
    }

    fn iter_changes_with_parents(
        &self,
        basis: &crate::repository::RevisionTree,
        other_parents: &[crate::repository::RevisionTree],
    ) -> Result<Vec<WorkingTreeChange>, WorkingTreeError> {
        compute_changes(
            &self.transport,
            &self.collect_live_entries(),
            basis,
            other_parents,
        )
    }

    fn add(
        &mut self,
        path: &str,
        kind: EntryKind,
        file_id: Option<&[u8]>,
    ) -> Result<Vec<u8>, WorkingTreeError> {
        let path = path.trim_matches('/');
        if let Some(existing) = self.path2id(path) {
            return Ok(existing);
        }
        let file_id = match file_id {
            Some(id) => id.to_vec(),
            None => crate::gen_ids::gen_file_id(path),
        };
        let parent_id = self.parent_id_for(path)?;
        let name = basename(path).to_string();
        let fid = crate::FileId::from(file_id.as_slice());
        let pid = crate::FileId::from(parent_id.as_slice());
        let entry = match kind {
            EntryKind::File => {
                crate::inventory::Entry::file(fid, name, pid, None, None, None, None, None)
            }
            EntryKind::Directory => crate::inventory::Entry::directory(fid, name, pid, None),
            EntryKind::Symlink => crate::inventory::Entry::link(fid, name, pid, None, None),
            EntryKind::TreeReference => {
                crate::inventory::Entry::tree_reference(fid, name, pid, None, None)
            }
        };
        self.inventory
            .add(entry)
            .map_err(|e| WorkingTreeError::Commit(format!("add to inventory: {e:?}")))?;
        self.save_inventory()?;
        Ok(file_id)
    }

    fn remove(&mut self, path: &str) -> Result<(), WorkingTreeError> {
        let path = path.trim_matches('/');
        let file_id = self
            .path2id(path)
            .ok_or_else(|| WorkingTreeError::NotVersioned(path.to_string()))?;
        // delete() removes the entry and its descendants from the inventory.
        self.inventory
            .delete(&crate::FileId::from(file_id.as_slice()))
            .map_err(|e| WorkingTreeError::Commit(format!("remove from inventory: {e:?}")))?;
        self.save_inventory()
    }

    fn rename(&mut self, from_path: &str, to_path: &str) -> Result<(), WorkingTreeError> {
        let from_path = from_path.trim_matches('/');
        let to_path = to_path.trim_matches('/');
        let file_id = self
            .path2id(from_path)
            .ok_or_else(|| WorkingTreeError::NotVersioned(from_path.to_string()))?;
        if self.path2id(to_path).is_some() {
            return Err(WorkingTreeError::Commit(format!(
                "destination already versioned: {to_path}"
            )));
        }
        let new_parent = self.parent_id_for(to_path)?;
        let new_name = basename(to_path).to_string();
        self.inventory
            .rename(
                &crate::FileId::from(file_id.as_slice()),
                &crate::FileId::from(new_parent.as_slice()),
                &new_name,
            )
            .map_err(|e| WorkingTreeError::Commit(format!("rename in inventory: {e:?}")))?;
        // Move the file on disk to match the dirstate backend's behaviour.
        if self.transport.has(from_path)? {
            self.transport.rename(from_path, to_path)?;
        }
        self.save_inventory()
    }

    fn commit(
        &mut self,
        repository: &mut dyn crate::repository::Repository,
        branch: &crate::branch::Branch,
        options: &CommitOptions,
    ) -> Result<Vec<u8>, WorkingTreeError> {
        if options.strict {
            let unknowns = self.unknowns()?;
            if !unknowns.is_empty() {
                return Err(WorkingTreeError::StrictCommitFailed(unknowns));
            }
        }

        let parents = self.parent_ids();
        let revid = match &options.revision_id {
            Some(id) => id.clone(),
            None => crate::RevisionId::generate(&options.committer, Some(options.timestamp))
                .as_bytes()
                .to_vec(),
        };
        let properties = options.build_properties()?;
        let basis_revision_id = parents
            .first()
            .cloned()
            .unwrap_or_else(|| crate::branch::NULL_REVISION.to_vec());

        let selective = !options.specific_files.is_empty() || !options.exclude.is_empty();
        if selective && parents.len() > 1 {
            return Err(WorkingTreeError::CannotCommitSelectedFileMerge);
        }

        let basis = repository
            .revision_tree(&basis_revision_id)
            .map_err(WorkingTreeError::Repository)?;
        let other_parents: Vec<crate::repository::RevisionTree> = parents
            .iter()
            .skip(1)
            .map(|p| repository.revision_tree(p))
            .collect::<Result<_, _>>()
            .map_err(WorkingTreeError::Repository)?;
        let live = self.collect_live_entries();
        let mut changes = compute_changes(&self.transport, &live, &basis, &other_parents)?;
        if selective {
            changes.retain(|c| change_selected(c, &options.specific_files, &options.exclude));
        }

        if !options.allow_pointless && parents.len() <= 1 {
            let basis_is_null = basis_revision_id == crate::branch::NULL_REVISION;
            let pointless = if basis_is_null {
                changes.len() <= 1
            } else {
                changes.is_empty()
            };
            if pointless {
                return Err(WorkingTreeError::PointlessCommit);
            }
        }

        repository
            .start_write_group()
            .map_err(WorkingTreeError::Repository)?;
        {
            let mut builder = repository
                .get_commit_builder(
                    parents.clone(),
                    revid.clone(),
                    options.committer.clone(),
                    options.timestamp,
                    options.timezone,
                )
                .with_properties(properties.clone());
            builder
                .record_iter_changes(&changes, |path| {
                    self.transport
                        .get_bytes(path)
                        .map_err(crate::repository::RepositoryError::Transport)
                })
                .map_err(WorkingTreeError::Repository)?;
            builder
                .finish_inventory()
                .map_err(WorkingTreeError::Repository)?;
            builder
                .commit(&options.message)
                .map_err(WorkingTreeError::Repository)?;
        }

        if let Some(key) = &options.signing_key {
            let (paths, inv_entries) =
                build_committed_entries(&self.transport, &live, &revid, &basis, &changes)?;
            let signature = sign_commit(
                &parents,
                &revid,
                options,
                &properties,
                &paths,
                &inv_entries,
                key,
            )?;
            repository
                .add_signature_text(&revid, &signature)
                .map_err(WorkingTreeError::Repository)?;
        }

        repository
            .commit_write_group()
            .map_err(WorkingTreeError::Repository)?;

        // Unversion files committed as deletions (they vanished from disk).
        let deleted_paths: Vec<String> = changes
            .iter()
            .filter(|c| c.new_path.is_none())
            .filter_map(|c| c.old_path.clone())
            .filter(|p| self.path2id(p).is_some())
            .collect();
        for path in &deleted_paths {
            self.remove(path)?;
        }

        // Advance the branch tip. The new revno is one past the branch's
        // current tip (format 5's full history determines it).
        let new_revno = branch
            .last_revision_info()
            .map_err(WorkingTreeError::Branch)?
            .0
            + 1;
        branch
            .set_last_revision_info(new_revno, &revid)
            .map_err(WorkingTreeError::Branch)?;

        // Update the basis: the new revision becomes the basis and the only
        // parent, so pending-merges is cleared. The working inventory stays
        // revision-less (it now equals the basis). For the checkout layout the
        // basis lives in a dedicated last-revision file; for the all-in-one
        // layout it is the branch's revision-history, which the branch already
        // advanced above, so nothing more to write there.
        //
        // TODO: basis-inventory-cache is not written; it is an optimization
        // brz keeps but is not required for correctness.
        if let Wt3Basis::LastRevisionFile(path) = self.layout.basis {
            self.transport.put_bytes(path, &revid, None)?;
        }
        self.transport
            .put_bytes(self.layout.pending_merges, b"", None)?;

        Ok(revid)
    }
}

impl WorkingTree3 {
    /// Collect the live tree entries from the working inventory, pairing each
    /// with its on-disk symlink target (read lazily) for the diff.
    fn collect_live_entries(&self) -> LiveEntries {
        let root_id = self.root_id();
        let mut entries = Vec::new();
        for (path, entry) in self.inventory.entries() {
            let kind = match EntryKind::from_inventory_kind(entry.kind()) {
                Some(k) => k,
                None => continue,
            };
            let symlink_target = if kind == EntryKind::Symlink {
                entry
                    .symlink_target()
                    .map(|t| t.as_bytes().to_vec())
                    .unwrap_or_default()
            } else {
                Vec::new()
            };
            entries.push(LiveEntry {
                path,
                file_id: entry.file_id().as_bytes().to_vec(),
                kind,
                executable: entry.executable(),
                symlink_target,
            });
        }
        LiveEntries { root_id, entries }
    }
}

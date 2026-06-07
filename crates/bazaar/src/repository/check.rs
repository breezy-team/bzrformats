//! Repository integrity checking.
//!
//! [`check`] walks every revision in a repository and cross-checks the data
//! the format stores: that each revision is present and self-consistent (its
//! recorded id matches the id it is stored under), that its parents exist
//! (otherwise they are ghosts), that its inventory is present and readable, and
//! that every file text the inventory references is present and hashes to the
//! sha1 the inventory records. It works on the abstract [`Repository`] trait,
//! so it checks any format.
//!
//! (Comparing a revision's recorded `inventory_sha1` against the serialised
//! inventory is not done here: the serialised form is format-specific and not
//! exposed on the trait. The inventory is still verified present and readable.)
//!
//! Like breezy's `check`, problems are collected into a [`CheckResult`] report
//! rather than raised: a single call surfaces every inconsistency found, and a
//! clean repository yields an empty problem list.

use std::collections::HashSet;

use super::{Repository, RepositoryError};

/// The outcome of [`check`]: counts of what was examined and a list of the
/// inconsistencies found. `is_clean` is true when no problems were reported.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CheckResult {
    /// Number of revisions examined.
    pub checked_revisions: usize,
    /// Number of file texts examined (content verified against the recorded
    /// sha1).
    pub checked_texts: usize,
    /// Parent revision ids referenced but not present in the repository.
    /// Ghosts are recorded but are not themselves problems (a repository may
    /// legitimately reference revisions it does not hold).
    pub ghosts: Vec<Vec<u8>>,
    /// Human-readable descriptions of the inconsistencies found.
    pub problems: Vec<String>,
}

impl CheckResult {
    /// Whether the check found no problems.
    pub fn is_clean(&self) -> bool {
        self.problems.is_empty()
    }
}

/// Check the integrity of `repo`, returning a report of any inconsistencies.
///
/// This never fails on a *data* inconsistency (those go in the report); it only
/// returns `Err` if the repository cannot be read at all (an I/O or decode
/// error outside the checked data, e.g. listing revisions).
pub fn check(repo: &(impl Repository + ?Sized)) -> Result<CheckResult, RepositoryError> {
    let mut result = CheckResult::default();
    let revision_ids = repo.all_revision_ids()?;
    let present: HashSet<Vec<u8>> = revision_ids.iter().cloned().collect();
    let mut ghosts: HashSet<Vec<u8>> = HashSet::new();

    for rev_id in &revision_ids {
        check_one_revision(repo, rev_id, &present, &mut ghosts, &mut result);
        result.checked_revisions += 1;
    }

    result.ghosts = ghosts.into_iter().collect();
    result.ghosts.sort();
    Ok(result)
}

/// Cross-check one revision and the data it references, appending any problems
/// to `result` and any ghost parents to `ghosts`.
fn check_one_revision(
    repo: &(impl Repository + ?Sized),
    rev_id: &[u8],
    present: &HashSet<Vec<u8>>,
    ghosts: &mut HashSet<Vec<u8>>,
    result: &mut CheckResult,
) {
    let revision = match repo.get_revision(rev_id) {
        Ok(r) => r,
        Err(e) => {
            result
                .problems
                .push(format!("revision {} could not be read: {e}", lossy(rev_id)));
            return;
        }
    };

    // The revision's own id must match the id it is stored under.
    if revision.revision_id.as_bytes() != rev_id {
        result.problems.push(format!(
            "revision {} records a different internal revision-id {}",
            lossy(rev_id),
            lossy(revision.revision_id.as_bytes())
        ));
    }

    // Parents not present in the repository are ghosts.
    for parent in &revision.parent_ids {
        let p = parent.as_bytes();
        if p != crate::branch::NULL_REVISION && !present.contains(p) {
            ghosts.insert(p.to_vec());
        }
    }

    // The inventory must be present, and (when recorded) its sha1 must match.
    let tree = match repo.revision_tree(rev_id) {
        Ok(t) => t,
        Err(e) => {
            result.problems.push(format!(
                "inventory for revision {} could not be read: {e}",
                lossy(rev_id)
            ));
            return;
        }
    };

    // Every entry the inventory introduces at this revision must have a present
    // text, and a file's text sha1 must match the entry's recorded sha1.
    let root = tree.inventory().root_entry().ok().flatten();
    let entries = tree.iter_entries();
    for entry in root.iter().chain(entries.iter().map(|(_, e)| e)) {
        let introduced = entry
            .revision()
            .map(|r| r.as_bytes() == rev_id)
            .unwrap_or(false);
        if !introduced {
            continue;
        }
        if entry.kind() == crate::osutils::Kind::File {
            result.checked_texts += 1;
        }
        check_entry_text(repo, rev_id, entry, result);
    }
}

/// Verify a file entry's text is present and its content sha1 matches the
/// entry's recorded `text_sha1`.
///
/// Only files carry a separate fulltext record that must be present: in the
/// CHK/groupcompress formats a directory's structure lives in the inventory
/// itself rather than a per-entry text, so a missing text record for a
/// non-file is not an inconsistency. A file's content is the integrity-bearing
/// data, so it must be present and hash to the recorded sha1.
fn check_entry_text(
    repo: &(impl Repository + ?Sized),
    rev_id: &[u8],
    entry: &crate::inventory::Entry,
    result: &mut CheckResult,
) {
    use crate::osutils::Kind;
    if entry.kind() != Kind::File {
        return;
    }
    let file_id = entry.file_id().as_bytes();
    match repo.get_file_text(file_id, rev_id) {
        Ok(text) => {
            if let Some(expected) = entry.text_sha1() {
                let actual = crate::weave::sha_strings(&[text.as_slice()]);
                if actual != expected {
                    result.problems.push(format!(
                        "text sha1 mismatch for file {} in revision {}: \
                         inventory records {}, content is {}",
                        lossy(file_id),
                        lossy(rev_id),
                        lossy(expected),
                        lossy(&actual)
                    ));
                }
            }
        }
        Err(e) => result.problems.push(format!(
            "missing text for file {} in revision {}: {e}",
            lossy(file_id),
            lossy(rev_id)
        )),
    }
}

/// Render bytes for a problem message: utf-8 if possible, else a lossy form.
fn lossy(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::Pack2aRepository;
    use crate::transport::{LocalTransport, SharedTransport};
    use std::sync::Arc;

    fn temp_repo() -> (tempfile::TempDir, SharedTransport) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("repository");
        std::fs::create_dir_all(&path).unwrap();
        (dir, Arc::new(LocalTransport::new(&path)))
    }

    fn revision(id: &[u8], parents: Vec<&[u8]>) -> crate::revision::Revision {
        crate::revision::Revision::new(
            crate::RevisionId::from(id),
            parents.into_iter().map(crate::RevisionId::from).collect(),
            Some("T <t@e>".to_string()),
            "m".to_string(),
            std::collections::HashMap::new(),
            None,
            1577880000.0,
            Some(0),
        )
    }

    /// Commit one revision with a file, returning the repo open for reading.
    fn make_one(t: &SharedTransport, rev: &[u8], text: &[u8]) {
        let mut repo = Pack2aRepository::create(t.clone()).unwrap();
        let root = crate::inventory::ROOT_ID;
        repo.start_write_group().unwrap();
        repo.add_text(b"file-1", rev, &[], text).unwrap();
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
                Some(crate::weave::sha_strings(&[text])),
                Some(text.len() as u64),
                Some(false),
                None,
            ),
        ];
        repo.add_inventory_from_entries(rev, &[], root, &entries)
            .unwrap();
        repo.add_revision(&revision(rev, vec![]), &[]).unwrap();
        repo.commit_write_group().unwrap();
    }

    #[test]
    fn check_clean_repository() {
        let (_d, t) = temp_repo();
        make_one(&t, b"rev-1", b"hello\n");
        let repo = Pack2aRepository::open(t).unwrap();
        let result = check(&repo).unwrap();
        assert!(result.is_clean(), "problems: {:?}", result.problems);
        assert_eq!(result.checked_revisions, 1);
        // Only the file's text is integrity-checked (the root directory has no
        // separate fulltext record in 2a).
        assert_eq!(result.checked_texts, 1);
        assert!(result.ghosts.is_empty());
    }

    #[test]
    fn check_reports_ghost_parent() {
        // A revision whose parent is not in the repository: the parent is a
        // ghost, recorded but not a problem.
        let (_d, t) = temp_repo();
        let mut repo = Pack2aRepository::create(t.clone()).unwrap();
        let root = crate::inventory::ROOT_ID;
        repo.start_write_group().unwrap();
        repo.add_text(b"file-1", b"rev-2", &[], b"hi\n").unwrap();
        let entries = vec![
            crate::inventory::Entry::root(
                crate::FileId::from(root),
                Some(crate::RevisionId::from(&b"rev-2"[..])),
            ),
            crate::inventory::Entry::file(
                crate::FileId::from(&b"file-1"[..]),
                "a.txt".into(),
                crate::FileId::from(root),
                Some(crate::RevisionId::from(&b"rev-2"[..])),
                Some(crate::weave::sha_strings(&[b"hi\n"])),
                Some(3),
                Some(false),
                None,
            ),
        ];
        repo.add_inventory_from_entries(b"rev-2", &[b"rev-1".to_vec()], root, &entries)
            .unwrap();
        repo.add_revision(&revision(b"rev-2", vec![b"rev-1"]), &[b"rev-1".to_vec()])
            .unwrap();
        repo.commit_write_group().unwrap();

        let repo = Pack2aRepository::open(t).unwrap();
        let result = check(&repo).unwrap();
        assert!(result.is_clean(), "problems: {:?}", result.problems);
        assert_eq!(result.ghosts, vec![b"rev-1".to_vec()]);
    }

    #[test]
    fn check_detects_text_sha1_mismatch() {
        // Record a wrong text sha1 in the inventory; check must flag it.
        let (_d, t) = temp_repo();
        let mut repo = Pack2aRepository::create(t.clone()).unwrap();
        let root = crate::inventory::ROOT_ID;
        repo.start_write_group().unwrap();
        repo.add_text(b"file-1", b"rev-1", &[], b"hello\n").unwrap();
        let entries = vec![
            crate::inventory::Entry::root(
                crate::FileId::from(root),
                Some(crate::RevisionId::from(&b"rev-1"[..])),
            ),
            crate::inventory::Entry::file(
                crate::FileId::from(&b"file-1"[..]),
                "a.txt".into(),
                crate::FileId::from(root),
                Some(crate::RevisionId::from(&b"rev-1"[..])),
                // Deliberately wrong sha1 (sha of different content).
                Some(crate::weave::sha_strings(&[b"WRONG"])),
                Some(6),
                Some(false),
                None,
            ),
        ];
        repo.add_inventory_from_entries(b"rev-1", &[], root, &entries)
            .unwrap();
        repo.add_revision(&revision(b"rev-1", vec![]), &[]).unwrap();
        repo.commit_write_group().unwrap();

        let repo = Pack2aRepository::open(t).unwrap();
        let result = check(&repo).unwrap();
        assert!(!result.is_clean());
        assert!(
            result
                .problems
                .iter()
                .any(|p| p.contains("text sha1 mismatch")),
            "expected a sha1 mismatch problem, got {:?}",
            result.problems
        );
    }
}

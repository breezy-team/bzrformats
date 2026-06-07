//! Copying revisions between repositories (inter-repository fetch).
//!
//! [`fetch`] copies a set of revisions, with everything they need
//! (inventories, file texts, signatures), from one repository into another. It
//! works on the abstract [`Repository`] trait, so it copies between *any* pair
//! of formats -- including across storage families (knit-pack to 2a) -- by
//! rebuilding each revision through the neutral `add_*` API. Each backend then
//! stores the data in its own representation.
//!
//! This object-level rebuild is the universal path. A same-format fast path
//! that streams raw records without re-encoding is a future optimisation
//! (it would downcast the concrete pack repositories and copy their stores
//! directly, the way [`super::Pack2aRepository::pack`] does).

use std::collections::{HashMap, HashSet};

use super::{Repository, RepositoryError};

/// Copy revisions from `source` into `target`.
///
/// `revision_id` selects what to copy: `Some(id)` copies that revision and its
/// full ancestry; `None` copies every revision in `source`. Revisions already
/// present in `target` are skipped. Returns the number of revisions copied.
///
/// The copy runs in a single write group on `target`, committed at the end.
pub fn fetch(
    source: &dyn Repository,
    target: &mut dyn Repository,
    revision_id: Option<&[u8]>,
) -> Result<usize, RepositoryError> {
    // The revisions we must copy: the requested closure, minus what the target
    // already has, in topological (parents-first) order.
    let missing = missing_revisions(source, target, revision_id)?;
    if missing.is_empty() {
        return Ok(0);
    }
    let ordered = toposort(source, &missing)?;
    let copied = ordered.len();

    // Give the target a chance to copy these revisions with a format-specific
    // fast path (e.g. two 2a repositories streaming raw records). The target
    // decides whether it can; `false` means "no fast path applies", so fall
    // back to the generic per-revision rebuild. The generic fetcher stays free
    // of any per-format knowledge.
    if target.try_fetch_from(source, &ordered)? {
        return Ok(copied);
    }

    target.start_write_group()?;
    for rev_id in &ordered {
        copy_revision(source, target, rev_id)?;
    }
    target.commit_write_group()?;
    Ok(copied)
}

/// The set of revisions present in `source` (within the requested closure) but
/// absent from `target`.
fn missing_revisions(
    source: &dyn Repository,
    target: &dyn Repository,
    revision_id: Option<&[u8]>,
) -> Result<HashSet<Vec<u8>>, RepositoryError> {
    let wanted = match revision_id {
        // Whole-repository fetch: every revision the source has.
        None => source.all_revision_ids()?.into_iter().collect(),
        // Targeted fetch: the revision plus its full ancestry.
        Some(id) => ancestry_closure(source, id)?,
    };
    let present: HashSet<Vec<u8>> = target.all_revision_ids()?.into_iter().collect();
    Ok(wanted.difference(&present).cloned().collect())
}

/// Every revision in the ancestry of `revision_id` (inclusive), found by
/// walking parents through [`Repository::get_parent_map`]. The null revision is
/// not a real revision and is excluded.
fn ancestry_closure(
    source: &dyn Repository,
    revision_id: &[u8],
) -> Result<HashSet<Vec<u8>>, RepositoryError> {
    let mut seen = HashSet::new();
    let mut pending = vec![revision_id.to_vec()];
    while let Some(id) = pending.pop() {
        if id == crate::branch::NULL_REVISION || !seen.insert(id.clone()) {
            continue;
        }
        let parent_map = source.get_parent_map(std::slice::from_ref(&id))?;
        if let Some(parents) = parent_map.get(&id) {
            for p in parents {
                if p != crate::branch::NULL_REVISION && !seen.contains(p) {
                    pending.push(p.clone());
                }
            }
        }
    }
    Ok(seen)
}

/// Order `revisions` so every revision comes after its parents (a topological
/// sort over the source's revision graph, restricted to the set). Required
/// because the target records each revision against parents that must already
/// be present.
fn toposort(
    source: &dyn Repository,
    revisions: &HashSet<Vec<u8>>,
) -> Result<Vec<Vec<u8>>, RepositoryError> {
    let all: Vec<Vec<u8>> = revisions.iter().cloned().collect();
    let parent_map = source.get_parent_map(&all)?;
    // In-set parents only; parents outside the set are already in the target.
    let deps: HashMap<Vec<u8>, Vec<Vec<u8>>> = all
        .iter()
        .map(|id| {
            let parents = parent_map
                .get(id)
                .map(|ps| {
                    ps.iter()
                        .filter(|p| revisions.contains(*p))
                        .cloned()
                        .collect()
                })
                .unwrap_or_default();
            (id.clone(), parents)
        })
        .collect();

    // Kahn's algorithm, processing ready nodes in sorted order for a stable,
    // reproducible result.
    let mut remaining: HashMap<Vec<u8>, usize> =
        deps.iter().map(|(k, v)| (k.clone(), v.len())).collect();
    let mut children: HashMap<Vec<u8>, Vec<Vec<u8>>> = HashMap::new();
    for (id, parents) in &deps {
        for p in parents {
            children.entry(p.clone()).or_default().push(id.clone());
        }
    }
    let mut ready: Vec<Vec<u8>> = remaining
        .iter()
        .filter(|(_, &n)| n == 0)
        .map(|(k, _)| k.clone())
        .collect();
    ready.sort();
    let mut order = Vec::with_capacity(all.len());
    while let Some(id) = ready.pop() {
        order.push(id.clone());
        if let Some(kids) = children.get(&id) {
            let mut newly_ready = Vec::new();
            for child in kids {
                let count = remaining.get_mut(child).expect("child tracked");
                *count -= 1;
                if *count == 0 {
                    newly_ready.push(child.clone());
                }
            }
            // Keep `ready` sorted-descending so `pop` yields the smallest id.
            ready.extend(newly_ready);
            ready.sort();
        }
    }
    if order.len() != all.len() {
        return Err(RepositoryError::Corrupt(
            "revision graph has a cycle or a missing parent within the fetch set".to_string(),
        ));
    }
    Ok(order)
}

/// Copy one revision and everything it introduces from `source` to `target`'s
/// open write group: the per-entry file texts, the inventory, then the
/// revision record and its signature.
fn copy_revision(
    source: &dyn Repository,
    target: &mut dyn Repository,
    rev_id: &[u8],
) -> Result<(), RepositoryError> {
    let revision = source.get_revision(rev_id)?;
    let parents: Vec<Vec<u8>> = revision
        .parent_ids
        .iter()
        .map(|p| p.as_bytes().to_vec())
        .collect();

    let tree = source.revision_tree(rev_id)?;
    let root = tree
        .inventory()
        .root_entry()
        .map_err(|e| RepositoryError::Corrupt(format!("reading root entry: {e:?}")))?;
    let root_id = root
        .as_ref()
        .map(|r| r.file_id().as_bytes().to_vec())
        .unwrap_or_else(|| crate::inventory::ROOT_ID.to_vec());

    // The full entry set, root first. add_inventory_from_entries indexes every
    // entry it is given (including the root) into the inventory, so the root
    // must be present or the rebuilt inventory has no TREE_ROOT.
    let mut entries: Vec<crate::inventory::Entry> = Vec::new();
    entries.extend(root);
    entries.extend(tree.iter_entries().into_iter().map(|(_, e)| e));

    // A text record exists per inventory entry at the revision that introduced
    // it. Copy the texts this revision introduces (entry.revision == rev_id);
    // entries carried over from older revisions are already in the target.
    // Texts are copied as fulltext with no per-file parents -- parents are a
    // delta/graph optimisation, not needed to read the content back.
    use crate::osutils::Kind;
    for entry in &entries {
        let introduced = entry
            .revision()
            .map(|r| r.as_bytes() == rev_id)
            .unwrap_or(false);
        if !introduced {
            continue;
        }
        let file_id = entry.file_id().as_bytes().to_vec();
        let bytes: Vec<u8> = match entry.kind() {
            Kind::File => source.get_file_text(&file_id, rev_id)?,
            Kind::Symlink => entry
                .symlink_target()
                .map(|t| t.as_bytes().to_vec())
                .unwrap_or_default(),
            // Directories and tree references store an empty text record.
            Kind::Directory | Kind::TreeReference => Vec::new(),
        };
        target.add_text(&file_id, rev_id, &[], &bytes)?;
    }

    // The inventory, rebuilt from the full entry set (root included).
    target.add_inventory_from_entries(rev_id, &parents, &root_id, &entries)?;

    // The revision record.
    target.add_revision(&revision, &parents)?;

    // The signature, if the source has one.
    if let Some(sig) = source.get_signature_text(rev_id)? {
        target.add_signature_text(rev_id, &sig)?;
    }
    Ok(())
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

    /// Commit a chain of revisions rev-1..rev-n (each child of the previous),
    /// each adding a file text, into a fresh 2a repository.
    fn make_chain(t: &SharedTransport, n: usize) -> Vec<Vec<u8>> {
        let mut repo = Pack2aRepository::create(t.clone()).unwrap();
        let root = crate::inventory::ROOT_ID;
        let mut ids: Vec<Vec<u8>> = Vec::new();
        for i in 1..=n {
            let rev = format!("rev-{i}").into_bytes();
            let parents: Vec<&[u8]> = if i == 1 {
                vec![]
            } else {
                vec![ids[i - 2].as_slice()]
            };
            let parent_vecs: Vec<Vec<u8>> = parents.iter().map(|p| p.to_vec()).collect();
            repo.start_write_group().unwrap();
            let text = format!("hello {i}\n").into_bytes();
            repo.add_text(b"file-1", &rev, &[], &text).unwrap();
            let entries = vec![
                crate::inventory::Entry::root(
                    crate::FileId::from(root),
                    Some(crate::RevisionId::from(rev.as_slice())),
                ),
                crate::inventory::Entry::file(
                    crate::FileId::from(&b"file-1"[..]),
                    "a.txt".into(),
                    crate::FileId::from(root),
                    Some(crate::RevisionId::from(rev.as_slice())),
                    Some(crate::weave::sha_strings(&[text.as_slice()])),
                    Some(text.len() as u64),
                    Some(false),
                    None,
                ),
            ];
            repo.add_inventory_from_entries(&rev, &parent_vecs, root, &entries)
                .unwrap();
            repo.add_revision(&revision(&rev, parents), &parent_vecs)
                .unwrap();
            repo.commit_write_group().unwrap();
            ids.push(rev);
        }
        ids
    }

    /// Fetching the tip of a chain copies the whole ancestry; the data reads
    /// back from the target.
    #[test]
    fn fetch_copies_ancestry() {
        let (_sd, st) = temp_repo();
        let ids = make_chain(&st, 3);
        let source = Pack2aRepository::open(st).unwrap();

        let (_td, tt) = temp_repo();
        let mut target = Pack2aRepository::create(tt.clone()).unwrap();

        let copied = fetch(&source, &mut target, Some(&ids[2])).unwrap();
        assert_eq!(copied, 3);

        let target = Pack2aRepository::open(tt).unwrap();
        let mut got = target.all_revision_ids().unwrap();
        got.sort();
        assert_eq!(got, ids);
        // File text at the tip reads back.
        assert_eq!(
            target.get_file_text(b"file-1", &ids[2]).unwrap(),
            b"hello 3\n"
        );
    }

    /// Fetching with no revision id copies everything; a second fetch is a
    /// no-op (target already has it all).
    #[test]
    fn fetch_everything_then_noop() {
        let (_sd, st) = temp_repo();
        let ids = make_chain(&st, 2);
        let source = Pack2aRepository::open(st).unwrap();

        let (_td, tt) = temp_repo();
        let mut target = Pack2aRepository::create(tt.clone()).unwrap();

        assert_eq!(fetch(&source, &mut target, None).unwrap(), 2);
        // Re-open target and fetch again: nothing left to copy.
        let mut target = Pack2aRepository::open(tt).unwrap();
        assert_eq!(fetch(&source, &mut target, None).unwrap(), 0);
        let _ = ids;
    }

    /// Fetching into a target that already has part of the ancestry copies only
    /// the missing tail.
    #[test]
    fn fetch_only_missing() {
        let (_sd, st) = temp_repo();
        let ids = make_chain(&st, 3);
        let source = Pack2aRepository::open(st).unwrap();

        let (_td, tt) = temp_repo();
        let mut target = Pack2aRepository::create(tt.clone()).unwrap();
        // Seed the target with rev-1 only.
        assert_eq!(fetch(&source, &mut target, Some(&ids[0])).unwrap(), 1);
        let mut target = Pack2aRepository::open(tt).unwrap();
        // Now fetch the tip: only rev-2 and rev-3 remain.
        assert_eq!(fetch(&source, &mut target, Some(&ids[2])).unwrap(), 2);
    }

    /// Cross-format fetch: copy from a knit-pack source into a 2a target. The
    /// two formats use different inventory serializers (XML vs CHK) and
    /// record encodings, so this exercises the universal object-level rebuild.
    #[cfg(feature = "knitpack")]
    #[test]
    fn fetch_across_formats_knitpack_to_2a() {
        use crate::repository::KnitPackRepository;

        // Source: a knit-pack (1.9) repository with one revision + a file.
        let (_sd, st) = temp_repo();
        let knitpack6 =
            crate::repository::find_format(b"Bazaar RepositoryFormatKnitPack6 (bzr 1.9)\n")
                .unwrap();
        let mut src = KnitPackRepository::create(st.clone(), knitpack6).unwrap();
        let root = crate::inventory::ROOT_ID;
        let rev = b"rev-1";
        src.start_write_group().unwrap();
        let text = b"hello\n";
        src.add_text(b"file-1", rev, &[], text).unwrap();
        let entries = vec![
            crate::inventory::Entry::root(
                crate::FileId::from(root),
                Some(crate::RevisionId::from(&rev[..])),
            ),
            crate::inventory::Entry::file(
                crate::FileId::from(&b"file-1"[..]),
                "a.txt".into(),
                crate::FileId::from(root),
                Some(crate::RevisionId::from(&rev[..])),
                Some(crate::weave::sha_strings(&[text.as_slice()])),
                Some(text.len() as u64),
                Some(false),
                None,
            ),
        ];
        src.add_inventory_from_entries(rev, &[], root, &entries)
            .unwrap();
        src.add_revision(&revision(rev, vec![]), &[]).unwrap();
        src.commit_write_group().unwrap();
        let source = KnitPackRepository::open(st).unwrap();

        // Target: a 2a (CHK) repository.
        let (_td, tt) = temp_repo();
        let mut target = Pack2aRepository::create(tt.clone()).unwrap();

        assert_eq!(fetch(&source, &mut target, Some(rev)).unwrap(), 1);

        // The revision, its file text and inventory read back from the 2a repo.
        let target = Pack2aRepository::open(tt).unwrap();
        assert!(target.has_revision(rev).unwrap());
        assert_eq!(target.get_revision(rev).unwrap().message, "m");
        assert_eq!(target.get_file_text(b"file-1", rev).unwrap(), b"hello\n");
        // The CHK inventory rebuilt in the 2a target lists the file by path.
        let inv = target.get_inventory(rev).unwrap();
        let paths: Vec<String> = inv.entries().unwrap().into_iter().map(|(p, _)| p).collect();
        assert_eq!(paths, vec!["a.txt".to_string()]);
        assert_eq!(
            target.get_file_text_at_path("a.txt", rev).unwrap(),
            b"hello\n"
        );
    }

    /// The same-format streaming fast path produces a target identical to the
    /// generic per-revision rebuild: same revisions, inventory entries and
    /// file texts. Uses a multi-revision chain so CHK pages branch across
    /// revisions and the reachability walk is exercised.
    #[test]
    fn streaming_matches_generic() {
        let (_sd, st) = temp_repo();
        let ids = make_chain(&st, 4);
        let source = Pack2aRepository::open(st).unwrap();

        // Fast path: the normal fetch (2a -> 2a uses streaming).
        let (_fd, ft) = temp_repo();
        let mut fast = Pack2aRepository::create(ft.clone()).unwrap();
        fetch(&source, &mut fast, Some(&ids[3])).unwrap();
        let fast = Pack2aRepository::open(ft).unwrap();

        // Generic path: drive copy_revision directly into a second target.
        let (_gd, gt) = temp_repo();
        let mut generic = Pack2aRepository::create(gt.clone()).unwrap();
        generic.start_write_group().unwrap();
        for rev in &ids {
            copy_revision(&source, &mut generic, rev).unwrap();
        }
        generic.commit_write_group().unwrap();
        let generic = Pack2aRepository::open(gt).unwrap();

        // Both targets hold the same revisions and read identically.
        let mut a = fast.all_revision_ids().unwrap();
        let mut b = generic.all_revision_ids().unwrap();
        a.sort();
        b.sort();
        assert_eq!(a, ids);
        assert_eq!(b, ids);
        for rev in &ids {
            assert_eq!(
                fast.get_revision(rev).unwrap().message,
                generic.get_revision(rev).unwrap().message
            );
            assert_eq!(
                fast.get_file_text(b"file-1", rev).unwrap(),
                generic.get_file_text(b"file-1", rev).unwrap()
            );
            let fp: Vec<String> = fast
                .get_inventory(rev)
                .unwrap()
                .entries()
                .unwrap()
                .into_iter()
                .map(|(p, _)| p)
                .collect();
            let gp: Vec<String> = generic
                .get_inventory(rev)
                .unwrap()
                .entries()
                .unwrap()
                .into_iter()
                .map(|(p, _)| p)
                .collect();
            assert_eq!(fp, gp);
        }
    }

    /// Incremental streaming fetch into a non-empty 2a target: the second fetch
    /// copies only the new tail and the CHK reachability walk skips pages
    /// already present (the `uninteresting_roots` path).
    #[test]
    fn streaming_incremental_into_nonempty() {
        let (_sd, st) = temp_repo();
        let ids = make_chain(&st, 4);
        let source = Pack2aRepository::open(st).unwrap();

        let (_td, tt) = temp_repo();
        let mut target = Pack2aRepository::create(tt.clone()).unwrap();
        // First fetch up to rev-2.
        assert_eq!(fetch(&source, &mut target, Some(&ids[1])).unwrap(), 2);
        // Then the tip: only rev-3 and rev-4 are missing.
        let mut target = Pack2aRepository::open(tt.clone()).unwrap();
        assert_eq!(fetch(&source, &mut target, Some(&ids[3])).unwrap(), 2);

        // Everything reads back.
        let target = Pack2aRepository::open(tt).unwrap();
        let mut got = target.all_revision_ids().unwrap();
        got.sort();
        assert_eq!(got, ids);
        for (i, rev) in ids.iter().enumerate() {
            assert_eq!(
                target.get_file_text(b"file-1", rev).unwrap(),
                format!("hello {}\n", i + 1).into_bytes()
            );
        }
    }
}

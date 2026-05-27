use crate::inventory::Entry;

/// Serialise entry as a single bytestring.
///
/// :param Entry: An inventory entry.
/// :return: A bytestring for the entry.
///
/// The BNF:
/// ENTRY ::= FILE | DIR | SYMLINK | TREE
/// FILE ::= "file: " COMMON SEP SHA SEP SIZE SEP EXECUTABLE
/// DIR ::= "dir: " COMMON
/// SYMLINK ::= "symlink: " COMMON SEP TARGET_UTF8
/// TREE ::= "tree: " COMMON REFERENCE_REVISION
/// COMMON ::= FILE_ID SEP PARENT_ID SEP NAME_UTF8 SEP REVISION
/// SEP ::= "\n"
pub fn chk_inventory_entry_to_bytes(entry: &Entry) -> Vec<u8> {
    let ts;

    let (header, mut lines) = match entry {
        Entry::File {
            name,
            executable,
            revision,
            text_sha1,
            text_size,
            parent_id,
            ..
        } => {
            ts = format!("{}", text_size.expect("no text size set"));

            (
                &b"file"[..],
                vec![
                    parent_id.as_bytes(),
                    name.as_bytes(),
                    revision.as_ref().expect("no revision set").as_bytes(),
                    text_sha1.as_ref().expect("no text sha1 set").as_slice(),
                    ts.as_bytes(),
                    if *executable { b"Y" } else { b"N" },
                ],
            )
        }
        Entry::Directory {
            revision,
            name,
            parent_id,
            ..
        } => (
            &b"dir"[..],
            vec![
                parent_id.as_bytes(),
                name.as_bytes(),
                revision.as_ref().expect("no revision set").as_bytes(),
            ],
        ),
        Entry::Root { revision, .. } => (
            &b"dir"[..],
            vec![
                &b""[..],
                &b""[..],
                revision.as_ref().expect("no revision set").as_bytes(),
            ],
        ),
        Entry::Link {
            name,
            revision,
            symlink_target,
            parent_id,
            ..
        } => (
            &b"symlink"[..],
            vec![
                parent_id.as_bytes(),
                name.as_bytes(),
                revision.as_ref().expect("no revision set").as_bytes(),
                symlink_target
                    .as_ref()
                    .expect("no symlink target set")
                    .as_bytes(),
            ],
        ),
        Entry::TreeReference {
            revision,
            name,
            reference_revision,
            parent_id,
            ..
        } => (
            &b"tree"[..],
            vec![
                parent_id.as_bytes(),
                name.as_bytes(),
                revision.as_ref().expect("no revision set").as_bytes(),
                reference_revision
                    .as_ref()
                    .expect("no reference revision set")
                    .as_bytes(),
            ],
        ),
    };

    let header = [header, b": ", entry.file_id().as_bytes()].concat();

    lines.insert(0, header.as_slice());

    lines.join(&b"\n"[..])
}

pub fn chk_inventory_bytes_to_entry(data: &[u8]) -> Entry {
    let sections = data.split(|&c| c == b'\n').collect::<Vec<_>>();

    let sp: Vec<&[u8]> = sections[0].splitn(2, |&c| c == b':').collect();
    assert!(&sp[1][..1] == b" ");

    let kind = sp[0];
    let file_id = crate::FileId::from(&sp[1][1..]);

    let name = String::from_utf8(sections[2].to_vec()).unwrap();
    let parent_id = if sections[1].is_empty() {
        None
    } else {
        Some(crate::FileId::from(sections[1]))
    };
    let revision = Some(crate::RevisionId::from(sections[3]));

    match String::from_utf8(kind.to_vec()).unwrap().as_str() {
        "file" => Entry::File {
            name,
            file_id,
            parent_id: parent_id.unwrap(),
            text_sha1: Some(sections[4].to_vec()),
            text_size: Some(
                String::from_utf8(sections[5].to_vec())
                    .unwrap()
                    .parse()
                    .unwrap(),
            ),
            executable: sections[6] == b"Y",
            revision,
            text_id: None,
        },
        "dir" => {
            if let Some(parent_id) = parent_id {
                Entry::Directory {
                    name,
                    file_id,
                    parent_id,
                    revision,
                }
            } else {
                Entry::Root { file_id, revision }
            }
        }
        "symlink" => Entry::Link {
            name,
            file_id,
            parent_id: parent_id.unwrap(),
            symlink_target: Some(String::from_utf8(sections[4].to_vec()).unwrap()),
            revision,
        },
        "tree" => Entry::TreeReference {
            name,
            file_id,
            parent_id: parent_id.unwrap(),
            reference_revision: Some(crate::RevisionId::from(sections[4])),
            revision,
        },
        _ => {
            panic!("Invalid inventory entry");
        }
    }
}

pub fn chk_inventory_bytes_to_utf8_name_key(
    data: &[u8],
) -> (&[u8], crate::FileId, crate::RevisionId) {
    let sections = data.split(|&c| c == b'\n').collect::<Vec<_>>();
    let sp: Vec<&[u8]> = sections[0].splitn(2, |&c| c == b':').collect();
    assert!(&sp[1][..1] == b" ");

    let file_id = crate::FileId::from(&sp[1][1..]);
    let revision = crate::RevisionId::from(sections[3]);
    (sections[2], file_id, revision)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chk_map::testing::FakeChkStore;
    use crate::chk_map::{CHKMap, InMemoryPageCache, PageCache, SearchKeyFunc};
    use crate::FileId;

    fn build_test_inv() -> (
        CHKInventory<FakeChkStore>,
        std::sync::Arc<FakeChkStore>,
        std::sync::Arc<dyn PageCache>,
    ) {
        let store: std::sync::Arc<FakeChkStore> = std::sync::Arc::new(FakeChkStore::new());
        let cache: std::sync::Arc<dyn PageCache> = std::sync::Arc::new(InMemoryPageCache::new());
        let inv = CHKInventory::new(store.clone(), cache.clone(), b"plain".to_vec());
        // Populate id_to_entry with a single root entry.
        let mut id_map = CHKMap::new(store.clone(), cache.clone(), None, SearchKeyFunc::Plain);
        let root = Entry::Root {
            file_id: FileId::from(&b"root-id"[..]),
            revision: Some(crate::RevisionId::from(&b"rev1"[..])),
        };
        id_map
            .map(
                vec![root.file_id().as_bytes().to_vec()],
                chk_inventory_entry_to_bytes(&root),
            )
            .unwrap();
        id_map.save().unwrap();
        inv.id_to_entry.replace(Some(id_map));
        let mut out = CHKInventory::new(store.clone(), cache.clone(), b"plain".to_vec());
        out.root_id = Some(FileId::from(&b"root-id"[..]));
        out.revision_id = Some(crate::RevisionId::from(&b"rev1"[..]));
        out.id_to_entry.replace(inv.id_to_entry.take());
        (out, store, cache)
    }

    /// Build an inventory with a root + one child file + the
    /// parent_id_basename_to_file_id map populated. Used by path2id /
    /// get_children tests.
    fn build_test_inv_with_child() -> (
        CHKInventory<FakeChkStore>,
        std::sync::Arc<FakeChkStore>,
        std::sync::Arc<dyn PageCache>,
    ) {
        let store: std::sync::Arc<FakeChkStore> = std::sync::Arc::new(FakeChkStore::new());
        let cache: std::sync::Arc<dyn PageCache> = std::sync::Arc::new(InMemoryPageCache::new());
        let root = Entry::Root {
            file_id: FileId::from(&b"root-id"[..]),
            revision: Some(crate::RevisionId::from(&b"rev1"[..])),
        };
        let child = Entry::File {
            file_id: FileId::from(&b"file-id"[..]),
            revision: Some(crate::RevisionId::from(&b"rev1"[..])),
            parent_id: FileId::from(&b"root-id"[..]),
            name: "hello.txt".to_string(),
            text_sha1: Some(b"da39a3ee5e6b4b0d3255bfef95601890afd80709".to_vec()),
            text_size: Some(0),
            text_id: None,
            executable: false,
        };
        let mut id_map = CHKMap::new(store.clone(), cache.clone(), None, SearchKeyFunc::Plain);
        for e in [&root, &child] {
            id_map
                .map(
                    vec![e.file_id().as_bytes().to_vec()],
                    chk_inventory_entry_to_bytes(e),
                )
                .unwrap();
        }
        id_map.save().unwrap();
        let mut pid_initial: indexmap::IndexMap<Vec<Vec<u8>>, Vec<u8>> =
            indexmap::IndexMap::new();
        for e in [&root, &child] {
            pid_initial.insert(parent_id_basename_key(e), e.file_id().as_bytes().to_vec());
        }
        let pid_root_key = CHKMap::from_dict(
            store.clone(),
            cache.clone(),
            pid_initial,
            0,
            2,
            SearchKeyFunc::Plain,
        )
        .unwrap();
        let pid_map = CHKMap::new(
            store.clone(),
            cache.clone(),
            Some(pid_root_key),
            SearchKeyFunc::Plain,
        );
        let out = CHKInventory::new(store.clone(), cache.clone(), b"plain".to_vec());
        let out = CHKInventory {
            root_id: Some(FileId::from(&b"root-id"[..])),
            revision_id: Some(crate::RevisionId::from(&b"rev1"[..])),
            id_to_entry: std::cell::RefCell::new(Some(id_map)),
            parent_id_basename_to_file_id: std::cell::RefCell::new(Some(pid_map)),
            ..out
        };
        (out, store, cache)
    }

    #[test]
    fn get_entry_returns_root() {
        let (inv, _store, _cache) = build_test_inv();
        let entry = inv.get_entry(&FileId::from(&b"root-id"[..])).unwrap();
        match entry {
            Entry::Root { file_id, .. } => {
                assert_eq!(file_id.as_bytes(), b"root-id");
            }
            _ => panic!("expected Root"),
        }
    }

    #[test]
    fn get_entry_missing_returns_no_such_id() {
        let (inv, _store, _cache) = build_test_inv();
        let err = inv.get_entry(&FileId::from(&b"absent"[..])).unwrap_err();
        match err {
            Error::NoSuchId(id) => assert_eq!(id.as_bytes(), b"absent"),
            other => panic!("unexpected error: {:?}", other),
        }
    }

    #[test]
    fn has_id_uses_chk_map() {
        let (inv, _store, _cache) = build_test_inv();
        assert!(inv.has_id(&FileId::from(&b"root-id"[..])).unwrap());
        assert!(!inv.has_id(&FileId::from(&b"absent"[..])).unwrap());
    }

    #[test]
    fn is_root_compares_against_root_id() {
        let (inv, _store, _cache) = build_test_inv();
        assert!(inv.is_root(&FileId::from(&b"root-id"[..])));
        assert!(!inv.is_root(&FileId::from(&b"other"[..])));
    }

    #[test]
    fn iter_all_ids_returns_seeded_id() {
        let (inv, _store, _cache) = build_test_inv();
        let ids = inv.iter_all_ids().unwrap();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0].as_bytes(), b"root-id");
    }

    #[test]
    fn iter_just_entries_returns_deserialised_entry() {
        let (inv, _store, _cache) = build_test_inv();
        let entries = inv.iter_just_entries().unwrap();
        assert_eq!(entries.len(), 1);
        assert!(matches!(entries[0], Entry::Root { .. }));
    }

    #[test]
    fn len_matches_chk_map_count() {
        let (inv, _store, _cache) = build_test_inv();
        assert_eq!(inv.len().unwrap(), 1);
        assert!(!inv.is_empty().unwrap());
    }

    /// Inventory: root -> dir/ -> {dir/file.txt, dir/sub/, dir/sub/nested.txt}
    /// + root/top.txt
    fn build_test_inv_deeper() -> CHKInventory<FakeChkStore> {
        let store: std::sync::Arc<FakeChkStore> = std::sync::Arc::new(FakeChkStore::new());
        let cache: std::sync::Arc<dyn PageCache> = std::sync::Arc::new(InMemoryPageCache::new());
        let root = Entry::Root {
            file_id: FileId::from(&b"root-id"[..]),
            revision: Some(crate::RevisionId::from(&b"rev1"[..])),
        };
        let top = Entry::File {
            file_id: FileId::from(&b"top-id"[..]),
            revision: Some(crate::RevisionId::from(&b"rev1"[..])),
            parent_id: FileId::from(&b"root-id"[..]),
            name: "top.txt".to_string(),
            text_sha1: Some(b"x".to_vec()),
            text_size: Some(0),
            text_id: None,
            executable: false,
        };
        let dir = Entry::Directory {
            file_id: FileId::from(&b"dir-id"[..]),
            revision: Some(crate::RevisionId::from(&b"rev1"[..])),
            parent_id: FileId::from(&b"root-id"[..]),
            name: "dir".to_string(),
        };
        let file_in_dir = Entry::File {
            file_id: FileId::from(&b"f1"[..]),
            revision: Some(crate::RevisionId::from(&b"rev1"[..])),
            parent_id: FileId::from(&b"dir-id"[..]),
            name: "file.txt".to_string(),
            text_sha1: Some(b"x".to_vec()),
            text_size: Some(0),
            text_id: None,
            executable: false,
        };
        let sub = Entry::Directory {
            file_id: FileId::from(&b"sub-id"[..]),
            revision: Some(crate::RevisionId::from(&b"rev1"[..])),
            parent_id: FileId::from(&b"dir-id"[..]),
            name: "sub".to_string(),
        };
        let nested = Entry::File {
            file_id: FileId::from(&b"f2"[..]),
            revision: Some(crate::RevisionId::from(&b"rev1"[..])),
            parent_id: FileId::from(&b"sub-id"[..]),
            name: "nested.txt".to_string(),
            text_sha1: Some(b"y".to_vec()),
            text_size: Some(0),
            text_id: None,
            executable: false,
        };
        let mut id_map = CHKMap::new(store.clone(), cache.clone(), None, SearchKeyFunc::Plain);
        let entries = [&root, &top, &dir, &file_in_dir, &sub, &nested];
        for e in entries {
            id_map
                .map(
                    vec![e.file_id().as_bytes().to_vec()],
                    chk_inventory_entry_to_bytes(e),
                )
                .unwrap();
        }
        id_map.save().unwrap();
        let mut pid_initial: indexmap::IndexMap<Vec<Vec<u8>>, Vec<u8>> =
            indexmap::IndexMap::new();
        for e in entries {
            pid_initial.insert(parent_id_basename_key(e), e.file_id().as_bytes().to_vec());
        }
        let pid_root_key = CHKMap::from_dict(
            store.clone(),
            cache.clone(),
            pid_initial,
            0,
            2,
            SearchKeyFunc::Plain,
        )
        .unwrap();
        let pid_map = CHKMap::new(
            store.clone(),
            cache.clone(),
            Some(pid_root_key),
            SearchKeyFunc::Plain,
        );
        let out = CHKInventory::new(store.clone(), cache.clone(), b"plain".to_vec());
        CHKInventory {
            root_id: Some(FileId::from(&b"root-id"[..])),
            revision_id: Some(crate::RevisionId::from(&b"rev1"[..])),
            id_to_entry: std::cell::RefCell::new(Some(id_map)),
            parent_id_basename_to_file_id: std::cell::RefCell::new(Some(pid_map)),
            ..out
        }
    }

    #[test]
    fn iter_changes_reports_added_file() {
        let basis = build_test_inv_deeper();
        // Make a derived inventory adding "added.txt".
        let new_file = Entry::File {
            file_id: FileId::from(&b"new-id"[..]),
            revision: Some(crate::RevisionId::from(&b"rev2"[..])),
            parent_id: FileId::from(&b"root-id"[..]),
            name: "added.txt".to_string(),
            text_sha1: Some(b"y".to_vec()),
            text_size: Some(7),
            text_id: None,
            executable: false,
        };
        let delta = crate::inventory_delta::InventoryDelta(vec![
            crate::inventory_delta::InventoryDeltaEntry {
                old_path: None,
                new_path: Some("added.txt".to_string()),
                file_id: FileId::from(&b"new-id"[..]),
                new_entry: Some(new_file),
            },
        ]);
        let derived = basis
            .create_by_apply_delta(&delta, crate::RevisionId::from(&b"rev2"[..]), false)
            .unwrap();
        let changes = derived.iter_changes(&basis).unwrap();
        let added: Vec<&InventoryChange> = changes
            .iter()
            .filter(|c| c.file_id.as_bytes() == b"new-id")
            .collect();
        assert_eq!(added.len(), 1);
        assert!(added[0].path_in_source.is_none());
        assert_eq!(added[0].path_in_target.as_deref(), Some("added.txt"));
        assert!(added[0].changed_content);
        assert_eq!(added[0].versioned, (false, true));
    }

    #[test]
    fn iter_changes_no_diff_returns_empty() {
        let inv = build_test_inv_deeper();
        // Save and reload to get an equivalent inventory under the
        // same root key — avoiding the RefCell aliasing that would
        // result from comparing an inventory against itself.
        let lines = inv.to_lines().unwrap();
        let reloaded = CHKInventory::deserialise(
            inv.store.clone(),
            inv.cache.clone(),
            &lines,
            inv.revision_id.as_ref().unwrap(),
        )
        .unwrap();
        let changes = reloaded.iter_changes(&inv).unwrap();
        assert!(changes.is_empty());
    }

    #[test]
    fn create_by_apply_delta_adds_a_file() {
        let inv = build_test_inv_deeper();
        // Apply a delta that adds one new file under the root.
        let new_file = Entry::File {
            file_id: FileId::from(&b"new-id"[..]),
            revision: Some(crate::RevisionId::from(&b"rev2"[..])),
            parent_id: FileId::from(&b"root-id"[..]),
            name: "added.txt".to_string(),
            text_sha1: Some(b"x".to_vec()),
            text_size: Some(0),
            text_id: None,
            executable: false,
        };
        let delta = crate::inventory_delta::InventoryDelta(vec![
            crate::inventory_delta::InventoryDeltaEntry {
                old_path: None,
                new_path: Some("added.txt".to_string()),
                file_id: FileId::from(&b"new-id"[..]),
                new_entry: Some(new_file),
            },
        ]);
        let new_inv = inv
            .create_by_apply_delta(&delta, crate::RevisionId::from(&b"rev2"[..]), false)
            .unwrap();
        assert!(new_inv.has_id(&FileId::from(&b"new-id"[..])).unwrap());
        // Original inventory is unchanged.
        assert!(!inv.has_id(&FileId::from(&b"new-id"[..])).unwrap());
        let new_path = new_inv.id2path(&FileId::from(&b"new-id"[..])).unwrap();
        assert_eq!(new_path, "added.txt");
    }

    #[test]
    fn create_by_apply_delta_deletes_a_file() {
        let inv = build_test_inv_deeper();
        let delta = crate::inventory_delta::InventoryDelta(vec![
            crate::inventory_delta::InventoryDeltaEntry {
                old_path: Some("top.txt".to_string()),
                new_path: None,
                file_id: FileId::from(&b"top-id"[..]),
                new_entry: None,
            },
        ]);
        let new_inv = inv
            .create_by_apply_delta(&delta, crate::RevisionId::from(&b"rev2"[..]), false)
            .unwrap();
        assert!(!new_inv.has_id(&FileId::from(&b"top-id"[..])).unwrap());
    }

    #[test]
    fn from_inventory_round_trips_through_to_lines() {
        let store: std::sync::Arc<FakeChkStore> = std::sync::Arc::new(FakeChkStore::new());
        let cache: std::sync::Arc<dyn PageCache> = std::sync::Arc::new(InMemoryPageCache::new());
        let root = Entry::Root {
            file_id: FileId::from(&b"root-id"[..]),
            revision: Some(crate::RevisionId::from(&b"rev1"[..])),
        };
        let file = Entry::File {
            file_id: FileId::from(&b"file-id"[..]),
            revision: Some(crate::RevisionId::from(&b"rev1"[..])),
            parent_id: FileId::from(&b"root-id"[..]),
            name: "f.txt".to_string(),
            text_sha1: Some(b"da39a3ee5e6b4b0d3255bfef95601890afd80709".to_vec()),
            text_size: Some(0),
            text_id: None,
            executable: false,
        };
        let inv = CHKInventory::from_inventory(
            store.clone(),
            cache.clone(),
            crate::RevisionId::from(&b"rev1"[..]),
            FileId::from(&b"root-id"[..]),
            &[root, file],
            0,
            b"plain".to_vec(),
        )
        .unwrap();
        let lines = inv.to_lines().unwrap();
        let expected_rev = crate::RevisionId::from(&b"rev1"[..]);
        let restored = CHKInventory::deserialise(
            store.clone(),
            cache.clone(),
            &lines,
            &expected_rev,
        )
        .unwrap();
        assert_eq!(restored.root_id.as_ref(), inv.root_id.as_ref());
        let restored_items = restored.iter_all_ids().unwrap();
        assert_eq!(restored_items.len(), 2);
    }

    #[test]
    fn get_entry_by_path_finds_nested_file() {
        let inv = build_test_inv_deeper();
        let entry = inv.get_entry_by_path("dir/sub/nested.txt").unwrap();
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().name(), "nested.txt");
    }

    #[test]
    fn get_entry_by_path_missing_returns_none() {
        let inv = build_test_inv_deeper();
        let entry = inv.get_entry_by_path("dir/missing.txt").unwrap();
        assert!(entry.is_none());
    }

    #[test]
    fn get_entry_by_path_root_returns_root() {
        let inv = build_test_inv_deeper();
        let entry = inv.get_entry_by_path("").unwrap();
        assert!(matches!(entry, Some(Entry::Root { .. })));
    }

    #[test]
    fn get_entry_by_path_partial_returns_full_path_when_no_tree_ref() {
        let inv = build_test_inv_deeper();
        let result = inv
            .get_entry_by_path_partial("dir/sub/nested.txt")
            .unwrap();
        let (entry, resolved, remaining) = result.unwrap();
        assert_eq!(entry.name(), "nested.txt");
        assert_eq!(resolved, vec!["dir", "sub", "nested.txt"]);
        assert!(remaining.is_empty());
    }

    #[test]
    fn iter_entries_root_recursive_yields_full_tree() {
        let inv = build_test_inv_deeper();
        let entries = inv.iter_entries(None, true).unwrap();
        let paths: Vec<&str> = entries.iter().map(|(p, _)| p.as_str()).collect();
        // Root yielded first as "", then sorted depth-first.
        assert_eq!(
            paths,
            vec![
                "",
                "dir",
                "dir/file.txt",
                "dir/sub",
                "dir/sub/nested.txt",
                "top.txt",
            ]
        );
    }

    #[test]
    fn iter_entries_non_recursive_yields_direct_children_only() {
        let inv = build_test_inv_deeper();
        let entries = inv
            .iter_entries(Some(&FileId::from(&b"dir-id"[..])), false)
            .unwrap();
        let names: Vec<&str> = entries.iter().map(|(p, _)| p.as_str()).collect();
        assert_eq!(names, vec!["file.txt", "sub"]);
    }

    #[test]
    fn entries_excludes_root() {
        let inv = build_test_inv_deeper();
        let entries = inv.entries().unwrap();
        assert!(entries.iter().all(|(p, _)| !p.is_empty()));
        assert_eq!(entries.len(), 5);
    }

    #[test]
    fn id2path_returns_slash_separated_path() {
        let (inv, _store, _cache) = build_test_inv_with_child();
        let path = inv.id2path(&FileId::from(&b"file-id"[..])).unwrap();
        assert_eq!(path, "hello.txt");
        let root_path = inv.id2path(&FileId::from(&b"root-id"[..])).unwrap();
        assert_eq!(root_path, "");
    }

    #[test]
    fn path2id_resolves_existing_path() {
        let (inv, _store, _cache) = build_test_inv_with_child();
        let id = inv.path2id("hello.txt").unwrap();
        assert_eq!(id.map(|f| f.as_bytes().to_vec()), Some(b"file-id".to_vec()));
    }

    #[test]
    fn path2id_missing_returns_none() {
        let (inv, _store, _cache) = build_test_inv_with_child();
        let id = inv.path2id("no/such/path").unwrap();
        assert_eq!(id, None);
    }

    #[test]
    fn has_filename_true_for_existing() {
        let (inv, _store, _cache) = build_test_inv_with_child();
        assert!(inv.has_filename("hello.txt").unwrap());
        assert!(!inv.has_filename("missing.txt").unwrap());
    }

    #[test]
    fn get_children_returns_root_child() {
        let (inv, _store, _cache) = build_test_inv_with_child();
        let kids = inv.get_children(&FileId::from(&b"root-id"[..])).unwrap();
        assert_eq!(kids.len(), 1);
        assert!(kids.contains_key("hello.txt"));
    }

    #[test]
    fn get_child_by_name() {
        let (inv, _store, _cache) = build_test_inv_with_child();
        let child = inv
            .get_child(&FileId::from(&b"root-id"[..]), "hello.txt")
            .unwrap();
        assert!(child.is_some());
        let absent = inv
            .get_child(&FileId::from(&b"root-id"[..]), "nope.txt")
            .unwrap();
        assert!(absent.is_none());
    }

    #[test]
    fn iter_sorted_children_returns_sorted_names() {
        let (inv, _store, _cache) = build_test_inv_with_child();
        let kids = inv
            .iter_sorted_children(&FileId::from(&b"root-id"[..]))
            .unwrap();
        let names: Vec<&str> = kids.iter().map(|e| e.name()).collect();
        assert_eq!(names, vec!["hello.txt"]);
    }

    #[test]
    fn parent_id_basename_key_directory() {
        let entry = Entry::Directory {
            file_id: FileId::from(&b"dir-id"[..]),
            revision: None,
            parent_id: FileId::from(&b"parent-id"[..]),
            name: "subdir".to_string(),
        };
        let key = parent_id_basename_key(&entry);
        assert_eq!(key, vec![b"parent-id".to_vec(), b"subdir".to_vec()]);
    }

    #[test]
    fn parent_id_basename_key_root_uses_empty_parent_and_name() {
        let entry = Entry::Root {
            file_id: FileId::from(&b"root-id"[..]),
            revision: None,
        };
        let key = parent_id_basename_key(&entry);
        assert_eq!(key, vec![b"".to_vec(), b"".to_vec()]);
    }
}

/// A single change reported by [`CHKInventory::iter_changes`].
/// Mirrors Python's `tree.iter_changes` 8-tuple. Each `(basis, self)`
/// pair has `basis` first matching Python's argument order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventoryChange {
    pub file_id: crate::FileId,
    pub path_in_source: Option<String>,
    pub path_in_target: Option<String>,
    pub changed_content: bool,
    pub versioned: (bool, bool),
    pub parent: (Option<crate::FileId>, Option<crate::FileId>),
    pub name: (Option<String>, Option<String>),
    pub kind: (Option<crate::osutils::Kind>, Option<crate::osutils::Kind>),
    pub executable: (Option<bool>, Option<bool>),
}

/// Build the `(parent_id, basename_utf8)` key used by a
/// `parent_id_basename_to_file_id` CHKMap. Mirrors Python's
/// `CHKInventory._parent_id_basename_key`.
pub fn parent_id_basename_key(entry: &Entry) -> Vec<Vec<u8>> {
    let (parent_id, name) = match entry {
        Entry::Root { .. } => (Vec::new(), String::new()),
        Entry::Directory {
            parent_id, name, ..
        }
        | Entry::File {
            parent_id, name, ..
        }
        | Entry::Link {
            parent_id, name, ..
        }
        | Entry::TreeReference {
            parent_id, name, ..
        } => (parent_id.as_bytes().to_vec(), name.clone()),
    };
    vec![parent_id, name.into_bytes()]
}

/// Error returned by CHKInventory methods.
#[derive(Debug)]
pub enum Error {
    /// Wraps an error from the underlying CHKMap.
    ChkMap(crate::chk_map::Error),
    /// Malformed serialised inventory bytes.
    InvalidFormat(String),
    /// A serialised header included a key we don't recognise.
    UnknownKey(Vec<u8>),
    /// A serialised header listed the same key twice.
    DuplicateKey(Vec<u8>),
    /// Inventory's declared revision id didn't match what the caller expected.
    RevisionMismatch {
        got: crate::RevisionId,
        expected: crate::RevisionId,
    },
    /// `file_id` not present in the inventory. Mirrors Python's `NoSuchId`.
    NoSuchId(crate::FileId),
}

impl From<crate::chk_map::Error> for Error {
    fn from(e: crate::chk_map::Error) -> Self {
        Error::ChkMap(e)
    }
}

/// A CHK-store-backed inventory. Mirrors Python's `CHKInventory`.
///
/// Holds two `CHKMap`s:
/// * `id_to_entry`: `(file_id,)` → serialised entry bytes;
/// * `parent_id_basename_to_file_id`: `(parent_id, basename_utf8)` →
///   `file_id` (optional; older CHK inventories omit it).
///
/// All lookups go through the `CHKMap`s, with small in-memory caches
/// (`fileid_to_entry_cache` / `path_to_fileid_cache` /
/// `children_cache`) to avoid repeated demand-loads. Uses interior
/// mutability so the caches can fill from read-only-looking accessors.
pub struct CHKInventory<S>
where
    S: crate::versionedfile::VersionedFiles + ?Sized,
{
    pub search_key_name: Vec<u8>,
    pub revision_id: Option<crate::RevisionId>,
    pub root_id: Option<crate::FileId>,
    pub id_to_entry: std::cell::RefCell<Option<crate::chk_map::CHKMap<S>>>,
    pub parent_id_basename_to_file_id: std::cell::RefCell<Option<crate::chk_map::CHKMap<S>>>,
    fileid_to_entry_cache: std::cell::RefCell<std::collections::HashMap<crate::FileId, Entry>>,
    fully_cached: std::cell::Cell<bool>,
    path_to_fileid_cache: std::cell::RefCell<std::collections::HashMap<String, crate::FileId>>,
    children_cache: std::cell::RefCell<
        std::collections::HashMap<
            crate::FileId,
            std::collections::HashMap<String, Entry>,
        >,
    >,
    store: std::sync::Arc<S>,
    cache: std::sync::Arc<dyn crate::chk_map::PageCache>,
}

impl<S> CHKInventory<S>
where
    S: crate::versionedfile::VersionedFiles + ?Sized,
{
    /// Construct an empty CHKInventory with the given search-key
    /// variant. The inventory has no maps until populated.
    pub fn new(
        store: std::sync::Arc<S>,
        cache: std::sync::Arc<dyn crate::chk_map::PageCache>,
        search_key_name: Vec<u8>,
    ) -> Self {
        Self {
            search_key_name,
            revision_id: None,
            root_id: None,
            id_to_entry: std::cell::RefCell::new(None),
            parent_id_basename_to_file_id: std::cell::RefCell::new(None),
            fileid_to_entry_cache: std::cell::RefCell::new(std::collections::HashMap::new()),
            fully_cached: std::cell::Cell::new(false),
            path_to_fileid_cache: std::cell::RefCell::new(std::collections::HashMap::new()),
            children_cache: std::cell::RefCell::new(std::collections::HashMap::new()),
            store,
            cache,
        }
    }

    /// Resolve the configured `search_key_name` to a `SearchKeyFunc`
    /// variant. Errors when the name is unknown.
    pub fn search_key_func(&self) -> Result<crate::chk_map::SearchKeyFunc, Error> {
        crate::chk_map::SearchKeyFunc::from_name(&self.search_key_name).map_err(|raw| {
            Error::InvalidFormat(format!("unknown search_key_name: {:?}", raw))
        })
    }

    /// Look up an inventory entry by file id, consulting the cache
    /// first. Mirrors Python's `CHKInventory.get_entry`. Returns
    /// `NoSuchId` when the entry isn't present.
    pub fn get_entry(&self, file_id: &crate::FileId) -> Result<Entry, Error> {
        if let Some(entry) = self.fileid_to_entry_cache.borrow().get(file_id) {
            return Ok(entry.clone());
        }
        let key = vec![file_id.as_bytes().to_vec()];
        let mut id_to_entry = self.id_to_entry.borrow_mut();
        let map = id_to_entry
            .as_mut()
            .ok_or_else(|| Error::InvalidFormat("id_to_entry not set".into()))?;
        let items = map.iteritems(Some(&[key]))?;
        let value = items
            .into_iter()
            .next()
            .map(|(_k, v)| v)
            .ok_or_else(|| Error::NoSuchId(file_id.clone()))?;
        let entry = chk_inventory_bytes_to_entry(&value);
        self.fileid_to_entry_cache
            .borrow_mut()
            .insert(file_id.clone(), entry.clone());
        Ok(entry)
    }

    /// Bulk lookup. Returns entries for whichever of `file_ids` are
    /// present; missing ids are silently omitted. Mirrors Python's
    /// `_getitems`. Order is undefined.
    pub fn get_items(
        &self,
        file_ids: &[crate::FileId],
    ) -> Result<Vec<Entry>, Error> {
        let mut result: Vec<Entry> = Vec::new();
        let mut remaining: Vec<Vec<Vec<u8>>> = Vec::new();
        {
            let cache = self.fileid_to_entry_cache.borrow();
            for fid in file_ids {
                match cache.get(fid) {
                    Some(e) => result.push(e.clone()),
                    None => remaining.push(vec![fid.as_bytes().to_vec()]),
                }
            }
        }
        if remaining.is_empty() {
            return Ok(result);
        }
        let mut id_to_entry = self.id_to_entry.borrow_mut();
        let map = id_to_entry
            .as_mut()
            .ok_or_else(|| Error::InvalidFormat("id_to_entry not set".into()))?;
        for (_k, value) in map.iteritems(Some(&remaining))? {
            let entry = chk_inventory_bytes_to_entry(&value);
            self.fileid_to_entry_cache
                .borrow_mut()
                .insert(entry.file_id().clone(), entry.clone());
            result.push(entry);
        }
        Ok(result)
    }

    /// True if `file_id` is present in the inventory. Mirrors
    /// Python's `has_id`.
    pub fn has_id(&self, file_id: &crate::FileId) -> Result<bool, Error> {
        if self.fileid_to_entry_cache.borrow().contains_key(file_id) {
            return Ok(true);
        }
        let key = vec![file_id.as_bytes().to_vec()];
        let mut id_to_entry = self.id_to_entry.borrow_mut();
        let map = id_to_entry
            .as_mut()
            .ok_or_else(|| Error::InvalidFormat("id_to_entry not set".into()))?;
        Ok(!map.iteritems(Some(&[key]))?.is_empty())
    }

    /// True if `file_id` is the root id. Mirrors `is_root`.
    pub fn is_root(&self, file_id: &crate::FileId) -> bool {
        self.root_id.as_ref() == Some(file_id)
    }

    /// Yield every file id stored in the inventory. Mirrors
    /// `iter_all_ids`.
    pub fn iter_all_ids(&self) -> Result<Vec<crate::FileId>, Error> {
        let mut id_to_entry = self.id_to_entry.borrow_mut();
        let map = id_to_entry
            .as_mut()
            .ok_or_else(|| Error::InvalidFormat("id_to_entry not set".into()))?;
        Ok(map
            .iteritems(None)?
            .into_iter()
            .map(|(k, _)| crate::FileId::from(k.last().cloned().unwrap_or_default().as_slice()))
            .collect())
    }

    /// Yield every entry in the inventory (order undefined). Mirrors
    /// Python's `iter_just_entries`. Caches as it walks.
    pub fn iter_just_entries(&self) -> Result<Vec<Entry>, Error> {
        let mut out: Vec<Entry> = Vec::new();
        let pairs = {
            let mut id_to_entry = self.id_to_entry.borrow_mut();
            let map = id_to_entry
                .as_mut()
                .ok_or_else(|| Error::InvalidFormat("id_to_entry not set".into()))?;
            map.iteritems(None)?
        };
        for (key, value) in pairs {
            let file_id =
                crate::FileId::from(key.last().cloned().unwrap_or_default().as_slice());
            if let Some(entry) = self.fileid_to_entry_cache.borrow().get(&file_id) {
                out.push(entry.clone());
                continue;
            }
            let entry = chk_inventory_bytes_to_entry(&value);
            self.fileid_to_entry_cache
                .borrow_mut()
                .insert(file_id, entry.clone());
            out.push(entry);
        }
        Ok(out)
    }

    /// Number of entries in the inventory. Mirrors `__len__`.
    pub fn len(&self) -> Result<usize, Error> {
        let mut id_to_entry = self.id_to_entry.borrow_mut();
        let map = id_to_entry
            .as_mut()
            .ok_or_else(|| Error::InvalidFormat("id_to_entry not set".into()))?;
        Ok(map.len()?)
    }

    /// True if the inventory holds no entries.
    pub fn is_empty(&self) -> Result<bool, Error> {
        Ok(self.len()? == 0)
    }

    /// Yield the chain of entries from `file_id` up to the root, in
    /// child-to-root order. Mirrors `_iter_file_id_parents`.
    pub fn iter_file_id_parents(
        &self,
        file_id: &crate::FileId,
    ) -> Result<Vec<Entry>, Error> {
        let mut out = Vec::new();
        let mut cur = Some(file_id.clone());
        while let Some(id) = cur {
            let entry = self.get_entry(&id)?;
            cur = entry.parent_id().cloned();
            out.push(entry);
        }
        Ok(out)
    }

    /// Return the slash-separated path of `file_id` from the root.
    /// Mirrors Python's `id2path`. The root's path is `""`.
    pub fn id2path(&self, file_id: &crate::FileId) -> Result<String, Error> {
        let mut parents = self.iter_file_id_parents(file_id)?;
        // The last parent is the root; drop it (its name is "").
        parents.pop();
        parents.reverse();
        let segments: Vec<String> = parents
            .into_iter()
            .map(|e| e.name().to_string())
            .collect();
        Ok(segments.join("/"))
    }

    /// Return the file_id corresponding to `relpath`, or `None` if no
    /// such entry exists. Mirrors Python's `path2id`. `relpath` may
    /// be either a slash-separated path or a vector of basenames.
    pub fn path2id(&self, relpath: &str) -> Result<Option<crate::FileId>, Error> {
        let names: Vec<&str> = if relpath.is_empty() {
            Vec::new()
        } else {
            relpath.split('/').collect()
        };
        if let Some(cached) = self.path_to_fileid_cache.borrow().get(relpath) {
            return Ok(Some(cached.clone()));
        }
        let mut current_id = match &self.root_id {
            None => return Ok(None),
            Some(id) => id.clone(),
        };
        let mut cur_path: Option<String> = None;
        for basename in names {
            cur_path = Some(match cur_path {
                None => basename.to_string(),
                Some(p) => format!("{}/{}", p, basename),
            });
            if let Some(cached) = self
                .path_to_fileid_cache
                .borrow()
                .get(cur_path.as_ref().unwrap())
            {
                current_id = cached.clone();
                continue;
            }
            let basename_utf8 = basename.as_bytes().to_vec();
            let key = vec![current_id.as_bytes().to_vec(), basename_utf8.clone()];
            let mut parent_map = self.parent_id_basename_to_file_id.borrow_mut();
            let map = parent_map.as_mut().ok_or_else(|| {
                Error::InvalidFormat(
                    "parent_id_basename_to_file_id not set; can't path2id".into(),
                )
            })?;
            let items = map.iteritems(Some(&[key]))?;
            let Some((found_key, file_id_bytes)) = items.into_iter().next() else {
                return Ok(None);
            };
            // Sanity check the returned key matches what we asked for.
            if found_key.len() != 2
                || found_key[0] != current_id.as_bytes()
                || found_key[1] != basename_utf8
            {
                return Err(Error::InvalidFormat(format!(
                    "corrupt inventory lookup! key={:?}",
                    found_key
                )));
            }
            let file_id = crate::FileId::from(file_id_bytes.as_slice());
            self.path_to_fileid_cache
                .borrow_mut()
                .insert(cur_path.clone().unwrap(), file_id.clone());
            current_id = file_id;
        }
        Ok(Some(current_id))
    }

    /// True if `filename` exists in the inventory.
    pub fn has_filename(&self, filename: &str) -> Result<bool, Error> {
        Ok(self.path2id(filename)?.is_some())
    }

    /// Return the children of `dir_id` as a `{name -> Entry}` map.
    ///
    /// Mirrors `get_children`: looks them up via the
    /// `parent_id_basename_to_file_id` map (returning the file_ids),
    /// then dereferences via `id_to_entry`. Caches the result.
    pub fn get_children(
        &self,
        dir_id: &crate::FileId,
    ) -> Result<std::collections::HashMap<String, Entry>, Error> {
        if let Some(c) = self.children_cache.borrow().get(dir_id) {
            return Ok(c.clone());
        }
        let mut parent_map = self.parent_id_basename_to_file_id.borrow_mut();
        let map = parent_map.as_mut().ok_or_else(|| {
            Error::InvalidFormat(
                "Inventories without parent_id_basename_to_file_id are no longer supported"
                    .into(),
            )
        })?;
        // 1-element prefix filter looks up just this directory's children.
        let prefix = vec![dir_id.as_bytes().to_vec()];
        let pairs = map.iteritems(Some(&[prefix]))?;
        drop(parent_map);
        let mut child_keys: Vec<crate::FileId> = pairs
            .into_iter()
            .map(|(_k, v)| crate::FileId::from(v.as_slice()))
            .collect();
        let mut result: std::collections::HashMap<String, Entry> =
            std::collections::HashMap::new();
        // Drain from the cache first.
        {
            let cache = self.fileid_to_entry_cache.borrow();
            child_keys.retain(|cid| {
                if let Some(entry) = cache.get(cid) {
                    result.insert(entry.name().to_string(), entry.clone());
                    false
                } else {
                    true
                }
            });
        }
        // Look up the rest via id_to_entry.
        if !child_keys.is_empty() {
            let entries = self.get_items(&child_keys)?;
            for entry in entries {
                result.insert(entry.name().to_string(), entry);
            }
        }
        self.children_cache
            .borrow_mut()
            .insert(dir_id.clone(), result.clone());
        Ok(result)
    }

    /// Return a specific child of `dir_id` by name. Mirrors `get_child`.
    pub fn get_child(
        &self,
        dir_id: &crate::FileId,
        name: &str,
    ) -> Result<Option<Entry>, Error> {
        // TODO(jelmer): implement a version that doesn't load all children.
        let children = self.get_children(dir_id)?;
        Ok(children.get(name).cloned())
    }

    /// Return children of `dir_id` sorted by name. Mirrors
    /// `iter_sorted_children`.
    pub fn iter_sorted_children(
        &self,
        dir_id: &crate::FileId,
    ) -> Result<Vec<Entry>, Error> {
        let children = self.get_children(dir_id)?;
        let mut sorted: Vec<(String, Entry)> = children.into_iter().collect();
        sorted.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(sorted.into_iter().map(|(_, e)| e).collect())
    }

    /// Single record returned by [`iter_changes`].
    ///
    /// Mirrors Python's tree.iter_changes 8-tuple
    /// `(file_id, (path_in_source, path_in_target), changed_content,
    /// versioned, parent, name, kind, executable)`.
    pub fn iter_changes(
        &self,
        basis: &Self,
    ) -> Result<Vec<InventoryChange>, Error> {
        let mut self_id_map = self.id_to_entry.borrow_mut();
        let mut basis_id_map = basis.id_to_entry.borrow_mut();
        let self_map = self_id_map.as_mut().ok_or_else(|| {
            Error::InvalidFormat("self.id_to_entry not set".into())
        })?;
        let basis_map = basis_id_map.as_mut().ok_or_else(|| {
            Error::InvalidFormat("basis.id_to_entry not set".into())
        })?;
        let changes = self_map.iter_changes(basis_map)?;
        drop(self_id_map);
        drop(basis_id_map);
        let mut out: Vec<InventoryChange> = Vec::new();
        for (key, basis_value, self_value) in changes {
            let file_id =
                crate::FileId::from(key.last().cloned().unwrap_or_default().as_slice());
            let basis_entry = basis_value.as_deref().map(chk_inventory_bytes_to_entry);
            let self_entry = self_value.as_deref().map(chk_inventory_bytes_to_entry);
            let path_in_source = match basis_entry.as_ref() {
                Some(_) => Some(basis.id2path(&file_id)?),
                None => None,
            };
            let path_in_target = match self_entry.as_ref() {
                Some(_) => Some(self.id2path(&file_id)?),
                None => None,
            };
            let basis_parent = basis_entry.as_ref().and_then(|e| e.parent_id().cloned());
            let basis_name = basis_entry.as_ref().map(|e| e.name().to_string());
            let basis_executable = basis_entry.as_ref().and_then(|e| match e {
                Entry::File { executable, .. } => Some(*executable),
                _ => None,
            });
            let self_parent = self_entry.as_ref().and_then(|e| e.parent_id().cloned());
            let self_name = self_entry.as_ref().map(|e| e.name().to_string());
            let self_executable = self_entry.as_ref().and_then(|e| match e {
                Entry::File { executable, .. } => Some(*executable),
                _ => None,
            });
            let basis_kind = basis_entry.as_ref().map(|e| e.kind());
            let self_kind = self_entry.as_ref().map(|e| e.kind());
            let versioned = (basis_value.is_some(), self_value.is_some());
            let mut changed_content = basis_kind != self_kind;
            if !changed_content {
                match (&basis_entry, &self_entry) {
                    (
                        Some(Entry::File {
                            text_size: bs,
                            text_sha1: bsha,
                            ..
                        }),
                        Some(Entry::File {
                            text_size: ss,
                            text_sha1: ssha,
                            ..
                        }),
                    ) => {
                        if bs != ss || bsha != ssha {
                            changed_content = true;
                        }
                    }
                    (
                        Some(Entry::Link {
                            symlink_target: bt, ..
                        }),
                        Some(Entry::Link {
                            symlink_target: st, ..
                        }),
                    ) => {
                        if bt != st {
                            changed_content = true;
                        }
                    }
                    (
                        Some(Entry::TreeReference {
                            reference_revision: br,
                            ..
                        }),
                        Some(Entry::TreeReference {
                            reference_revision: sr,
                            ..
                        }),
                    ) => {
                        if br != sr {
                            changed_content = true;
                        }
                    }
                    _ => {}
                }
            }
            if !changed_content
                && basis_parent == self_parent
                && basis_name == self_name
                && basis_executable == self_executable
            {
                continue;
            }
            out.push(InventoryChange {
                file_id,
                path_in_source,
                path_in_target,
                changed_content,
                versioned,
                parent: (basis_parent, self_parent),
                name: (basis_name, self_name),
                kind: (basis_kind, self_kind),
                executable: (basis_executable, self_executable),
            });
        }
        Ok(out)
    }

    /// Apply `inventory_delta` to this inventory, returning a new
    /// CHKInventory under `new_revision_id`. Mirrors Python's
    /// `create_by_apply_delta` — the receiver is *not* modified.
    ///
    /// `propagate_caches` carries forward this inventory's
    /// `path_to_fileid_cache` (minus deleted paths) into the new
    /// inventory to amortise lookups when the caller will be doing
    /// many id2path/path2id calls on the result.
    pub fn create_by_apply_delta(
        &self,
        inventory_delta: &crate::inventory_delta::InventoryDelta,
        new_revision_id: crate::RevisionId,
        propagate_caches: bool,
    ) -> Result<Self, Error> {
        inventory_delta.check().map_err(|e| {
            Error::InvalidFormat(format!("inventory delta failed precheck: {:?}", e))
        })?;
        let search_key_func = self.search_key_func()?;
        // Establish the new id_to_entry map sharing this inventory's
        // current root (we'll apply_delta into it). Preserving the
        // existing maximum_size requires ensure_root on the original
        // then reading off the root_node.
        let id_root_key = {
            let mut id_to_entry = self.id_to_entry.borrow_mut();
            let map = id_to_entry
                .as_mut()
                .ok_or_else(|| Error::InvalidFormat("id_to_entry not set".into()))?;
            map.ensure_root()?;
            map.key()
                .ok_or_else(|| Error::InvalidFormat("id_to_entry has no key".into()))?
        };
        let mut result_id_map = crate::chk_map::CHKMap::new(
            self.store.clone(),
            self.cache.clone(),
            Some(id_root_key),
            search_key_func.clone(),
        );
        // Establish the new parent_id_basename_to_file_id map similarly.
        let parent_root_key = {
            let mut pid_map = self.parent_id_basename_to_file_id.borrow_mut();
            match pid_map.as_mut() {
                None => None,
                Some(map) => {
                    map.ensure_root()?;
                    Some(
                        map.key()
                            .ok_or_else(|| {
                                Error::InvalidFormat(
                                    "parent_id_basename_to_file_id has no key".into(),
                                )
                            })?,
                    )
                }
            }
        };
        let mut result_pid_map = parent_root_key.map(|k| {
            crate::chk_map::CHKMap::new(
                self.store.clone(),
                self.cache.clone(),
                Some(k),
                search_key_func.clone(),
            )
        });
        let mut new_root_id = self.root_id.clone();
        let mut path_cache: std::collections::HashMap<String, crate::FileId> =
            if propagate_caches {
                self.path_to_fileid_cache.borrow().clone()
            } else {
                std::collections::HashMap::new()
            };
        let mut id_delta: Vec<(
            Option<Vec<Vec<u8>>>,
            Option<Vec<Vec<u8>>>,
            Vec<u8>,
        )> = Vec::new();
        let mut parent_delta: indexmap::IndexMap<Vec<Vec<u8>>, (Option<Vec<Vec<u8>>>, Option<Vec<u8>>)> =
            indexmap::IndexMap::new();
        let mut parents: std::collections::HashSet<(String, Option<crate::FileId>)> =
            std::collections::HashSet::new();
        let mut deletes: std::collections::HashSet<crate::FileId> =
            std::collections::HashSet::new();
        let mut altered: std::collections::HashSet<crate::FileId> =
            std::collections::HashSet::new();
        for entry_d in inventory_delta.iter() {
            // Adjust root_id if the new path is "".
            if entry_d.new_path.as_deref() == Some("") {
                new_root_id = Some(entry_d.file_id.clone());
            }
            let (new_key, new_value): (Option<Vec<Vec<u8>>>, Option<Vec<u8>>) =
                match &entry_d.new_path {
                    None => {
                        if propagate_caches {
                            if let Some(op) = &entry_d.old_path {
                                path_cache.remove(op);
                            }
                        }
                        deletes.insert(entry_d.file_id.clone());
                        (None, None)
                    }
                    Some(new_path) => {
                        let entry = entry_d.new_entry.as_ref().ok_or_else(|| {
                            Error::InvalidFormat("delta entry with new_path missing new_entry".into())
                        })?;
                        let key = vec![entry_d.file_id.as_bytes().to_vec()];
                        let value = chk_inventory_entry_to_bytes(entry);
                        path_cache.insert(new_path.clone(), entry_d.file_id.clone());
                        let split_at = new_path.rfind('/').unwrap_or(0);
                        let parent_path = if split_at == 0 {
                            String::new()
                        } else {
                            new_path[..split_at].to_string()
                        };
                        parents.insert((parent_path, entry.parent_id().cloned()));
                        (Some(key), Some(value))
                    }
                };
            let old_key: Option<Vec<Vec<u8>>> = if entry_d.old_path.is_some() {
                let k = vec![entry_d.file_id.as_bytes().to_vec()];
                // Sanity check: the existing path matches what the
                // delta claims.
                let observed = self.id2path(&entry_d.file_id)?;
                if Some(&observed) != entry_d.old_path.as_ref() {
                    return Err(Error::InvalidFormat(format!(
                        "Entry {:?} was at wrong path {:?} (delta expected {:?})",
                        entry_d.file_id, observed, entry_d.old_path
                    )));
                }
                altered.insert(entry_d.file_id.clone());
                Some(k)
            } else {
                None
            };
            id_delta.push((
                old_key,
                new_key,
                new_value.unwrap_or_default(),
            ));
            if result_pid_map.is_some() {
                let old_pkey = if entry_d.old_path.is_some() {
                    let old_entry = self.get_entry(&entry_d.file_id)?;
                    Some(parent_id_basename_key(&old_entry))
                } else {
                    None
                };
                let (new_pkey, new_pvalue): (Option<Vec<Vec<u8>>>, Option<Vec<u8>>) =
                    match (&entry_d.new_path, &entry_d.new_entry) {
                        (None, _) => (None, None),
                        (Some(_), None) => (None, None),
                        (Some(_), Some(entry)) => (
                            Some(parent_id_basename_key(entry)),
                            Some(entry_d.file_id.as_bytes().to_vec()),
                        ),
                    };
                if old_pkey != new_pkey {
                    if let Some(ok) = &old_pkey {
                        let slot = parent_delta.entry(ok.clone()).or_insert((None, None));
                        slot.0 = Some(ok.clone());
                    }
                    if let Some(nk) = &new_pkey {
                        let slot = parent_delta.entry(nk.clone()).or_insert((None, None));
                        slot.1 = new_pvalue;
                    }
                }
            }
        }
        // Validate that deletes are complete (every child of a deleted
        // directory was either deleted or moved).
        for fid in &deletes {
            let entry = self.get_entry(fid)?;
            if !matches!(entry, Entry::Directory { .. }) {
                continue;
            }
            for child in self.iter_sorted_children(fid)? {
                if !altered.contains(child.file_id()) {
                    return Err(Error::InvalidFormat(format!(
                        "Child {:?} not deleted or reparented when parent {:?} deleted",
                        child.file_id(),
                        fid
                    )));
                }
            }
        }
        result_id_map.apply_delta(id_delta)?;
        if let Some(pid_map) = result_pid_map.as_mut() {
            if !parent_delta.is_empty() {
                let delta_list: Vec<(
                    Option<Vec<Vec<u8>>>,
                    Option<Vec<Vec<u8>>>,
                    Vec<u8>,
                )> = parent_delta
                    .into_iter()
                    .map(|(key, (old_key, value))| match value {
                        Some(v) => (old_key, Some(key), v),
                        None => (old_key, None, Vec::new()),
                    })
                    .collect();
                pid_map.apply_delta(delta_list)?;
            }
        }
        let result = Self {
            search_key_name: self.search_key_name.clone(),
            revision_id: Some(new_revision_id),
            root_id: new_root_id,
            id_to_entry: std::cell::RefCell::new(Some(result_id_map)),
            parent_id_basename_to_file_id: std::cell::RefCell::new(result_pid_map),
            fileid_to_entry_cache: std::cell::RefCell::new(std::collections::HashMap::new()),
            fully_cached: std::cell::Cell::new(false),
            path_to_fileid_cache: std::cell::RefCell::new(path_cache),
            children_cache: std::cell::RefCell::new(std::collections::HashMap::new()),
            store: self.store.clone(),
            cache: self.cache.clone(),
        };
        // Validate parent expectations.
        let mut parents = parents;
        parents.retain(|(p, id)| !(p.is_empty() && id.is_none()));
        for (parent_path, parent_id) in &parents {
            let parent_id = match parent_id {
                None => continue,
                Some(id) => id,
            };
            match result.get_entry(parent_id) {
                Ok(entry) => {
                    if !matches!(entry, Entry::Directory { .. } | Entry::Root { .. }) {
                        return Err(Error::InvalidFormat(format!(
                            "Parent {:?} is not a directory, but given children",
                            parent_id
                        )));
                    }
                }
                Err(Error::NoSuchId(_)) => {
                    return Err(Error::InvalidFormat(format!(
                        "Parent {:?} is not present in resulting inventory.",
                        parent_id
                    )));
                }
                Err(e) => return Err(e),
            }
            if result.path2id(parent_path)? != Some(parent_id.clone()) {
                return Err(Error::InvalidFormat(format!(
                    "Parent {:?} has wrong path {:?}",
                    parent_id, parent_path
                )));
            }
        }
        Ok(result)
    }

    /// Bulk-create a CHKInventory by serialising every entry in
    /// `entries` into a fresh pair of CHKMaps under `store`. Mirrors
    /// Python's `from_inventory` / `_populate_from_dicts`.
    ///
    /// `entries` should be an in-order traversal of an inventory (the
    /// Python version iterates `inventory.iter_entries()`); the root
    /// entry's file_id becomes `root_id`.
    pub fn from_inventory(
        store: std::sync::Arc<S>,
        cache: std::sync::Arc<dyn crate::chk_map::PageCache>,
        revision_id: crate::RevisionId,
        root_id: crate::FileId,
        entries: &[Entry],
        maximum_size: usize,
        search_key_name: Vec<u8>,
    ) -> Result<Self, Error> {
        let search_key_func = crate::chk_map::SearchKeyFunc::from_name(&search_key_name)
            .map_err(|raw| {
                Error::InvalidFormat(format!("unknown search_key_name: {:?}", raw))
            })?;
        let mut id_to_entry_dict: indexmap::IndexMap<Vec<Vec<u8>>, Vec<u8>> =
            indexmap::IndexMap::new();
        let mut parent_dict: indexmap::IndexMap<Vec<Vec<u8>>, Vec<u8>> =
            indexmap::IndexMap::new();
        for entry in entries {
            id_to_entry_dict.insert(
                vec![entry.file_id().as_bytes().to_vec()],
                chk_inventory_entry_to_bytes(entry),
            );
            parent_dict.insert(
                parent_id_basename_key(entry),
                entry.file_id().as_bytes().to_vec(),
            );
        }
        let id_root = crate::chk_map::CHKMap::from_dict(
            store.clone(),
            cache.clone(),
            id_to_entry_dict,
            maximum_size,
            1,
            search_key_func.clone(),
        )?;
        let parent_root = crate::chk_map::CHKMap::from_dict(
            store.clone(),
            cache.clone(),
            parent_dict,
            maximum_size,
            2,
            search_key_func.clone(),
        )?;
        let id_map = crate::chk_map::CHKMap::new(
            store.clone(),
            cache.clone(),
            Some(id_root),
            search_key_func.clone(),
        );
        let pid_map = crate::chk_map::CHKMap::new(
            store.clone(),
            cache.clone(),
            Some(parent_root),
            search_key_func,
        );
        Ok(Self {
            search_key_name,
            revision_id: Some(revision_id),
            root_id: Some(root_id),
            id_to_entry: std::cell::RefCell::new(Some(id_map)),
            parent_id_basename_to_file_id: std::cell::RefCell::new(Some(pid_map)),
            fileid_to_entry_cache: std::cell::RefCell::new(std::collections::HashMap::new()),
            fully_cached: std::cell::Cell::new(false),
            path_to_fileid_cache: std::cell::RefCell::new(std::collections::HashMap::new()),
            children_cache: std::cell::RefCell::new(std::collections::HashMap::new()),
            store,
            cache,
        })
    }

    /// Return the entry at `relpath`, or `None` if missing.
    /// Mirrors Python's `get_entry_by_path`.
    pub fn get_entry_by_path(&self, relpath: &str) -> Result<Option<Entry>, Error> {
        let names: Vec<&str> = if relpath.is_empty() {
            Vec::new()
        } else {
            relpath.split('/').collect()
        };
        let mut parent = match &self.root_id {
            None => return Ok(None),
            Some(id) => self.get_entry(id)?,
        };
        if names.is_empty() {
            return Ok(Some(parent));
        }
        for name in names {
            let parent_id = parent.file_id().clone();
            let child = self.get_child(&parent_id, name)?;
            match child {
                None => return Ok(None),
                Some(ie) => parent = ie,
            }
        }
        Ok(Some(parent))
    }

    /// Like `get_entry_by_path`, but stops at the first tree
    /// reference. Returns the (entry, resolved_elements,
    /// remaining_elements) tuple. Mirrors Python's
    /// `get_entry_by_path_partial`.
    pub fn get_entry_by_path_partial<'a>(
        &self,
        relpath: &'a str,
    ) -> Result<Option<(Entry, Vec<&'a str>, Vec<&'a str>)>, Error> {
        let names: Vec<&str> = if relpath.is_empty() {
            Vec::new()
        } else {
            relpath.split('/').collect()
        };
        let mut parent = match &self.root_id {
            None => return Ok(None),
            Some(id) => self.get_entry(id)?,
        };
        for (i, f) in names.iter().enumerate() {
            let parent_id = parent.file_id().clone();
            let child = self.get_child(&parent_id, f)?;
            match child {
                None => return Ok(None),
                Some(ie) => {
                    if matches!(ie, Entry::TreeReference { .. }) {
                        return Ok(Some((
                            ie,
                            names[..=i].to_vec(),
                            names[i + 1..].to_vec(),
                        )));
                    }
                    parent = ie;
                }
            }
        }
        Ok(Some((parent, names.clone(), Vec::new())))
    }

    /// Walk the inventory in lexicographic order from `from_dir`
    /// (or the root if `None`), yielding `(path, entry)` pairs.
    /// `recursive=false` only yields the direct children of
    /// `from_dir`.
    ///
    /// Mirrors Python's `iter_entries`. When starting from the
    /// root, the root entry itself is yielded with an empty path.
    pub fn iter_entries(
        &self,
        from_dir: Option<&crate::FileId>,
        recursive: bool,
    ) -> Result<Vec<(String, Entry)>, Error> {
        let mut out: Vec<(String, Entry)> = Vec::new();
        let start_id = match from_dir {
            Some(id) => id.clone(),
            None => match &self.root_id {
                None => return Ok(out),
                Some(id) => {
                    let root = self.get_entry(id)?;
                    out.push((String::new(), root));
                    id.clone()
                }
            },
        };
        let direct: Vec<Entry> = self.iter_sorted_children(&start_id)?;
        if !recursive {
            for ch in direct {
                let name = ch.name().to_string();
                out.push((name, ch));
            }
            return Ok(out);
        }
        // Iterative depth-first walk over the subtree.
        // Stack frames: (path_so_far, queue_of_pending_children).
        let mut stack: Vec<(String, std::collections::VecDeque<Entry>)> = Vec::new();
        stack.push((String::new(), direct.into_iter().collect()));
        while let Some((path, children)) = stack.last_mut() {
            if let Some(ie) = children.pop_front() {
                let child_path = format!("{}/{}", path, ie.name());
                // Trim leading slash for the top-level children.
                let yield_path = child_path.trim_start_matches('/').to_string();
                let is_directory = matches!(ie, Entry::Directory { .. });
                let file_id = ie.file_id().clone();
                out.push((yield_path, ie));
                if is_directory {
                    let new_children: std::collections::VecDeque<Entry> =
                        self.iter_sorted_children(&file_id)?.into_iter().collect();
                    stack.push((child_path, new_children));
                }
            } else {
                stack.pop();
            }
        }
        Ok(out)
    }

    /// Return `[(path, entry)]` for every entry except the root.
    /// Mirrors Python's `entries`, which exists as a slightly-faster
    /// alternative to `iter_entries`.
    pub fn entries(&self) -> Result<Vec<(String, Entry)>, Error> {
        let mut all = self.iter_entries(None, true)?;
        // Drop the synthetic root entry yielded under "".
        if let Some(first) = all.first() {
            if first.0.is_empty() {
                all.remove(0);
            }
        }
        Ok(all)
    }

    /// Serialise the inventory header to lines (the part that
    /// references the two CHK maps; the maps themselves are stored
    /// separately). Mirrors Python's `to_lines`.
    pub fn to_lines(&self) -> Result<Vec<Vec<u8>>, Error> {
        let mut lines: Vec<Vec<u8>> = Vec::new();
        lines.push(b"chkinventory:\n".to_vec());
        let id_to_entry_key = self
            .id_to_entry
            .borrow()
            .as_ref()
            .and_then(|m| m.key())
            .ok_or_else(|| Error::InvalidFormat("id_to_entry has no key".into()))?;
        let parent_key = self
            .parent_id_basename_to_file_id
            .borrow()
            .as_ref()
            .and_then(|m| m.key());
        let revision_id = self
            .revision_id
            .as_ref()
            .ok_or_else(|| Error::InvalidFormat("revision_id not set".into()))?;
        let root_id = self
            .root_id
            .as_ref()
            .ok_or_else(|| Error::InvalidFormat("root_id not set".into()))?;
        if &self.search_key_name[..] != b"plain" {
            // Mirror Python's "custom ordering grouping things that
            // don't change together" for non-plain serialisers.
            lines.push({
                let mut l = b"search_key_name: ".to_vec();
                l.extend_from_slice(&self.search_key_name);
                l.push(b'\n');
                l
            });
            lines.push({
                let mut l = b"root_id: ".to_vec();
                l.extend_from_slice(root_id.as_bytes());
                l.push(b'\n');
                l
            });
            if let Some(pk) = &parent_key {
                lines.push({
                    let mut l = b"parent_id_basename_to_file_id: ".to_vec();
                    l.extend_from_slice(pk);
                    l.push(b'\n');
                    l
                });
            }
            lines.push({
                let mut l = b"revision_id: ".to_vec();
                l.extend_from_slice(revision_id.as_bytes());
                l.push(b'\n');
                l
            });
            lines.push({
                let mut l = b"id_to_entry: ".to_vec();
                l.extend_from_slice(&id_to_entry_key);
                l.push(b'\n');
                l
            });
        } else {
            lines.push({
                let mut l = b"revision_id: ".to_vec();
                l.extend_from_slice(revision_id.as_bytes());
                l.push(b'\n');
                l
            });
            lines.push({
                let mut l = b"root_id: ".to_vec();
                l.extend_from_slice(root_id.as_bytes());
                l.push(b'\n');
                l
            });
            if let Some(pk) = &parent_key {
                lines.push({
                    let mut l = b"parent_id_basename_to_file_id: ".to_vec();
                    l.extend_from_slice(pk);
                    l.push(b'\n');
                    l
                });
            }
            lines.push({
                let mut l = b"id_to_entry: ".to_vec();
                l.extend_from_slice(&id_to_entry_key);
                l.push(b'\n');
                l
            });
        }
        Ok(lines)
    }

    /// Deserialise an inventory from `lines`. Mirrors Python's
    /// `CHKInventory.deserialise(chk_store, lines, expected_revision_id)`.
    pub fn deserialise(
        store: std::sync::Arc<S>,
        cache: std::sync::Arc<dyn crate::chk_map::PageCache>,
        lines: &[Vec<u8>],
        expected_revision_id: &crate::RevisionId,
    ) -> Result<Self, Error> {
        if lines.is_empty() || !lines[lines.len() - 1].ends_with(b"\n") {
            return Err(Error::InvalidFormat(
                "last line should have trailing eol".into(),
            ));
        }
        if lines[0] != b"chkinventory:\n" {
            return Err(Error::InvalidFormat(
                "not a serialised CHKInventory".into(),
            ));
        }
        let allowed: &[&[u8]] = &[
            b"root_id",
            b"revision_id",
            b"parent_id_basename_to_file_id",
            b"search_key_name",
            b"id_to_entry",
        ];
        let mut info: std::collections::HashMap<Vec<u8>, Vec<u8>> =
            std::collections::HashMap::new();
        for line in &lines[1..] {
            let line = line.strip_suffix(b"\n").unwrap_or(line);
            let split_at = line
                .windows(2)
                .position(|w| w == b": ")
                .ok_or_else(|| Error::InvalidFormat("inventory line missing ': '".into()))?;
            let key = line[..split_at].to_vec();
            let value = line[split_at + 2..].to_vec();
            if !allowed.iter().any(|a| *a == &key[..]) {
                return Err(Error::UnknownKey(key));
            }
            if info.contains_key(&key) {
                return Err(Error::DuplicateKey(key));
            }
            info.insert(key, value);
        }
        let revision_id = info
            .remove(&b"revision_id"[..].to_vec())
            .map(crate::RevisionId::from)
            .ok_or_else(|| Error::InvalidFormat("missing revision_id".into()))?;
        let root_id = info
            .remove(&b"root_id"[..].to_vec())
            .map(|v| crate::FileId::from(v.as_slice()))
            .ok_or_else(|| Error::InvalidFormat("missing root_id".into()))?;
        let search_key_name = info
            .remove(&b"search_key_name"[..].to_vec())
            .unwrap_or_else(|| b"plain".to_vec());
        let parent_key = info
            .remove(&b"parent_id_basename_to_file_id"[..].to_vec());
        let id_to_entry_key = info
            .remove(&b"id_to_entry"[..].to_vec())
            .ok_or_else(|| Error::InvalidFormat("missing id_to_entry".into()))?;
        if let Some(pk) = &parent_key {
            if !pk.starts_with(b"sha1:") {
                return Err(Error::InvalidFormat(format!(
                    "parent_id_basename_to_file_id should be a sha1 key, not {:?}",
                    pk
                )));
            }
        }
        if !id_to_entry_key.starts_with(b"sha1:") {
            return Err(Error::InvalidFormat(format!(
                "id_to_entry should be a sha1 key, not {:?}",
                id_to_entry_key
            )));
        }
        if &revision_id != expected_revision_id {
            return Err(Error::RevisionMismatch {
                got: revision_id,
                expected: expected_revision_id.clone(),
            });
        }
        let search_key_func = crate::chk_map::SearchKeyFunc::from_name(&search_key_name)
            .map_err(|raw| {
                Error::InvalidFormat(format!("unknown search_key_name: {:?}", raw))
            })?;
        let id_map = crate::chk_map::CHKMap::new(
            store.clone(),
            cache.clone(),
            Some(id_to_entry_key),
            search_key_func.clone(),
        );
        let parent_map = parent_key.map(|pk| {
            crate::chk_map::CHKMap::new(store.clone(), cache.clone(), Some(pk), search_key_func)
        });
        let result = Self::new(store, cache, search_key_name);
        result.id_to_entry.replace(Some(id_map));
        result.parent_id_basename_to_file_id.replace(parent_map);
        Ok(Self {
            revision_id: Some(revision_id),
            root_id: Some(root_id),
            ..result
        })
    }
}

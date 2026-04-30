use super::*;

#[test]
fn new_matches_python_defaults() {
    let state = DirState::new(
        "/tmp/.bzr/checkout/dirstate",
        Box::new(DefaultSHA1Provider::new()),
        0,
        true,
        false,
    );
    assert_eq!(state.filename, PathBuf::from("/tmp/.bzr/checkout/dirstate"));
    assert_eq!(state.header_state, MemoryState::NotInMemory);
    assert_eq!(state.dirblock_state, MemoryState::NotInMemory);
    assert!(!state.changes_aborted);
    assert!(state.dirblocks.is_empty());
    assert!(state.ghosts.is_empty());
    assert!(state.parents.is_empty());
    assert_eq!(state.end_of_header, None);
    assert_eq!(state.cutoff_time, None);
    assert_eq!(state.num_entries, 0);
    assert_eq!(state.lock_state, None);
    assert!(state.known_hash_changes.is_empty());
    assert_eq!(state.worth_saving_limit, 0);
    assert!(!state.fdatasync);
    assert!(state.use_filesystem_for_exec);
    assert_eq!(state.bisect_page_size, BISECT_PAGE_SIZE);
    assert!(state.id_index.is_none());
}

#[test]
fn new_honours_overrides() {
    let state = DirState::new(
        "dirstate",
        Box::new(DefaultSHA1Provider::new()),
        -1,
        false,
        true,
    );
    assert_eq!(state.worth_saving_limit, -1);
    assert!(!state.use_filesystem_for_exec);
    assert!(state.fdatasync);
}

/// Build a minimal dirstate file containing just a header (no entries)
/// by running the same `get_output_lines` / `get_parents_line` /
/// `get_ghosts_line` helpers Python uses when writing.
fn make_header_bytes(parents: &[&[u8]], ghosts: &[&[u8]]) -> Vec<u8> {
    let parents_line = get_parents_line(parents);
    let ghosts_line = get_ghosts_line(ghosts);
    // Matches `get_lines` with no entries: lines[0]=parents, lines[1]=ghosts.
    let lines: Vec<&[u8]> = vec![parents_line.as_slice(), ghosts_line.as_slice()];
    let chunks = get_output_lines(lines);
    chunks.into_iter().flatten().collect()
}

#[test]
fn read_header_no_parents_no_ghosts() {
    let bytes = make_header_bytes(&[], &[]);
    let header = read_header(&bytes).expect("parse header");
    assert_eq!(header.num_entries, 0);
    assert!(header.parents.is_empty());
    assert!(header.ghosts.is_empty());
}

#[test]
fn read_header_with_parents_and_ghosts() {
    let bytes = make_header_bytes(&[b"rev-a", b"rev-b"], &[b"ghost-1"]);
    let header = read_header(&bytes).expect("parse header");
    assert_eq!(header.parents, vec![b"rev-a".to_vec(), b"rev-b".to_vec()]);
    assert_eq!(header.ghosts, vec![b"ghost-1".to_vec()]);
}

/// Cross-check the reader against bytes produced by the Python side
/// calling `get_output_lines` + `get_parents_line` + `get_ghosts_line`.
/// Pinning the exact byte sequence guards against any drift between
/// the reader and the (already-Rust-backed) writer.
#[test]
fn read_header_matches_python_generated_bytes() {
    let bytes: &[u8] = b"#bazaar dirstate flat format 3\n\
                         crc32: 2265437010\n\
                         num_entries: 0\n\
                         2\x00rev-a\x00rev-b\x00\n\
                         \x001\x00ghost-1\x00\n\x00";
    let header = read_header(bytes).expect("parse header");
    assert_eq!(header.crc_expected, 2265437010);
    assert_eq!(header.num_entries, 0);
    assert_eq!(header.parents, vec![b"rev-a".to_vec(), b"rev-b".to_vec()]);
    assert_eq!(header.ghosts, vec![b"ghost-1".to_vec()]);
}

#[test]
fn read_header_populates_struct_fields() {
    let bytes = make_header_bytes(&[b"rev-a"], &[]);
    let mut state = DirState::new(
        "dirstate",
        Box::new(DefaultSHA1Provider::new()),
        0,
        true,
        false,
    );
    state.read_header(&bytes).expect("parse header");
    assert_eq!(state.header_state, MemoryState::InMemoryUnmodified);
    assert_eq!(state.parents, vec![b"rev-a".to_vec()]);
    assert!(state.ghosts.is_empty());
    assert_eq!(state.num_entries, 0);
    assert!(state.end_of_header.is_some());
}

#[test]
fn read_header_rejects_wrong_format_line() {
    let bytes = b"#bazaar dirstate flat format 2\ncrc32: 0\nnum_entries: 0\n0\n\x000\n";
    match read_header(bytes) {
        Err(HeaderError::BadFormatLine(line)) => {
            assert_eq!(line, HEADER_FORMAT_2.to_vec());
        }
        other => panic!("expected BadFormatLine, got {:?}", other),
    }
}

#[test]
fn read_header_rejects_missing_crc() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(HEADER_FORMAT_3);
    bytes.extend_from_slice(b"not-a-crc-line\n");
    assert!(matches!(
        read_header(&bytes),
        Err(HeaderError::MissingCrcLine(_))
    ));
}

#[test]
fn read_header_rejects_bad_num_entries() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(HEADER_FORMAT_3);
    bytes.extend_from_slice(b"crc32: 0\n");
    bytes.extend_from_slice(b"num_entries: abc\n");
    bytes.extend_from_slice(b"0\n\x000\n");
    assert!(matches!(
        read_header(&bytes),
        Err(HeaderError::BadNumEntries(_))
    ));
}

/// Hand-built line for a single entry with one tree. Mirrors
/// `DirState._entry_to_line` in `bzrformats/dirstate.py`: the 3 key
/// fields followed by 5 fields per tree, all joined by NUL.
fn entry_line(
    dirname: &[u8],
    basename: &[u8],
    file_id: &[u8],
    trees: &[(&[u8], &[u8], u64, bool, &[u8])],
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(dirname);
    out.push(0);
    out.extend_from_slice(basename);
    out.push(0);
    out.extend_from_slice(file_id);
    for (minikind, fingerprint, size, executable, info) in trees {
        out.push(0);
        out.extend_from_slice(minikind);
        out.push(0);
        out.extend_from_slice(fingerprint);
        out.push(0);
        out.extend_from_slice(format!("{}", size).as_bytes());
        out.push(0);
        out.push(if *executable { b'y' } else { b'n' });
        out.push(0);
        out.extend_from_slice(info);
    }
    out
}

/// Build the body text (post-header) by running `get_output_lines` on a
/// [parents, ghosts, entry_lines...] sequence and then trimming the
/// bytes preceding the first NUL that begins the entry block.
fn make_body_bytes(parents: &[&[u8]], ghosts: &[&[u8]], entries: &[Vec<u8>]) -> Vec<u8> {
    let parents_line = get_parents_line(parents);
    let ghosts_line = get_ghosts_line(ghosts);
    let mut lines: Vec<&[u8]> = vec![parents_line.as_slice(), ghosts_line.as_slice()];
    for e in entries {
        lines.push(e.as_slice());
    }
    let chunks = get_output_lines(lines);
    let data: Vec<u8> = chunks.into_iter().flatten().collect();
    // Locate `end_of_header` by parsing the header in the same way
    // `DirState::read_header` does, then return the remainder.
    let header = read_header(&data).expect("header parses");
    data[header.end_of_header..].to_vec()
}

#[test]
fn parse_dirblocks_empty_body() {
    let blocks = parse_dirblocks(&[], 1, 0).expect("empty body parses");
    assert!(blocks.is_empty());
}

#[test]
fn parse_dirblocks_single_root_entry_one_tree() {
    let nullstat = b"x".repeat(32);
    let entry = entry_line(
        b"",
        b"",
        b"TREE_ROOT",
        &[(b"d", b"", 0, false, nullstat.as_slice())],
    );
    let body = make_body_bytes(&[], &[], &[entry]);
    let blocks = parse_dirblocks(&body, 1, 1).expect("parse dirblocks");
    assert_eq!(blocks.len(), 2, "expected two sentinel blocks");
    assert_eq!(blocks[0].dirname, b"".to_vec());
    assert_eq!(blocks[1].dirname, b"".to_vec());
    assert_eq!(blocks[0].entries.len(), 1);
    let entry = &blocks[0].entries[0];
    assert_eq!(entry.key.dirname, b"".to_vec());
    assert_eq!(entry.key.basename, b"".to_vec());
    assert_eq!(entry.key.file_id, b"TREE_ROOT".to_vec());
    assert_eq!(entry.trees.len(), 1);
    let tree = &entry.trees[0];
    assert_eq!(tree.minikind, Kind::Directory);
    assert_eq!(tree.fingerprint, Vec::<u8>::new());
    assert_eq!(tree.size, 0);
    assert!(!tree.executable);
    assert_eq!(tree.packed_stat, nullstat);
}

#[test]
fn parse_dirblocks_multiple_dirs_group_by_dirname() {
    let nullstat = b"x".repeat(32);
    // Three entries: root, a/file-a, b/file-b. Must be sorted by
    // `(dirname, basename)` to match what the writer produces.
    let entries = vec![
        entry_line(
            b"",
            b"",
            b"TREE_ROOT",
            &[(b"d", b"", 0, false, nullstat.as_slice())],
        ),
        entry_line(
            b"a",
            b"file-a",
            b"fid-a",
            &[(b"f", b"sha-a", 5, true, nullstat.as_slice())],
        ),
        entry_line(
            b"b",
            b"file-b",
            b"fid-b",
            &[(b"f", b"sha-b", 7, false, nullstat.as_slice())],
        ),
    ];
    let body = make_body_bytes(&[], &[], &entries);
    let blocks = parse_dirblocks(&body, 1, 3).expect("parse dirblocks");
    // Two sentinels plus two real dir blocks.
    assert_eq!(blocks.len(), 4);
    assert_eq!(blocks[0].dirname, b"".to_vec());
    assert_eq!(blocks[0].entries.len(), 1);
    assert_eq!(blocks[1].dirname, b"".to_vec());
    assert_eq!(blocks[1].entries.len(), 0);
    assert_eq!(blocks[2].dirname, b"a".to_vec());
    assert_eq!(blocks[2].entries.len(), 1);
    assert_eq!(blocks[2].entries[0].key.basename, b"file-a".to_vec());
    assert!(blocks[2].entries[0].trees[0].executable);
    assert_eq!(blocks[2].entries[0].trees[0].size, 5);
    assert_eq!(blocks[3].dirname, b"b".to_vec());
    assert_eq!(blocks[3].entries.len(), 1);
    assert_eq!(blocks[3].entries[0].trees[0].size, 7);
    assert!(!blocks[3].entries[0].trees[0].executable);
}

#[test]
fn parse_dirblocks_rejects_wrong_entry_count() {
    let nullstat = b"x".repeat(32);
    let entry = entry_line(
        b"",
        b"",
        b"TREE_ROOT",
        &[(b"d", b"", 0, false, nullstat.as_slice())],
    );
    let body = make_body_bytes(&[], &[], &[entry]);
    // Header claimed 2 entries but body only has 1.
    match parse_dirblocks(&body, 1, 2) {
        Err(DirblocksError::WrongEntryCount {
            expected: 2,
            actual: 1,
        }) => {}
        other => panic!("expected WrongEntryCount, got {:?}", other),
    }
}

#[test]
fn parse_dirblocks_multi_tree() {
    let nullstat = b"x".repeat(32);
    // Two trees per entry: current + one parent.
    let entry = entry_line(
        b"",
        b"README",
        b"file-id-1",
        &[
            (b"f", b"sha-current", 10, true, nullstat.as_slice()),
            (b"f", b"sha-parent", 8, false, nullstat.as_slice()),
        ],
    );
    let body = make_body_bytes(&[b"rev-a"], &[], &[entry]);
    let blocks = parse_dirblocks(&body, 2, 1).expect("parse");
    assert_eq!(blocks.len(), 2);
    let e = &blocks[0].entries[0];
    assert_eq!(e.trees.len(), 2);
    assert_eq!(e.trees[0].fingerprint, b"sha-current".to_vec());
    assert_eq!(e.trees[0].size, 10);
    assert!(e.trees[0].executable);
    assert_eq!(e.trees[1].fingerprint, b"sha-parent".to_vec());
    assert_eq!(e.trees[1].size, 8);
    assert!(!e.trees[1].executable);
}

/// Cross-check against bytes produced by a full
/// `DirState.initialize(...); _set_data(...); save()` cycle. Pinning
/// the exact on-disk representation guards against any future drift
/// between the writer and the new Rust reader.
#[test]
fn parse_dirblocks_matches_python_saved_file() {
    let bytes: &[u8] = b"#bazaar dirstate flat format 3\n\
                         crc32: 2823629280\n\
                         num_entries: 1\n\
                         0\x00\n\
                         \x000\x00\n\
                         \x00\x00\x00TREE_ROOT\x00d\x00\x000\x00n\x00xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\x00\n\x00";
    let header = read_header(bytes).expect("parse header");
    assert_eq!(header.num_entries, 1);
    assert!(header.parents.is_empty());
    assert!(header.ghosts.is_empty());
    let body = &bytes[header.end_of_header..];
    let blocks = parse_dirblocks(body, 1, header.num_entries).expect("parse body");
    assert_eq!(blocks.len(), 2);
    assert_eq!(blocks[0].entries.len(), 1);
    let entry = &blocks[0].entries[0];
    assert_eq!(entry.key.file_id, b"TREE_ROOT".to_vec());
    assert_eq!(entry.trees[0].minikind, Kind::Directory);
    assert_eq!(entry.trees[0].packed_stat, b"x".repeat(32));
}

fn make_entry(dirname: &[u8], basename: &[u8], file_id: &[u8]) -> Entry {
    Entry {
        key: EntryKey {
            dirname: dirname.to_vec(),
            basename: basename.to_vec(),
            file_id: file_id.to_vec(),
        },
        trees: vec![TreeData {
            minikind: Kind::File,
            fingerprint: Vec::new(),
            size: 0,
            executable: false,
            packed_stat: b"x".repeat(32),
        }],
    }
}

#[test]
fn split_root_dirblock_separates_root_from_contents() {
    let mut dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![
                make_entry(b"", b"", b"TREE_ROOT"),
                make_entry(b"", b"README", b"fid-readme"),
                make_entry(b"", b"CONTRIBUTING", b"fid-contrib"),
            ],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: Vec::new(),
        },
    ];
    split_root_dirblock_into_contents(&mut dirblocks).expect("split");
    assert_eq!(dirblocks.len(), 2);
    assert_eq!(dirblocks[0].entries.len(), 1);
    assert_eq!(dirblocks[0].entries[0].key.file_id, b"TREE_ROOT".to_vec());
    assert_eq!(dirblocks[1].entries.len(), 2);
    assert_eq!(dirblocks[1].entries[0].key.basename, b"README".to_vec());
    assert_eq!(
        dirblocks[1].entries[1].key.basename,
        b"CONTRIBUTING".to_vec()
    );
}

#[test]
fn split_root_dirblock_preserves_order_within_partitions() {
    // The partition step must keep the original relative order in both
    // halves — Python's implementation walks `_dirblocks[0][1]` in
    // order and appends to two separate lists.
    let mut dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![
                make_entry(b"", b"a", b"fid-a"),
                make_entry(b"", b"", b"TREE_ROOT"),
                make_entry(b"", b"b", b"fid-b"),
            ],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: Vec::new(),
        },
    ];
    split_root_dirblock_into_contents(&mut dirblocks).expect("split");
    assert_eq!(dirblocks[0].entries.len(), 1);
    assert_eq!(dirblocks[0].entries[0].key.file_id, b"TREE_ROOT".to_vec());
    assert_eq!(dirblocks[1].entries.len(), 2);
    assert_eq!(dirblocks[1].entries[0].key.basename, b"a".to_vec());
    assert_eq!(dirblocks[1].entries[1].key.basename, b"b".to_vec());
}

#[test]
fn split_root_dirblock_leaves_later_blocks_alone() {
    let mut dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![make_entry(b"", b"", b"TREE_ROOT")],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: Vec::new(),
        },
        Dirblock {
            dirname: b"subdir".to_vec(),
            entries: vec![make_entry(b"subdir", b"file", b"fid-s")],
        },
    ];
    split_root_dirblock_into_contents(&mut dirblocks).expect("split");
    assert_eq!(dirblocks.len(), 3);
    assert_eq!(dirblocks[2].dirname, b"subdir".to_vec());
    assert_eq!(dirblocks[2].entries.len(), 1);
}

#[test]
fn split_root_dirblock_rejects_missing_sentinel() {
    let mut dirblocks = vec![Dirblock {
        dirname: Vec::new(),
        entries: Vec::new(),
    }];
    assert_eq!(
        split_root_dirblock_into_contents(&mut dirblocks),
        Err(SplitRootError::MissingSentinels)
    );
}

#[test]
fn split_root_dirblock_rejects_polluted_sentinel() {
    let mut dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![make_entry(b"", b"", b"TREE_ROOT")],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: vec![make_entry(b"", b"x", b"fid-x")],
        },
    ];
    match split_root_dirblock_into_contents(&mut dirblocks) {
        Err(SplitRootError::BadSecondSentinel {
            dirname,
            entry_count,
        }) => {
            assert!(dirname.is_empty());
            assert_eq!(entry_count, 1);
        }
        other => panic!("expected BadSecondSentinel, got {:?}", other),
    }
}

fn dirblock_with_entries(dirname: &[u8], entries: Vec<Entry>) -> Dirblock {
    Dirblock {
        dirname: dirname.to_vec(),
        entries,
    }
}

/// Build the canonical two-sentinel-plus-real-blocks layout used by
/// the lookup tests. `subdirs` is a list of `(dirname, entries)`
/// pairs that become real blocks after the sentinels.
fn make_dirblocks(subdirs: Vec<(&[u8], Vec<Entry>)>) -> Vec<Dirblock> {
    let mut blocks = vec![
        dirblock_with_entries(b"", Vec::new()),
        dirblock_with_entries(b"", Vec::new()),
    ];
    for (dirname, entries) in subdirs {
        blocks.push(dirblock_with_entries(dirname, entries));
    }
    blocks
}

fn tree(minikind: Kind) -> TreeData {
    TreeData {
        minikind,
        fingerprint: Vec::new(),
        size: 0,
        executable: false,
        packed_stat: b"x".repeat(32),
    }
}

fn entry_with_trees(
    dirname: &[u8],
    basename: &[u8],
    file_id: &[u8],
    trees: Vec<TreeData>,
) -> Entry {
    Entry {
        key: EntryKey {
            dirname: dirname.to_vec(),
            basename: basename.to_vec(),
            file_id: file_id.to_vec(),
        },
        trees,
    }
}

#[test]
fn bisect_dirblock_component_order_not_byte_order() {
    // Component-wise ordering: `a/b` splits to ["a", "b"] which is
    // less than ["a-b"] because the first-element comparison of "a"
    // and "a-b" treats "a" as a prefix of "a-b". A pure byte sort
    // would place "a-b" before "a/b" (0x2d < 0x2f), so this test
    // pins the path-component-aware behaviour.
    // Sorted input: ["a", "a/b", "a-b", "b"].
    let blocks = make_dirblocks(vec![
        (b"a", vec![]),
        (b"a/b", vec![]),
        (b"a-b", vec![]),
        (b"b", vec![]),
    ]);
    // 2 sentinels + 4 real. lo=1 skips the first sentinel (matching
    // Python's bisect_dirblock(..., 1, hi) idiom), hi=len.
    assert_eq!(bisect_dirblock(&blocks, b"a", 1, blocks.len()), 2);
    assert_eq!(bisect_dirblock(&blocks, b"a/b", 1, blocks.len()), 3);
    assert_eq!(bisect_dirblock(&blocks, b"a-b", 1, blocks.len()), 4);
    assert_eq!(bisect_dirblock(&blocks, b"b", 1, blocks.len()), 5);
    // Insertion for a missing dirname: "aa" > "a-b" byte-wise in
    // single-component form, so it lands after "a-b" (index 4) at
    // index 5, which is also the slot for "b".
    assert_eq!(bisect_dirblock(&blocks, b"aa", 1, blocks.len()), 5);
}

/// Build dirblocks from a list of sorted paths and, for each path,
/// assert that `bisect_dirblock` agrees with a manual `bisect_left`
/// over the split-by-`/` form. Mirrors `assertBisect` from the
/// Python `TestBisectDirblock` test class.
fn assert_bisect_matches_bisect_left(paths: &[&[u8]]) {
    // Verify the caller's list is actually sorted component-wise
    // (matches Python's `assertEqual(sorted(split_dirblocks), split_dirblocks)`).
    let split: Vec<Vec<&[u8]>> = paths.iter().map(|p| split_dirname(p)).collect();
    let mut sorted = split.clone();
    sorted.sort();
    assert_eq!(split, sorted, "test input paths are not sorted");

    let blocks: Vec<Dirblock> = paths
        .iter()
        .map(|p| Dirblock {
            dirname: p.to_vec(),
            entries: Vec::new(),
        })
        .collect();

    for probe in paths {
        let got = bisect_dirblock(&blocks, probe, 0, blocks.len());
        let probe_split = split_dirname(probe);
        let expected = split.partition_point(|s| *s < probe_split);
        assert_eq!(
            got, expected,
            "bisect_dirblock disagreed for {:?}: got {}, expected {}",
            probe, got, expected,
        );
    }
}

/// Rust counterpart of Python `TestBisectDirblock.test_simple`.
#[test]
fn bisect_dirblock_simple() {
    let paths: Vec<&[u8]> = vec![b"", b"a", b"b", b"c", b"d"];
    assert_bisect_matches_bisect_left(&paths);
}

/// Rust counterpart of Python `TestBisectDirblock.test_involved`.
/// The pure-Rust `bisect_dirblock` does not have a `cache` parameter
/// (Python's `_split_path_cache` only speeds up repeated lookups and
/// does not affect results), so Python's `test_involved` and
/// `test_involved_cached` collapse into a single Rust test over the
/// same input.
#[test]
fn bisect_dirblock_involved() {
    let paths: Vec<&[u8]> = vec![
        b"", b"a", b"a/a", b"a/a/a", b"a/a/z", b"a/a-a", b"a/a-z", b"a/z", b"a/z/a", b"a/z/z",
        b"a/z-a", b"a/z-z", b"a-a", b"a-z", b"z", b"z/a/a", b"z/a/z", b"z/a-a", b"z/a-z",
        b"z/z", b"z/z/a", b"z/z/z", b"z/z-a", b"z/z-z", b"z-a", b"z-z",
    ];
    assert_bisect_matches_bisect_left(&paths);
}

#[test]
fn find_block_index_from_key_root_fast_path() {
    let blocks = make_dirblocks(vec![(b"sub", vec![])]);
    let key = EntryKey {
        dirname: b"".to_vec(),
        basename: b"".to_vec(),
        file_id: b"TREE_ROOT".to_vec(),
    };
    assert_eq!(find_block_index_from_key(&blocks, &key), (0, true));
}

#[test]
fn find_block_index_from_key_hit_and_miss() {
    let blocks = make_dirblocks(vec![(b"a", vec![]), (b"c", vec![])]);
    let hit = EntryKey {
        dirname: b"a".to_vec(),
        basename: b"foo".to_vec(),
        file_id: b"".to_vec(),
    };
    assert_eq!(find_block_index_from_key(&blocks, &hit), (2, true));
    let miss = EntryKey {
        dirname: b"b".to_vec(),
        basename: b"foo".to_vec(),
        file_id: b"".to_vec(),
    };
    // "b" would be inserted between "a" (index 2) and "c" (index 3).
    assert_eq!(find_block_index_from_key(&blocks, &miss), (3, false));
}

#[test]
fn find_entry_index_exact_and_insertion() {
    let block = vec![
        entry_with_trees(b"dir", b"a", b"fid-a", vec![tree(Kind::File)]),
        entry_with_trees(b"dir", b"b", b"fid-b", vec![tree(Kind::File)]),
        entry_with_trees(b"dir", b"c", b"fid-c", vec![tree(Kind::File)]),
    ];
    let hit = EntryKey {
        dirname: b"dir".to_vec(),
        basename: b"b".to_vec(),
        file_id: b"fid-b".to_vec(),
    };
    assert_eq!(find_entry_index(&hit, &block), (1, true));
    let miss_before = EntryKey {
        dirname: b"dir".to_vec(),
        basename: b"ab".to_vec(),
        file_id: b"".to_vec(),
    };
    assert_eq!(find_entry_index(&miss_before, &block), (1, false));
    let miss_end = EntryKey {
        dirname: b"dir".to_vec(),
        basename: b"z".to_vec(),
        file_id: b"".to_vec(),
    };
    assert_eq!(find_entry_index(&miss_end, &block), (3, false));
}

#[test]
fn get_block_entry_index_finds_live_entry() {
    let blocks = make_dirblocks(vec![(
        b"dir",
        vec![entry_with_trees(
            b"dir",
            b"a",
            b"fid-a",
            vec![tree(Kind::File)],
        )],
    )]);
    let bei = get_block_entry_index(&blocks, b"dir", b"a", 0);
    assert_eq!(bei.block_index, 2);
    assert_eq!(bei.entry_index, 0);
    assert!(bei.dir_present);
    assert!(bei.path_present);
}

#[test]
fn get_block_entry_index_absent_dir() {
    let blocks = make_dirblocks(vec![(b"a", vec![])]);
    let bei = get_block_entry_index(&blocks, b"missing", b"file", 0);
    assert!(!bei.dir_present);
    assert!(!bei.path_present);
}

#[test]
fn get_block_entry_index_skips_absent_and_relocated() {
    // Two entries at (dir, a): the first is absent in tree 0, the
    // second is live. Python walks the contiguous run so the live
    // one should be returned.
    let blocks = make_dirblocks(vec![(
        b"dir",
        vec![
            entry_with_trees(b"dir", b"a", b"fid-absent", vec![tree(Kind::Absent)]),
            entry_with_trees(b"dir", b"a", b"fid-live", vec![tree(Kind::File)]),
        ],
    )]);
    let bei = get_block_entry_index(&blocks, b"dir", b"a", 0);
    assert!(bei.path_present);
    assert_eq!(bei.entry_index, 1);
    assert_eq!(
        blocks[bei.block_index].entries[bei.entry_index].key.file_id,
        b"fid-live".to_vec()
    );
}

#[test]
fn get_block_entry_index_all_absent_returns_not_present() {
    let blocks = make_dirblocks(vec![(
        b"dir",
        vec![
            entry_with_trees(b"dir", b"a", b"fid-1", vec![tree(Kind::Absent)]),
            entry_with_trees(b"dir", b"a", b"fid-2", vec![tree(Kind::Relocated)]),
        ],
    )]);
    let bei = get_block_entry_index(&blocks, b"dir", b"a", 0);
    assert!(bei.dir_present);
    assert!(!bei.path_present);
    assert_eq!(bei.entry_index, 2);
}

/// Packed_stat constant matching Python's test fixtures.
const PACKED_STAT: &[u8] = b"AAAAREUHaIpFB2iKAAADAQAtkqUAAIGk";
/// Null-sha matching Python's test fixtures.
const NULL_SHA: &[u8] = b"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";

fn stat_tree(minikind: Kind) -> TreeData {
    TreeData {
        minikind,
        fingerprint: Vec::new(),
        size: 0,
        executable: false,
        packed_stat: PACKED_STAT.to_vec(),
    }
}

fn file_tree(size: u64) -> TreeData {
    TreeData {
        minikind: Kind::File,
        fingerprint: NULL_SHA.to_vec(),
        size,
        executable: false,
        packed_stat: PACKED_STAT.to_vec(),
    }
}

/// Rust mirror of Python's `create_dirstate_with_root_and_subdir`:
/// a root entry plus a single `subdir` entry in the contents-of-root
/// block. Used by `TestGetBlockRowIndex.test_simple_structure`.
fn create_dirstate_with_root_and_subdir() -> DirState {
    let mut state = fresh_state();
    state.dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![entry_with_trees(
                b"",
                b"",
                b"a-root-value",
                vec![stat_tree(Kind::Directory)],
            )],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: vec![entry_with_trees(
                b"",
                b"subdir",
                b"subdir-id",
                vec![stat_tree(Kind::Directory)],
            )],
        },
    ];
    state
}

/// Rust mirror of Python's `create_complex_dirstate`. Matches the
/// docstring in test_dirstate.py: root + directories a/ and b/, files
/// c and d, a/e (empty dir), a/f, b/g, b/h\xc3\xa5.
fn create_complex_dirstate() -> DirState {
    let mut state = fresh_state();
    state.dirblocks = vec![
        // Block 0: root entry.
        Dirblock {
            dirname: Vec::new(),
            entries: vec![entry_with_trees(
                b"",
                b"",
                b"a-root-value",
                vec![stat_tree(Kind::Directory)],
            )],
        },
        // Block 1: contents of root — a, b (both dirs), c, d (files).
        Dirblock {
            dirname: Vec::new(),
            entries: vec![
                entry_with_trees(b"", b"a", b"a-dir", vec![stat_tree(Kind::Directory)]),
                entry_with_trees(b"", b"b", b"b-dir", vec![stat_tree(Kind::Directory)]),
                entry_with_trees(b"", b"c", b"c-file", vec![file_tree(10)]),
                entry_with_trees(b"", b"d", b"d-file", vec![file_tree(20)]),
            ],
        },
        // Block 2: inside a/ — e (dir), f (file).
        Dirblock {
            dirname: b"a".to_vec(),
            entries: vec![
                entry_with_trees(b"a", b"e", b"e-dir", vec![stat_tree(Kind::Directory)]),
                entry_with_trees(b"a", b"f", b"f-file", vec![file_tree(30)]),
            ],
        },
        // Block 3: inside b/ — g, h\xc3\xa5 (file with non-ASCII name).
        Dirblock {
            dirname: b"b".to_vec(),
            entries: vec![
                entry_with_trees(b"b", b"g", b"g-file", vec![file_tree(30)]),
                entry_with_trees(b"b", b"h\xc3\xa5", b"h-\xc3\xa5-file", vec![file_tree(40)]),
            ],
        },
    ];
    state
}

/// Rust counterpart of Python
/// `TestGetBlockRowIndex.test_simple_structure`.
/// Rust mirror of Python's `create_dirstate_with_two_trees` fixture
/// used by `TestIterChildEntries`. Two trees per row; the tree at
/// index 1 is a pretend parent revision with a few differences from
/// the working tree (b/g absent, b/h with a different file id,
/// b/i new, c renamed to b/j).
fn create_dirstate_with_two_trees() -> DirState {
    let mut state = fresh_state();
    state.parents = vec![b"parent".to_vec()];
    let stat_current = TreeData {
        minikind: Kind::Directory,
        fingerprint: Vec::new(),
        size: 0,
        executable: false,
        packed_stat: PACKED_STAT.to_vec(),
    };
    let stat_parent_dir = TreeData {
        minikind: Kind::Directory,
        fingerprint: Vec::new(),
        size: 0,
        executable: false,
        packed_stat: b"parent-revid".to_vec(),
    };
    let null_parent = TreeData {
        minikind: Kind::Absent,
        fingerprint: Vec::new(),
        size: 0,
        executable: false,
        packed_stat: Vec::new(),
    };
    let file_cur = |size: u64| TreeData {
        minikind: Kind::File,
        fingerprint: NULL_SHA.to_vec(),
        size,
        executable: false,
        packed_stat: PACKED_STAT.to_vec(),
    };
    let file_parent = |fingerprint: &[u8], size: u64| TreeData {
        minikind: Kind::File,
        fingerprint: fingerprint.to_vec(),
        size,
        executable: false,
        packed_stat: b"parent-revid".to_vec(),
    };
    let relocated = |to: &[u8]| TreeData {
        minikind: Kind::Relocated,
        fingerprint: to.to_vec(),
        size: 0,
        executable: false,
        packed_stat: Vec::new(),
    };

    state.dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![Entry {
                key: EntryKey {
                    dirname: b"".to_vec(),
                    basename: b"".to_vec(),
                    file_id: b"a-root-value".to_vec(),
                },
                trees: vec![stat_current.clone(), stat_parent_dir.clone()],
            }],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: vec![
                Entry {
                    key: EntryKey {
                        dirname: b"".to_vec(),
                        basename: b"a".to_vec(),
                        file_id: b"a-dir".to_vec(),
                    },
                    trees: vec![stat_current.clone(), stat_parent_dir.clone()],
                },
                Entry {
                    key: EntryKey {
                        dirname: b"".to_vec(),
                        basename: b"b".to_vec(),
                        file_id: b"b-dir".to_vec(),
                    },
                    trees: vec![stat_current.clone(), stat_parent_dir.clone()],
                },
                Entry {
                    key: EntryKey {
                        dirname: b"".to_vec(),
                        basename: b"c".to_vec(),
                        file_id: b"c-file".to_vec(),
                    },
                    trees: vec![file_cur(10), relocated(b"b/j")],
                },
                Entry {
                    key: EntryKey {
                        dirname: b"".to_vec(),
                        basename: b"d".to_vec(),
                        file_id: b"d-file".to_vec(),
                    },
                    trees: vec![file_cur(20), file_parent(b"d", 20)],
                },
            ],
        },
        Dirblock {
            dirname: b"a".to_vec(),
            entries: vec![
                Entry {
                    key: EntryKey {
                        dirname: b"a".to_vec(),
                        basename: b"e".to_vec(),
                        file_id: b"e-dir".to_vec(),
                    },
                    trees: vec![stat_current.clone(), stat_parent_dir.clone()],
                },
                Entry {
                    key: EntryKey {
                        dirname: b"a".to_vec(),
                        basename: b"f".to_vec(),
                        file_id: b"f-file".to_vec(),
                    },
                    trees: vec![file_cur(30), file_parent(b"f", 20)],
                },
            ],
        },
        Dirblock {
            dirname: b"b".to_vec(),
            entries: vec![
                Entry {
                    key: EntryKey {
                        dirname: b"b".to_vec(),
                        basename: b"g".to_vec(),
                        file_id: b"g-file".to_vec(),
                    },
                    trees: vec![file_cur(30), null_parent.clone()],
                },
                Entry {
                    key: EntryKey {
                        dirname: b"b".to_vec(),
                        basename: b"h\xc3\xa5".to_vec(),
                        file_id: b"h-\xc3\xa5-file1".to_vec(),
                    },
                    trees: vec![file_cur(40), null_parent.clone()],
                },
                Entry {
                    key: EntryKey {
                        dirname: b"b".to_vec(),
                        basename: b"h\xc3\xa5".to_vec(),
                        file_id: b"h-\xc3\xa5-file2".to_vec(),
                    },
                    trees: vec![null_parent.clone(), file_parent(b"h", 20)],
                },
                Entry {
                    key: EntryKey {
                        dirname: b"b".to_vec(),
                        basename: b"i".to_vec(),
                        file_id: b"i-file".to_vec(),
                    },
                    trees: vec![null_parent.clone(), file_parent(b"h", 20)],
                },
                Entry {
                    key: EntryKey {
                        dirname: b"b".to_vec(),
                        basename: b"j".to_vec(),
                        file_id: b"c-file".to_vec(),
                    },
                    trees: vec![relocated(b"c"), file_parent(b"j", 20)],
                },
            ],
        },
    ];
    state
}

/// Rust counterpart of Python
/// `TestIterChildEntries.test_iter_children_b`. Walks the b/
/// subtree in tree_index=1 (the parent revision) and expects to
/// see the live entries h2, i, and j (in that order).
#[test]
fn iter_child_entries_children_b_tree_one() {
    let state = create_dirstate_with_two_trees();
    let children = state.iter_child_entries(1, b"b");
    let basenames: Vec<&[u8]> = children.iter().map(|e| e.key.basename.as_slice()).collect();
    let file_ids: Vec<&[u8]> = children.iter().map(|e| e.key.file_id.as_slice()).collect();
    // h2 and i share the basename "h\xc3\xa5" and "i"; distinguish
    // by file id so the test pins the exact row.
    assert_eq!(basenames, vec![&b"h\xc3\xa5"[..], b"i", b"j"]);
    assert_eq!(
        file_ids,
        vec![&b"h-\xc3\xa5-file2"[..], b"i-file", b"c-file"]
    );
}

/// Rust counterpart of Python
/// `TestIterChildEntries.test_iter_child_root`. Walks the whole
/// tree in tree_index=1 and expects: a, b, d (c is relocated so
/// absent from this tree), then e, f from a/, then h2, i, j from
/// b/.
#[test]
fn iter_child_entries_root_tree_one() {
    let state = create_dirstate_with_two_trees();
    let children = state.iter_child_entries(1, b"");
    let basenames: Vec<&[u8]> = children.iter().map(|e| e.key.basename.as_slice()).collect();
    let expected: Vec<&[u8]> = vec![b"a", b"b", b"d", b"e", b"f", b"h\xc3\xa5", b"i", b"j"];
    assert_eq!(basenames, expected);
}

#[test]
fn iter_child_entries_non_directory_returns_empty() {
    let state = create_complex_dirstate();
    // "c" is a file, not a directory — iter_child_entries of a
    // non-directory path yields nothing.
    let children = state.iter_child_entries(0, b"c");
    assert!(children.is_empty());
}

#[test]
fn split_path_utf8_matches_osutils_split() {
    assert_eq!(split_path_utf8(b"a/b/c"), (&b"a/b"[..], &b"c"[..]));
    assert_eq!(split_path_utf8(b"a"), (&b""[..], &b"a"[..]));
    assert_eq!(split_path_utf8(b""), (&b""[..], &b""[..]));
    assert_eq!(split_path_utf8(b"a/"), (&b"a"[..], &b""[..]));
}

/// Rust counterpart of Python
/// `TestGetEntry.test_simple_structure`. Probe a small dirstate by
/// path and verify the expected (dirname, basename, file_id) key
/// comes back — or `None` for paths that don't exist or live under
/// a non-existent directory.
#[test]
fn maybe_remove_row_keeps_row_with_any_live_tree() {
    let mut entries = vec![
        entry_with_trees(
            b"",
            b"a",
            b"fid-a",
            vec![tree(Kind::File), tree(Kind::Absent)],
        ),
        entry_with_trees(
            b"",
            b"b",
            b"fid-b",
            vec![tree(Kind::Absent), tree(Kind::Absent)],
        ),
    ];
    let mut id_index = IdIndex::new();
    let fid_a = FileId::from(&b"fid-a".to_vec());
    id_index.add((b"", b"a", &fid_a));

    let removed = DirState::maybe_remove_row(&mut entries, 0, &mut id_index);
    assert!(!removed);
    assert_eq!(entries.len(), 2);
    assert_eq!(id_index.get(&fid_a).len(), 1);
}

#[test]
fn maybe_remove_row_drops_row_when_all_trees_dead() {
    let mut entries = vec![
        entry_with_trees(b"", b"a", b"fid-a", vec![tree(Kind::File)]),
        entry_with_trees(
            b"",
            b"b",
            b"fid-b",
            vec![tree(Kind::Absent), tree(Kind::Relocated)],
        ),
    ];
    let mut id_index = IdIndex::new();
    let fid_a = FileId::from(&b"fid-a".to_vec());
    let fid_b = FileId::from(&b"fid-b".to_vec());
    id_index.add((b"", b"a", &fid_a));
    id_index.add((b"", b"b", &fid_b));

    let removed = DirState::maybe_remove_row(&mut entries, 1, &mut id_index);
    assert!(removed);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key.file_id, b"fid-a".to_vec());
    // fid_b was dropped from the index.
    assert!(id_index.get(&fid_b).is_empty());
    // fid_a is still indexed.
    assert_eq!(id_index.get(&fid_a).len(), 1);
}

#[test]
fn sort_entries_orders_by_dirname_basename_file_id() {
    // Shuffled input covering root, shallow file, nested file, and
    // deeper nested file; we expect canonical dirblock order on the
    // way out.
    let mut entries = vec![
        make_entry(b"a", b"e", b"fid-e"),
        make_entry(b"", b"", b"TREE_ROOT"),
        make_entry(b"b", b"g", b"fid-g"),
        make_entry(b"", b"a", b"fid-a"),
        make_entry(b"a", b"f", b"fid-f"),
    ];
    DirState::sort_entries(&mut entries);
    let keys: Vec<(&[u8], &[u8], &[u8])> = entries
        .iter()
        .map(|e| {
            (
                e.key.dirname.as_slice(),
                e.key.basename.as_slice(),
                e.key.file_id.as_slice(),
            )
        })
        .collect();
    assert_eq!(
        keys,
        vec![
            (b"".as_slice(), b"".as_slice(), b"TREE_ROOT".as_slice()),
            (b"".as_slice(), b"a".as_slice(), b"fid-a".as_slice()),
            (b"a".as_slice(), b"e".as_slice(), b"fid-e".as_slice()),
            (b"a".as_slice(), b"f".as_slice(), b"fid-f".as_slice()),
            (b"b".as_slice(), b"g".as_slice(), b"fid-g".as_slice()),
        ]
    );
}

#[test]
fn sort_entries_breaks_basename_ties_on_file_id() {
    // Same (dirname, basename) with different file ids.
    let mut entries = vec![
        make_entry(b"a", b"e", b"fid-z"),
        make_entry(b"a", b"e", b"fid-a"),
    ];
    DirState::sort_entries(&mut entries);
    assert_eq!(entries[0].key.file_id, b"fid-a".to_vec());
    assert_eq!(entries[1].key.file_id, b"fid-z".to_vec());
}

#[test]
fn sort_entries_split_ordering_differs_from_raw_bytes() {
    // Python's `_sort_entries` splits the dirname on '/' before
    // comparing, which is the whole point of the port: a purely
    // byte-wise sort would put `"a-b"` before `"a/..."` because
    // `'-' < '/'`, while the split-based sort puts them *after*
    // every entry under `"a"`.
    let mut entries = vec![
        make_entry(b"a-b", b"x", b"fid-x"),
        make_entry(b"a/c", b"y", b"fid-y"),
        make_entry(b"a", b"z", b"fid-z"),
    ];
    DirState::sort_entries(&mut entries);
    let dirnames: Vec<&[u8]> = entries.iter().map(|e| e.key.dirname.as_slice()).collect();
    assert_eq!(
        dirnames,
        vec![b"a".as_slice(), b"a/c".as_slice(), b"a-b".as_slice()]
    );
}

#[test]
fn entries_for_path_returns_all_rows_at_path() {
    let state = create_complex_dirstate();
    let rows = state.entries_for_path(b"a/e");
    assert!(!rows.is_empty());
    for row in &rows {
        assert_eq!(row.key.dirname, b"a".to_vec());
        assert_eq!(row.key.basename, b"e".to_vec());
    }
    // At least the direct entry should be present.
    assert!(rows.iter().any(|r| r.key.file_id == b"e-dir".to_vec()));
}

#[test]
fn entries_for_path_root_returns_root_row() {
    let state = create_complex_dirstate();
    let rows = state.entries_for_path(b"");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].key.dirname, b"".to_vec());
    assert_eq!(rows[0].key.basename, b"".to_vec());
    assert_eq!(rows[0].key.file_id, b"a-root-value".to_vec());
}

#[test]
fn entries_for_path_missing_directory_returns_empty() {
    let state = create_complex_dirstate();
    assert!(state.entries_for_path(b"nosuchdir/nope").is_empty());
}

#[test]
fn entries_for_path_missing_basename_returns_empty() {
    let state = create_complex_dirstate();
    assert!(state.entries_for_path(b"a/nope").is_empty());
}

#[test]
fn get_entry_by_path_simple_structure() {
    let state = create_dirstate_with_root_and_subdir();
    let root = state.get_entry_by_path(0, b"").expect("root");
    assert_eq!(root.key.file_id, b"a-root-value".to_vec());
    let subdir = state.get_entry_by_path(0, b"subdir").expect("subdir");
    assert_eq!(subdir.key.basename, b"subdir".to_vec());
    assert_eq!(subdir.key.file_id, b"subdir-id".to_vec());
    assert!(state.get_entry_by_path(0, b"missing").is_none());
    assert!(state.get_entry_by_path(0, b"missing/foo").is_none());
    assert!(state.get_entry_by_path(0, b"subdir/foo").is_none());
}

/// Rust counterpart of Python
/// `TestGetEntry.test_complex_structure_exists`.
#[test]
fn get_entry_by_path_complex_structure_exists() {
    let state = create_complex_dirstate();
    let cases: &[(&[u8], &[u8], &[u8], &[u8])] = &[
        (b"", b"", b"", b"a-root-value"),
        (b"a", b"", b"a", b"a-dir"),
        (b"b", b"", b"b", b"b-dir"),
        (b"c", b"", b"c", b"c-file"),
        (b"d", b"", b"d", b"d-file"),
        (b"a/e", b"a", b"e", b"e-dir"),
        (b"a/f", b"a", b"f", b"f-file"),
        (b"b/g", b"b", b"g", b"g-file"),
        (b"b/h\xc3\xa5", b"b", b"h\xc3\xa5", b"h-\xc3\xa5-file"),
    ];
    for (path, dirname, basename, file_id) in cases {
        let entry = state
            .get_entry_by_path(0, path)
            .unwrap_or_else(|| panic!("expected entry at {:?}", path));
        assert_eq!(
            entry.key.dirname,
            dirname.to_vec(),
            "dirname for {:?}",
            path
        );
        assert_eq!(
            entry.key.basename,
            basename.to_vec(),
            "basename for {:?}",
            path
        );
        assert_eq!(
            entry.key.file_id,
            file_id.to_vec(),
            "file_id for {:?}",
            path
        );
    }
}

#[test]
fn iter_entries_yields_every_entry_across_blocks() {
    let state = create_complex_dirstate();
    let entries: Vec<&[u8]> = state
        .iter_entries()
        .map(|e| e.key.file_id.as_slice())
        .collect();
    // Expected order: root, then root contents (a, b, c, d), then
    // the a/ block (e, f), then the b/ block (g, h\xc3\xa5).
    let expected: Vec<&[u8]> = vec![
        b"a-root-value",
        b"a-dir",
        b"b-dir",
        b"c-file",
        b"d-file",
        b"e-dir",
        b"f-file",
        b"g-file",
        b"h-\xc3\xa5-file",
    ];
    assert_eq!(entries, expected);
}

#[test]
fn build_id_index_maps_every_file_id_to_its_key() {
    let state = create_complex_dirstate();
    let idx = state.build_id_index();
    // Every file_id in the complex dirstate should round-trip
    // through the index to the same (dirname, basename) triple.
    let expected: &[(&[u8], &[u8], &[u8])] = &[
        (b"a-root-value", b"", b""),
        (b"a-dir", b"", b"a"),
        (b"b-dir", b"", b"b"),
        (b"c-file", b"", b"c"),
        (b"d-file", b"", b"d"),
        (b"e-dir", b"a", b"e"),
        (b"f-file", b"a", b"f"),
        (b"g-file", b"b", b"g"),
        (b"h-\xc3\xa5-file", b"b", b"h\xc3\xa5"),
    ];
    for (file_id, dirname, basename) in expected {
        let got = idx.get(&FileId::from(&file_id.to_vec()));
        assert_eq!(got.len(), 1, "expected one entry for {:?}", file_id);
        assert_eq!(got[0].0, dirname.to_vec());
        assert_eq!(got[0].1, basename.to_vec());
        assert_eq!(got[0].2.as_bytes(), *file_id);
    }
}

#[test]
fn build_id_index_collapses_duplicate_file_ids_across_trees() {
    // The two-tree fixture has two rows with the same file_id in
    // different trees (c-file appears as the c/ entry in tree 0
    // and the b/j relocation in tree 1). Both rows share the same
    // file_id and should appear under the same id_index bucket.
    let state = create_dirstate_with_two_trees();
    let idx = state.build_id_index();
    let c_file_entries = idx.get(&FileId::from(&b"c-file".to_vec()));
    // Two rows share the file_id across the dirstate (c and b/j).
    assert_eq!(c_file_entries.len(), 2);
    let basenames: Vec<&[u8]> = c_file_entries
        .iter()
        .map(|(_, b, _)| b.as_slice())
        .collect();
    assert!(basenames.contains(&&b"c"[..]));
    assert!(basenames.contains(&&b"j"[..]));
}

#[test]
fn get_entry_by_file_id_direct_hit_in_tree_zero() {
    let mut state = create_complex_dirstate();
    let result = state.get_entry_by_file_id(0, b"c-file", false);
    match result {
        GetEntryResult::Entry(key) => {
            assert_eq!(key.dirname, b"".to_vec());
            assert_eq!(key.basename, b"c".to_vec());
            assert_eq!(key.file_id, b"c-file".to_vec());
        }
        other => panic!("expected Entry, got {:?}", other),
    }
}

#[test]
fn get_entry_by_file_id_follows_relocation_chain() {
    // In create_dirstate_with_two_trees, c-file's row at (b"", b"c")
    // is relocated in tree 1 → b/j. The id_index should find the
    // (b"b", b"j") variant on the second pass and return it.
    let mut state = create_dirstate_with_two_trees();
    let result = state.get_entry_by_file_id(1, b"c-file", false);
    match result {
        GetEntryResult::Entry(key) => {
            assert_eq!(key.dirname, b"b".to_vec());
            assert_eq!(key.basename, b"j".to_vec());
            assert_eq!(key.file_id, b"c-file".to_vec());
        }
        other => panic!("expected Entry, got {:?}", other),
    }
}

#[test]
fn get_entry_by_file_id_not_found_for_unknown_id() {
    let mut state = create_complex_dirstate();
    assert_eq!(
        state.get_entry_by_file_id(0, b"nonexistent", false),
        GetEntryResult::NotFound
    );
}

#[test]
fn get_entry_by_file_id_absent_without_include_deleted() {
    // g-file is absent in tree 1 (null_parent). Without
    // include_deleted the lookup returns NotFound.
    let mut state = create_dirstate_with_two_trees();
    assert_eq!(
        state.get_entry_by_file_id(1, b"g-file", false),
        GetEntryResult::NotFound
    );
}

#[test]
fn get_entry_by_file_id_absent_with_include_deleted() {
    // Same g-file/tree 1 lookup, but with include_deleted we get
    // the absent entry back.
    let mut state = create_dirstate_with_two_trees();
    match state.get_entry_by_file_id(1, b"g-file", true) {
        GetEntryResult::Entry(key) => {
            assert_eq!(key.basename, b"g".to_vec());
            assert_eq!(key.file_id, b"g-file".to_vec());
        }
        other => panic!("expected Entry, got {:?}", other),
    }
}

/// Python-style `present_dir` tuple: `(b"d", b"", 0, False, NULLSTAT)`.
fn dmp_present_dir() -> TreeData {
    TreeData {
        minikind: Kind::Directory,
        fingerprint: Vec::new(),
        size: 0,
        executable: false,
        packed_stat: PACKED_STAT.to_vec(),
    }
}

/// Python-style `present_file` tuple: `(b"f", b"", 0, False, NULLSTAT)`.
fn dmp_present_file() -> TreeData {
    TreeData {
        minikind: Kind::File,
        fingerprint: Vec::new(),
        size: 0,
        executable: false,
        packed_stat: PACKED_STAT.to_vec(),
    }
}

/// Python-style `NULL_PARENT_DETAILS`: `(b"a", b"", 0, False, b"")`.
fn dmp_absent() -> TreeData {
    TreeData {
        minikind: Kind::Absent,
        fingerprint: Vec::new(),
        size: 0,
        executable: false,
        packed_stat: Vec::new(),
    }
}

/// Python-style relocation target tree data:
/// `(b"r", real_path, 0, False, b"")`.
fn dmp_relocated(real_path: &[u8]) -> TreeData {
    TreeData {
        minikind: Kind::Relocated,
        fingerprint: real_path.to_vec(),
        size: 0,
        executable: false,
        packed_stat: Vec::new(),
    }
}

fn mk_entry(dirname: &[u8], basename: &[u8], file_id: &[u8], trees: Vec<TreeData>) -> Entry {
    Entry {
        key: EntryKey {
            dirname: dirname.to_vec(),
            basename: basename.to_vec(),
            file_id: file_id.to_vec(),
        },
        trees,
    }
}

/// Rust counterpart of Python
/// `TestDiscardMergeParents.test_discard_no_parents`: no-op on an
/// empty dirstate.
#[test]
fn discard_merge_parents_no_parents_is_noop() {
    let mut state = fresh_state();
    state.dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: Vec::new(),
        },
        Dirblock {
            dirname: Vec::new(),
            entries: Vec::new(),
        },
    ];
    state.discard_merge_parents();
    assert!(state.parents.is_empty());
    assert!(state.ghosts.is_empty());
    assert_eq!(state.dirblocks.len(), 2);
}

/// Rust counterpart of Python
/// `TestDiscardMergeParents.test_discard_one_parent`: with exactly
/// one parent there is nothing beyond tree 1 to discard, but the
/// method still runs and leaves dirblocks unchanged.
#[test]
fn discard_merge_parents_one_parent_is_noop_on_dirblocks() {
    let mut state = fresh_state();
    state.parents = vec![b"parent-id".to_vec()];
    let original_dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![mk_entry(
                b"",
                b"",
                b"a-root-value",
                vec![dmp_present_dir(), dmp_present_dir()],
            )],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: Vec::new(),
        },
    ];
    state.dirblocks = original_dirblocks.clone();
    state.discard_merge_parents();
    assert_eq!(state.parents, vec![b"parent-id".to_vec()]);
    assert_eq!(state.dirblocks, original_dirblocks);
}

/// Rust counterpart of Python
/// `TestDiscardMergeParents.test_discard_simple`: three trees per
/// row collapse to two, dropping the merged parent column.
#[test]
fn discard_merge_parents_strips_merge_column() {
    let mut state = fresh_state();
    state.parents = vec![b"parent-id".to_vec(), b"merged-id".to_vec()];
    state.dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![mk_entry(
                b"",
                b"",
                b"a-root-value",
                vec![dmp_present_dir(), dmp_present_dir(), dmp_present_dir()],
            )],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: Vec::new(),
        },
    ];
    state.discard_merge_parents();
    assert_eq!(state.parents, vec![b"parent-id".to_vec()]);
    assert_eq!(state.dirblocks[0].entries.len(), 1);
    assert_eq!(state.dirblocks[0].entries[0].trees.len(), 2);
    assert_eq!(state.dirblocks[1].entries.len(), 0);
}

/// Rust counterpart of Python
/// `TestDiscardMergeParents.test_discard_absent`: a row that only
/// exists in the merge parent (absent in tree 0 and 1) is removed
/// entirely.
#[test]
fn discard_merge_parents_removes_absent_only_rows() {
    let mut state = fresh_state();
    state.parents = vec![b"parent-id".to_vec(), b"merged-id".to_vec()];
    state.dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![mk_entry(
                b"",
                b"",
                b"a-root-value",
                vec![dmp_present_dir(), dmp_present_dir(), dmp_present_dir()],
            )],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: vec![
                mk_entry(
                    b"",
                    b"file-in-merged",
                    b"b-file-id",
                    vec![dmp_absent(), dmp_absent(), dmp_present_file()],
                ),
                mk_entry(
                    b"",
                    b"file-in-root",
                    b"a-file-id",
                    vec![dmp_present_file(), dmp_present_file(), dmp_present_file()],
                ),
            ],
        },
    ];
    state.discard_merge_parents();
    // file-in-merged was only in the merge tree — dropped.
    assert_eq!(state.dirblocks[1].entries.len(), 1);
    assert_eq!(
        state.dirblocks[1].entries[0].key.basename,
        b"file-in-root".to_vec()
    );
    assert_eq!(state.dirblocks[1].entries[0].trees.len(), 2);
}

/// Rust counterpart of Python
/// `TestDiscardMergeParents.test_discard_renamed`: rows whose
/// tree 0 / tree 1 kinds are `(a, r)`, `(r, a)`, or `(r, r)` are
/// removed; rows that still have a live kind in one of the first
/// two trees survive with their columns truncated.
#[test]
fn discard_merge_parents_removes_dead_relocation_rows() {
    let mut state = fresh_state();
    state.parents = vec![b"parent-id".to_vec(), b"merged-id".to_vec()];
    state.dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![mk_entry(
                b"",
                b"",
                b"a-root-value",
                vec![dmp_present_dir(), dmp_present_dir(), dmp_present_dir()],
            )],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: vec![
                // (absent, present, r) — tree0/tree1 = (a, f). Lives on.
                mk_entry(
                    b"",
                    b"file-in-1",
                    b"c-file-id",
                    vec![
                        dmp_absent(),
                        dmp_present_file(),
                        dmp_relocated(b"file-in-2"),
                    ],
                ),
                // (absent, r, present) — tree0/tree1 = (a, r). Dead.
                mk_entry(
                    b"",
                    b"file-in-2",
                    b"c-file-id",
                    vec![
                        dmp_absent(),
                        dmp_relocated(b"file-in-1"),
                        dmp_present_file(),
                    ],
                ),
                // normal file — tree0/tree1 = (f, f). Lives on.
                mk_entry(
                    b"",
                    b"file-in-root",
                    b"a-file-id",
                    vec![dmp_present_file(), dmp_present_file(), dmp_present_file()],
                ),
                // (r, absent, present) — tree0/tree1 = (r, a). Dead.
                mk_entry(
                    b"",
                    b"file-s",
                    b"b-file-id",
                    vec![dmp_relocated(b"file-t"), dmp_absent(), dmp_present_file()],
                ),
                // (present, absent, r) — tree0/tree1 = (f, a). Lives on.
                mk_entry(
                    b"",
                    b"file-t",
                    b"b-file-id",
                    vec![dmp_present_file(), dmp_absent(), dmp_relocated(b"file-s")],
                ),
            ],
        },
    ];
    state.discard_merge_parents();

    let surviving: Vec<&[u8]> = state.dirblocks[1]
        .entries
        .iter()
        .map(|e| e.key.basename.as_slice())
        .collect();
    assert_eq!(
        surviving,
        vec![&b"file-in-1"[..], b"file-in-root", b"file-t"]
    );
    for entry in &state.dirblocks[1].entries {
        assert_eq!(entry.trees.len(), 2, "{:?}", entry.key.basename);
    }
}

/// Rust counterpart of Python
/// `TestDiscardMergeParents.test_discard_all_subdir`: a whole
/// block of merge-only children is emptied (but the block itself
/// remains).
#[test]
fn discard_merge_parents_empties_block_of_merge_only_children() {
    let mut state = fresh_state();
    state.parents = vec![b"parent-id".to_vec(), b"merged-id".to_vec()];
    state.dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![mk_entry(
                b"",
                b"",
                b"a-root-value",
                vec![dmp_present_dir(), dmp_present_dir(), dmp_present_dir()],
            )],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: vec![mk_entry(
                b"",
                b"sub",
                b"dir-id",
                vec![dmp_present_dir(), dmp_present_dir(), dmp_present_dir()],
            )],
        },
        Dirblock {
            dirname: b"sub".to_vec(),
            entries: vec![
                mk_entry(
                    b"sub",
                    b"child1",
                    b"child1-id",
                    vec![dmp_absent(), dmp_absent(), dmp_present_file()],
                ),
                mk_entry(
                    b"sub",
                    b"child2",
                    b"child2-id",
                    vec![dmp_absent(), dmp_absent(), dmp_present_file()],
                ),
                mk_entry(
                    b"sub",
                    b"child3",
                    b"child3-id",
                    vec![dmp_absent(), dmp_absent(), dmp_present_file()],
                ),
            ],
        },
    ];
    state.discard_merge_parents();
    assert_eq!(state.dirblocks.len(), 3);
    assert_eq!(state.dirblocks[0].entries.len(), 1);
    assert_eq!(state.dirblocks[1].entries.len(), 1);
    assert_eq!(state.dirblocks[2].dirname, b"sub".to_vec());
    assert!(
        state.dirblocks[2].entries.is_empty(),
        "all children were merge-only and should have been removed"
    );
    // Each surviving entry has exactly two trees.
    for block in &state.dirblocks {
        for entry in &block.entries {
            assert_eq!(entry.trees.len(), 2);
        }
    }
}

/// Ghost parent path: the first parent is in the ghosts list, so
/// every surviving row gets `NULL_PARENT_DETAILS` in slot 1
/// rather than slot 1's real data. Python documents this
/// behaviour via the `entry[1][1:] = empty_parent` branch.
#[test]
fn discard_merge_parents_ghost_first_parent_replaces_with_null_parent() {
    let mut state = fresh_state();
    state.parents = vec![b"parent-id".to_vec(), b"merged-id".to_vec()];
    state.ghosts = vec![b"parent-id".to_vec()];
    state.dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![mk_entry(
                b"",
                b"",
                b"a-root-value",
                vec![dmp_present_dir(), dmp_present_dir(), dmp_present_dir()],
            )],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: Vec::new(),
        },
    ];
    state.discard_merge_parents();
    // First parent is kept but the tree-1 slot is replaced with
    // a NULL_PARENT_DETAILS (minikind b'a').
    assert_eq!(state.parents, vec![b"parent-id".to_vec()]);
    assert!(state.ghosts.is_empty());
    let root = &state.dirblocks[0].entries[0];
    assert_eq!(root.trees.len(), 2);
    assert_eq!(root.trees[0].minikind, Kind::Directory);
    assert_eq!(root.trees[1].minikind, Kind::Absent);
    assert!(root.trees[1].fingerprint.is_empty());
    assert!(root.trees[1].packed_stat.is_empty());
}

/// A tree-row builder that lets tests set a non-default fingerprint
/// on a single row — needed to exercise the relocation branch of
/// `make_absent`, which uses the fingerprint as the target path.
fn tree_with_fingerprint(minikind: Kind, fingerprint: &[u8]) -> TreeData {
    TreeData {
        minikind,
        fingerprint: fingerprint.to_vec(),
        size: 0,
        executable: false,
        packed_stat: b"x".repeat(32),
    }
}

/// Build a minimal dirblocks layout containing the two empty
/// sentinel blocks plus a single dirblock named `dir` with the
/// supplied entries. The `make_absent` tests only need this
/// shape; richer structure goes through `create_complex_dirstate`.
fn absent_fixture(entries: Vec<Entry>) -> DirState {
    let mut state = fresh_state();
    state.parents = vec![b"parent-id".to_vec(), b"merged-id".to_vec()];
    state.dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: Vec::new(),
        },
        Dirblock {
            dirname: Vec::new(),
            entries: Vec::new(),
        },
        Dirblock {
            dirname: b"dir".to_vec(),
            entries,
        },
    ];
    state
}

/// The entry has no trees beyond tree 0, so marking it absent is
/// the last reference — the row is removed from its block and
/// dropped from the id_index.
#[test]
fn make_absent_removes_last_reference_and_updates_id_index() {
    let mut state = absent_fixture(vec![entry_with_trees(
        b"dir",
        b"a",
        b"fid-a",
        vec![tree(Kind::File)],
    )]);
    // Prime the id_index cache.
    let fid = FileId::from(&b"fid-a".to_vec());
    state.get_or_build_id_index();
    assert_eq!(state.id_index.as_ref().unwrap().get(&fid).len(), 1);

    let last_reference = state
        .make_absent(&EntryKey {
            dirname: b"dir".to_vec(),
            basename: b"a".to_vec(),
            file_id: b"fid-a".to_vec(),
        })
        .expect("make_absent");
    assert!(last_reference);
    assert!(state.dirblocks[2].entries.is_empty());
    assert!(state.id_index.as_ref().unwrap().get(&fid).is_empty());
    assert_eq!(state.dirblock_state, MemoryState::InMemoryModified);
}

/// A merge-parent row keeps the entry alive at its current key,
/// so `make_absent` does not remove it — instead it sets tree 0
/// to NULL_PARENT_DETAILS and leaves the block populated.
#[test]
fn make_absent_sets_tree0_absent_when_other_trees_keep_entry() {
    let mut state = absent_fixture(vec![entry_with_trees(
        b"dir",
        b"a",
        b"fid-a",
        vec![tree(Kind::File), tree(Kind::Directory)],
    )]);

    let last_reference = state
        .make_absent(&EntryKey {
            dirname: b"dir".to_vec(),
            basename: b"a".to_vec(),
            file_id: b"fid-a".to_vec(),
        })
        .expect("make_absent");
    assert!(!last_reference);
    assert_eq!(state.dirblocks[2].entries.len(), 1);
    let entry = &state.dirblocks[2].entries[0];
    assert_eq!(entry.trees[0].minikind, Kind::Absent);
    assert!(entry.trees[0].fingerprint.is_empty());
    assert!(entry.trees[0].packed_stat.is_empty());
    assert_eq!(entry.trees[1].minikind, Kind::Directory);
}

/// A relocated parent row promotes the relocation target to a
/// remaining-key, whose tree 0 slot must get set to absent too.
/// The original entry is removed (last_reference=true).
#[test]
fn make_absent_follows_relocation_and_updates_target() {
    // Two entries:
    //   - dir/a: tree 0 present, tree 1 relocation → dir/b
    //   - dir/b: tree 0 present, tree 1 present (so make_absent
    //     against dir/a wipes dir/a and sets dir/b's tree 0 to a).
    let mut state = absent_fixture(vec![
        entry_with_trees(
            b"dir",
            b"a",
            b"fid-a",
            vec![
                tree(Kind::File),
                tree_with_fingerprint(Kind::Relocated, b"dir/b"),
            ],
        ),
        entry_with_trees(
            b"dir",
            b"b",
            b"fid-a",
            vec![tree(Kind::File), tree(Kind::File)],
        ),
    ]);
    // Prime the id_index; make_absent should remove dir/a from it.
    let fid = FileId::from(&b"fid-a".to_vec());
    state.get_or_build_id_index();
    assert_eq!(state.id_index.as_ref().unwrap().get(&fid).len(), 2);

    let last_reference = state
        .make_absent(&EntryKey {
            dirname: b"dir".to_vec(),
            basename: b"a".to_vec(),
            file_id: b"fid-a".to_vec(),
        })
        .expect("make_absent");
    assert!(last_reference);
    // dir/a is gone.
    assert_eq!(state.dirblocks[2].entries.len(), 1);
    assert_eq!(state.dirblocks[2].entries[0].key.basename, b"b".to_vec());
    // dir/b's tree 0 flipped to absent; tree 1 stayed intact.
    let survivor = &state.dirblocks[2].entries[0];
    assert_eq!(survivor.trees[0].minikind, Kind::Absent);
    assert_eq!(survivor.trees[1].minikind, Kind::File);
    assert_eq!(state.id_index.as_ref().unwrap().get(&fid).len(), 1);
}

/// An absent-parent row contributes nothing to remaining-keys —
/// still a last_reference, still removes the entry, but makes no
/// tree-0 updates elsewhere.
#[test]
fn make_absent_absent_parent_row_is_ignored() {
    let mut state = absent_fixture(vec![entry_with_trees(
        b"dir",
        b"a",
        b"fid-a",
        vec![tree(Kind::File), tree(Kind::Absent)],
    )]);
    let last_reference = state
        .make_absent(&EntryKey {
            dirname: b"dir".to_vec(),
            basename: b"a".to_vec(),
            file_id: b"fid-a".to_vec(),
        })
        .expect("make_absent");
    assert!(last_reference);
    assert!(state.dirblocks[2].entries.is_empty());
}

#[test]
fn packed_stat_index_only_contains_file_entries() {
    // Mix of file, directory, symlink, absent, relocated: only
    // `f` entries should make it into the index, keyed by their
    // packed_stat and valued by their fingerprint (sha1).
    let mut state = fresh_state();
    let f1 = TreeData {
        minikind: Kind::File,
        fingerprint: b"sha-f1".to_vec(),
        size: 10,
        executable: false,
        packed_stat: b"stat-f1".to_vec(),
    };
    let f2 = TreeData {
        minikind: Kind::File,
        fingerprint: b"sha-f2".to_vec(),
        size: 20,
        executable: true,
        packed_stat: b"stat-f2".to_vec(),
    };
    let dir = tree(Kind::Directory);
    let symlink = TreeData {
        minikind: Kind::Symlink,
        fingerprint: b"target".to_vec(),
        size: 0,
        executable: false,
        packed_stat: b"stat-l".to_vec(),
    };
    state.dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![entry_with_trees(b"", b"", b"TREE_ROOT", vec![dir.clone()])],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: vec![
                entry_with_trees(b"", b"a", b"fid-a", vec![f1.clone()]),
                entry_with_trees(b"", b"b", b"fid-b", vec![f2.clone()]),
                entry_with_trees(b"", b"c", b"fid-c", vec![symlink]),
                entry_with_trees(b"", b"d", b"fid-d", vec![tree(Kind::Absent)]),
            ],
        },
    ];
    let idx = state.build_packed_stat_index();
    assert_eq!(idx.len(), 2);
    assert_eq!(idx.get(&b"stat-f1".to_vec()).unwrap(), &b"sha-f1".to_vec());
    assert_eq!(idx.get(&b"stat-f2".to_vec()).unwrap(), &b"sha-f2".to_vec());
}

#[test]
fn get_or_build_packed_stat_index_caches_result() {
    let mut state = fresh_state();
    state.dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![entry_with_trees(
                b"",
                b"",
                b"TREE_ROOT",
                vec![tree(Kind::Directory)],
            )],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: vec![entry_with_trees(
                b"",
                b"a",
                b"fid-a",
                vec![TreeData {
                    minikind: Kind::File,
                    fingerprint: b"sha-a".to_vec(),
                    size: 1,
                    executable: false,
                    packed_stat: b"stat-a".to_vec(),
                }],
            )],
        },
    ];
    assert!(state.packed_stat_index.is_none());
    let idx = state.get_or_build_packed_stat_index();
    assert_eq!(idx.len(), 1);
    // Second call returns the cached map — structural equality
    // since we can't easily compare references without breaking
    // the borrow.
    assert!(state.packed_stat_index.is_some());
    let idx_again = state.get_or_build_packed_stat_index();
    assert_eq!(idx_again.len(), 1);
}

#[test]
fn packed_stat_index_invalidated_by_set_data() {
    let mut state = fresh_state();
    state.packed_stat_index = Some(HashMap::new());
    state.set_data(Vec::new(), Vec::new());
    assert!(state.packed_stat_index.is_none());
}

#[test]
fn packed_stat_index_invalidated_by_wipe_state() {
    let mut state = fresh_state();
    state.packed_stat_index = Some(HashMap::new());
    state.wipe_state();
    assert!(state.packed_stat_index.is_none());
}

/// Missing entry raises EntryNotFound — Python's corresponding
/// AssertionError.
#[test]
fn make_absent_missing_entry_returns_error() {
    let mut state = absent_fixture(vec![entry_with_trees(
        b"dir",
        b"a",
        b"fid-a",
        vec![tree(Kind::File)],
    )]);
    let err = state
        .make_absent(&EntryKey {
            dirname: b"dir".to_vec(),
            basename: b"ghost".to_vec(),
            file_id: b"fid-ghost".to_vec(),
        })
        .unwrap_err();
    assert!(matches!(err, MakeAbsentError::EntryNotFound { .. }));
}

/// A minimal root-only dirblock layout, used to test `add` paths
/// that insert new entries directly at the root.
fn add_fixture() -> DirState {
    let mut state = fresh_state();
    state.parents = vec![];
    state.dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![Entry {
                key: EntryKey {
                    dirname: Vec::new(),
                    basename: Vec::new(),
                    file_id: b"TREE_ROOT".to_vec(),
                },
                trees: vec![TreeData {
                    minikind: Kind::Directory,
                    fingerprint: Vec::new(),
                    size: 0,
                    executable: false,
                    packed_stat: b"x".repeat(32),
                }],
            }],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: Vec::new(),
        },
    ];
    state
}

#[test]
fn add_inserts_new_file_at_root() {
    let mut state = add_fixture();
    let stat = b"x".repeat(32);
    state
        .add(
            b"a",
            b"",
            b"a",
            b"fid-a",
            osutils::Kind::File,
            7,
            &stat,
            b"sha1",
        )
        .expect("add");
    // Root-contents block (index 1) now has one entry.
    assert_eq!(state.dirblocks[1].entries.len(), 1);
    let entry = &state.dirblocks[1].entries[0];
    assert_eq!(entry.key.basename, b"a");
    assert_eq!(entry.key.file_id, b"fid-a");
    assert_eq!(entry.trees[0].minikind, Kind::File);
    assert_eq!(entry.trees[0].size, 7);
    assert_eq!(entry.trees[0].fingerprint, b"sha1");
}

#[test]
fn add_directory_creates_child_block() {
    let mut state = add_fixture();
    let stat = b"x".repeat(32);
    state
        .add(
            b"sub",
            b"",
            b"sub",
            b"fid-sub",
            osutils::Kind::Directory,
            0,
            &stat,
            b"",
        )
        .expect("add");
    // A new block for the directory 'sub' should now exist.
    let block_names: Vec<&[u8]> = state
        .dirblocks
        .iter()
        .map(|b| b.dirname.as_slice())
        .collect();
    assert!(block_names.contains(&b"sub".as_slice()));
}

#[test]
fn add_duplicate_file_id_errors() {
    let mut state = add_fixture();
    let stat = b"x".repeat(32);
    state
        .add(
            b"a",
            b"",
            b"a",
            b"fid-a",
            osutils::Kind::File,
            1,
            &stat,
            b"",
        )
        .expect("first add");
    let err = state
        .add(
            b"b",
            b"",
            b"b",
            b"fid-a",
            osutils::Kind::File,
            1,
            &stat,
            b"",
        )
        .unwrap_err();
    assert!(matches!(err, AddError::DuplicateFileId { .. }));
}

#[test]
fn add_second_path_same_basename_errors() {
    let mut state = add_fixture();
    let stat = b"x".repeat(32);
    state
        .add(
            b"a",
            b"",
            b"a",
            b"fid-a",
            osutils::Kind::File,
            1,
            &stat,
            b"",
        )
        .expect("first add");
    let err = state
        .add(
            b"a",
            b"",
            b"a",
            b"fid-other",
            osutils::Kind::File,
            1,
            &stat,
            b"",
        )
        .unwrap_err();
    assert!(matches!(err, AddError::AlreadyAdded { .. }));
}

#[test]
fn add_parent_missing_errors_not_versioned() {
    let mut state = add_fixture();
    let stat = b"x".repeat(32);
    // There is no block for 'missing', and its parent ('') has no
    // entry named 'missing' at tree 0.
    let err = state
        .add(
            b"missing/child",
            b"missing",
            b"child",
            b"fid-c",
            osutils::Kind::File,
            0,
            &stat,
            b"",
        )
        .unwrap_err();
    assert!(matches!(err, AddError::NotVersioned { .. }));
}

#[test]
fn set_path_id_rejects_non_root_path() {
    let mut state = add_fixture();
    let err = state.set_path_id(b"foo", b"new-id").unwrap_err();
    assert!(matches!(err, SetPathIdError::NonRootPath));
}

#[test]
fn set_path_id_unchanged_id_is_noop() {
    let mut state = add_fixture();
    let before = state.dirblocks.clone();
    state.set_path_id(b"", b"TREE_ROOT").expect("same-id noop");
    assert_eq!(state.dirblocks, before);
}

#[test]
fn set_path_id_rewrites_root_and_preserves_packed_stat() {
    let mut state = add_fixture();
    // The root row's packed_stat is `b"x".repeat(32)` per
    // add_fixture; no parent trees keep the entry alive, so the
    // new row should carry the same packed_stat.
    let original_packed_stat = state.dirblocks[0].entries[0].trees[0].packed_stat.clone();
    state.set_path_id(b"", b"new-id").expect("set_path_id");
    assert_eq!(state.dirblocks[0].entries.len(), 1);
    let new_root = &state.dirblocks[0].entries[0];
    assert_eq!(new_root.key.file_id, b"new-id");
    assert_eq!(new_root.trees[0].minikind, Kind::Directory);
    assert_eq!(new_root.trees[0].packed_stat, original_packed_stat);
}

#[test]
fn set_state_from_inventory_rename_same_id_bug_395556() {
    // Regression for the bug395556 scenario: start with root + 'b'
    // (file-id b-id); then rename b -> a in the inventory.  After
    // the second set_state_from_inventory the dirstate should hold
    // root + 'a' (file-id b-id) with no stale 'b' row.
    let mut state = add_fixture();
    let stat = b"x".repeat(32);
    state
        .add(b"b", b"", b"b", b"b-id", osutils::Kind::File, 0, &stat, b"")
        .expect("add");

    let inv_after_rename: Vec<(Vec<u8>, Vec<u8>, Kind, Vec<u8>, bool)> = vec![
        (
            Vec::new(),
            b"TREE_ROOT".to_vec(),
            Kind::Directory,
            Vec::new(),
            false,
        ),
        (
            b"a".to_vec(),
            b"b-id".to_vec(),
            Kind::File,
            Vec::new(),
            false,
        ),
    ];
    state
        .set_state_from_inventory(inv_after_rename)
        .expect("set_state_from_inventory");

    // Expect: root row, then 'a' with file_id b-id in the root
    // contents block.  No live 'b' row.
    let mut live_entries = Vec::new();
    for block in &state.dirblocks {
        for entry in &block.entries {
            let t0 = match entry.trees.first().map(|t| t.minikind) {
                Some(k) if !k.is_absent_or_relocated() => k,
                _ => continue,
            };
            live_entries.push((
                entry.key.dirname.clone(),
                entry.key.basename.clone(),
                entry.key.file_id.clone(),
                t0,
            ));
        }
    }
    assert_eq!(
        live_entries,
        vec![
            (
                Vec::new(),
                Vec::new(),
                b"TREE_ROOT".to_vec(),
                Kind::Directory
            ),
            (Vec::new(), b"a".to_vec(), b"b-id".to_vec(), Kind::File),
        ]
    );
}

#[test]
fn walkdirs_utf8_visits_depth_first() {
    // Build a fake filesystem: /root + children (a [file], b [dir with b/c file], f [file])
    let mut t = MemoryTransport::new();
    let stat_dir = StatInfo {
        mode: 0o040755,
        size: 0,
        mtime: 0,
        ctime: 0,
        dev: 1,
        ino: 1,
    };
    let stat_file = StatInfo {
        mode: 0o100644,
        size: 3,
        mtime: 0,
        ctime: 0,
        dev: 1,
        ino: 2,
    };
    t.set_fs(b"", stat_dir, None);
    t.set_fs(b"a", stat_file, None);
    t.set_fs(b"b", stat_dir, None);
    t.set_fs(b"b/c", stat_file, None);
    t.set_fs(b"f", stat_file, None);

    let mut walker = WalkDirsUtf8::new(b"", b"");
    let mut visited: Vec<Vec<u8>> = Vec::new();
    while walker
        .next_dir(&t, |_rel, abspath, _entries| {
            visited.push(abspath.to_vec());
        })
        .unwrap()
    {}
    assert_eq!(
        visited,
        vec![b"".to_vec(), b"b".to_vec()],
        "expected only directories visited in depth-first order"
    );
}

#[test]
fn walkdirs_utf8_skips_pruned_subdirectories() {
    // Same tree but callback removes `b` from the dirblock, so
    // the walk never recurses into it.
    let mut t = MemoryTransport::new();
    let stat_dir = StatInfo {
        mode: 0o040755,
        size: 0,
        mtime: 0,
        ctime: 0,
        dev: 1,
        ino: 1,
    };
    let stat_file = StatInfo {
        mode: 0o100644,
        size: 3,
        mtime: 0,
        ctime: 0,
        dev: 1,
        ino: 2,
    };
    t.set_fs(b"", stat_dir, None);
    t.set_fs(b"b", stat_dir, None);
    t.set_fs(b"b/c", stat_file, None);

    let mut walker = WalkDirsUtf8::new(b"", b"");
    let mut visited: Vec<Vec<u8>> = Vec::new();
    while walker
        .next_dir(&t, |_rel, abspath, entries| {
            visited.push(abspath.to_vec());
            entries.retain(|e| e.basename != b"b");
        })
        .unwrap()
    {}
    assert_eq!(visited, vec![b"".to_vec()], "pruned dir should not recurse");
}

#[test]
fn walkdirs_utf8_depth_first_across_siblings() {
    // Root contains two sibling dirs `a` and `a-b`.  The walker
    // should visit `a`, recurse into `a/b`, then visit `a-b` —
    // i.e. depth-first in byte-sorted order.  Regression for a
    // pending-stack reversal that flipped sibling order after
    // the first level.
    let mut t = MemoryTransport::new();
    let dir = StatInfo {
        mode: 0o040755,
        size: 0,
        mtime: 0,
        ctime: 0,
        dev: 1,
        ino: 1,
    };
    let file = StatInfo {
        mode: 0o100644,
        size: 0,
        mtime: 0,
        ctime: 0,
        dev: 1,
        ino: 2,
    };
    t.set_fs(b"", dir, None);
    t.set_fs(b"a", dir, None);
    t.set_fs(b"a/b", dir, None);
    t.set_fs(b"a/b/foo", file, None);
    t.set_fs(b"a-b", dir, None);
    t.set_fs(b"a-b/bar", file, None);

    let mut walker = WalkDirsUtf8::new(b"", b"");
    let mut visited: Vec<Vec<u8>> = Vec::new();
    while walker
        .next_dir(&t, |rel, _abs, _entries| {
            visited.push(rel.to_vec());
        })
        .unwrap()
    {}
    assert_eq!(
        visited,
        vec![
            b"".to_vec(),
            b"a".to_vec(),
            b"a/b".to_vec(),
            b"a-b".to_vec(),
        ]
    );
}

#[test]
fn iter_changes_next_emits_unversioned_files() {
    // In-memory filesystem with a single unversioned file at root;
    // empty dirstate; want_unversioned=true.  The iterator should
    // yield exactly one change (for the unversioned file).
    let mut t = MemoryTransport::new();
    let stat_dir = StatInfo {
        mode: 0o040755,
        size: 0,
        mtime: 0,
        ctime: 0,
        dev: 1,
        ino: 1,
    };
    let stat_file = StatInfo {
        mode: 0o100644,
        size: 5,
        mtime: 0,
        ctime: 0,
        dev: 1,
        ino: 2,
    };
    t.set_fs(b"", stat_dir, None);
    t.set_fs(b"a", stat_file, None);

    let mut state = add_fixture();
    let mut pstate = ProcessEntryState {
        source_index: None,
        target_index: 0,
        include_unchanged: false,
        want_unversioned: true,
        partial: false,
        supports_tree_reference: false,
        root_abspath: Vec::new(),
        searched_specific_files: std::collections::HashSet::new(),
        search_specific_files: std::collections::HashSet::from([Vec::new()]),
        search_specific_file_parents: std::collections::HashSet::new(),
        searched_exact_paths: std::collections::HashSet::new(),
        seen_ids: std::collections::HashSet::new(),
        new_dirname_to_file_id: std::collections::HashMap::new(),
        old_dirname_to_file_id: std::collections::HashMap::new(),
        last_source_parent: None,
        last_target_parent: None,
    };
    let mut iter = IterChangesIter::new();
    let mut changes = Vec::new();
    loop {
        match state.iter_changes_next(&mut iter, &mut pstate, &t).unwrap() {
            Some(c) => changes.push(c),
            None => break,
        }
    }
    // Expect: at least one change for `a` as unversioned.
    let unversioned_for_a = changes
        .iter()
        .any(|c| c.new_path.as_deref() == Some(b"a" as &[u8]) && c.file_id.is_empty());
    assert!(
        unversioned_for_a,
        "expected unversioned-file change for 'a'; got: {:?}",
        changes
    );
}

#[test]
fn update_entry_refreshes_sha_after_content_change() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let fpath = dir.path().join("a-file");
    {
        let mut f = std::fs::File::create(&fpath).unwrap();
        f.write_all(b"first content\n").unwrap();
    }

    let mut state = add_fixture();
    // Give the dirstate a committed parent so update_entry's
    // "stat-cacheable and tree-1 is live" branch runs and the sha
    // actually gets written.  Without a parent the row is still
    // in "initial add" mode and we skip the sha computation.
    state.parents = vec![b"parent-rev".to_vec()];
    state.dirblocks[0].entries[0].trees.push(TreeData {
        minikind: Kind::Directory,
        fingerprint: Vec::new(),
        size: 0,
        executable: false,
        packed_stat: Vec::new(),
    });
    let stat = b"x".repeat(32);
    state
        .add(
            b"a-file",
            b"",
            b"a-file",
            b"file-id",
            osutils::Kind::File,
            0,
            &stat,
            b"",
        )
        .expect("add");
    // The newly-added entry still has tree-1 = absent; make it
    // live so update_entry writes the sha.
    let bei = state.get_block_entry_index(b"", b"a-file", 0);
    state.dirblocks[bei.block_index].entries[bei.entry_index].trees[1] = TreeData {
        minikind: Kind::File,
        fingerprint: b"parent-sha".to_vec(),
        size: 0,
        executable: false,
        packed_stat: Vec::new(),
    };
    // Set cutoff_time so the on-disk stat is considered cacheable.
    state.cutoff_time = Some(i64::MAX);

    let meta = std::fs::symlink_metadata(&fpath).unwrap();
    let stat_info = StatInfo {
        mode: {
            #[cfg(unix)]
            {
                meta.mode()
            }
            #[cfg(not(unix))]
            {
                0o100644
            }
        },
        size: meta.len(),
        mtime: metadata_mtime_secs(&meta),
        ctime: metadata_ctime_secs(&meta),
        dev: {
            #[cfg(unix)]
            {
                meta.dev()
            }
            #[cfg(not(unix))]
            {
                0
            }
        },
        ino: {
            #[cfg(unix)]
            {
                meta.ino()
            }
            #[cfg(not(unix))]
            {
                0
            }
        },
    };
    let key = EntryKey {
        dirname: Vec::new(),
        basename: b"a-file".to_vec(),
        file_id: b"file-id".to_vec(),
    };
    let abspath_bytes = fpath.as_os_str().as_encoded_bytes().to_vec();
    let transport = MemoryTransport::new();
    let result = state
        .update_entry(&key, &abspath_bytes, &stat_info, &transport)
        .expect("update_entry");
    let sha = result.expect("file should yield a sha");
    // Sha of "first content\n".
    assert_eq!(
        std::str::from_utf8(&sha).unwrap(),
        "c0a245ade45b97366321074bb27a39a6ae1dc4fc"
    );
    // Tree-0 row should now carry that same sha.
    let bei = state.get_block_entry_index(b"", b"a-file", 0);
    assert!(bei.path_present);
    let entry = &state.dirblocks[bei.block_index].entries[bei.entry_index];
    assert_eq!(entry.trees[0].fingerprint, sha);
}

#[test]
fn bisect_roundtrips_via_get_lines() {
    // Populate a dirstate, serialise it via get_lines, then bisect
    // the serialised byte stream for a known path.  Exercises the
    // full read pipeline (header, entry row parsing, bisect).
    let mut state = add_fixture();
    let stat = b"x".repeat(32);
    state
        .add(
            b"alpha",
            b"",
            b"alpha",
            b"a-id",
            osutils::Kind::File,
            11,
            &stat,
            b"sha-a",
        )
        .expect("add alpha");
    state
        .add(
            b"bravo",
            b"",
            b"bravo",
            b"b-id",
            osutils::Kind::File,
            22,
            &stat,
            b"sha-b",
        )
        .expect("add bravo");

    let lines = state.get_lines();
    let buf: Vec<u8> = lines.into_iter().flatten().collect();

    // Extract end_of_header just like the Python header reader:
    // it is the byte offset of the NUL right after the fifth
    // newline.  read_header handles that for us.
    let mut reader = DirState::new(
        "/tmp/fake",
        Box::new(DefaultSHA1Provider::new()),
        0,
        true,
        false,
    );
    reader.read_header(&buf).expect("read_header");
    reader.dirblock_state = MemoryState::NotInMemory;

    // Build a read_range closure over the buffer.
    let buf_clone = buf.clone();
    let read_range = move |offset: u64, len: usize| -> Result<Vec<u8>, BisectError> {
        let start = offset as usize;
        let end = std::cmp::min(start + len, buf_clone.len());
        if start > buf_clone.len() {
            return Ok(Vec::new());
        }
        Ok(buf_clone[start..end].to_vec())
    };

    let file_size = buf.len() as u64;
    let found = reader
        .bisect(vec![b"bravo".to_vec()], file_size, read_range)
        .expect("bisect");
    let bravo = found.get(b"bravo".as_slice()).expect("bravo present");
    assert_eq!(bravo.len(), 1);
    assert_eq!(bravo[0].key.basename, b"bravo");
    assert_eq!(bravo[0].key.file_id, b"b-id");
    assert_eq!(bravo[0].trees[0].size, 22);
    assert_eq!(bravo[0].trees[0].fingerprint, b"sha-b");
}

#[test]
fn set_parent_trees_simple_case() {
    // Start from a tree with root + 'a-file' in tree-0.
    let mut state = add_fixture();
    let stat = b"x".repeat(32);
    state
        .add(
            b"a-file",
            b"",
            b"a-file",
            b"file-id",
            osutils::Kind::File,
            0,
            &stat,
            b"",
        )
        .expect("add");

    // One non-ghost parent tree that contains the same entries but with
    // different details (simulating a committed revision).
    let details_root = TreeData {
        minikind: Kind::Directory,
        fingerprint: Vec::new(),
        size: 0,
        executable: false,
        packed_stat: b"rev1".to_vec(),
    };
    let details_file = TreeData {
        minikind: Kind::File,
        fingerprint: b"sha1-parent".to_vec(),
        size: 42,
        executable: false,
        packed_stat: b"rev1".to_vec(),
    };
    let parent_entries = vec![
        (Vec::new(), b"TREE_ROOT".to_vec(), details_root.clone()),
        (
            b"a-file".to_vec(),
            b"file-id".to_vec(),
            details_file.clone(),
        ),
    ];

    state
        .set_parent_trees(vec![b"rev1".to_vec()], vec![], vec![parent_entries])
        .expect("set_parent_trees");

    // Root row should have tree-0 (directory) and tree-1 = details_root.
    let bei = get_block_entry_index(&state.dirblocks, b"", b"", 0);
    assert!(bei.path_present);
    let root = &state.dirblocks[bei.block_index].entries[bei.entry_index];
    assert_eq!(root.trees.len(), 2);
    assert_eq!(root.trees[1], details_root);

    // a-file row should have tree-0 (file) and tree-1 = details_file.
    let bei = get_block_entry_index(&state.dirblocks, b"", b"a-file", 0);
    assert!(bei.path_present);
    let file_entry = &state.dirblocks[bei.block_index].entries[bei.entry_index];
    assert_eq!(file_entry.trees.len(), 2);
    assert_eq!(file_entry.trees[1], details_file);

    assert_eq!(state.parents, vec![b"rev1".to_vec()]);
    assert!(state.ghosts.is_empty());
}

#[test]
fn set_parent_trees_ghost_parent_has_no_entries() {
    // Ghost parents occupy a tree slot but contribute no entries.
    let mut state = add_fixture();
    let stat = b"x".repeat(32);
    state
        .add(b"x", b"", b"x", b"x-id", osutils::Kind::File, 0, &stat, b"")
        .expect("add");

    state
        .set_parent_trees(
            vec![b"ghost-rev".to_vec()],
            vec![b"ghost-rev".to_vec()],
            vec![], // no non-ghost parent trees
        )
        .expect("set_parent_trees");

    // Only one tree slot (tree-0) per entry since there are no
    // non-ghost parents.
    for block in &state.dirblocks {
        for entry in &block.entries {
            assert_eq!(entry.trees.len(), 1);
        }
    }
    assert_eq!(state.parents, vec![b"ghost-rev".to_vec()]);
    assert_eq!(state.ghosts, vec![b"ghost-rev".to_vec()]);
}

#[test]
fn set_parent_trees_cross_path_relocation() {
    // Parent tree has file-id at a different path than tree-0.
    // Expect a relocation pointer in the new row.
    let mut state = add_fixture();
    let stat = b"x".repeat(32);
    state
        .add(
            b"new-path",
            b"",
            b"new-path",
            b"fid",
            osutils::Kind::File,
            0,
            &stat,
            b"",
        )
        .expect("add");

    let root_details = TreeData {
        minikind: Kind::Directory,
        fingerprint: Vec::new(),
        size: 0,
        executable: false,
        packed_stat: b"rev".to_vec(),
    };
    let file_details = TreeData {
        minikind: Kind::File,
        fingerprint: b"old-sha".to_vec(),
        size: 7,
        executable: false,
        packed_stat: b"rev".to_vec(),
    };
    let parent_entries = vec![
        (Vec::new(), b"TREE_ROOT".to_vec(), root_details),
        (b"old-path".to_vec(), b"fid".to_vec(), file_details.clone()),
    ];

    state
        .set_parent_trees(vec![b"rev".to_vec()], vec![], vec![parent_entries])
        .expect("set_parent_trees");

    // New path still has tree-0 (file) and tree-1 now holds a
    // relocation pointer to old-path.
    let bei = get_block_entry_index(&state.dirblocks, b"", b"new-path", 0);
    assert!(bei.path_present);
    let new_entry = &state.dirblocks[bei.block_index].entries[bei.entry_index];
    assert_eq!(new_entry.trees[1].minikind, Kind::Relocated);
    assert_eq!(new_entry.trees[1].fingerprint, b"old-path");

    // old-path exists as a row with tree-0 = relocation to new-path
    // and tree-1 = the actual parent-tree details.
    let bei = get_block_entry_index(&state.dirblocks, b"", b"old-path", 1);
    assert!(bei.path_present);
    let old_entry = &state.dirblocks[bei.block_index].entries[bei.entry_index];
    assert_eq!(old_entry.trees[0].minikind, Kind::Relocated);
    assert_eq!(old_entry.trees[0].fingerprint, b"new-path");
    assert_eq!(old_entry.trees[1], file_details);
}

#[test]
fn set_path_id_zeroes_packed_stat_when_parents_retain_entry() {
    let mut state = add_fixture();
    // Add a parent tree that still references the root row.
    state.parents = vec![b"parent-rev".to_vec()];
    state.dirblocks[0].entries[0].trees.push(TreeData {
        minikind: Kind::Directory,
        fingerprint: Vec::new(),
        size: 0,
        executable: false,
        packed_stat: Vec::new(),
    });
    state.set_path_id(b"", b"new-id").expect("set_path_id");
    let new_root = state
        .dirblocks
        .iter()
        .flat_map(|b| b.entries.iter())
        .find(|e| e.key.file_id == b"new-id")
        .expect("new root entry");
    // With parents holding the old row alive, Python's in-place
    // mutation produced an empty packed_stat on the replacement.
    assert_eq!(new_root.trees[0].packed_stat, b"");
}

/// Build a TreeData that looks like Python's
/// `(minikind, fingerprint, size, executable, packed_stat)`
/// tuple — more convenient than the raw `tree()` helper when a
/// test needs to set specific size/fingerprint fields.
fn basis_details(minikind: Kind, fingerprint: &[u8], size: u64, executable: bool) -> TreeData {
    TreeData {
        minikind,
        fingerprint: fingerprint.to_vec(),
        size,
        executable,
        packed_stat: b"x".repeat(32),
    }
}

fn null_parent_details() -> TreeData {
    TreeData {
        minikind: Kind::Absent,
        fingerprint: Vec::new(),
        size: 0,
        executable: false,
        packed_stat: Vec::new(),
    }
}

/// Build a minimal two-tree dirstate populated with a single
/// file at `b""/README` in tree 0 and NULL_PARENT_DETAILS in
/// tree 1. Suitable for exercising the "insert new add" path of
/// `update_basis_apply_adds`.
fn basis_adds_fixture_one_file() -> DirState {
    let mut state = fresh_state();
    state.parents = vec![b"parent-id".to_vec()];
    let tree0 = basis_details(Kind::File, b"sha-r", 10, false);
    let tree1 = null_parent_details();
    state.dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![entry_with_trees(
                b"",
                b"",
                b"TREE_ROOT",
                vec![tree(Kind::Directory), tree(Kind::Directory)],
            )],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: vec![entry_with_trees(
                b"",
                b"README",
                b"fid-readme",
                vec![tree0, tree1],
            )],
        },
    ];
    state
}

#[test]
fn update_basis_apply_adds_inserts_new_entry() {
    // Add a brand new file at b"" / b"a.txt". The block for the
    // root contents already exists; the entry does not. ASCII
    // ordering places b"README" before b"a.txt" (0x52 < 0x61), so
    // the new entry lands after README.
    let mut state = basis_adds_fixture_one_file();
    let mut adds = vec![BasisAdd {
        old_path: None,
        new_path: b"a.txt".to_vec(),
        file_id: b"fid-a".to_vec(),
        new_details: basis_details(Kind::File, b"sha-a", 7, false),
        real_add: true,
    }];
    state.update_basis_apply_adds(&mut adds).expect("apply");

    let block = &state.dirblocks[1];
    assert_eq!(block.entries.len(), 2);
    assert_eq!(block.entries[0].key.basename, b"README".to_vec());
    assert_eq!(block.entries[1].key.basename, b"a.txt".to_vec());
    assert_eq!(block.entries[1].trees[0].minikind, Kind::Absent);
    assert_eq!(block.entries[1].trees[1].minikind, Kind::File);
    assert_eq!(block.entries[1].trees[1].fingerprint, b"sha-a".to_vec());
}

#[test]
fn update_basis_apply_adds_updates_absent_tree1_slot_in_place() {
    // README already exists with tree1=absent; adding the same
    // entry fills in tree 1 instead of inserting a new row.
    let mut state = basis_adds_fixture_one_file();
    let mut adds = vec![BasisAdd {
        old_path: None,
        new_path: b"README".to_vec(),
        file_id: b"fid-readme".to_vec(),
        new_details: basis_details(Kind::File, b"sha-updated", 42, true),
        real_add: true,
    }];
    state.update_basis_apply_adds(&mut adds).expect("apply");

    let block = &state.dirblocks[1];
    assert_eq!(block.entries.len(), 1);
    assert_eq!(
        block.entries[0].trees[1].fingerprint,
        b"sha-updated".to_vec()
    );
    assert_eq!(block.entries[0].trees[1].size, 42);
    assert!(block.entries[0].trees[1].executable);
}

#[test]
fn update_basis_apply_adds_conflicting_existing_basis_is_invalid() {
    // README already has tree1 populated with a live file entry;
    // trying to add a new entry at the same path flags it as
    // InconsistentDelta rather than silently overwriting.
    let mut state = basis_adds_fixture_one_file();
    state.dirblocks[1].entries[0].trees[1] =
        basis_details(Kind::File, b"sha-existing", 11, false);

    let mut adds = vec![BasisAdd {
        old_path: None,
        new_path: b"README".to_vec(),
        file_id: b"fid-readme".to_vec(),
        new_details: basis_details(Kind::File, b"sha-new", 22, false),
        real_add: true,
    }];
    let err = state.update_basis_apply_adds(&mut adds).unwrap_err();
    match err {
        BasisApplyError::Invalid { path, reason, .. } => {
            assert_eq!(path, b"README".to_vec());
            assert!(
                reason.contains("basis target already existed"),
                "{}",
                reason
            );
        }
        other => panic!("expected Invalid, got {:?}", other),
    }
}

#[test]
fn update_basis_apply_adds_real_add_with_old_path_is_invalid() {
    let mut state = basis_adds_fixture_one_file();
    let mut adds = vec![BasisAdd {
        old_path: Some(b"some/old".to_vec()),
        new_path: b"new.txt".to_vec(),
        file_id: b"fid-new".to_vec(),
        new_details: basis_details(Kind::File, b"sha", 0, false),
        real_add: true,
    }];
    let err = state.update_basis_apply_adds(&mut adds).unwrap_err();
    assert!(matches!(err, BasisApplyError::Invalid { .. }));
}

#[test]
fn update_basis_apply_changes_updates_existing_tree1_slot() {
    // README exists in both trees 0 and 1. The change records a
    // new tree-1 value; tree 0 is left alone.
    let mut state = fresh_state();
    state.parents = vec![b"parent-id".to_vec()];
    let tree0 = basis_details(Kind::File, b"sha-r", 10, false);
    let tree1 = basis_details(Kind::File, b"sha-old", 10, false);
    state.dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![entry_with_trees(
                b"",
                b"",
                b"TREE_ROOT",
                vec![tree(Kind::Directory), tree(Kind::Directory)],
            )],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: vec![entry_with_trees(
                b"",
                b"README",
                b"fid-readme",
                vec![tree0.clone(), tree1],
            )],
        },
    ];

    let new_details = basis_details(Kind::File, b"sha-updated", 99, true);
    let changes = vec![(
        b"README".to_vec(),
        b"README".to_vec(),
        b"fid-readme".to_vec(),
        new_details.clone(),
    )];
    state
        .update_basis_apply_changes(&changes)
        .expect("apply_changes");

    let entry = &state.dirblocks[1].entries[0];
    assert_eq!(entry.trees[0].fingerprint, b"sha-r".to_vec());
    assert_eq!(entry.trees[1].fingerprint, b"sha-updated".to_vec());
    assert_eq!(entry.trees[1].size, 99);
    assert!(entry.trees[1].executable);
}

#[test]
fn update_basis_apply_changes_absent_entry_is_invalid() {
    let mut state = basis_adds_fixture_one_file();
    // README's tree-1 is absent in the fixture; a change targeting it
    // is inconsistent.
    let changes = vec![(
        b"README".to_vec(),
        b"README".to_vec(),
        b"fid-readme".to_vec(),
        basis_details(Kind::File, b"sha", 1, false),
    )];
    let err = state.update_basis_apply_changes(&changes).unwrap_err();
    assert!(matches!(err, BasisApplyError::Invalid { .. }));
}

#[test]
fn update_basis_apply_deletes_removes_row_when_active_also_absent() {
    // README has tree 0 absent and tree 1 live; a real_delete
    // should drop the row entirely.
    let mut state = fresh_state();
    state.parents = vec![b"parent-id".to_vec()];
    let t0 = null_parent_details();
    let t1 = basis_details(Kind::File, b"sha", 1, false);
    state.dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![entry_with_trees(
                b"",
                b"",
                b"TREE_ROOT",
                vec![tree(Kind::Directory), tree(Kind::Directory)],
            )],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: vec![entry_with_trees(
                b"",
                b"README",
                b"fid-readme",
                vec![t0, t1],
            )],
        },
    ];

    let deletes = vec![(
        b"README".to_vec(),
        None::<Vec<u8>>,
        b"fid-readme".to_vec(),
        true,
    )];
    state
        .update_basis_apply_deletes(&deletes)
        .expect("apply_deletes");
    assert!(state.dirblocks[1].entries.is_empty());
}

#[test]
fn update_basis_apply_deletes_keeps_row_when_active_still_present() {
    // README has tree 0 live and tree 1 live; a non-real-delete
    // (split rename) should nullify tree 1 but keep the row.
    let mut state = basis_adds_fixture_one_file();
    state.dirblocks[1].entries[0].trees[1] = basis_details(Kind::File, b"sha-old", 10, false);

    let deletes = vec![(
        b"README".to_vec(),
        Some(b"README.new".to_vec()),
        b"fid-readme".to_vec(),
        false,
    )];
    state
        .update_basis_apply_deletes(&deletes)
        .expect("apply_deletes");
    let entry = &state.dirblocks[1].entries[0];
    assert_eq!(entry.trees[1].minikind, Kind::Absent);
    assert!(entry.trees[1].fingerprint.is_empty());
}

#[test]
fn update_basis_apply_deletes_bad_delta_is_invalid() {
    let mut state = basis_adds_fixture_one_file();
    // real_delete=true but new_path=Some — inconsistent.
    let deletes = vec![(
        b"README".to_vec(),
        Some(b"README.new".to_vec()),
        b"fid-readme".to_vec(),
        true,
    )];
    let err = state.update_basis_apply_deletes(&deletes).unwrap_err();
    match err {
        BasisApplyError::Invalid { reason, .. } => {
            assert!(reason.contains("bad delete delta"), "{}", reason);
        }
        other => panic!("expected Invalid, got {:?}", other),
    }
}

#[test]
fn update_basis_apply_deletes_missing_entry_is_invalid() {
    let mut state = basis_adds_fixture_one_file();
    let deletes = vec![(
        b"ghost".to_vec(),
        None::<Vec<u8>>,
        b"fid-ghost".to_vec(),
        true,
    )];
    let err = state.update_basis_apply_deletes(&deletes).unwrap_err();
    match err {
        BasisApplyError::Invalid { reason, .. } => {
            assert!(reason.contains("basis tree does not contain"), "{}", reason);
        }
        other => panic!("expected Invalid, got {:?}", other),
    }
}

#[test]
fn update_basis_apply_adds_sorts_input_so_parents_come_first() {
    // Feed the adds out of order and confirm the function still
    // processes them correctly. We add `dir/` (a directory) and
    // then `dir/child`; the sort must place `dir/` first so the
    // directory block exists by the time the child is processed.
    let mut state = fresh_state();
    state.parents = vec![b"parent-id".to_vec()];
    state.dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![entry_with_trees(
                b"",
                b"",
                b"TREE_ROOT",
                vec![tree(Kind::Directory), tree(Kind::Directory)],
            )],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: Vec::new(),
        },
    ];

    let mut adds = vec![
        BasisAdd {
            old_path: None,
            new_path: b"dir/child".to_vec(),
            file_id: b"fid-child".to_vec(),
            new_details: basis_details(Kind::File, b"sha-c", 1, false),
            real_add: true,
        },
        BasisAdd {
            old_path: None,
            new_path: b"dir".to_vec(),
            file_id: b"fid-dir".to_vec(),
            new_details: basis_details(Kind::Directory, b"", 0, false),
            real_add: true,
        },
    ];
    state.update_basis_apply_adds(&mut adds).expect("apply");

    // After the adds: a dirblock for b"dir" exists, the contents-of-root
    // block holds dir, and a dedicated dir block holds dir/child.
    let dirblock_names: Vec<&[u8]> = state
        .dirblocks
        .iter()
        .map(|b| b.dirname.as_slice())
        .collect();
    assert!(dirblock_names.iter().any(|&n| n == b"dir"));
    let dir_block = state
        .dirblocks
        .iter()
        .find(|b| b.dirname == b"dir")
        .unwrap();
    assert_eq!(dir_block.entries.len(), 1);
    assert_eq!(dir_block.entries[0].key.basename, b"child".to_vec());
}

/// Build a fresh single-tree dirstate with a live root entry and
/// an empty contents-of-root block. Suitable for exercising
/// `update_by_delta` adds/removes.
fn one_tree_root_state() -> DirState {
    let mut state = fresh_state();
    state.dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![entry_with_trees(
                b"",
                b"",
                b"TREE_ROOT",
                vec![tree(Kind::Directory)],
            )],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: Vec::new(),
        },
    ];
    state
}

#[test]
fn update_by_delta_add_file_at_root() {
    // Minimal add: one row inserts README under the root.
    let mut state = one_tree_root_state();
    let entries = vec![FlatDeltaEntry {
        old_path: None,
        new_path: Some(b"README".to_vec()),
        file_id: b"fid-r".to_vec(),
        parent_id: Some(b"TREE_ROOT".to_vec()),
        minikind: Kind::File,
        executable: false,
        fingerprint: Vec::new(),
    }];
    state.update_by_delta(entries).expect("update_by_delta");

    let bei = get_block_entry_index(&state.dirblocks, b"", b"README", 0);
    assert!(bei.path_present);
    let entry = &state.dirblocks[bei.block_index].entries[bei.entry_index];
    assert_eq!(entry.trees[0].minikind, Kind::File);
    assert_eq!(entry.key.file_id, b"fid-r".to_vec());
}

#[test]
fn update_by_delta_delete_then_reinsert_different_id_is_rejected() {
    // Adding a file id already present (not part of the
    // simultaneous delete) must fail via check_delta_ids_absent.
    let mut state = one_tree_root_state();
    state.dirblocks[1].entries.push(entry_with_trees(
        b"",
        b"README",
        b"fid-existing",
        vec![tree(Kind::File)],
    ));

    let entries = vec![FlatDeltaEntry {
        old_path: None,
        new_path: Some(b"OTHER".to_vec()),
        file_id: b"fid-existing".to_vec(),
        parent_id: Some(b"TREE_ROOT".to_vec()),
        minikind: Kind::File,
        executable: false,
        fingerprint: Vec::new(),
    }];
    let err = state.update_by_delta(entries).unwrap_err();
    match err {
        BasisApplyError::Invalid { file_id, .. } => {
            assert_eq!(file_id, b"fid-existing".to_vec());
        }
        other => panic!("expected Invalid, got {:?}", other),
    }
}

#[test]
fn update_by_delta_repeated_file_id_is_rejected() {
    // Two delta rows touching the same file_id must fail — this
    // matches Python's "repeated file_id" _raise_invalid branch.
    let mut state = one_tree_root_state();
    let entries = vec![
        FlatDeltaEntry {
            old_path: None,
            new_path: Some(b"a".to_vec()),
            file_id: b"fid-dup".to_vec(),
            parent_id: Some(b"TREE_ROOT".to_vec()),
            minikind: Kind::File,
            executable: false,
            fingerprint: Vec::new(),
        },
        FlatDeltaEntry {
            old_path: None,
            new_path: Some(b"b".to_vec()),
            file_id: b"fid-dup".to_vec(),
            parent_id: Some(b"TREE_ROOT".to_vec()),
            minikind: Kind::File,
            executable: false,
            fingerprint: Vec::new(),
        },
    ];
    let err = state.update_by_delta(entries).unwrap_err();
    match err {
        BasisApplyError::Invalid { reason, .. } => {
            assert_eq!(reason, "repeated file_id");
        }
        other => panic!("expected Invalid, got {:?}", other),
    }
}

#[test]
fn update_by_delta_rename_expands_children() {
    // Rename a/ -> z/ when a/ has a child a/f. After the delta:
    // a/ and a/f should be gone from tree 0, and z/ + z/f should
    // be present.
    let mut state = fresh_state();
    state.dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![entry_with_trees(
                b"",
                b"",
                b"TREE_ROOT",
                vec![tree(Kind::Directory)],
            )],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: vec![entry_with_trees(
                b"",
                b"a",
                b"a-dir",
                vec![tree(Kind::Directory)],
            )],
        },
        Dirblock {
            dirname: b"a".to_vec(),
            entries: vec![entry_with_trees(
                b"a",
                b"f",
                b"f-file",
                vec![tree(Kind::File)],
            )],
        },
    ];

    let entries = vec![FlatDeltaEntry {
        old_path: Some(b"a".to_vec()),
        new_path: Some(b"z".to_vec()),
        file_id: b"a-dir".to_vec(),
        parent_id: Some(b"TREE_ROOT".to_vec()),
        minikind: Kind::Directory,
        executable: false,
        fingerprint: Vec::new(),
    }];
    state.update_by_delta(entries).expect("rename");

    // Old paths are now absent/relocated (make_absent), new paths
    // are present.
    let old_a = get_block_entry_index(&state.dirblocks, b"", b"a", 0);
    assert!(!old_a.path_present, "a should be gone from tree 0");
    let old_af = get_block_entry_index(&state.dirblocks, b"a", b"f", 0);
    assert!(!old_af.path_present, "a/f should be gone from tree 0");

    let new_z = get_block_entry_index(&state.dirblocks, b"", b"z", 0);
    assert!(new_z.path_present, "z should be present in tree 0");
    let new_zf = get_block_entry_index(&state.dirblocks, b"z", b"f", 0);
    assert!(new_zf.path_present, "z/f should be present in tree 0");
    assert_eq!(
        state.dirblocks[new_zf.block_index].entries[new_zf.entry_index]
            .key
            .file_id,
        b"f-file".to_vec()
    );
}

/// Build a two-tree dirstate suitable for `update_basis_by_delta`
/// tests: tree 0 and tree 1 both contain the root and a single
/// README file with different fingerprints.
fn two_tree_basis_state() -> DirState {
    let mut state = fresh_state();
    state.parents = vec![b"old-revid".to_vec()];
    state.dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![entry_with_trees(
                b"",
                b"",
                b"TREE_ROOT",
                vec![tree(Kind::Directory), tree(Kind::Directory)],
            )],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: vec![entry_with_trees(
                b"",
                b"README",
                b"fid-readme",
                vec![
                    basis_details(Kind::File, b"sha-cur", 10, false),
                    basis_details(Kind::File, b"sha-old", 10, false),
                ],
            )],
        },
    ];
    state
}

#[test]
fn update_basis_by_delta_in_place_change() {
    // In-place change of README: keep tree 0 untouched, update
    // tree 1 fingerprint.
    let mut state = two_tree_basis_state();
    let entries = vec![FlatBasisDeltaEntry {
        old_path: Some(b"README".to_vec()),
        new_path: Some(b"README".to_vec()),
        file_id: b"fid-readme".to_vec(),
        parent_id: Some(b"TREE_ROOT".to_vec()),
        details: Some((
            Kind::File,
            b"sha-new".to_vec(),
            20,
            false,
            b"new-revid".to_vec(),
        )),
    }];
    state
        .update_basis_by_delta(entries, b"new-revid".to_vec())
        .expect("update_basis_by_delta");

    let bei = get_block_entry_index(&state.dirblocks, b"", b"README", 1);
    assert!(bei.path_present);
    let entry = &state.dirblocks[bei.block_index].entries[bei.entry_index];
    assert_eq!(entry.trees[1].fingerprint, b"sha-new".to_vec());
    assert_eq!(entry.trees[1].size, 20);
    // Tree 0 is untouched.
    assert_eq!(entry.trees[0].fingerprint, b"sha-cur".to_vec());
}

#[test]
fn update_basis_by_delta_add_new_file() {
    // Add NEWFILE to tree 1 only.
    let mut state = two_tree_basis_state();
    let entries = vec![FlatBasisDeltaEntry {
        old_path: None,
        new_path: Some(b"NEWFILE".to_vec()),
        file_id: b"fid-new".to_vec(),
        parent_id: Some(b"TREE_ROOT".to_vec()),
        details: Some((
            Kind::File,
            b"sha-new".to_vec(),
            5,
            false,
            b"new-revid".to_vec(),
        )),
    }];
    state
        .update_basis_by_delta(entries, b"new-revid".to_vec())
        .expect("update_basis_by_delta");

    let bei = get_block_entry_index(&state.dirblocks, b"", b"NEWFILE", 1);
    assert!(bei.path_present);
    let entry = &state.dirblocks[bei.block_index].entries[bei.entry_index];
    assert_eq!(entry.trees[1].minikind, Kind::File);
    assert_eq!(entry.key.file_id, b"fid-new".to_vec());
}

#[test]
fn update_basis_by_delta_rename_directory_with_child() {
    // Rename a/ -> z/ when a/ has a child a/f in tree 1. After
    // the delta the rename child-expansion must emit add+delete
    // pairs for a/f so that tree 1 ends up with z/ + z/f live
    // and a/ + a/f gone. This exercises the mid-loop
    // apply_deletes drain + iter_child_entries(1, ...) walk.
    let mut state = fresh_state();
    state.parents = vec![b"old-revid".to_vec()];
    state.dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![entry_with_trees(
                b"",
                b"",
                b"TREE_ROOT",
                vec![tree(Kind::Directory), tree(Kind::Directory)],
            )],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: vec![entry_with_trees(
                b"",
                b"a",
                b"a-dir",
                vec![tree(Kind::Directory), tree(Kind::Directory)],
            )],
        },
        Dirblock {
            dirname: b"a".to_vec(),
            entries: vec![entry_with_trees(
                b"a",
                b"f",
                b"f-file",
                vec![
                    basis_details(Kind::File, b"sha-cur-f", 3, false),
                    basis_details(Kind::File, b"sha-old-f", 3, false),
                ],
            )],
        },
    ];

    let entries = vec![FlatBasisDeltaEntry {
        old_path: Some(b"a".to_vec()),
        new_path: Some(b"z".to_vec()),
        file_id: b"a-dir".to_vec(),
        parent_id: Some(b"TREE_ROOT".to_vec()),
        details: Some((Kind::Directory, Vec::new(), 0, false, b"new-revid".to_vec())),
    }];
    state
        .update_basis_by_delta(entries, b"new-revid".to_vec())
        .expect("update_basis_by_delta");

    // Tree 1: a and a/f should no longer be live.
    let old_a = get_block_entry_index(&state.dirblocks, b"", b"a", 1);
    assert!(!old_a.path_present, "a should be gone from tree 1");
    let old_af = get_block_entry_index(&state.dirblocks, b"a", b"f", 1);
    assert!(!old_af.path_present, "a/f should be gone from tree 1");

    // Tree 1: z and z/f should now be live.
    let new_z = get_block_entry_index(&state.dirblocks, b"", b"z", 1);
    assert!(new_z.path_present, "z should be present in tree 1");
    let new_zf = get_block_entry_index(&state.dirblocks, b"z", b"f", 1);
    assert!(new_zf.path_present, "z/f should be present in tree 1");
    assert_eq!(
        state.dirblocks[new_zf.block_index].entries[new_zf.entry_index]
            .key
            .file_id,
        b"f-file".to_vec()
    );
}

#[test]
fn update_basis_by_delta_delete_file() {
    // Delete README from tree 1 (old_path set, new_path None).
    let mut state = two_tree_basis_state();
    let entries = vec![FlatBasisDeltaEntry {
        old_path: Some(b"README".to_vec()),
        new_path: None,
        file_id: b"fid-readme".to_vec(),
        parent_id: None,
        details: None,
    }];
    state
        .update_basis_by_delta(entries, b"new-revid".to_vec())
        .expect("update_basis_by_delta");

    // After delete: tree 1 for README is absent.
    let bei = get_block_entry_index(&state.dirblocks, b"", b"README", 1);
    assert!(!bei.path_present);
}

#[test]
fn get_or_build_id_index_caches_result() {
    let mut state = create_complex_dirstate();
    assert!(state.id_index.is_none());
    state.get_or_build_id_index();
    assert!(state.id_index.is_some());
    // Second call does not rebuild — we can't observe that
    // directly, but we can verify the cache survives by mutating
    // `dirblocks` and re-calling: the cached index should still
    // point to the pre-mutation data. (Invalidation is the
    // caller's responsibility, as Python documents.)
    state.dirblocks.clear();
    let idx_after = state.get_or_build_id_index();
    assert!(
        idx_after
            .get(&FileId::from(&b"a-root-value".to_vec()))
            .iter()
            .any(|(_, _, f)| f.as_bytes() == b"a-root-value"),
        "cache should survive dirblock mutation"
    );
}

/// Rust counterpart of Python
/// `TestGetEntry.test_complex_structure_missing`.
#[test]
fn get_entry_by_path_complex_structure_missing() {
    let state = create_complex_dirstate();
    for path in [&b"_"[..], b"_\xc3\xa5", b"a/b", b"c/d"] {
        assert!(
            state.get_entry_by_path(0, path).is_none(),
            "expected None for {:?}",
            path
        );
    }
}

#[test]
fn get_block_entry_index_simple_structure() {
    let state = create_dirstate_with_root_and_subdir();
    // subdir is present at (1, 0) in the contents-of-root block.
    let bei = state.get_block_entry_index(b"", b"subdir", 0);
    assert_eq!(bei.block_index, 1);
    assert_eq!(bei.entry_index, 0);
    assert!(bei.dir_present);
    assert!(bei.path_present);
    // bdir would sort before subdir — insertion point is still 0,
    // dir_present = true, path_present = false.
    let bei = state.get_block_entry_index(b"", b"bdir", 0);
    assert_eq!(bei.block_index, 1);
    assert_eq!(bei.entry_index, 0);
    assert!(bei.dir_present);
    assert!(!bei.path_present);
    // zdir would sort after subdir — insertion point is 1.
    let bei = state.get_block_entry_index(b"", b"zdir", 0);
    assert_eq!(bei.block_index, 1);
    assert_eq!(bei.entry_index, 1);
    assert!(bei.dir_present);
    assert!(!bei.path_present);
    // Non-existent parent directories — dir_present = false and the
    // block index is where they would be inserted (past the end).
    let bei = state.get_block_entry_index(b"a", b"foo", 0);
    assert_eq!(bei.block_index, 2);
    assert_eq!(bei.entry_index, 0);
    assert!(!bei.dir_present);
    assert!(!bei.path_present);
    let bei = state.get_block_entry_index(b"subdir", b"foo", 0);
    assert_eq!(bei.block_index, 2);
    assert!(!bei.dir_present);
    assert!(!bei.path_present);
}

/// Rust counterpart of Python
/// `TestGetBlockRowIndex.test_complex_structure_exists`.
#[test]
fn get_block_entry_index_complex_structure_exists() {
    let state = create_complex_dirstate();
    // Root: (0, 0, true, true).
    let bei = state.get_block_entry_index(b"", b"", 0);
    assert_eq!(
        (
            bei.block_index,
            bei.entry_index,
            bei.dir_present,
            bei.path_present
        ),
        (0, 0, true, true)
    );
    // Root contents in block 1, each at their own index.
    for (i, basename) in [&b"a"[..], b"b", b"c", b"d"].iter().enumerate() {
        let bei = state.get_block_entry_index(b"", basename, 0);
        assert_eq!(
            (
                bei.block_index,
                bei.entry_index,
                bei.dir_present,
                bei.path_present
            ),
            (1, i, true, true),
            "root/{:?}",
            basename
        );
    }
    // a/e and a/f live in block 2.
    let bei = state.get_block_entry_index(b"a", b"e", 0);
    assert_eq!(
        (
            bei.block_index,
            bei.entry_index,
            bei.dir_present,
            bei.path_present
        ),
        (2, 0, true, true)
    );
    let bei = state.get_block_entry_index(b"a", b"f", 0);
    assert_eq!(
        (
            bei.block_index,
            bei.entry_index,
            bei.dir_present,
            bei.path_present
        ),
        (2, 1, true, true)
    );
    // b/g and b/h\xc3\xa5 live in block 3.
    let bei = state.get_block_entry_index(b"b", b"g", 0);
    assert_eq!(
        (
            bei.block_index,
            bei.entry_index,
            bei.dir_present,
            bei.path_present
        ),
        (3, 0, true, true)
    );
    let bei = state.get_block_entry_index(b"b", b"h\xc3\xa5", 0);
    assert_eq!(
        (
            bei.block_index,
            bei.entry_index,
            bei.dir_present,
            bei.path_present
        ),
        (3, 1, true, true)
    );
}

/// Rust counterpart of Python
/// `TestGetBlockRowIndex.test_complex_structure_missing`. Checks
/// that insertion points match Python's expectations for paths
/// that don't yet exist in the complex dirstate.
#[test]
fn get_block_entry_index_complex_structure_missing() {
    let state = create_complex_dirstate();
    // Root row still present.
    let bei = state.get_block_entry_index(b"", b"", 0);
    assert_eq!(
        (
            bei.block_index,
            bei.entry_index,
            bei.dir_present,
            bei.path_present
        ),
        (0, 0, true, true)
    );
    // "_" sorts before "a" in the contents-of-root block.
    let bei = state.get_block_entry_index(b"", b"_", 0);
    assert_eq!(
        (
            bei.block_index,
            bei.entry_index,
            bei.dir_present,
            bei.path_present
        ),
        (1, 0, true, false)
    );
    // "aa" sorts between "a" (index 0) and "b" (index 1).
    let bei = state.get_block_entry_index(b"", b"aa", 0);
    assert_eq!(
        (
            bei.block_index,
            bei.entry_index,
            bei.dir_present,
            bei.path_present
        ),
        (1, 1, true, false)
    );
    // "h\xc3\xa5" sorts after "d" — insertion point is 4 (end of block).
    let bei = state.get_block_entry_index(b"", b"h\xc3\xa5", 0);
    assert_eq!(
        (
            bei.block_index,
            bei.entry_index,
            bei.dir_present,
            bei.path_present
        ),
        (1, 4, true, false)
    );
    // Directories that don't exist: _, aa, bb.
    let bei = state.get_block_entry_index(b"_", b"a", 0);
    assert_eq!(
        (
            bei.block_index,
            bei.entry_index,
            bei.dir_present,
            bei.path_present
        ),
        (2, 0, false, false)
    );
    let bei = state.get_block_entry_index(b"aa", b"a", 0);
    assert_eq!(
        (
            bei.block_index,
            bei.entry_index,
            bei.dir_present,
            bei.path_present
        ),
        (3, 0, false, false)
    );
    let bei = state.get_block_entry_index(b"bb", b"a", 0);
    assert_eq!(
        (
            bei.block_index,
            bei.entry_index,
            bei.dir_present,
            bei.path_present
        ),
        (4, 0, false, false)
    );
    // "a/e" as a dirname sorts component-wise between "a" (2) and "b" (3).
    let bei = state.get_block_entry_index(b"a/e", b"a", 0);
    assert_eq!(
        (
            bei.block_index,
            bei.entry_index,
            bei.dir_present,
            bei.path_present
        ),
        (3, 0, false, false)
    );
    // "e" comes after "b" — insertion point is 4 (past end).
    let bei = state.get_block_entry_index(b"e", b"a", 0);
    assert_eq!(
        (
            bei.block_index,
            bei.entry_index,
            bei.dir_present,
            bei.path_present
        ),
        (4, 0, false, false)
    );
}

#[test]
fn dirstate_method_wrappers_delegate_to_free_functions() {
    let mut state = DirState::new(
        "dirstate",
        Box::new(DefaultSHA1Provider::new()),
        0,
        true,
        false,
    );
    state.dirblocks = make_dirblocks(vec![(
        b"dir",
        vec![entry_with_trees(
            b"dir",
            b"a",
            b"fid-a",
            vec![tree(Kind::File)],
        )],
    )]);
    let key = EntryKey {
        dirname: b"dir".to_vec(),
        basename: b"a".to_vec(),
        file_id: b"fid-a".to_vec(),
    };
    assert_eq!(state.find_block_index_from_key(&key), (2, true));
    let block = &state.dirblocks[2].entries.clone();
    assert_eq!(state.find_entry_index(&key, block), (0, true));
    let bei = state.get_block_entry_index(b"dir", b"a", 0);
    assert_eq!(bei.block_index, 2);
    assert!(bei.path_present);
}

#[test]
fn entry_to_line_single_tree_matches_expected_layout() {
    let nullstat = b"x".repeat(32);
    let entry = Entry {
        key: EntryKey {
            dirname: b"".to_vec(),
            basename: b"README".to_vec(),
            file_id: b"fid-readme".to_vec(),
        },
        trees: vec![TreeData {
            minikind: Kind::File,
            fingerprint: b"sha1value".to_vec(),
            size: 42,
            executable: true,
            packed_stat: nullstat.clone(),
        }],
    };
    let line = entry_to_line(&entry);
    let mut expected = Vec::new();
    expected.extend_from_slice(b"\x00README\x00fid-readme\x00f\x00sha1value\x0042\x00y\x00");
    expected.extend_from_slice(&nullstat);
    assert_eq!(line, expected);
}

#[test]
fn entry_to_line_multi_tree() {
    let nullstat = b"x".repeat(32);
    let entry = Entry {
        key: EntryKey {
            dirname: b"sub".to_vec(),
            basename: b"f".to_vec(),
            file_id: b"fid".to_vec(),
        },
        trees: vec![
            TreeData {
                minikind: Kind::File,
                fingerprint: b"cur".to_vec(),
                size: 7,
                executable: false,
                packed_stat: nullstat.clone(),
            },
            TreeData {
                minikind: Kind::Absent,
                fingerprint: b"".to_vec(),
                size: 0,
                executable: false,
                packed_stat: nullstat.clone(),
            },
        ],
    };
    let line = entry_to_line(&entry);
    let mut expected = Vec::new();
    expected.extend_from_slice(b"sub\x00f\x00fid\x00f\x00cur\x007\x00n\x00");
    expected.extend_from_slice(&nullstat);
    expected.extend_from_slice(b"\x00a\x00\x000\x00n\x00");
    expected.extend_from_slice(&nullstat);
    assert_eq!(line, expected);
}

/// Rust counterpart of Python
/// `TestGetLines.test_entry_to_line_with_parent`. Root entry with
/// current tree details plus one parent whose "tree data" is the
/// absent-pointer form `(b"a", <relocated-path>, 0, False, b"")`.
#[test]
fn entry_to_line_with_parent_matches_python_bytes() {
    let entry = Entry {
        key: EntryKey {
            dirname: b"".to_vec(),
            basename: b"".to_vec(),
            file_id: b"a-root-value".to_vec(),
        },
        trees: vec![
            TreeData {
                minikind: Kind::Directory,
                fingerprint: Vec::new(),
                size: 0,
                executable: false,
                packed_stat: PACKED_STAT.to_vec(),
            },
            TreeData {
                minikind: Kind::Absent,
                fingerprint: b"dirname/basename".to_vec(),
                size: 0,
                executable: false,
                packed_stat: Vec::new(),
            },
        ],
    };
    let expected: &[u8] = b"\x00\x00a-root-value\x00\
                            d\x00\x000\x00n\x00AAAAREUHaIpFB2iKAAADAQAtkqUAAIGk\x00\
                            a\x00dirname/basename\x000\x00n\x00";
    assert_eq!(entry_to_line(&entry), expected);
}

/// Rust counterpart of Python
/// `TestGetLines.test_entry_to_line_with_two_parents_at_different_paths`.
/// Root entry with current tree details, one parent at the same
/// path, and a second parent whose data is the absent-pointer form
/// pointing at `dirname/basename`.
#[test]
fn entry_to_line_with_two_parents_at_different_paths_matches_python_bytes() {
    let entry = Entry {
        key: EntryKey {
            dirname: b"".to_vec(),
            basename: b"".to_vec(),
            file_id: b"a-root-value".to_vec(),
        },
        trees: vec![
            TreeData {
                minikind: Kind::Directory,
                fingerprint: Vec::new(),
                size: 0,
                executable: false,
                packed_stat: PACKED_STAT.to_vec(),
            },
            TreeData {
                minikind: Kind::Directory,
                fingerprint: Vec::new(),
                size: 0,
                executable: false,
                packed_stat: b"rev_id".to_vec(),
            },
            TreeData {
                minikind: Kind::Absent,
                fingerprint: b"dirname/basename".to_vec(),
                size: 0,
                executable: false,
                packed_stat: Vec::new(),
            },
        ],
    };
    let expected: &[u8] = b"\x00\x00a-root-value\x00\
                            d\x00\x000\x00n\x00AAAAREUHaIpFB2iKAAADAQAtkqUAAIGk\x00\
                            d\x00\x000\x00n\x00rev_id\x00\
                            a\x00dirname/basename\x000\x00n\x00";
    assert_eq!(entry_to_line(&entry), expected);
}

#[test]
fn entry_to_line_round_trip_through_parse_dirblocks() {
    // Build a DirState, serialise it via get_lines, then feed the
    // body back through parse_dirblocks + split_root_dirblock_into_contents
    // and check the dirblocks survive the round-trip.
    let nullstat = b"x".repeat(32);
    let original = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![Entry {
                key: EntryKey {
                    dirname: b"".to_vec(),
                    basename: b"".to_vec(),
                    file_id: b"TREE_ROOT".to_vec(),
                },
                trees: vec![TreeData {
                    minikind: Kind::Directory,
                    fingerprint: Vec::new(),
                    size: 0,
                    executable: false,
                    packed_stat: nullstat.clone(),
                }],
            }],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: vec![Entry {
                key: EntryKey {
                    dirname: b"".to_vec(),
                    basename: b"README".to_vec(),
                    file_id: b"fid-readme".to_vec(),
                },
                trees: vec![TreeData {
                    minikind: Kind::File,
                    fingerprint: b"sha1".to_vec(),
                    size: 10,
                    executable: true,
                    packed_stat: nullstat.clone(),
                }],
            }],
        },
    ];

    let mut state = DirState::new(
        "dirstate",
        Box::new(DefaultSHA1Provider::new()),
        0,
        true,
        false,
    );
    state.dirblocks = original.clone();

    let chunks = state.get_lines();
    let data: Vec<u8> = chunks.into_iter().flatten().collect();

    // Re-parse: two entries → get_output_lines writes num_entries=2.
    let header = read_header(&data).expect("parse header");
    assert_eq!(header.num_entries, 2);
    let body = &data[header.end_of_header..];
    let mut parsed = parse_dirblocks(body, 1, header.num_entries).expect("parse body");
    split_root_dirblock_into_contents(&mut parsed).expect("split");

    assert_eq!(parsed.len(), 2);
    // Block 0: just the root entry.
    assert_eq!(parsed[0].entries.len(), 1);
    assert_eq!(parsed[0].entries[0].key.file_id, b"TREE_ROOT".to_vec());
    // Block 1: the contents-of-root entry.
    assert_eq!(parsed[1].entries.len(), 1);
    assert_eq!(parsed[1].entries[0].key.basename, b"README".to_vec());
    assert_eq!(parsed[1].entries[0].trees[0].size, 10);
    assert!(parsed[1].entries[0].trees[0].executable);
}

#[test]
fn dirstate_get_lines_matches_python_saved_bytes() {
    // The same single-entry layout we pinned earlier for
    // parse_dirblocks, but now produced by the Rust writer and
    // compared byte-for-byte.
    let nullstat = b"x".repeat(32);
    let mut state = DirState::new(
        "dirstate",
        Box::new(DefaultSHA1Provider::new()),
        0,
        true,
        false,
    );
    state.dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![Entry {
                key: EntryKey {
                    dirname: b"".to_vec(),
                    basename: b"".to_vec(),
                    file_id: b"TREE_ROOT".to_vec(),
                },
                trees: vec![TreeData {
                    minikind: Kind::Directory,
                    fingerprint: Vec::new(),
                    size: 0,
                    executable: false,
                    packed_stat: nullstat,
                }],
            }],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: Vec::new(),
        },
    ];
    let chunks = state.get_lines();
    let actual: Vec<u8> = chunks.into_iter().flatten().collect();
    let expected: &[u8] = b"#bazaar dirstate flat format 3\n\
                            crc32: 2823629280\n\
                            num_entries: 1\n\
                            0\x00\n\
                            \x000\x00\n\
                            \x00\x00\x00TREE_ROOT\x00d\x00\x000\x00n\x00xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\x00\n\x00";
    assert_eq!(actual, expected);
}

#[test]
fn dirstate_get_lines_multi_tree_with_parent_matches_python() {
    // Cross-check against bytes produced by a real
    // `DirState.initialize(...); _set_data([b"rev-a"], [...]); save()`
    // cycle with one parent tree and a README file entry.
    let nullstat = b"x".repeat(32);
    let mut state = DirState::new(
        "dirstate",
        Box::new(DefaultSHA1Provider::new()),
        0,
        true,
        false,
    );
    state.parents = vec![b"rev-a".to_vec()];
    state.dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![Entry {
                key: EntryKey {
                    dirname: b"".to_vec(),
                    basename: b"".to_vec(),
                    file_id: b"TREE_ROOT".to_vec(),
                },
                trees: vec![
                    TreeData {
                        minikind: Kind::Directory,
                        fingerprint: Vec::new(),
                        size: 0,
                        executable: false,
                        packed_stat: nullstat.clone(),
                    },
                    TreeData {
                        minikind: Kind::Directory,
                        fingerprint: Vec::new(),
                        size: 0,
                        executable: false,
                        packed_stat: nullstat.clone(),
                    },
                ],
            }],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: vec![Entry {
                key: EntryKey {
                    dirname: b"".to_vec(),
                    basename: b"README".to_vec(),
                    file_id: b"fid-readme".to_vec(),
                },
                trees: vec![
                    TreeData {
                        minikind: Kind::File,
                        fingerprint: b"sha-cur".to_vec(),
                        size: 10,
                        executable: true,
                        packed_stat: nullstat.clone(),
                    },
                    TreeData {
                        minikind: Kind::File,
                        fingerprint: b"sha-par".to_vec(),
                        size: 8,
                        executable: false,
                        packed_stat: nullstat,
                    },
                ],
            }],
        },
    ];
    let chunks = state.get_lines();
    let actual: Vec<u8> = chunks.into_iter().flatten().collect();
    let expected: &[u8] = b"#bazaar dirstate flat format 3\n\
                            crc32: 2831533605\n\
                            num_entries: 2\n\
                            1\x00rev-a\x00\n\
                            \x000\x00\n\
                            \x00\x00\x00TREE_ROOT\x00d\x00\x000\x00n\x00xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\x00d\x00\x000\x00n\x00xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\x00\n\
                            \x00\x00README\x00fid-readme\x00f\x00sha-cur\x0010\x00y\x00xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\x00f\x00sha-par\x008\x00n\x00xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\x00\n\x00";
    assert_eq!(actual, expected);
}

fn fresh_state() -> DirState {
    DirState::new(
        "dirstate",
        Box::new(DefaultSHA1Provider::new()),
        0,
        true,
        false,
    )
}

fn entry_key(dirname: &[u8], basename: &[u8], file_id: &[u8]) -> EntryKey {
    EntryKey {
        dirname: dirname.to_vec(),
        basename: basename.to_vec(),
        file_id: file_id.to_vec(),
    }
}

#[test]
fn mark_modified_no_hash_changes_marks_full_dirblock_state() {
    let mut state = fresh_state();
    state.dirblock_state = MemoryState::InMemoryUnmodified;
    state.mark_modified(&[], false);
    assert_eq!(state.dirblock_state, MemoryState::InMemoryModified);
    assert_eq!(state.header_state, MemoryState::NotInMemory);
    assert!(state.known_hash_changes.is_empty());
}

#[test]
fn mark_modified_hash_only_promotes_unmodified_to_hash_modified() {
    let mut state = fresh_state();
    state.dirblock_state = MemoryState::InMemoryUnmodified;
    let key = entry_key(b"", b"README", b"fid-readme");
    state.mark_modified(std::slice::from_ref(&key), false);
    assert_eq!(state.dirblock_state, MemoryState::InMemoryHashModified);
    assert!(state.known_hash_changes.contains(&key));
}

#[test]
fn mark_modified_hash_only_promotes_not_in_memory_to_hash_modified() {
    let mut state = fresh_state();
    assert_eq!(state.dirblock_state, MemoryState::NotInMemory);
    state.mark_modified(&[entry_key(b"", b"a", b"fid-a")], false);
    assert_eq!(state.dirblock_state, MemoryState::InMemoryHashModified);
}

#[test]
fn mark_modified_hash_only_leaves_in_memory_modified_alone() {
    // If the dirstate is already fully modified, a hash-only change
    // must not downgrade it back to InMemoryHashModified — Python's
    // comment explicitly flags the precedence rule.
    let mut state = fresh_state();
    state.dirblock_state = MemoryState::InMemoryModified;
    state.mark_modified(&[entry_key(b"", b"a", b"fid-a")], false);
    assert_eq!(state.dirblock_state, MemoryState::InMemoryModified);
}

#[test]
fn mark_modified_header_flag_promotes_header_state() {
    let mut state = fresh_state();
    state.header_state = MemoryState::InMemoryUnmodified;
    state.mark_modified(&[], true);
    assert_eq!(state.header_state, MemoryState::InMemoryModified);
    assert_eq!(state.dirblock_state, MemoryState::InMemoryModified);
}

#[test]
fn num_present_parents_subtracts_ghosts() {
    let mut state = fresh_state();
    state.parents = vec![b"rev-a".to_vec(), b"rev-b".to_vec(), b"rev-c".to_vec()];
    state.ghosts = vec![b"rev-b".to_vec()];
    assert_eq!(state.num_present_parents(), 2);
}

#[test]
fn num_present_parents_no_parents() {
    let state = fresh_state();
    assert_eq!(state.num_present_parents(), 0);
}

#[test]
fn num_present_parents_saturates_when_ghosts_exceed_parents() {
    // Defensive: if somehow ghosts > parents we return 0 rather than
    // underflow. Python would raise a ValueError from `-` on ints,
    // but saturating is safer and less surprising.
    let mut state = fresh_state();
    state.parents = vec![b"rev-a".to_vec()];
    state.ghosts = vec![b"g1".to_vec(), b"g2".to_vec()];
    assert_eq!(state.num_present_parents(), 0);
}

#[test]
fn entries_to_current_state_builds_expected_dirblock_layout() {
    let mut state = fresh_state();
    let nullstat = b"x".repeat(32);
    let mk_entry = |dirname: &[u8], basename: &[u8], file_id: &[u8]| Entry {
        key: EntryKey {
            dirname: dirname.to_vec(),
            basename: basename.to_vec(),
            file_id: file_id.to_vec(),
        },
        trees: vec![TreeData {
            minikind: Kind::Directory,
            fingerprint: Vec::new(),
            size: 0,
            executable: false,
            packed_stat: nullstat.clone(),
        }],
    };
    let new_entries = vec![
        mk_entry(b"", b"", b"TREE_ROOT"),
        mk_entry(b"", b"README", b"fid-readme"),
        mk_entry(b"", b"sub", b"fid-sub"),
        mk_entry(b"sub", b"inner", b"fid-inner"),
    ];
    state
        .entries_to_current_state(new_entries)
        .expect("entries_to_current_state");

    // Two sentinels + one real block for "sub".
    assert_eq!(state.dirblocks.len(), 3);
    // Block 0 holds just the root entry.
    assert_eq!(state.dirblocks[0].entries.len(), 1);
    assert_eq!(
        state.dirblocks[0].entries[0].key.file_id,
        b"TREE_ROOT".to_vec()
    );
    // Block 1 holds README and sub (the root's contents, post-split).
    assert_eq!(state.dirblocks[1].entries.len(), 2);
    assert_eq!(
        state.dirblocks[1].entries[0].key.basename,
        b"README".to_vec()
    );
    assert_eq!(state.dirblocks[1].entries[1].key.basename, b"sub".to_vec());
    // Block 2 is the real "sub" block holding inner.
    assert_eq!(state.dirblocks[2].dirname, b"sub".to_vec());
    assert_eq!(state.dirblocks[2].entries.len(), 1);
    assert_eq!(
        state.dirblocks[2].entries[0].key.basename,
        b"inner".to_vec()
    );
}

#[test]
fn entries_to_current_state_rejects_missing_root_row() {
    let mut state = fresh_state();
    let entry = Entry {
        key: EntryKey {
            dirname: b"".to_vec(),
            basename: b"README".to_vec(),
            file_id: b"fid".to_vec(),
        },
        trees: vec![tree(Kind::File)],
    };
    match state.entries_to_current_state(vec![entry]) {
        Err(EntriesToStateError::MissingRootRow { key }) => {
            assert_eq!(key.basename, b"README".to_vec());
        }
        other => panic!("expected MissingRootRow, got {:?}", other),
    }
}

#[test]
fn entries_to_current_state_rejects_empty_list() {
    let mut state = fresh_state();
    assert_eq!(
        state.entries_to_current_state(Vec::new()),
        Err(EntriesToStateError::Empty)
    );
}

#[test]
fn ensure_block_root_shortcut_returns_one() {
    let mut state = fresh_state();
    state.dirblocks = make_dirblocks(vec![]);
    // Root row coordinates: block 0, row 0, dirname=b"".
    assert_eq!(state.ensure_block(0, 0, b""), Ok(1));
    // No new block was created — we still have just the two
    // sentinel blocks.
    assert_eq!(state.dirblocks.len(), 2);
}

#[test]
fn ensure_block_creates_missing_block() {
    let mut state = fresh_state();
    // Root entry lives in the first sentinel block's row 0.
    state.dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![entry_with_trees(
                b"",
                b"",
                b"TREE_ROOT",
                vec![tree(Kind::Directory)],
            )],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: vec![entry_with_trees(
                b"",
                b"sub",
                b"fid-sub",
                vec![tree(Kind::Directory)],
            )],
        },
    ];
    // Parent entry at block 1, row 0 has basename "sub"; "sub"
    // ends with "sub", so the assertion passes.
    let idx = state.ensure_block(1, 0, b"sub").expect("ensure");
    // A new block for dirname=b"sub" should have been inserted.
    assert_eq!(idx, 2);
    assert_eq!(state.dirblocks.len(), 3);
    assert_eq!(state.dirblocks[2].dirname, b"sub".to_vec());
    assert!(state.dirblocks[2].entries.is_empty());
}

#[test]
fn ensure_block_idempotent_for_existing_block() {
    let mut state = fresh_state();
    state.dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![entry_with_trees(
                b"",
                b"",
                b"TREE_ROOT",
                vec![tree(Kind::Directory)],
            )],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: vec![entry_with_trees(
                b"",
                b"sub",
                b"fid-sub",
                vec![tree(Kind::Directory)],
            )],
        },
        Dirblock {
            dirname: b"sub".to_vec(),
            entries: vec![],
        },
    ];
    let idx = state.ensure_block(1, 0, b"sub").expect("ensure");
    assert_eq!(idx, 2);
    assert_eq!(state.dirblocks.len(), 3);
}

#[test]
fn ensure_block_rejects_bad_dirname() {
    let mut state = fresh_state();
    state.dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![entry_with_trees(
                b"",
                b"",
                b"TREE_ROOT",
                vec![tree(Kind::Directory)],
            )],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: vec![entry_with_trees(
                b"",
                b"sub",
                b"fid-sub",
                vec![tree(Kind::Directory)],
            )],
        },
    ];
    // dirname "other" does not end with parent basename "sub".
    let err = state.ensure_block(1, 0, b"other").expect_err("bad dirname");
    assert_eq!(err, EnsureBlockError::BadDirname(b"other".to_vec()));
    // No block was inserted.
    assert_eq!(state.dirblocks.len(), 2);
}

#[test]
fn mark_unmodified_resets_everything() {
    let mut state = fresh_state();
    state.header_state = MemoryState::InMemoryModified;
    state.dirblock_state = MemoryState::InMemoryHashModified;
    state
        .known_hash_changes
        .insert(entry_key(b"", b"x", b"fid"));
    state.mark_unmodified();
    assert_eq!(state.header_state, MemoryState::InMemoryUnmodified);
    assert_eq!(state.dirblock_state, MemoryState::InMemoryUnmodified);
    assert!(state.known_hash_changes.is_empty());
}

#[test]
fn dirstate_split_root_dirblock_method_wires_through() {
    // Verify the `DirState::split_root_dirblock_into_contents` method
    // calls the free function on its own `dirblocks` field.
    let mut state = DirState::new(
        "dirstate",
        Box::new(DefaultSHA1Provider::new()),
        0,
        true,
        false,
    );
    state.dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![
                make_entry(b"", b"", b"TREE_ROOT"),
                make_entry(b"", b"README", b"fid-readme"),
            ],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: Vec::new(),
        },
    ];
    state.split_root_dirblock_into_contents().expect("split");
    assert_eq!(state.dirblocks[0].entries.len(), 1);
    assert_eq!(state.dirblocks[1].entries.len(), 1);
    assert_eq!(
        state.dirblocks[1].entries[0].key.basename,
        b"README".to_vec()
    );
}

#[test]
fn parse_dirblocks_rejects_bad_size() {
    // Build a body with an invalid size field. Hand-craft to bypass the
    // `entry_line` helper which only takes u64 sizes.
    let nullstat = b"x".repeat(32);
    let mut entry = Vec::new();
    entry.extend_from_slice(b"");
    entry.push(0);
    entry.extend_from_slice(b"");
    entry.push(0);
    entry.extend_from_slice(b"TREE_ROOT");
    entry.push(0);
    entry.extend_from_slice(b"d");
    entry.push(0);
    entry.extend_from_slice(b"");
    entry.push(0);
    entry.extend_from_slice(b"not-a-number");
    entry.push(0);
    entry.push(b'n');
    entry.push(0);
    entry.extend_from_slice(nullstat.as_slice());

    let body = make_body_bytes(&[], &[], &[entry]);
    match parse_dirblocks(&body, 1, 1) {
        Err(DirblocksError::BadSize(bytes)) => {
            assert_eq!(bytes, b"not-a-number".to_vec());
        }
        other => panic!("expected BadSize, got {:?}", other),
    }
}

/// In-memory [`Transport`] for tests and non-persistent use. Holds
/// the file contents in a `Vec<u8>`; lock state is tracked
/// explicitly so the tests can verify the state transitions.
/// Additionally maintains a simple `path -> (StatInfo, Option<symlink_target>)`
/// map so tests that exercise `lstat`/`read_link` can pre-seed
/// working-tree file metadata.
struct MemoryTransport {
    contents: Option<Vec<u8>>,
    lock: Option<LockState>,
    fs: std::collections::HashMap<Vec<u8>, (StatInfo, Option<Vec<u8>>)>,
}

impl MemoryTransport {
    fn new() -> Self {
        Self {
            contents: None,
            lock: None,
            fs: std::collections::HashMap::new(),
        }
    }

    fn with_contents(bytes: &[u8]) -> Self {
        Self {
            contents: Some(bytes.to_vec()),
            lock: None,
            fs: std::collections::HashMap::new(),
        }
    }

    #[allow(dead_code)]
    fn set_fs(&mut self, path: &[u8], info: StatInfo, symlink_target: Option<Vec<u8>>) {
        self.fs.insert(path.to_vec(), (info, symlink_target));
    }
}

impl Transport for MemoryTransport {
    fn exists(&self) -> Result<bool, TransportError> {
        Ok(self.contents.is_some())
    }

    fn lock_read(&mut self) -> Result<(), TransportError> {
        if self.lock.is_some() {
            return Err(TransportError::AlreadyLocked);
        }
        self.lock = Some(LockState::Read);
        Ok(())
    }

    fn lock_write(&mut self) -> Result<(), TransportError> {
        if self.lock.is_some() {
            return Err(TransportError::AlreadyLocked);
        }
        self.lock = Some(LockState::Write);
        // A write lock creates the file if it does not yet exist,
        // matching the semantics of `lock.WriteLock` in Python.
        if self.contents.is_none() {
            self.contents = Some(Vec::new());
        }
        Ok(())
    }

    fn unlock(&mut self) -> Result<(), TransportError> {
        if self.lock.is_none() {
            return Err(TransportError::NotLocked);
        }
        self.lock = None;
        Ok(())
    }

    fn lock_state(&self) -> Option<LockState> {
        self.lock
    }

    fn read_all(&mut self) -> Result<Vec<u8>, TransportError> {
        if self.lock.is_none() {
            return Err(TransportError::NotLocked);
        }
        self.contents
            .clone()
            .ok_or_else(|| TransportError::NotFound("memory".to_string()))
    }

    fn write_all(&mut self, bytes: &[u8]) -> Result<(), TransportError> {
        match self.lock {
            Some(LockState::Write) => {}
            Some(LockState::Read) => {
                return Err(TransportError::Other(
                    "write_all requires a write lock".to_string(),
                ));
            }
            None => return Err(TransportError::NotLocked),
        }
        self.contents = Some(bytes.to_vec());
        Ok(())
    }

    fn fdatasync(&mut self) -> Result<(), TransportError> {
        // No-op for in-memory transport; the call is still valid
        // so `DirState.save` can call it unconditionally.
        Ok(())
    }

    fn lstat(&self, abspath: &[u8]) -> Result<StatInfo, TransportError> {
        self.fs.get(abspath).map(|(info, _)| *info).ok_or_else(|| {
            TransportError::NotFound(String::from_utf8_lossy(abspath).into_owned())
        })
    }

    fn read_link(&self, abspath: &[u8]) -> Result<Vec<u8>, TransportError> {
        self.fs
            .get(abspath)
            .and_then(|(_, link)| link.clone())
            .ok_or_else(|| {
                TransportError::NotFound(String::from_utf8_lossy(abspath).into_owned())
            })
    }

    fn is_tree_reference_dir(&self, _abspath: &[u8]) -> Result<bool, TransportError> {
        // In-memory fixture has no concept of nested trees.
        Ok(false)
    }

    fn list_dir(&self, abspath: &[u8]) -> Result<Vec<DirEntryInfo>, TransportError> {
        // Iterate self.fs and collect direct children.  A path is
        // a direct child of `abspath` when it starts with the
        // prefix and the remainder contains no slash.  Treats
        // `abspath == b""` as the root.
        let prefix: Vec<u8> = if abspath.is_empty() {
            Vec::new()
        } else {
            let mut p = abspath.to_vec();
            p.push(b'/');
            p
        };
        let mut out = Vec::new();
        let mut found_dir = abspath.is_empty();
        for (path, (info, link)) in &self.fs {
            if path.as_slice() == abspath {
                found_dir = true;
                continue;
            }
            if !path.starts_with(&prefix) {
                continue;
            }
            let tail = &path[prefix.len()..];
            if tail.iter().any(|&b| b == b'/') {
                continue;
            }
            let kind = if info.is_dir() {
                Some(osutils::Kind::Directory)
            } else if info.is_file() {
                Some(osutils::Kind::File)
            } else if info.is_symlink() {
                Some(osutils::Kind::Symlink)
            } else {
                None
            };
            let _ = link; // link metadata is available via read_link
            out.push(DirEntryInfo {
                basename: tail.to_vec(),
                kind,
                stat: *info,
                abspath: path.clone(),
            });
        }
        if !found_dir {
            return Err(TransportError::NotFound(
                String::from_utf8_lossy(abspath).into_owned(),
            ));
        }
        Ok(out)
    }
}

#[test]
fn transport_exists_reports_contents_presence() {
    let empty = MemoryTransport::new();
    assert!(!empty.exists().unwrap());
    let populated = MemoryTransport::with_contents(b"hi");
    assert!(populated.exists().unwrap());
}

#[test]
fn transport_read_all_requires_lock() {
    let mut t = MemoryTransport::with_contents(b"hi");
    assert_eq!(t.read_all().unwrap_err(), TransportError::NotLocked);
    t.lock_read().unwrap();
    assert_eq!(t.read_all().unwrap(), b"hi".to_vec());
}

#[test]
fn transport_write_all_requires_write_lock() {
    let mut t = MemoryTransport::with_contents(b"hi");
    // No lock at all.
    assert_eq!(t.write_all(b"new").unwrap_err(), TransportError::NotLocked);
    // Read lock is not enough.
    t.lock_read().unwrap();
    assert!(matches!(
        t.write_all(b"new").unwrap_err(),
        TransportError::Other(_)
    ));
    t.unlock().unwrap();
    // Write lock works.
    t.lock_write().unwrap();
    t.write_all(b"new").unwrap();
    assert_eq!(t.read_all().unwrap(), b"new".to_vec());
}

#[test]
fn transport_write_all_truncates_trailing_bytes() {
    let mut t = MemoryTransport::with_contents(b"previous long contents");
    t.lock_write().unwrap();
    t.write_all(b"short").unwrap();
    assert_eq!(t.read_all().unwrap(), b"short".to_vec());
}

#[test]
fn transport_lock_write_creates_missing_file() {
    let mut t = MemoryTransport::new();
    assert!(!t.exists().unwrap());
    t.lock_write().unwrap();
    // After lock_write the file exists (empty), matching Python's
    // `lock.WriteLock` behaviour.
    assert!(t.exists().unwrap());
    assert_eq!(t.read_all().unwrap(), Vec::<u8>::new());
}

#[test]
fn transport_double_lock_is_error() {
    let mut t = MemoryTransport::with_contents(b"");
    t.lock_read().unwrap();
    assert_eq!(t.lock_read().unwrap_err(), TransportError::AlreadyLocked);
    assert_eq!(t.lock_write().unwrap_err(), TransportError::AlreadyLocked);
}

#[test]
fn transport_unlock_without_lock_is_error() {
    let mut t = MemoryTransport::with_contents(b"");
    assert_eq!(t.unlock().unwrap_err(), TransportError::NotLocked);
}

#[test]
fn transport_lock_state_tracks_current_lock() {
    let mut t = MemoryTransport::with_contents(b"");
    assert_eq!(t.lock_state(), None);
    t.lock_read().unwrap();
    assert_eq!(t.lock_state(), Some(LockState::Read));
    t.unlock().unwrap();
    assert_eq!(t.lock_state(), None);
    t.lock_write().unwrap();
    assert_eq!(t.lock_state(), Some(LockState::Write));
}

#[test]
fn transport_fdatasync_is_noop_on_memory_transport() {
    let mut t = MemoryTransport::with_contents(b"");
    // fdatasync without a lock is also fine — it just flushes
    // whatever is already committed.
    t.fdatasync().unwrap();
    t.lock_write().unwrap();
    t.fdatasync().unwrap();
}

#[test]
fn transport_round_trip_through_get_lines_and_read_header() {
    // End-to-end sanity: write a serialised DirState to the
    // transport via write_all, read it back via read_all, then
    // parse the header out of the returned bytes.
    let nullstat = b"x".repeat(32);
    let mut state = fresh_state();
    state.dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![Entry {
                key: EntryKey {
                    dirname: b"".to_vec(),
                    basename: b"".to_vec(),
                    file_id: b"TREE_ROOT".to_vec(),
                },
                trees: vec![TreeData {
                    minikind: Kind::Directory,
                    fingerprint: Vec::new(),
                    size: 0,
                    executable: false,
                    packed_stat: nullstat,
                }],
            }],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: Vec::new(),
        },
    ];
    let chunks = state.get_lines();
    let bytes: Vec<u8> = chunks.into_iter().flatten().collect();

    let mut t = MemoryTransport::new();
    t.lock_write().unwrap();
    t.write_all(&bytes).unwrap();
    t.unlock().unwrap();

    t.lock_read().unwrap();
    let read_back = t.read_all().unwrap();
    assert_eq!(read_back, bytes);
    let header = read_header(&read_back).expect("header parses");
    assert_eq!(header.num_entries, 1);
}

/// Build a minimal in-memory DirState whose dirblocks are the two
/// empty-dirname sentinel blocks plus a single TREE_ROOT entry —
/// the smallest shape `get_lines` accepts without panicking.
fn minimal_populated_state() -> DirState {
    let nullstat = b"x".repeat(32);
    let mut state = fresh_state();
    state.dirblocks = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: vec![Entry {
                key: EntryKey {
                    dirname: b"".to_vec(),
                    basename: b"".to_vec(),
                    file_id: b"TREE_ROOT".to_vec(),
                },
                trees: vec![TreeData {
                    minikind: Kind::Directory,
                    fingerprint: Vec::new(),
                    size: 0,
                    executable: false,
                    packed_stat: nullstat,
                }],
            }],
        },
        Dirblock {
            dirname: Vec::new(),
            entries: Vec::new(),
        },
    ];
    state
}

#[test]
fn worth_saving_full_dirblock_modification_always_saves() {
    let mut state = fresh_state();
    state.dirblock_state = MemoryState::InMemoryModified;
    assert!(state.worth_saving());
}

#[test]
fn worth_saving_header_modification_always_saves() {
    let mut state = fresh_state();
    state.header_state = MemoryState::InMemoryModified;
    assert!(state.worth_saving());
}

#[test]
fn worth_saving_unmodified_state_is_not_worth_saving() {
    let mut state = fresh_state();
    state.header_state = MemoryState::InMemoryUnmodified;
    state.dirblock_state = MemoryState::InMemoryUnmodified;
    assert!(!state.worth_saving());
}

#[test]
fn worth_saving_hash_only_under_limit_is_not_worth_saving() {
    let mut state = fresh_state();
    state.worth_saving_limit = 5;
    state.dirblock_state = MemoryState::InMemoryHashModified;
    state
        .known_hash_changes
        .insert(entry_key(b"", b"a", b"fid-a"));
    assert!(!state.worth_saving());
}

#[test]
fn worth_saving_hash_only_at_or_above_limit_saves() {
    let mut state = fresh_state();
    state.worth_saving_limit = 2;
    state.dirblock_state = MemoryState::InMemoryHashModified;
    state
        .known_hash_changes
        .insert(entry_key(b"", b"a", b"fid-a"));
    state
        .known_hash_changes
        .insert(entry_key(b"", b"b", b"fid-b"));
    assert!(state.worth_saving());
}

#[test]
fn worth_saving_hash_only_with_negative_limit_never_saves() {
    let mut state = fresh_state();
    state.worth_saving_limit = -1;
    state.dirblock_state = MemoryState::InMemoryHashModified;
    for i in 0..10 {
        state
            .known_hash_changes
            .insert(entry_key(b"", &[b'a' + i], b"fid"));
    }
    assert!(!state.worth_saving());
}

#[test]
fn save_to_writes_get_lines_and_marks_unmodified() {
    let mut state = minimal_populated_state();
    state.dirblock_state = MemoryState::InMemoryModified;
    let expected: Vec<u8> = state.get_lines().into_iter().flatten().collect();

    let mut t = MemoryTransport::new();
    t.lock_write().unwrap();
    let wrote = state.save_to(&mut t).expect("save_to");
    assert!(wrote);
    assert_eq!(t.read_all().unwrap(), expected);
    // After a successful save the state flips back to unmodified.
    assert_eq!(state.dirblock_state, MemoryState::InMemoryUnmodified);
    assert_eq!(state.header_state, MemoryState::InMemoryUnmodified);
}

#[test]
fn save_to_honours_changes_aborted() {
    let mut state = minimal_populated_state();
    state.dirblock_state = MemoryState::InMemoryModified;
    state.changes_aborted = true;
    let mut t = MemoryTransport::new();
    t.lock_write().unwrap();
    let wrote = state.save_to(&mut t).expect("save_to");
    assert!(!wrote);
    // Nothing was written.
    assert_eq!(t.read_all().unwrap(), Vec::<u8>::new());
    // State flags are left alone.
    assert_eq!(state.dirblock_state, MemoryState::InMemoryModified);
}

#[test]
fn save_to_skips_when_not_worth_saving() {
    let mut state = minimal_populated_state();
    // Fresh + unmodified → worth_saving is false.
    state.header_state = MemoryState::InMemoryUnmodified;
    state.dirblock_state = MemoryState::InMemoryUnmodified;
    let mut t = MemoryTransport::new();
    t.lock_write().unwrap();
    let wrote = state.save_to(&mut t).expect("save_to");
    assert!(!wrote);
    assert_eq!(t.read_all().unwrap(), Vec::<u8>::new());
}

#[test]
fn set_data_replaces_parents_and_dirblocks_and_marks_modified() {
    let mut state = fresh_state();
    // Start in a clean, unmodified state and make sure set_data
    // flips both dirblock and header to InMemoryModified.
    state.header_state = MemoryState::InMemoryUnmodified;
    state.dirblock_state = MemoryState::InMemoryUnmodified;
    // Pre-populate id_index so we can verify it is invalidated.
    state.id_index = Some(IdIndex::new());

    let new_parents = vec![b"rev-x".to_vec()];
    let new_dirblocks = vec![Dirblock {
        dirname: b"sub".to_vec(),
        entries: Vec::new(),
    }];

    state.set_data(new_parents.clone(), new_dirblocks.clone());

    assert_eq!(state.parents, new_parents);
    assert_eq!(state.dirblocks.len(), 1);
    assert_eq!(state.dirblocks[0].dirname, b"sub".to_vec());
    assert_eq!(state.dirblock_state, MemoryState::InMemoryModified);
    assert_eq!(state.header_state, MemoryState::InMemoryModified);
    assert!(state.id_index.is_none());
}

#[test]
fn wipe_state_resets_all_fields() {
    let mut state = minimal_populated_state();
    state.parents = vec![b"rev-a".to_vec(), b"rev-b".to_vec()];
    state.ghosts = vec![b"rev-b".to_vec()];
    state.header_state = MemoryState::InMemoryModified;
    state.dirblock_state = MemoryState::InMemoryModified;
    state.changes_aborted = true;
    state.end_of_header = Some(42);
    state.cutoff_time = Some(123);
    let _ = state.get_or_build_id_index();
    assert!(state.id_index.is_some());

    state.wipe_state();

    assert_eq!(state.header_state, MemoryState::NotInMemory);
    assert_eq!(state.dirblock_state, MemoryState::NotInMemory);
    assert!(!state.changes_aborted);
    assert!(state.parents.is_empty());
    assert!(state.ghosts.is_empty());
    assert!(state.dirblocks.is_empty());
    assert!(state.id_index.is_none());
    assert!(state.end_of_header.is_none());
    assert!(state.cutoff_time.is_none());
}

#[test]
fn save_to_requires_write_lock() {
    let mut state = minimal_populated_state();
    state.dirblock_state = MemoryState::InMemoryModified;
    // No lock at all.
    let mut t = MemoryTransport::new();
    assert!(matches!(
        state.save_to(&mut t).unwrap_err(),
        TransportError::Other(_)
    ));
    // Read lock is still not enough.
    t.lock_read().unwrap();
    assert!(matches!(
        state.save_to(&mut t).unwrap_err(),
        TransportError::Other(_)
    ));
}

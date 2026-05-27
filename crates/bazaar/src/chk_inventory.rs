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
    use crate::FileId;

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

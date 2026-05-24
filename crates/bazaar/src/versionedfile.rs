use byteorder::{BigEndian, WriteBytesExt};

use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryInto;

#[derive(Debug)]
pub enum Error {
    ExistingContent(Key),
    VersionNotPresent(VersionId),
    Io(std::io::Error),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error {
        Error::Io(e)
    }
}

#[cfg(feature = "pyo3")]
impl From<Error> for pyo3::PyErr {
    fn from(e: Error) -> pyo3::PyErr {
        pyo3::import_exception!(bzrformats.errors, RevisionNotPresent);
        pyo3::import_exception!(bzrformats.versionedfile, ExistingContent);
        match e {
            Error::VersionNotPresent(key) => {
                RevisionNotPresent::new_err(format!("Version not present: {:?}", key))
            }
            Error::ExistingContent(key) => {
                ExistingContent::new_err(format!("Existing content: {:?}", key))
            }
            Error::Io(e) => e.into(),
        }
    }
}

#[cfg(feature = "pyo3")]
impl From<pyo3::PyErr> for Error {
    fn from(e: pyo3::PyErr) -> Error {
        use pyo3::prelude::PyAnyMethods;
        pyo3::import_exception!(bzrformats.errors, RevisionNotPresent);
        pyo3::import_exception!(bzrformats.versionedfile, ExistingContent);
        pyo3::Python::attach(|py| {
            if e.is_instance_of::<RevisionNotPresent>(py) {
                Error::VersionNotPresent(
                    e.value(py)
                        .getattr("args")
                        .unwrap()
                        .get_item(0)
                        .unwrap()
                        .extract()
                        .unwrap(),
                )
            } else if e.is_instance_of::<ExistingContent>(py) {
                Error::ExistingContent(
                    e.value(py)
                        .getattr("args")
                        .unwrap()
                        .get_item(0)
                        .unwrap()
                        .extract()
                        .unwrap(),
                )
            } else {
                panic!("Unexpected error: {:?}", e)
            }
        })
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::ExistingContent(key) => write!(f, "Existing content: {:?}", key),
            Error::VersionNotPresent(version) => write!(f, "Version not present: {:?}", version),
            Error::Io(e) => write!(f, "IO error: {}", e),
        }
    }
}

impl std::error::Error for Error {}

pub enum Ordering {
    Unordered,
    Topological,
}

impl ToString for Ordering {
    fn to_string(&self) -> String {
        match self {
            Ordering::Unordered => "unordered".to_string(),
            Ordering::Topological => "topological".to_string(),
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> pyo3::IntoPyObject<'py> for Ordering {
    type Target = pyo3::types::PyString;

    type Output = pyo3::Bound<'py, Self::Target>;

    type Error = pyo3::PyErr;

    fn into_pyobject(self, py: pyo3::Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(self.to_string().into_pyobject(py)?)
    }
}

#[cfg(feature = "pyo3")]
impl<'a, 'py> pyo3::FromPyObject<'a, 'py> for Ordering {
    type Error = pyo3::PyErr;

    fn extract(ob: pyo3::Borrowed<'a, 'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        let s = ob.extract::<String>()?;
        match s.as_str() {
            "unordered" => Ok(Ordering::Unordered),
            "topological" => Ok(Ordering::Topological),
            _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Expected 'unordered' or 'topological', got '{}'",
                s
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct VersionId(Vec<u8>);

#[cfg(feature = "pyo3")]
impl<'py> pyo3::IntoPyObject<'py> for &VersionId {
    type Target = pyo3::types::PyBytes;

    type Output = pyo3::Bound<'py, Self::Target>;

    type Error = pyo3::PyErr;

    fn into_pyobject(self, py: pyo3::Python<'py>) -> Result<Self::Output, Self::Error> {
        let bytes = pyo3::types::PyBytes::new(py, &self.0);
        Ok(bytes.into_pyobject(py)?)
    }
}

#[cfg(feature = "pyo3")]
impl<'a, 'py> pyo3::FromPyObject<'a, 'py> for VersionId {
    type Error = pyo3::PyErr;

    fn extract(ob: pyo3::Borrowed<'a, 'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        let bytes = ob.extract::<Vec<u8>>()?;
        Ok(VersionId(bytes))
    }
}

impl std::fmt::Display for VersionId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "VersionId({:?})", self.0)?;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Key {
    Fixed(Vec<Vec<u8>>),
    ContentAddressed(Vec<Vec<u8>>),
}

impl Key {
    pub fn fixed(segments: Vec<Vec<u8>>) -> Self {
        Key::Fixed(segments)
    }

    /// All segments of the key.
    pub fn segments(&self) -> &[Vec<u8>] {
        match self {
            Key::Fixed(v) | Key::ContentAddressed(v) => v,
        }
    }

    fn segments_mut(&mut self) -> &mut Vec<Vec<u8>> {
        match self {
            Key::Fixed(v) | Key::ContentAddressed(v) => v,
        }
    }

    /// Last segment (the version id / suffix).
    pub fn version_id(&self) -> &[u8] {
        self.segments().last().map(Vec::as_slice).unwrap_or(&[])
    }

    /// All segments except the last (the "prefix" used for file-based routing).
    pub fn prefix(&self) -> &[Vec<u8>] {
        let segs = self.segments();
        if segs.is_empty() {
            &[]
        } else {
            &segs[..segs.len() - 1]
        }
    }

    /// Build a new `Key::Fixed` from a prefix slice and a suffix segment.
    pub fn from_prefix_and_suffix(prefix: &[Vec<u8>], suffix: Vec<u8>) -> Self {
        let mut v = prefix.to_vec();
        v.push(suffix);
        Key::Fixed(v)
    }

    /// Replace the last segment (the version id) in place.
    pub fn set_version_id(&mut self, id: Vec<u8>) {
        let segs = self.segments_mut();
        if let Some(last) = segs.last_mut() {
            *last = id;
        }
    }

    pub fn add_prefix(&mut self, prefix: &[&[u8]]) {
        let v = self.segments_mut();
        for p in prefix.iter().rev() {
            v.insert(0, p.to_vec());
        }
    }
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Key::Fixed(v) => {
                write!(f, "(")?;
                for (i, v) in v.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", v)?;
                }
                write!(f, ")")
            }
            Key::ContentAddressed(v) => {
                write!(f, "(")?;
                for v in v.iter() {
                    write!(f, "{:?}", v)?;
                    write!(f, ", ")?;
                }
                write!(f, "<ContentAddressed>")?;
                write!(f, ")")
            }
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'a, 'py> pyo3::FromPyObject<'a, 'py> for Key {
    type Error = pyo3::PyErr;

    fn extract(ob: pyo3::Borrowed<'a, 'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        use pyo3::prelude::*;
        use pyo3::types::PyBytes;
        // Look at the type name, stripping out the module name.
        match ob
            .get_type()
            .name()
            .unwrap()
            .to_string()
            .split('.')
            .next_back()
            .unwrap()
        {
            "tuple" | "StaticTuple" => {}
            _ => {
                return Err(pyo3::exceptions::PyTypeError::new_err(format!(
                    "Expected tuple or StaticTuple, got {}",
                    ob.get_type().name().unwrap()
                )));
            }
        }
        let mut v = Vec::with_capacity(ob.len()?);
        for i in 0..ob.len()? - 1 {
            let b = ob.get_item(i)?.extract::<Bound<PyBytes>>()?;
            v.push(b.as_bytes().to_vec());
        }
        if let Some(b) = ob
            .get_item(ob.len()? - 1)?
            .extract::<Option<Bound<PyBytes>>>()?
        {
            v.push(b.as_bytes().to_vec());
            Ok(Key::Fixed(v))
        } else {
            Ok(Key::ContentAddressed(v))
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> pyo3::IntoPyObject<'py> for Key {
    type Target = pyo3::types::PyTuple;

    type Output = pyo3::Bound<'py, Self::Target>;

    type Error = pyo3::PyErr;

    fn into_pyobject(self, py: pyo3::Python<'py>) -> Result<Self::Output, Self::Error> {
        use pyo3::types::{PyBytes, PyTuple};
        match self {
            Key::Fixed(v) => {
                let t = PyTuple::new(py, v.into_iter().map(|v| PyBytes::new(py, v.as_slice())));
                t
            }
            Key::ContentAddressed(v) => {
                let mut entries = v
                    .into_iter()
                    .map(|v| PyBytes::new(py, v.as_slice()).into_any())
                    .collect::<Vec<_>>();
                entries.push(py.None().into_bound(py).into_any());
                PyTuple::new(py, entries)
            }
        }
    }
}

/// Reference-count bookkeeping for a compression-parent graph.
///
/// `KeyRefs` tracks which keys are referenced by which other keys, so that
/// during stream insertion we can tell which texts still have unresolved
/// parents (`get_unsatisfied_refs`) and discard cached content for parents
/// whose children have all been processed (`_satisfy_refs_for_key`).
///
/// Mirrors the Python `_KeyRefs` class in `bzrformats/versionedfile.py`
/// one-to-one. Generic over the key type so pyo3 callers can store opaque
/// Python tuples while pure-Rust callers can use `Vec<Vec<u8>>`.
#[derive(Debug, Clone)]
pub struct KeyRefs<K: Eq + std::hash::Hash + Clone> {
    /// Map from referenced key -> set of keys that reference it.
    refs: HashMap<K, std::collections::HashSet<K>>,
    /// Optional "new keys" tracking set. `None` when `track_new_keys` was
    /// `false` at construction.
    new_keys: Option<std::collections::HashSet<K>>,
}

impl<K: Eq + std::hash::Hash + Clone> KeyRefs<K> {
    /// Construct a fresh `KeyRefs`. Pass `track_new_keys = true` to enable
    /// [`KeyRefs::new_keys`]; when `false`, calls to [`KeyRefs::add_key`]
    /// skip recording the key as new.
    pub fn new(track_new_keys: bool) -> Self {
        Self {
            refs: HashMap::new(),
            new_keys: if track_new_keys {
                Some(std::collections::HashSet::new())
            } else {
                None
            },
        }
    }

    /// Remove all tracked references and new keys, keeping the
    /// `track_new_keys` mode of this instance.
    pub fn clear(&mut self) {
        self.refs.clear();
        if let Some(new_keys) = self.new_keys.as_mut() {
            new_keys.clear();
        }
    }

    /// Record that `key` references each of `refs`, then call
    /// [`Self::add_key`] so any reference to `key` itself is satisfied.
    pub fn add_references<I>(&mut self, key: K, refs: I)
    where
        I: IntoIterator<Item = K>,
    {
        for referenced in refs {
            self.refs.entry(referenced).or_default().insert(key.clone());
        }
        self.add_key(key);
    }

    /// Satisfy any outstanding references to `key` and, if this instance
    /// tracks new keys, remember that we've seen it.
    pub fn add_key(&mut self, key: K) {
        self.satisfy_refs_for_key(&key);
        if let Some(new_keys) = self.new_keys.as_mut() {
            new_keys.insert(key);
        }
    }

    /// Satisfy outstanding references to `key` without recording it as a
    /// new key. Safe to call even if `key` has no referrers.
    pub fn satisfy_refs_for_key(&mut self, key: &K) {
        self.refs.remove(key);
    }

    /// Bulk variant of [`Self::satisfy_refs_for_key`].
    pub fn satisfy_refs_for_keys<I>(&mut self, keys: I)
    where
        I: IntoIterator<Item = K>,
    {
        for key in keys {
            self.satisfy_refs_for_key(&key);
        }
    }

    /// Keys that still have unresolved references, i.e. the parents of
    /// other keys in the graph that have not yet been inserted themselves.
    pub fn unsatisfied_refs(&self) -> impl Iterator<Item = &K> {
        self.refs.keys()
    }

    /// All keys currently known to reference something. Flattens the
    /// per-parent referrer sets — a key referenced by multiple children is
    /// returned exactly once.
    pub fn referrers(&self) -> std::collections::HashSet<K> {
        let mut out = std::collections::HashSet::new();
        for set in self.refs.values() {
            for key in set {
                out.insert(key.clone());
            }
        }
        out
    }

    /// Set of keys that have been added since construction (or last
    /// `clear()`), or `None` if this instance isn't tracking new keys.
    pub fn new_keys(&self) -> Option<&std::collections::HashSet<K>> {
        self.new_keys.as_ref()
    }
}

impl<K: Eq + std::hash::Hash + Clone> Default for KeyRefs<K> {
    fn default() -> Self {
        Self::new(false)
    }
}

#[cfg(test)]
mod key_refs_tests {
    use super::*;

    #[test]
    fn add_references_records_referrers() {
        let mut refs: KeyRefs<Vec<u8>> = KeyRefs::new(false);
        refs.add_references(b"child".to_vec(), vec![b"p1".to_vec(), b"p2".to_vec()]);
        let unsatisfied: std::collections::HashSet<Vec<u8>> =
            refs.unsatisfied_refs().cloned().collect();
        let expected: std::collections::HashSet<Vec<u8>> =
            vec![b"p1".to_vec(), b"p2".to_vec()].into_iter().collect();
        assert_eq!(unsatisfied, expected);
    }

    #[test]
    fn adding_a_parent_satisfies_references_to_it() {
        let mut refs: KeyRefs<Vec<u8>> = KeyRefs::new(false);
        refs.add_references(b"child".to_vec(), vec![b"p1".to_vec()]);
        refs.add_key(b"p1".to_vec());
        assert!(refs.unsatisfied_refs().next().is_none());
    }

    #[test]
    fn new_keys_only_tracked_when_enabled() {
        let mut tracking: KeyRefs<Vec<u8>> = KeyRefs::new(true);
        let mut untracked: KeyRefs<Vec<u8>> = KeyRefs::new(false);
        tracking.add_key(b"k".to_vec());
        untracked.add_key(b"k".to_vec());
        assert_eq!(tracking.new_keys().map(|s| s.len()), Some(1));
        assert!(untracked.new_keys().is_none());
    }

    #[test]
    fn referrers_flattens_duplicate_children() {
        let mut refs: KeyRefs<Vec<u8>> = KeyRefs::new(false);
        // c1 references p1 and p2; c2 references p1; p1 should be referenced
        // by {c1, c2} but the flat referrers set is still {c1, c2}.
        refs.add_references(b"c1".to_vec(), vec![b"p1".to_vec(), b"p2".to_vec()]);
        refs.add_references(b"c2".to_vec(), vec![b"p1".to_vec()]);
        let referrers = refs.referrers();
        let expected: std::collections::HashSet<Vec<u8>> =
            vec![b"c1".to_vec(), b"c2".to_vec()].into_iter().collect();
        assert_eq!(referrers, expected);
    }

    #[test]
    fn clear_resets_refs_and_new_keys() {
        let mut refs: KeyRefs<Vec<u8>> = KeyRefs::new(true);
        refs.add_references(b"c".to_vec(), vec![b"p".to_vec()]);
        refs.clear();
        assert!(refs.unsatisfied_refs().next().is_none());
        assert_eq!(refs.new_keys().map(|s| s.len()), Some(0));
    }
}

impl bendy::encoding::ToBencode for Key {
    const MAX_DEPTH: usize = 10;

    fn encode(
        &self,
        encoder: bendy::encoding::SingleItemEncoder<'_>,
    ) -> Result<(), bendy::encoding::Error> {
        match self {
            Key::Fixed(v) => encoder.emit_list(|e| {
                for v in v.iter() {
                    e.emit_bytes(v)?;
                }
                Ok(())
            }),
            Key::ContentAddressed(_v) => {
                panic!("ContentAddressed keys are not supported in bencode")
            }
        }
    }
}

#[test]
fn test_network_bytes_to_kind_and_offset() {
    let (kind, offset) = network_bytes_to_kind_and_offset(b"fulltext\nrest");
    assert_eq!(kind, "fulltext");
    assert_eq!(offset, 9);
}

#[test]
fn test_key_bencode() {
    let x = Key::Fixed(vec![b"foo".to_vec(), b"bar".to_vec()]);
    let z = bendy::encoding::ToBencode::to_bencode(&x).unwrap();
    assert_eq!(z, b"l3:foo3:bare".to_vec());
}

pub trait ContentFactory {
    /// None, or the sha1 of the content fulltext
    fn sha1(&self) -> Option<Vec<u8>>;

    /// None, or the size of the content fulltext.
    fn size(&self) -> Option<usize>;

    /// The key of this content. Each key is a tuple with a single string in it.
    fn key(&self) -> Key;

    /// A tuple of parent keys for self.key. If the object has no parent information, None (as
    /// opposed to () for an empty list of parents).
    fn parents(&self) -> Option<Vec<Key>>;

    fn to_fulltext<'a, 'b>(&'a self) -> Cow<'b, [u8]>
    where
        'a: 'b;

    fn to_chunks<'a, 'b>(&'a self) -> Box<dyn Iterator<Item = Cow<'b, [u8]>> + 'b>
    where
        'a: 'b;

    fn to_lines<'a, 'b>(&'a self) -> Box<dyn Iterator<Item = Cow<'b, [u8]>> + 'b>
    where
        'a: 'b;

    fn into_fulltext(self) -> Vec<u8>;

    fn into_chunks(self) -> Box<dyn Iterator<Item = Vec<u8>>>;

    fn into_lines(self) -> Box<dyn Iterator<Item = Vec<u8>>>
    where
        Self: Sized,
    {
        Box::new(
            crate::osutils::chunks_to_lines(self.into_chunks().map(Ok::<_, std::io::Error>))
                .map(|v| v.unwrap().into_owned()),
        )
    }

    fn storage_kind(&self) -> String;

    fn map_key(&mut self, f: &dyn Fn(Key) -> Key);
}

pub struct FulltextContentFactory {
    sha1: Option<Vec<u8>>,
    size: usize,
    key: Key,
    parents: Option<Vec<Key>>,
    fulltext: Vec<u8>,
}

impl ContentFactory for FulltextContentFactory {
    fn sha1(&self) -> Option<Vec<u8>> {
        self.sha1.clone()
    }

    fn size(&self) -> Option<usize> {
        Some(self.size)
    }

    fn key(&self) -> Key {
        self.key.clone()
    }

    fn parents(&self) -> Option<Vec<Key>> {
        self.parents.clone()
    }

    fn to_fulltext<'a, 'b>(&'a self) -> Cow<'b, [u8]>
    where
        'a: 'b,
    {
        Cow::Borrowed(&self.fulltext)
    }

    fn to_chunks<'a, 'b>(&'a self) -> Box<dyn Iterator<Item = Cow<'b, [u8]>> + 'b>
    where
        'a: 'b,
    {
        Box::new(
            self.fulltext
                .as_slice()
                .chunks(crate::DEFAULT_CHUNK_SIZE)
                .map(|v| v.into()),
        )
    }

    fn to_lines<'a, 'b>(&'a self) -> Box<dyn Iterator<Item = Cow<'b, [u8]>> + 'b>
    where
        'a: 'b,
    {
        Box::new(
            crate::osutils::chunks_to_lines(std::iter::once(Ok::<_, std::io::Error>(
                &self.fulltext,
            )))
            .map(|v| v.unwrap()),
        )
    }

    fn into_fulltext(self) -> Vec<u8> {
        self.fulltext
    }

    fn into_chunks(self) -> Box<dyn Iterator<Item = Vec<u8>>> {
        let mut fulltext = self.fulltext;
        Box::new(std::iter::from_fn(move || {
            if fulltext.is_empty() {
                None
            } else {
                let chunk = fulltext
                    .drain(..std::cmp::min(crate::DEFAULT_CHUNK_SIZE, fulltext.len()))
                    .collect::<Vec<_>>();
                Some(chunk)
            }
        }))
    }

    fn storage_kind(&self) -> String {
        "fulltext".into()
    }

    fn map_key(&mut self, f: &dyn Fn(Key) -> Key) {
        self.key = f(self.key.clone());
        self.parents = self.parents.take().map(|v| v.into_iter().map(f).collect());
    }
}

impl FulltextContentFactory {
    pub fn new(
        sha1: Option<Vec<u8>>,
        key: Key,
        parents: Option<Vec<Key>>,
        fulltext: Vec<u8>,
    ) -> Self {
        Self {
            sha1,
            size: fulltext.len(),
            key,
            parents,
            fulltext,
        }
    }
}

pub struct ChunkedContentFactory {
    sha1: Option<Vec<u8>>,
    size: usize,
    key: Key,
    parents: Option<Vec<Key>>,
    chunks: Vec<Vec<u8>>,
}

impl ChunkedContentFactory {
    pub fn new(
        sha1: Option<Vec<u8>>,
        key: Key,
        parents: Option<Vec<Key>>,
        chunks: Vec<Vec<u8>>,
    ) -> Self {
        Self {
            sha1,
            size: chunks.iter().map(|v| v.len()).sum(),
            key,
            parents,
            chunks,
        }
    }
}

impl ContentFactory for ChunkedContentFactory {
    fn sha1(&self) -> Option<Vec<u8>> {
        self.sha1.clone()
    }

    fn size(&self) -> Option<usize> {
        Some(self.size)
    }

    fn key(&self) -> Key {
        self.key.clone()
    }

    fn parents(&self) -> Option<Vec<Key>> {
        self.parents.clone()
    }

    fn to_fulltext<'a, 'b>(&'a self) -> Cow<'b, [u8]>
    where
        'a: 'b,
    {
        self.chunks.concat().into()
    }

    fn to_chunks<'a, 'b>(&'a self) -> Box<dyn Iterator<Item = Cow<'b, [u8]>> + 'b>
    where
        'a: 'b,
    {
        Box::new(self.chunks.iter().map(|v| v.into()))
    }

    fn to_lines<'a, 'b>(&'a self) -> Box<dyn Iterator<Item = Cow<'b, [u8]>> + 'b>
    where
        'a: 'b,
    {
        Box::new(
            crate::osutils::chunks_to_lines(self.chunks.iter().map(Ok::<_, std::io::Error>))
                .map(|l| l.unwrap()),
        )
    }

    fn into_fulltext(self) -> Vec<u8> {
        self.chunks.into_iter().flatten().collect()
    }

    fn into_chunks(self) -> Box<dyn Iterator<Item = Vec<u8>>> {
        Box::new(self.chunks.into_iter())
    }

    fn storage_kind(&self) -> String {
        "chunked".into()
    }

    fn map_key(&mut self, f: &dyn Fn(Key) -> Key) {
        self.key = f(self.key.clone());
        self.parents = self.parents.take().map(|v| v.into_iter().map(f).collect());
    }
}

pub struct AbsentContentFactory {
    key: Key,
}

impl AbsentContentFactory {
    pub fn new(key: Key) -> Self {
        Self { key }
    }
}

impl ContentFactory for AbsentContentFactory {
    fn sha1(&self) -> Option<Vec<u8>> {
        None
    }

    fn size(&self) -> Option<usize> {
        None
    }

    fn key(&self) -> Key {
        self.key.clone()
    }

    fn parents(&self) -> Option<Vec<Key>> {
        None
    }

    fn to_fulltext<'a, 'b>(&'a self) -> Cow<'b, [u8]>
    where
        'a: 'b,
    {
        panic!("A request was made for key: {}, but that content is not available, and the calling code does not handle if it is missing.", self.key);
    }

    fn to_chunks<'a, 'b>(&'a self) -> Box<dyn Iterator<Item = Cow<'b, [u8]>> + 'b>
    where
        'a: 'b,
    {
        panic!("A request was made for key: {}, but that content is not available, and the calling code does not handle if it is missing.", self.key);
    }

    fn to_lines<'a, 'b>(&'a self) -> Box<dyn Iterator<Item = Cow<'b, [u8]>> + 'b>
    where
        'a: 'b,
    {
        panic!("A request was made for key: {}, but that content is not available, and the calling code does not handle if it is missing.", self.key);
    }

    fn into_fulltext(self) -> Vec<u8> {
        panic!("A request was made for key: {}, but that content is not available, and the calling code does not handle if it is missing.", self.key);
    }

    fn into_chunks(self) -> Box<dyn Iterator<Item = Vec<u8>>> {
        panic!("A request was made for key: {}, but that content is not available, and the calling code does not handle if it is missing.", self.key);
    }

    fn storage_kind(&self) -> String {
        "absent".into()
    }

    fn map_key(&mut self, f: &dyn Fn(Key) -> Key) {
        self.key = f(self.key.clone());
    }
}

pub trait VersionedFile<CF: ContentFactory, I> {
    fn check_not_reserved_id(id: &VersionId) -> bool;

    fn get_record_stream(
        &self,
        keys: &[&VersionId],
        ordering: Ordering,
        include_delta_closure: bool,
    ) -> Box<dyn Iterator<Item = CF>>;

    fn add_lines<'a>(
        &mut self,
        version_id: &VersionId,
        parent_texts: Option<HashMap<VersionId, I>>,
        lines: impl Iterator<Item = &'a [u8]>,
        nostore_sha: Option<bool>,
        random_id: bool,
    ) -> Result<(Vec<u8>, usize, I), Error>;

    fn has_version(&self, version_id: &VersionId) -> bool;

    fn insert_record_stream(
        &mut self,
        stream: impl Iterator<Item = Box<dyn ContentFactory>>,
    ) -> Result<(), Error>;

    fn get_format_signature(&self) -> String;

    fn get_lines(
        &self,
        version_id: &VersionId,
    ) -> Result<Box<dyn Iterator<Item = Vec<u8>>>, Error> {
        let record_stream = self.get_record_stream(&[version_id], Ordering::Unordered, false);
        if let Some(record) = record_stream.into_iter().next() {
            Ok(record.into_lines())
        } else {
            Err(Error::VersionNotPresent(version_id.clone()))
        }
    }

    fn get_text(&self, version_id: &VersionId) -> Result<Vec<u8>, Error> {
        let record_stream = self.get_record_stream(&[version_id], Ordering::Unordered, false);
        if let Some(record) = record_stream.into_iter().next() {
            Ok(record.into_fulltext())
        } else {
            Err(Error::VersionNotPresent(version_id.clone()))
        }
    }

    fn get_chunks(
        &self,
        version_id: &VersionId,
    ) -> Result<Box<dyn Iterator<Item = Vec<u8>>>, Error> {
        let record_stream = self.get_record_stream(&[version_id], Ordering::Unordered, false);
        if let Some(record) = record_stream.into_iter().next() {
            Ok(record.into_chunks())
        } else {
            Err(Error::VersionNotPresent(version_id.clone()))
        }
    }
}

/// Storage for many versioned files.
///
/// This object allows a single keyspace for accessing the history graph and
/// contents of named bytestrings.
///
/// Currently no implementation allows the graph of different key prefixes to
/// intersect, but the API does allow such implementations in the future.
///
/// The keyspace is expressed via simple tuples. Any instance of VersionedFiles
/// may have a different length key-size, but that size will be constant for
/// all texts added to or retrieved from it. For instance, breezy uses
/// instances with a key-size of 2 for storing user files in a repository, with
/// the first element the fileid, and the second the version of that file.
///
/// The use of tuples allows a single code base to support several different
/// uses with only the mapping logic changing from instance to instance.
///
/// :ivar _immediate_fallback_vfs: For subclasses that support stacking,
///     this is a list of other VersionedFiles immediately underneath this
///     one.  They may in turn each have further fallbacks.
pub trait VersionedFiles: Send + Sync {
    fn get_parent_map(
        &self,
        keys: &[Key],
    ) -> Result<HashMap<Key, Vec<Key>>, crate::knit::KnitError>;

    fn get_record_stream(
        &self,
        keys: &[Key],
        ordering: &str,
        include_delta_closure: bool,
    ) -> Result<
        Box<dyn Iterator<Item = Result<Box<dyn ContentFactory>, crate::knit::KnitError>>>,
        crate::knit::KnitError,
    >;

    fn get_sha1s(&self, keys: &[Key]) -> Result<HashMap<Key, Vec<u8>>, crate::knit::KnitError>;

    fn keys(&self) -> Result<Vec<Key>, crate::knit::KnitError>;

    fn add_lines(
        &self,
        key: &Key,
        parents: Option<&[Key]>,
        lines: &[Vec<u8>],
    ) -> Result<(Vec<u8>, usize), crate::knit::KnitError>;

    fn insert_record_stream(
        &self,
        stream: Box<dyn Iterator<Item = Box<dyn ContentFactory>>>,
    ) -> Result<(), crate::knit::KnitError>;

    fn iter_lines_added_or_present_in_keys(
        &self,
        keys: &[Key],
    ) -> Result<Vec<(Vec<u8>, Key)>, crate::knit::KnitError>;

    fn annotate(&self, key: &Key) -> Result<Vec<(Key, Vec<u8>)>, crate::knit::KnitError>;

    /// Keys of missing compression parents left behind by a prior
    /// `insert_record_stream`.
    ///
    /// Mirrors `VersionedFiles.get_missing_compression_parent_keys`, which
    /// is abstract on the Python base; stores that do not track this raise
    /// `NotImplementedError`.
    fn get_missing_compression_parent_keys(&self) -> Result<Vec<Key>, crate::knit::KnitError> {
        Err(crate::knit::KnitError::NotImplemented(
            "get_missing_compression_parent_keys",
        ))
    }

    /// Drop whatever caches this store holds.
    ///
    /// Mirrors `VersionedFiles.clear_cache`, whose base implementation is a
    /// no-op; stores with caches override this.
    fn clear_cache(&self) {}

    /// Check this store for integrity.
    ///
    /// Mirrors `VersionedFiles.check`, which is abstract on the Python base.
    fn check(&self) -> Result<(), crate::knit::KnitError> {
        Err(crate::knit::KnitError::NotImplemented("check"))
    }

    /// Resolve the full ancestry of `keys` as a `{key: parents}` map.
    ///
    /// Walks `get_parent_map` outward from `keys` until no unresolved
    /// parents remain. Mirrors the loop in
    /// `VersionedFiles.get_known_graph_ancestry`; the caller wraps the
    /// result in a `KnownGraph`.
    fn known_graph_ancestry_map(
        &self,
        keys: &[Key],
    ) -> Result<HashMap<Key, Vec<Key>>, crate::knit::KnitError> {
        let mut parent_map: HashMap<Key, Vec<Key>> = HashMap::new();
        let mut pending: Vec<Key> = keys.to_vec();
        while !pending.is_empty() {
            let this = self.get_parent_map(&pending)?;
            let mut next: Vec<Key> = Vec::new();
            for parents in this.values() {
                for p in parents {
                    if !parent_map.contains_key(p) && !this.contains_key(p) {
                        next.push(p.clone());
                    }
                }
            }
            for (k, v) in this {
                parent_map.insert(k, v);
            }
            next.sort();
            next.dedup();
            pending = next;
        }
        Ok(parent_map)
    }
}

/// Whether every entry in `lines` is a single full line.
///
/// A full line carries a newline only as its final byte; an embedded `\n`
/// anywhere before the last position means the caller passed text that was
/// not split into lines. Mirrors `VersionedFiles._check_lines_are_lines`,
/// which raises `ValueError` when this returns false.
pub fn check_lines_are_lines(lines: &[Vec<u8>]) -> bool {
    lines.iter().all(|line| {
        let body = line.split_last().map_or(&[][..], |(_, rest)| rest);
        !body.contains(&b'\n')
    })
}

/// Encode a record's metadata + fulltext into the `fulltext\n<len><meta><body>`
/// network framing. Mirrors `bzrformats.versionedfile.record_to_fulltext_bytes`.
///
/// The arguments are the data the framing actually needs: the record's
/// key, optional parents (None means "no graph info"), and fulltext
/// bytes. Callers in pyo3-land extract these from the Python record
/// with `?` so any AttributeError or extraction failure surfaces as a
/// real Python exception.
pub fn write_fulltext_record<W: std::io::Write>(
    key: &Key,
    parents: Option<&[Key]>,
    fulltext: &[u8],
    w: &mut W,
) -> std::io::Result<()> {
    let mut record_meta = bendy::encoding::Encoder::new();

    record_meta
        .emit_list(|e| {
            e.emit(key)?;
            if let Some(parents) = parents {
                e.emit_list(|e| {
                    for parent in parents {
                        e.emit(parent)?;
                    }
                    Ok(())
                })?;
            } else {
                e.emit_bytes(&b"nil"[..])?; // default to a single byte vector containing "nil"
            }
            Ok(())
        })
        .unwrap();

    let record_meta = record_meta.get_output().unwrap();

    w.write_all(b"fulltext\n")?;
    w.write_all(&length_prefix(&record_meta))?;
    w.write_all(&record_meta)?;
    w.write_all(fulltext)?;

    Ok(())
}

/// Convenience wrapper that pulls all the framing inputs out of a
/// `ContentFactory`. Used by Rust-only callers; pyo3 wrappers should
/// call [`write_fulltext_record`] directly to keep error propagation
/// fallible.
pub fn record_to_fulltext_bytes<R: ContentFactory, W: std::io::Write>(
    record: R,
    w: &mut W,
) -> std::io::Result<()> {
    let key = record.key();
    let parents = record.parents();
    write_fulltext_record(&key, parents.as_deref(), &record.into_fulltext(), w)
}

fn length_prefix(data: &[u8]) -> Vec<u8> {
    let length = data.len() as u32;
    let mut length_bytes = vec![];

    // Write the length as a 4-byte big-endian representation
    length_bytes
        .write_u32::<BigEndian>(length)
        .expect("Failed to write length bytes");

    length_bytes
}

/// Strip a record kind line from the front of `network_bytes`.
///
/// Returns the ASCII storage kind and the offset of the remaining bytes.
pub fn network_bytes_to_kind_and_offset(network_bytes: &[u8]) -> (String, usize) {
    let line_end = network_bytes
        .iter()
        .position(|&b| b == b'\n')
        .expect("network bytes must contain a newline-terminated kind");
    let storage_kind =
        std::str::from_utf8(&network_bytes[..line_end]).expect("storage kind must be ASCII");
    (storage_kind.to_string(), line_end + 1)
}

pub fn fulltext_network_to_record(bytes: &[u8], line_end: usize) -> FulltextContentFactory {
    // Extract meta_len from the network fulltext record
    let meta_len_bytes: [u8; 4] = bytes[line_end..line_end + 4]
        .try_into()
        .expect("Expected 4 bytes for meta_len");
    let meta_len = u32::from_be_bytes(meta_len_bytes) as usize;

    // Extract record_meta using meta_len
    let record_meta = &bytes[line_end + 4..line_end + 4 + meta_len];

    // Decode record_meta using Bencode
    let mut decoder = bendy::decoding::Decoder::new(record_meta);

    let mut tuple = decoder
        .next_object()
        .expect("Failed to decode record_meta using Bencode")
        .expect("Failed to decode tuple using Bencode")
        .try_into_list()
        .unwrap();

    fn decode_key(o: bendy::decoding::Object) -> Key {
        let mut ret = vec![];

        let mut l = o.try_into_list().unwrap();

        while let Some(b) = l.next_object().unwrap() {
            ret.push(b.try_into_bytes().unwrap().to_vec());
        }

        Key::Fixed(ret)
    }

    let key = decode_key(
        tuple
            .next_object()
            .expect("Failed to decode record_meta using Bencode")
            .expect("Failed to decode key using Bencode"),
    );

    let parents = tuple
        .next_object()
        .expect("Failed to decode record_meta using Bencode")
        .expect("Failed to decode parents using Bencode");

    // Convert parents from "nil" to None
    let parents = match parents {
        bendy::decoding::Object::Bytes(bytes) => {
            if bytes == b"nil" {
                None
            } else {
                panic!("Expected parents to be a list or nil");
            }
        }
        bendy::decoding::Object::List(mut l) => {
            let mut parents = vec![];
            while let Some(parent) = l.next_object().unwrap() {
                parents.push(decode_key(parent));
            }
            Some(parents)
        }
        _ => panic!("Expected parents to be a list or nil"),
    };

    // Extract fulltext from the remaining bytes
    let fulltext = &bytes[line_end + 4 + meta_len..];

    FulltextContentFactory::new(None, key, parents, fulltext.to_vec())
}

/// One input to [`add_mpdiffs_build`] / [`add_mpdiffs_prepare`]: the key,
/// its parents, the expected sha1 of the reconstructed text, and the diff.
#[derive(Clone, Debug)]
pub struct MpdiffRecord {
    pub key: Key,
    pub parents: Vec<Key>,
    pub expected_sha1: Vec<u8>,
    pub diff: crate::multiparent::MultiParent,
}

/// One dispatch row produced by [`add_mpdiffs_prepare`]: a reconstructed
/// text ready to hand to `VersionedFiles.add_lines`, plus the
/// `left_matching_blocks` hint to thread in (when there's exactly one
/// parent) and the `expected_sha1` for the post-add check.
#[derive(Clone, Debug)]
pub struct PreparedAddLines {
    pub key: Key,
    pub parents: Vec<Key>,
    pub lines: Vec<Vec<u8>>,
    pub left_matching_blocks: Option<Vec<(usize, usize, usize)>>,
    pub expected_sha1: Vec<u8>,
}

/// Phase 1 of `add_mpdiffs`: load the input records into a fresh in-memory
/// multiparent versioned file and report which parent keys are still
/// missing.
///
/// The caller is expected to fetch the missing parents' fulltexts (typically
/// via `VersionedFiles.get_record_stream`) and seed them into the returned
/// mpvf with `add_version(lines, key, [], None, false)` before calling
/// [`add_mpdiffs_prepare`].
pub fn add_mpdiffs_build(
    records: &[MpdiffRecord],
) -> (crate::multiparent::MultiMemoryVersionedFile<Key>, Vec<Key>) {
    let mut mpvf = crate::multiparent::MultiMemoryVersionedFile::<Key>::default();
    for r in records {
        mpvf.add_diff(r.diff.clone(), r.key.clone(), r.parents.clone());
    }
    let mut needed: std::collections::HashSet<Key> = std::collections::HashSet::new();
    for r in records {
        for p in &r.parents {
            if !mpvf.has_version(p) {
                needed.insert(p.clone());
            }
        }
    }
    (mpvf, needed.into_iter().collect())
}

/// Phase 2 of `add_mpdiffs`: reconstruct each input record's fulltext from
/// the now-populated mpvf and pre-compute the `left_matching_blocks` hint
/// for the single-parent case (matching Python's logic in
/// `VersionedFiles.add_mpdiffs`).
///
/// Returns one [`PreparedAddLines`] per input record, in input order.
pub fn add_mpdiffs_prepare(
    mpvf: &mut crate::multiparent::MultiMemoryVersionedFile<Key>,
    records: &[MpdiffRecord],
) -> Vec<PreparedAddLines> {
    let keys: Vec<Key> = records.iter().map(|r| r.key.clone()).collect();
    let reconstructed = mpvf.get_line_list(&keys);
    records
        .iter()
        .zip(reconstructed.into_iter())
        .map(|(r, lines)| {
            let left_matching_blocks = if r.parents.len() == 1 {
                let parent_len = mpvf
                    .get_diff(&r.parents[0])
                    .map(crate::multiparent::MultiParent::num_lines)
                    .unwrap_or(0);
                Some(r.diff.get_matching_blocks(0, parent_len))
            } else {
                None
            };
            PreparedAddLines {
                key: r.key.clone(),
                parents: r.parents.clone(),
                lines,
                left_matching_blocks,
                expected_sha1: r.expected_sha1.clone(),
            }
        })
        .collect()
}

/// Generate multi-parent diffs for `ordered_keys`, in input order.
///
/// Mirrors `bzrformats.versionedfile._MPDiffGenerator.compute_diffs`. The
/// generator pulls every key's text (and its non-ghost parents' texts) out
/// of `vf` via `get_record_stream`, then walks the stream in topological
/// order, refcount-releasing parent cache entries as soon as no children
/// still need them, and computes one [`crate::multiparent::MultiParent`]
/// per ordered key.
pub fn make_mpdiffs(
    vf: &dyn VersionedFiles,
    ordered_keys: &[Key],
) -> Result<Vec<crate::multiparent::MultiParent>, crate::knit::KnitError> {
    let parent_map = vf.get_parent_map(ordered_keys)?;

    let mut missing_keys: Vec<Key> = Vec::new();
    for k in ordered_keys {
        if !parent_map.contains_key(k) {
            missing_keys.push(k.clone());
        }
    }
    if let Some(first) = missing_keys.into_iter().next() {
        return Err(crate::knit::KnitError::RevisionNotPresent(
            first.segments().to_vec(),
        ));
    }

    // Refcounts and just_parents track which texts we still need to keep
    // cached so we can pop them as soon as the last child has consumed
    // them. just_parents is the set of parents that aren't themselves in
    // ordered_keys (so we have to look them up to distinguish present from
    // ghost).
    let mut needed_keys: std::collections::HashSet<Key> = ordered_keys.iter().cloned().collect();
    let mut refcounts: std::collections::HashMap<Key, usize> = std::collections::HashMap::new();
    let mut just_parents: std::collections::HashSet<Key> = std::collections::HashSet::new();
    for parents in parent_map.values() {
        if parents.is_empty() {
            continue;
        }
        for p in parents {
            just_parents.insert(p.clone());
            needed_keys.insert(p.clone());
            *refcounts.entry(p.clone()).or_insert(0) += 1;
        }
    }
    // just_parents = parents that aren't already in parent_map.
    just_parents.retain(|p| !parent_map.contains_key(p));

    // Distinguish ghost parents (not present in storage) from real ones; we
    // only need to fetch the real ones.
    let just_parents_vec: Vec<Key> = just_parents.iter().cloned().collect();
    let present_parents = vf.get_parent_map(&just_parents_vec)?;
    let ghost_parents: std::collections::HashSet<Key> = just_parents
        .iter()
        .filter(|p| !present_parents.contains_key(*p))
        .cloned()
        .collect();
    for g in &ghost_parents {
        needed_keys.remove(g);
    }

    // Keep parent_map mutable so we can pop entries as we process them
    // (mirrors Python's `self.parent_map.pop(key)`).
    let mut parent_map = parent_map;
    let mut chunks_cache: std::collections::HashMap<Key, Vec<Vec<u8>>> =
        std::collections::HashMap::new();
    let mut diffs: std::collections::HashMap<Key, crate::multiparent::MultiParent> =
        std::collections::HashMap::new();

    let needed_keys_vec: Vec<Key> = needed_keys.into_iter().collect();
    let stream = vf.get_record_stream(&needed_keys_vec, "topological", true)?;
    for rec in stream {
        let rec = rec?;
        if rec.storage_kind() == "absent" {
            return Err(crate::knit::KnitError::RevisionNotPresent(
                rec.key().segments().to_vec(),
            ));
        }
        let key = rec.key();
        let this_lines: Vec<Vec<u8>> = rec.to_lines().map(|c| c.into_owned()).collect();

        let cache_this = refcounts.contains_key(&key);

        if let Some(parents) = parent_map.remove(&key) {
            // Collect parent line lists in original order, popping cache
            // entries whose refcount drops to zero.
            let mut parent_lines: Vec<Vec<Vec<u8>>> = Vec::with_capacity(parents.len());
            for p in &parents {
                if ghost_parents.contains(p) {
                    continue;
                }
                let count = refcounts.get_mut(p).ok_or_else(|| {
                    crate::knit::KnitError::Corrupt(format!(
                        "make_mpdiffs: missing refcount for {:?}",
                        p
                    ))
                })?;
                if *count == 1 {
                    refcounts.remove(p);
                    let cached = chunks_cache.remove(p).ok_or_else(|| {
                        crate::knit::KnitError::Corrupt(format!(
                            "make_mpdiffs: missing cached chunks for {:?}",
                            p
                        ))
                    })?;
                    parent_lines.push(cached);
                } else {
                    *count -= 1;
                    let cached = chunks_cache.get(p).cloned().ok_or_else(|| {
                        crate::knit::KnitError::Corrupt(format!(
                            "make_mpdiffs: missing cached chunks for {:?}",
                            p
                        ))
                    })?;
                    parent_lines.push(cached);
                }
            }
            let parent_refs: Vec<&[Vec<u8>]> = parent_lines.iter().map(Vec::as_slice).collect();
            let diff = crate::multiparent::MultiParent::from_lines(&this_lines, &parent_refs, None);
            diffs.insert(key.clone(), diff);
        }

        if cache_this {
            chunks_cache.insert(key, this_lines);
        }
    }

    // Emit results in the original input order.
    let mut out: Vec<crate::multiparent::MultiParent> = Vec::with_capacity(ordered_keys.len());
    for k in ordered_keys {
        match diffs.remove(k) {
            Some(d) => out.push(d),
            None => {
                return Err(crate::knit::KnitError::Corrupt(format!(
                    "make_mpdiffs: never produced a diff for {:?}",
                    k
                )));
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_lines_are_lines_accepts_proper_lines() {
        assert!(check_lines_are_lines(&[]));
        assert!(check_lines_are_lines(&[b"a\n".to_vec(), b"b\n".to_vec()]));
        // A final line without a trailing newline is still a single line.
        assert!(check_lines_are_lines(&[
            b"a\n".to_vec(),
            b"no-eol".to_vec()
        ]));
        // A bare newline is one (empty) line.
        assert!(check_lines_are_lines(&[b"\n".to_vec()]));
        // An empty entry has no embedded newline.
        assert!(check_lines_are_lines(&[b"".to_vec()]));
    }

    #[test]
    fn check_lines_are_lines_rejects_embedded_newlines() {
        // A newline before the last byte means the text was not split.
        assert!(!check_lines_are_lines(&[b"a\nb\n".to_vec()]));
        assert!(!check_lines_are_lines(&[
            b"ok\n".to_vec(),
            b"a\nb".to_vec()
        ]));
    }

    fn k(s: &str) -> Key {
        Key::Fixed(vec![s.as_bytes().to_vec()])
    }

    #[test]
    fn add_mpdiffs_build_collects_only_missing_parents() {
        use crate::multiparent::{Hunk, MultiParent};
        let snap_a = MultiParent::with_hunks(vec![Hunk::NewText(vec![b"a\n".to_vec()])]);
        let snap_b = MultiParent::with_hunks(vec![Hunk::NewText(vec![b"b\n".to_vec()])]);
        let records = vec![
            MpdiffRecord {
                key: k("a"),
                parents: vec![],
                expected_sha1: vec![],
                diff: snap_a,
            },
            MpdiffRecord {
                key: k("b"),
                parents: vec![k("a"), k("z")],
                expected_sha1: vec![],
                diff: snap_b,
            },
        ];
        let (mpvf, needed) = add_mpdiffs_build(&records);
        // 'a' is in the mpvf as a record; 'z' is not.
        assert_eq!(needed, vec![k("z")]);
        assert!(mpvf.has_version(&k("a")));
        assert!(mpvf.has_version(&k("b")));
    }

    #[test]
    fn add_mpdiffs_prepare_single_parent_emits_blocks() {
        use crate::multiparent::{Hunk, MultiParent};
        // 'a' is a fulltext; 'b' is a delta against 'a' that re-uses every
        // line of the parent.
        let snap_a =
            MultiParent::with_hunks(vec![Hunk::NewText(vec![b"x\n".to_vec(), b"y\n".to_vec()])]);
        let delta_b = MultiParent::with_hunks(vec![Hunk::ParentText {
            parent: 0,
            parent_pos: 0,
            child_pos: 0,
            num_lines: 2,
        }]);
        let records = vec![
            MpdiffRecord {
                key: k("a"),
                parents: vec![],
                expected_sha1: vec![],
                diff: snap_a,
            },
            MpdiffRecord {
                key: k("b"),
                parents: vec![k("a")],
                expected_sha1: vec![],
                diff: delta_b,
            },
        ];
        let (mut mpvf, needed) = add_mpdiffs_build(&records);
        assert!(needed.is_empty());
        let prepared = add_mpdiffs_prepare(&mut mpvf, &records);
        assert_eq!(prepared.len(), 2);
        assert_eq!(prepared[0].lines, vec![b"x\n".to_vec(), b"y\n".to_vec()]);
        assert_eq!(prepared[0].left_matching_blocks, None);
        assert_eq!(prepared[1].lines, vec![b"x\n".to_vec(), b"y\n".to_vec()]);
        // One ParentText hunk plus the trailing sentinel from
        // get_matching_blocks.
        let blocks = prepared[1].left_matching_blocks.as_ref().unwrap();
        assert_eq!(blocks, &vec![(0, 0, 2), (2, 2, 0)]);
    }

    #[test]
    fn add_mpdiffs_prepare_multi_parent_no_blocks() {
        use crate::multiparent::{Hunk, MultiParent};
        let snap = || MultiParent::with_hunks(vec![Hunk::NewText(vec![b"x\n".to_vec()])]);
        let records = vec![
            MpdiffRecord {
                key: k("p1"),
                parents: vec![],
                expected_sha1: vec![],
                diff: snap(),
            },
            MpdiffRecord {
                key: k("p2"),
                parents: vec![],
                expected_sha1: vec![],
                diff: snap(),
            },
            MpdiffRecord {
                key: k("c"),
                parents: vec![k("p1"), k("p2")],
                expected_sha1: vec![],
                diff: snap(),
            },
        ];
        let (mut mpvf, _) = add_mpdiffs_build(&records);
        let prepared = add_mpdiffs_prepare(&mut mpvf, &records);
        // Multi-parent: no left_matching_blocks hint.
        assert_eq!(prepared[2].left_matching_blocks, None);
    }
}

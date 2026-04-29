use memchr::memchr;
use rand::Rng;
use std::borrow::Cow;

pub fn is_well_formed_line(line: &[u8]) -> bool {
    if line.is_empty() {
        return false;
    }
    memchr(b'\n', line) == Some(line.len() - 1)
}

pub trait AsCow<'a, T: ToOwned + ?Sized> {
    fn as_cow(self) -> Cow<'a, T>;
}

impl<'a> AsCow<'a, [u8]> for &'a [u8] {
    fn as_cow(self) -> Cow<'a, [u8]> {
        Cow::Borrowed(self)
    }
}

impl<'a> AsCow<'a, [u8]> for Cow<'a, [u8]> {
    fn as_cow(self) -> Cow<'a, [u8]> {
        self
    }
}

impl<'a> AsCow<'a, [u8]> for Vec<u8> {
    fn as_cow(self) -> Cow<'a, [u8]> {
        Cow::Owned(self)
    }
}

impl<'a> AsCow<'a, [u8]> for &'a Vec<u8> {
    fn as_cow(self) -> Cow<'a, [u8]> {
        Cow::Borrowed(self.as_slice())
    }
}

pub fn chunks_to_lines<'a, C, I, E>(chunks: I) -> impl Iterator<Item = Result<Cow<'a, [u8]>, E>>
where
    I: Iterator<Item = Result<C, E>> + 'a,
    C: AsCow<'a, [u8]> + 'a,
    E: std::fmt::Debug,
{
    pub struct ChunksToLines<'a, C, E>
    where
        C: AsCow<'a, [u8]>,
        E: std::fmt::Debug,
    {
        chunks: Box<dyn Iterator<Item = Result<C, E>> + 'a>,
        tail: Vec<u8>,
    }

    impl<'a, C, E: std::fmt::Debug> Iterator for ChunksToLines<'a, C, E>
    where
        C: AsCow<'a, [u8]>,
    {
        type Item = Result<Cow<'a, [u8]>, E>;

        fn next(&mut self) -> Option<Self::Item> {
            loop {
                // See if we can find a line in tail
                if let Some(newline) = memchr(b'\n', &self.tail) {
                    // The chunk contains multiple lines, so split it into lines
                    let line = Cow::Owned(self.tail[..=newline].to_vec());
                    self.tail.drain(..=newline);
                    return Some(Ok(line));
                } else {
                    // We couldn't find a newline
                    if let Some(next_chunk) = self.chunks.next() {
                        match next_chunk {
                            Err(e) => {
                                return Some(Err(e));
                            }
                            Ok(next_chunk) => {
                                let next_chunk = next_chunk.as_cow();
                                // If the chunk is well-formed, return it
                                if self.tail.is_empty() && is_well_formed_line(next_chunk.as_ref())
                                {
                                    return Some(Ok(next_chunk));
                                } else {
                                    self.tail.extend_from_slice(next_chunk.as_ref());
                                }
                            }
                        }
                    } else {
                        // We've reached the end of the chunks, so return the last chunk
                        if self.tail.is_empty() {
                            return None;
                        }
                        let line = Cow::Owned(self.tail.to_vec());
                        self.tail.clear();
                        return Some(Ok(line));
                    }
                }
            }
        }
    }

    ChunksToLines {
        chunks: Box::new(chunks),
        tail: Vec::new(),
    }
}

#[test]
fn test_chunks_to_lines() {
    assert_eq!(
        chunks_to_lines(vec![Ok::<_, std::io::Error>("foo\nbar".as_bytes().as_cow())].into_iter())
            .map(|x| x.unwrap())
            .collect::<Vec<_>>(),
        vec!["foo\n".as_bytes().as_cow(), "bar".as_bytes().as_cow()]
    );
}

pub fn split_lines(text: &[u8]) -> impl Iterator<Item = Cow<'_, [u8]>> {
    pub struct SplitLines<'a> {
        text: &'a [u8],
    }

    impl<'a> Iterator for SplitLines<'a> {
        type Item = Cow<'a, [u8]>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.text.is_empty() {
                return None;
            }
            if let Some(newline) = memchr(b'\n', self.text) {
                let line = Cow::Borrowed(&self.text[..=newline]);
                self.text = &self.text[newline + 1..];
                Some(line)
            } else {
                // No newline found, so return the rest of the text
                let line = Cow::Borrowed(self.text);
                self.text = &self.text[self.text.len()..];
                Some(line)
            }
        }
    }

    SplitLines { text }
}

#[test]
fn test_split_lines() {
    assert_eq!(
        split_lines("foo\nbar".as_bytes())
            .map(|x| x.to_vec())
            .collect::<Vec<_>>(),
        vec!["foo\n".as_bytes().to_vec(), "bar".as_bytes().to_vec()]
    );
}

const ALNUM: &str = "0123456789abcdefghijklmnopqrstuvwxyz";

pub fn rand_chars(num: usize) -> String {
    let mut rng = rand::rng();
    let mut s = String::new();
    for _ in 0..num {
        let raw_byte = rng.random_range(0..256);
        s.push(ALNUM.chars().nth(raw_byte % 36).unwrap());
    }
    s
}

pub fn contains_whitespace(s: &str) -> bool {
    let ws = " \t\n\r\u{000B}\u{000C}";
    for ch in ws.chars() {
        if s.contains(ch) {
            return true;
        }
    }
    false
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Kind {
    File,
    Directory,
    Symlink,
    TreeReference,
}

impl Kind {
    pub fn marker(&self) -> &'static str {
        match self {
            Kind::File => "",
            Kind::Directory => "/",
            Kind::Symlink => "@",
            Kind::TreeReference => "+",
        }
    }

    /// The string form used throughout the codebase (``"file"``,
    /// ``"directory"``, ``"symlink"``, ``"tree-reference"``) — the
    /// same tokens Python's inventory layer speaks.
    pub fn as_str(&self) -> &'static str {
        match self {
            Kind::File => "file",
            Kind::Directory => "directory",
            Kind::Symlink => "symlink",
            Kind::TreeReference => "tree-reference",
        }
    }
}

impl std::fmt::Display for Kind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Error returned by [`<Kind as FromStr>::from_str`] when the input is
/// not one of the four recognised kind names.  Carries the offending
/// string so callers can surface it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KindParseError(pub String);

impl std::fmt::Display for KindParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unknown kind {:?}", self.0)
    }
}

impl std::error::Error for KindParseError {}

impl std::str::FromStr for Kind {
    type Err = KindParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "file" => Ok(Kind::File),
            "directory" => Ok(Kind::Directory),
            "symlink" => Ok(Kind::Symlink),
            "tree-reference" => Ok(Kind::TreeReference),
            other => Err(KindParseError(other.to_string())),
        }
    }
}

#[cfg(feature = "pyo3")]
impl<'py> pyo3::IntoPyObject<'py> for Kind {
    type Target = pyo3::types::PyString;

    type Output = pyo3::Bound<'py, Self::Target>;

    type Error = std::convert::Infallible;

    fn into_pyobject(self, py: pyo3::Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            Kind::File => "file",
            Kind::Directory => "directory",
            Kind::Symlink => "symlink",
            Kind::TreeReference => "tree-reference",
        }
        .into_pyobject(py)
    }
}

#[cfg(feature = "pyo3")]
impl<'a, 'py> pyo3::FromPyObject<'a, 'py> for Kind {
    type Error = pyo3::PyErr;

    fn extract(ob: pyo3::Borrowed<'a, 'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        let s: String = ob.extract()?;
        match s.as_str() {
            "file" => Ok(Kind::File),
            "directory" => Ok(Kind::Directory),
            "symlink" => Ok(Kind::Symlink),
            "tree-reference" => Ok(Kind::TreeReference),
            _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Invalid kind: {}",
                s
            ))),
        }
    }
}

pub mod chunkreader;
#[cfg(unix)]
#[path = "mounts-unix.rs"]
pub mod mounts;
#[cfg(windows)]
#[path = "mounts-win32.rs"]
pub mod mounts;
pub mod path;
pub mod sha;
pub mod time;

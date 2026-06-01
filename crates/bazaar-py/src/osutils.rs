use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyModule};
use std::path::{Path, PathBuf};

#[pyfunction]
fn split_lines<'a>(py: Python<'a>, text: &'a [u8]) -> PyResult<Bound<'a, PyList>> {
    let ret = PyList::empty(py);
    for line in bazaar::osutils::split_lines(text) {
        let line_bytes = PyBytes::new(py, &line);
        ret.append(line_bytes)?;
    }
    Ok(ret)
}

#[pyfunction]
fn rand_chars(num: usize) -> PyResult<String> {
    Ok(bazaar::osutils::rand_chars(num))
}

#[pyfunction]
fn contains_whitespace(s: &str) -> bool {
    bazaar::osutils::contains_whitespace(s)
}

/// Join the input chunks and split the result into lines, keeping each
/// line's trailing `\n`. Mirrors `osutils.chunks_to_lines`: a list of
/// bytes chunks in, a list of bytes lines out, with the final line
/// possibly missing its terminator. Delegates the actual splitting to
/// `bazaar::osutils::split_lines`.
#[pyfunction]
fn chunks_to_lines<'py>(
    py: Python<'py>,
    chunks: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyList>> {
    let mut joined: Vec<u8> = Vec::new();
    for chunk in chunks.try_iter()? {
        let chunk = chunk?;
        let bytes = chunk.cast_into::<PyBytes>()?;
        joined.extend_from_slice(bytes.as_bytes());
    }
    let out = PyList::empty(py);
    for line in bazaar::osutils::split_lines(&joined) {
        out.append(PyBytes::new(py, &line))?;
    }
    Ok(out)
}

#[pyfunction]
fn is_inside(dir: &str, fname: &str) -> PyResult<bool> {
    let dir_path = Path::new(dir);
    let fname_path = Path::new(fname);
    Ok(bazaar::osutils::path::is_inside(dir_path, fname_path))
}

#[pyfunction]
fn is_inside_any(dir_list: Vec<String>, fname: &str) -> PyResult<bool> {
    let dir_paths: Vec<&Path> = dir_list.iter().map(|d| Path::new(d.as_str())).collect();
    let fname_path = Path::new(fname);
    Ok(bazaar::osutils::path::is_inside_any(&dir_paths, fname_path))
}

#[pyfunction]
fn parent_directories(path: &str) -> PyResult<Vec<String>> {
    let path_obj = Path::new(path);
    let parents: Vec<String> = bazaar::osutils::path::parent_directories(path_obj)
        .map(|p| p.to_string_lossy().to_string())
        .collect();
    Ok(parents)
}

// Walkdirs implementation - simplified version for basic functionality
#[pyfunction]
fn walkdirs_utf8(top: &str) -> PyResult<Vec<(String, Vec<(String, String, u64, String)>)>> {
    use std::fs;

    let mut results = Vec::new();
    let walk = walkdir::WalkDir::new(top).follow_links(false);

    for entry in walk {
        let entry = entry.map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let path = entry.path();

        if path.is_dir() {
            let mut dir_entries = Vec::new();

            // Read directory contents
            if let Ok(read_dir) = fs::read_dir(path) {
                for dir_entry in read_dir.flatten() {
                    let name = dir_entry.file_name().to_string_lossy().to_string();
                    let metadata = dir_entry.metadata();

                    if let Ok(metadata) = metadata {
                        let kind = if metadata.is_dir() {
                            "directory"
                        } else if metadata.is_symlink() {
                            "symlink"
                        } else {
                            "file"
                        };

                        let size = metadata.len();
                        let utf8path = dir_entry.path().to_string_lossy().to_string();

                        dir_entries.push((name, kind.to_string(), size, utf8path));
                    }
                }
            }

            results.push((path.to_string_lossy().to_string(), dir_entries));
        }
    }

    Ok(results)
}

#[pyfunction]
fn normalizes_filenames() -> bool {
    bazaar::osutils::path::normalizes_filenames()
}

#[pyfunction]
pub fn supports_symlinks(path: PathBuf) -> Option<bool> {
    bazaar::osutils::mounts::supports_symlinks(path)
}

/// Extract the utf-8 bytes of a str-or-bytes value (str is encoded utf-8).
fn str_or_bytes(value: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(b) = value.downcast::<PyBytes>() {
        Ok(b.as_bytes().to_vec())
    } else {
        let s: String = value.extract()?;
        Ok(s.into_bytes())
    }
}

/// The sha1 of concatenated strings, as ascii hex bytes. str items are utf-8
/// encoded. Mirrors `osutils.sha_strings`.
#[pyfunction]
fn sha_strings<'py>(py: Python<'py>, strings: Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
    let mut chunks: Vec<Vec<u8>> = Vec::new();
    for s in strings.try_iter()? {
        chunks.push(str_or_bytes(&s?)?);
    }
    let hex = bazaar::osutils::sha::sha_chunks(chunks.iter());
    Ok(PyBytes::new(py, hex.as_bytes()))
}

/// The sha1 of a single string, as ascii hex bytes. Mirrors
/// `osutils.sha_string`.
#[pyfunction]
fn sha_string<'py>(py: Python<'py>, string: Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = str_or_bytes(&string)?;
    let hex = bazaar::osutils::sha::sha_string(&bytes);
    Ok(PyBytes::new(py, hex.as_bytes()))
}

/// The sha1 of a file object (read in 64KiB chunks), as ascii hex bytes.
/// Mirrors `osutils.sha_file`.
#[pyfunction]
fn sha_file<'py>(py: Python<'py>, file_obj: Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
    use sha1::{Digest, Sha1};
    let mut hasher = Sha1::new();
    loop {
        let chunk = file_obj.call_method1("read", (65536,))?;
        let bytes = chunk.downcast::<PyBytes>()?.as_bytes();
        if bytes.is_empty() {
            break;
        }
        hasher.update(bytes);
    }
    let digest = hasher.finalize();
    let hex: String = digest.iter().map(|b| format!("{:02x}", b)).collect();
    Ok(PyBytes::new(py, hex.as_bytes()))
}

/// Split a path into a list of components, dropping a leading `/`. Preserves
/// the str-vs-bytes type of `path`. Mirrors `osutils.splitpath`.
#[pyfunction]
fn splitpath<'py>(py: Python<'py>, path: Bound<'py, PyAny>) -> PyResult<Bound<'py, PyList>> {
    if let Ok(b) = path.downcast::<PyBytes>() {
        let mut data = b.as_bytes();
        if data.first() == Some(&b'/') {
            data = &data[1..];
        }
        let out = PyList::empty(py);
        if !data.is_empty() {
            for seg in data.split(|&c| c == b'/') {
                out.append(PyBytes::new(py, seg))?;
            }
        }
        Ok(out)
    } else {
        let s: String = path.extract()?;
        let s = s.strip_prefix('/').unwrap_or(&s);
        let out = PyList::empty(py);
        if !s.is_empty() {
            for seg in s.split('/') {
                out.append(seg)?;
            }
        }
        Ok(out)
    }
}

/// Map a stat `st_mode` to a bzr file-kind string. Mirrors
/// `osutils.file_kind_from_stat_mode`.
#[pyfunction]
fn file_kind_from_stat_mode(mode: u32) -> &'static str {
    // S_IFMT mask = 0o170000; compare the format bits.
    match mode & 0o170000 {
        0o100000 => "file",
        0o040000 => "directory",
        0o120000 => "symlink",
        0o010000 => "fifo",
        0o140000 => "socket",
        0o020000 => "chardev",
        0o060000 => "block",
        _ => "unknown",
    }
}

/// Coerce a str/PathLike/utf-8-bytes value to str. Mirrors
/// `osutils.safe_unicode` (raises TypeError on invalid utf-8 bytes).
#[pyfunction]
fn safe_unicode<'py>(py: Python<'py>, value: Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    if value.is_instance_of::<pyo3::types::PyString>() {
        return Ok(value);
    }
    // os.PathLike is left as-is too.
    let pathlike = py.import("os")?.getattr("PathLike")?;
    if value.is_instance(&pathlike)? {
        return Ok(value);
    }
    match value.call_method1("decode", ("utf8",)) {
        Ok(s) => Ok(s),
        Err(e) if e.is_instance_of::<pyo3::exceptions::PyUnicodeDecodeError>(py) => {
            Err(PyTypeError::new_err(value.unbind()))
        }
        Err(e) => Err(e),
    }
}

/// Coerce a str/utf-8-bytes value to utf-8 bytes. Mirrors `osutils.safe_utf8`
/// (raises TypeError on invalid utf-8 bytes).
#[pyfunction]
fn safe_utf8<'py>(py: Python<'py>, value: Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    if let Ok(b) = value.downcast::<PyBytes>() {
        // Validate utf-8, matching the Python guard.
        if std::str::from_utf8(b.as_bytes()).is_err() {
            return Err(PyTypeError::new_err(value.unbind()));
        }
        return Ok(value);
    }
    value.call_method1("encode", ("utf-8",))
}

/// A file-like object backed by an iterator of byte chunks, supporting
/// read/readline/readlines and line iteration. Mirrors `osutils.IterableFile`.
#[pyclass(name = "IterableFile", module = "bzrformats._bzr_rs.osutils")]
struct PyIterableFile {
    iter: Py<PyAny>,
    buf: Vec<u8>,
}

impl PyIterableFile {
    /// Pull the next chunk from the iterator into `buf`; return false at end.
    fn fill_one(&mut self, py: Python<'_>) -> PyResult<bool> {
        match self.iter.bind(py).call_method0("__next__") {
            Ok(chunk) => {
                self.buf
                    .extend_from_slice(chunk.downcast::<PyBytes>()?.as_bytes());
                Ok(true)
            }
            Err(e) if e.is_instance_of::<pyo3::exceptions::PyStopIteration>(py) => Ok(false),
            Err(e) => Err(e),
        }
    }
}

#[pymethods]
impl PyIterableFile {
    #[new]
    fn new(py: Python<'_>, iterable: Bound<'_, PyAny>) -> PyResult<Self> {
        let iter = py
            .import("builtins")?
            .getattr("iter")?
            .call1((iterable,))?
            .unbind();
        Ok(PyIterableFile {
            iter,
            buf: Vec::new(),
        })
    }

    #[pyo3(signature = (size=-1))]
    fn read<'py>(&mut self, py: Python<'py>, size: isize) -> PyResult<Bound<'py, PyBytes>> {
        if size < 0 {
            while self.fill_one(py)? {}
            let out = PyBytes::new(py, &self.buf);
            self.buf.clear();
            return Ok(out);
        }
        let size = size as usize;
        while self.buf.len() < size {
            if !self.fill_one(py)? {
                break;
            }
        }
        let take = size.min(self.buf.len());
        let out = PyBytes::new(py, &self.buf[..take]);
        self.buf.drain(..take);
        Ok(out)
    }

    fn readline<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        loop {
            if let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
                let out = PyBytes::new(py, &self.buf[..=pos]);
                self.buf.drain(..=pos);
                return Ok(out);
            }
            if !self.fill_one(py)? {
                let out = PyBytes::new(py, &self.buf);
                self.buf.clear();
                return Ok(out);
            }
        }
    }

    fn readlines<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let out = PyList::empty(py);
        loop {
            let line = self.readline(py)?;
            if line.as_bytes().is_empty() {
                break;
            }
            out.append(line)?;
        }
        Ok(out)
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(
        mut slf: PyRefMut<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let line = slf.readline(py)?;
        if line.as_bytes().is_empty() {
            Ok(None)
        } else {
            Ok(Some(line))
        }
    }
}

pub fn _osutils_rs(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(split_lines, m)?)?;
    m.add_function(wrap_pyfunction!(rand_chars, m)?)?;
    m.add_function(wrap_pyfunction!(contains_whitespace, m)?)?;
    m.add_function(wrap_pyfunction!(chunks_to_lines, m)?)?;
    m.add_function(wrap_pyfunction!(is_inside, m)?)?;
    m.add_function(wrap_pyfunction!(is_inside_any, m)?)?;
    m.add_function(wrap_pyfunction!(parent_directories, m)?)?;
    m.add_function(wrap_pyfunction!(walkdirs_utf8, m)?)?;
    m.add_function(wrap_pyfunction!(normalizes_filenames, m)?)?;
    m.add_function(wrap_pyfunction!(supports_symlinks, m)?)?;
    m.add_function(wrap_pyfunction!(sha_strings, m)?)?;
    m.add_function(wrap_pyfunction!(sha_string, m)?)?;
    m.add_function(wrap_pyfunction!(sha_file, m)?)?;
    m.add_function(wrap_pyfunction!(splitpath, m)?)?;
    m.add_function(wrap_pyfunction!(file_kind_from_stat_mode, m)?)?;
    m.add_function(wrap_pyfunction!(safe_unicode, m)?)?;
    m.add_function(wrap_pyfunction!(safe_utf8, m)?)?;
    m.add_class::<PyIterableFile>()?;
    Ok(())
}

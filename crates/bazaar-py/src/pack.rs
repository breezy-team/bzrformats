use bazaar::pack;
use pyo3::exceptions::{PyStopIteration, PyTypeError, PyValueError};
use pyo3::import_exception;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyTuple};
use std::sync::{Arc, Mutex};

import_exception!(bzrformats.pack, ContainerHasExcessDataError);
import_exception!(bzrformats.pack, DuplicateRecordNameError);
import_exception!(bzrformats.pack, InvalidRecordError);
import_exception!(bzrformats.pack, UnexpectedEndOfContainerError);
import_exception!(bzrformats.pack, UnknownContainerFormatError);
import_exception!(bzrformats.pack, UnknownRecordTypeError);

fn pack_err_to_py(err: pack::PackError) -> PyErr {
    Python::attach(|py| match err {
        pack::PackError::InvalidName(n) => {
            let bytes = PyBytes::new(py, &n);
            InvalidRecordError::new_err((format!("{:?} is not a valid name.", bytes),))
        }
        pack::PackError::UnknownContainerFormat(line) => {
            UnknownContainerFormatError::new_err((PyBytes::new(py, &line).unbind(),))
        }
        pack::PackError::UnknownRecordType(b) => {
            UnknownRecordTypeError::new_err((PyBytes::new(py, &[b]).unbind(),))
        }
        pack::PackError::InvalidRecord(reason) => InvalidRecordError::new_err((reason,)),
    })
}

fn extract_names(names: &Bound<PyAny>) -> PyResult<Vec<Vec<Vec<u8>>>> {
    let mut out = Vec::new();
    for name_tuple in names.try_iter()? {
        let name_tuple = name_tuple?;
        let mut parts = Vec::new();
        for part in name_tuple.try_iter()? {
            let part = part?;
            let bytes = part
                .cast_into::<PyBytes>()
                .map_err(|_| PyTypeError::new_err("name parts must be bytes"))?;
            parts.push(bytes.as_bytes().to_vec());
        }
        out.push(parts);
    }
    Ok(out)
}

fn names_to_py<'py>(py: Python<'py>, names: &[Vec<Vec<u8>>]) -> PyResult<Bound<'py, PyList>> {
    let tuples: Vec<Bound<PyTuple>> = names
        .iter()
        .map(|nt| {
            let parts: Vec<Bound<PyBytes>> = nt.iter().map(|p| PyBytes::new(py, p)).collect();
            PyTuple::new(py, parts)
        })
        .collect::<PyResult<_>>()?;
    PyList::new(py, tuples)
}

fn record_to_py<'py>(py: Python<'py>, record: pack::Record) -> PyResult<Bound<'py, PyTuple>> {
    let (names, body) = record;
    let names_list = names_to_py(py, &names)?;
    PyTuple::new(
        py,
        [names_list.into_any(), PyBytes::new(py, &body).into_any()],
    )
}

/// Rust-backed port of `bzrformats.pack.ContainerSerialiser`. All methods
/// return bytes; the class is stateless aside from being a namespace.
#[pyclass(module = "bzrformats._bzr_rs.pack")]
struct ContainerSerialiser;

#[pymethods]
impl ContainerSerialiser {
    #[new]
    fn new() -> Self {
        ContainerSerialiser
    }

    fn begin<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &pack::begin())
    }

    fn end<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, pack::end())
    }

    fn bytes_header<'py>(
        &self,
        py: Python<'py>,
        length: usize,
        names: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let names = extract_names(&names)?;
        let out = pack::bytes_header(length, &names).map_err(pack_err_to_py)?;
        Ok(PyBytes::new(py, &out))
    }

    fn bytes_record<'py>(
        &self,
        py: Python<'py>,
        bytes: &[u8],
        names: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let names = extract_names(&names)?;
        let out = pack::bytes_record(bytes, &names).map_err(pack_err_to_py)?;
        Ok(PyBytes::new(py, &out))
    }
}

/// Rust-backed port of `bzrformats.pack.ContainerPushParser`.
#[pyclass(module = "bzrformats._bzr_rs.pack")]
struct ContainerPushParser {
    inner: pack::ContainerPushParser,
}

#[pymethods]
impl ContainerPushParser {
    #[new]
    fn new() -> Self {
        Self {
            inner: pack::ContainerPushParser::new(),
        }
    }

    #[getter]
    fn finished(&self) -> bool {
        self.inner.finished()
    }

    fn accept_bytes(&mut self, bytes: &[u8]) -> PyResult<()> {
        self.inner.accept_bytes(bytes).map_err(pack_err_to_py)
    }

    #[pyo3(signature = (max = None))]
    fn read_pending_records<'py>(
        &mut self,
        py: Python<'py>,
        max: Option<usize>,
    ) -> PyResult<Bound<'py, PyList>> {
        let records = self.inner.read_pending_records(max);
        let tuples: Vec<Bound<PyTuple>> = records
            .into_iter()
            .map(|r| record_to_py(py, r))
            .collect::<PyResult<_>>()?;
        PyList::new(py, tuples)
    }

    fn read_size_hint(&self) -> usize {
        self.inner.read_size_hint()
    }
}

/// Validate a name per `pack._check_name` — rejects whitespace bytes.
#[pyfunction]
#[pyo3(name = "_check_name")]
fn py_check_name(name: &[u8]) -> PyResult<()> {
    pack::check_name(name).map_err(|e| match e {
        pack::PackError::InvalidName(_) => Python::attach(|py| {
            InvalidRecordError::new_err((format!(
                "{:?} is not a valid name.",
                PyBytes::new(py, name)
            ),))
        }),
        _ => PyValueError::new_err(e.to_string()),
    })
}

/// Validate a name's UTF-8 encoding per `pack._check_name_encoding`.
#[pyfunction]
#[pyo3(name = "_check_name_encoding")]
fn py_check_name_encoding(name: &[u8]) -> PyResult<()> {
    pack::check_name_encoding(name).map_err(|e| match e {
        pack::PackError::InvalidRecord(reason) => InvalidRecordError::new_err((reason,)),
        _ => PyValueError::new_err(e.to_string()),
    })
}

/// Rust-backed port of `bzrformats.pack.ContainerWriter`.
///
/// Accepts a Python callable (`write_func`) and pushes serialised bytes
/// into it. The callable is mutable so tests can swap it out (the existing
/// test suite does this via `self.writer.write_func = ...`).
#[pyclass(module = "bzrformats._bzr_rs.pack")]
struct ContainerWriter {
    write_func: Py<PyAny>,
    /// Records below this many bytes coalesce header+body into one write.
    /// Exposed under the Python attribute name `_JOIN_WRITES_THRESHOLD`
    /// so the existing tests can mutate it.
    join_writes_threshold: usize,
    current_offset: u64,
    records_written: u64,
}

#[pymethods]
impl ContainerWriter {
    #[new]
    fn new(write_func: Py<PyAny>) -> Self {
        Self {
            write_func,
            join_writes_threshold: pack::DEFAULT_JOIN_WRITES_THRESHOLD,
            current_offset: 0,
            records_written: 0,
        }
    }

    #[getter]
    fn write_func(&self, py: Python) -> Py<PyAny> {
        self.write_func.clone_ref(py)
    }

    #[setter]
    fn set_write_func(&mut self, value: Py<PyAny>) {
        self.write_func = value;
    }

    #[getter(_JOIN_WRITES_THRESHOLD)]
    fn get_join_writes_threshold(&self) -> usize {
        self.join_writes_threshold
    }

    #[setter(_JOIN_WRITES_THRESHOLD)]
    fn set_join_writes_threshold(&mut self, value: usize) {
        self.join_writes_threshold = value;
    }

    #[getter]
    fn current_offset(&self) -> u64 {
        self.current_offset
    }

    #[getter]
    fn records_written(&self) -> u64 {
        self.records_written
    }

    fn begin(&mut self, py: Python) -> PyResult<()> {
        self.do_write(py, &pack::begin())
    }

    fn end(&mut self, py: Python) -> PyResult<()> {
        self.do_write(py, pack::end())
    }

    fn add_bytes_record<'py>(
        &mut self,
        py: Python<'py>,
        chunks: Bound<'py, PyAny>,
        length: usize,
        names: Bound<'py, PyAny>,
    ) -> PyResult<(u64, u64)> {
        let names = extract_names(&names)?;
        let header = pack::bytes_header(length, &names).map_err(pack_err_to_py)?;
        let start = self.current_offset;
        if length < self.join_writes_threshold {
            // Coalesce into a single write call.
            let mut buf = Vec::with_capacity(header.len() + length);
            buf.extend_from_slice(&header);
            for chunk in chunks.try_iter()? {
                let chunk = chunk?;
                let b = chunk
                    .cast_into::<PyBytes>()
                    .map_err(|_| PyTypeError::new_err("chunks must yield bytes"))?;
                buf.extend_from_slice(b.as_bytes());
            }
            self.do_write(py, &buf)?;
        } else {
            self.do_write(py, &header)?;
            for chunk in chunks.try_iter()? {
                let chunk = chunk?;
                let b = chunk
                    .cast_into::<PyBytes>()
                    .map_err(|_| PyTypeError::new_err("chunks must yield bytes"))?;
                let bytes = b.as_bytes().to_vec();
                self.do_write(py, &bytes)?;
            }
        }
        self.records_written += 1;
        Ok((start, self.current_offset - start))
    }
}

impl ContainerWriter {
    fn do_write(&mut self, py: Python, bytes: &[u8]) -> PyResult<()> {
        let n = bytes.len() as u64;
        self.write_func.call1(py, (PyBytes::new(py, bytes),))?;
        self.current_offset += n;
        Ok(())
    }
}

/// Source: a Python file-like that we drive via its `read(n)` and
/// `readline()` methods. This matches the Python `ContainerReader`'s
/// approach exactly — including avoiding speculative buffering, which
/// upstream wrappers like `ReadVFile` cannot tolerate.
struct PyFileSource(Py<PyAny>);

impl PyFileSource {
    /// Read up to `n` bytes via `source.read(n)`. Returns the bytes read.
    fn read(&self, py: Python<'_>, n: usize) -> PyResult<Vec<u8>> {
        let result = self.0.call_method1(py, "read", (n,))?;
        let bytes = result.extract::<Bound<PyBytes>>(py)?;
        Ok(bytes.as_bytes().to_vec())
    }

    /// Read a line via `source.readline()`. Returns bytes including the
    /// trailing `\n` (or short if EOF reached).
    fn readline(&self, py: Python<'_>) -> PyResult<Vec<u8>> {
        let result = self.0.call_method0(py, "readline")?;
        let bytes = result.extract::<Bound<PyBytes>>(py)?;
        Ok(bytes.as_bytes().to_vec())
    }
}

type SharedSource = Arc<Mutex<PyFileSource>>;

fn build_source(f: Py<PyAny>) -> SharedSource {
    Arc::new(Mutex::new(PyFileSource(f)))
}

/// Read a `\n`-terminated line. Strips the trailing newline. Returns
/// `Err(UnexpectedEof)` if the source returns a line without one (i.e.
/// hit EOF mid-line).
fn read_one_line(py: Python<'_>, source: &PyFileSource) -> Result<Vec<u8>, ReadStreamError> {
    let mut line = source.readline(py).map_err(ReadStreamError::Py)?;
    if line.is_empty() {
        // Distinguish clean EOF from line-without-newline: the caller
        // decides which is acceptable.
        return Err(ReadStreamError::Eof);
    }
    if line.last() != Some(&b'\n') {
        return Err(ReadStreamError::Eof);
    }
    line.pop();
    Ok(line)
}

/// Read exactly `n` bytes from the source, calling `source.read` until
/// satisfied or EOF.
fn read_exact_n(
    py: Python<'_>,
    source: &PyFileSource,
    n: usize,
) -> Result<Vec<u8>, ReadStreamError> {
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        let chunk = source
            .read(py, n - out.len())
            .map_err(ReadStreamError::Py)?;
        if chunk.is_empty() {
            return Err(ReadStreamError::Eof);
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

/// Read a single byte, returning `None` on clean EOF.
fn read_one_byte(py: Python<'_>, source: &PyFileSource) -> Result<Option<u8>, ReadStreamError> {
    let chunk = source.read(py, 1).map_err(ReadStreamError::Py)?;
    if chunk.is_empty() {
        Ok(None)
    } else {
        Ok(Some(chunk[0]))
    }
}

/// Errors emitted while driving a Python file-like.
enum ReadStreamError {
    Py(PyErr),
    Eof,
    Pack(pack::PackError),
}

impl From<pack::PackError> for ReadStreamError {
    fn from(e: pack::PackError) -> Self {
        Self::Pack(e)
    }
}

impl ReadStreamError {
    fn into_pyerr(self) -> PyErr {
        match self {
            ReadStreamError::Py(e) => e,
            ReadStreamError::Eof => UnexpectedEndOfContainerError::new_err(()),
            ReadStreamError::Pack(e) => pack_err_to_py(e),
        }
    }
}

/// Parse the prelude (length + name list) of a Bytes record from a Python
/// source. Returns `(names, length)`.
fn read_bytes_record_prelude(
    py: Python<'_>,
    source: &PyFileSource,
) -> Result<(Vec<Vec<Vec<u8>>>, usize), ReadStreamError> {
    let length_line = read_one_line(py, source)?;
    let s = std::str::from_utf8(&length_line).map_err(|_| {
        ReadStreamError::Pack(pack::PackError::InvalidRecord(format!(
            "{:?} is not a valid length.",
            length_line
        )))
    })?;
    let length: usize = s.parse().map_err(|_| {
        ReadStreamError::Pack(pack::PackError::InvalidRecord(format!(
            "{:?} is not a valid length.",
            length_line
        )))
    })?;

    let mut names: Vec<Vec<Vec<u8>>> = Vec::new();
    loop {
        let line = read_one_line(py, source)?;
        if line.is_empty() {
            break;
        }
        let parts: Vec<Vec<u8>> = line.split(|&b| b == 0).map(|p| p.to_vec()).collect();
        for part in &parts {
            pack::check_name(part)?;
        }
        names.push(parts);
    }
    Ok((names, length))
}

/// Read the format header line and verify it.
fn read_format_line(py: Python<'_>, source: &PyFileSource) -> Result<(), ReadStreamError> {
    let line = read_one_line(py, source)?;
    if line != pack::FORMAT_ONE {
        return Err(ReadStreamError::Pack(
            pack::PackError::UnknownContainerFormat(line),
        ));
    }
    Ok(())
}

/// Rust-backed port of `bzrformats.pack.ContainerReader`.
#[pyclass(module = "bzrformats._bzr_rs.pack")]
struct ContainerReader {
    source: Option<SharedSource>,
    format_read: bool,
}

#[pymethods]
impl ContainerReader {
    #[new]
    fn new(py: Python<'_>, source: Py<PyAny>) -> PyResult<Self> {
        // Match the Python constructor: it doesn't touch the source even
        // if it's None.
        let source = if source.is_none(py) {
            None
        } else {
            Some(build_source(source))
        };
        Ok(Self {
            source,
            format_read: false,
        })
    }

    fn iter_records<'py>(slf: &Bound<'py, Self>, py: Python<'py>) -> PyResult<Py<RecordIter>> {
        Self::iter_inner(slf, py, true)
    }

    fn iter_record_objects<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Py<RecordIter>> {
        Self::iter_inner(slf, py, false)
    }

    fn validate(&mut self, py: Python) -> PyResult<()> {
        let source = self
            .source
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("reader has no source"))?
            .clone();
        let was_format_read = self.format_read;
        let result: Result<(), ReadStreamError> = (|| {
            let guard = source.lock().unwrap();
            let s = &*guard;
            if !was_format_read {
                read_format_line(py, s)?;
            }
            let mut seen: std::collections::HashSet<Vec<Vec<u8>>> =
                std::collections::HashSet::new();
            loop {
                match read_one_byte(py, s)? {
                    None => return Err(ReadStreamError::Eof),
                    Some(b'B') => {
                        let (names, length) = read_bytes_record_prelude(py, s)?;
                        for name_tuple in &names {
                            for name in name_tuple {
                                pack::check_name_encoding(name)?;
                            }
                            if !seen.insert(name_tuple.clone()) {
                                let first = name_tuple.first().cloned().unwrap_or_default();
                                return Err(ReadStreamError::Py(Python::attach(|py| {
                                    DuplicateRecordNameError::new_err((
                                        PyBytes::new(py, &first).unbind(),
                                    ))
                                })));
                            }
                        }
                        // Drain the body.
                        let _ = read_exact_n(py, s, length)?;
                    }
                    Some(b'E') => break,
                    Some(other) => {
                        return Err(ReadStreamError::Pack(pack::PackError::UnknownRecordType(
                            other,
                        )));
                    }
                }
            }
            // Excess-data check.
            let tail = s.read(py, 1).map_err(ReadStreamError::Py)?;
            if !tail.is_empty() {
                return Err(ReadStreamError::Py(Python::attach(|py| {
                    ContainerHasExcessDataError::new_err((PyBytes::new(py, &tail).unbind(),))
                })));
            }
            Ok(())
        })();
        result.map_err(ReadStreamError::into_pyerr)?;
        self.format_read = true;
        Ok(())
    }
}

impl ContainerReader {
    fn iter_inner<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        yield_bytes: bool,
    ) -> PyResult<Py<RecordIter>> {
        let mut s = slf.borrow_mut();
        let source = s
            .source
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("reader has no source"))?
            .clone();
        // Match Python: _read_format runs eagerly so format errors surface
        // before the iterator is returned.
        if !s.format_read {
            let result = {
                let guard = source.lock().unwrap();
                read_format_line(py, &guard)
            };
            result.map_err(ReadStreamError::into_pyerr)?;
            s.format_read = true;
        }
        Py::new(
            py,
            RecordIter {
                source,
                format_read: true,
                yield_bytes,
                done: false,
            },
        )
    }
}

/// Iterator returned by `iter_records` / `iter_record_objects`. Each
/// record is read eagerly (prelude + body), so the resulting `read_bytes`
/// callable / `BytesRecordObject` is independent of the underlying source.
#[pyclass(module = "bzrformats._bzr_rs.pack")]
struct RecordIter {
    source: SharedSource,
    format_read: bool,
    yield_bytes: bool,
    done: bool,
}

#[pymethods]
impl RecordIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if slf.done {
            return Err(PyStopIteration::new_err(()));
        }
        let source = slf.source.clone();
        let was_format_read = slf.format_read;

        let result: Result<Option<pack::Record>, ReadStreamError> = (|| {
            let guard = source.lock().unwrap();
            let s = &*guard;
            if !was_format_read {
                read_format_line(py, s)?;
            }
            // Read the record kind. A clean EOF here means the stream
            // ended without an end marker.
            let kind = match read_one_byte(py, s)? {
                None => return Err(ReadStreamError::Eof),
                Some(b) => b,
            };
            match kind {
                b'B' => {
                    let (names, length) = read_bytes_record_prelude(py, s)?;
                    let body = read_exact_n(py, s, length)?;
                    Ok(Some((names, body)))
                }
                b'E' => Ok(None),
                other => Err(ReadStreamError::Pack(pack::PackError::UnknownRecordType(
                    other,
                ))),
            }
        })();

        slf.format_read = true;
        match result.map_err(ReadStreamError::into_pyerr)? {
            None => {
                slf.done = true;
                Err(PyStopIteration::new_err(()))
            }
            Some(record) => {
                if slf.yield_bytes {
                    let names_list = names_to_py(py, &record.0)?;
                    let bro = Py::new(py, BytesRecordObject::new(record.0, record.1))?;
                    let read_attr = bro.bind(py).getattr("_read_content")?;
                    Ok(
                        PyTuple::new(py, [names_list.into_any().unbind(), read_attr.unbind()])?
                            .into_any()
                            .unbind(),
                    )
                } else {
                    Ok(Py::new(py, BytesRecordObject::new(record.0, record.1))?.into_any())
                }
            }
        }
    }
}

/// Wraps a single record's prelude + body. Created either by
/// `iter_record_objects` (returned directly) or `iter_records` (returned
/// indirectly: its `_read_content` method is the legacy callable).
#[pyclass(module = "bzrformats._bzr_rs.pack")]
struct BytesRecordObject {
    names: Vec<Vec<Vec<u8>>>,
    body: Vec<u8>,
    /// Bytes already drained by previous `_read_content` calls.
    consumed: usize,
}

impl BytesRecordObject {
    fn new(names: Vec<Vec<Vec<u8>>>, body: Vec<u8>) -> Self {
        Self {
            names,
            body,
            consumed: 0,
        }
    }
}

#[pymethods]
impl BytesRecordObject {
    /// `BytesRecordReader.read()` returns `(names, callable)`. The
    /// callable is `_read_content`.
    fn read<'py>(slf: &Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        let s = slf.borrow();
        let names_list = names_to_py(py, &s.names)?;
        let read_attr = slf.getattr("_read_content")?;
        PyTuple::new(py, [names_list.into_any(), read_attr])
    }

    /// Drain remaining bytes and verify name encodings.
    fn validate(&mut self) -> PyResult<()> {
        for name_tuple in &self.names {
            for name in name_tuple {
                pack::check_name_encoding(name).map_err(|e| match e {
                    pack::PackError::InvalidRecord(reason) => {
                        InvalidRecordError::new_err((reason,))
                    }
                    _ => PyValueError::new_err(e.to_string()),
                })?;
            }
        }
        self.consumed = self.body.len();
        Ok(())
    }

    #[pyo3(signature = (max_length = None))]
    fn _read_content<'py>(
        &mut self,
        py: Python<'py>,
        max_length: Option<usize>,
    ) -> Bound<'py, PyBytes> {
        let remaining = self.body.len() - self.consumed;
        let want = match max_length {
            Some(n) => n.min(remaining),
            None => remaining,
        };
        let slice = &self.body[self.consumed..self.consumed + want];
        self.consumed += want;
        PyBytes::new(py, slice)
    }
}

/// Rust-backed port of `bzrformats.pack.BytesRecordReader`. Constructed
/// directly from a Python file-like; `read()` parses the prelude lazily
/// and returns `(names, callable)` like the Python class.
#[pyclass(module = "bzrformats._bzr_rs.pack")]
struct BytesRecordReader {
    source: Option<SharedSource>,
    record: Option<Py<BytesRecordObject>>,
}

#[pymethods]
impl BytesRecordReader {
    #[new]
    fn new(py: Python<'_>, source: Py<PyAny>) -> Self {
        let source = if source.is_none(py) {
            None
        } else {
            Some(build_source(source))
        };
        Self {
            source,
            record: None,
        }
    }

    fn read<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        let source = self
            .source
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("reader has no source"))?
            .clone();
        let result: Result<pack::Record, ReadStreamError> = (|| {
            let guard = source.lock().unwrap();
            let s = &*guard;
            let (names, length) = read_bytes_record_prelude(py, s)?;
            let body = read_exact_n(py, s, length)?;
            Ok((names, body))
        })();
        let record = result.map_err(ReadStreamError::into_pyerr)?;
        let names_list = names_to_py(py, &record.0)?;
        let bro = Py::new(py, BytesRecordObject::new(record.0, record.1))?;
        let read_attr = bro.bind(py).getattr("_read_content")?;
        self.record = Some(bro);
        PyTuple::new(py, [names_list.into_any(), read_attr])
    }

    fn validate(&mut self, py: Python<'_>) -> PyResult<()> {
        // Match Python: read() the record (which validates _check_name on
        // names), then read all bytes, then validate the names' UTF-8.
        let _ = self.read(py)?;
        let bro = self.record.as_ref().expect("read populates record");
        let mut bro_borrow = bro.borrow_mut(py);
        bro_borrow.validate()
    }
}

pub fn _pack_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "pack")?;
    m.add_class::<ContainerSerialiser>()?;
    m.add_class::<ContainerPushParser>()?;
    m.add_class::<ContainerWriter>()?;
    m.add_class::<ContainerReader>()?;
    m.add_class::<RecordIter>()?;
    m.add_class::<BytesRecordObject>()?;
    m.add_class::<BytesRecordReader>()?;
    m.add_function(wrap_pyfunction!(py_check_name, &m)?)?;
    m.add_function(wrap_pyfunction!(py_check_name_encoding, &m)?)?;
    m.add("FORMAT_ONE", PyBytes::new(py, pack::FORMAT_ONE))?;
    Ok(m)
}

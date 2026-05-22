use bazaar::groupcompress::compressor::GroupCompressor;
use bazaar::versionedfile::Key;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PySet, PyTuple};
use pyo3::wrap_pyfunction;
use std::borrow::Cow;
use std::convert::TryInto;

pyo3::import_exception!(bzrformats.errors, ObjectNotLocked);
pyo3::import_exception!(bzrformats.errors, ReadOnlyError);
pyo3::import_exception!(bzrformats.errors, RevisionNotPresent);

fn extract_key_segments(obj: &Bound<PyAny>) -> PyResult<Vec<Vec<u8>>> {
    let tuple = obj.cast::<PyTuple>().map_err(|_| {
        PyValueError::new_err("sort_gc_optimal keys and parents must be tuples of bytes")
    })?;
    let mut out = Vec::with_capacity(tuple.len());
    for item in tuple.iter() {
        let b = item
            .cast::<PyBytes>()
            .map_err(|_| PyValueError::new_err("sort_gc_optimal keys must contain only bytes"))?;
        out.push(b.as_bytes().to_vec());
    }
    Ok(out)
}

/// Sort and group the keys in `parent_map` into groupcompress order.
///
/// Returns a list of keys in reverse-topological order, grouped by the
/// first segment of each key. Single-segment keys share an empty prefix.
#[pyfunction]
fn sort_gc_optimal<'py>(
    py: Python<'py>,
    parent_map: &Bound<'py, PyDict>,
) -> PyResult<Vec<Bound<'py, PyTuple>>> {
    let mut input = Vec::with_capacity(parent_map.len());
    for (key, value) in parent_map.iter() {
        let k = extract_key_segments(&key)?;
        let parents_tuple = value
            .cast::<PyTuple>()
            .map_err(|_| PyValueError::new_err("sort_gc_optimal values must be tuples of keys"))?;
        let mut parents = Vec::with_capacity(parents_tuple.len());
        for parent in parents_tuple.iter() {
            parents.push(extract_key_segments(&parent)?);
        }
        input.push((k, parents));
    }
    let sorted = bazaar::groupcompress::sort::sort_gc_optimal(input);
    sorted
        .into_iter()
        .map(|segments| PyTuple::new(py, segments.into_iter().map(|s| PyBytes::new(py, &s))))
        .collect()
}

#[pyfunction]
fn encode_base128_int(py: Python, value: u128) -> PyResult<Bound<PyBytes>> {
    let ret = bazaar::groupcompress::delta::encode_base128_int(value);
    Ok(PyBytes::new(py, &ret))
}

#[pyfunction]
fn decode_base128_int(value: Vec<u8>) -> PyResult<(u128, usize)> {
    Ok(bazaar::groupcompress::delta::decode_base128_int(&value))
}

#[pyfunction]
fn apply_delta(py: Python, basis: Vec<u8>, delta: Vec<u8>) -> PyResult<Bound<PyBytes>> {
    bazaar::groupcompress::delta::apply_delta(&basis, &delta)
        .map_err(|e| PyErr::new::<PyValueError, _>(format!("Invalid delta: {}", e)))
        .map(|x| PyBytes::new(py, &x))
}

#[pyfunction]
fn decode_copy_instruction(data: Vec<u8>, cmd: u8, pos: usize) -> PyResult<(usize, usize, usize)> {
    let ret = bazaar::groupcompress::delta::decode_copy_instruction(&data, cmd, pos);
    if ret.is_err() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Invalid copy instruction",
        ));
    }
    let ret = ret.unwrap();

    Ok((ret.0, ret.1, ret.2))
}

#[pyfunction]
#[pyo3(signature = (source, delta_start, delta_end))]
fn apply_delta_to_source<'a>(
    py: Python<'a>,
    source: &'a [u8],
    delta_start: usize,
    delta_end: usize,
) -> PyResult<Bound<'a, PyBytes>> {
    bazaar::groupcompress::delta::apply_delta_to_source(source, delta_start, delta_end)
        .map_err(|e| PyErr::new::<PyValueError, _>(format!("Invalid delta: {}", e)))
        .map(|x| PyBytes::new(py, &x))
}

#[pyfunction]
fn encode_copy_instruction(py: Python, offset: usize, length: usize) -> PyResult<Bound<PyBytes>> {
    let ret = bazaar::groupcompress::delta::encode_copy_instruction(offset, length);
    Ok(PyBytes::new(py, &ret))
}

#[pyfunction]
fn make_line_delta<'a>(
    py: Python<'a>,
    source_bytes: &'a [u8],
    target_bytes: &'a [u8],
) -> Bound<'a, PyBytes> {
    PyBytes::new(
        py,
        bazaar::groupcompress::line_delta::make_delta(source_bytes, target_bytes)
            .flat_map(|x| x.into_owned())
            .collect::<Vec<_>>()
            .as_slice(),
    )
}

#[pyfunction]
fn make_rabin_delta<'a>(
    py: Python<'a>,
    source_bytes: &'a [u8],
    target_bytes: &'a [u8],
) -> Bound<'a, PyBytes> {
    PyBytes::new(
        py,
        bazaar::groupcompress::rabin_delta::make_delta(source_bytes, target_bytes).as_slice(),
    )
}

#[pyclass]
pub struct LinesDeltaIndex(bazaar::groupcompress::line_delta::LinesDeltaIndex);

#[pymethods]
impl LinesDeltaIndex {
    #[new]
    fn new(lines: Vec<Vec<u8>>) -> Self {
        let index = bazaar::groupcompress::line_delta::LinesDeltaIndex::new(lines);
        Self(index)
    }

    #[getter]
    fn lines<'a>(&self, py: Python<'a>) -> Vec<Bound<'a, PyBytes>> {
        self.0
            .lines()
            .iter()
            .map(|x| PyBytes::new(py, x.as_ref()))
            .collect()
    }

    #[pyo3(signature = (source, bytes_length, soft = None))]
    fn make_delta<'a>(
        &'a self,
        py: Python<'a>,
        source: Vec<Vec<Vec<u8>>>,
        bytes_length: usize,
        soft: Option<bool>,
    ) -> (Vec<Bound<'a, PyBytes>>, Vec<bool>) {
        let source: Vec<Cow<[u8]>> = source
            .iter()
            .map(|x| Cow::Owned(x.iter().flatten().copied().collect::<Vec<_>>()))
            .collect::<Vec<_>>();
        let (delta, index) = self.0.make_delta(source.as_slice(), bytes_length, soft);
        (
            delta
                .into_iter()
                .map(|x| PyBytes::new(py, x.as_ref()))
                .collect(),
            index,
        )
    }

    fn extend_lines(&mut self, lines: Vec<Vec<u8>>, index: Vec<bool>) -> PyResult<()> {
        self.0.extend_lines(lines.as_slice(), index.as_slice());
        Ok(())
    }

    #[getter]
    fn endpoint(&self) -> usize {
        self.0.endpoint()
    }
}

#[pyclass]
struct GroupCompressBlock {
    inner: bazaar::groupcompress::block::GroupCompressBlock,
    /// Cached PyBytes for `_z_content`. Matches Python's semantics where
    /// `b"".join((x,))` returns `x` itself — tests do `assertIs` against
    /// the same block accessed twice.
    z_content_cache: Option<Py<PyBytes>>,
}

impl GroupCompressBlock {
    fn invalidate_cache(&mut self) {
        self.z_content_cache = None;
    }
}

#[pymethods]
impl GroupCompressBlock {
    #[new]
    fn new() -> Self {
        Self {
            inner: bazaar::groupcompress::block::GroupCompressBlock::new(),
            z_content_cache: None,
        }
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    #[getter]
    fn _z_content<'a>(&mut self, py: Python<'a>) -> PyResult<Bound<'a, PyBytes>> {
        if let Some(cached) = &self.z_content_cache {
            return Ok(cached.bind(py).clone());
        }
        let ret = self.inner.z_content();
        let bound = PyBytes::new(py, &ret);
        self.z_content_cache = Some(bound.clone().unbind());
        Ok(bound)
    }

    #[getter]
    fn _content<'a>(&mut self, py: Python<'a>) -> PyResult<Option<Bound<'a, PyBytes>>> {
        let ret = self.inner.content();
        Ok(ret.map(|x| PyBytes::new(py, x)))
    }

    #[getter]
    fn _content_length(&self) -> Option<usize> {
        self.inner.content_length()
    }

    #[setter(_content_length)]
    fn set_content_length_py(&mut self, value: usize) {
        self.inner.set_content_length(value);
    }

    #[getter]
    fn _z_content_length(&self) -> Option<usize> {
        self.inner.z_content_length()
    }

    #[setter(_z_content_length)]
    fn set_z_content_length_py(&mut self, value: usize) {
        self.inner.set_z_content_length(value);
    }

    #[setter(_z_content_chunks)]
    fn set_z_content_chunks_py(&mut self, chunks: Vec<Vec<u8>>) {
        self.inner.set_z_content_chunks(chunks);
        self.invalidate_cache();
    }

    /// Test probe: `None` before a streaming decompressor has been created
    /// (or after full content has been realised directly), otherwise
    /// `True`. Matches the Python class's `_z_content_decompressor` attr.
    #[getter]
    fn _z_content_decompressor(&self) -> Option<bool> {
        if self.inner.has_z_content_decompressor() {
            Some(true)
        } else {
            None
        }
    }

    #[setter(_compressor_name)]
    fn set_compressor_name_py(&mut self, name: &str) -> PyResult<()> {
        let kind = match name {
            "zlib" => bazaar::groupcompress::block::CompressorKind::Zlib,
            "lzma" => bazaar::groupcompress::block::CompressorKind::Lzma,
            other => {
                return Err(PyValueError::new_err(format!(
                    "Unknown compressor: {}",
                    other
                )));
            }
        };
        self.inner.set_compressor(kind);
        self.invalidate_cache();
        Ok(())
    }

    #[classmethod]
    fn from_bytes(_type: &pyo3::Bound<pyo3::types::PyType>, data: &[u8]) -> PyResult<Self> {
        let ret = bazaar::groupcompress::block::GroupCompressBlock::from_bytes(data);
        if ret.is_err() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Invalid block",
            ));
        }
        Ok(Self {
            inner: ret.unwrap(),
            z_content_cache: None,
        })
    }

    #[pyo3(signature = (key, start, end, sha1 = None))]
    fn extract<'a>(
        &mut self,
        py: Python<'a>,
        key: Py<PyAny>,
        start: usize,
        end: usize,
        sha1: Option<Py<PyAny>>,
    ) -> PyResult<Vec<Bound<'a, PyBytes>>> {
        let _ = key;
        let _ = sha1;
        let chunks = self
            .inner
            .extract(start, end)
            .map_err(|e| PyValueError::new_err(format!("Error during extract: {:?}", e)))?;
        Ok(chunks
            .into_iter()
            .map(|x| PyBytes::new(py, x.as_ref()))
            .collect())
    }

    fn set_chunked_content(&mut self, data: Vec<Vec<u8>>, length: usize) -> PyResult<()> {
        self.inner.set_chunked_content(data.as_slice(), length);
        self.invalidate_cache();
        Ok(())
    }

    fn set_content(&mut self, content: &[u8]) -> PyResult<()> {
        self.inner.set_content(content);
        self.invalidate_cache();
        Ok(())
    }

    #[pyo3(signature = (kind = None))]
    fn to_chunks<'a>(
        &mut self,
        py: Python<'a>,
        kind: Option<bazaar::groupcompress::block::CompressorKind>,
    ) -> (usize, Vec<Bound<'a, PyBytes>>) {
        // to_chunks may rebuild z_content_chunks internally; invalidate the
        // cached PyBytes so the next _z_content call picks up fresh bytes.
        self.invalidate_cache();
        let (size, chunks) = self.inner.to_chunks(kind);

        let chunks = chunks
            .into_iter()
            .map(|x| PyBytes::new(py, x.as_ref()))
            .collect();

        (size, chunks)
    }

    fn to_bytes<'a>(&mut self, py: Python<'a>) -> PyResult<Bound<'a, PyBytes>> {
        self.invalidate_cache();
        let ret = self.inner.to_bytes();
        Ok(PyBytes::new(py, &ret))
    }

    #[pyo3(signature = (size = None))]
    fn _ensure_content(&mut self, size: Option<usize>) -> PyResult<()> {
        self.inner
            .ensure_content(size)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    #[pyo3(signature = (include_text = None))]
    fn _dump<'a>(
        &mut self,
        py: Python<'a>,
        include_text: Option<bool>,
    ) -> PyResult<Bound<'a, pyo3::types::PyList>> {
        use bazaar::groupcompress::block::{DeltaInfo, DumpInfo};
        use pyo3::types::{PyList, PyTuple};

        let ret = self
            .inner
            .dump(include_text)
            .map_err(|e| PyValueError::new_err(format!("Error during dump: {:?}", e)))?;

        let items: Vec<Bound<PyAny>> = ret
            .into_iter()
            .map(|info| -> PyResult<Bound<PyAny>> {
                match info {
                    DumpInfo::Fulltext { length, text } => {
                        // (b"f", length) or (b"f", length, text) when include_text.
                        let kind = PyBytes::new(py, b"f").into_any();
                        let tuple = if let Some(text) = text {
                            PyTuple::new(
                                py,
                                [
                                    kind,
                                    length.into_pyobject(py)?.into_any(),
                                    PyBytes::new(py, &text).into_any(),
                                ],
                            )?
                        } else {
                            PyTuple::new(py, [kind, length.into_pyobject(py)?.into_any()])?
                        };
                        Ok(tuple.into_any())
                    }
                    DumpInfo::Delta {
                        delta_length,
                        decomp_length,
                        instructions,
                    } => {
                        // (b"d", delta_length, decomp_length, [insts]) where each inst is
                        // (b"c", offset, length) or (b"i", length, text).
                        let inst_items: Vec<Bound<PyAny>> = instructions
                            .into_iter()
                            .map(|inst| -> PyResult<Bound<PyAny>> {
                                let tuple = match inst {
                                    DeltaInfo::Copy {
                                        offset,
                                        length,
                                        text: _,
                                    } => PyTuple::new(
                                        py,
                                        [
                                            PyBytes::new(py, b"c").into_any(),
                                            offset.into_pyobject(py)?.into_any(),
                                            length.into_pyobject(py)?.into_any(),
                                        ],
                                    )?,
                                    DeltaInfo::Insert { length, text } => {
                                        let payload = match text {
                                            Some(t) => PyBytes::new(py, &t),
                                            None => PyBytes::new(py, b""),
                                        };
                                        PyTuple::new(
                                            py,
                                            [
                                                PyBytes::new(py, b"i").into_any(),
                                                length.into_pyobject(py)?.into_any(),
                                                payload.into_any(),
                                            ],
                                        )?
                                    }
                                };
                                Ok(tuple.into_any())
                            })
                            .collect::<PyResult<_>>()?;
                        let inst_list = PyList::new(py, inst_items)?;
                        let tuple = PyTuple::new(
                            py,
                            [
                                PyBytes::new(py, b"d").into_any(),
                                delta_length.into_pyobject(py)?.into_any(),
                                decomp_length.into_pyobject(py)?.into_any(),
                                inst_list.into_any(),
                            ],
                        )?;
                        Ok(tuple.into_any())
                    }
                }
            })
            .collect::<PyResult<_>>()?;
        PyList::new(py, items)
    }
}

#[pyclass]
struct TraditionalGroupCompressor(
    Option<bazaar::groupcompress::compressor::TraditionalGroupCompressor>,
);

#[pymethods]
impl TraditionalGroupCompressor {
    #[new]
    #[allow(unused_variables)]
    #[pyo3(signature = (settings = None))]
    fn new(settings: Option<Py<PyAny>>) -> Self {
        Self(Some(
            bazaar::groupcompress::compressor::TraditionalGroupCompressor::new(),
        ))
    }

    #[getter]
    fn chunks<'a>(&self, py: Python<'a>) -> PyResult<Vec<Bound<'a, PyBytes>>> {
        if let Some(c) = self.0.as_ref() {
            Ok(c.chunks()
                .iter()
                .map(|x| PyBytes::new(py, x.as_ref()))
                .collect())
        } else {
            Err(PyRuntimeError::new_err("Compressor is already finalized"))
        }
    }

    #[getter]
    fn endpoint(&self) -> PyResult<usize> {
        if let Some(c) = self.0.as_ref() {
            Ok(c.endpoint())
        } else {
            Err(PyRuntimeError::new_err("Compressor is already finalized"))
        }
    }

    fn ratio(&self) -> PyResult<f32> {
        if let Some(c) = self.0.as_ref() {
            Ok(c.ratio())
        } else {
            Err(PyRuntimeError::new_err("Compressor is already finalized"))
        }
    }

    fn extract<'a>(
        &self,
        py: Python<'a>,
        key: Vec<Vec<u8>>,
    ) -> PyResult<(Vec<Bound<'a, PyBytes>>, Bound<'a, PyBytes>)> {
        if let Some(c) = self.0.as_ref() {
            let (data, hash) = c
                .extract(&key)
                .map_err(|e| PyValueError::new_err(format!("Error during extract: {:?}", e)))?;
            Ok((
                data.iter().map(|x| PyBytes::new(py, x.as_ref())).collect(),
                PyBytes::new(py, hash.as_bytes()),
            ))
        } else {
            Err(PyRuntimeError::new_err("Compressor is already finalized"))
        }
    }

    fn flush<'a>(&mut self, py: Python<'a>) -> PyResult<(Vec<Bound<'a, PyBytes>>, usize)> {
        if let Some(c) = self.0.take() {
            let (chunks, endpoint) = c.flush();
            Ok((
                chunks
                    .into_iter()
                    .map(|x| PyBytes::new(py, x.as_ref()))
                    .collect(),
                endpoint,
            ))
        } else {
            Err(PyRuntimeError::new_err("Compressor is already finalized"))
        }
    }

    fn flush_without_last<'a>(
        &mut self,
        py: Python<'a>,
    ) -> PyResult<(Vec<Bound<'a, PyBytes>>, usize)> {
        if let Some(c) = self.0.take() {
            let (chunks, endpoint) = c.flush_without_last();
            Ok((
                chunks
                    .into_iter()
                    .map(|x| PyBytes::new(py, x.as_ref()))
                    .collect(),
                endpoint,
            ))
        } else {
            Err(PyRuntimeError::new_err("Compressor is already finalized"))
        }
    }

    #[pyo3(signature = (key, chunks, length, expected_sha = None, nostore_sha = None, soft = None))]
    fn compress<'a>(
        &mut self,
        py: Python<'a>,
        key: Key,
        chunks: Vec<Vec<u8>>,
        length: usize,
        expected_sha: Option<String>,
        nostore_sha: Option<String>,
        soft: Option<bool>,
    ) -> PyResult<(Bound<'a, PyBytes>, usize, usize, &'a str)> {
        let chunks_l = chunks.iter().map(|x| x.as_slice()).collect::<Vec<_>>();
        if let Some(c) = self.0.as_mut() {
            c.compress(
                &key,
                chunks_l.as_slice(),
                length,
                expected_sha,
                nostore_sha,
                soft,
            )
            .map_err(|e| PyValueError::new_err(format!("Error during compress: {:?}", e)))
            .map(|(hash, size, chunks, kind)| {
                (PyBytes::new(py, hash.as_ref()), size, chunks, kind.as_str())
            })
        } else {
            Err(PyRuntimeError::new_err("Compressor is already finalized"))
        }
    }
}

#[pyclass]
struct RabinGroupCompressor(Option<bazaar::groupcompress::compressor::RabinGroupCompressor>);

fn max_bytes_from_settings(settings: Option<&Bound<PyAny>>) -> PyResult<Option<usize>> {
    let Some(settings) = settings else {
        return Ok(None);
    };
    if settings.is_none() {
        return Ok(None);
    }
    let dict = settings.cast::<pyo3::types::PyDict>().map_err(|_| {
        PyValueError::new_err("RabinGroupCompressor settings must be a dict or None")
    })?;
    let Some(value) = dict.get_item("max_bytes_to_index")? else {
        return Ok(None);
    };
    let v: usize = value.extract()?;
    Ok(if v == 0 { None } else { Some(v) })
}

impl RabinGroupCompressor {
    /// Construct a `GroupCompressBlock` Py wrapper around the compressed
    /// chunks produced by a flush. Factored out so `flush` and
    /// `flush_without_last` share the plumbing.
    fn build_block<'a>(
        py: Python<'a>,
        chunks: Vec<Vec<u8>>,
        endpoint: usize,
    ) -> PyResult<Bound<'a, GroupCompressBlock>> {
        let mut inner = bazaar::groupcompress::block::GroupCompressBlock::new();
        inner.set_chunked_content(&chunks, endpoint);
        Bound::new(
            py,
            GroupCompressBlock {
                inner,
                z_content_cache: None,
            },
        )
    }
}

#[pymethods]
impl RabinGroupCompressor {
    #[new]
    #[pyo3(signature = (settings = None))]
    fn new(settings: Option<&Bound<PyAny>>) -> PyResult<Self> {
        let max_bytes_to_index = max_bytes_from_settings(settings)?;
        Ok(Self(Some(
            bazaar::groupcompress::compressor::RabinGroupCompressor::new(max_bytes_to_index),
        )))
    }

    #[getter]
    fn chunks<'a>(&self, py: Python<'a>) -> PyResult<Vec<Bound<'a, PyBytes>>> {
        if let Some(c) = self.0.as_ref() {
            Ok(c.chunks()
                .iter()
                .map(|x| PyBytes::new(py, x.as_ref()))
                .collect())
        } else {
            Err(PyRuntimeError::new_err("Compressor is already finalized"))
        }
    }

    #[getter]
    fn endpoint(&self) -> PyResult<usize> {
        if let Some(c) = self.0.as_ref() {
            Ok(c.endpoint())
        } else {
            Err(PyRuntimeError::new_err("Compressor is already finalized"))
        }
    }

    #[getter]
    fn input_bytes(&self) -> PyResult<usize> {
        if let Some(c) = self.0.as_ref() {
            Ok(c.input_bytes())
        } else {
            Err(PyRuntimeError::new_err("Compressor is already finalized"))
        }
    }

    /// Test probe: read the underlying delta-index byte budget.
    #[getter]
    fn _max_bytes_to_index(&self) -> PyResult<usize> {
        if let Some(c) = self.0.as_ref() {
            Ok(c.max_bytes_to_index().unwrap_or(0))
        } else {
            Err(PyRuntimeError::new_err("Compressor is already finalized"))
        }
    }

    /// Map of key tuple → (start_byte, start_chunk, end_byte, end_chunk).
    #[getter]
    fn labels_deltas<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, pyo3::types::PyDict>> {
        let Some(c) = self.0.as_ref() else {
            return Err(PyRuntimeError::new_err("Compressor is already finalized"));
        };
        let dict = pyo3::types::PyDict::new(py);
        for (k, &(sb, sc, eb, ec)) in c.labels_deltas() {
            let key_tuple =
                pyo3::types::PyTuple::new(py, k.iter().map(|seg| PyBytes::new(py, seg)))?;
            dict.set_item(key_tuple, (sb, sc, eb, ec))?;
        }
        Ok(dict)
    }

    fn ratio(&self) -> PyResult<f32> {
        if let Some(c) = self.0.as_ref() {
            Ok(c.ratio())
        } else {
            Err(PyRuntimeError::new_err("Compressor is already finalized"))
        }
    }

    fn extract<'a>(
        &self,
        py: Python<'a>,
        key: Vec<Vec<u8>>,
    ) -> PyResult<(Vec<Bound<'a, PyBytes>>, Bound<'a, PyBytes>)> {
        if let Some(c) = self.0.as_ref() {
            let (data, hash) = c
                .extract(&key)
                .map_err(|e| PyValueError::new_err(format!("Error during extract: {:?}", e)))?;
            Ok((
                data.iter().map(|x| PyBytes::new(py, x.as_ref())).collect(),
                PyBytes::new(py, hash.as_bytes()),
            ))
        } else {
            Err(PyRuntimeError::new_err("Compressor is already finalized"))
        }
    }

    /// Finish this group, returning a GroupCompressBlock containing the
    /// compressed chunks.
    fn flush<'a>(&mut self, py: Python<'a>) -> PyResult<Bound<'a, GroupCompressBlock>> {
        use bazaar::groupcompress::compressor::GroupCompressor;
        let Some(c) = self.0.take() else {
            return Err(PyRuntimeError::new_err("Compressor is already finalized"));
        };
        let (chunks, endpoint) = c.flush();
        Self::build_block(py, chunks, endpoint)
    }

    fn flush_without_last<'a>(
        &mut self,
        py: Python<'a>,
    ) -> PyResult<Bound<'a, GroupCompressBlock>> {
        use bazaar::groupcompress::compressor::GroupCompressor;
        let Some(c) = self.0.take() else {
            return Err(PyRuntimeError::new_err("Compressor is already finalized"));
        };
        let (chunks, endpoint) = c.flush_without_last();
        Self::build_block(py, chunks, endpoint)
    }

    #[pyo3(signature = (key, chunks, length, expected_sha = None, nostore_sha = None, soft = None))]
    fn compress<'a>(
        &mut self,
        py: Python<'a>,
        key: Key,
        chunks: Vec<Vec<u8>>,
        length: usize,
        expected_sha: Option<Vec<u8>>,
        nostore_sha: Option<Vec<u8>>,
        soft: Option<bool>,
    ) -> PyResult<(Bound<'a, PyBytes>, usize, usize, &'a str)> {
        use bazaar::groupcompress::compressor::GroupCompressor;
        let chunks_l = chunks.iter().map(|x| x.as_slice()).collect::<Vec<_>>();
        let expected_sha = expected_sha
            .map(|b| String::from_utf8(b).map_err(|e| PyValueError::new_err(e.to_string())))
            .transpose()?;
        let nostore_sha = nostore_sha
            .map(|b| String::from_utf8(b).map_err(|e| PyValueError::new_err(e.to_string())))
            .transpose()?;
        let Some(c) = self.0.as_mut() else {
            return Err(PyRuntimeError::new_err("Compressor is already finalized"));
        };
        let (hash, size, chunks, kind) = c.compress(
            &key,
            chunks_l.as_slice(),
            length,
            expected_sha,
            nostore_sha,
            soft,
        )?;
        Ok((PyBytes::new(py, hash.as_ref()), size, chunks, kind.as_str()))
    }
}

/// Parse the outer wire framing of a groupcompress block.
///
/// Returns `(block_bytes, factories)` where `factories` is a list of
/// `(key_tuple, parents_tuple_or_none, start, end)` tuples in record order.
#[pyfunction]
fn parse_wire_header<'py>(
    py: Python<'py>,
    bytes: &'py [u8],
) -> PyResult<(Bound<'py, PyBytes>, Bound<'py, pyo3::types::PyList>)> {
    let frame = bazaar::groupcompress::wire::parse_wire(bytes)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    let block_bytes = PyBytes::new(py, frame.block_bytes);
    let mut entries: Vec<Bound<PyTuple>> = Vec::with_capacity(frame.factories.len());
    for factory in frame.factories {
        let key = PyTuple::new(py, factory.key.iter().map(|s| PyBytes::new(py, s)))?;
        let parents: Bound<PyAny> = match factory.parents {
            None => py.None().into_bound(py),
            Some(parents) => PyTuple::new(
                py,
                parents
                    .iter()
                    .map(|p| PyTuple::new(py, p.iter().map(|s| PyBytes::new(py, s))).unwrap()),
            )?
            .into_any(),
        };
        let entry = PyTuple::new(
            py,
            [
                key.into_any(),
                parents,
                factory.start.into_pyobject(py)?.into_any(),
                factory.end.into_pyobject(py)?.into_any(),
            ],
        )?;
        entries.push(entry);
    }
    let list = pyo3::types::PyList::new(py, entries)?;
    Ok((block_bytes, list))
}

/// Build the framing prefix for the wire format of a groupcompress block.
///
/// `factories` is a list of `(key_tuple, parents_tuple_or_none, start, end)`
/// tuples and `block_bytes_len` is the length of the inner block payload that
/// will be appended after the returned prefix.
#[pyfunction]
fn build_wire_prefix<'py>(
    py: Python<'py>,
    factories: &Bound<'py, pyo3::types::PyList>,
    block_bytes_len: usize,
) -> PyResult<Bound<'py, PyBytes>> {
    let mut wire_factories = Vec::with_capacity(factories.len());
    for entry in factories.iter() {
        let tuple = entry.cast_into::<PyTuple>()?;
        if tuple.len() != 4 {
            return Err(PyValueError::new_err(
                "wire factory must be (key, parents, start, end)",
            ));
        }
        let key_tuple = tuple.get_item(0)?.cast_into::<PyTuple>()?;
        let key: Vec<Vec<u8>> = key_tuple
            .iter()
            .map(|seg| {
                seg.cast_into::<PyBytes>()
                    .map(|b| b.as_bytes().to_vec())
                    .map_err(|_| PyValueError::new_err("key segments must be bytes"))
            })
            .collect::<PyResult<_>>()?;

        let parents_obj = tuple.get_item(1)?;
        let parents: Option<Vec<Vec<Vec<u8>>>> = if parents_obj.is_none() {
            None
        } else {
            let parents_tuple = parents_obj.cast_into::<PyTuple>()?;
            let mut parents = Vec::with_capacity(parents_tuple.len());
            for parent_obj in parents_tuple.iter() {
                let parent_tuple = parent_obj.cast_into::<PyTuple>()?;
                let parent: Vec<Vec<u8>> = parent_tuple
                    .iter()
                    .map(|seg| {
                        seg.cast_into::<PyBytes>()
                            .map(|b| b.as_bytes().to_vec())
                            .map_err(|_| PyValueError::new_err("parent segments must be bytes"))
                    })
                    .collect::<PyResult<_>>()?;
                parents.push(parent);
            }
            Some(parents)
        };

        let start: u64 = tuple.get_item(2)?.extract()?;
        let end: u64 = tuple.get_item(3)?.extract()?;
        wire_factories.push(bazaar::groupcompress::wire::WireFactory {
            key,
            parents,
            start,
            end,
        });
    }

    let prefix = bazaar::groupcompress::wire::build_wire_prefix(&wire_factories, block_bytes_len)
        .map_err(|e| PyValueError::new_err(format!("zlib error: {}", e)))?;
    Ok(PyBytes::new(py, &prefix))
}

/// Parse a `_GCGraphIndex` node value into its four position integers.
///
/// Returns `(start, stop, basis_end, delta_end)`. The Python original is
/// `_GCGraphIndex._node_to_position`.
#[pyfunction]
fn parse_node_position(value: &[u8]) -> PyResult<(u64, u64, u64, u64)> {
    let pos = bazaar::groupcompress::manager::parse_node_position(value)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok((pos.start, pos.stop, pos.basis_end, pos.delta_end))
}

/// Decide whether a block should be repacked.
///
/// `factories` is an iterable of `(start, end)` tuples and `content_length`
/// is the uncompressed size of the block. Returns
/// `(action, last_byte_used, total_bytes_used)` where `action` is one of
/// `None`, `"trim"`, or `"rebuild"`.
#[pyfunction]
fn check_rebuild_action<'py>(
    py: Python<'py>,
    factories: Vec<(usize, usize)>,
    content_length: usize,
) -> PyResult<(Bound<'py, PyAny>, usize, usize)> {
    let (action, last, total) =
        bazaar::groupcompress::manager::check_rebuild_action(&factories, content_length);
    let action: Bound<'py, PyAny> = match action {
        bazaar::groupcompress::manager::RebuildAction::Keep => py.None().into_bound(py),
        bazaar::groupcompress::manager::RebuildAction::Trim => "trim".into_pyobject(py)?.into_any(),
        bazaar::groupcompress::manager::RebuildAction::Rebuild => {
            "rebuild".into_pyobject(py)?.into_any()
        }
    };
    Ok((action, last, total))
}

/// Decide whether a block is "well utilized" enough to leave intact.
///
/// `factories` is a list of `((start, end), prefix_bytes)` tuples where
/// `prefix_bytes` is the joined `key[:-1]` for the record (used for the
/// mixed-content heuristic).
#[pyfunction]
#[pyo3(signature = (
    factories,
    content_length,
    max_cut_fraction = 0.75,
    full_enough_block_size = 3 * 1024 * 1024,
    full_enough_mixed_block_size = 2 * 768 * 1024,
))]
fn check_is_well_utilized(
    factories: Vec<((usize, usize), Vec<u8>)>,
    content_length: usize,
    max_cut_fraction: f64,
    full_enough_block_size: usize,
    full_enough_mixed_block_size: usize,
) -> bool {
    let settings = bazaar::groupcompress::manager::WellUtilizedSettings {
        max_cut_fraction,
        full_enough_block_size,
        full_enough_mixed_block_size,
    };
    bazaar::groupcompress::manager::check_is_well_utilized(&factories, content_length, &settings)
}

#[pyfunction]
fn rabin_hash(data: Vec<u8>) -> PyResult<u32> {
    Ok(bazaar::groupcompress::rabin_delta::rabin_hash(
        data.try_into()
            .map_err(|e| PyValueError::new_err(format!("Error during rabin_hash: {:?}", e)))?,
    )
    .into())
}

/// One factory's per-record state inside a [`LazyGroupContentManager`].
///
/// Mirrors the public attributes of Python's `_LazyGroupCompressFactory` —
/// `key`, `parents`, `start`, `end`, optional cached chunks/sha1/size, and
/// the `_first` flag controlling its `storage_kind`.
#[derive(Default)]
struct FactoryState {
    key: Option<Py<PyTuple>>,
    parents: Option<Py<PyAny>>,
    start: u64,
    end: u64,
    sha1: Option<Py<PyAny>>,
    size: Option<usize>,
    chunks: Option<Vec<Py<PyBytes>>>,
    first: bool,
}

/// Rust-backed `_LazyGroupContentManager`.
///
/// Holds an inline list of [`FactoryState`]s and a `Py<GroupCompressBlock>`,
/// so the manager owns the underlying data without a Python-level reference
/// cycle. Factories are exposed as separate `LazyGroupCompressFactory`
/// pyclasses on demand; iteration breaks the back-reference exactly the same
/// way the Python original does.
#[pyclass(
    name = "LazyGroupContentManager",
    module = "bzrformats._bzr_rs.groupcompress"
)]
struct LazyGroupContentManager {
    block: Py<GroupCompressBlock>,
    factories: Vec<FactoryState>,
    last_byte: u64,
    get_settings: Option<Py<PyAny>>,
    compressor_settings: Option<Py<PyAny>>,
    /// Per-instance override for the well-utilized threshold. Tests poke at
    /// this directly to force smaller blocks to count as full.
    full_enough_block_size: usize,
    full_enough_mixed_block_size: usize,
    max_cut_fraction: f64,
}

const DEFAULT_MAX_BYTES_TO_INDEX: usize = 1024 * 1024;

const MAX_CUT_FRACTION: f64 = 0.75;
const FULL_ENOUGH_BLOCK_SIZE: usize = 3 * 1024 * 1024;
const FULL_ENOUGH_MIXED_BLOCK_SIZE: usize = 2 * 768 * 1024;

fn default_compressor_settings(py: Python) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("max_bytes_to_index", DEFAULT_MAX_BYTES_TO_INDEX)?;
    Ok(dict.into_any().unbind())
}

impl LazyGroupContentManager {
    fn ensure_compressor_settings(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if let Some(settings) = &self.compressor_settings {
            return Ok(settings.clone_ref(py));
        }
        let settings = if let Some(cb) = &self.get_settings {
            let result = cb.call0(py)?;
            if result.is_none(py) {
                default_compressor_settings(py)?
            } else {
                result
            }
        } else {
            default_compressor_settings(py)?
        };
        self.compressor_settings = Some(settings.clone_ref(py));
        Ok(settings)
    }

    fn factories_for_well_utilized(&self, py: Python<'_>) -> Vec<((usize, usize), Vec<u8>)> {
        self.factories
            .iter()
            .map(|f| {
                let prefix = if let Some(key) = &f.key {
                    let key = key.bind(py);
                    let len = key.len();
                    if len <= 1 {
                        Vec::new()
                    } else {
                        let mut out = Vec::new();
                        for i in 0..len - 1 {
                            if i > 0 {
                                out.push(b'\x00');
                            }
                            if let Ok(item) = key.get_item(i) {
                                if let Ok(b) = item.cast::<PyBytes>() {
                                    out.extend_from_slice(b.as_bytes());
                                }
                            }
                        }
                        out
                    }
                } else {
                    Vec::new()
                };
                ((f.start as usize, f.end as usize), prefix)
            })
            .collect()
    }

    fn invoke_check_rebuild(&self) -> PyResult<(Py<PyAny>, usize, usize)> {
        Python::attach(|py| {
            let positions: Vec<(usize, usize)> = self
                .factories
                .iter()
                .map(|f| (f.start as usize, f.end as usize))
                .collect();
            let block = self.block.borrow(py);
            let content_length = block
                .inner
                .content_length()
                .ok_or_else(|| PyValueError::new_err("block has no content length"))?;
            drop(block);
            let (action, last, total) =
                bazaar::groupcompress::manager::check_rebuild_action(&positions, content_length);
            let action_obj: Py<PyAny> = match action {
                bazaar::groupcompress::manager::RebuildAction::Keep => py.None(),
                bazaar::groupcompress::manager::RebuildAction::Trim => {
                    "trim".into_pyobject(py)?.into_any().unbind()
                }
                bazaar::groupcompress::manager::RebuildAction::Rebuild => {
                    "rebuild".into_pyobject(py)?.into_any().unbind()
                }
            };
            Ok((action_obj, last, total))
        })
    }
}

#[pymethods]
impl LazyGroupContentManager {
    #[new]
    #[pyo3(signature = (block, get_compressor_settings = None))]
    fn new(block: Py<GroupCompressBlock>, get_compressor_settings: Option<Py<PyAny>>) -> Self {
        Self {
            block,
            factories: Vec::new(),
            last_byte: 0,
            get_settings: get_compressor_settings,
            compressor_settings: None,
            full_enough_block_size: FULL_ENOUGH_BLOCK_SIZE,
            full_enough_mixed_block_size: FULL_ENOUGH_MIXED_BLOCK_SIZE,
            max_cut_fraction: MAX_CUT_FRACTION,
        }
    }

    #[getter]
    fn _full_enough_block_size(&self) -> usize {
        self.full_enough_block_size
    }

    #[setter(_full_enough_block_size)]
    fn set_full_enough_block_size_py(&mut self, v: usize) {
        self.full_enough_block_size = v;
    }

    #[getter]
    fn _full_enough_mixed_block_size(&self) -> usize {
        self.full_enough_mixed_block_size
    }

    #[setter(_full_enough_mixed_block_size)]
    fn set_full_enough_mixed_block_size_py(&mut self, v: usize) {
        self.full_enough_mixed_block_size = v;
    }

    #[getter]
    fn _max_cut_fraction(&self) -> f64 {
        self.max_cut_fraction
    }

    #[setter(_max_cut_fraction)]
    fn set_max_cut_fraction_py(&mut self, v: f64) {
        self.max_cut_fraction = v;
    }

    fn _make_group_compressor(&mut self, py: Python<'_>) -> PyResult<Py<RabinGroupCompressor>> {
        let settings = self.ensure_compressor_settings(py)?;
        let settings_bound = settings.into_bound(py);
        let settings_ref: Option<&Bound<PyAny>> = if settings_bound.is_none() {
            None
        } else {
            Some(&settings_bound)
        };
        let inner = RabinGroupCompressor::new(settings_ref)?;
        Py::new(py, inner)
    }

    #[getter]
    fn _block(&self, py: Python<'_>) -> Py<GroupCompressBlock> {
        self.block.clone_ref(py)
    }

    /// Test probe: number of registered factories.
    #[getter]
    fn _factories<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Vec<Bound<'py, LazyGroupCompressFactory>>> {
        let n = slf.factories.len();
        let manager: Py<LazyGroupContentManager> = slf.into();
        (0..n)
            .map(|i| {
                Bound::new(
                    py,
                    LazyGroupCompressFactory {
                        manager: Some(manager.clone_ref(py)),
                        index: i,
                    },
                )
            })
            .collect()
    }

    #[getter]
    fn _last_byte(&self) -> u64 {
        self.last_byte
    }

    #[getter]
    fn _compressor_settings(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.compressor_settings.as_ref().map(|s| s.clone_ref(py))
    }

    fn _get_compressor_settings(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.ensure_compressor_settings(py)
    }

    fn add_factory(
        &mut self,
        py: Python<'_>,
        key: Py<PyAny>,
        parents: Py<PyAny>,
        start: u64,
        end: u64,
    ) -> PyResult<()> {
        let key_tuple = key.bind(py).clone().cast_into::<PyTuple>().map_err(|_| {
            PyValueError::new_err("LazyGroupContentManager.add_factory: key must be a tuple")
        })?;
        let first = self.factories.is_empty();
        if end > self.last_byte {
            self.last_byte = end;
        }
        self.factories.push(FactoryState {
            key: Some(key_tuple.unbind()),
            parents: Some(parents),
            start,
            end,
            sha1: None,
            size: None,
            chunks: None,
            first,
        });
        Ok(())
    }

    /// Iterate the factories. After yielding a factory, its back-reference to
    /// this manager is cleared (matching the Python original).
    fn get_record_stream<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, RecordStreamIter>> {
        let n = slf.factories.len();
        let manager: Py<LazyGroupContentManager> = slf.into();
        Bound::new(
            py,
            RecordStreamIter {
                manager: Some(manager),
                index: 0,
                len: n,
            },
        )
    }

    fn check_is_well_utilized(&self, py: Python<'_>) -> PyResult<bool> {
        if self.factories.len() == 1 {
            return Ok(false);
        }
        let factories = self.factories_for_well_utilized(py);
        let block = self.block.borrow(py);
        let content_length = block
            .inner
            .content_length()
            .ok_or_else(|| PyValueError::new_err("block has no content length"))?;
        let settings = bazaar::groupcompress::manager::WellUtilizedSettings {
            max_cut_fraction: self.max_cut_fraction,
            full_enough_block_size: self.full_enough_block_size,
            full_enough_mixed_block_size: self.full_enough_mixed_block_size,
        };
        Ok(bazaar::groupcompress::manager::check_is_well_utilized(
            &factories,
            content_length,
            &settings,
        ))
    }

    fn _check_rebuild_action<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<(Bound<'py, PyAny>, usize, usize)> {
        let (action, last, total) = self.invoke_check_rebuild()?;
        Ok((action.into_bound(py), last, total))
    }

    fn _check_rebuild_block(&mut self, py: Python<'_>) -> PyResult<()> {
        let (action, last_byte_used, _) = self.invoke_check_rebuild()?;
        let action_bound = action.into_bound(py);
        if action_bound.is_none() {
            return Ok(());
        }
        let action_str: String = action_bound.extract()?;
        match action_str.as_str() {
            "trim" => self.trim_block(py, last_byte_used),
            "rebuild" => self.rebuild_block(py),
            other => Err(PyValueError::new_err(format!(
                "unknown rebuild action: {:?}",
                other
            ))),
        }
    }

    fn _rebuild_block(&mut self, py: Python<'_>) -> PyResult<()> {
        self.rebuild_block(py)
    }

    fn _trim_block(&mut self, py: Python<'_>, last_byte: usize) -> PyResult<()> {
        self.trim_block(py, last_byte)
    }

    /// Build the over-the-wire representation of this manager, repacking the
    /// underlying block first if `_check_rebuild_block` thinks it's worth it.
    fn _wire_bytes<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        self._check_rebuild_block(py)?;
        let mut wire_factories = Vec::with_capacity(self.factories.len());
        for f in &self.factories {
            let key_tuple = f
                .key
                .as_ref()
                .ok_or_else(|| PyValueError::new_err("factory missing key"))?
                .bind(py);
            let key: Vec<Vec<u8>> = key_tuple
                .iter()
                .map(|seg| {
                    seg.cast_into::<PyBytes>()
                        .map(|b| b.as_bytes().to_vec())
                        .map_err(|_| PyValueError::new_err("key segments must be bytes"))
                })
                .collect::<PyResult<_>>()?;
            let parents_obj = f
                .parents
                .as_ref()
                .map(|p| p.clone_ref(py).into_bound(py))
                .unwrap_or_else(|| py.None().into_bound(py));
            let parents: Option<Vec<Vec<Vec<u8>>>> = if parents_obj.is_none() {
                None
            } else {
                let pt = parents_obj.cast_into::<PyTuple>()?;
                let mut parents = Vec::with_capacity(pt.len());
                for parent_obj in pt.iter() {
                    let parent_tuple = parent_obj.cast_into::<PyTuple>()?;
                    let parent: Vec<Vec<u8>> = parent_tuple
                        .iter()
                        .map(|seg| {
                            seg.cast_into::<PyBytes>()
                                .map(|b| b.as_bytes().to_vec())
                                .map_err(|_| PyValueError::new_err("parent segments must be bytes"))
                        })
                        .collect::<PyResult<_>>()?;
                    parents.push(parent);
                }
                Some(parents)
            };
            wire_factories.push(bazaar::groupcompress::wire::WireFactory {
                key,
                parents,
                start: f.start,
                end: f.end,
            });
        }
        let (block_bytes_len, block_chunks) = {
            let mut block = self.block.borrow_mut(py);
            block.to_chunks(py, None)
        };
        let prefix =
            bazaar::groupcompress::wire::build_wire_prefix(&wire_factories, block_bytes_len)
                .map_err(|e| PyValueError::new_err(format!("zlib error: {}", e)))?;
        // Concatenate prefix + chunks into a single bytes object.
        let mut out = prefix;
        for chunk in block_chunks {
            out.extend_from_slice(chunk.as_bytes());
        }
        Ok(PyBytes::new(py, &out))
    }

    /// Used by `_LazyGroupCompressFactory._extract_bytes` to make sure the
    /// inner block content has been decompressed up to `_last_byte`.
    fn _prepare_for_extract(&self, py: Python<'_>) -> PyResult<()> {
        let mut block = self.block.borrow_mut(py);
        block
            .inner
            .ensure_content(Some(self.last_byte as usize))
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    #[classmethod]
    fn from_bytes<'py>(
        _cls: &Bound<'py, pyo3::types::PyType>,
        py: Python<'py>,
        bytes: &[u8],
    ) -> PyResult<Bound<'py, LazyGroupContentManager>> {
        let frame = bazaar::groupcompress::wire::parse_wire(bytes)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let block_inner =
            bazaar::groupcompress::block::GroupCompressBlock::from_bytes(frame.block_bytes)
                .map_err(|e| PyValueError::new_err(format!("Invalid block: {:?}", e)))?;
        let block = Bound::new(
            py,
            GroupCompressBlock {
                inner: block_inner,
                z_content_cache: None,
            },
        )?;
        let mgr = Bound::new(
            py,
            LazyGroupContentManager {
                block: block.unbind(),
                factories: Vec::new(),
                last_byte: 0,
                get_settings: None,
                compressor_settings: None,
                full_enough_block_size: FULL_ENOUGH_BLOCK_SIZE,
                full_enough_mixed_block_size: FULL_ENOUGH_MIXED_BLOCK_SIZE,
                max_cut_fraction: MAX_CUT_FRACTION,
            },
        )?;
        {
            let mut mgr_ref = mgr.borrow_mut();
            for factory in frame.factories {
                let key_tuple = PyTuple::new(py, factory.key.iter().map(|s| PyBytes::new(py, s)))?;
                let parents: Bound<PyAny> = match factory.parents {
                    None => py.None().into_bound(py),
                    Some(parents) => PyTuple::new(
                        py,
                        parents.iter().map(|p| {
                            PyTuple::new(py, p.iter().map(|s| PyBytes::new(py, s))).unwrap()
                        }),
                    )?
                    .into_any(),
                };
                let first = mgr_ref.factories.is_empty();
                if factory.end > mgr_ref.last_byte {
                    mgr_ref.last_byte = factory.end;
                }
                mgr_ref.factories.push(FactoryState {
                    key: Some(key_tuple.unbind()),
                    parents: Some(parents.unbind()),
                    start: factory.start,
                    end: factory.end,
                    sha1: None,
                    size: None,
                    chunks: None,
                    first,
                });
            }
        }
        Ok(mgr)
    }
}

impl LazyGroupContentManager {
    /// Snapshot the wrapper's per-record state into the pure-Rust
    /// [`bazaar::groupcompress::manager::FactoryState`] form. The result has
    /// no Python references and can be passed to the pure-Rust state machine.
    fn snapshot_factory_states(
        &self,
        py: Python<'_>,
    ) -> PyResult<Vec<bazaar::groupcompress::manager::FactoryState>> {
        self.factories
            .iter()
            .map(|f| {
                let chunks = if let Some(cached) = &f.chunks {
                    Some(
                        cached
                            .iter()
                            .map(|b| b.bind(py).as_bytes().to_vec())
                            .collect::<Vec<Vec<u8>>>(),
                    )
                } else {
                    None
                };
                Ok(bazaar::groupcompress::manager::FactoryState {
                    start: f.start,
                    end: f.end,
                    sha1: None,
                    size: f.size,
                    chunks,
                    first: f.first,
                })
            })
            .collect()
    }

    /// Snapshot just the per-record key segments (in pure bytes form), used
    /// to feed [`bazaar::groupcompress::manager::rebuild_block`].
    fn snapshot_factory_keys(&self, py: Python<'_>) -> PyResult<Vec<Vec<Vec<u8>>>> {
        self.factories
            .iter()
            .map(|f| {
                let key_tuple = f
                    .key
                    .as_ref()
                    .ok_or_else(|| PyValueError::new_err("factory missing key"))?
                    .bind(py);
                key_tuple
                    .iter()
                    .map(|seg| {
                        seg.cast_into::<PyBytes>()
                            .map(|b| b.as_bytes().to_vec())
                            .map_err(|_| PyValueError::new_err("key segments must be bytes"))
                    })
                    .collect::<PyResult<Vec<Vec<u8>>>>()
            })
            .collect()
    }

    fn install_block(
        &mut self,
        py: Python<'_>,
        block: bazaar::groupcompress::block::GroupCompressBlock,
    ) -> PyResult<()> {
        self.block = Bound::new(
            py,
            GroupCompressBlock {
                inner: block,
                z_content_cache: None,
            },
        )?
        .unbind();
        Ok(())
    }

    fn trim_block(&mut self, py: Python<'_>, last_byte: usize) -> PyResult<()> {
        let new_block = {
            let mut block = self.block.borrow_mut(py);
            bazaar::groupcompress::manager::trim_block(&mut block.inner, last_byte)
                .map_err(|e| PyValueError::new_err(e.to_string()))?
        };
        self.install_block(py, new_block)
    }

    fn rebuild_block(&mut self, py: Python<'_>) -> PyResult<()> {
        // Get the compressor settings (Python side may want to lazily compute
        // them via a callback).
        let settings_obj = self.ensure_compressor_settings(py)?;
        let settings_bound = settings_obj.into_bound(py);
        let settings_ref: Option<&Bound<PyAny>> = if settings_bound.is_none() {
            None
        } else {
            Some(&settings_bound)
        };
        let max_bytes_to_index = max_bytes_from_settings(settings_ref)?;

        let keys = self.snapshot_factory_keys(py)?;
        let mut states = self.snapshot_factory_states(py)?;
        let result = {
            let mut block = self.block.borrow_mut(py);
            bazaar::groupcompress::manager::rebuild_block(
                &mut block.inner,
                &mut states,
                &keys,
                max_bytes_to_index,
            )
            .map_err(PyValueError::new_err)?
        };
        // Write the new offsets/sha1s back into the wrapper's slots.
        for (slot, state) in self.factories.iter_mut().zip(states.iter()) {
            slot.start = state.start;
            slot.end = state.end;
            slot.sha1 = state
                .sha1
                .as_ref()
                .map(|s| PyBytes::new(py, s.as_bytes()).into_any().unbind());
            slot.chunks = None;
        }
        self.last_byte = result.last_byte;
        self.install_block(py, result.block)
    }
}

/// Iterator returned by `LazyGroupContentManager.get_record_stream`.
///
/// On each `__next__` it yields a fresh [`LazyGroupCompressFactory`] view of
/// the next slot, then on the *following* call it sets that factory's manager
/// reference to `None` to break the back-pointer (matching the Python
/// original's `factory._manager = None` after `yield factory`).
#[pyclass]
struct RecordStreamIter {
    manager: Option<Py<LazyGroupContentManager>>,
    index: usize,
    len: usize,
}

#[pymethods]
impl RecordStreamIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(
        mut slf: PyRefMut<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Option<Bound<'py, LazyGroupCompressFactory>>> {
        let Some(manager) = slf.manager.as_ref().map(|m| m.clone_ref(py)) else {
            return Ok(None);
        };
        if slf.index >= slf.len {
            slf.manager = None;
            return Ok(None);
        }
        let idx = slf.index;
        slf.index += 1;
        Bound::new(
            py,
            LazyGroupCompressFactory {
                manager: Some(manager),
                index: idx,
            },
        )
        .map(Some)
    }
}

/// Rust-backed `_LazyGroupCompressFactory`.
///
/// This is a thin view onto a slot inside [`LazyGroupContentManager`]. It
/// keeps an optional back-reference to the manager so its `get_bytes_as`
/// method can extract bytes lazily; the back-reference can be cleared from
/// Python (mirroring `factory._manager = None`).
#[pyclass(
    name = "LazyGroupCompressFactory",
    module = "bzrformats._bzr_rs.groupcompress"
)]
struct LazyGroupCompressFactory {
    manager: Option<Py<LazyGroupContentManager>>,
    index: usize,
}

impl LazyGroupCompressFactory {
    fn with_state<R, F>(&self, py: Python<'_>, f: F) -> PyResult<R>
    where
        F: FnOnce(&FactoryState) -> PyResult<R>,
    {
        let manager_py = self
            .manager
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("factory has no manager"))?;
        let manager = manager_py.borrow(py);
        let state = manager
            .factories
            .get(self.index)
            .ok_or_else(|| PyValueError::new_err("factory index out of range"))?;
        f(state)
    }

    fn with_state_mut<R, F>(&self, py: Python<'_>, f: F) -> PyResult<R>
    where
        F: FnOnce(&mut FactoryState) -> PyResult<R>,
    {
        let manager_py = self
            .manager
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("factory has no manager"))?;
        let mut manager = manager_py.borrow_mut(py);
        let index = self.index;
        let state = manager
            .factories
            .get_mut(index)
            .ok_or_else(|| PyValueError::new_err("factory index out of range"))?;
        f(state)
    }
}

#[pymethods]
impl LazyGroupCompressFactory {
    #[getter]
    fn key(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
        self.with_state(py, |s| {
            s.key
                .as_ref()
                .map(|k| k.clone_ref(py))
                .ok_or_else(|| PyValueError::new_err("factory missing key"))
        })
    }

    #[getter]
    fn parents(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.with_state(py, |s| {
            Ok(s.parents
                .as_ref()
                .map(|p| p.clone_ref(py))
                .unwrap_or_else(|| py.None()))
        })
    }

    #[setter]
    fn set_parents(&mut self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        // Contract: parents is a list/tuple of revision-id keys, or None.
        // Reject anything else at the binding boundary so a typo in the
        // caller (e.g. passing an int) fails loudly here rather than
        // surfacing as a confusing AttributeError later inside reconcile.
        if !value.is_none(py) {
            let bound = value.bind(py);
            if !bound.is_instance_of::<pyo3::types::PyList>()
                && !bound.is_instance_of::<pyo3::types::PyTuple>()
            {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "parents must be a list, tuple, or None",
                ));
            }
        }
        self.with_state_mut(py, |s| {
            s.parents = if value.is_none(py) { None } else { Some(value) };
            Ok(())
        })
    }

    #[getter]
    fn _start(&self, py: Python<'_>) -> PyResult<u64> {
        self.with_state(py, |s| Ok(s.start))
    }

    #[getter]
    fn _end(&self, py: Python<'_>) -> PyResult<u64> {
        self.with_state(py, |s| Ok(s.end))
    }

    #[getter]
    fn _first(&self, py: Python<'_>) -> PyResult<bool> {
        self.with_state(py, |s| Ok(s.first))
    }

    #[getter]
    fn sha1(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.with_state(py, |s| {
            Ok(s.sha1
                .as_ref()
                .map(|x| x.clone_ref(py))
                .unwrap_or_else(|| py.None()))
        })
    }

    #[getter]
    fn size(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.with_state(py, |s| {
            Ok(s.size
                .map(|x| x.into_pyobject(py).unwrap().into_any().unbind())
                .unwrap_or_else(|| py.None()))
        })
    }

    #[getter]
    fn storage_kind(&self, py: Python<'_>) -> PyResult<&'static str> {
        self.with_state(py, |s| {
            Ok(if s.first {
                "groupcompress-block"
            } else {
                "groupcompress-block-ref"
            })
        })
    }

    #[getter]
    fn _manager(&self, py: Python<'_>) -> Option<Py<LazyGroupContentManager>> {
        self.manager.as_ref().map(|m| m.clone_ref(py))
    }

    #[setter(_manager)]
    fn set_manager_py(&mut self, value: Option<Py<LazyGroupContentManager>>) {
        self.manager = value;
    }

    fn get_bytes_as<'py>(&mut self, py: Python<'py>, storage_kind: &str) -> PyResult<Py<PyAny>> {
        let manager_py = self
            .manager
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("factory has no manager"))?
            .clone_ref(py);

        // Determine our own storage_kind from the cached `_first` flag.
        let own_kind = {
            let manager = manager_py.borrow(py);
            let state = manager
                .factories
                .get(self.index)
                .ok_or_else(|| PyValueError::new_err("factory index out of range"))?;
            if state.first {
                "groupcompress-block"
            } else {
                "groupcompress-block-ref"
            }
        };

        if storage_kind == own_kind {
            if own_kind == "groupcompress-block" {
                // First factory → wire bytes for the whole manager.
                let mut manager = manager_py.borrow_mut(py);
                let bound = manager._wire_bytes(py)?;
                return Ok(bound.into_any().unbind());
            } else {
                return Ok(PyBytes::new(py, b"").into_any().unbind());
            }
        }
        if !matches!(storage_kind, "fulltext" | "chunked" | "lines") {
            return Err(unavailable_representation(
                py,
                &manager_py,
                self.index,
                storage_kind,
                own_kind,
            )?);
        }

        // Make sure the chunks have been extracted.
        let chunks = self.ensure_chunks(py, &manager_py)?;

        match storage_kind {
            "fulltext" => {
                let mut all = Vec::new();
                for c in &chunks {
                    all.extend_from_slice(c.bind(py).as_bytes());
                }
                Ok(PyBytes::new(py, &all).into_any().unbind())
            }
            "chunked" => {
                let list =
                    pyo3::types::PyList::new(py, chunks.into_iter().map(|c| c.into_bound(py)))?;
                Ok(list.into_any().unbind())
            }
            "lines" => {
                let raw: Vec<Vec<u8>> = chunks
                    .iter()
                    .map(|c| c.bind(py).as_bytes().to_vec())
                    .collect();
                let lines: Vec<Vec<u8>> = bazaar::osutils::chunks_to_lines(
                    raw.into_iter().map(Ok::<_, std::convert::Infallible>),
                )
                .map(|r| r.unwrap().into_owned())
                .collect();
                Ok(
                    pyo3::types::PyList::new(py, lines.iter().map(|l| PyBytes::new(py, l)))?
                        .into_any()
                        .unbind(),
                )
            }
            _ => unreachable!(),
        }
    }

    fn iter_bytes_as<'py>(&mut self, py: Python<'py>, storage_kind: &str) -> PyResult<Py<PyAny>> {
        let manager_py = self
            .manager
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("factory has no manager"))?
            .clone_ref(py);
        let chunks = self.ensure_chunks(py, &manager_py)?;
        match storage_kind {
            "chunked" => {
                let list =
                    pyo3::types::PyList::new(py, chunks.into_iter().map(|c| c.into_bound(py)))?;
                Ok(list.try_iter()?.unbind().into())
            }
            "lines" => {
                let raw: Vec<Vec<u8>> = chunks
                    .iter()
                    .map(|c| c.bind(py).as_bytes().to_vec())
                    .collect();
                let lines: Vec<Vec<u8>> = bazaar::osutils::chunks_to_lines(
                    raw.into_iter().map(Ok::<_, std::convert::Infallible>),
                )
                .map(|r| r.unwrap().into_owned())
                .collect();
                let list = pyo3::types::PyList::new(py, lines.iter().map(|l| PyBytes::new(py, l)))?;
                Ok(list.try_iter()?.unbind().into())
            }
            _ => Err(unavailable_representation(
                py,
                &manager_py,
                self.index,
                storage_kind,
                "groupcompress-block",
            )?),
        }
    }
}

impl LazyGroupCompressFactory {
    fn ensure_chunks(
        &self,
        py: Python<'_>,
        manager_py: &Py<LazyGroupContentManager>,
    ) -> PyResult<Vec<Py<PyBytes>>> {
        // Try the cached chunks first.
        {
            let manager = manager_py.borrow(py);
            let state = manager
                .factories
                .get(self.index)
                .ok_or_else(|| PyValueError::new_err("factory index out of range"))?;
            if let Some(c) = &state.chunks {
                return Ok(c.iter().map(|x| x.clone_ref(py)).collect());
            }
        }
        // Extract from the block. _prepare_for_extract first.
        {
            let manager = manager_py.borrow(py);
            manager._prepare_for_extract(py)?;
        }
        let chunks = {
            let manager = manager_py.borrow(py);
            let state = manager
                .factories
                .get(self.index)
                .ok_or_else(|| PyValueError::new_err("factory index out of range"))?;
            let start = state.start as usize;
            let end = state.end as usize;
            let _ = state;
            let mut block = manager.block.borrow_mut(py);
            block
                .inner
                .extract(start, end)
                .map_err(|e| {
                    let msg = format!("zlib: {:?}", e);
                    let dc = py
                        .import("bzrformats.groupcompress")
                        .and_then(|m| m.getattr("DecompressCorruption"))
                        .ok();
                    if let Some(cls) = dc {
                        let exc = cls.call1((msg.clone(),)).unwrap();
                        PyErr::from_value(exc)
                    } else {
                        PyValueError::new_err(msg)
                    }
                })?
                .into_iter()
                .map(|c| PyBytes::new(py, &c).unbind())
                .collect::<Vec<_>>()
        };
        // Store back on the state.
        {
            let mut manager = manager_py.borrow_mut(py);
            manager.factories[self.index].chunks =
                Some(chunks.iter().map(|c| c.clone_ref(py)).collect());
        }
        Ok(chunks)
    }
}

fn unavailable_representation(
    py: Python<'_>,
    manager_py: &Py<LazyGroupContentManager>,
    index: usize,
    requested: &str,
    own_kind: &str,
) -> PyResult<PyErr> {
    let key: Py<PyAny> = {
        let manager = manager_py.borrow(py);
        let state = manager
            .factories
            .get(index)
            .ok_or_else(|| PyValueError::new_err("factory index out of range"))?;
        match &state.key {
            Some(k) => k.clone_ref(py).into_any(),
            None => py.None(),
        }
    };
    let cls = py
        .import("bzrformats.versionedfile")?
        .getattr("UnavailableRepresentation")?;
    let exc = cls.call1((key, requested, own_kind))?;
    Ok(PyErr::from_value(exc))
}

/// Rust-backed `_GCBuildDetails`.
///
/// A tuple-like record holding a parent key list plus a 5-tuple index memo
/// `(index, group_start, group_end, basis_end, delta_end)`. `compression_parent`
/// is always `None` and `method` is always `"group"`, so `__getitem__` exposes
/// the 4-tuple `(index_memo, None, parents, ("group", None))`.
#[pyclass(name = "GCBuildDetails", module = "bzrformats._bzr_rs.groupcompress")]
struct GCBuildDetails {
    parents: Py<PyAny>,
    index: Py<PyAny>,
    group_start: u64,
    group_end: u64,
    basis_end: u64,
    delta_end: u64,
}

#[pymethods]
impl GCBuildDetails {
    #[new]
    fn new(parents: Py<PyAny>, position_info: &Bound<'_, PyAny>) -> PyResult<Self> {
        let tup: (Py<PyAny>, u64, u64, u64, u64) = position_info.extract()?;
        Ok(Self {
            parents,
            index: tup.0,
            group_start: tup.1,
            group_end: tup.2,
            basis_end: tup.3,
            delta_end: tup.4,
        })
    }

    #[classattr]
    fn method(py: Python<'_>) -> Py<PyAny> {
        pyo3::types::PyString::new(py, "group").into_any().unbind()
    }

    #[classattr]
    fn compression_parent(py: Python<'_>) -> Py<PyAny> {
        py.None()
    }

    #[getter]
    fn index_memo<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        PyTuple::new(
            py,
            [
                self.index.clone_ref(py).into_bound(py),
                self.group_start.into_pyobject(py)?.into_any(),
                self.group_end.into_pyobject(py)?.into_any(),
                self.basis_end.into_pyobject(py)?.into_any(),
                self.delta_end.into_pyobject(py)?.into_any(),
            ],
        )
    }

    #[getter]
    fn record_details<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        PyTuple::new(
            py,
            [
                pyo3::types::PyString::new(py, "group").into_any(),
                py.None().into_bound(py),
            ],
        )
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let memo = self.index_memo(py)?;
        let parents = self.parents.bind(py);
        Ok(format!(
            "_GCBuildDetails({}, {})",
            memo.repr()?.to_str()?,
            parents.repr()?.to_str()?
        ))
    }

    fn __len__(&self) -> usize {
        4
    }

    fn __getitem__<'py>(&self, py: Python<'py>, offset: isize) -> PyResult<Bound<'py, PyAny>> {
        match offset {
            0 => Ok(self.index_memo(py)?.into_any()),
            1 => Ok(py.None().into_bound(py)),
            2 => Ok(self.parents.clone_ref(py).into_bound(py)),
            3 => Ok(self.record_details(py)?.into_any()),
            _ => Err(pyo3::exceptions::PyIndexError::new_err(
                "offset out of range",
            )),
        }
    }
}

/// Mapper from `GroupCompressVersionedFiles` needs into `GraphIndex` storage.
///
/// Mirrors `bzrformats.groupcompress._GCGraphIndex`.
#[pyclass(name = "_GCGraphIndex")]
struct GCGraphIndex {
    graph_index: Py<PyAny>,
    is_locked: Py<PyAny>,
    parents: bool,
    add_callback: Option<Py<PyAny>>,
    inconsistency_fatal: bool,
    /// Integer cache for group start/stop values (avoids duplicate int objects).
    int_cache: std::collections::HashMap<u64, u64>,
    /// Optional external-parent-ref tracker.
    key_dependencies: Option<Py<crate::versionedfile::KeyRefs>>,
}

#[pymethods]
impl GCGraphIndex {
    #[new]
    #[pyo3(signature = (
        graph_index,
        is_locked,
        parents = true,
        add_callback = None,
        track_external_parent_refs = false,
        inconsistency_fatal = true,
        track_new_keys = false,
    ))]
    fn new(
        py: Python<'_>,
        graph_index: Bound<'_, PyAny>,
        is_locked: Bound<'_, PyAny>,
        parents: bool,
        add_callback: Option<Bound<'_, PyAny>>,
        track_external_parent_refs: bool,
        inconsistency_fatal: bool,
        track_new_keys: bool,
    ) -> PyResult<Self> {
        let key_dependencies = if track_external_parent_refs {
            let kr = crate::versionedfile::KeyRefs::new(py, track_new_keys)?;
            Some(Py::new(py, kr)?)
        } else {
            None
        };
        Ok(Self {
            graph_index: graph_index.unbind(),
            is_locked: is_locked.unbind(),
            parents,
            add_callback: add_callback.map(|c| c.unbind()),
            inconsistency_fatal,
            int_cache: std::collections::HashMap::new(),
            key_dependencies,
        })
    }

    #[getter]
    fn has_graph(&self) -> bool {
        self.parents
    }

    #[getter]
    fn _graph_index(&self, py: Python<'_>) -> Py<PyAny> {
        self.graph_index.clone_ref(py)
    }

    #[getter]
    fn _int_cache(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let d = PyDict::new(py);
        for (k, v) in &self.int_cache {
            d.set_item(k, v)?;
        }
        Ok(d.unbind())
    }

    #[getter]
    fn _key_dependencies(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &self.key_dependencies {
            Some(kd) => Ok(kd.clone_ref(py).into_any()),
            None => Ok(py.None()),
        }
    }

    fn _check_read(&self, py: Python<'_>) -> PyResult<()> {
        if !self.is_locked.bind(py).call0()?.is_truthy()? {
            return Err(ObjectNotLocked::new_err(py.None()));
        }
        Ok(())
    }

    fn _check_write_ok(&self, py: Python<'_>) -> PyResult<()> {
        if self.add_callback.is_none() {
            return Err(ReadOnlyError::new_err(py.None()));
        }
        self._check_read(py)
    }

    fn keys(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        self._check_read(py)?;
        let entries = self.graph_index.bind(py).call_method0("iter_all_entries")?;
        let result = PyList::empty(py);
        for entry in entries.try_iter()? {
            result.append(entry?.get_item(1)?)?;
        }
        Ok(result.unbind())
    }

    fn get_parent_map<'py>(
        &self,
        py: Python<'py>,
        keys: Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyDict>> {
        self._check_read(py)?;
        let result = PyDict::new(py);
        let nodes = self._get_entries(py, keys, false)?;
        if self.parents {
            for node in nodes.try_iter()? {
                let node = node?;
                let key = node.get_item(1)?;
                let parents = node.get_item(3)?.get_item(0)?;
                result.set_item(key, parents)?;
            }
        } else {
            for node in nodes.try_iter()? {
                let key = node?.get_item(1)?;
                result.set_item(key, py.None())?;
            }
        }
        Ok(result)
    }

    fn get_build_details<'py>(
        &mut self,
        py: Python<'py>,
        keys: Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyDict>> {
        self._check_read(py)?;
        let result = PyDict::new(py);
        let entries = self._get_entries(py, keys, false)?;
        for entry in entries.try_iter()? {
            let entry = entry?;
            let key = entry.get_item(1)?;
            let parents = if self.parents {
                entry.get_item(3)?.get_item(0)?.unbind()
            } else {
                py.None()
            };
            let position = self._node_to_position(py, &entry)?;
            let details = GCBuildDetails {
                parents,
                index: position.0,
                group_start: position.1,
                group_end: position.2,
                basis_end: position.3,
                delta_end: position.4,
            };
            result.set_item(key, Py::new(py, details)?)?;
        }
        Ok(result)
    }

    fn find_ancestry<'py>(
        &self,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.graph_index
            .bind(py)
            .call_method1("find_ancestry", (keys, 0i32))
    }

    fn get_missing_parents<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PySet>> {
        let kd = self.key_dependencies.as_ref().ok_or_else(|| {
            PyValueError::new_err("get_missing_parents called without key_dependencies")
        })?;
        let kd = kd.bind(py).borrow();
        let unsatisfied = kd.get_unsatisfied_refs(py)?;
        let parent_map = self.get_parent_map(py, unsatisfied)?;
        kd.satisfy_refs_for_keys(py, parent_map.into_any())?;
        let refs = kd.get_unsatisfied_refs(py)?;
        let out = PySet::empty(py)?;
        for key in refs.try_iter()? {
            out.add(key?)?;
        }
        Ok(out)
    }

    #[pyo3(signature = (records, random_id = false))]
    fn add_records(
        &mut self,
        py: Python<'_>,
        records: Bound<'_, PyAny>,
        random_id: bool,
    ) -> PyResult<()> {
        let add_callback = self
            .add_callback
            .as_ref()
            .ok_or_else(|| ReadOnlyError::new_err(py.None()))?;

        // Collect into a dict: key -> (value, refs)
        let keys_map = PyDict::new(py);
        let mut changed = false;
        for record in records.try_iter()? {
            let record = record?;
            let key = record.get_item(0)?;
            let value = record.get_item(1)?;
            let refs = record.get_item(2)?;

            // For parentless index, strip non-empty refs.
            if !self.parents {
                let has_refs = refs.try_iter()?.any(|r| {
                    r.as_ref()
                        .map(|r| r.is_truthy().unwrap_or(false))
                        .unwrap_or(false)
                });
                if has_refs {
                    pyo3::import_exception!(bzrformats.knit, KnitCorrupt);
                    return Err(KnitCorrupt::new_err((
                        py.None(),
                        "attempt to add node with parents in parentless index.",
                    )));
                }
                changed = true;
                keys_map.set_item(key, (value, PyTuple::empty(py)))?;
            } else {
                keys_map.set_item(key, (value, refs))?;
            }
        }

        // Check for duplicates if not random_id.
        if !random_id {
            let present = self._get_entries(py, keys_map.call_method0("keys")?, false)?;
            for node in present.try_iter()? {
                let node = node?;
                let key = node.get_item(1)?;
                let existing_value = node.get_item(2)?;
                let existing_refs = if self.parents {
                    node.get_item(3)?
                } else {
                    PyTuple::empty(py).into_any()
                };

                let entry = keys_map.get_item(&key)?.unwrap();
                let passed_refs = entry.get_item(1)?;

                // Compare refs as nested tuples.
                let passed_as_tuples = as_tuples(py, &passed_refs)?;
                let existing_as_tuples = as_tuples(py, &existing_refs)?;
                if !existing_as_tuples.eq(&passed_as_tuples)? {
                    // Match Python: f"{key} {value, node_refs} {passed}"
                    let existing_pair =
                        PyTuple::new(py, [existing_value.clone(), existing_refs.clone()])?;
                    let details = format!(
                        "{} {} {}",
                        key.repr()?.to_str()?,
                        existing_pair.repr()?.to_str()?,
                        entry.repr()?.to_str()?,
                    );
                    if self.inconsistency_fatal {
                        pyo3::import_exception!(bzrformats.knit, KnitCorrupt);
                        return Err(KnitCorrupt::new_err((
                            py.None(),
                            format!("inconsistent details in add_records: {}", details),
                        )));
                    } else {
                        // Log warning and skip.
                        let logging = py.import("logging")?;
                        let logger =
                            logging.call_method1("getLogger", ("bzrformats.groupcompress",))?;
                        logger.call_method1(
                            "warning",
                            (format!(
                                "inconsistent details in skipped record: {}",
                                details
                            ),),
                        )?;
                    }
                }
                keys_map.del_item(key)?;
                changed = true;
            }
        }

        // Build the records list for the callback.
        let result = PyList::empty(py);
        if self.parents {
            for (key, entry) in keys_map.iter() {
                let value = entry.get_item(0)?;
                let refs = entry.get_item(1)?;
                result.append(PyTuple::new(py, [key, value, refs])?)?;
            }
        } else {
            // Parentless: always emit 2-tuples.
            changed = true;
            for (key, entry) in keys_map.iter() {
                let value = entry.get_item(0)?;
                result.append(PyTuple::new(py, [key, value])?)?;
            }
        }

        // Update key_dependencies.
        if let Some(kd) = &self.key_dependencies {
            let kd = kd.bind(py).borrow();
            if self.parents {
                for item in result.iter() {
                    let item: Bound<'_, PyAny> = item;
                    let key = item.get_item(0)?;
                    let refs = item.get_item(2)?;
                    let parents = refs.get_item(0)?;
                    kd.add_references(py, key, parents)?;
                }
            } else {
                for item in result.iter() {
                    let item: Bound<'_, PyAny> = item;
                    let key = item.get_item(0)?;
                    kd.add_key(py, key)?;
                }
            }
        }

        let records_to_add = if changed {
            result.into_any()
        } else {
            // Re-use original records — they haven't changed shape.
            // (In practice `changed` is always true for parentless or when
            // duplicates were dropped; when false we can pass result directly
            // since we built it identically.)
            result.into_any()
        };

        add_callback.call1(py, (records_to_add,))?;
        Ok(())
    }

    fn scan_unvalidated_index(
        &self,
        py: Python<'_>,
        graph_index: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let kd = match &self.key_dependencies {
            Some(kd) => kd,
            None => return Ok(()),
        };
        let entries = graph_index.call_method0("iter_all_entries")?;
        let kd = kd.bind(py).borrow();
        for node in entries.try_iter()? {
            let node = node?;
            let key = node.get_item(1)?;
            let refs = node.get_item(3)?;
            let parents = refs.get_item(0)?;
            kd.add_references(py, key, parents)?;
        }
        Ok(())
    }
}

impl GCGraphIndex {
    /// Convert an index entry to its `(index, group_start, group_end, basis_end, delta_end)` tuple.
    fn _node_to_position(
        &mut self,
        py: Python<'_>,
        node: &Bound<'_, PyAny>,
    ) -> PyResult<(Py<PyAny>, u64, u64, u64, u64)> {
        let value: Vec<u8> = node.get_item(2)?.extract::<Vec<u8>>()?;
        let pos = bazaar::groupcompress::manager::parse_node_position(&value)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        // Cache start and stop to avoid duplicate int objects.
        let start = *self.int_cache.entry(pos.start).or_insert(pos.start);
        let stop = *self.int_cache.entry(pos.stop).or_insert(pos.stop);
        let index = node.get_item(0)?.unbind();
        Ok((index, start, stop, pos.basis_end, pos.delta_end))
    }

    /// Collect entries from the underlying graph_index for `keys`.
    /// When `parents` is false, adapts output to include an empty refs tuple.
    fn _get_entries<'py>(
        &self,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
        _check_present: bool,
    ) -> PyResult<Bound<'py, PyList>> {
        let iter = self
            .graph_index
            .bind(py)
            .call_method1("iter_entries", (keys,))?;
        let result = PyList::empty(py);
        if self.parents {
            for node in iter.try_iter()? {
                result.append(node?)?;
            }
        } else {
            for node in iter.try_iter()? {
                let node = node?;
                let idx = node.get_item(0)?;
                let key = node.get_item(1)?;
                let val = node.get_item(2)?;
                result.append(PyTuple::new(
                    py,
                    [idx, key, val, PyTuple::empty(py).into_any()],
                )?)?;
            }
        }
        Ok(result)
    }
}

/// Recursively convert `obj` to nested plain Python tuples.
fn as_tuples<'py>(py: Python<'py>, obj: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    if let Ok(seq) = obj.try_iter() {
        let items: Vec<Bound<'py, PyAny>> = seq
            .map(|r| r.and_then(|item| as_tuples(py, &item)))
            .collect::<PyResult<_>>()?;
        Ok(PyTuple::new(py, items)?.into_any())
    } else {
        Ok(obj.clone())
    }
}

/// Rust-backed implementation of Python's `GroupCompressVersionedFiles`.
///
/// At this stage the pyclass is only a state container: it holds the
/// Python index / access objects, the block cache, fallbacks and config,
/// and exposes construction, the `_index` / `_access` accessors, fallback
/// management and `clear_cache`. The record-stream and insert logic is
/// still provided by Python method overrides on the subclass; they migrate
/// onto this pyclass one step at a time.
#[pyclass(name = "GroupCompressVersionedFiles", subclass, dict)]
pub struct GroupCompressVersionedFiles {
    /// The `_GCGraphIndex` (or compatible) index object.
    index_obj: Py<PyAny>,
    /// The raw-record access object.
    access_obj: Py<PyAny>,
    /// Whether to delta-compress (True) or only entropy-compress.
    delta: bool,
    /// In-memory records added but not yet flushed, keyed by key.
    unadded_refs: Py<PyDict>,
    /// Block cache (`LRUSizeCache`), shared with clones from
    /// `without_fallbacks`.
    group_cache: Py<PyAny>,
    /// Fallback stores consulted for keys absent from this one.
    immediate_fallback_vfs: Vec<Py<PyAny>>,
    /// Cap on bytes a `GroupCompressor` indexes; `None` until first use.
    max_bytes_to_index: Option<usize>,
}

#[pymethods]
impl GroupCompressVersionedFiles {
    #[new]
    #[pyo3(signature = (index, access, delta=true, _unadded_refs=None, _group_cache=None))]
    fn new(
        py: Python<'_>,
        index: Bound<'_, PyAny>,
        access: Bound<'_, PyAny>,
        delta: bool,
        _unadded_refs: Option<Bound<'_, PyDict>>,
        _group_cache: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let unadded_refs = match _unadded_refs {
            Some(d) => d.unbind(),
            None => PyDict::new(py).unbind(),
        };
        let group_cache = match _group_cache {
            Some(c) => c.unbind(),
            None => {
                // Default: LRUSizeCache(max_size=50 * 1024 * 1024)
                let cls = py.import("bzrformats.lru_cache")?.getattr("LRUSizeCache")?;
                let kwargs = PyDict::new(py);
                kwargs.set_item("max_size", 50 * 1024 * 1024)?;
                cls.call((), Some(&kwargs))?.unbind()
            }
        };
        Ok(Self {
            index_obj: index.unbind(),
            access_obj: access.unbind(),
            delta,
            unadded_refs,
            group_cache,
            immediate_fallback_vfs: Vec::new(),
            max_bytes_to_index: None,
        })
    }

    #[getter]
    fn _index(&self, py: Python<'_>) -> Py<PyAny> {
        self.index_obj.clone_ref(py)
    }

    #[getter]
    fn _access(&self, py: Python<'_>) -> Py<PyAny> {
        self.access_obj.clone_ref(py)
    }

    #[getter]
    fn _delta(&self) -> bool {
        self.delta
    }

    #[getter]
    fn _unadded_refs(&self, py: Python<'_>) -> Py<PyDict> {
        self.unadded_refs.clone_ref(py)
    }

    #[setter]
    fn set__unadded_refs(&mut self, value: Bound<'_, PyDict>) {
        self.unadded_refs = value.unbind();
    }

    #[getter]
    fn _group_cache(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_cache.clone_ref(py)
    }

    #[getter]
    fn _immediate_fallback_vfs<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty(py);
        for vf in &self.immediate_fallback_vfs {
            list.append(vf.bind(py))?;
        }
        Ok(list)
    }

    #[getter]
    fn _max_bytes_to_index(&self) -> Option<usize> {
        self.max_bytes_to_index
    }

    #[setter]
    fn set__max_bytes_to_index(&mut self, value: Option<usize>) {
        self.max_bytes_to_index = value;
    }

    /// Return a clone of this object without any fallbacks configured.
    ///
    /// Mirrors `GroupCompressVersionedFiles.without_fallbacks`: the clone
    /// shares the block cache and gets a shallow copy of the unadded refs.
    /// The clone is built via `type(self)` so the Python subclass (which
    /// still carries the not-yet-ported record-stream methods) is produced,
    /// not the bare Rust base.
    fn without_fallbacks<'py>(slf: &Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let me = slf.borrow();
        let unadded_copy = me.unadded_refs.bind(py).copy()?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("_unadded_refs", unadded_copy)?;
        kwargs.set_item("_group_cache", me.group_cache.bind(py))?;
        slf.get_type().call(
            (me.index_obj.bind(py), me.access_obj.bind(py), me.delta),
            Some(&kwargs),
        )
    }

    /// Add a fallback store for texts not present in this one.
    fn add_fallback_versioned_files(&mut self, a_versioned_files: Bound<'_, PyAny>) {
        self.immediate_fallback_vfs.push(a_versioned_files.unbind());
    }

    /// Drop the block cache and the index's caches.
    ///
    /// Mirrors `GroupCompressVersionedFiles.clear_cache`.
    fn clear_cache(&self, py: Python<'_>) -> PyResult<()> {
        self.group_cache.bind(py).call_method0("clear")?;
        let index = self.index_obj.bind(py);
        index.getattr("_graph_index")?.call_method0("clear_cache")?;
        index.getattr("_int_cache")?.call_method0("clear")?;
        Ok(())
    }
}

pub(crate) fn _groupcompress_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "groupcompress")?;
    m.add_wrapped(wrap_pyfunction!(encode_base128_int))?;
    m.add_wrapped(wrap_pyfunction!(decode_base128_int))?;
    m.add_wrapped(wrap_pyfunction!(apply_delta))?;
    m.add_wrapped(wrap_pyfunction!(decode_copy_instruction))?;
    m.add_wrapped(wrap_pyfunction!(encode_copy_instruction))?;
    m.add_wrapped(wrap_pyfunction!(apply_delta_to_source))?;
    m.add_wrapped(wrap_pyfunction!(make_line_delta))?;
    m.add_wrapped(wrap_pyfunction!(make_rabin_delta))?;
    m.add_wrapped(wrap_pyfunction!(rabin_hash))?;
    m.add_function(wrap_pyfunction!(sort_gc_optimal, &m)?)?;
    m.add_function(wrap_pyfunction!(parse_wire_header, &m)?)?;
    m.add_function(wrap_pyfunction!(check_rebuild_action, &m)?)?;
    m.add_function(wrap_pyfunction!(check_is_well_utilized, &m)?)?;
    m.add_function(wrap_pyfunction!(build_wire_prefix, &m)?)?;
    m.add_function(wrap_pyfunction!(parse_node_position, &m)?)?;
    m.add_class::<GroupCompressBlock>()?;
    m.add_class::<LinesDeltaIndex>()?;
    m.add_class::<TraditionalGroupCompressor>()?;
    m.add_class::<RabinGroupCompressor>()?;
    m.add_class::<LazyGroupContentManager>()?;
    m.add_class::<LazyGroupCompressFactory>()?;
    m.add_class::<RecordStreamIter>()?;
    m.add_class::<GCBuildDetails>()?;
    m.add_class::<GCGraphIndex>()?;
    m.add_class::<GroupCompressVersionedFiles>()?;
    m.add_class::<crate::groupcompress_delta::DeltaIndex>()?;
    m.add_function(wrap_pyfunction!(
        crate::groupcompress_delta::_rabin_hash,
        &m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::groupcompress_delta::make_delta,
        &m
    )?)?;
    m.add(
        "NULL_SHA1",
        pyo3::types::PyBytes::new(py, &bazaar::groupcompress::NULL_SHA1),
    )?;
    Ok(m)
}

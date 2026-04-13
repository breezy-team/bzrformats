use bazaar::groupcompress::compressor::GroupCompressor;
use bazaar::versionedfile::Key;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyTuple};
use pyo3::wrap_pyfunction;
use std::borrow::Cow;
use std::convert::TryInto;

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

#[pyclass(unsendable)]
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

    #[setter]
    fn set__content_length(&mut self, value: usize) {
        self.inner.set_content_length(value);
    }

    #[getter]
    fn _z_content_length(&self) -> Option<usize> {
        self.inner.z_content_length()
    }

    #[setter]
    fn set__z_content_length(&mut self, value: usize) {
        self.inner.set_z_content_length(value);
    }

    #[setter]
    fn set__z_content_chunks(&mut self, chunks: Vec<Vec<u8>>) {
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

    #[setter]
    fn set__compressor_name(&mut self, name: &str) -> PyResult<()> {
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
        self.inner.ensure_content(size);
        Ok(())
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
            .map(|(hash, size, chunks, kind)| (PyBytes::new(py, hash.as_ref()), size, chunks, kind))
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
        Ok((PyBytes::new(py, hash.as_ref()), size, chunks, kind))
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

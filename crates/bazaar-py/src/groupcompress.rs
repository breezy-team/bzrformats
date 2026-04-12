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
struct GroupCompressBlock(bazaar::groupcompress::block::GroupCompressBlock);

#[pymethods]
impl GroupCompressBlock {
    #[new]
    fn new() -> Self {
        Self(bazaar::groupcompress::block::GroupCompressBlock::new())
    }

    fn __len__(&self) -> usize {
        self.0.len()
    }

    #[getter]
    fn _z_content<'a>(&mut self, py: Python<'a>) -> PyResult<Bound<'a, PyBytes>> {
        let ret = self.0.z_content();
        Ok(PyBytes::new(py, &ret))
    }

    #[getter]
    fn _content<'a>(&mut self, py: Python<'a>) -> PyResult<Option<Bound<'a, PyBytes>>> {
        let ret = self.0.content();
        Ok(ret.map(|x| PyBytes::new(py, x)))
    }

    #[getter]
    fn _content_length(&self) -> Option<usize> {
        self.0.content_length()
    }

    #[setter]
    fn set__content_length(&mut self, value: usize) {
        self.0.set_content_length(value);
    }

    #[getter]
    fn _z_content_length(&self) -> Option<usize> {
        self.0.z_content_length()
    }

    #[setter]
    fn set__z_content_length(&mut self, value: usize) {
        self.0.set_z_content_length(value);
    }

    #[setter]
    fn set__z_content_chunks(&mut self, chunks: Vec<Vec<u8>>) {
        self.0.set_z_content_chunks(chunks);
    }

    /// Test probe: `None` before a streaming decompressor has been created
    /// (or after full content has been realised directly), otherwise
    /// `True`. Matches the Python class's `_z_content_decompressor` attr.
    #[getter]
    fn _z_content_decompressor(&self) -> Option<bool> {
        if self.0.has_z_content_decompressor() {
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
        self.0.set_compressor(kind);
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
        Ok(Self(ret.unwrap()))
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
            .0
            .extract(start, end)
            .map_err(|e| PyValueError::new_err(format!("Error during extract: {:?}", e)))?;
        Ok(chunks
            .into_iter()
            .map(|x| PyBytes::new(py, x.as_ref()))
            .collect())
    }

    fn set_chunked_content(&mut self, data: Vec<Vec<u8>>, length: usize) -> PyResult<()> {
        self.0.set_chunked_content(data.as_slice(), length);
        Ok(())
    }

    fn set_content(&mut self, content: &[u8]) -> PyResult<()> {
        self.0.set_content(content);
        Ok(())
    }

    #[pyo3(signature = (kind = None))]
    fn to_chunks<'a>(
        &mut self,
        py: Python<'a>,
        kind: Option<bazaar::groupcompress::block::CompressorKind>,
    ) -> (usize, Vec<Bound<'a, PyBytes>>) {
        let (size, chunks) = self.0.to_chunks(kind);

        let chunks = chunks
            .into_iter()
            .map(|x| PyBytes::new(py, x.as_ref()))
            .collect();

        (size, chunks)
    }

    fn to_bytes<'a>(&mut self, py: Python<'a>) -> PyResult<Bound<'a, PyBytes>> {
        let ret = self.0.to_bytes();
        Ok(PyBytes::new(py, &ret))
    }

    #[pyo3(signature = (size = None))]
    fn _ensure_content(&mut self, size: Option<usize>) -> PyResult<()> {
        self.0.ensure_content(size);
        Ok(())
    }

    #[pyo3(signature = (include_text = None))]
    fn _dump(&mut self, py: Python, include_text: Option<bool>) -> PyResult<Py<PyAny>> {
        let ret = self
            .0
            .dump(include_text)
            .map_err(|e| PyValueError::new_err(format!("Error during dump: {:?}", e)))?;

        Ok(ret
            .into_iter()
            .map(|x| match x {
                bazaar::groupcompress::block::DumpInfo::Fulltext(text) => (
                    PyBytes::new(py, b"f"),
                    0usize,
                    text.map(|x| PyBytes::new(py, x.as_ref()).unbind().into()),
                ),
                bazaar::groupcompress::block::DumpInfo::Delta(decomp_len, info) => (
                    PyBytes::new(py, b"d"),
                    decomp_len,
                    Some(
                        info.into_iter()
                            .map(|x| match x {
                                bazaar::groupcompress::block::DeltaInfo::Copy(
                                    offset,
                                    len,
                                    text,
                                ) => (
                                    offset,
                                    len,
                                    text.map(|x| PyBytes::new(py, x.as_ref()).unbind()),
                                )
                                    .into_pyobject(py)
                                    .unwrap()
                                    .unbind(),
                                bazaar::groupcompress::block::DeltaInfo::Insert(len, data) => (
                                    0usize,
                                    len,
                                    data.map(|x| PyBytes::new(py, x.as_slice()).unbind()),
                                )
                                    .into_pyobject(py)
                                    .unwrap()
                                    .unbind(),
                            })
                            .collect::<Vec<_>>()
                            .into_pyobject(py)
                            .unwrap()
                            .unbind(),
                    ),
                ),
            })
            .collect::<Vec<_>>()
            .into_pyobject(py)?
            .unbind())
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

    fn flush<'a>(&mut self, py: Python<'a>) -> PyResult<(Vec<Bound<'a, PyBytes>>, usize)> {
        use bazaar::groupcompress::compressor::GroupCompressor;
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
        use bazaar::groupcompress::compressor::GroupCompressor;
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

//! pyo3 binding for `bzrformats.lru_cache.LRUSizeCache`.
//!
//! The LRU ordering and size-based eviction live in the pure-Rust
//! [`bazaar::lru_cache::LruOrder`]; this wrapper holds the Python keys and
//! values, computes value sizes via the optional `compute_size` callable
//! (defaulting to `len()`), and surfaces the dict-like API plus the
//! whitebox attributes (`_cache`, `_value_size`, `_max_size`, ...) the test
//! suite reads.

use bazaar::lru_cache::{LruOrder, NodeId};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::collections::HashMap;

/// A node handle handed out via the `_cache` mapping so whitebox callers can
/// do `node = cache._cache[key]; cache._remove_node(node)`. It carries the
/// Python key and value, mirroring the relevant `_LRUNode` attributes.
#[pyclass(name = "_LRUNode", module = "bzrformats._bzr_rs.lru_cache")]
struct LruNode {
    #[pyo3(get)]
    key: Py<PyAny>,
    #[pyo3(get)]
    value: Py<PyAny>,
}

/// `LRUSizeCache` — evicts entries based on the cumulative size of values.
///
/// Mirrors `bzrformats.lru_cache.LRUSizeCache`.
#[pyclass(name = "LRUSizeCache", module = "bzrformats._bzr_rs.lru_cache")]
pub struct LruSizeCache {
    order: LruOrder,
    /// node id -> (python key, python value, hash-key)
    entries: HashMap<NodeId, (Py<PyAny>, Py<PyAny>)>,
    /// python-key -> node id. The key is stored as a `Py<PyAny>` in a side
    /// table keyed by the key's `hash`/`eq` via a Python dict for lookup;
    /// to keep arbitrary hashable keys working we use a Python dict mapping
    /// key -> node id.
    key_to_id: Py<PyDict>,
    next_id: NodeId,
    max_size: usize,
    after_cleanup_size: usize,
    max_cache: usize,
    compute_size: Option<Py<PyAny>>,
}

impl LruSizeCache {
    /// Compute the size of `value` via the user callable or `len()`.
    fn value_size(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<usize> {
        match &self.compute_size {
            Some(cb) => cb.bind(py).call1((value,))?.extract(),
            None => value.len(),
        }
    }

    /// Drop a node id from the order and the Python-side maps.
    fn forget(&mut self, py: Python<'_>, id: NodeId) -> PyResult<()> {
        self.order.remove(id);
        if let Some((key, _value)) = self.entries.remove(&id) {
            self.key_to_id.bind(py).del_item(key.bind(py)).ok();
        }
        Ok(())
    }

    /// Evict LRU entries until the total size is under `after_cleanup_size`.
    fn cleanup_impl(&mut self, py: Python<'_>) -> PyResult<()> {
        let evicted = self.order.evict_until(self.after_cleanup_size);
        for id in evicted {
            if let Some((key, _value)) = self.entries.remove(&id) {
                self.key_to_id.bind(py).del_item(key.bind(py)).ok();
            }
        }
        Ok(())
    }
}

#[pymethods]
impl LruSizeCache {
    #[new]
    #[pyo3(signature = (max_size=1024 * 1024, after_cleanup_size=None, compute_size=None))]
    fn new(
        py: Python<'_>,
        max_size: usize,
        after_cleanup_size: Option<usize>,
        compute_size: Option<Py<PyAny>>,
    ) -> Self {
        let after_cleanup_size = match after_cleanup_size {
            Some(v) => v.min(max_size),
            None => max_size * 8 / 10,
        };
        // _update_max_cache(max(int(max_size // 512), 1)) from LRUCache.__init__
        let max_cache = std::cmp::max(max_size / 512, 1);
        Self {
            order: LruOrder::new(),
            entries: HashMap::new(),
            key_to_id: PyDict::new(py).unbind(),
            next_id: 0,
            max_size,
            after_cleanup_size,
            max_cache,
            compute_size,
        }
    }

    fn __contains__(&self, py: Python<'_>, key: Bound<'_, PyAny>) -> PyResult<bool> {
        self.key_to_id.bind(py).contains(key)
    }

    fn __len__(&self) -> usize {
        self.order.len()
    }

    fn __getitem__<'py>(
        &mut self,
        py: Python<'py>,
        key: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        match self.key_to_id.bind(py).get_item(&key)? {
            Some(id_obj) => {
                let id: NodeId = id_obj.extract()?;
                self.order.touch(id);
                let (_k, value) = &self.entries[&id];
                Ok(value.bind(py).clone())
            }
            None => Err(pyo3::exceptions::PyKeyError::new_err(key.unbind())),
        }
    }

    fn __setitem__(
        &mut self,
        py: Python<'_>,
        key: Bound<'_, PyAny>,
        value: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        // Mirror LRUSizeCache.__setitem__: reject the null-key sentinel.
        if is_null_key(py, &key)? {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "cannot use _null_key as a key",
            ));
        }
        let value_len = self.value_size(py, &value)?;
        let existing = self
            .key_to_id
            .bind(py)
            .get_item(&key)?
            .map(|o| o.extract::<NodeId>())
            .transpose()?;

        if value_len >= self.after_cleanup_size {
            // Too big to ever fit; drop any existing entry and bail.
            if let Some(id) = existing {
                self.forget(py, id)?;
            }
            return Ok(());
        }

        match existing {
            Some(id) => {
                // Replace value, adjusting the tracked size.
                self.order.update_size(id, value_len);
                self.entries
                    .insert(id, (key.clone().unbind(), value.unbind()));
                self.order.touch(id);
            }
            None => {
                let id = self.next_id;
                self.next_id += 1;
                self.order.insert(id, value_len);
                self.entries
                    .insert(id, (key.clone().unbind(), value.unbind()));
                self.key_to_id.bind(py).set_item(key, id)?;
            }
        }

        if self.order.total_size() > self.max_size {
            self.cleanup_impl(py)?;
        }
        Ok(())
    }

    #[pyo3(signature = (key, default=None))]
    fn get<'py>(
        &mut self,
        py: Python<'py>,
        key: Bound<'py, PyAny>,
        default: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        match self.key_to_id.bind(py).get_item(&key)? {
            Some(id_obj) => {
                let id: NodeId = id_obj.extract()?;
                self.order.touch(id);
                Ok(self.entries[&id].1.bind(py).clone())
            }
            None => Ok(default.unwrap_or_else(|| py.None().into_bound(py))),
        }
    }

    fn cache_size(&self) -> usize {
        self.max_size
    }

    /// An unordered snapshot of the currently-cached keys.
    fn keys<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        PyList::new(py, self.key_to_id.bind(py).keys())
    }

    /// A fresh dict with the same key:value pairs as the cache.
    fn as_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let out = PyDict::new(py);
        for (key, value) in self.entries.values() {
            out.set_item(key.bind(py), value.bind(py))?;
        }
        Ok(out)
    }

    fn cleanup(&mut self, py: Python<'_>) -> PyResult<()> {
        self.cleanup_impl(py)
    }

    fn clear(&mut self, py: Python<'_>) -> PyResult<()> {
        let drained = self.order.drain_lru();
        for id in drained {
            if let Some((key, _value)) = self.entries.remove(&id) {
                self.key_to_id.bind(py).del_item(key.bind(py)).ok();
            }
        }
        Ok(())
    }

    #[pyo3(signature = (max_size, after_cleanup_size=None))]
    fn resize(
        &mut self,
        py: Python<'_>,
        max_size: usize,
        after_cleanup_size: Option<usize>,
    ) -> PyResult<()> {
        self.max_size = max_size;
        self.after_cleanup_size = match after_cleanup_size {
            Some(v) => v.min(max_size),
            None => max_size * 8 / 10,
        };
        self.max_cache = std::cmp::max(max_size / 512, 1);
        // _update_max_cache triggers a cleanup in the Python LRUCache.
        self.cleanup_impl(py)
    }

    /// Whitebox: `cache._cache` is a `{key: _LRUNode}` mapping. Rebuilt on
    /// access; callers use it read-only to fetch a node and then call
    /// `_remove_node`.
    #[getter]
    fn _cache<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let out = PyDict::new(py);
        for (key, value) in self.entries.values() {
            let node = LruNode {
                key: key.clone_ref(py),
                value: value.clone_ref(py),
            };
            out.set_item(key.bind(py), Py::new(py, node)?)?;
        }
        Ok(out)
    }

    /// Whitebox: remove the entry for the given node's key.
    fn _remove_node(&mut self, py: Python<'_>, node: Bound<'_, LruNode>) -> PyResult<()> {
        let key = node.borrow().key.clone_ref(py);
        if let Some(id_obj) = self.key_to_id.bind(py).get_item(key.bind(py))? {
            let id: NodeId = id_obj.extract()?;
            self.forget(py, id)?;
        }
        Ok(())
    }

    #[getter]
    fn _value_size(&self) -> usize {
        self.order.total_size()
    }

    #[getter]
    fn _max_size(&self) -> usize {
        self.max_size
    }

    #[getter]
    fn _after_cleanup_size(&self) -> usize {
        self.after_cleanup_size
    }

    #[getter]
    fn _max_cache(&self) -> usize {
        self.max_cache
    }

    #[getter]
    fn _compute_size(&self, py: Python<'_>) -> Py<PyAny> {
        match &self.compute_size {
            Some(cb) => cb.clone_ref(py),
            // Default mirrors the Python `self._compute_size = len`.
            None => py
                .eval(
                    std::ffi::CString::new("len").unwrap().as_c_str(),
                    None,
                    None,
                )
                .map(|o| o.unbind())
                .unwrap_or_else(|_| py.None()),
        }
    }
}

/// Is `key` the `bzrformats.lru_cache._null_key` sentinel?
fn is_null_key(py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<bool> {
    let sentinel = py.import("bzrformats.lru_cache")?.getattr("_null_key")?;
    Ok(key.is(&sentinel))
}

pub(crate) fn _lru_cache_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "lru_cache")?;
    m.add_class::<LruSizeCache>()?;
    m.add_class::<LruNode>()?;
    Ok(m)
}

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
///
/// For `LRUCache` (count-based) the `prev`/`next_key` fields are also wired so
/// the test helper `walk_lru` can traverse the most-to-least-recently-used
/// chain; for `LRUSizeCache` only `key`/`value` are populated.
#[pyclass(name = "_LRUNode", module = "bzrformats._bzr_rs.lru_cache")]
struct LruNode {
    #[pyo3(get)]
    key: Py<PyAny>,
    #[pyo3(get)]
    value: Py<PyAny>,
    /// The more-recently-used neighbour (`None` for the MRU head).
    #[pyo3(get)]
    prev: Option<Py<LruNode>>,
    /// The key of the less-recently-used neighbour, or the `_null_key`
    /// sentinel for the LRU tail.
    #[pyo3(get)]
    next_key: Py<PyAny>,
}

#[pymethods]
impl LruNode {
    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let prev_key = match &self.prev {
            Some(p) => p.borrow(py).key.bind(py).repr()?.to_string(),
            None => "None".to_string(),
        };
        Ok(format!(
            "_LRUNode({} n:{} p:{})",
            self.key.bind(py).repr()?,
            self.next_key.bind(py).repr()?,
            prev_key,
        ))
    }
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
                prev: None,
                next_key: py.None(),
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

/// `LRUCache` — a count-based least-recently-used cache.
///
/// Mirrors `bzrformats.lru_cache.LRUCache`: it caches up to `max_cache`
/// entries and, once exceeded, evicts least-recently-used entries down to
/// `after_cleanup_count`. The ordering lives in [`LruOrder`] with every entry
/// given size 1, so the count is the total size.
#[pyclass(name = "LRUCache", module = "bzrformats._bzr_rs.lru_cache")]
pub struct LruCache {
    order: LruOrder,
    /// node id -> (python key, python value)
    entries: HashMap<NodeId, (Py<PyAny>, Py<PyAny>)>,
    /// python-key -> node id (a Python dict so arbitrary hashable keys work)
    key_to_id: Py<PyDict>,
    next_id: NodeId,
    max_cache: usize,
    after_cleanup_count: usize,
    /// Memoised `_LRUNode` chain for the whitebox getters, so a single
    /// `walk_lru` traversal sees one consistent set of node objects. Cleared
    /// on any mutation (including reorder-on-access).
    chain: Option<(Option<Py<LruNode>>, Option<Py<LruNode>>, Py<PyDict>)>,
}

impl LruCache {
    /// Invalidate the memoised whitebox chain after a mutation/reorder.
    fn dirty(&mut self) {
        self.chain = None;
    }

    fn forget(&mut self, py: Python<'_>, id: NodeId) {
        self.dirty();
        self.order.remove(id);
        if let Some((key, _value)) = self.entries.remove(&id) {
            self.key_to_id.bind(py).del_item(key.bind(py)).ok();
        }
    }

    fn cleanup_impl(&mut self, py: Python<'_>) {
        self.dirty();
        let evicted = self.order.evict_until(self.after_cleanup_count);
        for id in evicted {
            if let Some((key, _value)) = self.entries.remove(&id) {
                self.key_to_id.bind(py).del_item(key.bind(py)).ok();
            }
        }
    }

    fn set_max_cache(&mut self, py: Python<'_>, max_cache: usize, after: Option<usize>) {
        self.max_cache = max_cache;
        self.after_cleanup_count = match after {
            Some(v) => v.min(max_cache),
            None => max_cache * 8 / 10,
        };
        self.cleanup_impl(py);
    }
}

#[pymethods]
impl LruCache {
    #[new]
    #[pyo3(signature = (max_cache=100, after_cleanup_count=None))]
    fn new(py: Python<'_>, max_cache: usize, after_cleanup_count: Option<usize>) -> Self {
        let after_cleanup_count = match after_cleanup_count {
            Some(v) => v.min(max_cache),
            None => max_cache * 8 / 10,
        };
        Self {
            order: LruOrder::new(),
            entries: HashMap::new(),
            key_to_id: PyDict::new(py).unbind(),
            next_id: 0,
            max_cache,
            after_cleanup_count,
            chain: None,
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
                self.dirty();
                Ok(self.entries[&id].1.bind(py).clone())
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
        if is_null_key(py, &key)? {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "cannot use _null_key as a key",
            ));
        }
        self.dirty();
        let existing = self
            .key_to_id
            .bind(py)
            .get_item(&key)?
            .map(|o| o.extract::<NodeId>())
            .transpose()?;
        match existing {
            Some(id) => {
                self.entries
                    .insert(id, (key.clone().unbind(), value.unbind()));
                self.order.touch(id);
            }
            None => {
                let id = self.next_id;
                self.next_id += 1;
                self.order.insert(id, 1);
                self.entries
                    .insert(id, (key.clone().unbind(), value.unbind()));
                self.key_to_id.bind(py).set_item(key, id)?;
            }
        }
        if self.order.len() > self.max_cache {
            self.cleanup_impl(py);
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
                self.dirty();
                Ok(self.entries[&id].1.bind(py).clone())
            }
            None => Ok(default.unwrap_or_else(|| py.None().into_bound(py))),
        }
    }

    fn cache_size(&self) -> usize {
        self.max_cache
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

    fn cleanup(&mut self, py: Python<'_>) {
        self.cleanup_impl(py)
    }

    fn clear(&mut self, py: Python<'_>) {
        self.dirty();
        let drained = self.order.drain_lru();
        for id in drained {
            if let Some((key, _value)) = self.entries.remove(&id) {
                self.key_to_id.bind(py).del_item(key.bind(py)).ok();
            }
        }
    }

    #[pyo3(signature = (max_cache, after_cleanup_count=None))]
    fn resize(&mut self, py: Python<'_>, max_cache: usize, after_cleanup_count: Option<usize>) {
        self.set_max_cache(py, max_cache, after_cleanup_count)
    }

    /// Whitebox: remove the entry for the given node's key.
    fn _remove_node(&mut self, py: Python<'_>, node: Bound<'_, LruNode>) -> PyResult<()> {
        let key = node.borrow().key.clone_ref(py);
        if let Some(id_obj) = self.key_to_id.bind(py).get_item(key.bind(py))? {
            let id: NodeId = id_obj.extract()?;
            self.forget(py, id);
        }
        Ok(())
    }

    #[getter]
    fn _max_cache(&self) -> usize {
        self.max_cache
    }

    #[getter]
    fn _after_cleanup_count(&self) -> usize {
        self.after_cleanup_count
    }

    /// Whitebox: the most-recently-used `_LRUNode`, or `None` when empty.
    /// Built together with the rest of the chain so `walk_lru` sees a
    /// consistent doubly-linked list.
    #[getter]
    fn _most_recently_used(&mut self, py: Python<'_>) -> PyResult<Option<Py<LruNode>>> {
        self.ensure_chain(py)?;
        Ok(self
            .chain
            .as_ref()
            .unwrap()
            .0
            .as_ref()
            .map(|n| n.clone_ref(py)))
    }

    /// Whitebox: the least-recently-used `_LRUNode`, or `None` when empty.
    #[getter]
    fn _least_recently_used(&mut self, py: Python<'_>) -> PyResult<Option<Py<LruNode>>> {
        self.ensure_chain(py)?;
        Ok(self
            .chain
            .as_ref()
            .unwrap()
            .1
            .as_ref()
            .map(|n| n.clone_ref(py)))
    }

    /// Whitebox: the `{key: _LRUNode}` mapping with `prev`/`next_key` wired so
    /// the test helper `walk_lru` can traverse the chain.
    #[getter]
    fn _cache<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        self.ensure_chain(py)?;
        Ok(self.chain.as_ref().unwrap().2.bind(py).clone())
    }
}

impl LruCache {
    /// Build the whitebox `_LRUNode` chain if not already memoised. The same
    /// node objects then back `_most_recently_used`, `_least_recently_used`
    /// and `_cache` until the next mutation, so a single `walk_lru` traversal
    /// sees one consistent doubly-linked list.
    fn ensure_chain(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.chain.is_none() {
            self.chain = Some(self.build_chain(py)?);
        }
        Ok(())
    }

    /// Materialise the doubly-linked `_LRUNode` chain in MRU-to-LRU order.
    /// Returns `(most_recently_used, least_recently_used, {key: node})`.
    ///
    /// `prev` is wired to the already-created more-recently-used neighbour and
    /// `next_key` to the less-recently-used neighbour's key (or `_null_key`
    /// for the tail).
    fn build_chain(
        &self,
        py: Python<'_>,
    ) -> PyResult<(Option<Py<LruNode>>, Option<Py<LruNode>>, Py<PyDict>)> {
        let order = self.order.order_mru_to_lru();
        let cache = PyDict::new(py);
        let null_key = null_key_sentinel(py)?;
        let mut nodes: Vec<Py<LruNode>> = Vec::with_capacity(order.len());
        for (idx, id) in order.iter().enumerate() {
            let (key, value) = &self.entries[id];
            let next_key = match order.get(idx + 1) {
                Some(next_id) => self.entries[next_id].0.clone_ref(py),
                None => null_key.clone_ref(py),
            };
            let prev = nodes.last().map(|n| n.clone_ref(py));
            let node = Py::new(
                py,
                LruNode {
                    key: key.clone_ref(py),
                    value: value.clone_ref(py),
                    prev,
                    next_key,
                },
            )?;
            cache.set_item(key.bind(py), node.clone_ref(py))?;
            nodes.push(node);
        }
        let mru = nodes.first().map(|n| n.clone_ref(py));
        let lru = nodes.last().map(|n| n.clone_ref(py));
        Ok((mru, lru, cache.unbind()))
    }
}

/// `FIFOCache` — a `dict` subclass that evicts the oldest entries first.
///
/// Mirrors `bzrformats.lru_cache.FIFOCache`. The key/value storage is the
/// `dict` base; this wrapper layers a FIFO insertion `_queue` and an optional
/// per-key `_cleanup` callback invoked on eviction/removal.
#[pyclass(name = "FIFOCache", extends = pyo3::types::PyDict, module = "bzrformats._bzr_rs.lru_cache")]
pub struct FifoCache {
    max_cache: usize,
    after_cleanup_count: usize,
    /// Insertion order of live keys (front = oldest).
    queue: std::collections::VecDeque<Py<PyAny>>,
    /// key -> cleanup callable, applied when the key leaves the cache.
    cleanup: Py<PyDict>,
}

impl FifoCache {
    fn dict<'py>(slf: &Bound<'py, Self>) -> Bound<'py, PyDict> {
        slf.clone().into_any().cast_into::<PyDict>().unwrap()
    }

    /// Drop a key from the dict and fire its cleanup callback if any.
    fn remove(slf: &Bound<'_, Self>, key: &Bound<'_, PyAny>) -> PyResult<()> {
        let py = slf.py();
        let cleanup = slf.borrow().cleanup.bind(py).clone();
        let cb = cleanup.get_item(key)?;
        if cb.is_some() {
            cleanup.del_item(key)?;
        }
        let dict = Self::dict(slf);
        let val = dict.get_item(key)?;
        dict.del_item(key)?;
        if let (Some(cb), Some(val)) = (cb, val) {
            cb.call1((key, val))?;
        }
        Ok(())
    }

    fn remove_oldest(slf: &Bound<'_, Self>) -> PyResult<()> {
        let key = slf.borrow_mut().queue.pop_front();
        if let Some(key) = key {
            Self::remove(slf, key.bind(slf.py()))?;
        }
        Ok(())
    }
}

#[pymethods]
impl FifoCache {
    #[new]
    #[pyo3(signature = (max_cache=100, after_cleanup_count=None))]
    fn new(py: Python<'_>, max_cache: usize, after_cleanup_count: Option<usize>) -> Self {
        let after_cleanup_count = match after_cleanup_count {
            Some(v) => v.min(max_cache),
            None => max_cache * 8 / 10,
        };
        Self {
            max_cache,
            after_cleanup_count,
            queue: std::collections::VecDeque::new(),
            cleanup: PyDict::new(py).unbind(),
        }
    }

    /// Swallow the constructor arguments so the `dict` base is not initialised
    /// with them (otherwise `dict(max_cache=.., after_cleanup_count=..)` would
    /// populate the cache with those kwargs as entries).
    #[pyo3(signature = (max_cache=100, after_cleanup_count=None))]
    fn __init__(&self, max_cache: usize, after_cleanup_count: Option<usize>) {
        let _ = (max_cache, after_cleanup_count);
    }

    fn __setitem__(
        slf: &Bound<'_, Self>,
        key: Bound<'_, PyAny>,
        value: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        Self::add(slf, key, value, None)
    }

    fn __delitem__(slf: &Bound<'_, Self>, key: Bound<'_, PyAny>) -> PyResult<()> {
        // Remove from the FIFO queue, then from the dict (firing cleanup).
        {
            let mut me = slf.borrow_mut();
            if let Some(pos) = me
                .queue
                .iter()
                .position(|k| k.bind(slf.py()).eq(&key).unwrap_or(false))
            {
                me.queue.remove(pos);
            }
        }
        Self::remove(slf, &key)
    }

    #[pyo3(signature = (key, value, cleanup=None))]
    fn add(
        slf: &Bound<'_, Self>,
        key: Bound<'_, PyAny>,
        value: Bound<'_, PyAny>,
        cleanup: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let py = slf.py();
        let dict = Self::dict(slf);
        if dict.contains(&key)? {
            // Replace: drop the existing entry (and its cleanup) first.
            FifoCache::__delitem__(slf, key.clone())?;
        }
        slf.borrow_mut().queue.push_back(key.clone().unbind());
        dict.set_item(&key, value)?;
        if let Some(cb) = cleanup {
            slf.borrow().cleanup.bind(py).set_item(&key, cb)?;
        }
        let (len, max) = {
            let me = slf.borrow();
            (dict.len(), me.max_cache)
        };
        if len > max {
            Self::cleanup(slf)?;
        }
        Ok(())
    }

    fn cache_size(&self) -> usize {
        self.max_cache
    }

    #[getter]
    fn _max_cache(&self) -> usize {
        self.max_cache
    }

    #[getter]
    fn _after_cleanup_count(&self) -> usize {
        self.after_cleanup_count
    }

    fn cleanup(slf: &Bound<'_, Self>) -> PyResult<()> {
        while Self::dict(slf).len() > slf.borrow().after_cleanup_count {
            Self::remove_oldest(slf)?;
        }
        Ok(())
    }

    fn clear(slf: &Bound<'_, Self>) -> PyResult<()> {
        while Self::dict(slf).len() > 0 {
            Self::remove_oldest(slf)?;
        }
        Ok(())
    }

    #[pyo3(signature = (max_cache, after_cleanup_count=None))]
    fn resize(
        slf: &Bound<'_, Self>,
        max_cache: usize,
        after_cleanup_count: Option<usize>,
    ) -> PyResult<()> {
        {
            let mut me = slf.borrow_mut();
            me.max_cache = max_cache;
            me.after_cleanup_count = match after_cleanup_count {
                Some(v) => v.min(max_cache),
                None => max_cache * 8 / 10,
            };
        }
        if Self::dict(slf).len() > max_cache {
            Self::cleanup(slf)?;
        }
        Ok(())
    }

    #[pyo3(signature = (key, defaultval=None))]
    fn setdefault<'py>(
        slf: &Bound<'py, Self>,
        key: Bound<'py, PyAny>,
        defaultval: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let dict = Self::dict(slf);
        if let Some(v) = dict.get_item(&key)? {
            return Ok(v);
        }
        let defaultval = defaultval.unwrap_or_else(|| py.None().into_bound(py));
        Self::add(slf, key, defaultval.clone(), None)?;
        Ok(defaultval)
    }
}

/// Fetch the `bzrformats.lru_cache._null_key` sentinel object.
fn null_key_sentinel(py: Python<'_>) -> PyResult<Py<PyAny>> {
    Ok(py
        .import("bzrformats.lru_cache")?
        .getattr("_null_key")?
        .unbind())
}

/// Is `key` the `bzrformats.lru_cache._null_key` sentinel?
fn is_null_key(py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<bool> {
    let sentinel = py.import("bzrformats.lru_cache")?.getattr("_null_key")?;
    Ok(key.is(&sentinel))
}

pub(crate) fn _lru_cache_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "lru_cache")?;
    m.add_class::<LruCache>()?;
    m.add_class::<LruSizeCache>()?;
    m.add_class::<FifoCache>()?;
    m.add_class::<LruNode>()?;
    Ok(m)
}

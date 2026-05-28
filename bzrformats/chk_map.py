# Copyright (C) 2008-2011 Canonical Ltd
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

r"""Persistent maps from tuple_of_strings->string using CHK stores.

Overview and current status:

The CHKMap class implements a dict from tuple_of_strings->string by using a trie
with internal nodes of 8-bit fan out; The key tuples are mapped to strings by
joining them by \x00, and \x00 padding shorter keys out to the length of the
longest key. Leaf nodes are packed as densely as possible, and internal nodes
are all an additional 8-bits wide leading to a sparse upper tree.

Updates to a CHKMap are done preferentially via the apply_delta method, to
allow optimisation of the update operation; but individual map/unmap calls are
possible and supported. Individual changes via map/unmap are buffered in memory
until the _save method is called to force serialisation of the tree.
apply_delta records its changes immediately by performing an implicit _save.

Todo:
-----
Densely packed upper nodes.

"""

import abc
import heapq
from collections.abc import Callable, Generator, Iterator
from typing import Union

from . import lru_cache
from ._bzr_rs import chk_map as _chk_map_rs
from .registry import Registry

common_prefix_many = _chk_map_rs.common_prefix_many
common_prefix_pair = _chk_map_rs.common_prefix_pair

Key = tuple[bytes, ...]
SerialisedKey = bytes
SearchKeyFunc = Callable[[Key], bytes]
KeyFilter = list[Key]


clear_cache = _chk_map_rs.clear_cache
_page_cache_get = _chk_map_rs._page_cache_get
_page_cache_set = _chk_map_rs._page_cache_set


class _PageCacheProxy:
    """Dict-like view onto the Rust-backed CHK page cache.

    Returned by :func:`_get_cache` for compatibility with callers
    (notably breezy's test suite) that historically reached into the
    pure-Python ``LRUSizeCache`` instance directly.
    """

    def __getitem__(self, key):
        value = _page_cache_get(key)
        if value is None:
            raise KeyError(key)
        return value

    def __setitem__(self, key, value):
        _page_cache_set(key, value)

    def __contains__(self, key):
        return _page_cache_get(key) is not None


_page_cache_proxy = _PageCacheProxy()


def _get_cache():
    """Return a dict-like view onto the shared CHK page cache."""
    return _page_cache_proxy


# Plain search-key transform comes from the Rust extension so the
# pyclass `_search_key_func` getter and the registry hand back the
# same callable object (identity comparisons in tests rely on this).
_search_key_plain = _chk_map_rs._search_key_plain


search_key_registry = Registry[bytes, Callable[[Key], SerialisedKey], None]()
search_key_registry.register(b"plain", _search_key_plain)


def _deserialise_leaf_node(data, key, search_key_func=None):
    """Deserialise bytes into a LeafNode pyclass instance.

    Wraps a bare-bytes `key` into a 1-tuple — some callers/tests
    pass a placeholder bytes value where the canonical form is
    `(b"sha1:...",)`.
    """
    if isinstance(key, bytes):
        key = (key,)
    return _chk_map_rs.LeafNode.deserialise(data, key, search_key_func)


def _deserialise_internal_node(data, key, search_key_func=None):
    """Deserialise bytes into an InternalNode pyclass instance."""
    if isinstance(key, bytes):
        key = (key,)
    return _chk_map_rs.InternalNode.deserialise(data, key, search_key_func)


CHKMap = _chk_map_rs.CHKMap


def _chkmap_dump_tree(self, include_keys=False, encoding="utf-8"):
    self._ensure_root()

    def decode(x):
        return x.decode(encoding)

    res = self._dump_tree_node(
        self._root_node,
        prefix=b"",
        indent="",
        decode=decode,
        include_keys=include_keys,
    )
    res.append("")
    return "\n".join(res)


def _chkmap_dump_tree_node(self, node, prefix, indent, decode, include_keys=True):
    result = []
    if not include_keys:
        key_str = ""
    else:
        node_key = node.key()
        key_str = f" {decode(node_key[0])}" if node_key is not None else " None"
    result.append(f"{indent}{decode(prefix)!r} {node.__class__.__name__}{key_str}")
    if isinstance(node, InternalNode):
        list(node._iter_nodes(self._store))
        for prefix, sub in sorted(node._items.items()):
            result.extend(
                self._dump_tree_node(
                    sub,
                    prefix,
                    indent + "  ",
                    decode=decode,
                    include_keys=include_keys,
                )
            )
    else:
        for key, value in sorted(node._items.items()):
            result.append(
                f"      {tuple([decode(ke) for ke in key])!r} {decode(value)!r}"
            )
    return result


def _chkmap_iter_changes(self, basis):
    """Iterate over the changes between basis and self.

    Yields (key, old_value, new_value). Old_value is None for keys
    only in self; new_value is None for keys only in basis.
    """
    if self._node_key(self._root_node) == self._node_key(basis._root_node):
        return
    self._ensure_root()
    basis._ensure_root()
    excluded_keys = set()
    self_node = self._root_node
    basis_node = basis._root_node
    self_pending = []
    basis_pending = []

    def process_node(node, path, a_map, pending):
        node = a_map._get_node(node)
        if isinstance(node, LeafNode):
            path = (node._key, path)
            for key, value in node._items.items():
                search_key = node._search_key_func(key)
                heapq.heappush(pending, (search_key, key, value, path))
        else:
            path = (node._key, path)
            for prefix, child in node._items.items():
                heapq.heappush(pending, (prefix, None, child, path))

    def process_common_internal_nodes(self_node, basis_node):
        self_items = set(self_node._items.items())
        basis_items = set(basis_node._items.items())
        path = (self_node._key, None)
        for prefix, child in self_items - basis_items:
            heapq.heappush(self_pending, (prefix, None, child, path))
        path = (basis_node._key, None)
        for prefix, child in basis_items - self_items:
            heapq.heappush(basis_pending, (prefix, None, child, path))

    def process_common_leaf_nodes(self_node, basis_node):
        self_items = set(self_node._items.items())
        basis_items = set(basis_node._items.items())
        path = (self_node._key, None)
        for key, value in self_items - basis_items:
            prefix = self._search_key_func(key)
            heapq.heappush(self_pending, (prefix, key, value, path))
        path = (basis_node._key, None)
        for key, value in basis_items - self_items:
            prefix = basis._search_key_func(key)
            heapq.heappush(basis_pending, (prefix, key, value, path))

    def process_common_prefix_nodes(self_node, self_path, basis_node, basis_path):
        self_node = self._get_node(self_node)
        basis_node = basis._get_node(basis_node)
        if isinstance(self_node, InternalNode) and isinstance(
            basis_node, InternalNode
        ):
            process_common_internal_nodes(self_node, basis_node)
        elif isinstance(self_node, LeafNode) and isinstance(basis_node, LeafNode):
            process_common_leaf_nodes(self_node, basis_node)
        else:
            process_node(self_node, self_path, self, self_pending)
            process_node(basis_node, basis_path, basis, basis_pending)

    process_common_prefix_nodes(self_node, None, basis_node, None)
    excluded_keys = set()

    def check_excluded(key_path):
        while key_path is not None:
            key, key_path = key_path
            if key in excluded_keys:
                return True
        return False

    while self_pending or basis_pending:
        if not self_pending:
            for _prefix, key, node, path in basis_pending:
                if check_excluded(path):
                    continue
                node = basis._get_node(node)
                if key is not None:
                    yield (key, node, None)
                else:
                    for key, value in node.iteritems(basis._store):
                        yield (key, value, None)
            return
        elif not basis_pending:
            for _prefix, key, node, path in self_pending:
                if check_excluded(path):
                    continue
                node = self._get_node(node)
                if key is not None:
                    yield (key, None, node)
                else:
                    for key, value in node.iteritems(self._store):
                        yield (key, None, value)
            return
        else:
            if self_pending[0][0] < basis_pending[0][0]:
                _prefix, key, node, path = heapq.heappop(self_pending)
                if check_excluded(path):
                    continue
                if key is not None:
                    yield (key, None, node)
                else:
                    process_node(node, path, self, self_pending)
                    continue
            elif self_pending[0][0] > basis_pending[0][0]:
                _prefix, key, node, path = heapq.heappop(basis_pending)
                if check_excluded(path):
                    continue
                if key is not None:
                    yield (key, node, None)
                else:
                    process_node(node, path, basis, basis_pending)
                    continue
            else:
                if self_pending[0][1] is None:
                    read_self = True
                else:
                    read_self = False
                if basis_pending[0][1] is None:
                    read_basis = True
                else:
                    read_basis = False
                if not read_self and not read_basis:
                    self_details = heapq.heappop(self_pending)
                    basis_details = heapq.heappop(basis_pending)
                    if self_details[2] != basis_details[2]:
                        yield (self_details[1], basis_details[2], self_details[2])
                    continue
                if self._node_key(self_pending[0][2]) == self._node_key(
                    basis_pending[0][2]
                ):
                    heapq.heappop(self_pending)
                    heapq.heappop(basis_pending)
                    continue
                if read_self and read_basis:
                    self_prefix, _, self_node, self_path = heapq.heappop(self_pending)
                    basis_prefix, _, basis_node, basis_path = heapq.heappop(
                        basis_pending
                    )
                    if self_prefix != basis_prefix:
                        raise AssertionError(f"{self_prefix!r} != {basis_prefix!r}")
                    process_common_prefix_nodes(
                        self_node, self_path, basis_node, basis_path
                    )
                    continue
                if read_self:
                    _prefix, key, node, path = heapq.heappop(self_pending)
                    if check_excluded(path):
                        continue
                    process_node(node, path, self, self_pending)
                if read_basis:
                    _prefix, key, node, path = heapq.heappop(basis_pending)
                    if check_excluded(path):
                        continue
                    process_node(node, path, basis, basis_pending)


# Bind orchestration methods onto the CHKMap pyclass at module load.
# apply_delta, from_dict, _create_via_map and _create_directly are native
# pyclass methods; the tree dump and iter_changes stay in Python.
CHKMap._dump_tree = _chkmap_dump_tree
CHKMap._dump_tree_node = _chkmap_dump_tree_node
CHKMap.iter_changes = _chkmap_iter_changes


class Node(metaclass=abc.ABCMeta):
    """Base class defining the protocol for CHK Map nodes.

    :ivar _raw_size: The total size of the serialized key:value data, before
        adding the header bytes, and without prefix compression.

    The Rust-backed pyclass LeafNode is registered as a virtual
    subclass at the bottom of this module so `isinstance(_, Node)`
    works uniformly across LeafNode and InternalNode.
    """

    __slots__ = (
        "_items",
        "_key",
        "_key_width",
        "_len",
        "_maximum_size",
        "_raw_size",
        "_search_key_func",
        "_search_prefix",
    )

    def __init__(self, key_width=1):
        """Create a node.

        :param key_width: The width of keys for this node.
        """
        self._key = None
        # Current number of elements
        self._len = 0
        self._maximum_size = 0
        self._key_width = key_width
        # current size in bytes
        self._raw_size = 0
        # The pointers/values this node has - meaning defined by child classes.
        self._items = {}
        # The common search prefix
        self._search_prefix = None

    def __repr__(self):
        """Return string representation of the node."""
        items_str = str(sorted(self._items))
        if len(items_str) > 20:
            items_str = items_str[:16] + "...]"
        return "{}(key:{} len:{} size:{} max:{} prefix:{} items:{})".format(
            self.__class__.__name__,
            self._key,
            self._len,
            self._raw_size,
            self._maximum_size,
            self._search_prefix,
            items_str,
        )

    def iteritems(self, store, key_filter=None):
        """Iterate over items in the node.

        :param key_filter: A filter to apply to the node. It should be a
            list/set/dict or similar repeatedly iterable container.
        """
        raise NotImplementedError(self.iteritems)

    def unmap(self, store, key):
        """Unmap key from the node."""
        raise NotImplementedError(self.unmap)

    def map(self, store, key: Key, value):
        """Map key to value."""
        raise NotImplementedError(self.map)

    def key(self) -> Key:
        """Return the key for this node."""
        return self._key

    def __len__(self) -> int:
        """Return the number of items in this node."""
        return self._len

    @property
    def maximum_size(self) -> int:
        """What is the upper limit for adding references to a node."""
        return self._maximum_size

    def set_maximum_size(self, new_size):
        """Set the size threshold for nodes.

        :param new_size: The size at which no data is added to a node. 0 for
            unlimited.
        """
        self._maximum_size = new_size


# Singleton indicating we have not computed _search_prefix yet. Re-exported
# from the Rust extension so identity comparisons line up across the
# boundary: the LeafNode pyclass's `_search_prefix` getter returns this
# exact object when the underlying Rust state is `SearchPrefix::Unknown`.
_unknown = _chk_map_rs._unknown


LeafNode = _chk_map_rs.LeafNode


# Register the Rust-backed pyclasses with the Node ABC so existing
# `isinstance(_, Node)` checks across this module match LeafNode and
# InternalNode instances.
Node.register(LeafNode)


InternalNode = _chk_map_rs.InternalNode


# Virtual subclass for isinstance(_, Node) checks.
Node.register(InternalNode)


_deserialise = _chk_map_rs._deserialise


CHKMapDifference = _chk_map_rs.CHKMapDifference
iter_interesting_nodes = _chk_map_rs.iter_interesting_nodes


from ._bzr_rs import chk_map as _chk_map_rs

_bytes_to_text_key = _chk_map_rs._bytes_to_text_key
_search_key_16 = _chk_map_rs._search_key_16
_search_key_255 = _chk_map_rs._search_key_255

search_key_registry.register(b"hash-16-way", _search_key_16)
search_key_registry.register(b"hash-255-way", _search_key_255)


def _check_key(key):
    """Helper function to assert that a key is properly formatted.

    This generally shouldn't be used in production code, but it can be helpful
    to debug problems.
    """
    if not isinstance(key, tuple):
        raise TypeError(f"key {key!r} is not tuple but {type(key)}")
    if len(key) != 1:
        raise ValueError(f"key {key!r} should have length 1, not {len(key)}")
    if not isinstance(key[0], str):
        raise TypeError(f"key {key!r} should hold a str, not {type(key[0])!r}")
    if not key[0].startswith("sha1:"):
        raise ValueError(f"key {key!r} should point to a sha1:")

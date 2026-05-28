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
import logging
from collections.abc import Callable, Generator, Iterator
from typing import Union

from . import lru_cache
from ._bzr_rs import chk_map as _chk_map_rs
from .registry import Registry

logger = logging.getLogger("bzrformats.chk_map")

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


# If a ChildNode falls below this many bytes, we check for a remap
_INTERESTING_NEW_SIZE = 50
# If a ChildNode shrinks by more than this amount, we check for a remap
_INTERESTING_SHRINKAGE_LIMIT = 20


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


def _chkmap_apply_delta(self, delta):
    """Apply a delta to the map."""
    has_deletes = False
    new_items = {
        tuple(key) for (old, key, value) in delta if key is not None and old is None
    }
    existing_new = list(self.iteritems(key_filter=new_items))
    if existing_new:
        from .errors import InconsistentDeltaDelta

        raise InconsistentDeltaDelta(
            delta, f"New items are already in the map {existing_new!r}."
        )
    for old, new, _value in delta:
        if old is not None and old != new:
            self.unmap(old, check_remap=False)
            has_deletes = True
    for _old, new, value in delta:
        if new is not None:
            self.map(new, value)
    if has_deletes:
        self._check_remap()
    return self._save()


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


@classmethod
def _chkmap_from_dict(
    cls, store, initial_value, maximum_size=0, key_width=1, search_key_func=None
):
    """Create a CHKMap in store with initial_value as the content."""
    root_key = cls._create_directly(
        store,
        initial_value,
        maximum_size=maximum_size,
        key_width=key_width,
        search_key_func=search_key_func,
    )
    if not isinstance(root_key, tuple):
        raise AssertionError(f"we got a {type(root_key)} instead of a tuple")
    return root_key


@classmethod
def _chkmap_create_via_map(
    cls, store, initial_value, maximum_size=0, key_width=1, search_key_func=None
):
    result = cls(store, None, search_key_func=search_key_func)
    if not isinstance(result._root_node, Node):
        raise AssertionError("expected root node to be Node")
    result._root_node.set_maximum_size(maximum_size)
    result._root_node._key_width = key_width
    delta = [(None, key, value) for key, value in initial_value.items()]
    return result.apply_delta(delta)


@classmethod
def _chkmap_create_directly(
    cls, store, initial_value, maximum_size=0, key_width=1, search_key_func=None
):
    leaf_node = LeafNode(search_key_func=search_key_func)
    leaf_node.set_maximum_size(maximum_size)
    leaf_node._key_width = key_width
    leaf_node._items = {tuple(key): val for key, val in initial_value.items()}
    leaf_node._raw_size = sum(
        leaf_node._key_value_len(key, value)
        for key, value in leaf_node._items.items()
    )
    leaf_node._len = len(leaf_node._items)
    leaf_node._compute_search_prefix()
    leaf_node._compute_serialised_prefix()
    if (
        leaf_node._len > 1
        and maximum_size
        and leaf_node._current_size() > maximum_size
    ):
        prefix, node_details = leaf_node._split(store)
        if len(node_details) == 1:
            raise AssertionError("Failed to split using node._split")
        internal_node = InternalNode(prefix, search_key_func=search_key_func)
        internal_node.set_maximum_size(maximum_size)
        internal_node._key_width = key_width
        for split, subnode in node_details:
            internal_node.add_node(split, subnode)
        node = internal_node
    else:
        node = leaf_node
    keys = list(node.serialise(store))
    return keys[-1]


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
CHKMap.apply_delta = _chkmap_apply_delta
CHKMap._dump_tree = _chkmap_dump_tree
CHKMap._dump_tree_node = _chkmap_dump_tree_node
CHKMap.from_dict = _chkmap_from_dict
CHKMap._create_via_map = _chkmap_create_via_map
CHKMap._create_directly = _chkmap_create_directly
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


# Register the Rust-backed pyclass with the Node ABC so existing
# `isinstance(_, Node)` checks across this module match LeafNode
# instances. InternalNode (still pure Python) inherits Node directly.
Node.register(LeafNode)


InternalNode = _chk_map_rs.InternalNode


def _internal_iter_nodes(self, store, key_filter=None, batch_size=None):
    """Iterate over child nodes matching key_filter, demand-loading."""
    keys = {}
    shortcut = False
    if key_filter is None:
        shortcut = True
        for prefix, node in self._items.items():
            if isinstance(node, tuple):
                keys[node] = (prefix, None)
            elif isinstance(node, Node):
                yield node, None
            else:
                raise AssertionError("Invalid node type: {!r}".format(type(node)))
    elif len(key_filter) == 1:
        for key in key_filter:  # noqa: B007
            break
        search_prefix = self._search_prefix_filter(key)
        if len(search_prefix) == self._node_width:
            shortcut = True
            try:
                node = self._items[search_prefix]
            except KeyError:
                return
            if isinstance(node, tuple):
                keys[node] = (search_prefix, [key])
            elif isinstance(node, Node):
                yield node, [key]
                return
            else:
                raise AssertionError("Invalid node type: {!r}".format(type(node)))
    if not shortcut:
        prefix_to_keys = {}
        length_filters = {}
        node_key_filter = None
        if key_filter is None:
            raise AssertionError("key_filter must not be None")
        for key in key_filter:
            search_prefix = self._search_prefix_filter(key)
            length_filter = length_filters.setdefault(len(search_prefix), set())
            length_filter.add(search_prefix)
            prefix_to_keys.setdefault(search_prefix, []).append(key)

        if self._node_width in length_filters and len(length_filters) == 1:
            search_prefixes = length_filters[self._node_width]
            for search_prefix in search_prefixes:
                try:
                    node = self._items[search_prefix]
                except KeyError:
                    continue
                node_key_filter = prefix_to_keys[search_prefix]
                if isinstance(node, tuple):
                    keys[node] = (search_prefix, node_key_filter)
                elif isinstance(node, Node):
                    yield node, node_key_filter
                else:
                    raise AssertionError("Invalid node type: {!r}".format(type(node)))
        else:
            length_filters_itemview = length_filters.items()
            for prefix, node in self._items.items():
                node_key_filter = []
                for length, length_filter in length_filters_itemview:
                    sub_prefix = prefix[:length]
                    if sub_prefix in length_filter:
                        node_key_filter.extend(prefix_to_keys[sub_prefix])
                if node_key_filter:
                    if isinstance(node, tuple):
                        keys[node] = (prefix, node_key_filter)
                    elif isinstance(node, Node):
                        yield node, node_key_filter
                    else:
                        raise AssertionError("Invalid node type: {!r}".format(type(node)))
    if keys:
        found_keys = set()
        for key in keys:
            bytes = _page_cache_get(key)
            if bytes is None:
                continue
            node = _deserialise(bytes, key, search_key_func=self._search_key_func)
            prefix, node_key_filter = keys[key]
            if not isinstance(node, Node):
                raise AssertionError("Invalid node type: {!r}".format(type(node)))
            self._items[prefix] = node
            found_keys.add(key)
            yield node, node_key_filter
        for key in found_keys:
            del keys[key]
    if keys:
        if batch_size is None:
            batch_size = len(keys)
        key_order = list(keys)
        for batch_start in range(0, len(key_order), batch_size):
            batch = key_order[batch_start : batch_start + batch_size]
            stream = store.get_record_stream(batch, "unordered", True)
            node_and_filters = []
            for record in stream:
                bytes = record.get_bytes_as("fulltext")
                node = _deserialise(bytes, record.key, search_key_func=self._search_key_func)
                prefix, node_key_filter = keys[record.key]
                node_and_filters.append((node, node_key_filter))
                if not isinstance(node, Node):
                    raise AssertionError("Invalid node type: {!r}".format(type(node)))
                self._items[prefix] = node
                _page_cache_set(record.key, bytes)
            yield from node_and_filters


def _internal_iteritems(self, store, key_filter=None):
    """Iterate over items in this node and its children."""
    for node, node_filter in self._iter_nodes(store, key_filter=key_filter):
        yield from node.iteritems(store, key_filter=node_filter)


def _internal_map(self, store, key, value):
    """Map key to value, returning (prefix, [(node_prefix, node)])."""
    if not len(self._items):
        raise AssertionError("can't map in an empty InternalNode.")
    search_key = self._search_key(key)
    if self._node_width != len(self._search_prefix) + 1:
        raise AssertionError(
            "node width mismatch: %d is not %d"
            % (self._node_width, len(self._search_prefix) + 1)
        )
    if not search_key.startswith(self._search_prefix):
        new_prefix = common_prefix_pair(self._search_prefix, search_key)
        new_parent = InternalNode(new_prefix, search_key_func=self._search_key_func)
        new_parent.set_maximum_size(self._maximum_size)
        new_parent._key_width = self._key_width
        new_parent.add_node(self._search_prefix[: len(new_prefix) + 1], self)
        return new_parent.map(store, key, value)
    children = [node for node, _ in self._iter_nodes(store, key_filter=[key])]
    if children:
        child = children[0]
    else:
        child = _internal_new_child(self, search_key, LeafNode)
    old_len = len(child)
    old_size = child._current_size() if isinstance(child, LeafNode) else None
    prefix, node_details = child.map(store, key, value)
    if len(node_details) == 1:
        child = node_details[0][1]
        self._len = self._len - old_len + len(child)
        self._items[search_key] = child
        self._key = None
        new_node = self
        if isinstance(child, LeafNode):
            if old_size is None:
                logger.debug("checking remap as InternalNode -> LeafNode")
                new_node = _internal_check_remap(self, store)
            else:
                new_size = child._current_size()
                shrinkage = old_size - new_size
                if (
                    shrinkage > 0 and new_size < _INTERESTING_NEW_SIZE
                ) or shrinkage > _INTERESTING_SHRINKAGE_LIMIT:
                    logger.debug(
                        "checking remap as size shrunk by %d to be %d",
                        shrinkage,
                        new_size,
                    )
                    new_node = _internal_check_remap(self, store)
        if new_node._search_prefix is None:
            raise AssertionError("_search_prefix should not be None")
        return new_node._search_prefix, [(b"", new_node)]
    child = _internal_new_child(self, search_key, InternalNode)
    child._search_prefix = prefix
    for split, node in node_details:
        child.add_node(split, node)
    self._len = self._len - old_len + len(child)
    self._key = None
    return self._search_prefix, [(b"", self)]


def _internal_new_child(self, search_key, klass):
    """Create a new child node of type klass."""
    child = klass()
    child.set_maximum_size(self._maximum_size)
    child._key_width = self._key_width
    child._search_key_func = self._search_key_func
    self._items[search_key] = child
    return child


def _internal_serialise(self, store):
    """Serialise the node (and dirty children) to store, yielding sha1 keys."""
    for node in self._items.values():
        if isinstance(node, tuple):
            continue
        elif isinstance(node, Node):
            if node._key is not None:
                continue
            for key in node.serialise(store):
                yield key
        else:
            raise AssertionError(
                f"InternalNode._items should only contain tuples or Nodes, not {node.__class__}"
            )
    if self._search_prefix is None:
        raise AssertionError("_search_prefix should not be None")
    sorted_items = [
        (prefix, node[0] if isinstance(node, tuple) else node._key[0])
        for prefix, node in sorted(self._items.items())
    ]
    lines = _chk_map_rs._serialise_internal_node(
        self._maximum_size,
        self._key_width,
        self._len,
        self._search_prefix,
        sorted_items,
    )
    sha1, _, _ = store.add_lines((None,), (), lines)
    self._key = (b"sha1:" + sha1,)
    _page_cache_set(self._key, b"".join(lines))
    yield self._key


def _internal_split(self, offset):
    """Split this node into smaller nodes starting at offset.

    Yields (prefix, node) tuples — only meaningful when offset >= node_width,
    in which case it recurses into the children. Mostly unused in
    practice but kept for API parity.
    """
    if offset >= self._node_width:
        for node in self._items.values():
            yield from node._split(offset)


def _internal_unmap(self, store, key, check_remap=True):
    """Remove key from this subtree, returning the replacement node."""
    if not len(self._items):
        raise AssertionError("can't unmap in an empty InternalNode.")
    children = [node for node, _ in self._iter_nodes(store, key_filter=[key])]
    if children:
        child = children[0]
    else:
        raise KeyError(key)
    self._len -= 1
    unmapped = child.unmap(store, key)
    if unmapped is None:
        raise AssertionError("unmap returned None, but we expected a node")
    self._key = None
    search_key = self._search_key(key)
    if len(unmapped) == 0:
        del self._items[search_key]
        unmapped = None
    else:
        self._items[search_key] = unmapped
    if len(self._items) == 1:
        return list(self._items.values())[0]
    if isinstance(unmapped, InternalNode):
        return self
    if check_remap:
        return _internal_check_remap(self, store)
    else:
        return self


def _internal_check_remap(self, store):
    """Check if all keys in this subtree fit in a single LeafNode."""
    new_leaf = LeafNode(search_key_func=self._search_key_func)
    new_leaf.set_maximum_size(self._maximum_size)
    new_leaf._key_width = self._key_width
    for node, _ in self._iter_nodes(store, batch_size=16):
        if isinstance(node, InternalNode):
            return self
        for key, value in node._items.items():
            if new_leaf._map_no_split(key, value):
                return self
    logger.debug("remap generated a new LeafNode")
    return new_leaf


# Bind store-touching methods onto the pyclass.
InternalNode._iter_nodes = _internal_iter_nodes
InternalNode.iteritems = _internal_iteritems
InternalNode.map = _internal_map
InternalNode.serialise = _internal_serialise
InternalNode._split = _internal_split
InternalNode.unmap = _internal_unmap
InternalNode._check_remap = _internal_check_remap

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

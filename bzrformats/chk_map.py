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
from collections.abc import Callable

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

# The dict-like page-cache view, the node deserialisers (which normalise a
# bare-bytes key into a 1-tuple) and the _check_key debug helper are all
# implemented in Rust and re-exported here.
_PageCacheProxy = _chk_map_rs._PageCacheProxy
_get_cache = _chk_map_rs._get_cache
_deserialise_leaf_node = _chk_map_rs._deserialise_leaf_node
_deserialise_internal_node = _chk_map_rs._deserialise_internal_node
_check_key = _chk_map_rs._check_key


# Plain search-key transform comes from the Rust extension so the
# pyclass `_search_key_func` getter and the registry hand back the
# same callable object (identity comparisons in tests rely on this).
_search_key_plain = _chk_map_rs._search_key_plain


search_key_registry = Registry[bytes, Callable[[Key], SerialisedKey], None]()
search_key_registry.register(b"plain", _search_key_plain)


CHKMap = _chk_map_rs.CHKMap


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

    @abc.abstractmethod
    def iteritems(self, store, key_filter=None):
        """Iterate over items in the node.

        :param key_filter: A filter to apply to the node. It should be a
            list/set/dict or similar repeatedly iterable container.
        """
        raise NotImplementedError(self.iteritems)

    @abc.abstractmethod
    def unmap(self, store, key):
        """Unmap key from the node."""
        raise NotImplementedError(self.unmap)

    @abc.abstractmethod
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

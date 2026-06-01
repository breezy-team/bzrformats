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

from collections.abc import Callable

from ._bzr_rs import chk_map as _chk_map_rs

common_prefix_many = _chk_map_rs.common_prefix_many
common_prefix_pair = _chk_map_rs.common_prefix_pair

Key = tuple[bytes, ...]
SerialisedKey = bytes
SearchKeyFunc = Callable[[Key], bytes]
KeyFilter = list[Key]


clear_cache = _chk_map_rs.clear_cache
_page_cache_get = _chk_map_rs._page_cache_get
_page_cache_set = _chk_map_rs._page_cache_set

_PageCacheProxy = _chk_map_rs._PageCacheProxy
_get_cache = _chk_map_rs._get_cache
_deserialise_leaf_node = _chk_map_rs._deserialise_leaf_node
_deserialise_internal_node = _chk_map_rs._deserialise_internal_node
_check_key = _chk_map_rs._check_key


# Same object as the pyclass `_search_key_func` getter returns, so identity
# comparisons in tests hold.
_search_key_plain = _chk_map_rs._search_key_plain


# The search-key registry is built and pre-populated in Rust (the three
# built-in variants under "plain"/"hash-16-way"/"hash-255-way"); the callables
# it returns are the same objects the node/inventory `_search_key_func` getters
# return, so identity comparisons hold.
search_key_registry = _chk_map_rs.search_key_registry


CHKMap = _chk_map_rs.CHKMap


Node = _chk_map_rs.Node


# "_search_prefix not yet computed" sentinel. Same object the LeafNode pyclass
# `_search_prefix` getter returns for SearchPrefix::Unknown, so `is _unknown`
# checks hold across the boundary.
_unknown = _chk_map_rs._unknown


LeafNode = _chk_map_rs.LeafNode
InternalNode = _chk_map_rs.InternalNode


_deserialise = _chk_map_rs._deserialise


CHKMapDifference = _chk_map_rs.CHKMapDifference
iter_interesting_nodes = _chk_map_rs.iter_interesting_nodes


_bytes_to_text_key = _chk_map_rs._bytes_to_text_key
_search_key_16 = _chk_map_rs._search_key_16
_search_key_255 = _chk_map_rs._search_key_255

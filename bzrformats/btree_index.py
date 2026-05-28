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
#

"""B+Tree indices."""

import logging

from ._bzr_rs import btree_index as _btree_index_rs
from ._bzr_rs import btree_serializer as _btree_serializer

_BTSIGNATURE = b"B+Tree Graph Index 2\n"
_LEAF_FLAG = b"type=leaf\n"
_INTERNAL_FLAG = b"type=internal\n"

# Read by the Rust BTreeBuilder.write_nodes and BTreeGraphIndex via
# py.import — tests monkey-patch these via overrideAttr to force smaller
# trees.
_RESERVED_HEADER_BYTES = 120
_PAGE_SIZE = 4096

# 4K per page: 4MB - 1000 entries
_NODE_CACHE_SIZE = 1000


logger = logging.getLogger(name="bzrformats.btree_index")
evil_logger = logging.getLogger(name="bzrformats.evil")


class _DummyTransport:
    """Stand-in transport used when constructing in-memory backing
    indices from BTreeBuilder's spill output.

    BTreeGraphIndex requires a transport to read its underlying file,
    but spilled backings are read directly from an open file handle
    written to during the spill - the transport is never used. This
    class just satisfies the recommended_page_size protocol.
    """

    def recommended_page_size(self):
        return _PAGE_SIZE


BTreeBuilder = _btree_index_rs.BTreeBuilder
BTreeGraphIndex = _btree_index_rs.BTreeGraphIndex
_LeafNode = _btree_index_rs._LeafNode
_InternalNode = _btree_index_rs._InternalNode

# Factory used for chk indices: parses leaf bytes into the chk-optimised
# leaf node. Pack repositories assign this to a BTreeGraphIndex's
# `_leaf_factory` to read chk indices more efficiently.
_gcchk_factory = _btree_serializer._parse_into_chk

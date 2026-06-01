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

# The on-disk B+Tree page size, exposed for consumers that need to walk an
# index file page by page (e.g. ``brz dump-btree --raw``).
PAGE_SIZE = _btree_index_rs.PAGE_SIZE

logger = logging.getLogger(name="bzrformats.btree_index")
evil_logger = logging.getLogger(name="bzrformats.evil")


BTreeBuilder = _btree_index_rs.BTreeBuilder
BTreeGraphIndex = _btree_index_rs.BTreeGraphIndex
_LeafNode = _btree_index_rs._LeafNode
_InternalNode = _btree_index_rs._InternalNode

# Factory used for chk indices: parses leaf bytes into the chk-optimised
# leaf node. Pack repositories assign this to a BTreeGraphIndex's
# `_leaf_factory` to read chk indices more efficiently.
_gcchk_factory = _btree_serializer._parse_into_chk

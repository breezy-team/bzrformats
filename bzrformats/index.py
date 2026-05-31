# Copyright (C) 2007-2011 Canonical Ltd
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

"""Indexing facilities."""

__all__ = [
    "BadIndexData",
    "BadIndexDuplicateKey",
    "BadIndexFormatSignature",
    "BadIndexKey",
    "BadIndexOptions",
    "BadIndexValue",
    "CombinedGraphIndex",
    "GraphIndex",
    "GraphIndexBuilder",
    "GraphIndexPrefixAdapter",
    "InMemoryGraphIndex",
]

from ._bzr_rs import index as _index_rs

# The index error classes live in the Rust errors module; re-export them so
# bzrformats.index.BadIndex* keep working for callers and for the Rust
# import_exception!(bzrformats.index, ...) sites.
from .errors import (
    BadIndexData,
    BadIndexDuplicateKey,
    BadIndexFormatSignature,
    BadIndexKey,
    BadIndexOptions,
    BadIndexValue,
)


def _has_key_from_parent_map(self, key):
    """Check if this index has one key.

    Used as a method on objects that implement get_parent_map.
    """
    return key in self.get_parent_map([key])


def _missing_keys_from_parent_map(self, keys):
    return set(keys) - set(self.get_parent_map(keys))


GraphIndexBuilder = _index_rs.GraphIndexBuilder
GraphIndex = _index_rs.GraphIndex
InMemoryGraphIndex = _index_rs.InMemoryGraphIndex
CombinedGraphIndex = _index_rs.CombinedGraphIndex
GraphIndexPrefixAdapter = _index_rs.GraphIndexPrefixAdapter

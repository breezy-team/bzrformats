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
    "CombinedGraphIndex",
    "GraphIndex",
    "GraphIndexBuilder",
    "GraphIndexPrefixAdapter",
    "InMemoryGraphIndex",
]

import logging

from ._bzr_rs import index as _index_rs
from .errors import BzrFormatsError

logger = logging.getLogger("bzrformats.index")
evil_logger = logging.getLogger("bzrformats.evil")

_HEADER_READV = (0, 200)
_OPTION_KEY_ELEMENTS = b"key_elements="
_OPTION_LEN = b"len="
_OPTION_NODE_REFS = b"node_ref_lists="
_SIGNATURE = b"Bazaar Graph Index 1\n"


class BadIndexFormatSignature(BzrFormatsError):
    _fmt = "%(value)s is not an index of type %(_type)s."

    def __init__(self, value, _type):
        super().__init__()
        self.value = value
        self._type = _type


class BadIndexData(BzrFormatsError):
    _fmt = "Error in data for index %(value)s."

    def __init__(self, value):
        super().__init__()
        self.value = value


class BadIndexDuplicateKey(BzrFormatsError):
    _fmt = "The key '%(key)s' is already in index '%(index)s'."

    def __init__(self, key, index):
        super().__init__()
        self.key = key
        self.index = index


class BadIndexKey(BzrFormatsError):
    _fmt = "The key '%(key)s' is not a valid key."

    def __init__(self, key):
        super().__init__()
        self.key = key


class BadIndexOptions(BzrFormatsError):
    _fmt = "Could not parse options for index %(value)s."

    def __init__(self, value):
        super().__init__()
        self.value = value


class BadIndexValue(BzrFormatsError):
    _fmt = "The value '%(value)s' is not a valid value."

    def __init__(self, value):
        super().__init__()
        self.value = value


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

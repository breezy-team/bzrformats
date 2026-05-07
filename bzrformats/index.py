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


_RustGraphIndexBuilder = _index_rs.GraphIndexBuilder
_RustGraphIndex = _index_rs.GraphIndex
_RustInMemoryGraphIndex = _index_rs.InMemoryGraphIndex
_RustCombinedGraphIndex = _index_rs.CombinedGraphIndex
_RustGraphIndexPrefixAdapter = _index_rs.GraphIndexPrefixAdapter


def _wrap_iter(rust_method):
    """Wrap a Rust pyclass iter_* method to return a generator.

    Many callers rely on the validity checks (`BadIndexKey` etc.) firing
    only when the caller actually iterates the result, matching the
    historical Python generator semantics.
    """

    def wrapper(self, *args, **kwargs):
        yield from rust_method(self, *args, **kwargs)

    wrapper.__name__ = rust_method.__name__
    return wrapper


class GraphIndexBuilder(_RustGraphIndexBuilder):
    """A builder that can build a GraphIndex.

    See the Rust implementation for the file format.
    """

    iter_all_entries = _wrap_iter(_RustGraphIndexBuilder.iter_all_entries)
    iter_entries = _wrap_iter(_RustGraphIndexBuilder.iter_entries)
    iter_entries_prefix = _wrap_iter(_RustGraphIndexBuilder.iter_entries_prefix)


class GraphIndex(_RustGraphIndex):
    """Python facade over the pyo3-implemented graph index reader."""

    iter_entries_prefix = _wrap_iter(_RustGraphIndex.iter_entries_prefix)
    iter_entries = _wrap_iter(_RustGraphIndex.iter_entries)
    iter_all_entries = _wrap_iter(_RustGraphIndex.iter_all_entries)


class InMemoryGraphIndex(_RustInMemoryGraphIndex):
    """A GraphIndex which operates entirely out of memory and is mutable."""

    iter_all_entries = _wrap_iter(_RustInMemoryGraphIndex.iter_all_entries)
    iter_entries = _wrap_iter(_RustInMemoryGraphIndex.iter_entries)
    iter_entries_prefix = _wrap_iter(_RustInMemoryGraphIndex.iter_entries_prefix)

    def __lt__(self, other):
        """Order InMemoryGraphIndex relative to GraphIndex by hash."""
        if not isinstance(other, (GraphIndex, InMemoryGraphIndex)):
            raise TypeError(other)
        return hash(self) < hash(other)


class CombinedGraphIndex(_RustCombinedGraphIndex):
    """A GraphIndex made up from smaller GraphIndices."""

    iter_all_entries = _wrap_iter(_RustCombinedGraphIndex.iter_all_entries)
    iter_entries = _wrap_iter(_RustCombinedGraphIndex.iter_entries)
    iter_entries_prefix = _wrap_iter(_RustCombinedGraphIndex.iter_entries_prefix)


class GraphIndexPrefixAdapter(_RustGraphIndexPrefixAdapter):
    """Adapter that prefixes/strips a key prefix on every call."""

    iter_all_entries = _wrap_iter(_RustGraphIndexPrefixAdapter.iter_all_entries)
    iter_entries = _wrap_iter(_RustGraphIndexPrefixAdapter.iter_entries)
    iter_entries_prefix = _wrap_iter(_RustGraphIndexPrefixAdapter.iter_entries_prefix)

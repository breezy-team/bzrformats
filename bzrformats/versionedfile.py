# Copyright (C) 2006-2011 Canonical Ltd
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

"""Versioned text file storage api."""

from typing import Any

from ._bzr_rs import textmerge as _textmerge_rs
from ._bzr_rs import versionedfile as _versionedfile_rs
from .errors import (
    ExistingContent,  # noqa: F401  re-exported for callers (e.g. breezy)
    UnavailableRepresentation,  # noqa: F401  re-exported for callers
)
from .registry import Registry

FulltextContentFactory = _versionedfile_rs.FulltextContentFactory
ChunkedContentFactory = _versionedfile_rs.ChunkedContentFactory
AbsentContentFactory = _versionedfile_rs.AbsentContentFactory
record_to_fulltext_bytes = _versionedfile_rs.record_to_fulltext_bytes
fulltext_network_to_record = _versionedfile_rs.fulltext_network_to_record


adapter_registry = Registry[tuple[str, str], Any, None]()
adapter_registry.register_lazy(
    ("knit-annotated-delta-gz", "knit-delta-gz"),
    "bzrformats.knit",
    "DeltaAnnotatedToUnannotated",
)
adapter_registry.register_lazy(
    ("knit-annotated-ft-gz", "knit-ft-gz"),
    "bzrformats.knit",
    "FTAnnotatedToUnannotated",
)
for target_storage_kind in ("fulltext", "chunked", "lines"):
    adapter_registry.register_lazy(
        ("knit-delta-gz", target_storage_kind),
        "bzrformats.knit",
        "DeltaPlainToFullText",
    )
    adapter_registry.register_lazy(
        ("knit-ft-gz", target_storage_kind), "bzrformats.knit", "FTPlainToFullText"
    )
    adapter_registry.register_lazy(
        ("knit-annotated-ft-gz", target_storage_kind),
        "bzrformats.knit",
        "FTAnnotatedToFullText",
    )
    adapter_registry.register_lazy(
        ("knit-annotated-delta-gz", target_storage_kind),
        "bzrformats.knit",
        "DeltaAnnotatedToFullText",
    )


class ContentFactory:
    """Abstract interface for insertion and retrieval from a VersionedFile.

    :ivar sha1: None, or the sha1 of the content fulltext.
    :ivar size: None, or the size of the content fulltext.
    :ivar storage_kind: The native storage kind of this factory. One of
        'mpdiff', 'knit-annotated-ft', 'knit-annotated-delta', 'knit-ft',
        'knit-delta', 'fulltext', 'knit-annotated-ft-gz',
        'knit-annotated-delta-gz', 'knit-ft-gz', 'knit-delta-gz'.
    :ivar key: The key of this content. Each key is a tuple with a single
        string in it.
    :ivar parents: A tuple of parent keys for self.key. If the object has
        no parent information, None (as opposed to () for an empty list of
        parents).
    """

    def __init__(self) -> None:
        """Create a ContentFactory."""
        self.sha1: bytes | None = None
        self.size: int | None = None
        self.storage_kind: str | None = None
        self.key: tuple[bytes, ...] | None = None
        self.parents = None

    def map_key(self, cb):
        """Add prefix to all keys."""
        if self.key is not None:
            self.key = cb(self.key)
        if self.parents is not None:
            self.parents = tuple([cb(parent) for parent in self.parents])
        return self


FileContentFactory = _versionedfile_rs.FileContentFactory
"""See ContentFactory. File-backed content factory.

`__init__(key, parents, fileobj, sha1=None, size=None)`: reads bytes from
the supplied Python file-like on first ``get_bytes_as`` / ``iter_bytes_as``
call and caches the result. ``storage_kind`` is ``"file"``.
"""


AdapterFactory = _versionedfile_rs.AdapterFactory
"""See ContentFactory. Overrides ``key`` / ``parents`` while delegating
``storage_kind`` / ``sha1`` / ``size`` / ``get_bytes_as`` to the wrapped
factory passed as ``adapted``.
"""


def filter_absent(record_stream):
    """Adapt a record stream to remove absent records."""
    for record in record_stream:
        if record.storage_kind != "absent":
            yield record


# _MPDiffGenerator is implemented as a subclassable Rust pyclass: compute_diffs
# drives the pure-Rust make_mpdiffs fast path, while _find_needed_keys /
# _process_one_record / _compute_diff and the intermediate state remain
# available for callers (e.g. breezy's _MPDiffInventoryGenerator subclass).
_MPDiffGenerator = _versionedfile_rs._MPDiffGenerator


# VersionedFile is an abstract base implemented as a Rust pyclass; the
# concrete `Weave` extends it (in Rust), and breezy subclasses it in Python.
VersionedFile = _versionedfile_rs.VersionedFile


# RecordingVersionedFilesDecorator and OrderingVersionedFilesDecorator are
# implemented as Rust pyclasses (test support: they record calls made on a
# backing vf; the Ordering variant also returns keys in a defined priority
# order for 'unordered' get_record_stream requests).
RecordingVersionedFilesDecorator = _versionedfile_rs.RecordingVersionedFilesDecorator
OrderingVersionedFilesDecorator = _versionedfile_rs.OrderingVersionedFilesDecorator


class KeyMapper:
    """Abstract KeyMapper kept as a Python type for ``isinstance`` checks.

    The concrete mappers (``ConstantMapper``, ``PrefixMapper``,
    ``HashPrefixMapper``, ``HashEscapedPrefixMapper``) are pyclasses
    backed by ``crates/bazaar/src/key_mapper.rs``.
    """

    def map(self, key):
        """Map key to an underlying storage identifier.

        :param key: A key tuple e.g. (b'file-id', b'revision-id').
        :return: An underlying storage identifier, specific to the partitioning
            mechanism.
        """
        raise NotImplementedError(self.map)

    def unmap(self, partition_id):
        """Map a partitioned storage id back to a key prefix.

        :param partition_id: The underlying partition id.
        :return: As much of a key (or prefix) as is derivable from the partition
            id.
        """
        raise NotImplementedError(self.unmap)


ConstantMapper = _versionedfile_rs.ConstantMapper
PrefixMapper = _versionedfile_rs.PrefixMapper
HashPrefixMapper = _versionedfile_rs.HashPrefixMapper
HashEscapedPrefixMapper = _versionedfile_rs.HashEscapedPrefixMapper


def make_versioned_files_factory(versioned_file_factory, mapper):
    """Create a ThunkedVersionedFiles factory.

    This will create a callable which when called creates a
    ThunkedVersionedFiles on a transport, using mapper to access individual
    versioned files, and versioned_file_factory to create each individual file.
    """

    def factory(transport):
        return ThunkedVersionedFiles(
            transport, versioned_file_factory, mapper, lambda: True
        )

    return factory


# VersionedFiles is an abstract base implemented as a Rust pyclass. The
# concrete backends (knit, groupcompress) extend it via
# VersionedFilesWithFallbacks (also a Rust pyclass); the thunk/merge helpers
# below subclass it in Python.
VersionedFiles = _versionedfile_rs.VersionedFiles


# ThunkedVersionedFiles is implemented as a Rust pyclass extending
# VersionedFiles; it thunks a single (prefix, suffix) keyspace onto per-prefix
# old-style VersionedFile objects (used by breezy's weave_fmt plugin).
ThunkedVersionedFiles = _versionedfile_rs.ThunkedVersionedFiles


# VersionedFilesWithFallbacks is a Rust pyclass extending VersionedFiles.
VersionedFilesWithFallbacks = _versionedfile_rs.VersionedFilesWithFallbacks


# _PlanMergeVersionedFile is implemented as a Rust pyclass extending
# VersionedFiles; it holds uncommitted+committed texts to let merges be planned
# against working-tree texts, falling back to other VersionedFiles for missing
# texts. Re-exported here for callers (and breezy).
_PlanMergeVersionedFile = _versionedfile_rs._PlanMergeVersionedFile


# PlanWeaveMerge and WeaveMerge are TextMerge subclasses implemented as Rust
# pyclasses; re-exported here so callers (and breezy) keep importing them
# from bzrformats.versionedfile.
PlanWeaveMerge = _textmerge_rs.PlanWeaveMerge
WeaveMerge = _textmerge_rs.WeaveMerge


VirtualVersionedFiles = _versionedfile_rs.VirtualVersionedFiles
"""See VersionedFiles. Storage-less implementation backed by two callbacks.

`__init__(get_parent_map, get_lines)`: caller-supplied callables operating
on bare bytes keys. Backed by the Rust pyclass; the Python wrapper used
to live here and applied the same `(k,) <-> k` rewrapping the Rust
pyclass now does internally.
"""


# NoDupeAddLinesDecorator, NetworkRecordStream and sort_groupcompress are
# implemented in the Rust extension. NetworkRecordStream dispatches to the
# per-kind network factories by importing them at read() time.
NoDupeAddLinesDecorator = _versionedfile_rs.NoDupeAddLinesDecorator

network_bytes_to_kind_and_offset = _versionedfile_rs.network_bytes_to_kind_and_offset

NetworkRecordStream = _versionedfile_rs.NetworkRecordStream

sort_groupcompress = _versionedfile_rs.sort_groupcompress


_KeyRefs = _versionedfile_rs.KeyRefs

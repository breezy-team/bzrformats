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


ContentFactory = _versionedfile_rs.ContentFactory


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


_MPDiffGenerator = _versionedfile_rs._MPDiffGenerator
VersionedFile = _versionedfile_rs.VersionedFile
RecordingVersionedFilesDecorator = _versionedfile_rs.RecordingVersionedFilesDecorator
OrderingVersionedFilesDecorator = _versionedfile_rs.OrderingVersionedFilesDecorator
KeyMapper = _versionedfile_rs.KeyMapper
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


VersionedFiles = _versionedfile_rs.VersionedFiles
ThunkedVersionedFiles = _versionedfile_rs.ThunkedVersionedFiles
VersionedFilesWithFallbacks = _versionedfile_rs.VersionedFilesWithFallbacks
_PlanMergeVersionedFile = _versionedfile_rs._PlanMergeVersionedFile
PlanWeaveMerge = _textmerge_rs.PlanWeaveMerge
WeaveMerge = _textmerge_rs.WeaveMerge


VirtualVersionedFiles = _versionedfile_rs.VirtualVersionedFiles
"""See VersionedFiles. Storage-less implementation backed by two callbacks.

`__init__(get_parent_map, get_lines)`: caller-supplied callables operating
on bare bytes keys, which the pyclass rewraps as `(k,)` internally.
"""


NoDupeAddLinesDecorator = _versionedfile_rs.NoDupeAddLinesDecorator

network_bytes_to_kind_and_offset = _versionedfile_rs.network_bytes_to_kind_and_offset

NetworkRecordStream = _versionedfile_rs.NetworkRecordStream

sort_groupcompress = _versionedfile_rs.sort_groupcompress


_KeyRefs = _versionedfile_rs.KeyRefs

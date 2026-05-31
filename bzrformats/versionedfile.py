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

from copy import copy
from typing import Any

from . import multiparent, osutils
from ._bzr_rs import textmerge as _textmerge_rs
from ._bzr_rs import versionedfile as _versionedfile_rs
from .errors import (
    ExistingContent,  # noqa: F401  re-exported for callers (e.g. breezy)
    RevisionNotPresent,
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


class _MPDiffGenerator:
    """Pull out the functionality for generating mp_diffs.

    `compute_diffs` drives a pure-Rust fast path. The other methods exist
    for callers that need step-by-step access to the intermediate state
    (parent map, refcounts, ghost parents, chunk cache) - chiefly breezy's
    whitebox tests.
    """

    def __init__(self, vf, keys):
        self.vf = vf
        self.ordered_keys = tuple(keys)
        self.needed_keys = ()
        self.diffs = {}
        self.parent_map = {}
        self.ghost_parents = ()
        self.refcounts = {}
        self.chunks = {}

    def _find_needed_keys(self):
        """Find the keys we need to request from the underlying vf.

        Returns ``(needed_keys, refcounts)``. ``needed_keys`` is the set of
        all texts we need to extract; ``refcounts`` is a dict
        ``{key: num_children}`` so callers know when a cached parent text
        can be released.
        """
        parent_map = self.vf.get_parent_map(set(self.ordered_keys))
        self.parent_map = parent_map
        needed_keys, refcounts, just_parents, missing_keys = (
            _versionedfile_rs.mpdiff_first_pass(self.ordered_keys, parent_map)
        )
        if missing_keys:
            raise RevisionNotPresent(next(iter(missing_keys)), self.vf)
        self.present_parents = set(self.vf.get_parent_map(just_parents))
        self.ghost_parents = just_parents.difference(self.present_parents)
        needed_keys.difference_update(self.ghost_parents)
        self.needed_keys = needed_keys
        self.refcounts = refcounts
        return needed_keys, refcounts

    def _compute_diff(self, key, parent_lines, lines):
        diff = multiparent.MultiParent.from_lines(lines, parent_lines, None)
        self.diffs[key] = diff

    def _process_one_record(self, key, this_chunks):
        if key in self.parent_map:
            parent_keys = self.parent_map.pop(key)
            if parent_keys is None:
                parent_keys = ()
            parent_chunks_list = _versionedfile_rs.mpdiff_collect_parent_chunks(
                parent_keys, self.ghost_parents, self.refcounts, self.chunks
            )
            parent_lines = [osutils.chunks_to_lines(pc) for pc in parent_chunks_list]
            lines = osutils.chunks_to_lines(this_chunks)
            this_chunks = lines
            self._compute_diff(key, parent_lines, lines)
            del lines
        if key in self.refcounts:
            self.chunks[key] = this_chunks

    def compute_diffs(self):
        """Return one `MultiParent` per ordered key, in input order."""
        return list(_versionedfile_rs.make_mpdiffs(self.vf, self.ordered_keys))


# VersionedFile is an abstract base implemented as a Rust pyclass; the
# concrete `Weave` extends it (in Rust), and breezy subclasses it in Python.
VersionedFile = _versionedfile_rs.VersionedFile


class RecordingVersionedFilesDecorator:
    """A minimal versioned files that records calls made on it.

    Only enough methods have been added to support tests using it to date.

    :ivar calls: A list of the calls made; can be reset at any time by
        assigning [] to it.
    """

    def __init__(self, backing_vf):
        """Create a RecordingVersionedFilesDecorator decorating backing_vf.

        :param backing_vf: The versioned file to answer all methods.
        """
        self._backing_vf = backing_vf
        self.calls = []

    def add_lines(
        self,
        key,
        parents,
        lines,
        parent_texts=None,
        left_matching_blocks=None,
        nostore_sha=None,
        random_id=False,
        check_content=True,
    ):
        """Add lines to the versioned file and record the call.

        Args:
            key: The key for the new version.
            parents: Parent keys for the new version.
            lines: The text lines to add.
            parent_texts: Parent text data (optional).
            left_matching_blocks: Matching blocks for delta compression (optional).
            nostore_sha: SHA to skip storing if duplicate (optional).
            random_id: Whether to use a random ID (optional).
            check_content: Whether to validate content (optional).

        Returns:
            The result from the backing versioned file.
        """
        self.calls.append(
            (
                "add_lines",
                key,
                parents,
                lines,
                parent_texts,
                left_matching_blocks,
                nostore_sha,
                random_id,
                check_content,
            )
        )
        return self._backing_vf.add_lines(
            key,
            parents,
            lines,
            parent_texts,
            left_matching_blocks,
            nostore_sha,
            random_id,
            check_content,
        )

    def add_content(
        self,
        factory,
        parent_texts=None,
        left_matching_blocks=None,
        nostore_sha=None,
        random_id=False,
        check_content=True,
    ):
        """Add content from a factory and record the call.

        Args:
            factory: ContentFactory providing the content.
            parent_texts: Parent text data (optional).
            left_matching_blocks: Matching blocks for delta compression (optional).
            nostore_sha: SHA to skip storing if duplicate (optional).
            random_id: Whether to use a random ID (optional).
            check_content: Whether to validate content (optional).

        Returns:
            The result from the backing versioned file.
        """
        self.calls.append(
            (
                "add_content",
                factory,
                parent_texts,
                left_matching_blocks,
                nostore_sha,
                random_id,
                check_content,
            )
        )
        return self._backing_vf.add_content(
            factory,
            parent_texts,
            left_matching_blocks,
            nostore_sha,
            random_id,
            check_content,
        )

    def check(self):
        """Check the backing versioned file for consistency."""
        self._backing_vf.check()

    def get_parent_map(self, keys):
        """Get parent mapping for keys and record the call.

        Args:
            keys: Keys to get parent mapping for.

        Returns:
            dict: Mapping of keys to their parents.
        """
        self.calls.append(("get_parent_map", copy(keys)))
        return self._backing_vf.get_parent_map(keys)

    def get_record_stream(self, keys, sort_order, include_delta_closure):
        """Get a stream of records and record the call.

        Args:
            keys: Keys to get records for.
            sort_order: How to sort the results.
            include_delta_closure: Whether to include delta closure.

        Returns:
            Iterator over record data.
        """
        self.calls.append(
            ("get_record_stream", list(keys), sort_order, include_delta_closure)
        )
        return self._backing_vf.get_record_stream(
            keys, sort_order, include_delta_closure
        )

    def get_sha1s(self, keys):
        """Get SHA1 hashes for keys and record the call.

        Args:
            keys: Keys to get SHA1s for.

        Returns:
            dict: Mapping of keys to their SHA1 hashes.
        """
        self.calls.append(("get_sha1s", copy(keys)))
        return self._backing_vf.get_sha1s(keys)

    def iter_lines_added_or_present_in_keys(self, keys, pb=None):
        """Iterate over lines added or present in keys and record the call.

        Args:
            keys: Keys to iterate over.
            pb: Optional progress bar.

        Returns:
            Iterator over lines.
        """
        self.calls.append(("iter_lines_added_or_present_in_keys", copy(keys)))
        return self._backing_vf.iter_lines_added_or_present_in_keys(keys, pb=pb)

    def keys(self):
        """Get all keys and record the call.

        Returns:
            Iterable of all keys in the versioned file.
        """
        self.calls.append(("keys",))
        return self._backing_vf.keys()


class OrderingVersionedFilesDecorator(RecordingVersionedFilesDecorator):
    """A VF that records calls, and returns keys in specific order.

    :ivar calls: A list of the calls made; can be reset at any time by
        assigning [] to it.
    """

    def __init__(self, backing_vf, key_priority):
        """Create a RecordingVersionedFilesDecorator decorating backing_vf.

        :param backing_vf: The versioned file to answer all methods.
        :param key_priority: A dictionary defining what order keys should be
            returned from an 'unordered' get_record_stream request.
            Keys with lower priority are returned first, keys not present in
            the map get an implicit priority of 0, and are returned in
            lexicographical order.
        """
        RecordingVersionedFilesDecorator.__init__(self, backing_vf)
        self._key_priority = key_priority

    def get_record_stream(self, keys, sort_order, include_delta_closure):
        """Get a stream of records with custom ordering and record the call.

        Args:
            keys: Keys to get records for.
            sort_order: How to sort the results ('unordered' uses key_priority).
            include_delta_closure: Whether to include delta closure.

        Yields:
            Record data in the specified order.
        """
        self.calls.append(
            ("get_record_stream", list(keys), sort_order, include_delta_closure)
        )
        if sort_order == "unordered":

            def sort_key(key):
                return (self._key_priority.get(key, 0), key)

            # Use a defined order by asking for the keys one-by-one from the
            # backing_vf
            for key in sorted(keys, key=sort_key):
                yield from self._backing_vf.get_record_stream(
                    [key], "unordered", include_delta_closure
                )
        else:
            yield from self._backing_vf.get_record_stream(
                keys, sort_order, include_delta_closure
            )


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

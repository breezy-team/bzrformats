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

import functools
import os
from copy import copy
from io import BytesIO
from typing import Any

from vcsgraph import graph as _mod_graph
from vcsgraph import known_graph as _mod_known_graph

from . import index, multiparent, osutils, revision
from ._bzr_rs import versionedfile as _versionedfile_rs
from .errors import (
    BzrFormatsError,
    ObjectNotLocked,
    RevisionNotPresent,
    VersionedFileInvalidChecksum,
)

from .registry import Registry
from .textmerge import TextMerge
from .transport import TransportNoSuchFile

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


class UnavailableRepresentation(BzrFormatsError):
    """Raised when a requested content encoding is not available.

    This error occurs when trying to access content in a specific encoding
    that is not supported or available for the given key.
    """

    _fmt = (
        "The encoding '%(wanted)s' is not available for key %(key)s which "
        "is encoded as '%(native)s'."
    )

    def __init__(self, key, wanted, native):
        """Initialize an UnavailableRepresentation error.

        Args:
            key: The content key that was requested.
            wanted: The encoding that was requested.
            native: The encoding that is actually available.
        """
        super().__init__()
        self.wanted = wanted
        self.native = native
        self.key = key


class ExistingContent(BzrFormatsError):
    """Raised when attempting to insert content that already exists.

    This error occurs when trying to add content to a versioned file
    that has already been stored.
    """

    _fmt = "The content being inserted is already present."


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


class VersionedFile:
    """Versioned text file storage.

    A versioned file manages versions of line-based text files,
    keeping track of the originating version for each line.

    To clients the "lines" of the file are represented as a list of
    strings. These strings will typically have terminal newline
    characters, but this is not required.  In particular files commonly
    do not have a newline at the end of the file.

    Texts are identified by a version-id string.
    """

    @staticmethod
    def check_not_reserved_id(version_id):
        """Check that a version ID is not a reserved identifier.

        Args:
            version_id: The version ID to check, or None.

        Raises:
            ValueError: If version_id is a reserved identifier.
        """
        if version_id is not None:
            revision.check_not_reserved_id(version_id)

    def copy_to(self, name, transport):
        """Copy this versioned file to name on transport."""
        raise NotImplementedError(self.copy_to)

    def get_record_stream(self, versions, ordering, include_delta_closure):
        """Get a stream of records for versions.

        :param versions: The versions to include. Each version is a tuple
            (version,).
        :param ordering: Either 'unordered' or 'topological'. A topologically
            sorted stream has compression parents strictly before their
            children.
        :param include_delta_closure: If True then the closure across any
            compression parents will be included (in the data content of the
            stream, not in the emitted records). This guarantees that
            'fulltext' can be used successfully on every record.
        :return: An iterator of ContentFactory objects, each of which is only
            valid until the iterator is advanced.
        """
        raise NotImplementedError(self.get_record_stream)

    def has_version(self, version_id):
        """Returns whether version is present."""
        raise NotImplementedError(self.has_version)

    def insert_record_stream(self, stream):
        """Insert a record stream into this versioned file.

        :param stream: A stream of records to insert.
        :return: None
        :seealso VersionedFile.get_record_stream:
        """
        raise NotImplementedError

    def add_lines(
        self,
        version_id,
        parents,
        lines,
        parent_texts=None,
        left_matching_blocks=None,
        nostore_sha=None,
        random_id=False,
        check_content=True,
    ):
        r"""Add a single text on top of the versioned file.

        Must raise RevisionAlreadyPresent if the new version is
        already present in file history.

        Must raise RevisionNotPresent if any of the given parents are
        not present in file history.

        :param lines: A list of lines. Each line must be a bytestring. And all
            of them except the last must be terminated with \n and contain no
            other \n's. The last line may either contain no \n's or a single
            terminated \n. If the lines list does meet this constraint the add
            routine may error or may succeed - but you will be unable to read
            the data back accurately. (Checking the lines have been split
            correctly is expensive and extremely unlikely to catch bugs so it
            is not done at runtime unless check_content is True.)
        :param parent_texts: An optional dictionary containing the opaque
            representations of some or all of the parents of version_id to
            allow delta optimisations.  VERY IMPORTANT: the texts must be those
            returned by add_lines or data corruption can be caused.
        :param left_matching_blocks: a hint about which areas are common
            between the text and its left-hand-parent.  The format is
            the SequenceMatcher.get_matching_blocks format.
        :param nostore_sha: Raise ExistingContent and do not add the lines to
            the versioned file if the digest of the lines matches this.
        :param random_id: If True a random id has been selected rather than
            an id determined by some deterministic process such as a converter
            from a foreign VCS. When True the backend may choose not to check
            for uniqueness of the resulting key within the versioned file, so
            this should only be done when the result is expected to be unique
            anyway.
        :param check_content: If True, the lines supplied are verified to be
            bytestrings that are correctly formed lines.
        :return: The text sha1, the number of bytes in the text, and an opaque
                 representation of the inserted version which can be provided
                 back to future add_lines calls in the parent_texts dictionary.
        """
        self._check_write_ok()
        return self._add_lines(
            version_id,
            parents,
            lines,
            parent_texts,
            left_matching_blocks,
            nostore_sha,
            random_id,
            check_content,
        )

    def _add_lines(
        self,
        version_id,
        parents,
        lines,
        parent_texts,
        left_matching_blocks,
        nostore_sha,
        random_id,
        check_content,
    ):
        """Helper to do the class specific add_lines."""
        raise NotImplementedError(self.add_lines)

    def add_lines_with_ghosts(
        self,
        version_id,
        parents,
        lines,
        parent_texts=None,
        nostore_sha=None,
        random_id=False,
        check_content=True,
        left_matching_blocks=None,
    ):
        """Add lines to the versioned file, allowing ghosts to be present.

        This takes the same parameters as add_lines and returns the same.
        """
        self._check_write_ok()
        return self._add_lines_with_ghosts(
            version_id,
            parents,
            lines,
            parent_texts,
            nostore_sha,
            random_id,
            check_content,
            left_matching_blocks,
        )

    def _add_lines_with_ghosts(
        self,
        version_id,
        parents,
        lines,
        parent_texts,
        nostore_sha,
        random_id,
        check_content,
        left_matching_blocks,
    ):
        """Helper to do class specific add_lines_with_ghosts."""
        raise NotImplementedError(self.add_lines_with_ghosts)

    def check(self, progress_bar=None):
        """Check the versioned file for integrity."""
        raise NotImplementedError(self.check)

    def _check_lines_not_unicode(self, lines):
        """Check that lines being added to a versioned file are not unicode."""
        _versionedfile_rs.check_lines_not_unicode(lines)

    def _check_lines_are_lines(self, lines):
        """Check that the lines really are full lines without inline EOL."""
        _versionedfile_rs.check_lines_are_lines(lines)

    def get_format_signature(self):
        """Get a text description of the data encoding in this file.

        :since: 0.90
        """
        raise NotImplementedError(self.get_format_signature)

    def make_mpdiffs(self, version_ids):
        """Create multiparent diffs for specified versions.

        Drives the parent-map / ghost-filter / bulk-fetch / per-record
        ``MultiParent.from_lines`` loop in Rust; the only Python callbacks
        it invokes are ``self.get_parent_map`` (twice) and
        ``self._get_lf_split_line_list`` (once, in bulk).
        """
        return list(_versionedfile_rs.make_mpdiffs_singular(self, list(version_ids)))

    def add_mpdiffs(self, records):
        """Add mpdiffs to this VersionedFile.

        Records should be iterables of version, parents, expected_sha1,
        mpdiff. mpdiff should be a MultiParent instance.
        """
        # Drives the build-mpvf / fetch-parents / reconstruct / add_lines
        # loop in Rust; the only Python callbacks it invokes are
        # self.get_parent_map, self._get_lf_split_line_list,
        # self.add_lines_with_ghosts (with fallback to self.add_lines), and
        # self.get_sha1s for the post-hoc checksum verification.
        _versionedfile_rs.add_mpdiffs_singular(self, list(records))

    def get_text(self, version_id):
        """Return version contents as a text string.

        Raises RevisionNotPresent if version is not present in
        file history.
        """
        return b"".join(self.get_lines(version_id))

    get_string = get_text

    def get_texts(self, version_ids):
        """Return the texts of listed versions as a list of strings.

        Raises RevisionNotPresent if version is not present in
        file history.
        """
        return [b"".join(self.get_lines(v)) for v in version_ids]

    def get_lines(self, version_id):
        """Return version contents as a sequence of lines.

        Raises RevisionNotPresent if version is not present in
        file history.
        """
        raise NotImplementedError(self.get_lines)

    def _get_lf_split_line_list(self, version_ids):
        return [BytesIO(t).readlines() for t in self.get_texts(version_ids)]

    def get_ancestry(self, version_ids):
        """Return a list of all ancestors of given version(s). This
        will not include the null revision.

        Must raise RevisionNotPresent if any of the given versions are
        not present in file history.
        """
        raise NotImplementedError(self.get_ancestry)

    def get_ancestry_with_ghosts(self, version_ids):
        """Return a list of all ancestors of given version(s). This
        will not include the null revision.

        Must raise RevisionNotPresent if any of the given versions are
        not present in file history.

        Ghosts that are known about will be included in ancestry list,
        but are not explicitly marked.
        """
        raise NotImplementedError(self.get_ancestry_with_ghosts)

    def get_parent_map(self, version_ids):
        """Get a map of the parents of version_ids.

        :param version_ids: The version ids to look up parents for.
        :return: A mapping from version id to parents.
        """
        raise NotImplementedError(self.get_parent_map)

    def get_parents_with_ghosts(self, version_id):
        """Return version names for parents of version_id.

        Will raise RevisionNotPresent if version_id is not present
        in the history.

        Ghosts that are known about will be included in the parent list,
        but are not explicitly marked.
        """
        try:
            return list(self.get_parent_map([version_id])[version_id])
        except KeyError as e:
            raise RevisionNotPresent(version_id, self) from e

    def annotate(self, version_id):
        """Return a list of (version-id, line) tuples for version_id.

        :raise RevisionNotPresent: If the given version is
        not present in file history.
        """
        raise NotImplementedError(self.annotate)

    def iter_lines_added_or_present_in_versions(self, version_ids=None, pb=None):
        r"""Iterate over the lines in the versioned file from version_ids.

        This may return lines from other versions. Each item the returned
        iterator yields is a tuple of a line and a text version that that line
        is present in (not introduced in).

        Ordering of results is in whatever order is most suitable for the
        underlying storage format.

        If a progress bar is supplied, it may be used to indicate progress.
        The caller is responsible for cleaning up progress bars (because this
        is an iterator).

        NOTES: Lines are normalised: they will all have \n terminators.
               Lines are returned in arbitrary order.

        :return: An iterator over (line, version_id).
        """
        raise NotImplementedError(self.iter_lines_added_or_present_in_versions)

    def plan_merge(self, ver_a, ver_b, base=None):
        """Return pseudo-annotation indicating how the two versions merge.

        This is computed between versions a and b and their common
        base.

        Weave lines present in none of them are skipped entirely.

        Legend:
        killed-base Dead in base revision
        killed-both Killed in each revision
        killed-a    Killed in a
        killed-b    Killed in b
        unchanged   Alive in both a and b (possibly created in both)
        new-a       Created in a
        new-b       Created in b
        ghost-a     Killed in a, unborn in b
        ghost-b     Killed in b, unborn in a
        irrelevant  Not in either revision
        """
        raise NotImplementedError(VersionedFile.plan_merge)

    def weave_merge(
        self, plan, a_marker=TextMerge.A_MARKER, b_marker=TextMerge.B_MARKER
    ):
        """Merge text using a weave merge algorithm.

        Args:
            plan: The merge plan to execute.
            a_marker: Marker for 'A' side conflicts (optional).
            b_marker: Marker for 'B' side conflicts (optional).

        Returns:
            list: Merged lines of text.
        """
        return PlanWeaveMerge(plan, a_marker, b_marker).merge_lines()[0]


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


class VersionedFiles:
    """Storage for many versioned files.

    This object allows a single keyspace for accessing the history graph and
    contents of named bytestrings.

    Currently no implementation allows the graph of different key prefixes to
    intersect, but the API does allow such implementations in the future.

    The keyspace is expressed via simple tuples. Any instance of VersionedFiles
    may have a different length key-size, but that size will be constant for
    all texts added to or retrieved from it. For instance, bazaar uses
    instances with a key-size of 2 for storing user files in a repository, with
    the first element the fileid, and the second the version of that file.

    The use of tuples allows a single code base to support several different
    uses with only the mapping logic changing from instance to instance.

    :ivar _immediate_fallback_vfs: For subclasses that support stacking,
        this is a list of other VersionedFiles immediately underneath this
        one.  They may in turn each have further fallbacks.
    """

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
        r"""Add a text to the store.

        :param key: The key tuple of the text to add. If the last element is
            None, a CHK string will be generated during the addition.
        :param parents: The parents key tuples of the text to add.
        :param lines: A list of lines. Each line must be a bytestring. And all
            of them except the last must be terminated with \n and contain no
            other \n's. The last line may either contain no \n's or a single
            terminating \n. If the lines list does meet this constraint the add
            routine may error or may succeed - but you will be unable to read
            the data back accurately. (Checking the lines have been split
            correctly is expensive and extremely unlikely to catch bugs so it
            is not done at runtime unless check_content is True.)
        :param parent_texts: An optional dictionary containing the opaque
            representations of some or all of the parents of version_id to
            allow delta optimisations.  VERY IMPORTANT: the texts must be those
            returned by add_lines or data corruption can be caused.
        :param left_matching_blocks: a hint about which areas are common
            between the text and its left-hand-parent.  The format is
            the SequenceMatcher.get_matching_blocks format.
        :param nostore_sha: Raise ExistingContent and do not add the lines to
            the versioned file if the digest of the lines matches this.
        :param random_id: If True a random id has been selected rather than
            an id determined by some deterministic process such as a converter
            from a foreign VCS. When True the backend may choose not to check
            for uniqueness of the resulting key within the versioned file, so
            this should only be done when the result is expected to be unique
            anyway.
        :param check_content: If True, the lines supplied are verified to be
            bytestrings that are correctly formed lines.
        :return: The text sha1, the number of bytes in the text, and an opaque
                 representation of the inserted version which can be provided
                 back to future add_lines calls in the parent_texts dictionary.
        """
        raise NotImplementedError(self.add_lines)

    def add_content(
        self,
        factory,
        parent_texts=None,
        left_matching_blocks=None,
        nostore_sha=None,
        random_id=False,
        check_content=True,
    ):
        """Add a text to the store from a chunk iterable.

        :param key: The key tuple of the text to add. If the last element is
            None, a CHK string will be generated during the addition.
        :param parents: The parents key tuples of the text to add.
        :param chunk_iter: An iterable over bytestrings.
        :param parent_texts: An optional dictionary containing the opaque
            representations of some or all of the parents of version_id to
            allow delta optimisations.  VERY IMPORTANT: the texts must be those
            returned by add_lines or data corruption can be caused.
        :param left_matching_blocks: a hint about which areas are common
            between the text and its left-hand-parent.  The format is
            the SequenceMatcher.get_matching_blocks format.
        :param nostore_sha: Raise ExistingContent and do not add the lines to
            the versioned file if the digest of the lines matches this.
        :param random_id: If True a random id has been selected rather than
            an id determined by some deterministic process such as a converter
            from a foreign VCS. When True the backend may choose not to check
            for uniqueness of the resulting key within the versioned file, so
            this should only be done when the result is expected to be unique
            anyway.
        :param check_content: If True, the lines supplied are verified to be
            bytestrings that are correctly formed lines.
        :return: The text sha1, the number of bytes in the text, and an opaque
                 representation of the inserted version which can be provided
                 back to future add_lines calls in the parent_texts dictionary.
        """
        raise NotImplementedError(self.add_content)

    def add_mpdiffs(self, records):
        """Add mpdiffs to this VersionedFile.

        Records should be iterables of version, parents, expected_sha1,
        mpdiff. mpdiff should be a MultiParent instance.
        """
        # Drives the build-mpvf / fetch-parents / reconstruct / add_lines
        # loop in Rust; the only Python callbacks it invokes are
        # self.get_record_stream and self.add_lines.
        _versionedfile_rs.add_mpdiffs(self, records)

    def annotate(self, key):
        """Return a list of (version-key, line) tuples for the text of key.

        :raise RevisionNotPresent: If the key is not present.
        """
        raise NotImplementedError(self.annotate)

    def check(self, progress_bar=None):
        """Check this object for integrity.

        :param progress_bar: A progress bar to output as the check progresses.
        :param keys: Specific keys within the VersionedFiles to check. When
            this parameter is not None, check() becomes a generator as per
            get_record_stream. The difference to get_record_stream is that
            more or deeper checks will be performed.
        :return: None, or if keys was supplied a generator as per
            get_record_stream.
        """
        raise NotImplementedError(self.check)

    @staticmethod
    def check_not_reserved_id(version_id):
        """Check that a version ID is not a reserved identifier.

        Args:
            version_id: The version ID to check, or None.

        Raises:
            ValueError: If version_id is a reserved identifier.
        """
        if version_id is not None:
            revision.check_not_reserved_id(version_id)

    def clear_cache(self):
        """Clear whatever caches this VersionedFile holds.

        This is generally called after an operation has been performed, when we
        don't expect to be using this versioned file again soon.
        """

    def _check_lines_not_unicode(self, lines):
        """Check that lines being added to a versioned file are not unicode."""
        _versionedfile_rs.check_lines_not_unicode(lines)

    def _check_lines_are_lines(self, lines):
        """Check that the lines really are full lines without inline EOL."""
        _versionedfile_rs.check_lines_are_lines(lines)

    def get_known_graph_ancestry(self, keys):
        """Get a KnownGraph instance with the ancestry of keys."""
        # The get_parent_map walk runs in Rust; it only needs this object's
        # get_parent_map, which it calls back into.
        parent_map = _versionedfile_rs.known_graph_ancestry_map(self, list(keys))
        return _mod_known_graph.KnownGraph(parent_map)

    def get_parent_map(self, keys):
        """Get a map of the parents of keys.

        :param keys: The keys to look up parents for.
        :return: A mapping from keys to parents. Absent keys are absent from
            the mapping.
        """
        raise NotImplementedError(self.get_parent_map)

    def get_record_stream(self, keys, ordering, include_delta_closure):
        """Get a stream of records for keys.

        :param keys: The keys to include.
        :param ordering: Either 'unordered' or 'topological'. A topologically
            sorted stream has compression parents strictly before their
            children.
        :param include_delta_closure: If True then the closure across any
            compression parents will be included (in the opaque data).
        :return: An iterator of ContentFactory objects, each of which is only
            valid until the iterator is advanced.
        """
        raise NotImplementedError(self.get_record_stream)

    def get_sha1s(self, keys):
        """Get the sha1's of the texts for the given keys.

        :param keys: The names of the keys to lookup
        :return: a dict from key to sha1 digest. Keys of texts which are not
            present in the store are not present in the returned
            dictionary.
        """
        raise NotImplementedError(self.get_sha1s)

    __contains__ = index._has_key_from_parent_map

    def get_missing_compression_parent_keys(self):
        """Return an iterable of keys of missing compression parents.

        Check this after calling insert_record_stream to find out if there are
        any missing compression parents.  If there are, the records that
        depend on them are not able to be inserted safely. The precise
        behaviour depends on the concrete VersionedFiles class in use.

        Classes that do not support this will raise NotImplementedError.
        """
        raise NotImplementedError(self.get_missing_compression_parent_keys)

    def insert_record_stream(self, stream):
        """Insert a record stream into this container.

        :param stream: A stream of records to insert.
        :return: None
        :seealso VersionedFile.get_record_stream:
        """
        raise NotImplementedError

    def iter_lines_added_or_present_in_keys(self, keys, pb=None):
        r"""Iterate over the lines in the versioned files from keys.

        This may return lines from other keys. Each item the returned
        iterator yields is a tuple of a line and a text version that that line
        is present in (not introduced in).

        Ordering of results is in whatever order is most suitable for the
        underlying storage format.

        If a progress bar is supplied, it may be used to indicate progress.
        The caller is responsible for cleaning up progress bars (because this
        is an iterator).

        Notes:
         * Lines are normalised by the underlying store: they will all have \n
           terminators.
         * Lines are returned in arbitrary order.

        :return: An iterator over (line, key).
        """
        raise NotImplementedError(self.iter_lines_added_or_present_in_keys)

    def keys(self):
        """Return a iterable of the keys for all the contained texts."""
        raise NotImplementedError(self.keys)

    def make_mpdiffs(self, keys):
        """Create multiparent diffs for specified keys."""
        generator = _MPDiffGenerator(self, keys)
        return generator.compute_diffs()

    def get_annotator(self):
        """Get an annotator for this versioned file.

        Returns:
            VersionedFileAnnotator: An annotator instance for this versioned file.
        """
        from .annotate import VersionedFileAnnotator

        return VersionedFileAnnotator(self)

    missing_keys = index._missing_keys_from_parent_map

    def _transitive_fallbacks(self):
        """Return the whole stack of fallback versionedfiles.

        This VersionedFiles may have a list of fallbacks, but it doesn't
        necessarily know about the whole stack going down, and it can't know
        at open time because they may change after the objects are opened.
        """
        all_fallbacks = []
        for a_vfs in self._immediate_fallback_vfs:
            all_fallbacks.append(a_vfs)
            all_fallbacks.extend(a_vfs._transitive_fallbacks())
        return all_fallbacks


class ThunkedVersionedFiles(VersionedFiles):
    """Storage for many versioned files thunked onto a 'VersionedFile' class.

    This object allows a single keyspace for accessing the history graph and
    contents of named bytestrings.

    Currently no implementation allows the graph of different key prefixes to
    intersect, but the API does allow such implementations in the future.
    """

    def __init__(self, transport, file_factory, mapper, is_locked):
        """Create a ThunkedVersionedFiles."""
        self._transport = transport
        self._file_factory = file_factory
        self._mapper = mapper
        self._is_locked = is_locked

    def add_content(
        self,
        factory,
        parent_texts=None,
        left_matching_blocks=None,
        nostore_sha=None,
        random_id=False,
    ):
        """See VersionedFiles.add_content()."""
        lines = factory.get_bytes_as("lines")
        return self.add_lines(
            factory.key,
            factory.parents,
            lines,
            parent_texts=parent_texts,
            left_matching_blocks=left_matching_blocks,
            nostore_sha=nostore_sha,
            random_id=random_id,
            check_content=True,
        )

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
        """See VersionedFiles.add_lines()."""
        path = self._mapper.map(key)
        version_id = key[-1]
        parents = [parent[-1] for parent in parents]
        vf = self._get_vf(path)
        try:
            try:
                return vf.add_lines_with_ghosts(
                    version_id,
                    parents,
                    lines,
                    parent_texts=parent_texts,
                    left_matching_blocks=left_matching_blocks,
                    nostore_sha=nostore_sha,
                    random_id=random_id,
                    check_content=check_content,
                )
            except NotImplementedError:
                return vf.add_lines(
                    version_id,
                    parents,
                    lines,
                    parent_texts=parent_texts,
                    left_matching_blocks=left_matching_blocks,
                    nostore_sha=nostore_sha,
                    random_id=random_id,
                    check_content=check_content,
                )
        except TransportNoSuchFile:
            # parent directory may be missing, try again.
            self._transport.mkdir(osutils.dirname(path))
            try:
                return vf.add_lines_with_ghosts(
                    version_id,
                    parents,
                    lines,
                    parent_texts=parent_texts,
                    left_matching_blocks=left_matching_blocks,
                    nostore_sha=nostore_sha,
                    random_id=random_id,
                    check_content=check_content,
                )
            except NotImplementedError:
                return vf.add_lines(
                    version_id,
                    parents,
                    lines,
                    parent_texts=parent_texts,
                    left_matching_blocks=left_matching_blocks,
                    nostore_sha=nostore_sha,
                    random_id=random_id,
                    check_content=check_content,
                )

    def annotate(self, key):
        """Return a list of (version-key, line) tuples for the text of key.

        :raise RevisionNotPresent: If the key is not present.
        """
        prefix = key[:-1]
        path = self._mapper.map(prefix)
        vf = self._get_vf(path)
        origins = vf.annotate(key[-1])
        result = []
        for origin, line in origins:
            result.append((prefix + (origin,), line))
        return result

    def check(self, progress_bar=None, keys=None):
        """See VersionedFiles.check()."""
        # XXX: This is over-enthusiastic but as we only thunk for Weaves today
        # this is tolerable. Ideally we'd pass keys down to check() and
        # have the older VersiondFile interface updated too.
        for _prefix, vf in self._iter_all_components():
            vf.check()
        if keys is not None:
            return self.get_record_stream(keys, "unordered", True)

    def get_parent_map(self, keys):
        """Get a map of the parents of keys.

        :param keys: The keys to look up parents for.
        :return: A mapping from keys to parents. Absent keys are absent from
            the mapping.
        """
        prefixes = self._partition_keys(keys)
        result = {}
        for prefix, suffixes in prefixes.items():
            path = self._mapper.map(prefix)
            vf = self._get_vf(path)
            parent_map = vf.get_parent_map(suffixes)
            for key, parents in parent_map.items():
                result[prefix + (key,)] = tuple(
                    prefix + (parent,) for parent in parents
                )
        return result

    def _get_vf(self, path):
        if not self._is_locked():
            raise ObjectNotLocked(self)
        return self._file_factory(
            path, self._transport, create=True, get_scope=lambda: None
        )

    def _partition_keys(self, keys):
        """Turn keys into a dict of prefix:suffix_list."""
        result = {}
        for key in keys:
            prefix_keys = result.setdefault(key[:-1], [])
            prefix_keys.append(key[-1])
        return result

    def _iter_all_prefixes(self):
        # Identify all key prefixes.
        # XXX: A bit hacky, needs polish.
        if isinstance(self._mapper, ConstantMapper):
            paths = [self._mapper.map(())]
            prefixes = [()]
        else:
            relpaths = set()
            for quoted_relpath in self._transport.iter_files_recursive():
                path, _ext = os.path.splitext(quoted_relpath)
                relpaths.add(path)
            paths = list(relpaths)
            prefixes = [self._mapper.unmap(path) for path in paths]
        return zip(paths, prefixes, strict=False)

    def get_record_stream(self, keys, ordering, include_delta_closure):
        """See VersionedFiles.get_record_stream()."""

        # Ordering will be taken care of by each partitioned store; group keys
        # by partition.
        def add_prefix(p, k):
            return p + k

        keys = sorted(keys)
        for prefix, suffixes, vf in self._iter_keys_vf(keys):
            suffixes = [(suffix,) for suffix in suffixes]
            for record in vf.get_record_stream(
                suffixes, ordering, include_delta_closure
            ):
                record.map_key(functools.partial(add_prefix, prefix))
                yield record

    def _iter_keys_vf(self, keys):
        prefixes = self._partition_keys(keys)
        for prefix, suffixes in prefixes.items():
            path = self._mapper.map(prefix)
            vf = self._get_vf(path)
            yield prefix, suffixes, vf

    def get_sha1s(self, keys):
        """See VersionedFiles.get_sha1s()."""
        sha1s = {}
        for prefix, suffixes, vf in self._iter_keys_vf(keys):
            vf_sha1s = vf.get_sha1s(suffixes)
            for suffix, sha1 in vf_sha1s.items():
                sha1s[prefix + (suffix,)] = sha1
        return sha1s

    def insert_record_stream(self, stream):
        """Insert a record stream into this container.

        :param stream: A stream of records to insert.
        :return: None
        :seealso VersionedFile.get_record_stream:
        """
        for record in stream:
            prefix = record.key[:-1]
            key = record.key[-1:]
            if record.parents is not None:
                parents = [parent[-1:] for parent in record.parents]
            else:
                parents = None
            thunk_record = AdapterFactory(key, parents, record)
            path = self._mapper.map(prefix)
            # Note that this parses the file many times; we can do better but
            # as this only impacts weaves in terms of performance, it is
            # tolerable.
            vf = self._get_vf(path)
            vf.insert_record_stream([thunk_record])

    def iter_lines_added_or_present_in_keys(self, keys, pb=None):
        r"""Iterate over the lines in the versioned files from keys.

        This may return lines from other keys. Each item the returned
        iterator yields is a tuple of a line and a text version that that line
        is present in (not introduced in).

        Ordering of results is in whatever order is most suitable for the
        underlying storage format.

        If a progress bar is supplied, it may be used to indicate progress.
        The caller is responsible for cleaning up progress bars (because this
        is an iterator).

        Notes:
         * Lines are normalised by the underlying store: they will all have \n
           terminators.
         * Lines are returned in arbitrary order.

        :return: An iterator over (line, key).
        """
        for prefix, suffixes, vf in self._iter_keys_vf(keys):
            for line, version in vf.iter_lines_added_or_present_in_versions(suffixes):
                yield line, prefix + (version,)

    def _iter_all_components(self):
        for path, prefix in self._iter_all_prefixes():
            yield prefix, self._get_vf(path)

    def keys(self):
        """See VersionedFiles.keys()."""
        result = set()
        for prefix, vf in self._iter_all_components():
            for suffix in vf.versions():
                result.add(prefix + (suffix,))
        return result


class VersionedFilesWithFallbacks(VersionedFiles):
    """A versioned files implementation that supports fallback sources.

    This class extends VersionedFiles to provide support for fallback
    versioned files that can supply content not present in the primary
    versioned files.
    """

    def without_fallbacks(self):
        """Return a clone of this object without any fallbacks configured."""
        raise NotImplementedError(self.without_fallbacks)

    def add_fallback_versioned_files(self, a_versioned_files):
        """Add a source of texts for texts not present in this knit.

        :param a_versioned_files: A VersionedFiles object.
        """
        raise NotImplementedError(self.add_fallback_versioned_files)

    def get_known_graph_ancestry(self, keys):
        """Get a KnownGraph instance with the ancestry of keys."""
        parent_map, missing_keys = self._index.find_ancestry(keys)
        for fallback in self._transitive_fallbacks():
            if not missing_keys:
                break
            (f_parent_map, f_missing_keys) = fallback._index.find_ancestry(missing_keys)
            parent_map.update(f_parent_map)
            missing_keys = f_missing_keys
        kg = _mod_known_graph.KnownGraph(parent_map)
        return kg


class _PlanMergeVersionedFile(VersionedFiles):
    """A VersionedFile for uncommitted and committed texts.

    It is intended to allow merges to be planned with working tree texts.
    It implements only the small part of the VersionedFiles interface used by
    PlanMerge.  It falls back to multiple versionedfiles for data not stored in
    _PlanMergeVersionedFile itself.

    :ivar: fallback_versionedfiles a list of VersionedFiles objects that can be
        queried for missing texts.
    """

    def __init__(self, file_id):
        """Create a _PlanMergeVersionedFile.

        :param file_id: Used with _PlanMerge code which is not yet fully
            tuple-keyspace aware.
        """
        self._file_id = file_id
        # fallback locations
        self.fallback_versionedfiles = []
        # Parents for locally held keys.
        self._parents = {}
        # line data for locally held keys.
        self._lines = {}
        # key lookup providers
        self._providers = [_mod_graph.DictParentsProvider(self._parents)]

    def plan_merge(self, ver_a, ver_b, base=None):
        """See VersionedFile.plan_merge."""
        from .merge import _PlanMerge

        if base is None:
            return _PlanMerge(ver_a, ver_b, self, (self._file_id,)).plan_merge()
        old_plan = list(_PlanMerge(ver_a, base, self, (self._file_id,)).plan_merge())
        new_plan = list(_PlanMerge(ver_a, ver_b, self, (self._file_id,)).plan_merge())
        return _PlanMerge._subtract_plans(old_plan, new_plan)

    def plan_lca_merge(self, ver_a, ver_b, base=None):
        from .merge import _PlanLCAMerge

        graph = _mod_graph.Graph(self)
        new_plan = _PlanLCAMerge(
            ver_a, ver_b, self, (self._file_id,), graph
        ).plan_merge()
        if base is None:
            return new_plan
        old_plan = _PlanLCAMerge(
            ver_a, base, self, (self._file_id,), graph
        ).plan_merge()
        return _PlanLCAMerge._subtract_plans(list(old_plan), list(new_plan))

    def add_content(self, factory):
        return self.add_lines(
            factory.key, factory.parents, factory.get_bytes_as("lines")
        )

    def add_lines(self, key, parents, lines):
        """See VersionedFiles.add_lines.

        Lines are added locally, not to fallback versionedfiles.  Also, ghosts
        are permitted.  Only reserved ids are permitted.
        """
        if not isinstance(key, tuple):
            raise TypeError(key)
        if not revision.is_reserved_id(key[-1]):
            raise ValueError("Only reserved ids may be used")
        if parents is None:
            raise ValueError("Parents may not be None")
        if lines is None:
            raise ValueError("Lines may not be None")
        self._parents[key] = tuple(parents)
        self._lines[key] = lines

    def get_record_stream(self, keys, ordering, include_delta_closure):
        pending = set(keys)
        for key in keys:
            if key in self._lines:
                lines = self._lines[key]
                parents = self._parents[key]
                pending.remove(key)
                yield ChunkedContentFactory(key, parents, None, lines)
        for versionedfile in self.fallback_versionedfiles:
            for record in versionedfile.get_record_stream(pending, "unordered", True):
                if record.storage_kind == "absent":
                    continue
                else:
                    pending.remove(record.key)
                    yield record
            if not pending:
                return
        # report absent entries
        for key in pending:
            yield AbsentContentFactory(key)

    def get_parent_map(self, keys):
        """See VersionedFiles.get_parent_map."""
        # We create a new provider because a fallback may have been added.
        # If we make fallbacks private we can update a stack list and avoid
        # object creation thrashing.
        keys = set(keys)
        result = {}
        if revision.NULL_REVISION in keys:
            keys.remove(revision.NULL_REVISION)
            result[revision.NULL_REVISION] = ()
        self._providers = self._providers[:1] + self.fallback_versionedfiles
        result.update(
            _mod_graph.StackedParentsProvider(self._providers).get_parent_map(keys)
        )
        for key, parents in result.items():
            if parents == ():
                result[key] = (revision.NULL_REVISION,)
        return result


class PlanWeaveMerge(TextMerge):
    """Weave merge that takes a plan as its input.

    This exists so that VersionedFile.plan_merge is implementable.
    Most callers will want to use WeaveMerge instead.
    """

    def __init__(self, plan, a_marker=TextMerge.A_MARKER, b_marker=TextMerge.B_MARKER):
        """Initialize a PlanWeaveMerge.

        Args:
            plan: The merge plan to execute.
            a_marker: Marker for 'A' side conflicts (optional).
            b_marker: Marker for 'B' side conflicts (optional).
        """
        TextMerge.__init__(self, a_marker, b_marker)
        self.plan = list(plan)

    def _merge_struct(self):
        from ._bzr_rs import textmerge as _textmerge_rs

        return iter(_textmerge_rs.merge_struct_from_plan(self.plan))

    def base_from_plan(self):
        """Construct a BASE file from the plan text."""
        from ._bzr_rs import textmerge as _textmerge_rs

        return _textmerge_rs.base_from_plan(self.plan)


class WeaveMerge(PlanWeaveMerge):
    """Weave merge that takes a VersionedFile and two versions as its input."""

    def __init__(
        self,
        versionedfile,
        ver_a,
        ver_b,
        a_marker=PlanWeaveMerge.A_MARKER,
        b_marker=PlanWeaveMerge.B_MARKER,
    ):
        """Initialize a WeaveMerge.

        Args:
            versionedfile: The versioned file containing the versions to merge.
            ver_a: First version ID to merge.
            ver_b: Second version ID to merge.
            a_marker: Marker for 'A' side conflicts (optional).
            b_marker: Marker for 'B' side conflicts (optional).
        """
        plan = versionedfile.plan_merge(ver_a, ver_b)
        PlanWeaveMerge.__init__(self, plan, a_marker, b_marker)


VirtualVersionedFiles = _versionedfile_rs.VirtualVersionedFiles
"""See VersionedFiles. Storage-less implementation backed by two callbacks.

`__init__(get_parent_map, get_lines)`: caller-supplied callables operating
on bare bytes keys. Backed by the Rust pyclass; the Python wrapper used
to live here and applied the same `(k,) <-> k` rewrapping the Rust
pyclass now does internally.
"""


class NoDupeAddLinesDecorator:
    """Decorator for a VersionedFiles that skips doing an add_lines if the key
    is already present.
    """

    def __init__(self, store):
        """Initialize a NoDupeAddLinesDecorator.

        Args:
            store: The underlying versioned files store to decorate.
        """
        self._store = store

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
        """See VersionedFiles.add_lines.

        This implementation may return None as the third element of the return
        value when the original store wouldn't.
        """
        if nostore_sha:
            raise NotImplementedError(
                "NoDupeAddLinesDecorator.add_lines does not implement the "
                "nostore_sha behaviour."
            )
        if key[-1] is None:
            sha1 = osutils.sha_strings(lines)
            key = (b"sha1:" + sha1,)
        else:
            sha1 = None
        if key in self._store.get_parent_map([key]):
            # This key has already been inserted, so don't do it again.
            if sha1 is None:
                sha1 = osutils.sha_strings(lines)
            return sha1, sum(map(len, lines)), None
        return self._store.add_lines(
            key,
            parents,
            lines,
            parent_texts=parent_texts,
            left_matching_blocks=left_matching_blocks,
            nostore_sha=nostore_sha,
            random_id=random_id,
            check_content=check_content,
        )

    def __getattr__(self, name):
        """Delegate attribute access to the underlying store.

        Args:
            name: Name of the attribute to access.

        Returns:
            The attribute value from the underlying store.
        """
        return getattr(self._store, name)


network_bytes_to_kind_and_offset = _versionedfile_rs.network_bytes_to_kind_and_offset


class NetworkRecordStream:
    """A record_stream which reconstitures a serialised stream."""

    def __init__(self, bytes_iterator):
        """Create a NetworkRecordStream.

        :param bytes_iterator: An iterator of bytes. Each item in this
            iterator should have been obtained from a record_streams'
            record.get_bytes_as(record.storage_kind) call.
        """
        from . import groupcompress, knit

        self._bytes_iterator = bytes_iterator
        self._kind_factory = {
            "fulltext": fulltext_network_to_record,
            "groupcompress-block": groupcompress.network_block_to_records,
            "knit-ft-gz": knit.knit_network_to_record,
            "knit-delta-gz": knit.knit_network_to_record,
            "knit-annotated-ft-gz": knit.knit_network_to_record,
            "knit-annotated-delta-gz": knit.knit_network_to_record,
            "knit-delta-closure": knit.knit_delta_closure_to_records,
        }

    def read(self):
        """Read the stream.

        :return: An iterator as per VersionedFiles.get_record_stream().
        """
        for bytes in self._bytes_iterator:
            storage_kind, line_end = network_bytes_to_kind_and_offset(bytes)
            yield from self._kind_factory[storage_kind](storage_kind, bytes, line_end)


def sort_groupcompress(parent_map):
    """Sort and group the keys in parent_map into groupcompress order.

    groupcompress is defined (currently) as reverse-topological order, grouped
    by the key prefix.

    :return: A sorted-list of keys
    """
    from ._bzr_rs import groupcompress as _groupcompress_rs

    # The Rust sort_gc_optimal accepts only tuple-shaped keys; wrap bare
    # bytes keys (used by Weave) into single-element tuples and unwrap on
    # the way back.
    bytes_keys = any(isinstance(k, bytes) for k in parent_map)
    if bytes_keys:
        wrapped = {(k,): tuple((p,) for p in v) for k, v in parent_map.items()}
        return [k[0] for k in _groupcompress_rs.sort_gc_optimal(wrapped)]
    return _groupcompress_rs.sort_gc_optimal(parent_map)


_KeyRefs = _versionedfile_rs.KeyRefs

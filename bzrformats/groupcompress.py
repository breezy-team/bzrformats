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

"""Core compression logic for compressing streams of related files."""

import logging

from ._bzr_rs import groupcompress as _groupcompress_rs
from ._bzr_rs.groupcompress import (  # noqa: F401
    GroupCompressBlock,
    RabinGroupCompressor,
    _BatchingBlockFetcher,
)
from ._bzr_rs.groupcompress import (
    GroupCompressVersionedFiles as _GroupCompressVersionedFilesRs,
)
from .btree_index import BTreeBuilder
from .errors import (
    BzrFormatsError,
    RevisionNotPresent,
)
from .versionedfile import (
    VersionedFilesWithFallbacks,
)

logger = logging.getLogger("bzrformats.groupcompress")

_null_sha1 = _groupcompress_rs.NULL_SHA1
PythonGroupCompressor = _groupcompress_rs.TraditionalGroupCompressor
rabin_hash = _groupcompress_rs.rabin_hash

# Minimum number of uncompressed bytes to try fetch at once when retrieving
# groupcompress blocks.
BATCH_SIZE = 2**16


def as_tuples(obj):
    """Ensure that the object and any referenced objects are plain tuples.

    :param obj: a list, tuple or StaticTuple
    :return: a plain tuple instance, with all children also being tuples.
    """
    result = []
    for item in obj:
        if isinstance(item, (tuple, list)):
            item = as_tuples(item)
        result.append(item)
    return tuple(result)


class DecompressCorruption(BzrFormatsError):
    """Exception raised when repository file decompression fails."""

    _fmt = "Corruption while decompressing repository file%(orig_error)s"

    def __init__(self, orig_error=None):
        """Initialize DecompressCorruption.

        Args:
            orig_error: The original error that caused the corruption.
        """
        if orig_error is not None:
            self.orig_error = f", {orig_error}"
        else:
            self.orig_error = ""
        super().__init__()


_LazyGroupCompressFactory = _groupcompress_rs.LazyGroupCompressFactory
_LazyGroupContentManager = _groupcompress_rs.LazyGroupContentManager


def network_block_to_records(storage_kind, bytes, line_end):
    """Convert a network block to records.

    Args:
        storage_kind: The type of storage (must be 'groupcompress-block').
        bytes: The block data bytes.
        line_end: Line ending marker.

    Returns:
        Generator yielding (key, data) tuples.
    """
    if storage_kind != "groupcompress-block":
        raise ValueError(f"Unknown storage kind: {storage_kind}")
    manager = _LazyGroupContentManager.from_bytes(bytes)
    return manager.get_record_stream()


def make_pack_factory(graph, delta, keylength, inconsistency_fatal=True):
    """Create a factory for creating a pack based groupcompress.

    This is only functional enough to run interface tests, it doesn't try to
    provide a full pack environment.

    :param graph: Store a graph.
    :param delta: Delta compress contents.
    :param keylength: How long should keys be.
    """
    from .pack import ContainerWriter
    from .pack_repo import _DirectPackAccess

    def factory(transport):
        parents = graph
        ref_length = 0
        if graph:
            ref_length = 1
        graph_index = BTreeBuilder(reference_lists=ref_length, key_elements=keylength)
        stream = transport.open_write_stream("newpack")
        writer = ContainerWriter(stream.write)
        writer.begin()
        index = _GCGraphIndex(
            graph_index,
            lambda: True,
            parents=parents,
            add_callback=graph_index.add_nodes,
            inconsistency_fatal=inconsistency_fatal,
        )
        access = _DirectPackAccess({})
        access.set_writer(writer, graph_index, (transport, "newpack"))
        result = GroupCompressVersionedFiles(index, access, delta)
        result.stream = stream
        result.writer = writer
        return result

    return factory


def cleanup_pack_group(versioned_files):
    """Clean up after packing a group of versioned files.

    Args:
        versioned_files: The versioned files to clean up.
    """
    versioned_files.writer.end()
    versioned_files.stream.close()


class GroupCompressVersionedFiles(
    _GroupCompressVersionedFilesRs, VersionedFilesWithFallbacks
):
    """A group-compress based VersionedFiles implementation.

    Storage state (the index/access objects, the block cache, fallbacks)
    and construction, ``without_fallbacks``, ``add_fallback_versioned_files``
    and ``clear_cache`` come from the Rust pyclass. ``VersionedFilesWithFallbacks``
    is mixed in so ``isinstance(x, VersionedFiles)`` holds. The remaining
    record-stream and insert methods are still defined here and migrate onto
    the Rust class over time.
    """

    # This controls how the GroupCompress DeltaIndex works. Basically, we
    # compute hash pointers into the source blocks (so hash(text) => text).
    # However each of these references costs some memory in trade against a
    # more accurate match result. For very large files, they either are
    # pre-compressed and change in bulk whenever they change, or change in just
    # local blocks. Either way, 'improved resolution' is not very helpful,
    # versus running out of memory trying to track everything. The default max
    # gives 100% sampling of a 1MB file.
    _DEFAULT_MAX_BYTES_TO_INDEX = 1024 * 1024
    _DEFAULT_COMPRESSOR_SETTINGS = {"max_bytes_to_index": _DEFAULT_MAX_BYTES_TO_INDEX}

    def annotate(self, key):
        """See VersionedFiles.annotate."""
        ann = self.get_annotator()
        return ann.annotate_flat(key)

    def get_annotator(self):
        """Get an annotator for this versioned file.

        Returns:
            A VersionedFileAnnotator instance.
        """
        from .annotate import VersionedFileAnnotator

        return VersionedFileAnnotator(self)

    def check(self, progress_bar=None, keys=None):
        """See VersionedFiles.check()."""
        if keys is None:
            keys = self.keys()
            for record in self.get_record_stream(keys, "unordered", True):
                for _chunk in record.iter_bytes_as("chunked"):
                    pass
        else:
            return self.get_record_stream(keys, "unordered", True)

    def get_missing_compression_parent_keys(self):
        """Return the keys of missing compression parents.

        Missing compression parents occur when a record stream was missing
        basis texts, or a index was scanned that had missing basis texts.
        """
        # GroupCompress cannot currently reference texts that are not in the
        # group, so this is valid for now
        return frozenset()

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
        keys = set(keys)
        total = len(keys)
        # we don't care about inclusions, the caller cares.
        # but we need to setup a list of records to visit.
        # we need key, position, length
        for key_idx, record in enumerate(
            self.get_record_stream(keys, "unordered", True)
        ):
            # XXX: todo - optimise to use less than full texts.
            key = record.key
            if pb is not None:
                pb.update("Walking content", key_idx, total)
            if record.storage_kind == "absent":
                raise RevisionNotPresent(key, self)
            for line in record.iter_bytes_as("lines"):
                yield line, key
        if pb is not None:
            pb.update("Walking content", total, total)


from ._bzr_rs import groupcompress
from ._bzr_rs.groupcompress import GCBuildDetails as _GCBuildDetails  # noqa: F401
from ._bzr_rs.groupcompress import _GCGraphIndex

encode_base128_int = groupcompress.encode_base128_int
encode_copy_instruction = groupcompress.encode_copy_instruction
LinesDeltaIndex = groupcompress.LinesDeltaIndex
make_line_delta = groupcompress.make_line_delta
make_rabin_delta = groupcompress.make_rabin_delta

apply_delta = groupcompress.apply_delta
apply_delta_to_source = groupcompress.apply_delta_to_source
decode_base128_int = groupcompress.decode_base128_int
decode_copy_instruction = groupcompress.decode_copy_instruction
encode_base128_int = groupcompress.encode_base128_int


GroupCompressor = RabinGroupCompressor

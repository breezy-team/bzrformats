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
import time
import zlib

from . import osutils
from ._bzr_rs import groupcompress as _groupcompress_rs
from ._bzr_rs.groupcompress import (
    GroupCompressBlock,
    RabinGroupCompressor,
    sort_gc_optimal,
)
from .btree_index import BTreeBuilder
from .errors import (
    BzrFormatsError,
    InvalidRevisionId,
    ObjectNotLocked,
    ReadOnlyError,
    RevisionNotPresent,
)
from .lru_cache import LRUSizeCache
from .osutils import sha_strings
from .versionedfile import (
    AbsentContentFactory,
    ChunkedContentFactory,
    UnavailableRepresentation,
    VersionedFilesWithFallbacks,
    _KeyRefs,
    adapter_registry,
)

evil_logger = logging.getLogger("bzrformats.evil")
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


class _LazyGroupCompressFactory:
    """Yield content from a GroupCompressBlock on demand."""

    def __init__(self, key, parents, manager, start, end, first):
        """Create a _LazyGroupCompressFactory.

        :param key: The key of just this record
        :param parents: The parents of this key (possibly None)
        :param gc_block: A GroupCompressBlock object
        :param start: Offset of the first byte for this record in the
            uncompressd content
        :param end: Offset of the byte just after the end of this record
            (ie, bytes = content[start:end])
        :param first: Is this the first Factory for the given block?
        """
        self.key = key
        self.parents = parents
        self.sha1 = None
        self.size = None
        # Note: This attribute coupled with Manager._factories creates a
        #       reference cycle. Perhaps we would rather use a weakref(), or
        #       find an appropriate time to release the ref. After the first
        #       get_bytes_as call? After Manager.get_record_stream() returns
        #       the object?
        self._manager = manager
        self._chunks = None
        self.storage_kind = "groupcompress-block"
        if not first:
            self.storage_kind = "groupcompress-block-ref"
        self._first = first
        self._start = start
        self._end = end

    def __repr__(self):
        return f"{self.__class__.__name__}({self.key}, first={self._first})"

    def _extract_bytes(self):
        # Grab and cache the raw bytes for this entry
        # and break the ref-cycle with _manager since we don't need it
        # anymore
        try:
            self._manager._prepare_for_extract()
        except zlib.error as value:
            raise DecompressCorruption("zlib: " + str(value)) from value
        block = self._manager._block
        self._chunks = block.extract(self.key, self._start, self._end)
        # There are code paths that first extract as fulltext, and then
        # extract as storage_kind (smart fetch). So we don't break the
        # refcycle here, but instead in manager.get_record_stream()

    def get_bytes_as(self, storage_kind):
        if storage_kind == self.storage_kind:
            if self._first:
                # wire bytes, something...
                return self._manager._wire_bytes()
            else:
                return b""
        if storage_kind in ("fulltext", "chunked", "lines"):
            if self._chunks is None:
                self._extract_bytes()
            if storage_kind == "fulltext":
                return b"".join(self._chunks)
            elif storage_kind == "chunked":
                return self._chunks
            else:
                return osutils.chunks_to_lines(self._chunks)
        raise UnavailableRepresentation(self.key, storage_kind, self.storage_kind)

    def iter_bytes_as(self, storage_kind):
        if self._chunks is None:
            self._extract_bytes()
        if storage_kind == "chunked":
            return iter(self._chunks)
        elif storage_kind == "lines":
            return osutils.chunks_to_lines_iter(iter(self._chunks))
        raise UnavailableRepresentation(self.key, storage_kind, self.storage_kind)


class _LazyGroupContentManager:
    """This manages a group of _LazyGroupCompressFactory objects."""

    _max_cut_fraction = 0.75  # We allow a block to be trimmed to 75% of
    # current size, and still be considered
    # resuable
    _full_block_size = 4 * 1024 * 1024
    _full_mixed_block_size = 2 * 1024 * 1024
    _full_enough_block_size = 3 * 1024 * 1024  # size at which we won't repack
    _full_enough_mixed_block_size = 2 * 768 * 1024  # 1.5MB

    def __init__(self, block, get_compressor_settings=None):
        self._block = block
        # We need to preserve the ordering
        self._factories = []
        self._last_byte = 0
        self._get_settings = get_compressor_settings
        self._compressor_settings = None

    def _get_compressor_settings(self):
        if self._compressor_settings is not None:
            return self._compressor_settings
        settings = None
        if self._get_settings is not None:
            settings = self._get_settings()
        if settings is None:
            vf = GroupCompressVersionedFiles
            settings = vf._DEFAULT_COMPRESSOR_SETTINGS
        self._compressor_settings = settings
        return self._compressor_settings

    def add_factory(self, key, parents, start, end):
        first = bool(not self._factories)
        # Note that this creates a reference cycle....
        factory = _LazyGroupCompressFactory(key, parents, self, start, end, first=first)
        # max() works here, but as a function call, doing a compare seems to be
        # significantly faster, timeit says 250ms for max() and 100ms for the
        # comparison
        if end > self._last_byte:
            self._last_byte = end
        self._factories.append(factory)

    def get_record_stream(self):
        """Get a record for all keys added so far."""
        for factory in self._factories:
            yield factory
            # Break the ref-cycle
            factory._bytes = None
            factory._manager = None
        # TODO: Consider setting self._factories = None after the above loop,
        #       as it will break the reference cycle

    def _trim_block(self, last_byte):
        """Create a new GroupCompressBlock, with just some of the content."""
        # None of the factories need to be adjusted, because the content is
        # located in an identical place. Just that some of the unreferenced
        # trailing bytes are stripped
        logger.debug(
            "stripping trailing bytes from groupcompress block %d => %d",
            self._block._content_length,
            last_byte,
        )
        new_block = GroupCompressBlock()
        self._block._ensure_content(last_byte)
        new_block.set_content(self._block._content[:last_byte])
        self._block = new_block

    def _make_group_compressor(self):
        return GroupCompressor(self._get_compressor_settings())

    def _rebuild_block(self):
        """Create a new GroupCompressBlock with only the referenced texts."""
        compressor = self._make_group_compressor()
        tstart = time.time()
        old_length = self._block._content_length
        end_point = 0
        for factory in self._factories:
            chunks = factory.get_bytes_as("chunked")
            chunks_len = factory.size
            if chunks_len is None:
                chunks_len = sum(map(len, chunks))
            (found_sha1, start_point, end_point, _type) = compressor.compress(
                factory.key, chunks, chunks_len, factory.sha1
            )
            # Now update this factory with the new offsets, etc
            factory.sha1 = found_sha1
            factory._start = start_point
            factory._end = end_point
        self._last_byte = end_point
        new_block = compressor.flush()
        # TODO: Should we check that new_block really *is* smaller than the old
        #       block? It seems hard to come up with a method that it would
        #       expand, since we do full compression again. Perhaps based on a
        #       request that ends up poorly ordered?
        # TODO: If the content would have expanded, then we would want to
        #       handle a case where we need to split the block.
        #       Now that we have a user-tweakable option
        #       (max_bytes_to_index), it is possible that one person set it
        #       to a very low value, causing poor compression.
        delta = time.time() - tstart
        self._block = new_block
        logger.debug(
            "creating new compressed block on-the-fly in %.3fs %d bytes => %d bytes",
            delta,
            old_length,
            self._block._content_length,
        )

    def _prepare_for_extract(self):
        """A _LazyGroupCompressFactory is about to extract to fulltext."""
        # We expect that if one child is going to fulltext, all will be. This
        # helps prevent all of them from extracting a small amount at a time.
        # Which in itself isn't terribly expensive, but resizing 2MB 32kB at a
        # time (self._block._content) is a little expensive.
        self._block._ensure_content(self._last_byte)

    def _check_rebuild_action(self):
        """Check to see if our block should be repacked."""
        total_bytes_used = 0
        last_byte_used = 0
        for factory in self._factories:
            total_bytes_used += factory._end - factory._start
            if last_byte_used < factory._end:
                last_byte_used = factory._end
        # If we are using more than half of the bytes from the block, we have
        # nothing else to check
        if total_bytes_used * 2 >= self._block._content_length:
            return None, last_byte_used, total_bytes_used
        # We are using less than 50% of the content. Is the content we are
        # using at the beginning of the block? If so, we can just trim the
        # tail, rather than rebuilding from scratch.
        if total_bytes_used * 2 > last_byte_used:
            return "trim", last_byte_used, total_bytes_used

        # We are using a small amount of the data, and it isn't just packed
        # nicely at the front, so rebuild the content.
        # Note: This would be *nicer* as a strip-data-from-group, rather than
        #       building it up again from scratch
        #       It might be reasonable to consider the fulltext sizes for
        #       different bits when deciding this, too. As you may have a small
        #       fulltext, and a trivial delta, and you are just trading around
        #       for another fulltext. If we do a simple 'prune' you may end up
        #       expanding many deltas into fulltexts, as well.
        #       If we build a cheap enough 'strip', then we could try a strip,
        #       if that expands the content, we then rebuild.
        return "rebuild", last_byte_used, total_bytes_used

    def check_is_well_utilized(self):
        """Is the current block considered 'well utilized'?

        This heuristic asks if the current block considers itself to be a fully
        developed group, rather than just a loose collection of data.
        """
        if len(self._factories) == 1:
            # A block of length 1 could be improved by combining with other
            # groups - don't look deeper. Even larger than max size groups
            # could compress well with adjacent versions of the same thing.
            return False
        _action, _last_byte_used, total_bytes_used = self._check_rebuild_action()
        block_size = self._block._content_length
        if total_bytes_used < block_size * self._max_cut_fraction:
            # This block wants to trim itself small enough that we want to
            # consider it under-utilized.
            return False
        # TODO: This code is meant to be the twin of _insert_record_stream's
        #       'start_new_block' logic. It would probably be better to factor
        #       out that logic into a shared location, so that it stays
        #       together better
        # We currently assume a block is properly utilized whenever it is >75%
        # of the size of a 'full' block. In normal operation, a block is
        # considered full when it hits 4MB of same-file content. So any block
        # >3MB is 'full enough'.
        # The only time this isn't true is when a given block has large-object
        # content. (a single file >4MB, etc.)
        # Under these circumstances, we allow a block to grow to
        # 2 x largest_content.  Which means that if a given block had a large
        # object, it may actually be under-utilized. However, given that this
        # is 'pack-on-the-fly' it is probably reasonable to not repack large
        # content blobs on-the-fly. Note that because we return False for all
        # 1-item blobs, we will repack them; we may wish to reevaluate our
        # treatment of large object blobs in the future.
        if block_size >= self._full_enough_block_size:
            return True
        # If a block is <3MB, it still may be considered 'full' if it contains
        # mixed content. The current rule is 2MB of mixed content is considered
        # full. So check to see if this block contains mixed content, and
        # set the threshold appropriately.
        common_prefix = None
        for factory in self._factories:
            prefix = factory.key[:-1]
            if common_prefix is None:
                common_prefix = prefix
            elif prefix != common_prefix:
                # Mixed content, check the size appropriately
                if block_size >= self._full_enough_mixed_block_size:
                    return True
                break
        # The content failed both the mixed check and the single-content check
        # so obviously it is not fully utilized
        # TODO: there is one other constraint that isn't being checked
        #       namely, that the entries in the block are in the appropriate
        #       order. For example, you could insert the entries in exactly
        #       reverse groupcompress order, and we would think that is ok.
        #       (all the right objects are in one group, and it is fully
        #       utilized, etc.) For now, we assume that case is rare,
        #       especially since we should always fetch in 'groupcompress'
        #       order.
        return False

    def _check_rebuild_block(self):
        action, last_byte_used, _total_bytes_used = self._check_rebuild_action()
        if action is None:
            return
        if action == "trim":
            self._trim_block(last_byte_used)
        elif action == "rebuild":
            self._rebuild_block()
        else:
            raise ValueError(f"unknown rebuild action: {action!r}")

    def _wire_bytes(self):
        """Return a byte stream suitable for transmitting over the wire."""
        self._check_rebuild_block()
        # The outer block starts with:
        #   'groupcompress-block\n'
        #   <length of compressed key info>\n
        #   <length of uncompressed info>\n
        #   <length of gc block>\n
        #   <header bytes>
        #   <gc-block>
        lines = [b"groupcompress-block\n"]
        # The minimal info we need is the key, the start offset, and the
        # parents. The length and type are encoded in the record itself.
        # However, passing in the other bits makes it easier.  The list of
        # keys, and the start offset, the length
        # 1 line key
        # 1 line with parents, '' for ()
        # 1 line for start offset
        # 1 line for end byte
        header_lines = []
        for factory in self._factories:
            key_bytes = b"\x00".join(factory.key)
            parents = factory.parents
            if parents is None:
                parent_bytes = b"None:"
            else:
                parent_bytes = b"\t".join(b"\x00".join(key) for key in parents)
            record_header = b"%s\n%s\n%d\n%d\n" % (
                key_bytes,
                parent_bytes,
                factory._start,
                factory._end,
            )
            header_lines.append(record_header)
            # TODO: Can we break the refcycle at this point and set
            #       factory._manager = None?
        header_bytes = b"".join(header_lines)
        del header_lines
        header_bytes_len = len(header_bytes)
        z_header_bytes = zlib.compress(header_bytes)
        del header_bytes
        z_header_bytes_len = len(z_header_bytes)
        block_bytes_len, block_chunks = self._block.to_chunks()
        lines.append(
            b"%d\n%d\n%d\n" % (z_header_bytes_len, header_bytes_len, block_bytes_len)
        )
        lines.append(z_header_bytes)
        lines.extend(block_chunks)
        del z_header_bytes, block_chunks
        # TODO: This is a point where we will double the memory consumption. To
        #       avoid this, we probably have to switch to a 'chunked' api
        return b"".join(lines)

    @classmethod
    def from_bytes(cls, bytes):
        block_bytes, factories = _groupcompress_rs.parse_wire_header(bytes)
        del bytes
        block = GroupCompressBlock.from_bytes(block_bytes)
        del block_bytes
        result = cls(block)
        for key, parents, start_offset, end_offset in factories:
            result.add_factory(key, parents, start_offset, end_offset)
        return result


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


class _BatchingBlockFetcher:
    """Fetch group compress blocks in batches.

    :ivar total_bytes: int of expected number of bytes needed to fetch the
        currently pending batch.
    """

    def __init__(self, gcvf, locations, get_compressor_settings=None):
        self.gcvf = gcvf
        self.locations = locations
        self.keys = []
        self.batch_memos = {}
        self.memos_to_get = []
        self.total_bytes = 0
        self.last_read_memo = None
        self.manager = None
        self._get_compressor_settings = get_compressor_settings

    def add_key(self, key):
        """Add another to key to fetch.

        :return: The estimated number of bytes needed to fetch the batch so
            far.
        """
        self.keys.append(key)
        index_memo, _, _, _ = self.locations[key]
        read_memo = index_memo[0:3]
        # Three possibilities for this read_memo:
        #  - it's already part of this batch; or
        #  - it's not yet part of this batch, but is already cached; or
        #  - it's not yet part of this batch and will need to be fetched.
        if read_memo in self.batch_memos:
            # This read memo is already in this batch.
            return self.total_bytes
        try:
            cached_block = self.gcvf._group_cache[read_memo]
        except KeyError:
            # This read memo is new to this batch, and the data isn't cached
            # either.
            self.batch_memos[read_memo] = None
            self.memos_to_get.append(read_memo)
            byte_length = read_memo[2]
            self.total_bytes += byte_length
        else:
            # This read memo is new to this batch, but cached.
            # Keep a reference to the cached block in batch_memos because it's
            # certain that we'll use it when this batch is processed, but
            # there's a risk that it would fall out of _group_cache between now
            # and then.
            self.batch_memos[read_memo] = cached_block
        return self.total_bytes

    def _flush_manager(self):
        if self.manager is not None:
            yield from self.manager.get_record_stream()
            self.manager = None
            self.last_read_memo = None

    def yield_factories(self, full_flush=False):
        """Yield factories for keys added since the last yield.  They will be
        returned in the order they were added via add_key.

        :param full_flush: by default, some results may not be returned in case
            they can be part of the next batch.  If full_flush is True, then
            all results are returned.
        """
        if self.manager is None and not self.keys:
            return
        # Fetch all memos in this batch.
        blocks = self.gcvf._get_blocks(self.memos_to_get)
        # Turn blocks into factories and yield them.
        memos_to_get_stack = list(self.memos_to_get)
        memos_to_get_stack.reverse()
        for key in self.keys:
            index_memo, _, parents, _ = self.locations[key]
            read_memo = index_memo[:3]
            if self.last_read_memo != read_memo:
                # We are starting a new block. If we have a
                # manager, we have found everything that fits for
                # now, so yield records
                yield from self._flush_manager()
                # Now start a new manager.
                if memos_to_get_stack and memos_to_get_stack[-1] == read_memo:
                    # The next block from _get_blocks will be the block we
                    # need.
                    block_read_memo, block = next(blocks)
                    if block_read_memo != read_memo:
                        raise AssertionError(
                            "block_read_memo out of sync with read_memo"
                            f"({block_read_memo!r} != {read_memo!r})"
                        )
                    self.batch_memos[read_memo] = block
                    memos_to_get_stack.pop()
                else:
                    block = self.batch_memos[read_memo]
                self.manager = _LazyGroupContentManager(
                    block, get_compressor_settings=self._get_compressor_settings
                )
                self.last_read_memo = read_memo
            start, end = index_memo[3:5]
            self.manager.add_factory(key, parents, start, end)
        if full_flush:
            yield from self._flush_manager()
        del self.keys[:]
        self.batch_memos.clear()
        del self.memos_to_get[:]
        self.total_bytes = 0


class GroupCompressVersionedFiles(VersionedFilesWithFallbacks):
    """A group-compress based VersionedFiles implementation."""

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

    def __init__(
        self, index, access, delta=True, _unadded_refs=None, _group_cache=None
    ):
        """Create a GroupCompressVersionedFiles object.

        :param index: The index object storing access and graph data.
        :param access: The access object storing raw data.
        :param delta: Whether to delta compress or just entropy compress.
        :param _unadded_refs: private parameter, don't use.
        :param _group_cache: private parameter, don't use.
        """
        self._index = index
        self._access = access
        self._delta = delta
        if _unadded_refs is None:
            _unadded_refs = {}
        self._unadded_refs = _unadded_refs
        if _group_cache is None:
            _group_cache = LRUSizeCache(max_size=50 * 1024 * 1024)
        self._group_cache = _group_cache
        self._immediate_fallback_vfs = []
        self._max_bytes_to_index = None

    def without_fallbacks(self):
        """Return a clone of this object without any fallbacks configured."""
        return GroupCompressVersionedFiles(
            self._index,
            self._access,
            self._delta,
            _unadded_refs=dict(self._unadded_refs),
            _group_cache=self._group_cache,
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
        r"""Add a text to the store.

        :param key: The key tuple of the text to add.
        :param parents: The parents key tuples of the text to add.
        :param lines: A list of lines. Each line must be a bytestring. And all
            of them except the last must be terminated with \n and contain no
            other \n's. The last line may either contain no \n's or a single
            terminating \n. If the lines list does meet this constraint the
            add routine may error or may succeed - but you will be unable to
            read the data back accurately. (Checking the lines have been split
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
        self._index._check_write_ok()
        if check_content:
            self._check_lines_not_unicode(lines)
            self._check_lines_are_lines(lines)
        return self.add_content(
            ChunkedContentFactory(key, parents, sha_strings(lines), lines),
            parent_texts,
            left_matching_blocks,
            nostore_sha,
            random_id,
        )

    def add_content(
        self,
        factory,
        parent_texts=None,
        left_matching_blocks=None,
        nostore_sha=None,
        random_id=False,
    ):
        """Add a text to the store.

        :param factory: A ContentFactory that can be used to retrieve the key,
            parents and contents.
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
        :return: The text sha1, the number of bytes in the text, and an opaque
                 representation of the inserted version which can be provided
                 back to future add_lines calls in the parent_texts dictionary.
        """
        self._index._check_write_ok()
        parents = factory.parents
        self._check_add(factory.key, random_id)
        if parents is None:
            # The caller might pass None if there is no graph data, but kndx
            # indexes can't directly store that, so we give them
            # an empty tuple instead.
            parents = ()
        # double handling for now. Make it work until then.
        sha1, length = list(
            self._insert_record_stream(
                [factory], random_id=random_id, nostore_sha=nostore_sha
            )
        )[0]
        return sha1, length, None

    def add_fallback_versioned_files(self, a_versioned_files):
        """Add a source of texts for texts not present in this knit.

        :param a_versioned_files: A VersionedFiles object.
        """
        self._immediate_fallback_vfs.append(a_versioned_files)

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

    def clear_cache(self):
        """See VersionedFiles.clear_cache()."""
        self._group_cache.clear()
        self._index._graph_index.clear_cache()
        self._index._int_cache.clear()

    def _check_add(self, key, random_id):
        """Check that version_id and lines are safe to add."""
        version_id = key[-1]
        if version_id is not None and osutils.contains_whitespace(version_id):
            raise InvalidRevisionId(version_id, self)
        self.check_not_reserved_id(version_id)
        # TODO: If random_id==False and the key is already present, we should
        # probably check that the existing content is identical to what is
        # being inserted, and otherwise raise an exception.  This would make
        # the bundle code simpler.

    def get_parent_map(self, keys):
        """Get a map of the graph parents of keys.

        :param keys: The keys to look up parents for.
        :return: A mapping from keys to parents. Absent keys are absent from
            the mapping.
        """
        return self._get_parent_map_with_sources(keys)[0]

    def _get_parent_map_with_sources(self, keys):
        """Get a map of the parents of keys.

        :param keys: The keys to look up parents for.
        :return: A tuple. The first element is a mapping from keys to parents.
            Absent keys are absent from the mapping. The second element is a
            list with the locations each key was found in. The first element
            is the in-this-knit parents, the second the first fallback source,
            and so on.
        """
        result = {}
        sources = [self._index] + self._immediate_fallback_vfs
        source_results = []
        missing = set(keys)
        for source in sources:
            if not missing:
                break
            new_result = source.get_parent_map(missing)
            source_results.append(new_result)
            result.update(new_result)
            missing.difference_update(set(new_result))
        return result, source_results

    def _get_blocks(self, read_memos):
        """Get GroupCompressBlocks for the given read_memos.

        :returns: a series of (read_memo, block) pairs, in the order they were
            originally passed.
        """
        cached = {}
        for read_memo in read_memos:
            try:
                block = self._group_cache[read_memo]
            except KeyError:
                pass
            else:
                cached[read_memo] = block
        not_cached = []
        not_cached_seen = set()
        for read_memo in read_memos:
            if read_memo in cached:
                # Don't fetch what we already have
                continue
            if read_memo in not_cached_seen:
                # Don't try to fetch the same data twice
                continue
            not_cached.append(read_memo)
            not_cached_seen.add(read_memo)
        raw_records = self._access.get_raw_records(not_cached)
        for read_memo in read_memos:
            try:
                yield read_memo, cached[read_memo]
            except KeyError:
                # Read the block, and cache it.
                zdata = next(raw_records)
                block = GroupCompressBlock.from_bytes(zdata)
                self._group_cache[read_memo] = block
                cached[read_memo] = block
                yield read_memo, block

    def get_missing_compression_parent_keys(self):
        """Return the keys of missing compression parents.

        Missing compression parents occur when a record stream was missing
        basis texts, or a index was scanned that had missing basis texts.
        """
        # GroupCompress cannot currently reference texts that are not in the
        # group, so this is valid for now
        return frozenset()

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
        from .pack_repo import RetryWithNewPacks

        # keys might be a generator
        orig_keys = list(keys)
        keys = set(keys)
        if not keys:
            return
        if not self._index.has_graph and ordering in ("topological", "groupcompress"):
            # Cannot topological order when no graph has been stored.
            # but we allow 'as-requested' or 'unordered'
            ordering = "unordered"

        remaining_keys = keys
        while True:
            try:
                keys = set(remaining_keys)
                for content_factory in self._get_remaining_record_stream(
                    keys, orig_keys, ordering, include_delta_closure
                ):
                    remaining_keys.discard(content_factory.key)
                    yield content_factory
                return
            except RetryWithNewPacks as e:
                self._access.reload_or_raise(e)

    def _find_from_fallback(self, missing):
        """Find whatever keys you can from the fallbacks.

        :param missing: A set of missing keys. This set will be mutated as keys
            are found from a fallback_vfs
        :return: (parent_map, key_to_source_map, source_results)
            parent_map  the overall key => parent_keys
            key_to_source_map   a dict from {key: source}
            source_results      a list of (source: keys)
        """
        parent_map = {}
        key_to_source_map = {}
        source_results = []
        for source in self._immediate_fallback_vfs:
            if not missing:
                break
            source_parents = source.get_parent_map(missing)
            parent_map.update(source_parents)
            source_parents = list(source_parents)
            source_results.append((source, source_parents))
            key_to_source_map.update((key, source) for key in source_parents)
            missing.difference_update(source_parents)
        return parent_map, key_to_source_map, source_results

    def _get_ordered_source_keys(self, ordering, parent_map, key_to_source_map):
        """Get the (source, [keys]) list.

        The returned objects should be in the order defined by 'ordering',
        which can weave between different sources.

        :param ordering: Must be one of 'topological' or 'groupcompress'
        :return: List of [(source, [keys])] tuples, such that all keys are in
            the defined order, regardless of source.
        """
        import vcsgraph.tsort as tsort

        if ordering == "topological":
            present_keys = tsort.topo_sort(parent_map)
        else:
            # ordering == 'groupcompress'
            # XXX: This only optimizes for the target ordering. We may need
            #      to balance that with the time it takes to extract
            #      ordering, by somehow grouping based on
            #      locations[key][0:3]
            present_keys = sort_gc_optimal(parent_map)
        # Now group by source:
        source_keys = []
        current_source = None
        for key in present_keys:
            source = key_to_source_map.get(key, self)
            if source is not current_source:
                source_keys.append((source, []))
                current_source = source
            source_keys[-1][1].append(key)
        return source_keys

    def _get_as_requested_source_keys(
        self, orig_keys, locations, unadded_keys, key_to_source_map
    ):
        source_keys = []
        current_source = None
        for key in orig_keys:
            if key in locations or key in unadded_keys:
                source = self
            elif key in key_to_source_map:
                source = key_to_source_map[key]
            else:  # absent
                continue
            if source is not current_source:
                source_keys.append((source, []))
                current_source = source
            source_keys[-1][1].append(key)
        return source_keys

    def _get_io_ordered_source_keys(self, locations, unadded_keys, source_result):
        def get_group(key):
            # This is the group the bytes are stored in, followed by the
            # location in the group
            return locations[key][0]

        # We don't have an ordering for keys in the in-memory object, but
        # lets process the in-memory ones first.
        present_keys = list(unadded_keys)
        present_keys.extend(sorted(locations, key=get_group))
        # Now grab all of the ones from other sources
        source_keys = [(self, present_keys)]
        source_keys.extend(source_result)
        return source_keys

    def _get_remaining_record_stream(
        self, keys, orig_keys, ordering, include_delta_closure
    ):
        """Get a stream of records for keys.

        :param keys: The keys to include.
        :param ordering: one of 'unordered', 'topological', 'groupcompress' or
            'as-requested'
        :param include_delta_closure: If True then the closure across any
            compression parents will be included (in the opaque data).
        :return: An iterator of ContentFactory objects, each of which is only
            valid until the iterator is advanced.
        """
        # Cheap: iterate
        locations = self._index.get_build_details(keys)
        unadded_keys = set(self._unadded_refs).intersection(keys)
        missing = keys.difference(locations)
        missing.difference_update(unadded_keys)
        (
            fallback_parent_map,
            key_to_source_map,
            source_result,
        ) = self._find_from_fallback(missing)
        if ordering in ("topological", "groupcompress"):
            # would be better to not globally sort initially but instead
            # start with one key, recurse to its oldest parent, then grab
            # everything in the same group, etc.
            parent_map = {key: details[2] for key, details in locations.items()}
            for key in unadded_keys:
                parent_map[key] = self._unadded_refs[key]
            parent_map.update(fallback_parent_map)
            source_keys = self._get_ordered_source_keys(
                ordering, parent_map, key_to_source_map
            )
        elif ordering == "as-requested":
            source_keys = self._get_as_requested_source_keys(
                orig_keys, locations, unadded_keys, key_to_source_map
            )
        else:
            # We want to yield the keys in a semi-optimal (read-wise) ordering.
            # Otherwise we thrash the _group_cache and destroy performance
            source_keys = self._get_io_ordered_source_keys(
                locations, unadded_keys, source_result
            )
        for key in missing:
            yield AbsentContentFactory(key)
        # Batch up as many keys as we can until either:
        #  - we encounter an unadded ref, or
        #  - we run out of keys, or
        #  - the total bytes to retrieve for this batch > BATCH_SIZE
        batcher = _BatchingBlockFetcher(
            self, locations, get_compressor_settings=self._get_compressor_settings
        )
        for source, keys in source_keys:
            if source is self:
                for key in keys:
                    if key in self._unadded_refs:
                        # Flush batch, then yield unadded ref from
                        # self._compressor.
                        yield from batcher.yield_factories(full_flush=True)
                        chunks, sha1 = self._compressor.extract(key)
                        parents = self._unadded_refs[key]
                        yield ChunkedContentFactory(key, parents, sha1, chunks)
                        continue
                    if batcher.add_key(key) > BATCH_SIZE:
                        # Ok, this batch is big enough.  Yield some results.
                        yield from batcher.yield_factories()
            else:
                yield from batcher.yield_factories(full_flush=True)
                yield from source.get_record_stream(
                    keys, ordering, include_delta_closure
                )
        yield from batcher.yield_factories(full_flush=True)

    def get_sha1s(self, keys):
        """See VersionedFiles.get_sha1s()."""
        result = {}
        for record in self.get_record_stream(keys, "unordered", True):
            if record.sha1 is not None:
                result[record.key] = record.sha1
            else:
                if record.storage_kind != "absent":
                    result[record.key] = sha_strings(record.iter_bytes_as("chunked"))
        return result

    def insert_record_stream(self, stream):
        """Insert a record stream into this container.

        :param stream: A stream of records to insert.
        :return: None
        :seealso VersionedFiles.get_record_stream:
        """
        # XXX: Setting random_id=True makes
        # test_insert_record_stream_existing_keys fail for groupcompress and
        # groupcompress-nograph, this needs to be revisited while addressing
        # 'bzr branch' performance issues.
        for _, _ in self._insert_record_stream(stream, random_id=False):
            pass

    def _get_compressor_settings(self):
        if self._max_bytes_to_index is None:
            self._max_bytes_to_index = self._DEFAULT_MAX_BYTES_TO_INDEX
        return {"max_bytes_to_index": self._max_bytes_to_index}

    def _make_group_compressor(self):
        return GroupCompressor(self._get_compressor_settings())

    def _insert_record_stream(
        self, stream, random_id=False, nostore_sha=None, reuse_blocks=True
    ):
        """Internal core to insert a record stream into this container.

        This helper function has a different interface than insert_record_stream
        to allow add_lines to be minimal, but still return the needed data.

        :param stream: A stream of records to insert.
        :param nostore_sha: If the sha1 of a given text matches nostore_sha,
            raise ExistingContent, rather than committing the new text.
        :param reuse_blocks: If the source is streaming from
            groupcompress-blocks, just insert the blocks as-is, rather than
            expanding the texts and inserting again.
        :return: An iterator over (sha1, length) of the inserted records.
        :seealso insert_record_stream:
        :seealso add_lines:
        """
        adapters = {}

        def get_adapter(adapter_key):
            try:
                return adapters[adapter_key]
            except KeyError:
                adapter_factory = adapter_registry.get(adapter_key)
                adapter = adapter_factory(self)
                adapters[adapter_key] = adapter
                return adapter

        # This will go up to fulltexts for gc to gc fetching, which isn't
        # ideal.
        self._compressor = self._make_group_compressor()
        self._unadded_refs = {}
        keys_to_add = []

        def flush(block):
            bytes_len, chunks = block.to_chunks()
            self._compressor = self._make_group_compressor()
            # Note: At this point we still have 1 copy of the fulltext (in
            #       record and the var 'bytes'), and this generates 2 copies of
            #       the compressed text (one for bytes, one in chunks)
            # TODO: Figure out how to indicate that we would be happy to free
            #       the fulltext content at this point. Note that sometimes we
            #       will want it later (streaming CHK pages), but most of the
            #       time we won't (everything else)
            _index, start, length = self._access.add_raw_record(None, bytes_len, chunks)
            nodes = []
            for key, reads, refs in keys_to_add:
                nodes.append((key, b"%d %d %s" % (start, length, reads), refs))
            self._index.add_records(nodes, random_id=random_id)
            self._unadded_refs = {}
            del keys_to_add[:]

        last_prefix = None
        max_fulltext_len = 0
        max_fulltext_prefix = None
        insert_manager = None
        block_start = None
        block_length = None
        # XXX: TODO: remove this, it is just for safety checking for now
        inserted_keys = set()
        reuse_this_block = reuse_blocks
        for record in stream:
            # Raise an error when a record is missing.
            if record.storage_kind == "absent":
                raise RevisionNotPresent(record.key, self)
            if random_id:
                if record.key in inserted_keys:
                    logger.info(
                        "Insert claimed random_id=True, but then inserted %r two times",
                        record.key,
                    )
                    continue
                inserted_keys.add(record.key)
            if reuse_blocks:
                # If the reuse_blocks flag is set, check to see if we can just
                # copy a groupcompress block as-is.
                # We only check on the first record (groupcompress-block) not
                # on all of the (groupcompress-block-ref) entries.
                # The reuse_this_block flag is then kept for as long as
                if record.storage_kind == "groupcompress-block":
                    # Check to see if we really want to re-use this block
                    insert_manager = record._manager
                    reuse_this_block = insert_manager.check_is_well_utilized()
            else:
                reuse_this_block = False
            if reuse_this_block:
                # We still want to reuse this block
                if record.storage_kind == "groupcompress-block":
                    # Insert the raw block into the target repo
                    insert_manager = record._manager
                    bytes_len, chunks = record._manager._block.to_chunks()
                    _, start, length = self._access.add_raw_record(
                        None, bytes_len, chunks
                    )
                    block_start = start
                    block_length = length
                if record.storage_kind in (
                    "groupcompress-block",
                    "groupcompress-block-ref",
                ):
                    if insert_manager is None:
                        raise AssertionError("No insert_manager set")
                    if insert_manager is not record._manager:
                        raise AssertionError(
                            "insert_manager does not match"
                            " the current record, we cannot be positive"
                            " that the appropriate content was inserted."
                        )
                    value = b"%d %d %d %d" % (
                        block_start,
                        block_length,
                        record._start,
                        record._end,
                    )
                    nodes = [(record.key, value, (record.parents,))]
                    # TODO: Consider buffering up many nodes to be added, not
                    #       sure how much overhead this has, but we're seeing
                    #       ~23s / 120s in add_records calls
                    self._index.add_records(nodes, random_id=random_id)
                    continue
            try:
                chunks = record.get_bytes_as("chunked")
            except UnavailableRepresentation:
                adapter_key = record.storage_kind, "chunked"
                adapter = get_adapter(adapter_key)
                chunks = adapter.get_bytes(record, "chunked")
            chunks_len = record.size
            if chunks_len is None:
                chunks_len = sum(map(len, chunks))
            if len(record.key) > 1:
                prefix = record.key[0]
                soft = prefix == last_prefix
            else:
                prefix = None
                soft = False
            if max_fulltext_len < chunks_len:
                max_fulltext_len = chunks_len
                max_fulltext_prefix = prefix
            (found_sha1, start_point, end_point, _type) = self._compressor.compress(
                record.key,
                chunks,
                chunks_len,
                record.sha1,
                soft=soft,
                nostore_sha=nostore_sha,
            )
            # delta_ratio = float(chunks_len) / (end_point - start_point)
            # Check if we want to continue to include that text
            if prefix == max_fulltext_prefix and end_point < 2 * max_fulltext_len:
                # As long as we are on the same file_id, we will fill at least
                # 2 * max_fulltext_len
                start_new_block = False
            elif end_point > 4 * 1024 * 1024:
                start_new_block = True
            elif (
                prefix is not None
                and prefix != last_prefix
                and end_point > 2 * 1024 * 1024
            ):
                start_new_block = True
            else:
                start_new_block = False
            last_prefix = prefix
            if start_new_block:
                flush(self._compressor.flush_without_last())
                max_fulltext_len = chunks_len
                (found_sha1, start_point, end_point, _type) = self._compressor.compress(
                    record.key, chunks, chunks_len, record.sha1
                )
            if record.key[-1] is None:
                key = record.key[:-1] + (b"sha1:" + found_sha1,)
            else:
                key = record.key
            self._unadded_refs[key] = record.parents
            yield found_sha1, chunks_len
            if record.parents is not None:
                parents = tuple([tuple(p) for p in record.parents])
            else:
                parents = None
            refs = (parents,)
            keys_to_add.append((key, b"%d %d" % (start_point, end_point), refs))
        if len(keys_to_add):
            flush(self._compressor.flush())
        self._compressor = None

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

    def keys(self):
        """See VersionedFiles.keys."""
        evil_logger.debug("keys scales with size of history")
        sources = [self._index] + self._immediate_fallback_vfs
        result = set()
        for source in sources:
            result.update(source.keys())
        return result


class _GCBuildDetails:
    """A blob of data about the build details.

    This stores the minimal data, which then allows compatibility with the old
    api, without taking as much memory.
    """

    __slots__ = (
        "_basis_end",
        "_delta_end",
        "_group_end",
        "_group_start",
        "_index",
        "_parents",
    )

    method = "group"
    compression_parent = None

    def __init__(self, parents, position_info):
        self._parents = parents
        (
            self._index,
            self._group_start,
            self._group_end,
            self._basis_end,
            self._delta_end,
        ) = position_info

    def __repr__(self):
        return f"{self.__class__.__name__}({self.index_memo}, {self._parents})"

    @property
    def index_memo(self):
        return (
            self._index,
            self._group_start,
            self._group_end,
            self._basis_end,
            self._delta_end,
        )

    @property
    def record_details(self):
        return (self.method, None)

    def __getitem__(self, offset):
        """Compatibility thunk to act like a tuple."""
        if offset == 0:
            return self.index_memo
        elif offset == 1:
            return self.compression_parent  # Always None
        elif offset == 2:
            return self._parents
        elif offset == 3:
            return self.record_details
        else:
            raise IndexError("offset out of range")

    def __len__(self):
        return 4


class _GCGraphIndex:
    """Mapper from GroupCompressVersionedFiles needs into GraphIndex storage."""

    def __init__(
        self,
        graph_index,
        is_locked,
        parents=True,
        add_callback=None,
        track_external_parent_refs=False,
        inconsistency_fatal=True,
        track_new_keys=False,
    ):
        """Construct a _GCGraphIndex on a graph_index.

        :param graph_index: An implementation of bzrformats.index.GraphIndex.
        :param is_locked: A callback, returns True if the index is locked and
            thus usable.
        :param parents: If True, record knits parents, if not do not record
            parents.
        :param add_callback: If not None, allow additions to the index and call
            this callback with a list of added GraphIndex nodes:
            [(node, value, node_refs), ...]
        :param track_external_parent_refs: As keys are added, keep track of the
            keys they reference, so that we can query get_missing_parents(),
            etc.
        :param inconsistency_fatal: When asked to add records that are already
            present, and the details are inconsistent with the existing
            record, raise an exception instead of warning (and skipping the
            record).
        """
        self._add_callback = add_callback
        self._graph_index = graph_index
        self._parents = parents
        self.has_graph = parents
        self._is_locked = is_locked
        self._inconsistency_fatal = inconsistency_fatal
        # GroupCompress records tend to have the same 'group' start + offset
        # repeated over and over, this creates a surplus of ints
        self._int_cache = {}
        if track_external_parent_refs:
            self._key_dependencies = _KeyRefs(track_new_keys=track_new_keys)
        else:
            self._key_dependencies = None

    def add_records(self, records, random_id=False):
        """Add multiple records to the index.

        This function does not insert data into the Immutable GraphIndex
        backing the KnitGraphIndex, instead it prepares data for insertion by
        the caller and checks that it is safe to insert then calls
        self._add_callback with the prepared GraphIndex nodes.

        :param records: a list of tuples:
                         (key, options, access_memo, parents).
        :param random_id: If True the ids being added were randomly generated
            and no check for existence will be performed.
        """
        if not self._add_callback:
            raise ReadOnlyError(self)
        # we hope there are no repositories with inconsistent parentage
        # anymore.

        changed = False
        keys = {}
        for key, value, refs in records:
            if not self._parents and refs:
                for ref in refs:
                    if ref:
                        from . import knit

                        raise knit.KnitCorrupt(
                            self,
                            "attempt to add node with parents in parentless index.",
                        )
                refs = ()
                changed = True
            keys[key] = (value, refs)
        # check for dups
        if not random_id:
            present_nodes = self._get_entries(keys)
            for _index, key, value, node_refs in present_nodes:
                # Sometimes these are passed as a list rather than a tuple
                node_refs = as_tuples(node_refs)
                passed = as_tuples(keys[key])
                if node_refs != passed[1]:
                    details = f"{key} {value, node_refs} {passed}"
                    if self._inconsistency_fatal:
                        from . import knit

                        raise knit.KnitCorrupt(
                            self,
                            "inconsistent details in add_records: {}".format(details),
                        )
                    else:
                        logger.warning(
                            "inconsistent details in skipped record: %s", details
                        )
                del keys[key]
                changed = True
        if changed:
            result = []
            if self._parents:
                for key, (value, node_refs) in keys.items():
                    result.append((key, value, node_refs))
            else:
                for key, (value, node_refs) in keys.items():  # noqa: B007
                    result.append((key, value))
            records = result
        key_dependencies = self._key_dependencies
        if key_dependencies is not None:
            if self._parents:
                for key, value, refs in records:  # noqa: B007
                    parents = refs[0]
                    key_dependencies.add_references(key, parents)
            else:
                for key, value, refs in records:  # noqa: B007
                    new_keys.add_key(key)
        self._add_callback(records)

    def _check_read(self):
        """Raise an exception if reads are not permitted."""
        if not self._is_locked():
            raise ObjectNotLocked(self)

    def _check_write_ok(self):
        """Raise an exception if writes are not permitted."""
        if not self._is_locked():
            raise ObjectNotLocked(self)

    def _get_entries(self, keys, check_present=False):
        """Get the entries for keys.

        Note: Callers are responsible for checking that the index is locked
        before calling this method.

        :param keys: An iterable of index key tuples.
        """
        keys = set(keys)
        found_keys = set()
        if self._parents:
            for node in self._graph_index.iter_entries(keys):
                yield node
                found_keys.add(node[1])
        else:
            # adapt parentless index to the rest of the code.
            for node in self._graph_index.iter_entries(keys):
                yield node[0], node[1], node[2], ()
                found_keys.add(node[1])
        if check_present:
            missing_keys = keys.difference(found_keys)
            if missing_keys:
                raise RevisionNotPresent(missing_keys.pop(), self)

    def find_ancestry(self, keys):
        """See CombinedGraphIndex.find_ancestry."""
        return self._graph_index.find_ancestry(keys, 0)

    def get_parent_map(self, keys):
        """Get a map of the parents of keys.

        :param keys: The keys to look up parents for.
        :return: A mapping from keys to parents. Absent keys are absent from
            the mapping.
        """
        self._check_read()
        nodes = self._get_entries(keys)
        result = {}
        if self._parents:
            for node in nodes:
                result[node[1]] = node[3][0]
        else:
            for node in nodes:
                result[node[1]] = None
        return result

    def get_missing_parents(self):
        """Return the keys of missing parents."""
        # Copied from _KnitGraphIndex.get_missing_parents
        # We may have false positives, so filter those out.
        self._key_dependencies.satisfy_refs_for_keys(
            self.get_parent_map(self._key_dependencies.get_unsatisfied_refs())
        )
        return frozenset(self._key_dependencies.get_unsatisfied_refs())

    def get_build_details(self, keys):
        """Get the various build details for keys.

        Ghosts are omitted from the result.

        :param keys: An iterable of keys.
        :return: A dict of key:
            (index_memo, compression_parent, parents, record_details).

            * index_memo: opaque structure to pass to read_records to extract
              the raw data
            * compression_parent: Content that this record is built upon, may
              be None
            * parents: Logical parents of this node
            * record_details: extra information about the content which needs
              to be passed to Factory.parse_record
        """
        self._check_read()
        result = {}
        entries = self._get_entries(keys)
        for entry in entries:
            key = entry[1]
            parents = None if not self._parents else entry[3][0]
            details = _GCBuildDetails(parents, self._node_to_position(entry))
            result[key] = details
        return result

    def keys(self):
        """Get all the keys in the collection.

        The keys are not ordered.
        """
        self._check_read()
        return [node[1] for node in self._graph_index.iter_all_entries()]

    def _node_to_position(self, node):
        """Convert an index value to position details."""
        bits = node[2].split(b" ")
        # It would be nice not to read the entire gzip.
        # start and stop are put into _int_cache because they are very common.
        # They define the 'group' that an entry is in, and many groups can have
        # thousands of objects.
        # Branching Launchpad, for example, saves ~600k integers, at 12 bytes
        # each, or about 7MB. Note that it might be even more when you consider
        # how PyInt is allocated in separate slabs. And you can't return a slab
        # to the OS if even 1 int on it is in use. Note though that Python uses
        # a LIFO when re-using PyInt slots, which might cause more
        # fragmentation.
        start = int(bits[0])
        start = self._int_cache.setdefault(start, start)
        stop = int(bits[1])
        stop = self._int_cache.setdefault(stop, stop)
        basis_end = int(bits[2])
        delta_end = int(bits[3])
        # We can't use tuple here, because node[0] is a BTreeGraphIndex
        # instance...
        return (node[0], start, stop, basis_end, delta_end)

    def scan_unvalidated_index(self, graph_index):
        """Inform this _GCGraphIndex that there is an unvalidated index.

        This allows this _GCGraphIndex to keep track of any missing
        compression parents we may want to have filled in to make those
        indices valid.  It also allows _GCGraphIndex to track any new keys.

        :param graph_index: A GraphIndex
        """
        key_dependencies = self._key_dependencies
        if key_dependencies is None:
            return
        for node in graph_index.iter_all_entries():
            # Add parent refs from graph_index (and discard parent refs
            # that the graph_index has).
            key_dependencies.add_references(node[1], node[3][0])


from ._bzr_rs import groupcompress

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

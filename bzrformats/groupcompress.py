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

from . import osutils
from ._bzr_rs import groupcompress as _groupcompress_rs
from ._bzr_rs.groupcompress import (  # noqa: F401
    GroupCompressBlock,
    RabinGroupCompressor,
    _BatchingBlockFetcher,
    sort_gc_optimal,
)
from ._bzr_rs.groupcompress import (
    GroupCompressVersionedFiles as _GroupCompressVersionedFilesRs,
)
from .btree_index import BTreeBuilder
from .errors import (
    BzrFormatsError,
    InvalidRevisionId,
    RevisionNotPresent,
)
from .osutils import sha_strings
from .versionedfile import (
    AbsentContentFactory,
    ChunkedContentFactory,
    UnavailableRepresentation,
    VersionedFilesWithFallbacks,
    adapter_registry,
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
            except ValueError as e:
                # Rust groupcompress raises ValueError for corrupt
                # deflate / mismatched length / unparseable content;
                # surface a structured BzrFormatsError so callers see
                # the same class regardless of source.
                raise DecompressCorruption(str(e)) from e
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

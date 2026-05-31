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

"""Knit versionedfile implementation.

A knit is a versioned file implementation that supports efficient append only
updates.

Knit file layout:
lifeless: the data file is made up of "delta records".  each delta record has a delta header
that contains; (1) a version id, (2) the size of the delta (in lines), and (3)  the digest of
the -expanded data- (ie, the delta applied to the parent).  the delta also ends with a
end-marker; simply "end VERSION"

delta can be line or full contents.a
... the 8's there are the index number of the annotation.
version robertc@robertcollins.net-20051003014215-ee2990904cc4c7ad 7 c7d23b2a5bd6ca00e8e266cec0ec228158ee9f9e
59,59,3
8
8         if ie.executable:
8             e.set('executable', 'yes')
130,130,2
8         if elt.get('executable') == 'yes':
8             ie.executable = True
end robertc@robertcollins.net-20051003014215-ee2990904cc4c7ad


whats in an index:
09:33 < jrydberg> lifeless: each index is made up of a tuple of; version id, options, position, size, parents
09:33 < jrydberg> lifeless: the parents are currently dictionary compressed
09:33 < jrydberg> lifeless: (meaning it currently does not support ghosts)
09:33 < lifeless> right
09:33 < jrydberg> lifeless: the position and size is the range in the data file


so the index sequence is the dictionary compressed sequence number used
in the deltas to provide line annotation

"""

import logging

from . import pack_repo
from .annotate import VersionedFileAnnotator

# The knit error hierarchy lives in the Rust errors module; re-export it so
# bzrformats.knit.KnitCorrupt (and friends) keep working for callers and for
# the Rust import_exception!(bzrformats.knit, ...) sites.
from .errors import (  # noqa: F401
    KnitCorrupt,
    KnitDataStreamIncompatible,
    KnitDataStreamUnknown,
    KnitError,
    KnitHeaderError,
    KnitIndexUnknownMethod,
    RevisionNotPresent,
    SHA1KnitCorrupt,
)
from .versionedfile import (
    UnavailableRepresentation,
)

evil_logger = logging.getLogger("bzrformats.evil")
logger = logging.getLogger("bzrformats.knit")

# TODO: Split out code specific to this format into an associated object.

# TODO: Can we put in some kind of value to check that the index and data
# files belong together?

# TODO: accommodate binaries, perhaps by storing a byte count

# TODO: function to check whole file

# TODO: atomically append data, then measure backwards from the cursor
# position after writing to work out where it was located.  we may need to
# bypass python file buffering.

DATA_SUFFIX = ".knit"
INDEX_SUFFIX = ".kndx"
_STREAM_MIN_BUFFER_SIZE = 5 * 1024 * 1024


class KnitAdapter:
    """Adapter shim wrapping the Rust ``KnitAdapter`` registry.

    Subclasses set ``_source_kind``; ``get_bytes`` looks up the
    ``(source, target)`` adapter at call time via ``get_knit_adapter`` and
    delegates to it. The real conversion logic lives in
    ``crates/bazaar/src/knit.rs``.
    """

    _source_kind: str = ""

    def __init__(self, basis_vf):
        """Create an adapter which accesses full texts from basis_vf.

        :param basis_vf: A versioned file to access basis texts of deltas from.
            May be None for adapters that do not need to access basis texts.
        """
        self._basis_vf = basis_vf

    def get_bytes(self, factory, target_storage_kind):
        adapter = _knit_rs.get_knit_adapter(
            self._source_kind, target_storage_kind, self._basis_vf
        )
        if adapter is None:
            raise UnavailableRepresentation(
                factory.key, target_storage_kind, factory.storage_kind
            )
        return adapter.get_bytes(factory, target_storage_kind)


class FTAnnotatedToUnannotated(KnitAdapter):
    """Annotated fulltext -> unannotated fulltext."""

    _source_kind = "knit-annotated-ft-gz"


class DeltaAnnotatedToUnannotated(KnitAdapter):
    """Annotated delta -> unannotated delta."""

    _source_kind = "knit-annotated-delta-gz"


class FTAnnotatedToFullText(KnitAdapter):
    """Annotated fulltext -> fulltext / chunked / lines."""

    _source_kind = "knit-annotated-ft-gz"


class DeltaAnnotatedToFullText(KnitAdapter):
    """Annotated delta -> fulltext / chunked / lines."""

    _source_kind = "knit-annotated-delta-gz"


class FTPlainToFullText(KnitAdapter):
    """Plain fulltext -> fulltext / chunked / lines."""

    _source_kind = "knit-ft-gz"


class DeltaPlainToFullText(KnitAdapter):
    """Plain delta -> fulltext / chunked / lines."""

    _source_kind = "knit-delta-gz"


# KnitContentFactory, KnitContent (with its get_line_delta_blocks static
# helper), LazyKnitContentFactory, and the AnnotatedKnitContent /
# PlainKnitContent concrete contents are Rust-backed and re-exported below.
# KnitContentFactory reproduces the Python constructor (network_bytes/knit) and
# get_bytes_as (native network bytes, fulltext decompression, and the knit
# delta fallback) in Rust.
from ._bzr_rs.knit import (
    AnnotatedKnitContent,
    KnitAnnotateFactory,
    KnitContent,
    KnitContentFactory,
    KnitPlainFactory,
    PlainKnitContent,
    _KndxIndex,
    _KnitGraphIndex,
    _KnitKeyAccess,
    _load_data,  # noqa: F401  re-exported for breezy's test suite
    _NetworkContentMapGenerator,  # noqa: F401  re-exported for compatibility
    _VFContentMapGenerator,
)
from ._bzr_rs.knit import KnitVersionedFiles as _KnitVersionedFilesRs


class KnitVersionedFiles(_KnitVersionedFilesRs):
    """Python view of the Rust-backed KnitVersionedFiles.

    The Rust pyclass extends the Rust ``VersionedFilesWithFallbacks`` base,
    so ``isinstance(x, VersionedFiles)`` holds without a Python mixin.
    """


__all__ = [
    "AnnotatedKnitContent",
    "KnitAnnotateFactory",
    "KnitContent",
    "KnitContentFactory",
    "KnitCorrupt",
    "KnitDataStreamIncompatible",
    "KnitDataStreamUnknown",
    "KnitHeaderError",
    "KnitIndexUnknownMethod",
    "KnitPlainFactory",
    "KnitVersionedFiles",
    "PlainKnitContent",
    "_KndxIndex",
    "_KnitAnnotator",
    "_KnitGraphIndex",
    "_KnitKeyAccess",
    "_VFContentMapGenerator",
    "annotate_knit",
    "cleanup_pack_knit",
    "knit_delta_closure_to_records",
    "knit_network_to_record",
    "make_file_factory",
    "make_pack_factory",
]


# make_file_factory, make_pack_factory, cleanup_pack_knit,
# knit_delta_closure_to_records and knit_network_to_record are implemented in
# the Rust extension; see the re-exports near the bottom of this module (after
# the _knit_rs import). The factories instantiate the Python
# KnitVersionedFiles (and _KndxIndex/_KnitGraphIndex/etc.) by importing them
# from this module.


def _get_total_build_size(self, keys, positions):
    """Determine the total bytes to build these keys.

    (helper function because _KnitGraphIndex and _KndxIndex work the same, but
    don't inherit from a common base.)

    :param keys: Keys that we want to build
    :param positions: dict of {key, (info, index_memo, comp_parent)} (such
        as returned by _get_components_positions)
    :return: Number of bytes to build those keys
    """
    return _knit_rs.get_total_build_size_rs(keys, positions)


class _KnitAnnotator(VersionedFileAnnotator):
    """Build up the annotations for a text.

    Python implementation of the knit annotator. The Rust port
    (`_KnitAnnotatorRs`) handles the public `annotate` / `annotate_flat`
    fast path; this class is preserved because callers (notably breezy's
    whitebox tests) reach into the per-step bookkeeping attributes
    `_num_compression_children`, `_content_objects`, `_pending_deltas`,
    `_pending_annotation`, `_matching_blocks`, `_all_build_details` and
    the `_expand_record` / `_process_pending` /
    `_get_parent_annotations_and_matches` helpers.
    """

    def __init__(self, vf):
        VersionedFileAnnotator.__init__(self, vf)

        self._matching_blocks = {}
        self._content_objects = {}
        self._num_compression_children = {}
        self._pending_deltas = {}
        self._pending_annotation = {}

        self._all_build_details = {}

    def _get_build_graph(self, key):
        pending = {key}
        records = []
        ann_keys = set()
        self._num_needed_children[key] = 1
        while pending:
            this_iteration = pending
            build_details = self._vf._index.get_build_details(this_iteration)
            self._all_build_details.update(build_details)
            pending = set()
            for key, details in build_details.items():
                (index_memo, compression_parent, parent_keys, _record_details) = details
                self._parent_map[key] = parent_keys
                self._heads_provider = None
                records.append((key, index_memo))
                pending.update(
                    [p for p in parent_keys if p not in self._all_build_details]
                )
                if parent_keys:
                    for parent_key in parent_keys:
                        if parent_key in self._num_needed_children:
                            self._num_needed_children[parent_key] += 1
                        else:
                            self._num_needed_children[parent_key] = 1
                if compression_parent:
                    if compression_parent in self._num_compression_children:
                        self._num_compression_children[compression_parent] += 1
                    else:
                        self._num_compression_children[compression_parent] = 1

            missing_versions = this_iteration.difference(build_details)
            if missing_versions:
                for key in missing_versions:
                    if key in self._parent_map and key in self._text_cache:
                        ann_keys.add(key)
                        parent_keys = self._parent_map[key]
                        for parent_key in parent_keys:
                            if parent_key in self._num_needed_children:
                                self._num_needed_children[parent_key] += 1
                            else:
                                self._num_needed_children[parent_key] = 1
                        pending.update(
                            [p for p in parent_keys if p not in self._all_build_details]
                        )
                    else:
                        raise RevisionNotPresent(key, self._vf)
        records.reverse()
        return records, ann_keys

    def _get_needed_texts(self, key, pb=None):
        if len(self._vf._immediate_fallback_vfs) > 0:
            yield from VersionedFileAnnotator._get_needed_texts(self, key, pb=pb)
            return
        while True:
            try:
                records, ann_keys = self._get_build_graph(key)
                for idx, (sub_key, text, num_lines) in enumerate(
                    self._extract_texts(records)
                ):
                    if pb is not None:
                        pb.update("annotating", idx, len(records))
                    yield sub_key, text, num_lines
                for sub_key in ann_keys:
                    text = self._text_cache[sub_key]
                    num_lines = len(text)
                    yield sub_key, text, num_lines
                return
            except pack_repo.RetryWithNewPacks as e:
                self._vf._access.reload_or_raise(e)
                self._all_build_details.clear()

    def _cache_delta_blocks(self, key, compression_parent, delta, lines):
        parent_lines = self._text_cache[compression_parent]
        blocks = list(KnitContent.get_line_delta_blocks(delta, parent_lines, lines))
        self._matching_blocks[(key, compression_parent)] = blocks

    def _expand_record(
        self, key, parent_keys, compression_parent, record, record_details
    ):
        delta = None
        if compression_parent:
            if compression_parent not in self._content_objects:
                self._pending_deltas.setdefault(compression_parent, []).append(
                    (key, parent_keys, record, record_details)
                )
                return None
            num = self._num_compression_children[compression_parent]
            num -= 1
            if num == 0:
                base_content = self._content_objects.pop(compression_parent)
                self._num_compression_children.pop(compression_parent)
            else:
                self._num_compression_children[compression_parent] = num
                base_content = self._content_objects[compression_parent]
            content, delta = self._vf._factory.parse_record(
                key, record, record_details, base_content, copy_base_content=True
            )
        else:
            content, _ = self._vf._factory.parse_record(
                key, record, record_details, None
            )
        if self._num_compression_children.get(key, 0) > 0:
            self._content_objects[key] = content
        lines = content.text()
        self._text_cache[key] = lines
        if delta is not None:
            self._cache_delta_blocks(key, compression_parent, delta, lines)
        return lines

    def _get_parent_annotations_and_matches(self, key, text, parent_key):
        block_key = (key, parent_key)
        if block_key in self._matching_blocks:
            blocks = self._matching_blocks.pop(block_key)
            parent_annotations = self._annotations_cache[parent_key]
            return parent_annotations, blocks
        return VersionedFileAnnotator._get_parent_annotations_and_matches(
            self, key, text, parent_key
        )

    def _process_pending(self, key):
        to_return = []
        if key in self._pending_deltas:
            compression_parent = key
            children = self._pending_deltas.pop(key)
            for child_key, parent_keys, record, record_details in children:
                self._expand_record(
                    child_key, parent_keys, compression_parent, record, record_details
                )
                if self._check_ready_for_annotations(child_key, parent_keys):
                    to_return.append(child_key)
        if key in self._pending_annotation:
            children = self._pending_annotation.pop(key)
            to_return.extend(
                [
                    c
                    for c, p_keys in children
                    if self._check_ready_for_annotations(c, p_keys)
                ]
            )
        return to_return

    def _check_ready_for_annotations(self, key, parent_keys):
        for parent_key in parent_keys:
            if parent_key not in self._annotations_cache:
                self._pending_annotation.setdefault(parent_key, []).append(
                    (key, parent_keys)
                )
                return False
        return True

    def _extract_texts(self, records):
        for key, record, _digest in self._vf._read_records_iter(records):
            details = self._all_build_details[key]
            (_, compression_parent, parent_keys, record_details) = details
            lines = self._expand_record(
                key, parent_keys, compression_parent, record, record_details
            )
            if lines is None:
                continue
            yield_this_text = self._check_ready_for_annotations(key, parent_keys)
            if yield_this_text:
                yield key, lines, len(lines)
            to_process = self._process_pending(key)
            while to_process:
                this_process = to_process
                to_process = []
                for key in this_process:
                    lines = self._text_cache[key]
                    yield key, lines, len(lines)
                    to_process.extend(self._process_pending(key))


def annotate_knit(knit, revision_id):
    """Annotate a knit with no cached annotations.

    This implementation is for knits with no cached annotations.
    It will work for knits with cached annotations, but this is not
    recommended.
    """
    annotator = _KnitAnnotator(knit)
    return iter(annotator.annotate_flat(revision_id))


from ._bzr_rs import knit as _knit_rs

# Rust-backed factory functions, network record converters and the lazy
# content factory.
knit_delta_closure_to_records = _knit_rs.knit_delta_closure_to_records
knit_network_to_record = _knit_rs.knit_network_to_record
make_file_factory = _knit_rs.make_file_factory
make_pack_factory = _knit_rs.make_pack_factory
cleanup_pack_knit = _knit_rs.cleanup_pack_knit
LazyKnitContentFactory = _knit_rs.LazyKnitContentFactory

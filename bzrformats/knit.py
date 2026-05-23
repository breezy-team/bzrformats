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

from bzrformats import pack

from . import index as _mod_index
from . import pack_repo
from .errors import (
    BzrFormatsError,
)
from .versionedfile import (
    ContentFactory,
    UnavailableRepresentation,
    VersionedFilesWithFallbacks,
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


class KnitError(BzrFormatsError):
    """Base exception for errors related to knit file operations."""

    _fmt = "Knit error"


class KnitCorrupt(KnitError):
    """Raised when a knit file is found to be corrupt."""

    _fmt = "Knit %(filename)s corrupt: %(how)s"

    def __init__(self, filename, how):
        """Initialize KnitCorrupt exception.

        Args:
            filename: The path to the corrupt knit file.
            how: Description of how the file is corrupt.
        """
        KnitError.__init__(self)
        self.filename = filename
        self.how = how


class SHA1KnitCorrupt(KnitCorrupt):
    """Raised when SHA-1 checksum validation fails for knit content."""

    _fmt = (
        "Knit %(filename)s corrupt: sha-1 of reconstructed text does not "
        "match expected sha-1. key %(key)s expected sha %(expected)s actual "
        "sha %(actual)s"
    )

    def __init__(self, filename, actual, expected, key, content):
        """Initialize SHA1KnitCorrupt exception.

        Args:
            filename: The path to the corrupt knit file.
            actual: The actual SHA-1 hash computed.
            expected: The expected SHA-1 hash.
            key: The key of the corrupt content.
            content: The content that failed validation.
        """
        KnitError.__init__(self)
        self.filename = filename
        self.actual = actual
        self.expected = expected
        self.key = key
        self.content = content


class KnitDataStreamIncompatible(KnitError):
    """Raised when attempting to insert incompatible knit data streams.

    Not raised anymore, as we can convert data streams. In future we may
    need it again for more exotic cases, so we're keeping it around for now.
    """

    _fmt = 'Cannot insert knit data stream of format "%(stream_format)s" into knit of format "%(target_format)s".'

    def __init__(self, stream_format, target_format):
        """Initialize KnitDataStreamIncompatible exception.

        Args:
            stream_format: The format of the data stream being inserted.
            target_format: The format of the target knit.
        """
        self.stream_format = stream_format
        self.target_format = target_format


class KnitDataStreamUnknown(KnitError):
    """Raised when encountering an unknown knit data stream format.

    Indicates a data stream we don't know how to handle.
    """

    _fmt = 'Cannot parse knit data stream of format "%(stream_format)s".'

    def __init__(self, stream_format):
        """Initialize KnitDataStreamUnknown exception.

        Args:
            stream_format: The unknown format of the data stream.
        """
        self.stream_format = stream_format


class KnitHeaderError(KnitError):
    """Raised when a knit file header is malformed or unexpected."""

    _fmt = 'Knit header error: %(badline)r unexpected for file "%(filename)s".'

    def __init__(self, badline, filename):
        """Initialize KnitHeaderError exception.

        Args:
            badline: The malformed header line.
            filename: The path to the knit file with the bad header.
        """
        KnitError.__init__(self)
        self.badline = badline
        self.filename = filename


class KnitIndexUnknownMethod(KnitError):
    """Raised when we don't understand the storage method.

    Currently only 'fulltext' and 'line-delta' are supported.
    """

    _fmt = (
        "Knit index %(filename)s does not have a known method in options: %(options)r"
    )

    def __init__(self, filename, options):
        """Initialize KnitIndexUnknownMethod exception.

        Args:
            filename: The path to the knit index file.
            options: The unknown options/methods found in the index.
        """
        KnitError.__init__(self)
        self.filename = filename
        self.options = options


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


class KnitContentFactory(ContentFactory):
    """Content factory for streaming from knits.

    :seealso ContentFactory:
    """

    def __init__(
        self,
        key,
        parents,
        build_details,
        sha1,
        raw_record,
        annotated,
        knit=None,
        network_bytes=None,
    ):
        """Create a KnitContentFactory for key.

        :param key: The key.
        :param parents: The parents.
        :param build_details: The build details as returned from
            get_build_details.
        :param sha1: The sha1 expected from the full text of this object.
        :param raw_record: The bytes of the knit data from disk.
        :param annotated: True if the raw data is annotated.
        :param network_bytes: None to calculate the network bytes on demand,
            not-none if they are already known.
        """
        ContentFactory.__init__(self)
        self.sha1 = sha1
        self.key = key
        self.parents = parents
        kind = "delta" if build_details[0] == "line-delta" else "ft"
        annotated_kind = "annotated-" if annotated else ""
        self.storage_kind = f"knit-{annotated_kind}{kind}-gz"
        self._raw_record = raw_record
        self._network_bytes = network_bytes
        self._build_details = build_details
        self._knit = knit

    def _create_network_bytes(self):
        """Create a fully serialised network version for transmission."""
        self._network_bytes = _knit_rs.build_network_record_rs(
            self.storage_kind,
            list(self.key),
            None if self.parents is None else [list(p) for p in self.parents],
            bool(self._build_details[1]),
            self._raw_record,
        )

    def get_bytes_as(self, storage_kind):
        """Get the bytes for this content in the specified storage format.

        Args:
            storage_kind: The desired storage format.

        Returns:
            The content bytes in the requested format.

        Raises:
            UnavailableRepresentation: If the format is not available.
        """
        if storage_kind == self.storage_kind:
            if self._network_bytes is None:
                self._create_network_bytes()
            return self._network_bytes
        if "-ft-" in self.storage_kind and storage_kind in (
            "chunked",
            "fulltext",
            "lines",
        ):
            adapter_key = (self.storage_kind, storage_kind)
            adapter_factory = adapter_registry.get(adapter_key)
            adapter = adapter_factory(None)
            return adapter.get_bytes(self, storage_kind)
        if self._knit is not None:
            # Not redundant with direct conversion above - that only handles
            # fulltext cases.
            if storage_kind in ("chunked", "lines"):
                return self._knit.get_lines(self.key[0])
            elif storage_kind == "fulltext":
                return self._knit.get_text(self.key[0])
        raise UnavailableRepresentation(self.key, storage_kind, self.storage_kind)

    def iter_bytes_as(self, storage_kind):
        """Iterate over the bytes for this content in the specified format.

        Args:
            storage_kind: The desired storage format.

        Returns:
            An iterator over the content bytes.
        """
        return iter(self.get_bytes_as(storage_kind))


class LazyKnitContentFactory(ContentFactory):
    """A ContentFactory which can either generate full text or a wire form.

    :seealso ContentFactory:
    """

    def __init__(self, key, parents, generator, first):
        """Create a LazyKnitContentFactory.

        :param key: The key of the record.
        :param parents: The parents of the record.
        :param generator: A _ContentMapGenerator containing the record for this
            key.
        :param first: Is this the first content object returned from generator?
            if it is, its storage kind is knit-delta-closure, otherwise it is
            knit-delta-closure-ref
        """
        self.key = key
        self.parents = parents
        self.sha1 = None
        self.size = None
        self._generator = generator
        self.storage_kind = "knit-delta-closure"
        if not first:
            self.storage_kind = self.storage_kind + "-ref"
        self._first = first

    def get_bytes_as(self, storage_kind):
        """Get the bytes for this lazy content in the specified storage format.

        Args:
            storage_kind: The desired storage format.

        Returns:
            The content bytes in the requested format.

        Raises:
            UnavailableRepresentation: If the format is not available.
        """
        if storage_kind == self.storage_kind:
            if self._first:
                return self._generator._wire_bytes()
            else:
                # all the keys etc are contained in the bytes returned in the
                # first record.
                return b""
        if storage_kind in ("chunked", "fulltext", "lines"):
            chunks = self._generator._get_one_work(self.key).text()
            if storage_kind in ("chunked", "lines"):
                return chunks
            else:
                return b"".join(chunks)
        raise UnavailableRepresentation(self.key, storage_kind, self.storage_kind)

    def iter_bytes_as(self, storage_kind):
        """Iterate over the bytes for this lazy content in the specified format.

        Args:
            storage_kind: The desired storage format.

        Returns:
            An iterator over the content chunks.

        Raises:
            UnavailableRepresentation: If the format is not available.
        """
        if storage_kind in ("chunked", "lines"):
            chunks = self._generator._get_one_work(self.key).text()
            return iter(chunks)
        raise UnavailableRepresentation(self.key, storage_kind, self.storage_kind)


def knit_delta_closure_to_records(storage_kind, bytes, line_end):
    """Convert a network record to a iterator over stream records.

    :param storage_kind: The storage kind of the record.
        Must be 'knit-delta-closure'.
    :param bytes: The bytes of the record on the network.
    """
    generator = _NetworkContentMapGenerator(bytes, line_end)
    return generator.get_record_stream()


def knit_network_to_record(storage_kind, bytes, line_end):
    """Convert a network record to a record object.

    :param storage_kind: The storage kind of the record.
    :param bytes: The bytes of the record on the network.
    """
    key, parents, noeol, raw_offset = _knit_rs.parse_network_record_header_rs(
        bytes, line_end
    )
    method = "fulltext" if "ft" in storage_kind else "line-delta"
    build_details = (method, noeol)
    raw_record = bytes[raw_offset:]
    annotated = "annotated" in storage_kind
    return [
        KnitContentFactory(
            key,
            parents,
            build_details,
            None,
            raw_record,
            annotated,
            network_bytes=bytes,
        )
    ]


class KnitContent:
    r"""Base class for knit content objects.

    Provides the static get_line_delta_blocks helper used by callers that
    hold a plain KnitContent reference.  The concrete implementations
    (AnnotatedKnitContent, PlainKnitContent) are backed by Rust.
    """

    @staticmethod
    def get_line_delta_blocks(knit_delta, source, target):
        """Extract SequenceMatcher.get_matching_blocks() from a knit delta."""
        yield from _knit_rs.get_line_delta_blocks_rs(knit_delta, source, target)


from ._bzr_rs.knit import (
    AnnotatedKnitContent,
    KnitAnnotateFactory,
    KnitPlainFactory,
    PlainKnitContent,
    _KndxIndex,
    _KnitAnnotator,
    _KnitGraphIndex,
    _KnitKeyAccess,
    _load_data,  # noqa: F401  re-exported for breezy's test suite
    _NetworkContentMapGenerator,
    _VFContentMapGenerator,
)
from ._bzr_rs.knit import KnitVersionedFiles as _KnitVersionedFilesRs


class KnitVersionedFiles(_KnitVersionedFilesRs, VersionedFilesWithFallbacks):
    """Python view of the Rust-backed KnitVersionedFiles.

    Inherits the Rust pyclass for storage/methods and
    `VersionedFilesWithFallbacks` so `isinstance(x, VersionedFiles)`
    holds — the Rust pyclass has `__module__ == 'builtins'` and
    cannot itself extend a pure-Python class via PyO3.
    """

__all__ = [
    "AnnotatedKnitContent",
    "KnitAnnotateFactory",
    "KnitContent",
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


def make_file_factory(annotated, mapper):
    """Create a factory for creating a file based KnitVersionedFiles.

    This is only functional enough to run interface tests, it doesn't try to
    provide a full pack environment.

    :param annotated: knit annotations are wanted.
    :param mapper: The mapper from keys to paths.
    """

    def factory(transport):
        index = _KndxIndex(transport, mapper, lambda: None, lambda: True, lambda: True)
        access = _KnitKeyAccess(transport, mapper)
        return KnitVersionedFiles(index, access, annotated=annotated)

    return factory


def make_pack_factory(graph, delta, keylength):
    """Create a factory for creating a pack based VersionedFiles.

    This is only functional enough to run interface tests, it doesn't try to
    provide a full pack environment.

    :param graph: Store a graph.
    :param delta: Delta compress contents.
    :param keylength: How long should keys be.
    """

    def factory(transport):
        parents = graph or delta
        ref_length = 0
        if graph:
            ref_length += 1
        if delta:
            ref_length += 1
            max_delta_chain = 200
        else:
            max_delta_chain = 0
        graph_index = _mod_index.InMemoryGraphIndex(
            reference_lists=ref_length, key_elements=keylength
        )
        stream = transport.open_write_stream("newpack")
        writer = pack.ContainerWriter(stream.write)
        writer.begin()
        index = _KnitGraphIndex(
            graph_index,
            lambda: True,
            parents=parents,
            deltas=delta,
            add_callback=graph_index.add_nodes,
        )
        access = pack_repo._DirectPackAccess({})
        access.set_writer(writer, graph_index, (transport, "newpack"))
        result = KnitVersionedFiles(index, access, max_delta_chain=max_delta_chain)
        result.stream = stream
        result.writer = writer
        return result

    return factory


def cleanup_pack_knit(versioned_files):
    """Clean up resources used by a pack knit versioned files instance.

    Args:
        versioned_files: The KnitVersionedFiles instance to clean up.
    """
    # writer.end() writes the trailing record marker through the same
    # stream, so it has to run before stream.close() releases the fd.
    versioned_files.writer.end()
    versioned_files.stream.close()


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


def annotate_knit(knit, revision_id):
    """Annotate a knit with no cached annotations.

    This implementation is for knits with no cached annotations.
    It will work for knits with cached annotations, but this is not
    recommended.
    """
    annotator = _KnitAnnotator(knit)
    return iter(annotator.annotate_flat(revision_id))


from ._bzr_rs import knit as _knit_rs

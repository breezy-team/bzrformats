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
    SHA1KnitCorrupt,
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


# The knit content-record adapters are Rust pyclasses. KnitAdapter is the base
# (looks up the (source, target) adapter via the Rust registry and delegates);
# the concrete adapters override _source_kind. Re-exported from the extension.
# KnitContentFactory, KnitContent (with its get_line_delta_blocks static
# helper), LazyKnitContentFactory, and the AnnotatedKnitContent /
# PlainKnitContent concrete contents are Rust-backed and re-exported below.
# KnitContentFactory reproduces the Python constructor (network_bytes/knit) and
# get_bytes_as (native network bytes, fulltext decompression, and the knit
# delta fallback) in Rust.
from ._bzr_rs.knit import (  # noqa: F401
    AnnotatedKnitContent,
    DeltaAnnotatedToFullText,
    DeltaAnnotatedToUnannotated,
    DeltaPlainToFullText,
    FTAnnotatedToFullText,
    FTAnnotatedToUnannotated,
    FTPlainToFullText,
    KnitAdapter,
    KnitAnnotateFactory,
    KnitContent,
    KnitContentFactory,
    KnitPlainFactory,
    PlainKnitContent,
    _KndxIndex,
    _KnitAnnotator,
    _KnitGraphIndex,
    _KnitKeyAccess,
    _load_data,
    _NetworkContentMapGenerator,
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


# _KnitAnnotator is implemented as a Rust pyclass extending
# VersionedFileAnnotator (it reproduces the per-step build-graph
# bookkeeping breezy's whitebox tests reach into); re-exported below.


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

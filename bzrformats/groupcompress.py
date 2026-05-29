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
from .errors import (
    BzrFormatsError,
)

logger = logging.getLogger("bzrformats.groupcompress")

_null_sha1 = _groupcompress_rs.NULL_SHA1
PythonGroupCompressor = _groupcompress_rs.TraditionalGroupCompressor
rabin_hash = _groupcompress_rs.rabin_hash

# Minimum number of uncompressed bytes to try fetch at once when retrieving
# groupcompress blocks.
BATCH_SIZE = 2**16


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

# network_block_to_records, make_pack_factory and cleanup_pack_group are
# implemented in the Rust extension; re-export them here.
network_block_to_records = _groupcompress_rs.network_block_to_records
make_pack_factory = _groupcompress_rs.make_pack_factory
cleanup_pack_group = _groupcompress_rs.cleanup_pack_group


class GroupCompressVersionedFiles(_GroupCompressVersionedFilesRs):
    """A group-compress based VersionedFiles implementation.

    The full implementation -- storage state, record streams, inserts,
    ``annotate``/``get_annotator`` and the compressor-setting class
    attributes -- lives in the Rust pyclass, which extends the Rust
    ``VersionedFilesWithFallbacks`` base so ``isinstance(x, VersionedFiles)``
    holds.
    """


from ._bzr_rs import groupcompress
from ._bzr_rs.groupcompress import GCBuildDetails as _GCBuildDetails  # noqa: F401
from ._bzr_rs.groupcompress import _GCGraphIndex  # noqa: F401

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

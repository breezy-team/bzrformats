# Copyright (C) 2005, 2009 Canonical Ltd
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

# Author: Martin Pool <mbp@canonical.com>

"""Weave - storage of related text file versions.

The pure-logic core lives in the Rust ``bazaar::weave`` crate; this
module exposes that core to Python and adds the transport-backed
``WeaveFile`` subclass plus the package-public exception classes.
"""

from ._bzr_rs import weave as _weave_rs

# The weave error hierarchy lives in the Rust errors module; re-export it here
# so ``bzrformats.weave.WeaveError`` (and friends) keep working for callers and
# for the Rust ``import_exception!(bzrformats.weave, ...)`` sites.
from .errors import (
    WeaveError,
    WeaveFormatError,
    WeaveInvalidChecksum,
    WeaveParentMismatch,
    WeaveRevisionAlreadyPresent,
    WeaveRevisionNotPresent,
    WeaveTextDiffers,
)

__all__ = [
    "Weave",
    "WeaveContentFactory",
    "WeaveError",
    "WeaveFile",
    "WeaveFormatError",
    "WeaveInvalidChecksum",
    "WeaveParentMismatch",
    "WeaveRevisionAlreadyPresent",
    "WeaveRevisionNotPresent",
    "WeaveTextDiffers",
]

# Weave, the transport-backed WeaveFile, and WeaveContentFactory are all
# Rust pyclasses (Weave extends the Rust VersionedFile base; WeaveFile extends
# Weave and routes its save-on-write through the Rust Transport adapter, which
# wraps any Python Transport object). Re-exported here for the public API.
Weave = _weave_rs.Weave
WeaveFile = _weave_rs.WeaveFile
WeaveContentFactory = _weave_rs.WeaveContentFactory

# Copyright (C) 2025 Breezy Contributors
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

"""Errors specific to bzrformats.

The exception hierarchy is implemented in Rust (``bzrformats._bzr_rs.errors``)
and re-exported here so it can be imported as ``bzrformats.errors``. The base
class ``BzrFormatsError`` provides the lazy ``_fmt % self.__dict__`` formatting
that subclasses rely on; Python code (and downstream consumers such as breezy)
can still subclass these classes and override ``_fmt``.
"""

from ._bzr_rs import errors as _errors

BzrFormatsError = _errors.BzrFormatsError
UnexpectedInventoryFormat = _errors.UnexpectedInventoryFormat
UnsupportedInventoryKind = _errors.UnsupportedInventoryKind
KnitCorrupt = _errors.KnitCorrupt
KnitDataStreamIncompatible = _errors.KnitDataStreamIncompatible
KnitDataStreamUnknown = _errors.KnitDataStreamUnknown
KnitHeaderError = _errors.KnitHeaderError
BadIndexFormatSignature = _errors.BadIndexFormatSignature
BadIndexData = _errors.BadIndexData
BadIndexDuplicateKey = _errors.BadIndexDuplicateKey
BadIndexKey = _errors.BadIndexKey
BadIndexOptions = _errors.BadIndexOptions
BadIndexValue = _errors.BadIndexValue
InvalidEntryName = _errors.InvalidEntryName
DuplicateFileId = _errors.DuplicateFileId
DecompressCorruption = _errors.DecompressCorruption
VersionedFileError = _errors.VersionedFileError
RevisionNotPresent = _errors.RevisionNotPresent
RevisionAlreadyPresent = _errors.RevisionAlreadyPresent
VersionedFileInvalidChecksum = _errors.VersionedFileInvalidChecksum
InvalidRevisionId = _errors.InvalidRevisionId
UnavailableRepresentation = _errors.UnavailableRepresentation
ExistingContent = _errors.ExistingContent
WeaveError = _errors.WeaveError
WeaveRevisionAlreadyPresent = _errors.WeaveRevisionAlreadyPresent
WeaveRevisionNotPresent = _errors.WeaveRevisionNotPresent
WeaveFormatError = _errors.WeaveFormatError
WeaveParentMismatch = _errors.WeaveParentMismatch
WeaveInvalidChecksum = _errors.WeaveInvalidChecksum
WeaveTextDiffers = _errors.WeaveTextDiffers
BadInventoryFormat = _errors.BadInventoryFormat
ReservedId = _errors.ReservedId
BadFileKindError = _errors.BadFileKindError
PathError = _errors.PathError
NoSuchFile = _errors.NoSuchFile
InvalidNormalization = _errors.InvalidNormalization
InconsistentDelta = _errors.InconsistentDelta
InconsistentDeltaDelta = _errors.InconsistentDeltaDelta
InternalBzrFormatsError = _errors.InternalBzrFormatsError
BzrCheckError = _errors.BzrCheckError
NoSuchRevision = _errors.NoSuchRevision
LockError = _errors.LockError
ObjectNotLocked = _errors.ObjectNotLocked
ReadOnlyError = _errors.ReadOnlyError
ReadOnlyObjectDirtiedError = _errors.ReadOnlyObjectDirtiedError
OutSideTransaction = _errors.OutSideTransaction
LockContention = _errors.LockContention
LockNotHeld = _errors.LockNotHeld
AlreadyVersionedError = _errors.AlreadyVersionedError
NotVersionedError = _errors.NotVersionedError

__all__ = [
    "AlreadyVersionedError",
    "BadFileKindError",
    "BadIndexData",
    "BadIndexDuplicateKey",
    "BadIndexFormatSignature",
    "BadIndexKey",
    "BadIndexOptions",
    "BadIndexValue",
    "BadInventoryFormat",
    "BzrCheckError",
    "BzrFormatsError",
    "DecompressCorruption",
    "DuplicateFileId",
    "ExistingContent",
    "InconsistentDelta",
    "InconsistentDeltaDelta",
    "InternalBzrFormatsError",
    "InvalidEntryName",
    "InvalidNormalization",
    "InvalidRevisionId",
    "KnitCorrupt",
    "KnitDataStreamIncompatible",
    "KnitDataStreamUnknown",
    "KnitHeaderError",
    "LockContention",
    "LockError",
    "LockNotHeld",
    "NoSuchFile",
    "NoSuchRevision",
    "NotVersionedError",
    "ObjectNotLocked",
    "OutSideTransaction",
    "PathError",
    "ReadOnlyError",
    "ReadOnlyObjectDirtiedError",
    "ReservedId",
    "RevisionAlreadyPresent",
    "RevisionNotPresent",
    "UnavailableRepresentation",
    "UnexpectedInventoryFormat",
    "UnsupportedInventoryKind",
    "VersionedFileError",
    "VersionedFileInvalidChecksum",
    "WeaveError",
    "WeaveFormatError",
    "WeaveInvalidChecksum",
    "WeaveParentMismatch",
    "WeaveRevisionAlreadyPresent",
    "WeaveRevisionNotPresent",
    "WeaveTextDiffers",
]

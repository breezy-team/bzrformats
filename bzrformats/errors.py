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

"""Errors specific to bzrformats."""


class BzrFormatsError(Exception):
    """Base class for errors raised by bzrformats.

    Attributes:
      internal_error: if True this was probably caused by a brz bug and
                      should be displayed with a traceback; if False (or
                      absent) this was probably a user or environment error
                      and they don't need the gory details.  (That can be
                      overridden by -Derror on the command line.)

      _fmt: Format string to display the error; this is expanded
            by the instance's dict.
    """

    internal_error = False

    def __init__(self, msg=None, **kwds):
        """Construct a new BzrFormatsError.

        There are two alternative forms for constructing these objects.
        Either a preformatted string may be passed, or a set of named
        arguments can be given.  The first is for generic "user" errors which
        are not intended to be caught and so do not need a specific subclass.
        The second case is for use with subclasses that provide a _fmt format
        string to print the arguments.

        Keyword arguments are taken as parameters to the error, which can
        be inserted into the format string template.  It's recommended
        that subclasses override the __init__ method to require specific
        parameters.

        Args:
          msg: If given, this is the literal complete text for the error, not
               subject to expansion. 'msg' is used instead of 'message' because
               python evolved and, in 2.6, forbids the use of 'message'.
        """
        Exception.__init__(self)
        if msg is not None:
            # I was going to deprecate this, but it actually turns out to be
            # quite handy - mbp 20061103.
            self._preformatted_string = msg
        else:
            self._preformatted_string = None
            for key, value in kwds.items():
                setattr(self, key, value)

    def _format(self):
        s = getattr(self, "_preformatted_string", None)
        if s is not None:
            # contains a preformatted message
            return s
        err = None
        try:
            fmt = self._get_format_string()
            if fmt:
                d = dict(self.__dict__)
                s = fmt % d
                # __str__() should always return a 'str' object
                # never a 'unicode' object.
                return s
        except Exception as e:
            err = e
        return "Unprintable exception {}: dict={!r}, fmt={!r}, error={!r}".format(
            self.__class__.__name__, self.__dict__, getattr(self, "_fmt", None), err
        )

    __str__ = _format

    def __repr__(self):
        return f"{self.__class__.__name__}({self!s})"

    def _get_format_string(self):
        """Return format string for this exception or None."""
        return getattr(self, "_fmt", None)

    def __eq__(self, other):
        if self.__class__ is not other.__class__:
            return NotImplemented
        return self.__dict__ == other.__dict__

    def __hash__(self):
        return id(self)


class UnexpectedInventoryFormat(BzrFormatsError):
    _fmt = "Unexpected inventory format: %(msg)s"

    def __init__(self, msg):
        super().__init__()
        self.msg = msg


class UnsupportedInventoryKind(BzrFormatsError):
    _fmt = "Unsupported inventory kind: %(kind)s"

    def __init__(self, kind):
        super().__init__()
        self.kind = kind


class KnitCorrupt(BzrFormatsError):
    _fmt = "Knit %(knit)s corrupt: %(how)s"

    def __init__(self, knit, how):
        super().__init__()
        self.knit = knit
        self.how = how


class KnitDataStreamIncompatible(BzrFormatsError):
    _fmt = "Cannot insert knit data stream for %(key)s: %(msg)s"

    def __init__(self, key, msg):
        super().__init__()
        self.key = key
        self.msg = msg


class KnitDataStreamUnknown(BzrFormatsError):
    _fmt = "Unknown knit data stream for %(key)s"

    def __init__(self, key):
        super().__init__()
        self.key = key


class KnitHeaderError(BzrFormatsError):
    _fmt = "Knit header error: %(badline)r"

    def __init__(self, badline):
        super().__init__()
        self.badline = badline


class DirstateCorrupt(BzrFormatsError):
    _fmt = "The dirstate file (%(state)s) appears to be corrupt: %(msg)s"

    def __init__(self, state, msg):
        super().__init__()
        self.state = state
        self.msg = msg


# Index errors
class BadIndexFormatSignature(BzrFormatsError):
    _fmt = "%(value)s is not an index of type %(_type)s."

    def __init__(self, value, _type):
        super().__init__()
        self.value = value
        self._type = _type


class BadIndexData(BzrFormatsError):
    _fmt = "Error in data for index %(value)s."

    def __init__(self, value):
        super().__init__()
        self.value = value


class BadIndexDuplicateKey(BzrFormatsError):
    _fmt = "The key '%(key)s' is already in index '%(index)s'."

    def __init__(self, key, index):
        super().__init__()
        self.key = key
        self.index = index


class BadIndexKey(BzrFormatsError):
    _fmt = "The key '%(key)s' is not a valid key."

    def __init__(self, key):
        super().__init__()
        self.key = key


class BadIndexOptions(BzrFormatsError):
    _fmt = "Could not parse options for index %(value)s."

    def __init__(self, value):
        super().__init__()
        self.value = value


class BadIndexValue(BzrFormatsError):
    _fmt = "The value '%(value)s' is not a valid value."

    def __init__(self, value):
        super().__init__()
        self.value = value


# Inventory errors
class InvalidEntryName(BzrFormatsError):
    _fmt = "Invalid entry name: %(name)s"

    def __init__(self, name):
        super().__init__()
        self.name = name


class DuplicateFileId(BzrFormatsError):
    _fmt = "File id {%(file_id)s} already exists in inventory as %(entry)s"

    def __init__(self, file_id, entry):
        super().__init__()
        self.file_id = file_id
        self.entry = entry


# Groupcompress errors
class DecompressCorruption(BzrFormatsError):
    _fmt = "Corruption while decompressing repository file%(orig_error)s"

    def __init__(self, orig_error=""):
        if orig_error:
            self.orig_error = f", {orig_error}"
        else:
            self.orig_error = ""


# Versioned file errors
class VersionedFileError(BzrFormatsError):
    """Base class for versioned file errors.

    Raised when operations on versioned files encounter problems.
    """

    _fmt = "Versioned file error"


class RevisionNotPresent(VersionedFileError):
    """Revision not present in versioned file.

    Raised when attempting to access a revision that does not exist
    in the specified versioned file.
    """

    _fmt = 'Revision {%(revision_id)s} not present in "%(file_id)s".'

    def __init__(self, revision_id, file_id):
        """Initialize with revision and file information.

        Args:
            revision_id: The revision ID that was not found.
            file_id: The file ID where the revision was not found.
        """
        super().__init__()
        self.revision_id = revision_id
        self.file_id = file_id


class RevisionAlreadyPresent(VersionedFileError):
    """Revision already present in versioned file.

    Raised when attempting to add a revision that already exists
    in the specified versioned file.
    """

    _fmt = 'Revision {%(revision_id)s} already present in "%(file_id)s".'

    def __init__(self, revision_id, file_id):
        """Initialize with revision and file information.

        Args:
            revision_id: The revision ID that is already present.
            file_id: The file ID where the revision already exists.
        """
        super().__init__()
        self.revision_id = revision_id
        self.file_id = file_id


class InvalidRevisionId(BzrFormatsError):
    """Invalid revision ID specified.

    Raised when a revision ID is not valid or not found in the branch.
    """

    _fmt = "Invalid revision-id {%(revision_id)s} in %(branch)s"

    def __init__(self, revision_id, branch):
        """Initialize with the invalid revision ID and branch.

        Args:
            revision_id: The invalid revision ID.
            branch: The branch where the revision ID was not found.
        """
        super().__init__()
        self.revision_id = revision_id
        self.branch = branch


class UnavailableRepresentation(BzrFormatsError):
    _fmt = (
        "The encoding '%(wanted)s' is not available for key %(key)s which "
        "is encoded as '%(native)s'."
    )

    def __init__(self, key, wanted, native):
        super().__init__()
        self.wanted = wanted
        self.native = native
        self.key = key


class ExistingContent(BzrFormatsError):
    _fmt = "The content being inserted is already present."


# Weave errors
class WeaveError(BzrFormatsError):
    _fmt = "Error in processing weave"


class WeaveRevisionAlreadyPresent(WeaveError):
    _fmt = "Revision {%(revision_id)s} already present in weave"

    def __init__(self, revision_id):
        super().__init__()
        self.revision_id = revision_id


class WeaveRevisionNotPresent(WeaveError):
    _fmt = "Revision {%(revision_id)s} not present in weave"

    def __init__(self, revision_id):
        super().__init__()
        self.revision_id = revision_id


class WeaveFormatError(WeaveError):
    _fmt = "Weave invariant violated: %(what)s"

    def __init__(self, what):
        super().__init__()
        self.what = what


class WeaveParentMismatch(WeaveError):
    _fmt = "Parents are mismatched between two revisions. %(message)s"


class WeaveInvalidChecksum(WeaveError):
    _fmt = "Text did not match it's checksum: %(message)s"


class WeaveTextDiffers(WeaveError):
    _fmt = (
        "Weaves differ on text content. Revision:"
        " {%(revision_id)s}, %(weave_a)s, %(weave_b)s"
    )

    def __init__(self, revision_id, weave_a, weave_b):
        super().__init__()
        self.revision_id = revision_id
        self.weave_a = weave_a
        self.weave_b = weave_b


# Serializer errors
class BadInventoryFormat(BzrFormatsError):
    _fmt = "Root tag is %(tag)r"

    def __init__(self, tag):
        super().__init__()
        self.tag = tag


class ReservedId(BzrFormatsError):
    """A revision ID that is reserved for internal use was encountered."""

    _fmt = "Reserved revision-id {%(revision_id)s}"

    def __init__(self, revision_id):
        super().__init__()
        self.revision_id = revision_id


class BadFileKindError(BzrFormatsError):
    """Cannot operate on file of unsupported kind.

    Raised when attempting to perform an operation on a file whose type
    (kind) is not supported by the current operation.
    """

    _fmt = "Cannot operate on %(filename)s of unsupported kind %(kind)s"

    def __init__(self, filename, kind):
        """Create a BadFileKindError.

        Args:
            filename: Path to the file with unsupported kind.
            kind: The unsupported file kind.
        """
        super().__init__()
        self.filename = filename
        self.kind = kind


# Transport-related errors
class PathError(BzrFormatsError):
    """Base class for path-related errors."""

    _fmt = "Path error: %(path)r%(extra)s"

    def __init__(self, path, extra=None):
        super().__init__()
        self.path = path
        if extra:
            self.extra = ": " + str(extra)
        else:
            self.extra = ""


class NoSuchFile(PathError):
    """Exception raised when a file or directory does not exist.

    This is the standard exception raised by transports when attempting
    to access a non-existent file or directory.
    """

    _fmt = "No such file: %(path)r%(extra)s"


class VersionedFileInvalidChecksum(VersionedFileError):
    """Text checksum validation failed.

    Raised when the checksum of text in a versioned file does not match
    the expected checksum, indicating data corruption.
    """

    _fmt = "Text did not match its checksum: %(msg)s"


class InconsistentDelta(BzrFormatsError):
    """Used when we get a delta that is not valid."""

    _fmt = (
        "An inconsistent delta was supplied involving %(path)r,"
        " %(file_id)r\nreason: %(reason)s"
    )

    def __init__(self, path, file_id, reason):
        """Initialize with delta inconsistency details.

        Args:
            path: The path involved in the inconsistent delta.
            file_id: The file ID involved in the inconsistent delta.
            reason: The reason why the delta is inconsistent.
        """
        super().__init__()
        self.path = path
        self.file_id = file_id
        self.reason = reason


class InconsistentDeltaDelta(InconsistentDelta):
    """Used when we get a delta that is not valid."""

    _fmt = "An inconsistent delta was supplied: %(delta)r\nreason: %(reason)s"

    def __init__(self, delta, reason):
        """Initialize with delta and inconsistency reason.

        Args:
            delta: The inconsistent delta.
            reason: The reason why the delta is inconsistent.
        """
        BzrFormatsError.__init__(self)
        self.delta = delta
        self.reason = reason


class InternalBzrFormatsError(BzrFormatsError):
    """Base class for errors that indicate a bug in bzrformats."""

    internal_error = True


class BzrCheckError(InternalBzrFormatsError):
    _fmt = "Internal check failed: %(msg)s"

    def __init__(self, msg):
        super().__init__()
        self.msg = msg


class LockError(BzrFormatsError):
    _fmt = "Lock error: %(msg)s"

    internal_error = False


class ObjectNotLocked(LockError):
    _fmt = "%(obj)r is not locked"

    def __init__(self, obj):
        super().__init__()
        self.obj = obj


class ReadOnlyError(LockError):
    _fmt = "A write attempt was made in a read only transaction on %(obj)s"

    def __init__(self, obj):
        super().__init__()
        self.obj = obj


class ReadOnlyObjectDirtiedError(ReadOnlyError):
    _fmt = "Cannot change object %(obj)r in read only transaction"


class OutSideTransaction(BzrFormatsError):
    _fmt = (
        "A transaction related operation was attempted after the transaction finished."
    )


class LockContention(LockError):
    _fmt = 'Could not acquire lock "%(lock)s": %(msg)s'

    def __init__(self, lock, msg=""):
        super().__init__()
        self.lock = lock
        self.msg = msg


class LockNotHeld(LockError):
    _fmt = "Lock not held: %(lock)s"

    def __init__(self, lock):
        super().__init__()
        self.lock = lock


class InvalidNormalization(PathError):
    _fmt = 'Path "%(path)s" is not unicode normalized'


class AlreadyVersionedError(BzrFormatsError):
    _fmt = "%(context_info)s%(path)s is already versioned."

    def __init__(self, path, context_info=None):
        super().__init__()
        self.path = path
        if context_info is None:
            self.context_info = ""
        else:
            self.context_info = context_info + ". "


class NotVersionedError(BzrFormatsError):
    _fmt = "%(context_info)s%(path)s is not versioned."

    def __init__(self, path, context_info=""):
        super().__init__()
        self.path = path
        if context_info:
            self.context_info = context_info + ". "
        else:
            self.context_info = ""


class NoSuchRevision(InternalBzrFormatsError):
    _fmt = "%(branch)s has no revision %(revision)s"

    def __init__(self, branch, revision):
        super().__init__()
        self.branch = branch
        self.revision = revision

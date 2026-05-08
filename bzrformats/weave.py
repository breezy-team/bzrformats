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

import contextlib
import os
from io import BytesIO

from ._bzr_rs import weave as _weave_rs
from .errors import (
    BzrFormatsError,
    RevisionAlreadyPresent,
    RevisionNotPresent,
)
from .transport import TransportNoSuchFile
from .versionedfile import (
    VersionedFile,
    adapter_registry,
)
from .weavefile import _read_weave_v5, write_weave_v5


class WeaveError(BzrFormatsError):
    """Base class for weave-related errors."""

    _fmt = "Error in processing weave: %(msg)s"

    def __init__(self, msg=None):
        super().__init__()
        self.msg = msg


class WeaveRevisionAlreadyPresent(WeaveError):
    """Error raised when attempting to add a revision that already exists."""

    _fmt = "Revision {%(revision_id)s} already present in %(weave)s"

    def __init__(self, revision_id, weave):
        super().__init__()
        self.revision_id = revision_id
        self.weave = weave


class WeaveRevisionNotPresent(WeaveError):
    """Error raised when requesting a revision that doesn't exist."""

    _fmt = "Revision {%(revision_id)s} not present in %(weave)s"

    def __init__(self, revision_id, weave):
        super().__init__()
        self.revision_id = revision_id
        self.weave = weave


class WeaveFormatError(WeaveError):
    """Error raised when weave format is invalid or invariants are violated."""

    _fmt = "Weave invariant violated: %(what)s"

    def __init__(self, what):
        super().__init__()
        self.what = what


class WeaveParentMismatch(WeaveError):
    """Error raised when parent information doesn't match between revisions."""

    _fmt = "Parents are mismatched between two revisions. %(msg)s"


class WeaveInvalidChecksum(WeaveError):
    """Error raised when text content doesn't match its expected checksum."""

    _fmt = "Text did not match its checksum: %(msg)s"


class WeaveTextDiffers(WeaveError):
    """Error raised when two weaves have different text content for the same revision."""

    _fmt = (
        "Weaves differ on text content. Revision:"
        " {%(revision_id)s}, %(weave_a)s, %(weave_b)s"
    )

    def __init__(self, revision_id, weave_a, weave_b):
        super().__init__()
        self.revision_id = revision_id
        self.weave_a = weave_a
        self.weave_b = weave_b


# Re-export the Rust-backed WeaveContentFactory so callers that previously
# imported it from this module keep working.
WeaveContentFactory = _weave_rs.WeaveContentFactory


class Weave(_weave_rs.Weave, VersionedFile):
    """weave - versioned text file storage.

    A Weave manages versions of line-based text files, keeping track
    of the originating version for each line.

    The pure-logic core (parent table, sha1 list, weave entry stream
    plus all algorithms over them) lives in the Rust extension module.
    This Python class adds the ``VersionedFile`` API surface and the
    adapter-driven ``insert_record_stream`` that depends on the
    Python-side adapter registry.
    """

    def __new__(cls, *args, **kwargs):
        # The Rust pyclass __new__ accepts (weave_name, access_mode,
        # get_scope, allow_reserved). Subclasses (WeaveFile) take a
        # different positional/kwargs shape, so we can't just forward
        # **kwargs blindly. Pull out the names the Rust core knows about
        # (only the ones that reach this far — first positional is the
        # name in both Weave and WeaveFile).
        weave_name = kwargs.get("weave_name")
        if weave_name is None and args:
            weave_name = args[0]
        get_scope = kwargs.get("get_scope")
        allow_reserved = kwargs.get("allow_reserved", False)
        access_mode = kwargs.get("access_mode", "w")
        return _weave_rs.Weave.__new__(
            cls, weave_name, access_mode, get_scope, allow_reserved
        )

    def __init__(
        self,
        weave_name=None,
        access_mode="w",
        matcher=None,
        get_scope=None,
        allow_reserved=False,
    ):
        # The Rust core ignores ``matcher``; the diff matcher used by
        # ``_add`` is hard-coded to ``patiencediff::SequenceMatcher`` in
        # Rust. ``matcher`` is accepted for API compatibility.
        del matcher, weave_name, access_mode, get_scope, allow_reserved
        # Pyclass __new__ already initialised the Rust state; finish off
        # the VersionedFile side.
        VersionedFile.__init__(self)

    # ------------------------------------------------------------------
    # add_lines: the public ``VersionedFile`` entry point. The base class
    # delegates to ``_add_lines``; the Rust core provides ``_add_lines``
    # directly so we just need to surface the line-validation checks the
    # base class does. We also handle the ``check_content`` opt-out.
    # ------------------------------------------------------------------

    def add_lines(
        self,
        version_id,
        parents,
        lines,
        parent_texts=None,
        left_matching_blocks=None,
        nostore_sha=None,
        random_id=False,
        check_content=True,
    ):
        """Add a single text on top of the versioned file. See VersionedFile.add_lines.

        Line validation (`_check_lines_not_unicode` + `_check_lines_are_lines`)
        is delegated to the Rust `_add_lines`, which honours the
        ``check_content`` flag.
        """
        self._check_write_ok()
        return self._add_lines(
            version_id,
            list(parents),
            lines,
            parent_texts,
            left_matching_blocks,
            nostore_sha,
            random_id,
            check_content,
        )

    # `get_record_stream` is provided by the Rust `_weave_rs.Weave` base.
    # It returns a list (vs. the previous Python generator); callers
    # treat the result as iterable, which still works.

    def insert_record_stream(self, stream):
        """See VersionedFile.insert_record_stream."""
        adapters = {}
        for record in stream:
            if record.storage_kind == "absent":
                raise RevisionNotPresent([record.key[0]], self)
            parents = [parent[0] for parent in record.parents]
            if record.storage_kind in ("fulltext", "chunked", "lines"):
                self.add_lines(record.key[0], parents, record.get_bytes_as("lines"))
            else:
                adapter_key = record.storage_kind, "lines"
                try:
                    adapter = adapters[adapter_key]
                except KeyError:
                    adapter_factory = adapter_registry.get(adapter_key)
                    adapter = adapter_factory(self)
                    adapters[adapter_key] = adapter
                lines = adapter.get_bytes(record, "lines")
                with contextlib.suppress(RevisionAlreadyPresent):
                    self.add_lines(record.key[0], parents, lines)


class WeaveFile(Weave):
    """A WeaveFile represents a Weave on disk and writes on change."""

    WEAVE_SUFFIX = ".weave"

    def __init__(
        self,
        name,
        transport,
        filemode=None,
        create=False,
        access_mode="w",
        get_scope=None,
    ):
        """Create a WeaveFile.

        :param create: If not True, only open an existing knit.
        """
        super().__init__(name, access_mode, get_scope=get_scope, allow_reserved=False)
        self._transport = transport
        self._filemode = filemode
        try:
            with self._transport.get(name + WeaveFile.WEAVE_SUFFIX) as f:
                _read_weave_v5(BytesIO(f.read()), self)
        except TransportNoSuchFile:
            if not create:
                raise
            # new file, save it
            self._save()

    def _add_lines(
        self,
        version_id,
        parents,
        lines,
        parent_texts,
        left_matching_blocks,
        nostore_sha,
        random_id,
        check_content,
    ):
        """Add a version and save the weave."""
        self.check_not_reserved_id(version_id)
        result = super()._add_lines(
            version_id,
            parents,
            lines,
            parent_texts,
            left_matching_blocks,
            nostore_sha,
            random_id,
            check_content,
        )
        self._save()
        return result

    def copy_to(self, name, transport):
        """See VersionedFile.copy_to()."""
        # as we are all in memory always, just serialise to the new place.
        sio = BytesIO()
        write_weave_v5(self, sio)
        sio.seek(0)
        transport.put_file(name + WeaveFile.WEAVE_SUFFIX, sio, self._filemode)

    def _save(self):
        """Save the weave."""
        self._check_write_ok()
        bytes = self._to_v5_bytes()
        path = self._weave_name + WeaveFile.WEAVE_SUFFIX
        try:
            self._transport.put_bytes(path, bytes, self._filemode)
        except TransportNoSuchFile:
            self._transport.mkdir(os.path.dirname(path))
            self._transport.put_bytes(path, bytes, self._filemode)

    @staticmethod
    def get_suffixes():
        """See VersionedFile.get_suffixes()."""
        return [WeaveFile.WEAVE_SUFFIX]

    def insert_record_stream(self, stream):
        """Insert records from a stream and save the weave file."""
        super().insert_record_stream(stream)
        self._save()

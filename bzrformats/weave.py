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

import os
from io import BytesIO

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
from .transport import TransportNoSuchFile
from .weavefile import _read_weave_v5, write_weave_v5

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


# Re-export the Rust-backed WeaveContentFactory so callers that previously
# imported it from this module keep working.
WeaveContentFactory = _weave_rs.WeaveContentFactory


class Weave(_weave_rs.Weave):
    """weave - versioned text file storage.

    A Weave manages versions of line-based text files, keeping track
    of the originating version for each line.

    The pure-logic core (parent table, sha1 list, weave entry stream
    plus all algorithms over them), ``add_lines``, ``get_record_stream``
    and the adapter-driven ``insert_record_stream`` all live in the Rust
    extension module, which itself extends the Rust ``VersionedFile`` base
    so ``isinstance(x, VersionedFile)`` holds.
    """

    def __new__(cls, *args, **kwargs):
        """Create a new Weave instance."""
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
        """Initialize the Weave instance.

        :param weave_name: Name of the weave.
        :param access_mode: Access mode.
        :param matcher: Matcher to use.
        :param get_scope: Scope to use.
        :param allow_reserved: Whether to allow reserved names.
        """
        # The Rust core ignores ``matcher``; the diff matcher used by
        # ``_add`` is hard-coded to ``patiencediff::SequenceMatcher`` in
        # Rust. ``matcher`` is accepted for API compatibility.
        del matcher, weave_name, access_mode, get_scope, allow_reserved
        # Pyclass __new__ already initialised the Rust state (including the
        # VersionedFile base).

    # `add_lines`, `get_record_stream` and `insert_record_stream` are
    # provided by the Rust `_weave_rs.Weave` base. `insert_record_stream`
    # consults the Python `adapter_registry` for non-fulltext records.


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

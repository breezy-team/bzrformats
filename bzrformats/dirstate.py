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

r"""DirState objects record the state of a directory and its bzr metadata.

The DirState pyclass lives in Rust (``bzrformats._bzr_rs.dirstate.DirState``);
this module just re-exports it under ``bzrformats.dirstate.DirState`` along
with the SHA1Provider interface and a handful of helper functions.

Pseudo EBNF grammar for the state file. Fields are separated by NULLs, and
lines by NL. The field delimiters are ommitted in the grammar, line delimiters
are not - this is done for clarity of reading. All string data is in utf8.

::

    MINIKIND = "f" | "d" | "l" | "a" | "r" | "t";
    NL = "\n";
    NULL = "\0";
    WHOLE_NUMBER = {digit}, digit;
    BOOLEAN = "y" | "n";
    REVISION_ID = a non-empty utf8 string;

    dirstate format = header line, full checksum, row count, parent details,
     ghost_details, entries;
    header line = "#bazaar dirstate flat format 3", NL;
    full checksum = "crc32: ", ["-"], WHOLE_NUMBER, NL;
    row count = "num_entries: ", WHOLE_NUMBER, NL;
    parent_details = WHOLE NUMBER, {REVISION_ID}* NL;
    ghost_details = WHOLE NUMBER, {REVISION_ID}*, NL;
    entries = {entry};
    entry = entry_key, current_entry_details, {parent_entry_details};
    entry_key = dirname,  basename, fileid;
    current_entry_details = common_entry_details, working_entry_details;
    parent_entry_details = common_entry_details, history_entry_details;
    common_entry_details = MINIKIND, fingerprint, size, executable
    working_entry_details = packed_stat
    history_entry_details = REVISION_ID;
    executable = BOOLEAN;
    size = WHOLE_NUMBER;
    fingerprint = a nonempty utf8 sequence with meaning defined by minikind.
"""

from .errors import BzrFormatsError

# This is the Windows equivalent of ENOTDIR
# It is defined in pywin32.winerror, but we don't want a strong dependency for
# just an error code.
ERROR_PATH_NOT_FOUND = 3
ERROR_DIRECTORY = 267


class DirstateCorrupt(BzrFormatsError):
    """Exception raised when a dirstate file is corrupt."""

    _fmt = "The dirstate file (%(state)s) appears to be corrupt: %(msg)s"

    def __init__(self, state, msg):
        """Create a DirstateCorrupt exception.

        Args:
            state: The dirstate that is corrupt.
            msg: Error message describing the corruption.
        """
        super().__init__()
        self.state = state
        self.msg = msg


class SHA1Provider:
    """An interface for getting sha1s of a file."""

    def sha1(self, abspath):
        """Return the sha1 of a file given its absolute path.

        :param abspath:  May be a filesystem encoded absolute path
             or a unicode path.
        """
        raise NotImplementedError(self.sha1)

    def stat_and_sha1(self, abspath):
        """Return the stat and sha1 of a file given its absolute path.

        :param abspath:  May be a filesystem encoded absolute path
             or a unicode path.

        Note: the stat should be the stat of the physical file
        while the sha may be the sha of its canonical content.
        """
        raise NotImplementedError(self.stat_and_sha1)


from ._bzr_rs import dirstate as _dirstate_rs
from ._bzr_rs.dirstate import DirstateInventoryChange  # noqa: F401

DirState = _dirstate_rs.DirState
DefaultSHA1Provider = _dirstate_rs.DefaultSHA1Provider
bisect_dirblock = _dirstate_rs.bisect_dirblock
bisect_path_left = _dirstate_rs.bisect_path_left
bisect_path_right = _dirstate_rs.bisect_path_right
lt_by_dirs = _dirstate_rs.lt_by_dirs
lt_path_by_dirblock = _dirstate_rs.lt_path_by_dirblock
pack_stat = _dirstate_rs.pack_stat
_fields_per_entry = _dirstate_rs.fields_per_entry
_get_ghosts_line = _dirstate_rs.get_ghosts_line
_get_parents_line = _dirstate_rs.get_parents_line
IdIndex = _dirstate_rs.IdIndex
_inv_entry_to_details = _dirstate_rs.inv_entry_to_details
_get_output_lines = _dirstate_rs.get_output_lines
_read_dirblocks = _dirstate_rs._read_dirblocks

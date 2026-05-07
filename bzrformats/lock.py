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

"""File locking for bzrformats.

The locks combine fcntl OS-level advisory locks with in-process
bookkeeping so two locks held by the same process cannot collide
unbeknownst to fcntl. The implementation is in the pure-Rust
``bazaar::lock`` crate; this module re-exports the pyo3 bindings.
"""

from ._bzr_rs.lock import (
    LogicalLockResult,
    ReadLock,
    WriteLock,
    _reset_state,
    _snapshot_state,
)

__all__ = [
    "LogicalLockResult",
    "ReadLock",
    "WriteLock",
    "_reset_state",
    "_snapshot_state",
]

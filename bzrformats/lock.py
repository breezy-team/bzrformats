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

Uses fcntl.lockf which has per-process semantics: multiple file descriptors
within the same process can share a lock on the same file.  Because of that
semantic we also maintain in-process bookkeeping so callers can detect lock
contention between lock objects living in the same interpreter (e.g. a
second DirState opening the same dirstate file read-only while the first
tries to upgrade to a write lock).
"""

import fcntl
import logging

from .errors import LockContention

logger = logging.getLogger(__name__)

# Per-filename tallies so we can detect in-process contention that fcntl's
# per-process semantics would otherwise hide.
_read_locks: dict[str, int] = {}
_write_locks: set[str] = set()


class ReadLock:
    """OS-level shared (read) lock on a file.

    The locked file is accessible via the ``f`` attribute.
    """

    def __init__(self, filename):
        """Acquire a shared read lock on *filename*."""
        self.filename = filename
        if filename in _write_locks:
            # Matches breezy's non-strict default: a read lock taken
            # while the same process already holds a write lock isn't
            # a hard error (fcntl's per-process semantics let it share
            # the descriptor).  Log and carry on; the test harness
            # relies on this to open a second reference WorkingTree
            # through the same dirstate file.
            logger.debug("Read lock taken w/ an open write lock on: %s", filename)
        self.f = open(filename, "rb")
        try:
            fcntl.lockf(self.f, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            self.f.close()
            raise LockContention(filename) from None
        except OSError:
            self.f.close()
            raise
        _read_locks[filename] = _read_locks.get(filename, 0) + 1

    def unlock(self):
        """Release the lock and close the file."""
        fcntl.lockf(self.f, fcntl.LOCK_UN)
        self.f.close()
        count = _read_locks.get(self.filename, 0)
        if count <= 1:
            _read_locks.pop(self.filename, None)
        else:
            _read_locks[self.filename] = count - 1

    def temporary_write_lock(self):
        """Try to upgrade to a write lock.

        Returns ``(True, write_lock)`` if the upgrade succeeded, or
        ``(False, self)`` if it failed (read lock is re-acquired).
        """
        # fcntl.lockf has per-process semantics: a write lock in the same
        # process would happily coexist with an unrelated read lock on the
        # same file.  Refuse the upgrade whenever another in-process reader
        # is holding the file, matching legacy breezy behaviour.
        if _read_locks.get(self.filename, 0) > 1:
            return False, self
        fcntl.lockf(self.f, fcntl.LOCK_UN)
        self.f.close()
        count = _read_locks.get(self.filename, 0)
        if count <= 1:
            _read_locks.pop(self.filename, None)
        else:
            _read_locks[self.filename] = count - 1
        try:
            wl = WriteLock(self.filename)
            return True, wl
        except (LockContention, OSError):
            # Re-acquire read lock
            self.f = open(self.filename, "rb")
            fcntl.lockf(self.f, fcntl.LOCK_SH | fcntl.LOCK_NB)
            _read_locks[self.filename] = _read_locks.get(self.filename, 0) + 1
            return False, self


class WriteLock:
    """OS-level exclusive (write) lock on a file.

    The locked file is accessible via the ``f`` attribute.
    Creates the file if it does not exist.
    """

    def __init__(self, filename):
        """Acquire an exclusive write lock on *filename*."""
        self.filename = filename
        if filename in _write_locks or _read_locks.get(filename, 0) > 0:
            raise LockContention(filename)
        try:
            self.f = open(filename, "rb+")
        except FileNotFoundError:
            self.f = open(filename, "wb+")
        try:
            fcntl.lockf(self.f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.f.close()
            raise LockContention(filename) from None
        except OSError:
            self.f.close()
            raise
        _write_locks.add(filename)

    def unlock(self):
        """Release the lock and close the file."""
        fcntl.lockf(self.f, fcntl.LOCK_UN)
        self.f.close()
        _write_locks.discard(self.filename)

    def restore_read_lock(self):
        """Downgrade to a read lock, returning a new :class:`ReadLock`."""
        fcntl.lockf(self.f, fcntl.LOCK_UN)
        self.f.close()
        _write_locks.discard(self.filename)
        return ReadLock(self.filename)


class LogicalLockResult:
    """The result of a lock_read/lock_write call.

    Can be used as a context manager::

        with tree.lock_read():
            ...
    """

    def __init__(self, unlock, token=None):
        """Initialize with an unlock callable and optional token."""
        self.unlock = unlock
        self.token = token

    def __repr__(self):
        """Return string representation."""
        return f"LogicalLockResult({self.unlock})"

    def __enter__(self):
        """Enter context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager, releasing the lock."""
        try:
            self.unlock()
        except BaseException:
            if exc_type is None:
                raise
        return False

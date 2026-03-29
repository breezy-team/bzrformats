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
within the same process can share a lock on the same file.
"""

import fcntl

from .errors import LockContention


class ReadLock:
    """OS-level shared (read) lock on a file.

    The locked file is accessible via the ``f`` attribute.
    """

    def __init__(self, filename):
        self.filename = filename
        self.f = open(filename, "rb")
        try:
            fcntl.lockf(self.f, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            self.f.close()
            raise LockContention(filename) from None
        except OSError:
            self.f.close()
            raise

    def unlock(self):
        """Release the lock and close the file."""
        fcntl.lockf(self.f, fcntl.LOCK_UN)
        self.f.close()

    def temporary_write_lock(self):
        """Try to upgrade to a write lock.

        Returns ``(True, write_lock)`` if the upgrade succeeded, or
        ``(False, self)`` if it failed (read lock is re-acquired).
        """
        fcntl.lockf(self.f, fcntl.LOCK_UN)
        self.f.close()
        try:
            wl = WriteLock(self.filename)
            return True, wl
        except (LockContention, OSError):
            # Re-acquire read lock
            self.f = open(self.filename, "rb")
            fcntl.lockf(self.f, fcntl.LOCK_SH | fcntl.LOCK_NB)
            return False, self


class WriteLock:
    """OS-level exclusive (write) lock on a file.

    The locked file is accessible via the ``f`` attribute.
    Creates the file if it does not exist.
    """

    def __init__(self, filename):
        self.filename = filename
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

    def unlock(self):
        """Release the lock and close the file."""
        fcntl.lockf(self.f, fcntl.LOCK_UN)
        self.f.close()

    def restore_read_lock(self):
        """Downgrade to a read lock, returning a new :class:`ReadLock`."""
        fcntl.lockf(self.f, fcntl.LOCK_UN)
        self.f.close()
        return ReadLock(self.filename)


class LogicalLockResult:
    """The result of a lock_read/lock_write call.

    Can be used as a context manager::

        with tree.lock_read():
            ...
    """

    def __init__(self, unlock, token=None):
        self.unlock = unlock
        self.token = token

    def __repr__(self):
        return f"LogicalLockResult({self.unlock})"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.unlock()
        except BaseException:
            if exc_type is None:
                raise
        return False

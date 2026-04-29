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

The bookkeeping is asymmetric on purpose: a *read* lock taken while the
same process already holds a *write* lock is permitted (matching breezy's
historical behaviour — fcntl's per-process semantics let it share the
descriptor anyway), but a *write* lock cannot be taken while any reader
is live.  This mirrors the upstream constraint that breezy tests rely on.
"""

import fcntl
import logging
import threading

from .errors import LockContention, LockNotHeld

logger = logging.getLogger(__name__)

# Per-filename tallies so we can detect in-process contention that fcntl's
# per-process semantics would otherwise hide.  Guarded by ``_lock_state_lock``
# so concurrent constructors cannot race past the contention check.
_lock_state_lock = threading.Lock()
_read_locks: dict[str, int] = {}
_write_locks: set[str] = set()


def _acquire_read_slot(filename):
    """Reserve a read-lock slot for ``filename``.

    Returns the new read-count for the file.  Logs (but does not refuse)
    when the same process already holds a write lock — see module docstring.
    """
    with _lock_state_lock:
        if filename in _write_locks:
            logger.debug("Read lock taken w/ an open write lock on: %s", filename)
        new_count = _read_locks.get(filename, 0) + 1
        _read_locks[filename] = new_count
        return new_count


def _release_read_slot(filename):
    """Release a previously reserved read-lock slot for ``filename``."""
    with _lock_state_lock:
        count = _read_locks.get(filename, 0)
        if count <= 1:
            _read_locks.pop(filename, None)
        else:
            _read_locks[filename] = count - 1


def _acquire_write_slot(filename):
    """Reserve a write-lock slot for ``filename``.

    Raises ``LockContention`` if any in-process reader or writer holds
    the file.
    """
    with _lock_state_lock:
        if filename in _write_locks or _read_locks.get(filename, 0) > 0:
            raise LockContention(filename)
        _write_locks.add(filename)


def _release_write_slot(filename):
    """Release a previously reserved write-lock slot for ``filename``."""
    with _lock_state_lock:
        _write_locks.discard(filename)


class ReadLock:
    """OS-level shared (read) lock on a file.

    The locked file is accessible via the ``f`` attribute.
    """

    def __init__(self, filename):
        """Acquire a shared read lock on *filename*."""
        self.filename = filename
        _acquire_read_slot(filename)
        try:
            self.f = open(filename, "rb")
        except BaseException:
            _release_read_slot(filename)
            raise
        try:
            fcntl.lockf(self.f, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            self.f.close()
            _release_read_slot(filename)
            raise LockContention(filename) from None
        except BaseException:
            self.f.close()
            _release_read_slot(filename)
            raise

    def unlock(self):
        """Release the lock and close the file."""
        if self.f is None:
            raise LockNotHeld(self.filename)
        fcntl.lockf(self.f, fcntl.LOCK_UN)
        self.f.close()
        self.f = None
        _release_read_slot(self.filename)

    def temporary_write_lock(self):
        """Try to upgrade to a write lock.

        Returns ``(True, write_lock)`` if the upgrade succeeded, or
        ``(False, self)`` if it failed (read lock is re-acquired).

        If the upgrade fails AND the read lock cannot be re-acquired,
        marks this :class:`ReadLock` as released (``self.f`` becomes
        ``None``) and raises the underlying exception so the caller
        sees the lock loss rather than silently believing they still
        hold a read lock.
        """
        # fcntl.lockf has per-process semantics: a write lock in the same
        # process would happily coexist with an unrelated read lock on the
        # same file.  Refuse the upgrade whenever another in-process reader
        # is holding the file (the count includes ourselves, so >1 means
        # somebody else is also reading).
        with _lock_state_lock:
            if _read_locks.get(self.filename, 0) > 1:
                return False, self
        # Drop our read lock before attempting the upgrade.
        fcntl.lockf(self.f, fcntl.LOCK_UN)
        self.f.close()
        self.f = None
        _release_read_slot(self.filename)
        try:
            wl = WriteLock(self.filename)
            return True, wl
        except (LockContention, OSError):
            # Re-acquire the read lock so callers' invariants still hold.
            _acquire_read_slot(self.filename)
            try:
                self.f = open(self.filename, "rb")
                fcntl.lockf(self.f, fcntl.LOCK_SH | fcntl.LOCK_NB)
            except BaseException:
                # Re-acquire failed — release the slot we just took and
                # leave self.f as None so a subsequent unlock() doesn't
                # operate on a half-acquired handle.
                if self.f is not None:
                    try:
                        self.f.close()
                    finally:
                        self.f = None
                _release_read_slot(self.filename)
                raise
            return False, self


class WriteLock:
    """OS-level exclusive (write) lock on a file.

    The locked file is accessible via the ``f`` attribute.
    Creates the file if it does not exist.
    """

    def __init__(self, filename):
        """Acquire an exclusive write lock on *filename*."""
        self.filename = filename
        _acquire_write_slot(filename)
        try:
            try:
                self.f = open(filename, "rb+")
            except FileNotFoundError:
                self.f = open(filename, "wb+")
            try:
                fcntl.lockf(self.f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                self.f.close()
                raise LockContention(filename) from None
            except BaseException:
                self.f.close()
                raise
        except BaseException:
            _release_write_slot(filename)
            raise

    def unlock(self):
        """Release the lock and close the file."""
        fcntl.lockf(self.f, fcntl.LOCK_UN)
        self.f.close()
        _release_write_slot(self.filename)

    def restore_read_lock(self):
        """Downgrade to a read lock, returning a new :class:`ReadLock`."""
        fcntl.lockf(self.f, fcntl.LOCK_UN)
        self.f.close()
        _release_write_slot(self.filename)
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

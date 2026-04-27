# Copyright (C) 2026 Breezy Contributors
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

"""Tests for in-process lock bookkeeping in :mod:`bzrformats.lock`."""

import os
import tempfile

from .. import lock
from ..errors import LockContention
from . import TestCase


class TestLockBookkeeping(TestCase):
    """Tests for ``_read_locks``/``_write_locks`` bookkeeping invariants."""

    def setUp(self):
        super().setUp()
        # Reset module-global tallies between tests so failures don't
        # poison their neighbours.
        lock._read_locks.clear()
        lock._write_locks.clear()
        self.addCleanup(lock._read_locks.clear)
        self.addCleanup(lock._write_locks.clear)
        fd, self.path = tempfile.mkstemp()
        os.close(fd)
        self.addCleanup(self._safe_unlink, self.path)

    def _safe_unlink(self, path):
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass

    def test_two_read_locks_share(self):
        a = lock.ReadLock(self.path)
        b = lock.ReadLock(self.path)
        self.assertEqual(2, lock._read_locks[self.path])
        a.unlock()
        self.assertEqual(1, lock._read_locks[self.path])
        b.unlock()
        self.assertNotIn(self.path, lock._read_locks)

    def test_write_blocks_when_reader_open(self):
        rl = lock.ReadLock(self.path)
        try:
            self.assertRaises(LockContention, lock.WriteLock, self.path)
            # Bookkeeping must be unchanged after the failed acquire.
            self.assertEqual(1, lock._read_locks[self.path])
            self.assertNotIn(self.path, lock._write_locks)
        finally:
            rl.unlock()

    def test_read_after_write_logs_but_succeeds(self):
        wl = lock.WriteLock(self.path)
        try:
            rl = lock.ReadLock(self.path)
            try:
                self.assertEqual(1, lock._read_locks[self.path])
                self.assertIn(self.path, lock._write_locks)
            finally:
                rl.unlock()
            self.assertNotIn(self.path, lock._read_locks)
        finally:
            wl.unlock()
        self.assertNotIn(self.path, lock._write_locks)

    def test_temporary_write_lock_with_other_reader(self):
        a = lock.ReadLock(self.path)
        b = lock.ReadLock(self.path)
        try:
            ok, ret = a.temporary_write_lock()
            self.assertFalse(ok)
            self.assertIs(a, ret)
            # We still hold both read locks.
            self.assertEqual(2, lock._read_locks[self.path])
        finally:
            b.unlock()
            a.unlock()

    def test_temporary_write_lock_solo_reader(self):
        a = lock.ReadLock(self.path)
        ok, wl = a.temporary_write_lock()
        try:
            self.assertTrue(ok)
            self.assertNotIn(self.path, lock._read_locks)
            self.assertIn(self.path, lock._write_locks)
        finally:
            wl.unlock() if ok else a.unlock()
        self.assertNotIn(self.path, lock._write_locks)
        self.assertNotIn(self.path, lock._read_locks)

    def test_write_lock_failure_does_not_leak(self):
        # Trigger a contention failure by holding a reader, then verify
        # bookkeeping after the failed WriteLock acquire is clean.
        rl = lock.ReadLock(self.path)
        try:
            self.assertRaises(LockContention, lock.WriteLock, self.path)
            self.assertEqual({self.path: 1}, dict(lock._read_locks))
            self.assertEqual(set(), lock._write_locks)
        finally:
            rl.unlock()

    def test_read_lock_failure_does_not_leak(self):
        # Open the file unwritable so fcntl can still grab a shared lock —
        # we instead exercise the open-failure path by pointing at a
        # non-existent file.  The constructor must not leave a stale entry
        # in _read_locks.
        bogus = self.path + ".does-not-exist"
        self.assertRaises(FileNotFoundError, lock.ReadLock, bogus)
        self.assertNotIn(bogus, lock._read_locks)

    def test_restore_read_lock_keeps_tallies_consistent(self):
        wl = lock.WriteLock(self.path)
        rl = wl.restore_read_lock()
        try:
            self.assertNotIn(self.path, lock._write_locks)
            self.assertEqual(1, lock._read_locks[self.path])
        finally:
            rl.unlock()
        self.assertNotIn(self.path, lock._read_locks)

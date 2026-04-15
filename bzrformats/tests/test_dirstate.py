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

"""Tests of the dirstate functionality being built for WorkingTreeFormat4."""

import binascii
import bisect
import os
import struct

from testscenarios import load_tests_apply_scenarios

from bzrformats import osutils

from .. import dirstate, inventory
from . import TestCase, TestCaseInTempDir, dir_reader_scenarios

# TODO:
# TESTS to write:
# general checks for NOT_IN_MEMORY error conditions.
# set_path_id on a NOT_IN_MEMORY dirstate
# set_path_id  unicode support
# set_path_id  setting id of a path not root
# set_path_id  setting id when there are parents without the id in the parents
# set_path_id  setting id when there are parents with the id in the parents
# set_path_id  setting id when state is not in memory
# set_path_id  setting id when state is in memory unmodified
# set_path_id  setting id when state is in memory modified


class TestErrors(TestCase):
    def test_dirstate_corrupt(self):
        error = dirstate.DirstateCorrupt(
            ".bzr/checkout/dirstate", 'trailing garbage: "x"'
        )
        self.assertEqualDiff(
            "The dirstate file (.bzr/checkout/dirstate)"
            ' appears to be corrupt: trailing garbage: "x"',
            str(error),
        )


load_tests = load_tests_apply_scenarios


class TestCaseWithDirState:
    """Helper methods for creating DirState objects.

    Inherit from this alongside a TestCase that provides a temp directory.
    """

    scenarios = dir_reader_scenarios()

    # Set by load_tests
    _dir_reader_class = None
    _native_to_unicode = None  # Not used yet

    def setUp(self):
        super().setUp()
        if self._dir_reader_class is None:
            self._dir_reader_class = osutils.UnicodeDirReader
        self.overrideAttr(osutils, "_selected_dir_reader", self._dir_reader_class())

    def create_empty_dirstate(self):
        """Return a locked but empty dirstate."""
        state = dirstate.DirState.initialize("dirstate")
        return state

    def create_dirstate_with_root(self):
        """Return a write-locked state with a single root entry."""
        packed_stat = b"AAAAREUHaIpFB2iKAAADAQAtkqUAAIGk"
        root_entry_direntry = (
            (b"", b"", b"a-root-value"),
            [
                (b"d", b"", 0, False, packed_stat),
            ],
        )
        dirblocks = []
        dirblocks.append((b"", [root_entry_direntry]))
        dirblocks.append((b"", []))
        state = self.create_empty_dirstate()
        try:
            state._set_data([], dirblocks)
            state._validate()
        except:
            state.unlock()
            raise
        return state

    def create_dirstate_with_root_and_subdir(self):
        """Return a locked DirState with a root and a subdir."""
        packed_stat = b"AAAAREUHaIpFB2iKAAADAQAtkqUAAIGk"
        subdir_entry = (
            (b"", b"subdir", b"subdir-id"),
            [
                (b"d", b"", 0, False, packed_stat),
            ],
        )
        state = self.create_dirstate_with_root()
        try:
            dirblocks = list(state._dirblocks)
            dirblocks[1][1].append(subdir_entry)
            state._set_data([], dirblocks)
        except:
            state.unlock()
            raise
        return state

    def create_complex_dirstate(self):
        r"""This dirstate contains multiple files and directories.

         /        a-root-value
         a/       a-dir
         b/       b-dir
         c        c-file
         d        d-file
         a/e/     e-dir
         a/f      f-file
         b/g      g-file
         b/h\xc3\xa5  h-\xc3\xa5-file  #This is u'\xe5' encoded into utf-8

        Notice that a/e is an empty directory.

        :return: The dirstate, still write-locked.
        """
        packed_stat = b"AAAAREUHaIpFB2iKAAADAQAtkqUAAIGk"
        null_sha = b"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        root_entry = (
            (b"", b"", b"a-root-value"),
            [
                (b"d", b"", 0, False, packed_stat),
            ],
        )
        a_entry = (
            (b"", b"a", b"a-dir"),
            [
                (b"d", b"", 0, False, packed_stat),
            ],
        )
        b_entry = (
            (b"", b"b", b"b-dir"),
            [
                (b"d", b"", 0, False, packed_stat),
            ],
        )
        c_entry = (
            (b"", b"c", b"c-file"),
            [
                (b"f", null_sha, 10, False, packed_stat),
            ],
        )
        d_entry = (
            (b"", b"d", b"d-file"),
            [
                (b"f", null_sha, 20, False, packed_stat),
            ],
        )
        e_entry = (
            (b"a", b"e", b"e-dir"),
            [
                (b"d", b"", 0, False, packed_stat),
            ],
        )
        f_entry = (
            (b"a", b"f", b"f-file"),
            [
                (b"f", null_sha, 30, False, packed_stat),
            ],
        )
        g_entry = (
            (b"b", b"g", b"g-file"),
            [
                (b"f", null_sha, 30, False, packed_stat),
            ],
        )
        h_entry = (
            (b"b", b"h\xc3\xa5", b"h-\xc3\xa5-file"),
            [
                (b"f", null_sha, 40, False, packed_stat),
            ],
        )
        dirblocks = []
        dirblocks.append((b"", [root_entry]))
        dirblocks.append((b"", [a_entry, b_entry, c_entry, d_entry]))
        dirblocks.append((b"a", [e_entry, f_entry]))
        dirblocks.append((b"b", [g_entry, h_entry]))
        state = dirstate.DirState.initialize("dirstate")
        state._validate()
        try:
            state._set_data([], dirblocks)
        except:
            state.unlock()
            raise
        return state

    def check_state_with_reopen(self, expected_result, state):
        """Check that state has current state expected_result.

        This will check the current state, open the file anew and check it
        again.
        This function expects the current state to be locked for writing, and
        will unlock it before re-opening.
        This is required because we can't open a lock_read() while something
        else has a lock_write().
            write => mutually exclusive lock
            read => shared lock
        """
        # The state should already be write locked, since we just had to do
        # some operation to get here.
        self.assertIsNotNone(state._lock_token)
        try:
            self.assertEqual(expected_result[0], state.get_parent_ids())
            # there should be no ghosts in this tree.
            self.assertEqual([], state.get_ghosts())
            # there should be one fileid in this tree - the root of the tree.
            self.assertEqual(expected_result[1], list(state._iter_entries()))
            state.save()
        finally:
            state.unlock()
        del state
        state = dirstate.DirState.on_file("dirstate")
        state.lock_read()
        try:
            self.assertEqual(expected_result[1], list(state._iter_entries()))
        finally:
            state.unlock()


class TestDirStateInitialize(TestCaseWithDirState, TestCaseInTempDir):
    def test_initialize(self):
        expected_result = (
            [],
            [
                (
                    (b"", b"", b"TREE_ROOT"),  # common details
                    [
                        (
                            b"d",
                            b"",
                            0,
                            False,
                            dirstate.DirState.NULLSTAT,
                        ),  # current tree
                    ],
                )
            ],
        )
        state = dirstate.DirState.initialize("dirstate")
        try:
            self.assertIsInstance(state, dirstate.DirState)
            lines = state.get_lines()
        finally:
            state.unlock()
        # On win32 you can't read from a locked file, even within the same
        # process. So we have to unlock and release before we check the file
        # contents.
        self.assertFileEqual(b"".join(lines), "dirstate")
        state.lock_read()  # check_state_with_reopen will unlock
        self.check_state_with_reopen(expected_result, state)


class TestGetLines(TestCaseWithDirState, TestCaseInTempDir):
    def test_get_line_with_2_rows(self):
        state = self.create_dirstate_with_root_and_subdir()
        try:
            self.assertEqual(
                [
                    b"#bazaar dirstate flat format 3\n",
                    b"crc32: 41262208\n",
                    b"num_entries: 2\n",
                    b"0\x00\n\x00"
                    b"0\x00\n\x00"
                    b"\x00\x00a-root-value\x00"
                    b"d\x00\x000\x00n\x00AAAAREUHaIpFB2iKAAADAQAtkqUAAIGk\x00\n\x00"
                    b"\x00subdir\x00subdir-id\x00"
                    b"d\x00\x000\x00n\x00AAAAREUHaIpFB2iKAAADAQAtkqUAAIGk\x00\n\x00",
                ],
                state.get_lines(),
            )
        finally:
            state.unlock()

    def test_entry_to_line(self):
        state = self.create_dirstate_with_root()
        try:
            self.assertEqual(
                b"\x00\x00a-root-value\x00d\x00\x000\x00n"
                b"\x00AAAAREUHaIpFB2iKAAADAQAtkqUAAIGk",
                state._entry_to_line(state._dirblocks[0][1][0]),
            )
        finally:
            state.unlock()

    def test_entry_to_line_with_parent(self):
        packed_stat = b"AAAAREUHaIpFB2iKAAADAQAtkqUAAIGk"
        root_entry = (
            (b"", b"", b"a-root-value"),
            [
                (b"d", b"", 0, False, packed_stat),  # current tree details
                # first: a pointer to the current location
                (b"a", b"dirname/basename", 0, False, b""),
            ],
        )
        state = dirstate.DirState.initialize("dirstate")
        try:
            self.assertEqual(
                b"\x00\x00a-root-value\x00"
                b"d\x00\x000\x00n\x00AAAAREUHaIpFB2iKAAADAQAtkqUAAIGk\x00"
                b"a\x00dirname/basename\x000\x00n\x00",
                state._entry_to_line(root_entry),
            )
        finally:
            state.unlock()

    def test_entry_to_line_with_two_parents_at_different_paths(self):
        # / in the tree, at / in one parent and /dirname/basename in the other.
        packed_stat = b"AAAAREUHaIpFB2iKAAADAQAtkqUAAIGk"
        root_entry = (
            (b"", b"", b"a-root-value"),
            [
                (b"d", b"", 0, False, packed_stat),  # current tree details
                (b"d", b"", 0, False, b"rev_id"),  # first parent details
                # second: a pointer to the current location
                (b"a", b"dirname/basename", 0, False, b""),
            ],
        )
        state = dirstate.DirState.initialize("dirstate")
        try:
            self.assertEqual(
                b"\x00\x00a-root-value\x00"
                b"d\x00\x000\x00n\x00AAAAREUHaIpFB2iKAAADAQAtkqUAAIGk\x00"
                b"d\x00\x000\x00n\x00rev_id\x00"
                b"a\x00dirname/basename\x000\x00n\x00",
                state._entry_to_line(root_entry),
            )
        finally:
            state.unlock()

    def test_iter_entries(self):
        # we should be able to iterate the dirstate entries from end to end
        # this is for get_lines to be easy to read.
        packed_stat = b"AAAAREUHaIpFB2iKAAADAQAtkqUAAIGk"
        dirblocks = []
        root_entries = [
            (
                (b"", b"", b"a-root-value"),
                [
                    (b"d", b"", 0, False, packed_stat),  # current tree details
                ],
            )
        ]
        dirblocks.append((b"", root_entries))
        # add two files in the root
        subdir_entry = (
            (b"", b"subdir", b"subdir-id"),
            [
                (b"d", b"", 0, False, packed_stat),  # current tree details
            ],
        )
        afile_entry = (
            (b"", b"afile", b"afile-id"),
            [
                (b"f", b"sha1value", 34, False, packed_stat),  # current tree details
            ],
        )
        dirblocks.append((b"", [subdir_entry, afile_entry]))
        # and one in subdir
        file_entry2 = (
            (b"subdir", b"2file", b"2file-id"),
            [
                (b"f", b"sha1value", 23, False, packed_stat),  # current tree details
            ],
        )
        dirblocks.append((b"subdir", [file_entry2]))
        state = dirstate.DirState.initialize("dirstate")
        try:
            state._set_data([], dirblocks)
            expected_entries = [root_entries[0], subdir_entry, afile_entry, file_entry2]
            self.assertEqual(expected_entries, list(state._iter_entries()))
        finally:
            state.unlock()


class TestGetBlockRowIndex(TestCaseWithDirState, TestCaseInTempDir):
    def assertBlockRowIndexEqual(
        self,
        block_index,
        row_index,
        dir_present,
        file_present,
        state,
        dirname,
        basename,
        tree_index,
    ):
        self.assertEqual(
            (block_index, row_index, dir_present, file_present),
            state._get_block_entry_index(dirname, basename, tree_index),
        )
        if dir_present:
            block = state._dirblocks[block_index]
            self.assertEqual(dirname, block[0])
        if dir_present and file_present:
            row = state._dirblocks[block_index][1][row_index]
            self.assertEqual(dirname, row[0][0])
            self.assertEqual(basename, row[0][1])

    def test_simple_structure(self):
        state = self.create_dirstate_with_root_and_subdir()
        self.addCleanup(state.unlock)
        self.assertBlockRowIndexEqual(1, 0, True, True, state, b"", b"subdir", 0)
        self.assertBlockRowIndexEqual(1, 0, True, False, state, b"", b"bdir", 0)
        self.assertBlockRowIndexEqual(1, 1, True, False, state, b"", b"zdir", 0)
        self.assertBlockRowIndexEqual(2, 0, False, False, state, b"a", b"foo", 0)
        self.assertBlockRowIndexEqual(2, 0, False, False, state, b"subdir", b"foo", 0)

    def test_complex_structure_exists(self):
        state = self.create_complex_dirstate()
        self.addCleanup(state.unlock)
        # Make sure we can find everything that exists
        self.assertBlockRowIndexEqual(0, 0, True, True, state, b"", b"", 0)
        self.assertBlockRowIndexEqual(1, 0, True, True, state, b"", b"a", 0)
        self.assertBlockRowIndexEqual(1, 1, True, True, state, b"", b"b", 0)
        self.assertBlockRowIndexEqual(1, 2, True, True, state, b"", b"c", 0)
        self.assertBlockRowIndexEqual(1, 3, True, True, state, b"", b"d", 0)
        self.assertBlockRowIndexEqual(2, 0, True, True, state, b"a", b"e", 0)
        self.assertBlockRowIndexEqual(2, 1, True, True, state, b"a", b"f", 0)
        self.assertBlockRowIndexEqual(3, 0, True, True, state, b"b", b"g", 0)
        self.assertBlockRowIndexEqual(3, 1, True, True, state, b"b", b"h\xc3\xa5", 0)

    def test_complex_structure_missing(self):
        state = self.create_complex_dirstate()
        self.addCleanup(state.unlock)
        # Make sure things would be inserted in the right locations
        # '_' comes before 'a'
        self.assertBlockRowIndexEqual(0, 0, True, True, state, b"", b"", 0)
        self.assertBlockRowIndexEqual(1, 0, True, False, state, b"", b"_", 0)
        self.assertBlockRowIndexEqual(1, 1, True, False, state, b"", b"aa", 0)
        self.assertBlockRowIndexEqual(1, 4, True, False, state, b"", b"h\xc3\xa5", 0)
        self.assertBlockRowIndexEqual(2, 0, False, False, state, b"_", b"a", 0)
        self.assertBlockRowIndexEqual(3, 0, False, False, state, b"aa", b"a", 0)
        self.assertBlockRowIndexEqual(4, 0, False, False, state, b"bb", b"a", 0)
        # This would be inserted between a/ and b/
        self.assertBlockRowIndexEqual(3, 0, False, False, state, b"a/e", b"a", 0)
        # Put at the end
        self.assertBlockRowIndexEqual(4, 0, False, False, state, b"e", b"a", 0)


class TestGetEntry(TestCaseWithDirState, TestCaseInTempDir):
    def assertEntryEqual(self, dirname, basename, file_id, state, path, index):
        """Check that the right entry is returned for a request to getEntry."""
        entry = state._get_entry(index, path_utf8=path)
        if file_id is None:
            self.assertEqual((None, None), entry)
        else:
            cur = entry[0]
            self.assertEqual((dirname, basename, file_id), cur[:3])

    def test_simple_structure(self):
        state = self.create_dirstate_with_root_and_subdir()
        self.addCleanup(state.unlock)
        self.assertEntryEqual(b"", b"", b"a-root-value", state, b"", 0)
        self.assertEntryEqual(b"", b"subdir", b"subdir-id", state, b"subdir", 0)
        self.assertEntryEqual(None, None, None, state, b"missing", 0)
        self.assertEntryEqual(None, None, None, state, b"missing/foo", 0)
        self.assertEntryEqual(None, None, None, state, b"subdir/foo", 0)

    def test_complex_structure_exists(self):
        state = self.create_complex_dirstate()
        self.addCleanup(state.unlock)
        self.assertEntryEqual(b"", b"", b"a-root-value", state, b"", 0)
        self.assertEntryEqual(b"", b"a", b"a-dir", state, b"a", 0)
        self.assertEntryEqual(b"", b"b", b"b-dir", state, b"b", 0)
        self.assertEntryEqual(b"", b"c", b"c-file", state, b"c", 0)
        self.assertEntryEqual(b"", b"d", b"d-file", state, b"d", 0)
        self.assertEntryEqual(b"a", b"e", b"e-dir", state, b"a/e", 0)
        self.assertEntryEqual(b"a", b"f", b"f-file", state, b"a/f", 0)
        self.assertEntryEqual(b"b", b"g", b"g-file", state, b"b/g", 0)
        self.assertEntryEqual(
            b"b", b"h\xc3\xa5", b"h-\xc3\xa5-file", state, b"b/h\xc3\xa5", 0
        )

    def test_complex_structure_missing(self):
        state = self.create_complex_dirstate()
        self.addCleanup(state.unlock)
        self.assertEntryEqual(None, None, None, state, b"_", 0)
        self.assertEntryEqual(None, None, None, state, b"_\xc3\xa5", 0)
        self.assertEntryEqual(None, None, None, state, b"a/b", 0)
        self.assertEntryEqual(None, None, None, state, b"c/d", 0)

    def test_get_entry_uninitialized(self):
        """Calling get_entry will load data if it needs to."""
        state = self.create_dirstate_with_root()
        try:
            state.save()
        finally:
            state.unlock()
        del state
        state = dirstate.DirState.on_file("dirstate")
        state.lock_read()
        try:
            self.assertEqual(dirstate.DirState.NOT_IN_MEMORY, state._header_state)
            self.assertEqual(dirstate.DirState.NOT_IN_MEMORY, state._dirblock_state)
            self.assertEntryEqual(b"", b"", b"a-root-value", state, b"", 0)
        finally:
            state.unlock()


class TestIterChildEntries(TestCaseWithDirState, TestCaseInTempDir):
    def create_dirstate_with_two_trees(self):
        r"""This dirstate contains multiple files and directories.

         /        a-root-value
         a/       a-dir
         b/       b-dir
         c        c-file
         d        d-file
         a/e/     e-dir
         a/f      f-file
         b/g      g-file
         b/h\xc3\xa5  h-\xc3\xa5-file  #This is u'\xe5' encoded into utf-8

        Notice that a/e is an empty directory.

        There is one parent tree, which has the same shape with the following variations:
        b/g in the parent is gone.
        b/h in the parent has a different id
        b/i is new in the parent
        c is renamed to b/j in the parent

        :return: The dirstate, still write-locked.
        """
        packed_stat = b"AAAAREUHaIpFB2iKAAADAQAtkqUAAIGk"
        null_sha = b"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        NULL_PARENT_DETAILS = dirstate.DirState.NULL_PARENT_DETAILS
        root_entry = (
            (b"", b"", b"a-root-value"),
            [
                (b"d", b"", 0, False, packed_stat),
                (b"d", b"", 0, False, b"parent-revid"),
            ],
        )
        a_entry = (
            (b"", b"a", b"a-dir"),
            [
                (b"d", b"", 0, False, packed_stat),
                (b"d", b"", 0, False, b"parent-revid"),
            ],
        )
        b_entry = (
            (b"", b"b", b"b-dir"),
            [
                (b"d", b"", 0, False, packed_stat),
                (b"d", b"", 0, False, b"parent-revid"),
            ],
        )
        c_entry = (
            (b"", b"c", b"c-file"),
            [
                (b"f", null_sha, 10, False, packed_stat),
                (b"r", b"b/j", 0, False, b""),
            ],
        )
        d_entry = (
            (b"", b"d", b"d-file"),
            [
                (b"f", null_sha, 20, False, packed_stat),
                (b"f", b"d", 20, False, b"parent-revid"),
            ],
        )
        e_entry = (
            (b"a", b"e", b"e-dir"),
            [
                (b"d", b"", 0, False, packed_stat),
                (b"d", b"", 0, False, b"parent-revid"),
            ],
        )
        f_entry = (
            (b"a", b"f", b"f-file"),
            [
                (b"f", null_sha, 30, False, packed_stat),
                (b"f", b"f", 20, False, b"parent-revid"),
            ],
        )
        g_entry = (
            (b"b", b"g", b"g-file"),
            [
                (b"f", null_sha, 30, False, packed_stat),
                NULL_PARENT_DETAILS,
            ],
        )
        h_entry1 = (
            (b"b", b"h\xc3\xa5", b"h-\xc3\xa5-file1"),
            [
                (b"f", null_sha, 40, False, packed_stat),
                NULL_PARENT_DETAILS,
            ],
        )
        h_entry2 = (
            (b"b", b"h\xc3\xa5", b"h-\xc3\xa5-file2"),
            [
                NULL_PARENT_DETAILS,
                (b"f", b"h", 20, False, b"parent-revid"),
            ],
        )
        i_entry = (
            (b"b", b"i", b"i-file"),
            [
                NULL_PARENT_DETAILS,
                (b"f", b"h", 20, False, b"parent-revid"),
            ],
        )
        j_entry = (
            (b"b", b"j", b"c-file"),
            [
                (b"r", b"c", 0, False, b""),
                (b"f", b"j", 20, False, b"parent-revid"),
            ],
        )
        dirblocks = []
        dirblocks.append((b"", [root_entry]))
        dirblocks.append((b"", [a_entry, b_entry, c_entry, d_entry]))
        dirblocks.append((b"a", [e_entry, f_entry]))
        dirblocks.append((b"b", [g_entry, h_entry1, h_entry2, i_entry, j_entry]))
        state = dirstate.DirState.initialize("dirstate")
        state._validate()
        try:
            state._set_data([b"parent"], dirblocks)
        except:
            state.unlock()
            raise
        return state, dirblocks

    def test_iter_children_b(self):
        state, dirblocks = self.create_dirstate_with_two_trees()
        self.addCleanup(state.unlock)
        expected_result = []
        expected_result.append(dirblocks[3][1][2])  # h2
        expected_result.append(dirblocks[3][1][3])  # i
        expected_result.append(dirblocks[3][1][4])  # j
        self.assertEqual(expected_result, list(state._iter_child_entries(1, b"b")))

    def test_iter_child_root(self):
        state, dirblocks = self.create_dirstate_with_two_trees()
        self.addCleanup(state.unlock)
        expected_result = []
        expected_result.append(dirblocks[1][1][0])  # a
        expected_result.append(dirblocks[1][1][1])  # b
        expected_result.append(dirblocks[1][1][3])  # d
        expected_result.append(dirblocks[2][1][0])  # e
        expected_result.append(dirblocks[2][1][1])  # f
        expected_result.append(dirblocks[3][1][2])  # h2
        expected_result.append(dirblocks[3][1][3])  # i
        expected_result.append(dirblocks[3][1][4])  # j
        self.assertEqual(expected_result, list(state._iter_child_entries(1, b"")))


class InstrumentedDirState(dirstate.DirState):
    """An DirState with instrumented sha1 functionality."""

    def __init__(
        self,
        path,
        sha1_provider,
        worth_saving_limit=0,
        use_filesystem_for_exec=True,
        fdatasync=False,
    ):
        super().__init__(
            path,
            sha1_provider,
            worth_saving_limit=worth_saving_limit,
            use_filesystem_for_exec=use_filesystem_for_exec,
            fdatasync=fdatasync,
        )
        self._time_offset = 0
        self._log = []
        # member is dynamically set in DirState.__init__ to turn on trace
        self._sha1_provider = sha1_provider
        self._sha1_file = self._sha1_file_and_log

    def _sha_cutoff_time(self):
        timestamp = super()._sha_cutoff_time()
        self._cutoff_time = timestamp + self._time_offset

    def _sha1_file_and_log(self, abspath):
        self._log.append(("sha1", abspath))
        return self._sha1_provider.sha1(abspath)

    def _read_link(self, abspath, old_link):
        self._log.append(("read_link", abspath, old_link))
        return super()._read_link(abspath, old_link)

    def _lstat(self, abspath, entry):
        self._log.append(("lstat", abspath))
        return super()._lstat(abspath, entry)

    def _is_executable(self, mode, old_executable):
        self._log.append(("is_exec", mode, old_executable))
        return super()._is_executable(mode, old_executable)

    def adjust_time(self, secs):
        """Move the clock forward or back.

        :param secs: The amount to adjust the clock by. Positive values make it
        seem as if we are in the future, negative values make it seem like we
        are in the past.
        """
        self._time_offset += secs
        self._cutoff_time = None


class _FakeStat:
    """A class with the same attributes as a real stat result."""

    def __init__(self, size, mtime, ctime, dev, ino, mode):
        self.st_size = size
        self.st_mtime = mtime
        self.st_ctime = ctime
        self.st_dev = dev
        self.st_ino = ino
        self.st_mode = mode

    @staticmethod
    def from_stat(st):
        return _FakeStat(
            st.st_size, st.st_mtime, st.st_ctime, st.st_dev, st.st_ino, st.st_mode
        )


class TestDiscardMergeParents(TestCaseWithDirState, TestCaseInTempDir):
    def test_discard_no_parents(self):
        # This should be a no-op
        state = self.create_empty_dirstate()
        self.addCleanup(state.unlock)
        state._discard_merge_parents()
        state._validate()

    def test_discard_one_parent(self):
        # No-op
        packed_stat = b"AAAAREUHaIpFB2iKAAADAQAtkqUAAIGk"
        root_entry_direntry = (
            (b"", b"", b"a-root-value"),
            [
                (b"d", b"", 0, False, packed_stat),
                (b"d", b"", 0, False, packed_stat),
            ],
        )
        dirblocks = []
        dirblocks.append((b"", [root_entry_direntry]))
        dirblocks.append((b"", []))

        state = self.create_empty_dirstate()
        self.addCleanup(state.unlock)
        state._set_data([b"parent-id"], dirblocks[:])
        state._validate()

        state._discard_merge_parents()
        state._validate()
        self.assertEqual(dirblocks, state._dirblocks)

    def test_discard_simple(self):
        # No-op
        packed_stat = b"AAAAREUHaIpFB2iKAAADAQAtkqUAAIGk"
        root_entry_direntry = (
            (b"", b"", b"a-root-value"),
            [
                (b"d", b"", 0, False, packed_stat),
                (b"d", b"", 0, False, packed_stat),
                (b"d", b"", 0, False, packed_stat),
            ],
        )
        expected_root_entry_direntry = (
            (b"", b"", b"a-root-value"),
            [
                (b"d", b"", 0, False, packed_stat),
                (b"d", b"", 0, False, packed_stat),
            ],
        )
        dirblocks = []
        dirblocks.append((b"", [root_entry_direntry]))
        dirblocks.append((b"", []))

        state = self.create_empty_dirstate()
        self.addCleanup(state.unlock)
        state._set_data([b"parent-id", b"merged-id"], dirblocks[:])
        state._validate()

        # This should strip of the extra column
        state._discard_merge_parents()
        state._validate()
        expected_dirblocks = [(b"", [expected_root_entry_direntry]), (b"", [])]
        self.assertEqual(expected_dirblocks, state._dirblocks)

    def test_discard_absent(self):
        """If entries are only in a merge, discard should remove the entries."""
        null_stat = dirstate.DirState.NULLSTAT
        present_dir = (b"d", b"", 0, False, null_stat)
        present_file = (b"f", b"", 0, False, null_stat)
        absent = dirstate.DirState.NULL_PARENT_DETAILS
        root_key = (b"", b"", b"a-root-value")
        file_in_root_key = (b"", b"file-in-root", b"a-file-id")
        file_in_merged_key = (b"", b"file-in-merged", b"b-file-id")
        dirblocks = [
            (b"", [(root_key, [present_dir, present_dir, present_dir])]),
            (
                b"",
                [
                    (file_in_merged_key, [absent, absent, present_file]),
                    (file_in_root_key, [present_file, present_file, present_file]),
                ],
            ),
        ]

        state = self.create_empty_dirstate()
        self.addCleanup(state.unlock)
        state._set_data([b"parent-id", b"merged-id"], dirblocks[:])
        state._validate()

        exp_dirblocks = [
            (b"", [(root_key, [present_dir, present_dir])]),
            (
                b"",
                [
                    (file_in_root_key, [present_file, present_file]),
                ],
            ),
        ]
        state._discard_merge_parents()
        state._validate()
        self.assertEqual(exp_dirblocks, state._dirblocks)

    def test_discard_renamed(self):
        null_stat = dirstate.DirState.NULLSTAT
        present_dir = (b"d", b"", 0, False, null_stat)
        present_file = (b"f", b"", 0, False, null_stat)
        absent = dirstate.DirState.NULL_PARENT_DETAILS
        root_key = (b"", b"", b"a-root-value")
        file_in_root_key = (b"", b"file-in-root", b"a-file-id")
        # Renamed relative to parent
        file_rename_s_key = (b"", b"file-s", b"b-file-id")
        file_rename_t_key = (b"", b"file-t", b"b-file-id")
        # And one that is renamed between the parents, but absent in this
        key_in_1 = (b"", b"file-in-1", b"c-file-id")
        key_in_2 = (b"", b"file-in-2", b"c-file-id")

        # Production code always writes 5-tuple relocation rows
        # ((b"r", target_path, 0, False, b"")); the test used to
        # pass 3-tuples here because Python's _dirblocks was lax
        # about the shape. Normalised to match production so the
        # Rust pyclass converter accepts it.
        dirblocks = [
            (b"", [(root_key, [present_dir, present_dir, present_dir])]),
            (
                b"",
                [
                    (
                        key_in_1,
                        [absent, present_file, (b"r", b"file-in-2", 0, False, b"")],
                    ),
                    (
                        key_in_2,
                        [absent, (b"r", b"file-in-1", 0, False, b""), present_file],
                    ),
                    (file_in_root_key, [present_file, present_file, present_file]),
                    (
                        file_rename_s_key,
                        [(b"r", b"file-t", 0, False, b""), absent, present_file],
                    ),
                    (
                        file_rename_t_key,
                        [present_file, absent, (b"r", b"file-s", 0, False, b"")],
                    ),
                ],
            ),
        ]
        exp_dirblocks = [
            (b"", [(root_key, [present_dir, present_dir])]),
            (
                b"",
                [
                    (key_in_1, [absent, present_file]),
                    (file_in_root_key, [present_file, present_file]),
                    (file_rename_t_key, [present_file, absent]),
                ],
            ),
        ]
        state = self.create_empty_dirstate()
        self.addCleanup(state.unlock)
        state._set_data([b"parent-id", b"merged-id"], dirblocks[:])
        state._validate()

        state._discard_merge_parents()
        state._validate()
        self.assertEqual(exp_dirblocks, state._dirblocks)

    def test_discard_all_subdir(self):
        null_stat = dirstate.DirState.NULLSTAT
        present_dir = (b"d", b"", 0, False, null_stat)
        present_file = (b"f", b"", 0, False, null_stat)
        absent = dirstate.DirState.NULL_PARENT_DETAILS
        root_key = (b"", b"", b"a-root-value")
        subdir_key = (b"", b"sub", b"dir-id")
        child1_key = (b"sub", b"child1", b"child1-id")
        child2_key = (b"sub", b"child2", b"child2-id")
        child3_key = (b"sub", b"child3", b"child3-id")

        dirblocks = [
            (b"", [(root_key, [present_dir, present_dir, present_dir])]),
            (b"", [(subdir_key, [present_dir, present_dir, present_dir])]),
            (
                b"sub",
                [
                    (child1_key, [absent, absent, present_file]),
                    (child2_key, [absent, absent, present_file]),
                    (child3_key, [absent, absent, present_file]),
                ],
            ),
        ]
        exp_dirblocks = [
            (b"", [(root_key, [present_dir, present_dir])]),
            (b"", [(subdir_key, [present_dir, present_dir])]),
            (b"sub", []),
        ]
        state = self.create_empty_dirstate()
        self.addCleanup(state.unlock)
        state._set_data([b"parent-id", b"merged-id"], dirblocks[:])
        state._validate()

        state._discard_merge_parents()
        state._validate()
        self.assertEqual(exp_dirblocks, state._dirblocks)


class Test_InvEntryToDetails(TestCase):
    def assertDetails(self, expected, inv_entry):
        details = dirstate._inv_entry_to_details(inv_entry)
        self.assertEqual(expected, details)
        # details should always allow join() and always be a plain str when
        # finished
        (minikind, fingerprint, _size, _executable, tree_data) = details
        self.assertIsInstance(minikind, bytes)
        self.assertIsInstance(fingerprint, bytes)
        self.assertIsInstance(tree_data, bytes)

    def test_unicode_symlink(self):
        target = "link-targ\N{EURO SIGN}t"
        inv_entry = inventory.InventoryLink(
            b"link-file-id",
            "nam\N{EURO SIGN}e",
            b"link-parent-id",
            b"link-revision-id",
            symlink_target=target,
        )
        self.assertDetails(
            (b"l", target.encode("UTF-8"), 0, False, b"link-revision-id"), inv_entry
        )


class TestSHA1Provider(TestCaseInTempDir):
    def test_sha1provider_is_an_interface(self):
        p = dirstate.SHA1Provider()
        self.assertRaises(NotImplementedError, p.sha1, "foo")
        self.assertRaises(NotImplementedError, p.stat_and_sha1, "foo")

    def test_defaultsha1provider_sha1(self):
        text = b"test\r\nwith\nall\rpossible line endings\r\n"
        self.build_tree_contents([("foo", text)])
        expected_sha = osutils.sha_string(text)
        p = dirstate.DefaultSHA1Provider()
        self.assertEqual(expected_sha, p.sha1("foo"))

    def test_defaultsha1provider_stat_and_sha1(self):
        text = b"test\r\nwith\nall\rpossible line endings\r\n"
        self.build_tree_contents([("foo", text)])
        expected_sha = osutils.sha_string(text)
        p = dirstate.DefaultSHA1Provider()
        statvalue, sha1 = p.stat_and_sha1("foo")
        self.assertEqual(len(text), statvalue.st_size)
        self.assertEqual(expected_sha, sha1)


class TestBisectDirblock(TestCase):
    """Test that bisect_dirblock() returns the expected values.

    bisect_dirblock is intended to work like bisect.bisect_left() except it
    knows it is working on dirblocks and that dirblocks are sorted by ('path',
    'to', 'foo') chunks rather than by raw 'path/to/foo'.
    """

    def assertBisect(self, dirblocks, split_dirblocks, path, *args, **kwargs):
        """Assert that bisect_split works like bisect_left on the split paths.

        :param dirblocks: A list of (path, [info]) pairs.
        :param split_dirblocks: A list of ((split, path), [info]) pairs.
        :param path: The path we are indexing.

        All other arguments will be passed along.
        """
        self.assertIsInstance(dirblocks, list)
        bisect_split_idx = dirstate.bisect_dirblock(dirblocks, path, *args, **kwargs)
        split_dirblock = (path.split(b"/"), [])
        bisect_left_idx = bisect.bisect_left(split_dirblocks, split_dirblock, *args)
        self.assertEqual(
            bisect_left_idx,
            bisect_split_idx,
            "bisect_split disagreed. {} != {} for key {!r}".format(
                bisect_left_idx, bisect_split_idx, path
            ),
        )

    def paths_to_dirblocks(self, paths):
        """Convert a list of paths into dirblock form.

        Also, ensure that the paths are in proper sorted order.
        """
        dirblocks = [(path, []) for path in paths]
        split_dirblocks = [(path.split(b"/"), []) for path in paths]
        self.assertEqual(sorted(split_dirblocks), split_dirblocks)
        return dirblocks, split_dirblocks

    def test_simple(self):
        """In the simple case it works just like bisect_left."""
        paths = [b"", b"a", b"b", b"c", b"d"]
        dirblocks, split_dirblocks = self.paths_to_dirblocks(paths)
        for path in paths:
            self.assertBisect(dirblocks, split_dirblocks, path)
        self.assertBisect(dirblocks, split_dirblocks, b"_")
        self.assertBisect(dirblocks, split_dirblocks, b"aa")
        self.assertBisect(dirblocks, split_dirblocks, b"bb")
        self.assertBisect(dirblocks, split_dirblocks, b"cc")
        self.assertBisect(dirblocks, split_dirblocks, b"dd")
        self.assertBisect(dirblocks, split_dirblocks, b"a/a")
        self.assertBisect(dirblocks, split_dirblocks, b"b/b")
        self.assertBisect(dirblocks, split_dirblocks, b"c/c")
        self.assertBisect(dirblocks, split_dirblocks, b"d/d")

    def test_involved(self):
        """This is where bisect_left diverges slightly."""
        paths = [
            b"",
            b"a",
            b"a/a",
            b"a/a/a",
            b"a/a/z",
            b"a/a-a",
            b"a/a-z",
            b"a/z",
            b"a/z/a",
            b"a/z/z",
            b"a/z-a",
            b"a/z-z",
            b"a-a",
            b"a-z",
            b"z",
            b"z/a/a",
            b"z/a/z",
            b"z/a-a",
            b"z/a-z",
            b"z/z",
            b"z/z/a",
            b"z/z/z",
            b"z/z-a",
            b"z/z-z",
            b"z-a",
            b"z-z",
        ]
        dirblocks, split_dirblocks = self.paths_to_dirblocks(paths)
        for path in paths:
            self.assertBisect(dirblocks, split_dirblocks, path)

    def test_involved_cached(self):
        """This is where bisect_left diverges slightly."""
        paths = [
            b"",
            b"a",
            b"a/a",
            b"a/a/a",
            b"a/a/z",
            b"a/a-a",
            b"a/a-z",
            b"a/z",
            b"a/z/a",
            b"a/z/z",
            b"a/z-a",
            b"a/z-z",
            b"a-a",
            b"a-z",
            b"z",
            b"z/a/a",
            b"z/a/z",
            b"z/a-a",
            b"z/a-z",
            b"z/z",
            b"z/z/a",
            b"z/z/z",
            b"z/z-a",
            b"z/z-z",
            b"z-a",
            b"z-z",
        ]
        cache = {}
        dirblocks, split_dirblocks = self.paths_to_dirblocks(paths)
        for path in paths:
            self.assertBisect(dirblocks, split_dirblocks, path, cache=cache)


def _unpack_stat(packed_stat):
    """Turn a packed_stat back into the stat fields.

    This is meant as a debugging tool, should not be used in real code.
    """
    (st_size, st_mtime, st_ctime, st_dev, st_ino, st_mode) = struct.unpack(
        ">6L", binascii.a2b_base64(packed_stat)
    )
    return {
        "st_size": st_size,
        "st_mtime": st_mtime,
        "st_ctime": st_ctime,
        "st_dev": st_dev,
        "st_ino": st_ino,
        "st_mode": st_mode,
    }


class TestPackStatRobust(TestCase):
    """Check packed representaton of stat values is robust on all inputs."""

    def pack(self, statlike_tuple):
        return dirstate.pack_stat(os.stat_result(statlike_tuple))

    @staticmethod
    def unpack_field(packed_string, stat_field):
        return _unpack_stat(packed_string)[stat_field]

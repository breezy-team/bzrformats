# Copyright (C) 2007-2011 Canonical Ltd
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

"""Tests for the compiled dirstate helpers."""

import bisect
import os

from testscenarios import load_tests_apply_scenarios

from .. import _dirstate_helpers_py, dirstate
from .._bzr_rs import dirstate as _dirstate_rs
from . import TestCase

load_tests = load_tests_apply_scenarios

helper_scenarios = [("dirstate_Python", {"helpers": _dirstate_helpers_py})]


class TestBisectPathMixin:
    """Test that _bisect_path_*() returns the expected values.

    _bisect_path_* is intended to work like bisect.bisect_*() except it
    knows it is working on paths that are sorted by ('path', 'to', 'foo')
    chunks rather than by raw 'path/to/foo'.

    Test Cases should inherit from this and override ``get_bisect_path`` return
    their implementation, and ``get_bisect`` to return the matching
    bisect.bisect_* function.
    """

    def get_bisect_path(self):
        """Return an implementation of _bisect_path_*."""
        raise NotImplementedError

    def get_bisect(self):
        """Return a version of bisect.bisect_*.

        Also, for the 'exists' check, return the offset to the real values.
        For example bisect_left returns the index of an entry, while
        bisect_right returns the index *after* an entry

        :return: (bisect_func, offset)
        """
        raise NotImplementedError

    def assertBisect(self, paths, split_paths, path, exists=True):
        """Assert that bisect_split works like bisect_left on the split paths.

        :param paths: A list of path names
        :param split_paths: A list of path names that are already split up by directory
            ('path/to/foo' => ('path', 'to', 'foo'))
        :param path: The path we are indexing.
        :param exists: The path should be present, so make sure the
            final location actually points to the right value.

        All other arguments will be passed along.
        """
        bisect_path = self.get_bisect_path()
        self.assertIsInstance(paths, list)
        bisect_path_idx = bisect_path(paths, path)
        split_path = self.split_for_dirblocks([path])[0]
        bisect_func, offset = self.get_bisect()
        bisect_split_idx = bisect_func(split_paths, split_path)
        self.assertEqual(
            bisect_split_idx,
            bisect_path_idx,
            "{} disagreed. {} != {} for key {!r}".format(
                bisect_path.__name__, bisect_split_idx, bisect_path_idx, path
            ),
        )
        if exists:
            self.assertEqual(path, paths[bisect_path_idx + offset])

    def split_for_dirblocks(self, paths):
        dir_split_paths = []
        for path in paths:
            dirname, basename = os.path.split(path)
            dir_split_paths.append((dirname.split(b"/"), basename))
        dir_split_paths.sort()
        return dir_split_paths

    def test_simple(self):
        """In the simple case it works just like bisect_left."""
        paths = [b"", b"a", b"b", b"c", b"d"]
        split_paths = self.split_for_dirblocks(paths)
        for path in paths:
            self.assertBisect(paths, split_paths, path, exists=True)
        self.assertBisect(paths, split_paths, b"_", exists=False)
        self.assertBisect(paths, split_paths, b"aa", exists=False)
        self.assertBisect(paths, split_paths, b"bb", exists=False)
        self.assertBisect(paths, split_paths, b"cc", exists=False)
        self.assertBisect(paths, split_paths, b"dd", exists=False)
        self.assertBisect(paths, split_paths, b"a/a", exists=False)
        self.assertBisect(paths, split_paths, b"b/b", exists=False)
        self.assertBisect(paths, split_paths, b"c/c", exists=False)
        self.assertBisect(paths, split_paths, b"d/d", exists=False)

    def test_involved(self):
        """This is where bisect_path_* diverges slightly."""
        # This is the list of paths and their contents
        # a/
        #   a/
        #     a
        #     z
        #   a-a/
        #     a
        #   a-z/
        #     z
        #   a=a/
        #     a
        #   a=z/
        #     z
        #   z/
        #     a
        #     z
        #   z-a
        #   z-z
        #   z=a
        #   z=z
        # a-a/
        #   a
        # a-z/
        #   z
        # a=a/
        #   a
        # a=z/
        #   z
        # This is the exact order that is stored by dirstate
        # All children in a directory are mentioned before an children of
        # children are mentioned.
        # So all the root-directory paths, then all the
        # first sub directory, etc.
        paths = [  # content of '/'
            b"",
            b"a",
            b"a-a",
            b"a-z",
            b"a=a",
            b"a=z",
            # content of 'a/'
            b"a/a",
            b"a/a-a",
            b"a/a-z",
            b"a/a=a",
            b"a/a=z",
            b"a/z",
            b"a/z-a",
            b"a/z-z",
            b"a/z=a",
            b"a/z=z",
            # content of 'a/a/'
            b"a/a/a",
            b"a/a/z",
            # content of 'a/a-a'
            b"a/a-a/a",
            # content of 'a/a-z'
            b"a/a-z/z",
            # content of 'a/a=a'
            b"a/a=a/a",
            # content of 'a/a=z'
            b"a/a=z/z",
            # content of 'a/z/'
            b"a/z/a",
            b"a/z/z",
            # content of 'a-a'
            b"a-a/a",
            # content of 'a-z'
            b"a-z/z",
            # content of 'a=a'
            b"a=a/a",
            # content of 'a=z'
            b"a=z/z",
        ]
        split_paths = self.split_for_dirblocks(paths)
        sorted_paths = []
        for dir_parts, basename in split_paths:
            if dir_parts == [b""]:
                sorted_paths.append(basename)
            else:
                sorted_paths.append(b"/".join(dir_parts + [basename]))

        self.assertEqual(sorted_paths, paths)

        for path in paths:
            self.assertBisect(paths, split_paths, path, exists=True)


class TestBisectPathLeft(TestCase, TestBisectPathMixin):
    """Run all Bisect Path tests against bisect_path_left."""

    def get_bisect_path(self):
        from ..dirstate import bisect_path_left

        return bisect_path_left

    def get_bisect(self):
        return bisect.bisect_left, 0


class TestBisectPathRight(TestCase, TestBisectPathMixin):
    """Run all Bisect Path tests against bisect_path_right."""

    def get_bisect_path(self):
        from ..dirstate import bisect_path_right

        return bisect_path_right

    def get_bisect(self):
        return bisect.bisect_right, -1


class TestLtByDirs(TestCase):
    """Test an implementation of lt_by_dirs().

    lt_by_dirs() compares 2 paths by their directory sections, rather than as
    plain strings.
    """

    def assertCmpByDirs(self, expected, str1, str2):
        """Compare the two strings, in both directions.

        :param expected: The expected comparison value. -1 means str1 comes
            first, 0 means they are equal, 1 means str2 comes first
        :param str1: string to compare
        :param str2: string to compare
        """
        if expected == 0:
            self.assertEqual(str1, str2)
            self.assertFalse(dirstate.lt_by_dirs(str1, str2))
            self.assertFalse(dirstate.lt_by_dirs(str2, str1))
        elif expected > 0:
            self.assertFalse(dirstate.lt_by_dirs(str1, str2))
            self.assertTrue(dirstate.lt_by_dirs(str2, str1))
        else:
            self.assertTrue(dirstate.lt_by_dirs(str1, str2))
            self.assertFalse(dirstate.lt_by_dirs(str2, str1))

    def test_cmp_empty(self):
        """Compare against the empty string."""
        self.assertCmpByDirs(0, b"", b"")
        self.assertCmpByDirs(1, b"a", b"")
        self.assertCmpByDirs(1, b"ab", b"")
        self.assertCmpByDirs(1, b"abc", b"")
        self.assertCmpByDirs(1, b"abcd", b"")
        self.assertCmpByDirs(1, b"abcde", b"")
        self.assertCmpByDirs(1, b"abcdef", b"")
        self.assertCmpByDirs(1, b"abcdefg", b"")
        self.assertCmpByDirs(1, b"abcdefgh", b"")
        self.assertCmpByDirs(1, b"abcdefghi", b"")
        self.assertCmpByDirs(1, b"test/ing/a/path/", b"")

    def test_cmp_same_str(self):
        """Compare the same string."""
        self.assertCmpByDirs(0, b"a", b"a")
        self.assertCmpByDirs(0, b"ab", b"ab")
        self.assertCmpByDirs(0, b"abc", b"abc")
        self.assertCmpByDirs(0, b"abcd", b"abcd")
        self.assertCmpByDirs(0, b"abcde", b"abcde")
        self.assertCmpByDirs(0, b"abcdef", b"abcdef")
        self.assertCmpByDirs(0, b"abcdefg", b"abcdefg")
        self.assertCmpByDirs(0, b"abcdefgh", b"abcdefgh")
        self.assertCmpByDirs(0, b"abcdefghi", b"abcdefghi")
        self.assertCmpByDirs(0, b"testing a long string", b"testing a long string")
        self.assertCmpByDirs(0, b"x" * 10000, b"x" * 10000)
        self.assertCmpByDirs(0, b"a/b", b"a/b")
        self.assertCmpByDirs(0, b"a/b/c", b"a/b/c")
        self.assertCmpByDirs(0, b"a/b/c/d", b"a/b/c/d")
        self.assertCmpByDirs(0, b"a/b/c/d/e", b"a/b/c/d/e")

    def test_simple_paths(self):
        """Compare strings that act like normal string comparison."""
        self.assertCmpByDirs(-1, b"a", b"b")
        self.assertCmpByDirs(-1, b"aa", b"ab")
        self.assertCmpByDirs(-1, b"ab", b"bb")
        self.assertCmpByDirs(-1, b"aaa", b"aab")
        self.assertCmpByDirs(-1, b"aab", b"abb")
        self.assertCmpByDirs(-1, b"abb", b"bbb")
        self.assertCmpByDirs(-1, b"aaaa", b"aaab")
        self.assertCmpByDirs(-1, b"aaab", b"aabb")
        self.assertCmpByDirs(-1, b"aabb", b"abbb")
        self.assertCmpByDirs(-1, b"abbb", b"bbbb")
        self.assertCmpByDirs(-1, b"aaaaa", b"aaaab")
        self.assertCmpByDirs(-1, b"a/a", b"a/b")
        self.assertCmpByDirs(-1, b"a/b", b"b/b")
        self.assertCmpByDirs(-1, b"a/a/a", b"a/a/b")
        self.assertCmpByDirs(-1, b"a/a/b", b"a/b/b")
        self.assertCmpByDirs(-1, b"a/b/b", b"b/b/b")
        self.assertCmpByDirs(-1, b"a/a/a/a", b"a/a/a/b")
        self.assertCmpByDirs(-1, b"a/a/a/b", b"a/a/b/b")
        self.assertCmpByDirs(-1, b"a/a/b/b", b"a/b/b/b")
        self.assertCmpByDirs(-1, b"a/b/b/b", b"b/b/b/b")
        self.assertCmpByDirs(-1, b"a/a/a/a/a", b"a/a/a/a/b")

    def test_tricky_paths(self):
        self.assertCmpByDirs(1, b"ab/cd/ef", b"ab/cc/ef")
        self.assertCmpByDirs(1, b"ab/cd/ef", b"ab/c/ef")
        self.assertCmpByDirs(-1, b"ab/cd/ef", b"ab/cd-ef")
        self.assertCmpByDirs(-1, b"ab/cd", b"ab/cd-")
        self.assertCmpByDirs(-1, b"ab/cd", b"ab-cd")

    def test_cmp_non_ascii(self):
        self.assertCmpByDirs(-1, b"\xc2\xb5", b"\xc3\xa5")  # u'\xb5', u'\xe5'
        self.assertCmpByDirs(-1, b"a", b"\xc3\xa5")  # u'a', u'\xe5'
        self.assertCmpByDirs(-1, b"b", b"\xc2\xb5")  # u'b', u'\xb5'
        self.assertCmpByDirs(-1, b"a/b", b"a/\xc3\xa5")  # u'a/b', u'a/\xe5'
        self.assertCmpByDirs(-1, b"b/a", b"b/\xc2\xb5")  # u'b/a', u'b/\xb5'


class TestLtPathByDirblock(TestCase):
    """Test an implementation of lt_path_by_dirblock().

    lt_path_by_dirblock() compares two paths using the sort order used by
    DirState. All paths in the same directory are sorted together.

    Child test cases can override ``get_lt_path_by_dirblock`` to test a specific
    implementation.
    """

    def get_lt_path_by_dirblock(self):
        """Get a specific implementation of lt_path_by_dirblock."""
        from ..dirstate import lt_path_by_dirblock

        return lt_path_by_dirblock

    def assertLtPathByDirblock(self, paths):
        """Compare all paths and make sure they evaluate to the correct order.

        This does N^2 comparisons. It is assumed that ``paths`` is properly
        sorted list.

        :param paths: a sorted list of paths to compare
        """

        # First, make sure the paths being passed in are correct
        def _key(p):
            dirname, basename = os.path.split(p)
            return dirname.split(b"/"), basename

        self.assertEqual(sorted(paths, key=_key), paths)

        lt_path_by_dirblock = self.get_lt_path_by_dirblock()
        for idx1, path1 in enumerate(paths):
            for idx2, path2 in enumerate(paths):
                lt_result = lt_path_by_dirblock(path1, path2)
                self.assertEqual(
                    idx1 < idx2,
                    lt_result,
                    "{} did not state that {!r} < {!r}, lt={}".format(
                        lt_path_by_dirblock.__name__, path1, path2, lt_result
                    ),
                )

    def test_cmp_simple_paths(self):
        """Compare against the empty string."""
        self.assertLtPathByDirblock([b"", b"a", b"ab", b"abc", b"a/b/c", b"b/d/e"])
        self.assertLtPathByDirblock([b"kl", b"ab/cd", b"ab/ef", b"gh/ij"])

    def test_tricky_paths(self):
        self.assertLtPathByDirblock(
            [
                # Contents of ''
                b"",
                b"a",
                b"a-a",
                b"a=a",
                b"b",
                # Contents of 'a'
                b"a/a",
                b"a/a-a",
                b"a/a=a",
                b"a/b",
                # Contents of 'a/a'
                b"a/a/a",
                b"a/a/a-a",
                b"a/a/a=a",
                # Contents of 'a/a/a'
                b"a/a/a/a",
                b"a/a/a/b",
                # Contents of 'a/a/a-a',
                b"a/a/a-a/a",
                b"a/a/a-a/b",
                # Contents of 'a/a/a=a',
                b"a/a/a=a/a",
                b"a/a/a=a/b",
                # Contents of 'a/a-a'
                b"a/a-a/a",
                # Contents of 'a/a-a/a'
                b"a/a-a/a/a",
                b"a/a-a/a/b",
                # Contents of 'a/a=a'
                b"a/a=a/a",
                # Contents of 'a/b'
                b"a/b/a",
                b"a/b/b",
                # Contents of 'a-a',
                b"a-a/a",
                b"a-a/b",
                # Contents of 'a=a',
                b"a=a/a",
                b"a=a/b",
                # Contents of 'b',
                b"b/a",
                b"b/b",
            ]
        )
        self.assertLtPathByDirblock(
            [
                # content of '/'
                b"",
                b"a",
                b"a-a",
                b"a-z",
                b"a=a",
                b"a=z",
                # content of 'a/'
                b"a/a",
                b"a/a-a",
                b"a/a-z",
                b"a/a=a",
                b"a/a=z",
                b"a/z",
                b"a/z-a",
                b"a/z-z",
                b"a/z=a",
                b"a/z=z",
                # content of 'a/a/'
                b"a/a/a",
                b"a/a/z",
                # content of 'a/a-a'
                b"a/a-a/a",
                # content of 'a/a-z'
                b"a/a-z/z",
                # content of 'a/a=a'
                b"a/a=a/a",
                # content of 'a/a=z'
                b"a/a=z/z",
                # content of 'a/z/'
                b"a/z/a",
                b"a/z/z",
                # content of 'a-a'
                b"a-a/a",
                # content of 'a-z'
                b"a-z/z",
                # content of 'a=a'
                b"a=a/a",
                # content of 'a=z'
                b"a=z/z",
            ]
        )

    def test_nonascii(self):
        self.assertLtPathByDirblock(
            [
                # content of '/'
                b"",
                b"a",
                b"\xc2\xb5",
                b"\xc3\xa5",
                # content of 'a'
                b"a/a",
                b"a/\xc2\xb5",
                b"a/\xc3\xa5",
                # content of 'a/a'
                b"a/a/a",
                b"a/a/\xc2\xb5",
                b"a/a/\xc3\xa5",
                # content of 'a/\xc2\xb5'
                b"a/\xc2\xb5/a",
                b"a/\xc2\xb5/\xc2\xb5",
                b"a/\xc2\xb5/\xc3\xa5",
                # content of 'a/\xc3\xa5'
                b"a/\xc3\xa5/a",
                b"a/\xc3\xa5/\xc2\xb5",
                b"a/\xc3\xa5/\xc3\xa5",
                # content of '\xc2\xb5'
                b"\xc2\xb5/a",
                b"\xc2\xb5/\xc2\xb5",
                b"\xc2\xb5/\xc3\xa5",
                # content of '\xc2\xe5'
                b"\xc3\xa5/a",
                b"\xc3\xa5/\xc2\xb5",
                b"\xc3\xa5/\xc3\xa5",
            ]
        )


class TestUsingCompiledIfAvailable(TestCase):
    """Check that the Rust functions are being used as the default."""

    def test__read_dirblocks(self):
        self.assertIs(_dirstate_rs._read_dirblocks, dirstate._read_dirblocks)

    def test_update_entry(self):
        self.assertIs(_dirstate_rs.update_entry, dirstate.update_entry)

    def test_process_entry(self):
        self.assertIs(_dirstate_rs.ProcessEntryC, dirstate._process_entry)

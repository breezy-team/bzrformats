# Copyright (C) 2005-2012, 2016 Canonical Ltd
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

"""Tests for bzrformats osutils."""

import hashlib
import os

from .. import osutils
from . import TestCase, TestCaseInTempDir


class TestShaFunctions(TestCase):
    """Test the sha_string and sha_strings functions."""

    def test_sha_string_bytes(self):
        """Test sha_string with bytes input."""
        result = osutils.sha_string(b"hello world")
        expected = hashlib.sha1(b"hello world").hexdigest().encode("ascii")  # noqa: S324
        self.assertEqual(expected, result)

    def test_sha_string_unicode(self):
        """Test sha_string with unicode input."""
        result = osutils.sha_string("hello world")
        expected = hashlib.sha1(b"hello world").hexdigest().encode("ascii")  # noqa: S324
        self.assertEqual(expected, result)

    def test_sha_strings(self):
        """Test sha_strings with mixed input."""
        result = osutils.sha_strings([b"hello", " ", "world"])
        sha = hashlib.sha1()  # noqa: S324
        sha.update(b"hello")
        sha.update(b" ")
        sha.update(b"world")
        expected = sha.hexdigest().encode("ascii")
        self.assertEqual(expected, result)


class TestOsutilsFunctions(TestCase):
    """Test various osutils functions."""

    def test_split_unicode(self):
        """Test split with unicode paths."""
        dirname, basename = osutils.split("foo/bar")
        self.assertEqual("foo", dirname)
        self.assertEqual("bar", basename)

    def test_split_bytes(self):
        """Test split with byte paths."""
        dirname, basename = osutils.split(b"foo/bar")
        self.assertEqual(b"foo", dirname)
        self.assertEqual(b"bar", basename)

    def test_pathjoin_unicode(self):
        """Test pathjoin with unicode paths."""
        result = osutils.pathjoin("foo", "bar", "baz")
        self.assertEqual(os.path.join("foo", "bar", "baz"), result)

    def test_pathjoin_bytes(self):
        """Test pathjoin with byte paths."""
        result = osutils.pathjoin(b"foo", b"bar", b"baz")
        self.assertEqual(os.path.join(b"foo", b"bar", b"baz"), result)

    def test_basename_unicode(self):
        """Test basename with unicode path."""
        result = osutils.basename("foo/bar/baz")
        self.assertEqual("baz", result)

    def test_basename_bytes(self):
        """Test basename with byte path."""
        result = osutils.basename(b"foo/bar/baz")
        self.assertEqual(b"baz", result)

    def test_dirname_unicode(self):
        """Test dirname with unicode path."""
        result = osutils.dirname("foo/bar/baz")
        self.assertEqual("foo/bar", result)

    def test_dirname_bytes(self):
        """Test dirname with byte path."""
        result = osutils.dirname(b"foo/bar/baz")
        self.assertEqual(b"foo/bar", result)

    def test_splitpath(self):
        """Test splitpath function."""
        self.assertEqual(["foo", "bar"], osutils.splitpath("foo/bar"))
        self.assertEqual(["foo", "bar"], osutils.splitpath("/foo/bar"))
        self.assertEqual([b"foo", b"bar"], osutils.splitpath(b"foo/bar"))
        self.assertEqual([b"foo", b"bar"], osutils.splitpath(b"/foo/bar"))
        self.assertEqual([], osutils.splitpath(""))
        self.assertEqual([], osutils.splitpath("/"))

    def test_contains_whitespace(self):
        """Test contains_whitespace function."""
        self.assertTrue(osutils.contains_whitespace("hello world"))
        self.assertTrue(osutils.contains_whitespace("hello\tworld"))
        self.assertTrue(osutils.contains_whitespace("hello\nworld"))
        self.assertFalse(osutils.contains_whitespace("helloworld"))

        # Test bytes
        self.assertTrue(osutils.contains_whitespace(b"hello world"))
        self.assertFalse(osutils.contains_whitespace(b"helloworld"))

    def test_normalized_filename(self):
        """Test normalized_filename function."""
        # Simple ASCII filename
        result, can_access = osutils.normalized_filename("test.txt")
        self.assertEqual("test.txt", result)
        self.assertTrue(can_access)

        # Bytes filename
        result, can_access = osutils.normalized_filename(b"test.txt")
        self.assertEqual(b"test.txt", result)
        self.assertTrue(can_access)

    def test_chunks_to_lines(self):
        """Test chunks_to_lines function."""
        chunks = [b"line1\n", b"line2\nli", b"ne3\n"]
        result = osutils.chunks_to_lines(chunks)
        self.assertEqual([b"line1\n", b"line2\n", b"line3\n"], result)

        # Test with no newline at end
        chunks = [b"line1\n", b"line2"]
        result = osutils.chunks_to_lines(chunks)
        self.assertEqual([b"line1\n", b"line2"], result)

        # Test empty chunks
        self.assertEqual([], osutils.chunks_to_lines([]))

    def test_chunks_to_lines_iter(self):
        """Test chunks_to_lines_iter function."""
        chunks = iter([b"line1\n", b"line2\nli", b"ne3\n"])
        result = list(osutils.chunks_to_lines_iter(chunks))
        self.assertEqual([b"line1\n", b"line2\n", b"line3\n"], result)


class TestRustOsutilsFunctions(TestCase):
    """Test the Rust-based osutils functions."""

    def test_rand_chars(self):
        """Test rand_chars generates the right length string."""
        result = osutils.rand_chars(10)
        self.assertEqual(10, len(result))
        # Should only contain alphanumeric characters
        self.assertTrue(all(c.isalnum() for c in result))

    def test_is_inside(self):
        """Test is_inside function."""
        # Should work with both strings and bytes
        self.assertTrue(osutils.is_inside("/home", "/home/user"))
        self.assertTrue(osutils.is_inside("/home/", "/home/user"))
        self.assertFalse(osutils.is_inside("/home", "/usr/bin"))
        self.assertFalse(osutils.is_inside("/home/user", "/home"))

    def test_is_inside_any(self):
        """Test is_inside_any function."""
        dirs = ["/home", "/usr"]
        self.assertTrue(osutils.is_inside_any(dirs, "/home/user"))
        self.assertTrue(osutils.is_inside_any(dirs, "/usr/bin"))
        self.assertFalse(osutils.is_inside_any(dirs, "/var/log"))

    def test_parent_directories(self):
        """Test parent_directories function."""
        result = osutils.parent_directories("/home/user/documents/file.txt")
        # Convert to list since it returns an iterator
        parents = list(result)
        self.assertIn("/home/user/documents", parents)
        self.assertIn("/home/user", parents)
        self.assertIn("/home", parents)


class TestFileIterator(TestCase):
    """Test file_iterator function."""

    def test_file_iterator(self):
        """Test iterating over file contents."""
        import io

        content = b"a" * 100000  # 100KB of data
        file_obj = io.BytesIO(content)

        chunks = list(osutils.file_iterator(file_obj, chunk_size=1024))

        # Should have multiple chunks
        self.assertTrue(len(chunks) > 1)

        # Reassemble and check
        reassembled = b"".join(chunks)
        self.assertEqual(content, reassembled)

        # Check chunk sizes (all but last should be 1024)
        for chunk in chunks[:-1]:
            self.assertEqual(1024, len(chunk))


class TestPumpfile(TestCaseInTempDir):
    """Test pumpfile function."""

    def test_pumpfile(self):
        """Test copying data between file objects."""
        import io

        # Create source with some data
        source_data = b"Hello, world!" * 1000
        source = io.BytesIO(source_data)

        # Create destination
        dest = io.BytesIO()

        # Pump the data
        bytes_copied = osutils.pumpfile(source, dest)

        # Check the result
        self.assertEqual(len(source_data), bytes_copied)
        self.assertEqual(source_data, dest.getvalue())


class TestFileKindFromStatMode(TestCase):
    """Test file_kind_from_stat_mode function."""

    def test_regular_file(self):
        """Test regular file detection."""
        import stat

        mode = stat.S_IFREG | 0o644
        self.assertEqual("file", osutils.file_kind_from_stat_mode(mode))

    def test_directory(self):
        """Test directory detection."""
        import stat

        mode = stat.S_IFDIR | 0o755
        self.assertEqual("directory", osutils.file_kind_from_stat_mode(mode))

    def test_symlink(self):
        """Test symlink detection."""
        import stat

        mode = stat.S_IFLNK | 0o777
        self.assertEqual("symlink", osutils.file_kind_from_stat_mode(mode))

    def test_fifo(self):
        """Test FIFO detection."""
        import stat

        mode = stat.S_IFIFO | 0o666
        self.assertEqual("fifo", osutils.file_kind_from_stat_mode(mode))

    def test_socket(self):
        """Test socket detection."""
        import stat

        mode = stat.S_IFSOCK | 0o666
        self.assertEqual("socket", osutils.file_kind_from_stat_mode(mode))

    def test_char_device(self):
        """Test character device detection."""
        import stat

        mode = stat.S_IFCHR | 0o666
        self.assertEqual("chardev", osutils.file_kind_from_stat_mode(mode))

    def test_block_device(self):
        """Test block device detection."""
        import stat

        mode = stat.S_IFBLK | 0o666
        self.assertEqual("block", osutils.file_kind_from_stat_mode(mode))

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

"""Tests for bzrformats error classes."""

from .. import errors
from . import TestCase


class TestNoSuchFile(TestCase):
    """Test NoSuchFile error."""

    def test_no_such_file_str(self):
        """Test string representation of NoSuchFile."""
        err = errors.NoSuchFile("/path/to/missing/file")
        self.assertEqual("No such file: '/path/to/missing/file'", str(err))

    def test_no_such_file_with_extra(self):
        """Test NoSuchFile with extra information."""
        err = errors.NoSuchFile("/path/to/file", "additional info")
        self.assertEqual("No such file: '/path/to/file': additional info", str(err))


class TestPathError(TestCase):
    """Test PathError base class."""

    def test_path_error_str(self):
        """Test string representation of PathError."""
        err = errors.PathError("/some/path")
        self.assertEqual("Path error: '/some/path'", str(err))

    def test_path_error_with_extra(self):
        """Test PathError with extra information."""
        err = errors.PathError("/some/path", "extra details")
        self.assertEqual("Path error: '/some/path': extra details", str(err))


class TestReservedId(TestCase):
    """Test ReservedId error."""

    def test_reserved_id_str(self):
        """Test string representation of ReservedId."""
        err = errors.ReservedId(b"null:")
        self.assertEqual("Reserved revision-id {b'null:'}", str(err))


class TestRevisionNotPresent(TestCase):
    """Test RevisionNotPresent error."""

    def test_revision_not_present_str(self):
        """Test string representation of RevisionNotPresent."""
        err = errors.RevisionNotPresent(b"rev-123", b"file-456")
        expected = "Revision {b'rev-123'} not present in \"b'file-456'\"."
        self.assertEqual(expected, str(err))


class TestRevisionAlreadyPresent(TestCase):
    """Test RevisionAlreadyPresent error."""

    def test_revision_already_present_str(self):
        """Test string representation of RevisionAlreadyPresent."""
        err = errors.RevisionAlreadyPresent(b"rev-123", b"file-456")
        expected = "Revision {b'rev-123'} already present in \"b'file-456'\"."
        self.assertEqual(expected, str(err))


class TestInvalidRevisionId(TestCase):
    """Test InvalidRevisionId error."""

    def test_invalid_revision_id_str(self):
        """Test string representation of InvalidRevisionId."""
        err = errors.InvalidRevisionId(b"bad-rev", "mybranch")
        expected = "Invalid revision-id {b'bad-rev'} in mybranch"
        self.assertEqual(expected, str(err))


class TestNoSuchId(TestCase):
    """Test NoSuchId error."""

    def test_no_such_id_str(self):
        """Test string representation of NoSuchId."""
        from bzrformats.inventory import NoSuchId

        err = NoSuchId("tree-object", b"file-id-123")
        expected = (
            "The file id \"b'file-id-123'\" is not present in the tree tree-object."
        )
        self.assertEqual(expected, str(err))


class TestInconsistentDelta(TestCase):
    def test_inconsistent_delta_str(self):
        err = errors.InconsistentDelta("path", "file-id", "reason for foo")
        self.assertEqual(
            "An inconsistent delta was supplied involving 'path', 'file-id'\n"
            "reason: reason for foo",
            str(err),
        )


# Add test module discovery
def test_suite():
    """Return the test suite for error tests."""
    import unittest

    return unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])


if __name__ == "__main__":
    import unittest

    unittest.main()

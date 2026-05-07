# Copyright (C) 2007, 2009, 2010 Canonical Ltd
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

"""Container format for Bazaar data.

"Containers" and "records" are described in
doc/developers/container-format.txt.
"""

from io import BytesIO

from . import errors
from ._bzr_rs import pack as _pack_rs

FORMAT_ONE = _pack_rs.FORMAT_ONE


class ContainerError(errors.BzrFormatsError):
    """Base class of container errors."""


class UnknownContainerFormatError(ContainerError):
    """Exception raised when encountering unknown container format."""

    _fmt = "Unrecognised container format: %(container_format)r"

    def __init__(self, container_format):
        """Initialize UnknownContainerFormatError.

        Args:
            container_format: The unknown container format encountered.
        """
        self.container_format = container_format


class UnexpectedEndOfContainerError(ContainerError):
    """Exception raised when container stream ends unexpectedly."""

    _fmt = "Unexpected end of container stream"


class UnknownRecordTypeError(ContainerError):
    """Exception raised when encountering unknown record type."""

    _fmt = "Unknown record type: %(record_type)r"

    def __init__(self, record_type):
        """Initialize UnknownRecordTypeError.

        Args:
            record_type: The unknown record type encountered.
        """
        self.record_type = record_type


class InvalidRecordError(ContainerError):
    """Exception raised when a record is invalid."""

    _fmt = "Invalid record: %(reason)s"

    def __init__(self, reason):
        """Initialize InvalidRecordError.

        Args:
            reason: The reason the record is invalid.
        """
        self.reason = reason


class ContainerHasExcessDataError(ContainerError):
    """Exception raised when container has excess data after end marker."""

    _fmt = "Container has data after end marker: %(excess)r"

    def __init__(self, excess):
        """Initialize ContainerHasExcessDataError.

        Args:
            excess: The excess data found after end marker.
        """
        self.excess = excess


class DuplicateRecordNameError(ContainerError):
    """Exception raised when container has duplicate record names."""

    _fmt = "Container has multiple records with the same name: %(name)s"

    def __init__(self, name):
        """Initialize DuplicateRecordNameError.

        Args:
            name: The duplicate record name.
        """
        self.name = name.decode("utf-8")


_check_name = _pack_rs._check_name
_check_name_encoding = _pack_rs._check_name_encoding

ContainerSerialiser = _pack_rs.ContainerSerialiser
ContainerWriter = _pack_rs.ContainerWriter
ContainerReader = _pack_rs.ContainerReader
BytesRecordReader = _pack_rs.BytesRecordReader
ContainerPushParser = _pack_rs.ContainerPushParser


class ReadVFile:
    """Adapt a readv result iterator to a file like protocol.

    The readv result must support the iterator protocol returning (offset,
    data_bytes) pairs.
    """

    # XXX: This could be a generic transport class, as other code may want to
    # gradually consume the readv result.

    def __init__(self, readv_result):
        """Construct a new ReadVFile wrapper.

        :seealso: make_readv_reader

        :param readv_result: the most recent readv result - list or generator
        """
        # readv can return a sequence or an iterator, but we require an
        # iterator to know how much has been consumed.
        readv_result = iter(readv_result)
        self.readv_result = readv_result
        self._string = None

    def _next(self):
        if self._string is None or self._string.tell() == self._string_length:
            _offset, data = next(self.readv_result)
            self._string_length = len(data)
            self._string = BytesIO(data)

    def read(self, length):
        """Read specified number of bytes from the current string.

        Args:
            length: Number of bytes to read.

        Returns:
            The bytes read.

        Raises:
            BzrError: If insufficient bytes are available.
        """
        self._next()
        result = self._string.read(length)
        if len(result) < length:
            raise errors.BzrFormatsError(
                "wanted %d bytes but next "
                "hunk only contains %d: %r..." % (length, len(result), result[:20])
            )
        return result

    def readline(self):
        """Note that readline will not cross readv segments."""
        self._next()
        result = self._string.readline()
        if self._string.tell() == self._string_length and result[-1:] != b"\n":
            raise errors.BzrFormatsError(
                f"short readline in the readvfile hunk: {result!r}"
            )
        return result


def make_readv_reader(transport, filename, requested_records):
    """Create a ContainerReader that will read selected records only.

    :param transport: The transport the pack file is located on.
    :param filename: The filename of the pack file.
    :param requested_records: The record offset, length tuples as returned
        by add_bytes_record for the desired records.
    """
    readv_blocks = [(0, len(FORMAT_ONE) + 1)]
    readv_blocks.extend(requested_records)
    result = ContainerReader(ReadVFile(transport.readv(filename, readv_blocks)))
    return result


def iter_records_from_file(source_file):
    """Iterate over records from a file.

    Args:
        source_file: File-like object to read from.

    Yields:
        Records from the container file.
    """
    parser = ContainerPushParser()
    while True:
        bytes = source_file.read(parser.read_size_hint())
        parser.accept_bytes(bytes)
        yield from parser.read_pending_records()
        if parser.finished:
            break

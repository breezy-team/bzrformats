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
ReadVFile = _pack_rs.ReadVFile
make_readv_reader = _pack_rs.make_readv_reader
iter_records_from_file = _pack_rs.iter_records_from_file

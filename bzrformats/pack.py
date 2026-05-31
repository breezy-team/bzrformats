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

from ._bzr_rs import pack as _pack_rs

# The container error classes live in the Rust errors module; re-export them so
# bzrformats.pack.ContainerError (and friends) keep working for callers and for
# the Rust import_exception!(bzrformats.pack, ...) sites.
from .errors import (  # noqa: F401
    ContainerError,
    ContainerHasExcessDataError,
    DuplicateRecordNameError,
    InvalidRecordError,
    UnexpectedEndOfContainerError,
    UnknownContainerFormatError,
    UnknownRecordTypeError,
)

FORMAT_ONE = _pack_rs.FORMAT_ONE


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

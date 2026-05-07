# Copyright (C) 2005-2010 Canonical Ltd
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

"""XML serialization format version 8."""

import re

from ._bzr_rs import (
    inventory_serializer_v8,  # noqa: F401
    revision_serializer_v8,  # noqa: F401
)

_xml_unescape_map = {
    b"apos": b"'",
    b"quot": b'"',
    b"amp": b"&",
    b"lt": b"<",
    b"gt": b">",
}


def _unescaper(match, _map=_xml_unescape_map):
    code = match.group(1)
    try:
        return _map[code]
    except KeyError:
        if not code.startswith(b"#"):
            raise
        return chr(int(code[1:])).encode("utf8")


_unescape_re = re.compile(b"\\&([^;]*);")


def _unescape_xml(data):
    """Unescape predefined XML entities in a string of data."""
    return _unescape_re.sub(_unescaper, data)

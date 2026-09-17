# Copyright (C) 2008 Canonical Ltd
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

"""The ``views`` working tree control file.

Views scope a working tree to a subset of its paths. This module implements
the file format only; the view manager that reads and writes it lives in the
working tree. The implementation is in Rust (``bzrformats._bzr_rs.views``).
"""

from ._bzr_rs.views import (
    deserialize_view_content,
    serialize_view_content,
    view_display_str,
)

__all__ = [
    "deserialize_view_content",
    "serialize_view_content",
    "view_display_str",
]

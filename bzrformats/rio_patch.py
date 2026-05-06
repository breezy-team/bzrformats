# Copyright (C) 2005 Canonical Ltd
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

"""RIO-Patch format handling for email-safe stanza representation.

This module provides functions to convert between RIO stanzas and RIO-Patch format,
which is designed to be emailed as part of a patch. The format resists common forms
of damage such as newline conversion or removal of trailing whitespace.
"""

from ._bzr_rs.rio import read_patch_stanza, to_patch_lines

__all__ = ["read_patch_stanza", "to_patch_lines"]

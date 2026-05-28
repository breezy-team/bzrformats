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

"""XML escape helpers re-exported from the Rust extension.

The XML serializers themselves now live in Rust (see ``_bzr_rs``); this module
only retains the two helpers that downstream callers still import by name.
"""

from ._bzr_rs import encode_and_escape, escape_invalid_chars

__all__ = ["encode_and_escape", "escape_invalid_chars"]

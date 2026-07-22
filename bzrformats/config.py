# Copyright (C) 2005-2014 Canonical Ltd
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

"""A configobj-compatible reader/writer for the INI dialect breezy uses.

``ConfigObj`` is a drop-in for the subset of ``configobj.ConfigObj`` breezy's
config store relies on: a dict-like top level whose items are scalar option
strings and nested ``Section`` mappings, ``scalars``/``sections`` name lists,
``setdefault``/``write`` and live mutation. Parsing (UTF-8, list_values=False,
interpolation off), serialization and list-aware ``quote_value``/
``unquote_value`` are implemented as Rust pyclasses in the bazaar crate.
"""

from ._bzr_rs.config import (  # noqa: F401
    ConfigObj,
    Section,
    quote_value,
    unquote_value,
)

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

"""Parser and writer for the ConfigObj INI dialect breezy stores config in.

ConfigObj parses config bytes (UTF-8, list_values=False, interpolation off),
hands out Section views, and writes changes back preserving section/key order
and comments. quote_value/unquote_value implement breezy's list-aware quoting.
The parser is implemented as a Rust pyclass in the bazaar crate.
"""

from ._bzr_rs.config import (  # noqa: F401
    ConfigObj,
    Section,
    quote_value,
    unquote_value,
)

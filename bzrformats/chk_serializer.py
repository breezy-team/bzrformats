# Copyright (C) 2008, 2009, 2010 Canonical Ltd
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

"""Serializer object for CHK based inventory storage.

The serializer itself lives in Rust (see ``_bzr_rs``). Its write methods
dispatch on the inventory type: the Rust ``Inventory`` pyclass takes the
native path, while the pure-Python ``CHKInventory`` is read via attribute
access. The pre-built instances are re-exported here.
"""

from ._bzr_rs import CHKInventorySerializer as CHKSerializer
from ._bzr_rs import (
    inventory_chk_serializer_255_bigpage_9,
    inventory_chk_serializer_255_bigpage_10,
)

__all__ = [
    "CHKSerializer",
    "inventory_chk_serializer_255_bigpage_9",
    "inventory_chk_serializer_255_bigpage_10",
]

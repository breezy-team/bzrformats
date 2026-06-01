# Copyright (C) 2008, 2009 Canonical Ltd
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

"""Inventory delta serialisation.

See doc/developers/inventory.txt for the description of the format.

In this module the interesting classes are:
 - InventoryDeltaSerializer - object to read/write inventory deltas.
"""

__all__ = ["InventoryDeltaSerializer"]

from ._bzr_rs import inventory as _inventory_delta_rs

InventoryDeltaError = _inventory_delta_rs.InventoryDeltaError
IncompatibleInventoryDelta = _inventory_delta_rs.IncompatibleInventoryDelta
parse_inventory_entry = _inventory_delta_rs.parse_inventory_entry
serialize_inventory_entry = _inventory_delta_rs.serialize_inventory_entry
InventoryDelta = _inventory_delta_rs.InventoryDelta


# The serializer/deserializer are implemented as Rust pyclasses that hold the
# versioned_root / tree_references config and delegate to the Rust
# serialize_inventory_delta / parse_inventory_delta functions.
InventoryDeltaSerializer = _inventory_delta_rs.InventoryDeltaSerializer
InventoryDeltaDeserializer = _inventory_delta_rs.InventoryDeltaDeserializer

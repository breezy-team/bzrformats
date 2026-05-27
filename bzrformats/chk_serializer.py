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

"""Serializer object for CHK based inventory storage."""

from ._bzr_rs import CHKInventorySerializer as _RsCHKInventorySerializer
from ._bzr_rs.inventory import Inventory as _RsInventory


class CHKSerializer(_RsCHKInventorySerializer):
    """CHK serializer with Python overrides for duck-typed inventories.

    The Rust base class only accepts the Rust ``Inventory`` pyclass. CHK
    repositories use ``CHKInventory`` (a pure-Python class) instead, so the
    write methods are overridden here to fall back to a Python serializer
    that operates on either inventory type via attribute access.
    """

    supported_kinds = {"file", "directory", "symlink", "tree-reference"}

    def write_inventory_to_lines(self, inv):
        """Return a list of lines with the encoded inventory."""
        if isinstance(inv, _RsInventory):
            return super().write_inventory_to_lines(inv)
        return self._write_inventory_duck(inv, None, working=False)

    def write_inventory_to_chunks(self, inv):
        """Return a list of byte chunks with the encoded inventory."""
        if isinstance(inv, _RsInventory):
            return super().write_inventory_to_chunks(inv)
        return self._write_inventory_duck(inv, None, working=False)

    def write_inventory(self, inv, f, working=False):
        """Write inventory to a file, returning the lines written."""
        if isinstance(inv, _RsInventory):
            return super().write_inventory(inv, f, working)
        return self._write_inventory_duck(inv, f, working=working)

    def _write_inventory_duck(self, inv, f, working):
        output = self.write_inventory_duck_to_lines(inv, working)
        if f is not None:
            f.writelines(output)
        return output


inventory_chk_serializer_255_bigpage_9 = CHKSerializer(b"9", 65536, b"hash-255-way")
inventory_chk_serializer_255_bigpage_10 = CHKSerializer(b"10", 65536, b"hash-255-way")

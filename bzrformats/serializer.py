# Copyright (C) 2009, 2010 Canonical Ltd
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

"""Inventory/revision serialization."""

from . import registry

# The inventory-serialization error hierarchy lives in the Rust errors module;
# re-export it here so ``bzrformats.serializer.BadInventoryFormat`` (and
# friends) keep working for callers and for the Rust
# ``import_exception!(bzrformats.serializer, ...)`` sites.
from ._bzr_rs import (  # noqa: F401
    InventorySerializer,
    RevisionSerializer,
)
from .errors import (  # noqa: F401
    BadInventoryFormat,
    UnexpectedInventoryFormat,
    UnsupportedInventoryKind,
)


class SerializerRegistry(registry.Registry):
    """Registry for serializer objects."""


revision_format_registry = SerializerRegistry()
revision_format_registry.register_lazy(
    "5", "bzrformats._bzr_rs", "revision_serializer_v5"
)
revision_format_registry.register_lazy(
    "8", "bzrformats._bzr_rs", "revision_serializer_v8"
)
revision_format_registry.register_lazy(
    "10", "bzrformats._bzr_rs", "revision_bencode_serializer"
)


inventory_format_registry = SerializerRegistry()
inventory_format_registry.register_lazy(
    "5", "bzrformats.xml5", "inventory_serializer_v5"
)
inventory_format_registry.register_lazy(
    "6", "bzrformats.xml6", "inventory_serializer_v6"
)
inventory_format_registry.register_lazy(
    "7", "bzrformats.xml7", "inventory_serializer_v7"
)
inventory_format_registry.register_lazy(
    "8", "bzrformats.xml8", "inventory_serializer_v8"
)
inventory_format_registry.register_lazy(
    "9", "bzrformats.chk_serializer", "inventory_chk_serializer_255_bigpage_9"
)
inventory_format_registry.register_lazy(
    "10", "bzrformats.chk_serializer", "inventory_chk_serializer_255_bigpage_10"
)

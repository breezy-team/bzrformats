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

"""XML serialization support for weave format version 4.

v4 is a deprecated format: only deserialization is supported.
"""

from . import revision as _mod_revision
from ._bzr_rs import (
    _revision_serializer_v4_rs,
    inventory_serializer_v4,  # noqa: F401
)


class Revision(_mod_revision.Revision):
    """Revision class with additional v4-specific attributes."""

    def __new__(cls, *args, **kwargs):
        """Create a Revision instance, extracting v4-specific attributes from kwargs."""
        inventory_id = kwargs.pop("inventory_id", None)
        parent_sha1s = kwargs.pop("parent_sha1s", None)
        self = _mod_revision.Revision.__new__(cls, *args, **kwargs)
        self.inventory_id = inventory_id
        self.parent_sha1s = parent_sha1s
        return self


class _RevisionSerializer_v4:
    """v4 revision serializer (deserialization only).

    Wraps the Rust core serializer to produce :class:`Revision` instances
    that carry the extra v4-only ``inventory_id`` and ``parent_sha1s``
    attributes.
    """

    squashes_xml_invalid_characters = True
    format_name = "4"

    def _wrap(self, base, inventory_id, parent_sha1s):
        rev = Revision.__new__(
            Revision,
            revision_id=base.revision_id,
            parent_ids=base.parent_ids,
            committer=base.committer,
            message=base.message,
            properties=base.properties,
            inventory_sha1=base.inventory_sha1,
            timestamp=base.timestamp,
            timezone=base.timezone,
            inventory_id=inventory_id,
            parent_sha1s=parent_sha1s,
        )
        return rev

    def read_revision(self, f):
        base, inventory_id, parent_sha1s = _revision_serializer_v4_rs.read_revision(f)
        return self._wrap(base, inventory_id, parent_sha1s)

    def read_revision_from_string(self, xml_string):
        base, inventory_id, parent_sha1s = (
            _revision_serializer_v4_rs.read_revision_from_string(xml_string)
        )
        return self._wrap(base, inventory_id, parent_sha1s)


revision_serializer_v4 = _RevisionSerializer_v4()

# Copyright (C) 2006, 2008, 2009 Canonical Ltd
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

"""A simple least-recently-used (LRU) cache."""

import logging

# LRUCache, LRUSizeCache and FIFOCache are implemented as Rust pyclasses.
# LRUCache is count-based; LRUSizeCache evicts on the cumulative size of the
# values (compute_size(value), defaulting to len); FIFOCache is a dict subclass
# that evicts the oldest entries first. The ordering/eviction engines live in
# the bazaar crate. The _LRUNode handle is re-exported for the whitebox tests
# that walk the linked list.
from ._bzr_rs.lru_cache import (  # noqa: F401
    FIFOCache,
    LRUCache,
    LRUSizeCache,
    _LRUNode,
)

logger = logging.getLogger(__name__)

# Sentinel used by LRUCache to reject a reserved key. The Rust LRUCache reads
# this object back to compare against, so it must live here.
_null_key = object()

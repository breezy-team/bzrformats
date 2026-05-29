# Copyright (C) 2005-2011 Canonical Ltd
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

"""Plan merge implementation for versioned files.

The merge planners live in the Rust ``bazaar::plan_merge`` crate; this
module re-exports the pyo3 bindings.

* ``_PlanMerge(a_rev, b_rev, vf, key_prefix)`` plans an annotate merge by
  building an in-memory weave from the per-file graph between the two tips
  and running ``Weave.plan_merge`` over it (short-circuiting when one tip
  dominates the other). ``plan_merge()`` yields the cached ``(tag, line)``
  tuples; ``_subtract_plans``, ``_remove_external_references`` and
  ``_prune_tails`` are exposed for callers and tests.
* ``_PlanLCAMerge(a_rev, b_rev, vf, key_prefix, graph)`` plans an LCA merge;
  its constructor computes the plan eagerly.
"""

from ._bzr_rs.plan_merge import _PlanLCAMerge, _PlanMerge  # noqa: F401

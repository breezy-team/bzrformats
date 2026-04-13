# Copyright (C) 2006-2011 Canonical Ltd
# Written by Robert Collins <robert.collins@canonical.com>
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

"""Legacy bazaar specific gzip tunings."""

from ._bzr_rs import tuned_gzip as _tuned_gzip_rs

__all__ = ["chunks_to_gzip"]


chunks_to_gzip = _tuned_gzip_rs.chunks_to_gzip

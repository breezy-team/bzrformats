# Copyright (C) 2025 Breezy Contributors
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

"""Minimal progress bar protocol for bzrformats."""

from typing import Optional, Protocol, runtime_checkable


@runtime_checkable
class ProgressBar(Protocol):
    """Protocol for progress reporting."""

    def update(self, msg: Optional[str] = None, current: Optional[int] = None,
               total: Optional[int] = None) -> None:
        """Report progress.

        :param msg: Description of the current step.
        :param current: Current step number.
        :param total: Total number of steps.
        """
        ...

    def tick(self) -> None:
        """Indicate that some work was done without specific progress info."""
        ...

    def finished(self) -> None:
        """Mark the progress bar as complete."""
        ...

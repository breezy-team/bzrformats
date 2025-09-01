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

"""Revision related functionality and data structures.

This module provides utilities for working with revisions, including
iterating through revision ancestry and finding ancestors in revision
trees.
"""

# TODO: Some kind of command-line display of revision properties:
# perhaps show them in log -v and allow them as options to the commit command.

__docformat__ = "google"

from typing import Protocol, Optional, runtime_checkable

from . import errors

# Special revision IDs
NULL_REVISION = b"null:"
CURRENT_REVISION = b"current:"

RevisionID = bytes


def is_null(revision_id: RevisionID) -> bool:
    """Check if a revision ID is the null revision."""
    return revision_id == NULL_REVISION


def is_reserved_id(revision_id: Optional[RevisionID]) -> bool:
    """Check if a revision ID is reserved.
    
    Reserved IDs include null: and current:.
    """
    if revision_id is None:
        return False
    return revision_id in (NULL_REVISION, CURRENT_REVISION)


def check_not_reserved_id(revision_id: Optional[RevisionID]) -> None:
    """Raise an error if a revision ID is reserved."""
    if is_reserved_id(revision_id):
        raise errors.ReservedId(revision_id)


@runtime_checkable
class Revision(Protocol):
    """Protocol for revision objects across different VCS backends.
    
    This protocol defines the common interface that all revision
    implementations must provide, whether they're from Bazaar, Git,
    or other version control systems.
    """
    
    @property
    def revision_id(self) -> RevisionID:
        """The unique identifier for this revision."""
        ...
    
    @property
    def parent_ids(self) -> list[RevisionID]:
        """List of parent revision IDs."""
        ...
    
    @property
    def committer(self) -> Optional[str]:
        """The person who committed this revision."""
        ...
    
    @property
    def message(self) -> str:
        """The commit message."""
        ...
    
    @property
    def timestamp(self) -> float:
        """Unix timestamp when the revision was committed."""
        ...
    
    @property
    def timezone(self) -> Optional[int]:
        """Timezone offset in seconds from UTC."""
        ...
    
    @property
    def properties(self) -> dict[str, bytes]:
        """Additional properties stored with the revision."""
        ...
    
    @property
    def inventory_sha1(self) -> Optional[bytes]:
        """SHA1 of the inventory for this revision (Bazaar-specific, optional)."""
        ...
    
    def get_summary(self) -> str:
        """Get the first line of the commit message."""
        ...
    
    def get_apparent_authors(self) -> list[str]:
        """Get the apparent authors of this revision.
        
        Returns authors from properties if available, otherwise
        returns the committer.
        """
        ...
    
    def bug_urls(self) -> list[str]:
        """Get bug URLs associated with this revision."""
        ...


def iter_bugs(rev: Revision):
    """Iterate over the bugs associated with this revision."""
    from . import bugtracker

    return bugtracker.decode_bug_urls(rev.bug_urls())


def get_history(repository, current_revision):
    """Return the canonical line-of-history for this revision.

    If ghosts are present this may differ in result from a ghost-free
    repository.
    """
    reversed_result = []
    while current_revision is not None:
        reversed_result.append(current_revision.revision_id)
        if not len(current_revision.parent_ids):
            reversed_result.append(None)
            current_revision = None
        else:
            next_revision_id = current_revision.parent_ids[0]
            current_revision = repository.get_revision(next_revision_id)
    reversed_result.reverse()
    return reversed_result


def iter_ancestors(
    revision_id: RevisionID, revision_source, only_present: bool = False
):
    """Iterate through the ancestors of a revision.

    Args:
        revision_id: The revision ID to start from.
        revision_source: Source to retrieve revisions from.
        only_present: If True, only yield revisions that are present
            in the revision source.

    Yields:
        tuple[RevisionID, int]: Tuples of (ancestor_id, distance) where
            distance is the number of generations away from the starting
            revision.

    Raises:
        NoSuchRevision: If the starting revision_id cannot be found.
    """
    ancestors = [revision_id]
    distance = 0
    while len(ancestors) > 0:
        new_ancestors: list[bytes] = []
        for ancestor in ancestors:
            if not only_present:
                yield ancestor, distance
            try:
                revision = revision_source.get_revision(ancestor)
            except errors.NoSuchRevision as e:
                if e.revision == revision_id:
                    raise
                else:
                    continue
            if only_present:
                yield ancestor, distance
            new_ancestors.extend(revision.parent_ids)
        ancestors = new_ancestors
        distance += 1


def find_present_ancestors(
    revision_id: RevisionID, revision_source
) -> dict[RevisionID, tuple[int, int]]:
    """Return the ancestors of a revision present in a branch.

    It's possible that a branch won't have the complete ancestry of
    one of its revisions.
    """
    found_ancestors: dict[RevisionID, tuple[int, int]] = {}
    anc_iter = enumerate(
        iter_ancestors(revision_id, revision_source, only_present=True)
    )
    for anc_order, (anc_id, anc_distance) in anc_iter:
        if anc_id not in found_ancestors:
            found_ancestors[anc_id] = (anc_order, anc_distance)
    return found_ancestors
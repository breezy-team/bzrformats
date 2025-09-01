# Copyright (C) 2008-2018 Jelmer Vernooij <jelmer@jelmer.uk>
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

"""Git-specific revision implementation."""

from typing import Optional
from dulwich.objects import Commit
from ..revision import RevisionID


class GitRevision:
    """Adapter that makes a Git commit conform to the Revision protocol."""
    
    def __init__(self, commit: Commit, revision_id: RevisionID, parent_ids: list[RevisionID]):
        """Initialize a GitRevision wrapper.
        
        Args:
            commit: The underlying Git commit object
            revision_id: The Bazaar revision ID for this commit
            parent_ids: List of Bazaar revision IDs for parents
        """
        self._commit = commit
        self._revision_id = revision_id
        self._parent_ids = parent_ids
    
    @property
    def revision_id(self) -> RevisionID:
        """The unique identifier for this revision."""
        return self._revision_id
    
    @property
    def parent_ids(self) -> list[RevisionID]:
        """List of parent revision IDs."""
        return self._parent_ids
    
    @property
    def committer(self) -> Optional[str]:
        """The person who committed this revision."""
        if self._commit.committer:
            return self._commit.committer.decode('utf-8', errors='replace')
        return None
    
    @property
    def message(self) -> str:
        """The commit message."""
        if self._commit.message:
            return self._commit.message.decode('utf-8', errors='replace')
        return ""
    
    @property
    def timestamp(self) -> float:
        """Unix timestamp when the revision was committed."""
        return float(self._commit.commit_time)
    
    @property
    def timezone(self) -> Optional[int]:
        """Timezone offset in seconds from UTC."""
        return self._commit.commit_timezone
    
    @property
    def properties(self) -> dict[str, bytes]:
        """Additional properties stored with the revision."""
        properties = {}
        
        # Add Git-specific properties
        if self._commit.author != self._commit.committer:
            if self._commit.author:
                properties["author"] = self._commit.author
        
        if self._commit.author_time != self._commit.commit_time:
            properties["author-timestamp"] = str(self._commit.author_time).encode('ascii')
        
        if self._commit.author_timezone != self._commit.commit_timezone:
            properties["author-timezone"] = str(self._commit.author_timezone).encode('ascii')
        
        # Add encoding if not UTF-8
        if self._commit.encoding and self._commit.encoding != b'UTF-8':
            properties["git-encoding"] = self._commit.encoding
        
        # Add GPG signature if present
        if hasattr(self._commit, 'gpgsig') and self._commit.gpgsig:
            properties["git-gpgsig"] = self._commit.gpgsig
        
        return properties
    
    @property
    def inventory_sha1(self) -> Optional[bytes]:
        """SHA1 of the inventory for this revision (not applicable for Git)."""
        return None
    
    def get_summary(self) -> str:
        """Get the first line of the commit message."""
        message = self.message
        if not message:
            return ""
        lines = message.split('\n', 1)
        return lines[0].strip()
    
    def get_apparent_authors(self) -> list[str]:
        """Get the apparent authors of this revision.
        
        For Git, this returns the author if different from committer,
        otherwise returns the committer.
        """
        authors = []
        
        if self._commit.author:
            author = self._commit.author.decode('utf-8', errors='replace')
            if author and author not in authors:
                authors.append(author)
        
        if not authors and self.committer:
            authors.append(self.committer)
        
        return authors or []
    
    def bug_urls(self) -> list[str]:
        """Get bug URLs associated with this revision.
        
        For Git commits, this would typically be extracted from the
        commit message or Git notes, but for now we return empty.
        """
        # TODO: Parse commit message for bug URLs
        return []


__all__ = ['GitRevision']
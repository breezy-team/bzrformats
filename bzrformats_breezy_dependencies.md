# Comprehensive Summary of Breezy Dependencies in bzrformats

**Last Updated**: After moving all pack_repo classes including RetryWithNewPacks from breezy.bzr.pack_repo to bzrformats.pack_repo

## Overview

- **Total breezy modules used**: 12 distinct modules (down from 20)
- **Total files with breezy imports**: 35 out of 59 Python files (59%)
- **Test files with breezy imports**: 23 files (most dependencies are in tests)
- **Non-test files with breezy imports**: 12 files

Note: 
- Graph-related imports (tsort, graph, multiparent) have been moved to vcsgraph package
- _PlanMerge classes have been moved to bzrformats.merge
- breezy.trace has been replaced with Python's standard logging module
- textmerge and bisect_multi have been moved to bzrformats
- _PrematchedMatcher has been moved from breezy.diff to bzrformats.diff
- All pack_repo classes (_DirectPackAccess, Pack, ExistingPack, ResumedPack, NewPack, RetryWithNewPacks) have been moved from breezy.bzr.pack_repo to bzrformats.pack_repo
- RetryWithNewPacks now derives from bzrformats.errors.BzrFormatsError instead of breezy.errors.BzrError

## Key Dependencies by Category

### 1. Core Infrastructure (Most Critical)
These are deeply integrated and would require significant refactoring:

- **breezy.errors** (used in 17+ files)
  - Custom exception classes like `RevisionNotPresent`, `RevisionAlreadyPresent`, `InvalidRevisionId`
  - Critical for error handling throughout the codebase
  
- **breezy.transport** (used in 10+ files)
  - File system abstraction layer
  - Used for reading/writing versioned files, indexes, etc.
  - Includes `NoSuchFile` exception

- **breezy.osutils** (used in 7 files)
  - Utility functions: `sha_string`, `sha_strings`, `file_iterator`
  - Path manipulation and file operations

### 2. Core Data Types
- **breezy.revision** (used in 5 files)
  - `NULL_REVISION` constant
  - `Revision` class
  - `RevisionID` type

### 3. Performance & Caching
- **breezy.lru_cache** (used in 3 files)
  - `LRUSizeCache` for groupcompress
  - General `lru_cache` for btree_index and chk_map
  
- **breezy.fifo_cache** (used in 1 file - btree_index.py)

### 4. Registry System
- **breezy.registry** (used in 3 files)
  - `Registry` class for plugin systems
  - Used in serializer.py, chk_map.py, versionedfile.py

### 5. Debugging & Logging
- **breezy.debug** (used in 4 files)
  - Debug flags and logging
  
Note: breezy.trace has been removed - all files now use Python's standard logging module.

### 6. UI & Progress
- **breezy.ui** (used in 2 files)
- **breezy.progress** (used in 1 file)

### 7. Algorithms & Utilities

Note: The following have been moved:
- tsort, graph, multiparent → vcsgraph package
- bisect_multi → bzrformats.bisect_multi
- _PrematchedMatcher → bzrformats.diff
- textmerge → bzrformats.textmerge
- All pack_repo classes → bzrformats.pack_repo

### 8. Higher-Level Components
- **breezy.bzr.annotate** (used in 3 files)
  - `VersionedFileAnnotator` class

### 9. Test Infrastructure
- **breezy.tests** (used in 15 files)
  - Test base classes and utilities
  - Only needed for running tests

## Recommendations for Refactoring

### Priority 1 - Easy to Extract
1. **Constants**: `NULL_REVISION` - could be redefined locally
2. **Simple utilities**: SHA functions from osutils could be replaced with hashlib
3. **Debug flags**: Could be replaced with a local debug system

### Priority 2 - Moderate Effort
1. **Error classes**: Create bzrformats-specific exceptions inheriting from standard Python exceptions
2. **Registry system**: Could be simplified or replaced with a basic dict-based system
3. **LRU cache**: Could use functools.lru_cache or a third-party library

### Priority 3 - Significant Effort
1. **Transport layer**: This is deeply integrated and would need a major abstraction
2. **Revision and RevisionID types**: Would need careful API preservation
3. **Annotate functionality**: VersionedFileAnnotator class would need careful extraction

### Priority 4 - Test-Only Dependencies
Most test dependencies could remain as they only affect test execution, not the core functionality.

## Files with Most Dependencies
1. **versionedfile.py** - 6 different breezy imports (was 9, now uses vcsgraph and local merge)
2. **knit.py** - 6 different breezy imports (was 8, now uses vcsgraph and local diff module) 
3. **groupcompress.py** - 5 different breezy imports (was 7, now uses vcsgraph and standard logging)
4. **weave.py** - 5 different breezy imports (was 6, now uses vcsgraph)
5. **dirstate.py** - 5 different breezy imports

These files would be the most complex to refactor and might be good candidates for keeping the breezy dependency initially.
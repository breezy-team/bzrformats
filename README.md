# bzrformats

Core Bazaar format implementations and utilities, extracted from the
[Breezy](https://www.breezy-vcs.org/) version control system.

## Overview

bzrformats provides the internal format implementations that power Bazaar-compatible
version control. It includes serialization, compression, indexing, and data structure
modules for reading and writing Bazaar repositories, working trees, and branches.

## Features

- **Versioned file storage** — knit, weave, and groupcompress formats
- **Directory state tracking** — efficient metadata caching for working trees
- **Serialization** — XML-based inventory and revision serialization (formats 5–8),
  plus CHK-based serialization
- **Indexing** — graph index and B+Tree index for pack-based repositories
- **Compression** — groupcompress for efficient delta storage of related files
- **Pack repositories** — container format for bundling versioned data
- **Rust accelerators** — performance-critical code implemented in Rust with
  Python bindings via PyO3
- **Cython extensions** — optional compiled extensions for hot paths

## Installation

```
pip install bzrformats
```

### Build requirements

Building from source requires:

- Python >= 3.10, < 3.15
- A Rust toolchain (for the compiled extensions)
- Cython >= 0.29

## Usage

This package is primarily intended for use by version control systems and tools
that need to work with Bazaar format data. The modules provide building blocks
for implementing Bazaar-compatible storage formats.

```python
from bzrformats import knit, groupcompress, index
```

## License

GNU General Public License v2 or later (GPLv2+). See [COPYING.txt](COPYING.txt).

## History

These modules were originally part of the
[Breezy](https://github.com/breezy-team/breezy) project (`breezy.bzr`)
and have been extracted into a standalone package.

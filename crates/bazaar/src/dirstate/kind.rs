//! The dirstate's per-tree kind enum and the extension trait
//! `OptionKindExt` used to simplify liveness checks across the
//! dirstate code base.

/// The six entry-kinds dirstate tracks — the same set Python's
/// `DirState._minikind_to_kind` maps to/from.  Variant discriminants
/// are the on-disk "minikind" byte, so `kind as u8` produces the byte
/// and [`Kind::from_minikind`] round-trips back.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Kind {
    /// `b'a'` — absent in this tree.
    Absent = b'a',
    /// `b'f'` — a regular file; `fingerprint` is the sha1.
    File = b'f',
    /// `b'd'` — a directory; `fingerprint` is empty.
    Directory = b'd',
    /// `b'r'` — relocated; `fingerprint` is the target path.
    Relocated = b'r',
    /// `b'l'` — a symbolic link; `fingerprint` is the link target.
    Symlink = b'l',
    /// `b't'` — a tree reference; `fingerprint` is the referenced revision.
    TreeReference = b't',
}

impl Kind {
    /// The one-byte on-disk code — what Python calls the "minikind".
    #[inline]
    pub fn to_minikind(self) -> u8 {
        self as u8
    }

    /// Parse a minikind byte.  Returns the offending byte on failure
    /// so callers can surface a meaningful error (corrupt dirstate /
    /// parser input).
    #[inline]
    pub fn from_minikind(byte: u8) -> Result<Self, u8> {
        match byte {
            b'a' => Ok(Kind::Absent),
            b'f' => Ok(Kind::File),
            b'd' => Ok(Kind::Directory),
            b'r' => Ok(Kind::Relocated),
            b'l' => Ok(Kind::Symlink),
            b't' => Ok(Kind::TreeReference),
            other => Err(other),
        }
    }

    pub fn to_char(self) -> char {
        self.to_minikind() as char
    }

    pub fn to_str(&self) -> &str {
        match self {
            Kind::Absent => "absent",
            Kind::File => "file",
            Kind::Directory => "directory",
            Kind::Relocated => "relocated",
            Kind::Symlink => "symlink",
            Kind::TreeReference => "tree-reference",
        }
    }

    /// Whether this kind represents a real on-disk entity (`f`, `d`,
    /// `l`, `t`) — the cases `process_entry` treats as "content in
    /// this tree" as opposed to `a`bsent / `r`elocated.
    #[inline]
    pub fn is_fdlt(self) -> bool {
        matches!(
            self,
            Kind::File | Kind::Directory | Kind::Symlink | Kind::TreeReference
        )
    }

    /// `is_fdlt` plus relocation — anything except `a`bsent.  Used by
    /// `process_entry` to decide whether the source side of a
    /// comparison can contribute a visible change.
    #[inline]
    pub fn is_fdltr(self) -> bool {
        !matches!(self, Kind::Absent)
    }

    /// Either `a`bsent or `r`elocated — the two kinds that mean
    /// "this file is not really here".
    #[inline]
    pub fn is_absent_or_relocated(self) -> bool {
        matches!(self, Kind::Absent | Kind::Relocated)
    }

    /// Convert to the 4-variant [`osutils::Kind`]; returns `None`
    /// for ``Absent`` / ``Relocated`` (which have no filesystem
    /// counterpart).
    pub fn to_osutils_kind(self) -> Option<osutils::Kind> {
        match self {
            Kind::File => Some(osutils::Kind::File),
            Kind::Directory => Some(osutils::Kind::Directory),
            Kind::Symlink => Some(osutils::Kind::Symlink),
            Kind::TreeReference => Some(osutils::Kind::TreeReference),
            Kind::Absent | Kind::Relocated => None,
        }
    }
}

impl From<osutils::Kind> for Kind {
    fn from(k: osutils::Kind) -> Self {
        match k {
            osutils::Kind::File => Kind::File,
            osutils::Kind::Directory => Kind::Directory,
            osutils::Kind::Symlink => Kind::Symlink,
            osutils::Kind::TreeReference => Kind::TreeReference,
        }
    }
}

impl std::fmt::Display for Kind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}

/// Extension methods for `Option<Kind>` that collapse the repeated
/// `None | Some(Absent) | Some(Relocated)` pattern used throughout
/// the tree-slot lookup sites.
pub trait OptionKindExt {
    /// True when the slot is missing, absent, or relocated — i.e.
    /// there is no live entry at this position in this tree.
    fn is_not_live(self) -> bool;
    /// True when the slot holds a live entry (`f`/`d`/`l`/`t`).
    fn is_live(self) -> bool;
}

impl OptionKindExt for Option<Kind> {
    #[inline]
    fn is_not_live(self) -> bool {
        match self {
            None | Some(Kind::Absent) | Some(Kind::Relocated) => true,
            Some(_) => false,
        }
    }
    #[inline]
    fn is_live(self) -> bool {
        !self.is_not_live()
    }
}

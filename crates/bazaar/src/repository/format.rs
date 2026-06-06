//! Repository format metadata and a registry of known formats.
//!
//! Every on-disk repository format carries a marker string (the contents
//! of `.bzr/repository/format`) and a set of capabilities. [`RepositoryFormat`]
//! describes one format; the [`registry`] collects all known formats so a
//! `.bzr/repository/format` marker can be looked up.
//!
//! Formats are declared with [`declare_repository_format!`], which both
//! defines a `static RepositoryFormat` and submits it to the registry via
//! the `inventory` crate (the same distributed-registration mechanism the
//! knit adapters use). Looking a marker up tells you what the repository
//! can do and gives the [`OpenFn`] that opens it; whether that opener is
//! implemented yet is a separate question ([`RepositoryFormat::is_supported`]).

use crate::serializer::{InventorySerializer, RevisionSerializer};

use super::pack_2a::{RepositoryError, SharedTransport};
use super::Repository;

/// Opens the repository rooted at `transport` (`.bzr/repository`) as a
/// particular format, returning an abstract [`Repository`]. Each format
/// carries one of these so opening dispatches through the format itself
/// rather than a discriminant.
pub type OpenFn = fn(SharedTransport) -> Result<Box<dyn Repository>, RepositoryError>;

/// An [`OpenFn`] for formats this crate cannot open yet, reporting the
/// format as unsupported.
pub fn open_unsupported(
    _transport: SharedTransport,
) -> Result<Box<dyn Repository>, RepositoryError> {
    Err(RepositoryError::UnsupportedFormat("unsupported format"))
}

/// Static description of one repository format.
///
/// Held by `&'static` reference everywhere; instances are created by
/// [`declare_repository_format!`] and never mutated.
pub struct RepositoryFormat {
    /// The exact bytes of `.bzr/repository/format`.
    pub format_string: &'static [u8],
    /// A human-readable description.
    pub description: &'static str,
    /// Opens a repository of this format.
    pub open: OpenFn,
    /// The revision serializer.
    pub revision_serializer: &'static dyn RevisionSerializer,
    /// The inventory serializer.
    pub inventory_serializer: &'static dyn InventorySerializer,
    /// Whether the root entry carries per-file version data (rich root).
    pub rich_root_data: bool,
    /// Whether content-hash-keyed (CHK) storage is used.
    pub supports_chks: bool,
    /// Whether nested-tree references are supported.
    pub supports_tree_reference: bool,
    /// Whether external (stacked) lookups are supported.
    pub supports_external_lookups: bool,
    /// Whether this crate can currently open repositories of this format.
    pub supported: bool,
    /// Whether the format is deprecated (still readable, upgrade advised).
    pub deprecated: bool,
}

impl RepositoryFormat {
    /// A baseline format with all capability flags off, used by
    /// [`declare_repository_format!`] as the `..` base so a declaration
    /// only states the fields that differ.
    pub const DEFAULT: RepositoryFormat = RepositoryFormat {
        format_string: b"",
        description: "",
        open: open_unsupported,
        revision_serializer: &crate::bencode_serializer::BEncodeRevisionSerializer1,
        inventory_serializer: &crate::xml_serializer::Chk255BigPageInventorySerializer,
        rich_root_data: false,
        supports_chks: false,
        supports_tree_reference: false,
        supports_external_lookups: false,
        supported: false,
        deprecated: false,
    };

    /// The `.bzr/repository/format` marker for this format.
    pub fn format_string(&self) -> &'static [u8] {
        self.format_string
    }

    /// A human-readable description.
    pub fn get_format_description(&self) -> &'static str {
        self.description
    }

    /// Whether this crate can open repositories of this format.
    pub fn is_supported(&self) -> bool {
        self.supported
    }

    /// Whether the format is deprecated.
    pub fn is_deprecated(&self) -> bool {
        self.deprecated
    }

    /// The network name (metadir formats use their marker string).
    pub fn network_name(&self) -> &'static [u8] {
        self.format_string
    }
}

/// Registry entry, submitted by [`declare_repository_format!`] and
/// collected via the `inventory` crate.
pub struct RepositoryFormatRegistration(pub &'static RepositoryFormat);

inventory::collect!(RepositoryFormatRegistration);

/// Declare a repository format: define a `static` [`RepositoryFormat`] and
/// register it so [`registry`] can find it by marker string.
///
/// Usage names the static, then gives `field: value` pairs. Capability
/// fields default to `false` and `supported`/`deprecated` likewise unless
/// set, so a declaration only states what differs from a plain format.
#[macro_export]
macro_rules! declare_repository_format {
    (
        $name:ident {
            format_string: $fmt:expr,
            description: $desc:expr,
            revision_serializer: $rev:expr,
            inventory_serializer: $inv:expr,
            $( $field:ident : $value:expr, )*
        }
    ) => {
        pub static $name: $crate::repository::format::RepositoryFormat =
            $crate::repository::format::RepositoryFormat {
                format_string: $fmt,
                description: $desc,
                revision_serializer: $rev,
                inventory_serializer: $inv,
                $( $field: $value, )*
                ..$crate::repository::format::RepositoryFormat::DEFAULT
            };

        inventory::submit! {
            $crate::repository::format::RepositoryFormatRegistration(&$name)
        }
    };
}

/// Look up a repository format by its `.bzr/repository/format` marker.
///
/// Returns `None` if no declared format matches.
pub fn find_format(format_string: &[u8]) -> Option<&'static RepositoryFormat> {
    inventory::iter::<RepositoryFormatRegistration>
        .into_iter()
        .map(|r| r.0)
        .find(|f| f.format_string == format_string)
}

/// All declared repository formats.
pub fn all_formats() -> Vec<&'static RepositoryFormat> {
    inventory::iter::<RepositoryFormatRegistration>
        .into_iter()
        .map(|r| r.0)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_2a_format_is_registered_and_supported() {
        let f = find_format(b"Bazaar repository format 2a (needs bzr 1.16 or later)\n")
            .expect("2a format registered");
        assert!(std::ptr::fn_addr_eq(
            f.open,
            super::super::pack_2a::open_group_compress as OpenFn
        ));
        assert!(f.supports_chks);
        assert!(f.rich_root_data);
        assert!(f.is_supported());
    }

    #[test]
    fn knitpack_formats_are_registered() {
        let f = find_format(b"Bazaar pack repository format 1 (needs bzr 0.92)\n")
            .expect("knitpack 1 registered");
        assert!(std::ptr::fn_addr_eq(
            f.open,
            super::super::pack_knit::open_knit_pack as OpenFn
        ));
        assert!(!f.rich_root_data);
        assert_eq!(f.inventory_serializer.format_num(), b"5");
    }

    #[test]
    fn rich_root_knitpack_uses_xml6() {
        let f = find_format(b"Bazaar pack repository format 1 with rich root (needs bzr 1.0)\n")
            .expect("knitpack 4 registered");
        assert!(f.rich_root_data);
        assert_eq!(f.inventory_serializer.format_num(), b"6");
    }

    #[test]
    fn unknown_marker_is_none() {
        assert!(find_format(b"Bazaar nonsense format\n").is_none());
    }

    #[test]
    fn all_formats_are_nonempty() {
        assert!(all_formats().len() >= 10);
    }
}

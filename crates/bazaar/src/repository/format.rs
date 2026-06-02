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
//! can do and which decoder family it belongs to; whether that decoder is
//! actually implemented yet is a separate question ([`RepositoryFormat::is_supported`]).

/// Which storage family a repository format belongs to. This selects the
/// record codec and on-disk layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageKind {
    /// Old knit versioned-file format (not pack-based).
    Knit,
    /// Pack container with knit records and XML inventories.
    KnitPack,
    /// Pack container with groupcompress records and CHK inventories (2a).
    GroupCompress,
}

/// Which revision serializer a format uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RevisionSerializerKind {
    /// XML revision serializer, format 5.
    Xml5,
    /// Bencode revision serializer (2a).
    Bencode,
}

/// Which inventory serializer a format uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InventorySerializerKind {
    /// XML inventory serializer, format 5 (plain root).
    Xml5,
    /// XML inventory serializer, format 6 (rich root).
    Xml6,
    /// XML inventory serializer, format 7 (rich root + subtrees).
    Xml7,
    /// CHK inventory serializer (2a), `hash-255-way` big pages.
    Chk255BigPage,
}

/// Static description of one repository format.
///
/// Held by `&'static` reference everywhere; instances are created by
/// [`declare_repository_format!`] and never mutated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepositoryFormat {
    /// The exact bytes of `.bzr/repository/format`.
    pub format_string: &'static [u8],
    /// A human-readable description.
    pub description: &'static str,
    /// The storage family (record codec + layout).
    pub storage: StorageKind,
    /// The revision serializer.
    pub revision_serializer: RevisionSerializerKind,
    /// The inventory serializer.
    pub inventory_serializer: InventorySerializerKind,
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
        storage: StorageKind::GroupCompress,
        revision_serializer: RevisionSerializerKind::Bencode,
        inventory_serializer: InventorySerializerKind::Chk255BigPage,
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
            storage: $storage:expr,
            revision_serializer: $rev:expr,
            inventory_serializer: $inv:expr,
            $( $field:ident : $value:expr, )*
        }
    ) => {
        pub static $name: $crate::repository::format::RepositoryFormat =
            $crate::repository::format::RepositoryFormat {
                format_string: $fmt,
                description: $desc,
                storage: $storage,
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
        assert_eq!(f.storage, StorageKind::GroupCompress);
        assert!(f.supports_chks);
        assert!(f.rich_root_data);
        assert!(f.is_supported());
    }

    #[test]
    fn knitpack_formats_are_registered() {
        let f = find_format(b"Bazaar pack repository format 1 (needs bzr 0.92)\n")
            .expect("knitpack 1 registered");
        assert_eq!(f.storage, StorageKind::KnitPack);
        assert!(!f.rich_root_data);
        assert_eq!(f.inventory_serializer, InventorySerializerKind::Xml5);
    }

    #[test]
    fn rich_root_knitpack_uses_xml6() {
        let f = find_format(b"Bazaar pack repository format 1 with rich root (needs bzr 1.0)\n")
            .expect("knitpack 4 registered");
        assert!(f.rich_root_data);
        assert_eq!(f.inventory_serializer, InventorySerializerKind::Xml6);
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

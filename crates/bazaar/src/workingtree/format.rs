//! Working-tree format metadata and registry.
//!
//! Mirrors [`crate::repository::format`] for working trees: each format
//! carries its `.bzr/checkout/format` marker and capability flags,
//! declared with [`declare_workingtree_format!`].

/// Static description of one working-tree format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkingTreeFormat {
    /// The exact bytes of `.bzr/checkout/format`.
    pub format_string: &'static [u8],
    /// A human-readable description.
    pub description: &'static str,
    /// Whether the tree state is stored in a dirstate (formats 4/5/6).
    pub uses_dirstate: bool,
    /// Whether content filtering (eol etc.) is supported (format 6).
    pub supports_content_filtering: bool,
    /// Whether views are supported (format 6).
    pub supports_views: bool,
    /// Whether this crate can currently open trees of this format.
    pub supported: bool,
    /// Whether the format is deprecated.
    pub deprecated: bool,
}

impl WorkingTreeFormat {
    /// Baseline format with all flags off; the `..` base used by
    /// [`declare_workingtree_format!`].
    pub const DEFAULT: WorkingTreeFormat = WorkingTreeFormat {
        format_string: b"",
        description: "",
        uses_dirstate: false,
        supports_content_filtering: false,
        supports_views: false,
        supported: false,
        deprecated: false,
    };

    /// The `.bzr/checkout/format` marker for this format.
    pub fn format_string(&self) -> &'static [u8] {
        self.format_string
    }

    /// A human-readable description.
    pub fn get_format_description(&self) -> &'static str {
        self.description
    }

    /// Whether this crate can open trees of this format.
    pub fn is_supported(&self) -> bool {
        self.supported
    }
}

/// Registry entry, submitted by [`declare_workingtree_format!`].
pub struct WorkingTreeFormatRegistration(pub &'static WorkingTreeFormat);

inventory::collect!(WorkingTreeFormatRegistration);

/// Declare a working-tree format: define a `static` [`WorkingTreeFormat`]
/// and register it. Capability fields default to `false`.
#[macro_export]
macro_rules! declare_workingtree_format {
    (
        $name:ident {
            format_string: $fmt:expr,
            description: $desc:expr,
            $( $field:ident : $value:expr, )*
        }
    ) => {
        pub static $name: $crate::workingtree::format::WorkingTreeFormat =
            $crate::workingtree::format::WorkingTreeFormat {
                format_string: $fmt,
                description: $desc,
                $( $field: $value, )*
                ..$crate::workingtree::format::WorkingTreeFormat::DEFAULT
            };

        inventory::submit! {
            $crate::workingtree::format::WorkingTreeFormatRegistration(&$name)
        }
    };
}

/// Look up a working-tree format by its `.bzr/checkout/format` marker.
pub fn find_format(format_string: &[u8]) -> Option<&'static WorkingTreeFormat> {
    inventory::iter::<WorkingTreeFormatRegistration>
        .into_iter()
        .map(|r| r.0)
        .find(|f| f.format_string == format_string)
}

/// All declared working-tree formats.
pub fn all_formats() -> Vec<&'static WorkingTreeFormat> {
    inventory::iter::<WorkingTreeFormatRegistration>
        .into_iter()
        .map(|r| r.0)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_6_is_registered_and_supported() {
        let f = find_format(b"Bazaar Working Tree Format 6 (bzr 1.14)\n")
            .expect("working tree format 6 registered");
        assert!(f.uses_dirstate);
        assert!(f.supports_views);
        assert!(f.supports_content_filtering);
        assert!(f.is_supported());
    }

    #[test]
    fn format_3_is_not_dirstate() {
        let f = find_format(b"Bazaar-NG Working Tree format 3")
            .expect("working tree format 3 registered");
        assert!(!f.uses_dirstate);
    }

    #[test]
    fn unknown_marker_is_none() {
        assert!(find_format(b"nonsense\n").is_none());
    }
}

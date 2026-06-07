//! Branch format metadata and registry.
//!
//! Mirrors [`crate::repository::format`] for branches: each format carries
//! its `.bzr/branch/format` marker and capability flags, declared with
//! [`declare_branch_format!`] and collected into a registry.

/// Static description of one branch format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BranchFormat {
    /// The exact bytes of `.bzr/branch/format`.
    pub format_string: &'static [u8],
    /// A human-readable description.
    pub description: &'static str,
    /// Whether the format stores tags.
    pub supports_tags: bool,
    /// Whether the format supports stacking on another branch.
    pub supports_stacking: bool,
    /// Whether the format records reference locations (the `references` RIO
    /// file). True for formats 6, 7 and 8; setting a reference on a format-7
    /// branch upgrades its marker to format 8.
    pub supports_reference_locations: bool,
    /// Whether the tip is stored as a full `revision-history` list (format 5)
    /// rather than a `last-revision` `<revno> <revision_id>` line (6/7/8).
    pub full_history: bool,
    /// Whether this crate can currently open branches of this format.
    pub supported: bool,
    /// Whether the format is deprecated.
    pub deprecated: bool,
    /// Whether this is a reference to a branch held elsewhere.
    pub is_reference: bool,
}

impl BranchFormat {
    /// Baseline format with all flags off; the `..` base used by
    /// [`declare_branch_format!`].
    pub const DEFAULT: BranchFormat = BranchFormat {
        format_string: b"",
        description: "",
        supports_tags: false,
        supports_stacking: false,
        supports_reference_locations: false,
        full_history: false,
        supported: false,
        deprecated: false,
        is_reference: false,
    };

    /// The `.bzr/branch/format` marker for this format.
    pub fn format_string(&self) -> &'static [u8] {
        self.format_string
    }

    /// A human-readable description.
    pub fn get_format_description(&self) -> &'static str {
        self.description
    }

    /// Whether this crate can open branches of this format.
    pub fn is_supported(&self) -> bool {
        self.supported
    }
}

/// Registry entry, submitted by [`declare_branch_format!`].
pub struct BranchFormatRegistration(pub &'static BranchFormat);

inventory::collect!(BranchFormatRegistration);

/// Declare a branch format: define a `static` [`BranchFormat`] and register
/// it. Capability fields default to `false`; a declaration states only what
/// differs.
#[macro_export]
macro_rules! declare_branch_format {
    (
        $name:ident {
            format_string: $fmt:expr,
            description: $desc:expr,
            $( $field:ident : $value:expr, )*
        }
    ) => {
        pub static $name: $crate::branch::format::BranchFormat =
            $crate::branch::format::BranchFormat {
                format_string: $fmt,
                description: $desc,
                $( $field: $value, )*
                ..$crate::branch::format::BranchFormat::DEFAULT
            };

        inventory::submit! {
            $crate::branch::format::BranchFormatRegistration(&$name)
        }
    };
}

/// Look up a branch format by its `.bzr/branch/format` marker.
pub fn find_format(format_string: &[u8]) -> Option<&'static BranchFormat> {
    inventory::iter::<BranchFormatRegistration>
        .into_iter()
        .map(|r| r.0)
        .find(|f| f.format_string == format_string)
}

/// All declared branch formats.
pub fn all_formats() -> Vec<&'static BranchFormat> {
    inventory::iter::<BranchFormatRegistration>
        .into_iter()
        .map(|r| r.0)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_7_is_registered_and_supported() {
        let f = find_format(b"Bazaar Branch Format 7 (needs bzr 1.6)\n")
            .expect("branch format 7 registered");
        assert!(f.supports_tags);
        assert!(f.supports_stacking);
        // Format 7 also supports reference locations (it upgrades to 8 on set).
        assert!(f.supports_reference_locations);
        assert!(f.is_supported());
    }

    #[test]
    fn format_8_has_reference_locations() {
        let f = find_format(b"Bazaar Branch Format 8 (needs bzr 1.15)\n")
            .expect("branch format 8 registered");
        assert!(f.supports_reference_locations);
    }

    #[test]
    fn unknown_marker_is_none() {
        assert!(find_format(b"Bazaar nonsense branch\n").is_none());
    }
}

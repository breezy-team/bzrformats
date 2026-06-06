//! Control-directory format metadata and registry.
//!
//! Mirrors [`crate::repository::format`] and [`crate::branch::format`]: each
//! named `brz init --format=` combo is declared with
//! [`declare_bzrdir_format!`] and collected into a registry. A combo names
//! the repository, branch and working-tree format markers it creates; the
//! repository marker is looked up in the repository registry so creation
//! dispatches through that format's own `create` function.

/// A named control-directory format: the meta-directory layout plus the
/// markers for each component it creates. Mirrors an entry in breezy's
/// `controldir.format_registry` (e.g. "2a", "pack-0.92", "1.9").
#[derive(Debug, Clone, Copy)]
pub struct ControlDirFormat {
    /// The registry name (`brz init --format=<name>`).
    pub name: &'static str,
    /// The `.bzr/repository/format` marker of the repository to create.
    pub repo_marker: &'static [u8],
    /// The `.bzr/branch/format` marker of the branch to create.
    pub branch_marker: &'static [u8],
    /// The `.bzr/checkout/format` marker of the working tree to create.
    pub wt_marker: &'static [u8],
    /// Whether the working tree writes a `views` file (formats 6+).
    pub wt_has_views: bool,
}

impl ControlDirFormat {
    /// Baseline format with empty markers; the `..` base used by
    /// [`declare_bzrdir_format!`].
    pub const DEFAULT: ControlDirFormat = ControlDirFormat {
        name: "",
        repo_marker: b"",
        branch_marker: b"",
        wt_marker: b"",
        wt_has_views: false,
    };
}

/// Registry entry, submitted by [`declare_bzrdir_format!`].
pub struct ControlDirFormatRegistration(pub &'static ControlDirFormat);

inventory::collect!(ControlDirFormatRegistration);

/// Declare a control-directory format: define a `static` [`ControlDirFormat`]
/// and register it. A declaration states only the fields that differ from
/// [`ControlDirFormat::DEFAULT`].
#[macro_export]
macro_rules! declare_bzrdir_format {
    (
        $static_name:ident {
            $( $field:ident : $value:expr, )*
        }
    ) => {
        pub static $static_name: $crate::bzrdir::format::ControlDirFormat =
            $crate::bzrdir::format::ControlDirFormat {
                $( $field: $value, )*
                ..$crate::bzrdir::format::ControlDirFormat::DEFAULT
            };

        inventory::submit! {
            $crate::bzrdir::format::ControlDirFormatRegistration(&$static_name)
        }
    };
}

/// Look up a control-directory format by its `brz init --format=` name.
pub fn find_control_dir_format(name: &str) -> Option<&'static ControlDirFormat> {
    inventory::iter::<ControlDirFormatRegistration>
        .into_iter()
        .map(|r| r.0)
        .find(|f| f.name == name)
}

/// All declared control-directory formats.
pub fn control_dir_formats() -> Vec<&'static ControlDirFormat> {
    inventory::iter::<ControlDirFormatRegistration>
        .into_iter()
        .map(|r| r.0)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_2a_combo_is_registered() {
        let f = find_control_dir_format("2a").expect("2a combo registered");
        assert_eq!(f.repo_marker, super::super::REPOSITORY_FORMAT_2A);
        assert_eq!(f.branch_marker, super::super::BRANCH_FORMAT_7);
        assert_eq!(f.wt_marker, super::super::WORKINGTREE_FORMAT_6);
        assert!(f.wt_has_views);
    }

    #[test]
    fn the_knit_combo_pairs_branch_5_and_wt_3() {
        let f = find_control_dir_format("knit").expect("knit combo registered");
        assert_eq!(f.repo_marker, b"Bazaar-NG Knit Repository Format 1");
        assert_eq!(f.branch_marker, b"Bazaar-NG branch format 5\n");
        assert_eq!(f.wt_marker, b"Bazaar-NG Working Tree format 3");
        assert!(!f.wt_has_views);
    }

    #[test]
    fn every_combo_names_a_registered_repository_format() {
        for f in control_dir_formats() {
            assert!(
                crate::repository::find_format(f.repo_marker).is_some(),
                "combo {} names an unregistered repository marker",
                f.name
            );
        }
    }

    #[test]
    fn unknown_name_is_none() {
        assert!(find_control_dir_format("no-such-format").is_none());
    }
}

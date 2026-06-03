//! Declarations of all known branch formats.

use crate::declare_branch_format;

declare_branch_format! {
    FORMAT_5 {
        format_string: b"Bazaar-NG branch format 5\n",
        description: "Branch format 5 (full history)",
        supports_tags: false,
        deprecated: true,
    }
}

declare_branch_format! {
    FORMAT_6 {
        format_string: b"Bazaar Branch Format 6 (bzr 0.15)\n",
        description: "Branch format 6",
        supports_tags: true,
        supported: true,
    }
}

declare_branch_format! {
    FORMAT_7 {
        format_string: b"Bazaar Branch Format 7 (needs bzr 1.6)\n",
        description: "Branch format 7 (stackable)",
        supports_tags: true,
        supports_stacking: true,
        supported: true,
    }
}

declare_branch_format! {
    FORMAT_8 {
        format_string: b"Bazaar Branch Format 8 (needs bzr 1.15)\n",
        description: "Branch format 8 (reference locations)",
        supports_tags: true,
        supports_stacking: true,
        supports_reference_locations: true,
        supported: true,
    }
}

declare_branch_format! {
    REFERENCE_FORMAT_1 {
        format_string: b"Bazaar-NG Branch Reference Format 1\n",
        description: "Branch reference format 1",
        is_reference: true,
    }
}

//! Declarations of all known repository formats.
//!
//! Each [`declare_repository_format!`](crate::declare_repository_format)
//! both defines a `static` format and registers it with the format
//! [`registry`](super::format). Only `GroupCompress` (2a) is currently
//! `supported: true` for opening; the others are recognised (so a marker
//! is decoded as the right family and rejected cleanly when unsupported)
//! pending their decoder.

use super::format::StorageKind;
use crate::bencode_serializer::BEncodeRevisionSerializer1;
use crate::declare_repository_format;
use crate::xml_serializer::{
    Chk255BigPageInventorySerializer, XMLInventorySerializer5, XMLInventorySerializer6,
    XMLInventorySerializer7, XMLRevisionSerializer5,
};

declare_repository_format! {
    FORMAT_KNIT_1 {
        format_string: b"Bazaar-NG Knit Repository Format 1",
        description: "Knit repository format 1",
        storage: StorageKind::Knit,
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer5,
        deprecated: true,
    }
}

declare_repository_format! {
    FORMAT_KNIT_3 {
        format_string: b"Bazaar Knit Repository Format 3 (bzr 0.15)\n",
        description: "Knit repository format 3 (rich root, subtrees)",
        storage: StorageKind::Knit,
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer7,
        rich_root_data: true,
        supports_tree_reference: true,
        deprecated: true,
    }
}

declare_repository_format! {
    FORMAT_KNIT_4 {
        format_string: b"Bazaar Knit Repository Format 4 (bzr 1.0)\n",
        description: "Knit repository format 4 (rich root)",
        storage: StorageKind::Knit,
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer6,
        rich_root_data: true,
        deprecated: true,
    }
}

declare_repository_format! {
    FORMAT_KNIT_PACK_1 {
        format_string: b"Bazaar pack repository format 1 (needs bzr 0.92)\n",
        description: "Pack repository format 1",
        storage: StorageKind::KnitPack,
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer5,
    }
}

declare_repository_format! {
    FORMAT_KNIT_PACK_3 {
        format_string: b"Bazaar pack repository format 1 with subtree support (needs bzr 0.92)\n",
        description: "Pack repository format 1 with subtree support",
        storage: StorageKind::KnitPack,
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer7,
        rich_root_data: true,
        supports_tree_reference: true,
    }
}

declare_repository_format! {
    FORMAT_KNIT_PACK_4 {
        format_string: b"Bazaar pack repository format 1 with rich root (needs bzr 1.0)\n",
        description: "Pack repository format 1 with rich root",
        storage: StorageKind::KnitPack,
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer6,
        rich_root_data: true,
    }
}

declare_repository_format! {
    FORMAT_KNIT_PACK_5 {
        format_string: b"Bazaar RepositoryFormatKnitPack5 (bzr 1.6)\n",
        description: "Pack repository format 5 (stackable)",
        storage: StorageKind::KnitPack,
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer5,
        supports_external_lookups: true,
    }
}

declare_repository_format! {
    FORMAT_KNIT_PACK_5_RICH_ROOT {
        format_string: b"Bazaar RepositoryFormatKnitPack5RichRoot (bzr 1.6.1)\n",
        description: "Pack repository format 5 with rich root (stackable)",
        storage: StorageKind::KnitPack,
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer6,
        rich_root_data: true,
        supports_external_lookups: true,
    }
}

declare_repository_format! {
    FORMAT_KNIT_PACK_5_RICH_ROOT_BROKEN {
        format_string: b"Bazaar RepositoryFormatKnitPack5RichRoot (bzr 1.6)\n",
        description: "Pack repository format 5 with rich root (broken)",
        storage: StorageKind::KnitPack,
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer6,
        rich_root_data: true,
        supports_external_lookups: true,
        deprecated: true,
    }
}

declare_repository_format! {
    FORMAT_KNIT_PACK_6 {
        format_string: b"Bazaar RepositoryFormatKnitPack6 (bzr 1.9)\n",
        description: "Pack repository format 6 (btree indexes, stackable)",
        storage: StorageKind::KnitPack,
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer5,
        supports_external_lookups: true,
    }
}

declare_repository_format! {
    FORMAT_KNIT_PACK_6_RICH_ROOT {
        format_string: b"Bazaar RepositoryFormatKnitPack6RichRoot (bzr 1.9)\n",
        description: "Pack repository format 6 with rich root (btree, stackable)",
        storage: StorageKind::KnitPack,
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer6,
        rich_root_data: true,
        supports_external_lookups: true,
    }
}

declare_repository_format! {
    FORMAT_2A {
        format_string: b"Bazaar repository format 2a (needs bzr 1.16 or later)\n",
        description: "Repository format 2a (groupcompress, CHK)",
        storage: StorageKind::GroupCompress,
        revision_serializer: &BEncodeRevisionSerializer1,
        inventory_serializer: &Chk255BigPageInventorySerializer,
        rich_root_data: true,
        supports_chks: true,
        supports_tree_reference: true,
        supports_external_lookups: true,
        supported: true,
    }
}

declare_repository_format! {
    FORMAT_2A_SUBTREE {
        format_string: b"Bazaar development format 8\n",
        description: "Repository format 2a with subtree support",
        storage: StorageKind::GroupCompress,
        revision_serializer: &BEncodeRevisionSerializer1,
        inventory_serializer: &Chk255BigPageInventorySerializer,
        rich_root_data: true,
        supports_chks: true,
        supports_tree_reference: true,
        supports_external_lookups: true,
        supported: true,
    }
}

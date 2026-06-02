//! Repository access: format metadata/registry plus the 2a reader/writer.

pub mod format;
mod formats;
mod pack_2a;
mod pack_2a_writer;
mod pack_knit;

pub use format::{
    all_formats, find_format, InventorySerializerKind, RepositoryFormat, RevisionSerializerKind,
    StorageKind,
};
pub use pack_2a::{Pack2aRepository, RepositoryError, SharedTransport};
pub use pack_knit::KnitPackRepository;

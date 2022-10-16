use std::path::PathBuf;

/// Index into `open_tables`.
pub struct TableId {
    id: usize,
}

/// A table is just a directory under the root path.
pub struct Table {
    pub path: PathBuf,
}

impl TableId {
    pub fn new(id: usize) -> Self {
        Self { id }
    }
}

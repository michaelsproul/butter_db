use std::path::PathBuf;

/// Index into `open_tables`.
pub struct TableId {
    pub id: usize,
}

/// A table is just a directory under the snapshot path.
pub struct Table {
    pub path: PathBuf,
}

impl TableId {
    pub fn new(id: usize) -> Self {
        Self { id }
    }
}

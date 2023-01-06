use crate::IndexFile;
use faster_hex::hex_string;
use std::path::PathBuf;

/// Index into `open_tables`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableId {
    pub id: usize,
}

/// A table is a directory under `/{generation}/{table_name}` containing an index file.
#[derive(Debug)]
pub struct Table {
    pub path: PathBuf,
    pub index_file: IndexFile,
}

impl Table {
    /// Path to the file for a key.
    ///
    /// Keys are encoded to ensure the path is filesystem safe.
    pub fn key_path(&self, key: &[u8]) -> PathBuf {
        let encoded_key = hex_string(key);
        self.path.join(encoded_key)
    }
}

impl TableId {
    pub fn new(id: usize) -> Self {
        Self { id }
    }
}

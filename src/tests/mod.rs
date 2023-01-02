#![cfg(test)]
mod basic;
mod cursor;

use std::path::PathBuf;
use tempfile::{tempdir_in, TempDir};

/// User nominated test directory.
///
/// This *must* be a BTRFS volume.
pub fn test_dir() -> PathBuf {
    PathBuf::from("/mnt/database/")
}

/// New temporary directory in `test_dir`.
pub fn test_root() -> TempDir {
    tempdir_in(test_dir()).unwrap()
}

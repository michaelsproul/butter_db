use btrfsutil::error::BtrfsUtilError;
use std::io;

#[derive(Debug)]
pub enum Error {
    Oops,
    Btrfs(BtrfsUtilError),
    Io(io::Error),
    Sqlite(sqlite::Error),
}

impl From<BtrfsUtilError> for Error {
    fn from(e: BtrfsUtilError) -> Self {
        Self::Btrfs(e)
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<sqlite::Error> for Error {
    fn from(e: sqlite::Error) -> Self {
        Self::Sqlite(e)
    }
}

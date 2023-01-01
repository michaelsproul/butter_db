use btrfsutil::error::BtrfsUtilError;
use std::io;

#[derive(Debug)]
pub enum Error {
    Oops,
    Btrfs(BtrfsUtilError),
    Io(io::Error),
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

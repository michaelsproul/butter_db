use crate::{util::key_from_hex_bytes, Error};
use std::borrow::Cow;
use std::fs::{File, ReadDir};
use std::io::Read;
use std::marker::PhantomData;
use std::os::unix::ffi::OsStrExt;

pub type OwnedKey = Vec<u8>;
pub type OwnedValue = Vec<u8>;

pub type Key<'a> = Cow<'a, [u8]>;
pub type Value<'a> = Cow<'a, [u8]>;

pub struct Cursor<'txn> {
    /// Directory iterator for `table`.
    read_dir: ReadDir,
    /// Most recently read entry or `None` if no entry has been read yet.
    current_key_value: Option<(OwnedKey, OwnedValue)>,
    /// Is the cursor positioned at the first key?
    is_at_first_key: bool,
    _phantom: PhantomData<&'txn ()>,
}

impl<'txn> Cursor<'txn> {
    pub fn first_key(&mut self) -> Result<Option<Key>, Error> {
        if let Some((ref k, _)) = self.current_key_value {
            if self.is_at_first_key {
                Ok(Some(Cow::Borrowed(k)))
            } else {
                Err(Error::Oops)
            }
        } else {
            self.next_key()
        }
    }

    pub fn next_key(&mut self) -> Result<Option<Key>, Error> {
        let Some(new_entry) = self.read_dir.next().transpose()? else {
            // End of the iterator.
            return Ok(None)
        };

        // Parse key and value from filename + contents.
        let key = key_from_hex_bytes(new_entry.file_name().as_bytes())?;
        let mut file = File::open(new_entry.path())?;

        let mut value = vec![];
        file.read_to_end(&mut value)?;

        // Update the current key-value.
        let prev_kv = std::mem::replace(&mut self.current_key_value, Some((key.clone(), value)));

        // Only if the previous KV was `None` are we still at the first key-value.
        self.is_at_first_key = prev_kv.is_none();

        Ok(Some(Cow::Owned(key)))
    }
}

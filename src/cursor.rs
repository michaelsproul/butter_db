use crate::{Error, Table};
use sqlite::CursorWithOwnership as SqliteCursor;
use std::borrow::Cow;
use std::fs::File;
use std::io::Read;

pub type OwnedKey = Vec<u8>;
pub type OwnedValue = Vec<u8>;

pub type Key<'a> = Cow<'a, [u8]>;
pub type Value<'a> = Cow<'a, [u8]>;

pub struct Cursor<'txn> {
    table: &'txn Table,
    /// Iterator over index file.
    rows: SqliteCursor<'txn>,
    /// Most recently read entry or `None` if no entry has been read yet.
    current_key_value: Option<(OwnedKey, OwnedValue)>,
    /// Is the cursor positioned at the first key?
    is_at_first_key: bool,
}

impl<'txn> Cursor<'txn> {
    pub fn new(table: &'txn Table) -> Result<Self, Error> {
        let rows = table
            .index_file
            .conn
            .prepare("SELECT key FROM keys ORDER BY key ASC")?
            .into_iter();
        Ok(Cursor {
            table,
            rows,
            current_key_value: None,
            is_at_first_key: true,
        })
    }

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
        let Some(new_row) = self.rows.next().transpose()? else {
            // End of the iterator.
            self.is_at_first_key = false;
            return Ok(None)
        };

        // Parse key and value from filename + contents.
        let key = new_row.try_read::<&[u8], _>(0)?.to_vec();
        let mut file = File::open(self.table.key_path(&key))?;

        let mut value = vec![];
        file.read_to_end(&mut value)?;

        // Update the current key-value.
        let prev_kv = std::mem::replace(&mut self.current_key_value, Some((key.clone(), value)));

        // Only if the previous KV was `None` are we still at the first key-value.
        self.is_at_first_key = prev_kv.is_none();

        Ok(Some(Cow::Owned(key)))
    }

    pub fn get_current(&mut self) -> Result<Option<(Key, Value)>, Error> {
        if self.current_key_value.is_none() && self.is_at_first_key {
            self.next_key()?;
        }

        if let Some((k, v)) = &self.current_key_value {
            Ok(Some((Cow::Borrowed(k), Cow::Borrowed(v))))
        } else {
            Ok(None)
        }
    }
}

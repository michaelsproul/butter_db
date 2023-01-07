use crate::{Error, Table};
use derivative::Derivative;
use sqlite::CursorWithOwnership as SqliteCursor;
use std::borrow::Cow;
use std::fs::{self, File};
use std::io::{self, Read};

pub type OwnedKey = Vec<u8>;
pub type OwnedValue = Vec<u8>;

pub type Key<'a> = Cow<'a, [u8]>;
pub type Value<'a> = Cow<'a, [u8]>;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct Cursor<'txn> {
    table: &'txn Table,
    /// Iterator over index file.
    #[derivative(Debug = "ignore")]
    rows: Option<SqliteCursor<'txn>>,
    /// Most recently read key or `None` if no entry has been read yet.
    current_key: Option<OwnedKey>,
    /// The value corresponding to `current_key`, or `None` if it hasn't been loaded yet.
    current_value: Option<OwnedValue>,
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
            rows: Some(rows),
            current_key: None,
            current_value: None,
            is_at_first_key: true,
        })
    }

    pub fn first_key(&mut self) -> Result<Option<Key>, Error> {
        if let Some(ref k) = self.current_key {
            if self.is_at_first_key {
                Ok(Some(Cow::Borrowed(k)))
            } else {
                Err(Error::Oops)
            }
        } else {
            self.next_key()
        }
    }

    pub fn last_key(&mut self) -> Result<Option<Key>, Error> {
        let opt_key = self.table.index_file.last_key()?;

        self.current_key = opt_key.clone();
        self.current_value = None;
        self.rows = None;

        Ok(opt_key.map(Cow::Owned))
    }

    pub fn next_key(&mut self) -> Result<Option<Key>, Error> {
        let Some(new_row) = self.rows.as_mut().and_then(|rows| rows.next()).transpose()? else {
            // End of the iterator.
            self.is_at_first_key = false;
            return Ok(None)
        };

        // Parse the key from the row.
        let key = new_row.try_read::<&[u8], _>(0)?.to_vec();

        // Update the current key and value.
        let prev_key = std::mem::replace(&mut self.current_key, Some(key.clone()));
        self.current_value = None;

        // Only if the previous key was `None` are we still at the first key.
        self.is_at_first_key = prev_key.is_none();

        Ok(Some(Cow::Owned(key)))
    }

    pub fn get_current(&mut self) -> Result<Option<(Key, Value)>, Error> {
        if self.current_key.is_none() && self.is_at_first_key {
            self.next_key()?;
        }

        if let Some(key) = &self.current_key {
            if self.current_value.is_none() {
                let mut file = File::open(self.table.key_path(key))?;
                let mut value = vec![];
                file.read_to_end(&mut value)?;
                self.current_value = Some(value);
            }
            let value = self
                .current_value
                .as_deref()
                .map(Cow::Borrowed)
                .ok_or(Error::Oops)?;
            Ok(Some((Cow::Borrowed(key), value)))
        } else {
            Ok(None)
        }
    }

    pub fn delete_current(&mut self) -> Result<(), Error> {
        let Some(key) = &self.current_key else {
            return Ok(());
        };

        // Delete from the index.
        self.table.index_file.delete_key(key)?;

        // Delete from disk.
        let key_path = self.table.key_path(key);
        fs::remove_file(key_path).or_else(|e| {
            if e.kind() == io::ErrorKind::NotFound {
                Ok(())
            } else {
                Err(e)
            }
        })?;

        // Erase from cursor.
        self.current_value = None;

        Ok(())
    }
}

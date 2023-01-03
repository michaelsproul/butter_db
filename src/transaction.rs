use crate::{Cursor, Error, IndexFile, Snapshot, Table, TableId};
use parking_lot::{MutexGuard, RwLock};
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

pub struct Transaction<'a> {
    pub(crate) read_snapshot: &'a RwLock<Snapshot>,
    pub(crate) txn_lock: MutexGuard<'a, ()>,
    pub(crate) write_snapshot: Snapshot,
    pub(crate) open_tables: Vec<Table>,
}

impl<'a> Transaction<'a> {
    pub fn commit(self) -> Result<(), Error> {
        // Obtain a write lock on the read snapshot, ensuring there are no readers active.
        let mut read_snapshot = self.read_snapshot.write();

        // Update the read snapshot with the results of the current transaction.
        let old_read_snapshot = std::mem::replace(&mut *read_snapshot, self.write_snapshot);

        // Delete the previous read snapshot from disk.
        // FIXME(sproul): this is probably slow, could delete in the background.
        fs::remove_dir_all(&old_read_snapshot.path)?;

        // Drop write lock on `read_snapshot`, allowing new readers to observe the changes.
        drop(read_snapshot);

        // Drop transaction lock, allowing new write transactions to begin.
        drop(self.txn_lock);

        Ok(())
    }

    /// Path to the directory for a table.
    ///
    /// Assume table names are filesystem safe.
    fn table_path(&self, name: &str) -> PathBuf {
        self.write_snapshot.path.join(name)
    }

    /// Path to the index file for a table.
    fn index_file_path(table_path: &Path) -> PathBuf {
        table_path.join("index.sqlite")
    }

    /// Create a table in the database with `name`.
    ///
    /// Return the ID of the table.
    pub fn create_table(&mut self, name: &str) -> Result<TableId, Error> {
        let path = self.table_path(name);
        fs::create_dir(&path)?;

        let index_file = IndexFile::create(Self::index_file_path(&path))?;

        let id = TableId::new(self.open_tables.len());
        self.open_tables.push(Table { path, index_file });

        Ok(id)
    }

    // TODO(sproul): consider using interior mutabilty to enable returning a `Table`.
    pub fn open_table(&mut self, name: &str) -> Result<TableId, Error> {
        let path = self.table_path(name);

        if path.is_dir() {
            let id = TableId::new(self.open_tables.len());
            let index_file = IndexFile::open(Self::index_file_path(&path))?;
            self.open_tables.push(Table { path, index_file });
            Ok(id)
        } else {
            Err(Error::Oops)
        }
    }

    pub fn get_table(&self, id: TableId) -> Result<&Table, Error> {
        self.open_tables.get(id.id).ok_or(Error::Oops)
    }

    pub fn put(&self, table: &Table, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let key_path = table.key_path(key);
        let mut key_file = File::create(&key_path)?;
        key_file.write_all(value)?;
        table.index_file.put_key(key)?;
        Ok(())
    }

    pub fn get(&self, table: &Table, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let key_path = table.key_path(key);
        let mut key_file = match File::open(key_path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let mut bytes = vec![];
        key_file.read_to_end(&mut bytes)?;
        Ok(Some(bytes))
    }

    pub fn delete(&self, table: &Table, key: &[u8]) -> Result<(), Error> {
        let key_path = table.key_path(key);
        fs::remove_file(key_path).or_else(|e| {
            if e.kind() == io::ErrorKind::NotFound {
                Ok(())
            } else {
                Err(e)
            }
        })?;
        table.index_file.delete_key(key)
    }

    pub fn cursor<'b>(&'b self, table: &'a Table) -> Result<Cursor<'b>, Error> {
        Cursor::new(table)
    }
}

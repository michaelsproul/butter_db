use crate::Generation;
use parking_lot::MutexGuard;
use std::path::Path;

pub struct Transaction<'a> {
    read_gen: &'a RwLock<Generation>,
    write_gen: MutexGuard<'a, Generation>,
    open_tables: Vec<Table>,
    root_path: &'a Path,
}

impl Transaction<'a> {
    pub fn commit(self) -> Result<(), Error> {
        // Update read snapshot with results of current transaction.
        let read_gen = self.read_gen.write();
        *read_gen = *self.write_gen;

        // TODO: could sync here

        // Leave write generation the same as the read generation, it will be incremented
        // on next transaction.
        drop(read_gen);
        drop(self.write_gen);

        Ok(())
    }

    /// Path to the directory for a table.
    ///
    /// Assume table names are filesystem safe.
    fn table_path(&self, name: &str) -> PathBuf {
        self.root_path.join(self.write_gen.as_str()).join(name)
    }

    /// Path to the file for a key.
    ///
    /// Keys are encoded to ensure the path is filesystem safe.
    fn key_path(&self, table: &Table, key: &[u8]) -> Result<PathBuf, Error> {
        let encoded_key = 
        table.path.join(
    }

    pub fn create_table(&mut self, name: &str) -> Result<TableId, Error> {
        let path = self.table_path(name);
        fs::create_dir(&path)?;

        let id = TableId::new(self.open_tables.len());
        self.open_tables.push(Table { path });

        Ok(id)
    }
}

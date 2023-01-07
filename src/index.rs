use crate::Error;
use derivative::Derivative;
use sqlite::Connection;
use std::path::PathBuf;

/// An index is an ordered list of keys for a table stored as an SQLite database on disk.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct IndexFile {
    #[derivative(Debug = "ignore")]
    pub(crate) conn: Connection,
    #[allow(dead_code)]
    path: PathBuf,
}

impl IndexFile {
    pub fn create(path: PathBuf) -> Result<Self, Error> {
        let conn = Connection::open(&path)?;
        Self::apply_pragmas(&conn)?;

        // Create the index table.
        // FIXME(sproul): benchmark default vs WITHOUT ROWID
        conn.execute(
            "CREATE TABLE keys (
                key BLOB PRIMARY KEY ASC
            ) WITHOUT ROWID",
        )?;

        Ok(Self { conn, path })
    }

    pub fn open(path: PathBuf) -> Result<Self, Error> {
        let conn = Connection::open(&path)?;
        Self::apply_pragmas(&conn)?;
        Ok(Self { conn, path })
    }

    fn apply_pragmas(conn: &Connection) -> Result<(), Error> {
        conn.execute("PRAGMA journal_mode=MEMORY")?;
        Ok(())
    }

    // Turn the journal off completely. We don't need SQLite's atomic commit because we
    // use BTRFS snapshots for atomicity.
    // FIXME(sproul): try some benchmarks with journal ON/OFF and transactions ON/OFF.
    /*
    fn apply_pragmas(conn: &mut Connection) -> Result<(), Error> {
        conn.pragma_update(None, "journal_mode", &"OFF")?;
        Ok(())
    }
    */

    /// Ensure that `key` is present in the index file.
    // FIXME(sproul): consider bulking in a transaction
    pub fn put_key(&self, key: &[u8]) -> Result<(), Error> {
        let mut stmt = self
            .conn
            .prepare("INSERT INTO keys VALUES (?1) ON CONFLICT DO NOTHING")?;
        stmt.bind((1, key))?;
        stmt.into_iter().collect::<Result<Vec<_>, _>>()?;
        Ok(())
    }

    /// Remove `key` from the index file.
    pub fn delete_key(&self, key: &[u8]) -> Result<(), Error> {
        let mut stmt = self.conn.prepare("DELETE FROM keys WHERE key = ?1")?;
        stmt.bind((1, key))?;
        stmt.into_iter().collect::<Result<Vec<_>, _>>()?;
        Ok(())
    }

    pub fn last_key(&self) -> Result<Option<Vec<u8>>, Error> {
        let stmt = self.conn.prepare("SELECT MAX(key) FROM keys")?;
        for maybe_row in stmt {
            let row = maybe_row?;
            let key: Option<&[u8]> = row.try_read(0)?;
            return Ok(key.map(|slice| slice.to_vec()));
        }
        Ok(None)
    }
}

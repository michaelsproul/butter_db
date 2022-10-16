use crate::{Error, Table, Transaction};
use parking_lot::{Mutex, RwLock};
use std::path::PathBuf;
use strum::EnumString;

#[derive(Debug, Copy, Default, EnumString)]
#[strum(serialize_all = "lower")]
pub enum Generation {
    #[default]
    Tick,
    Tock,
}

impl Generation {
    pub fn increment(&mut self) {
        match self {
            Self::Tick => *self = Self::Tock,
            Self::Tock => *self = Self::Tick,
        }
    }
}

pub struct Database {
    // Lock order: `write_gen` must always acquired before `read_gen`.
    read_gen: RwLock<Generation>,
    write_gen: Mutex<Generation>,
    root_path: PathBuf,
}

impl Database {
    pub fn begin_transaction(&self) -> Result<Transaction, Error> {
        let write_gen = self.write_gen.lock();
        write_gen.increment();

        // TODO: snapshot current directory

        Ok(Transaction {
            read_gen: &self.read_gen,
            write_gen,
            root_path: &self.root_path,
        })
    }
}

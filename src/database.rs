use crate::{Error, Transaction};
use btrfsutil::bindings::{btrfs_util_create_snapshot_fd2, btrfs_util_error_BTRFS_UTIL_OK};
use btrfsutil::subvolume::Subvolume;
use parking_lot::{Mutex, RwLock};
use std::ffi::CString;
use std::fs::File;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use strum::AsRefStr;

#[derive(Debug, Copy, Clone, Default, AsRefStr)]
#[strum(serialize_all = "lowercase")]
pub enum Generation {
    #[default]
    Tick,
    Tock,
}

impl Generation {
    pub fn incremented(self) -> Self {
        match self {
            Self::Tick => Self::Tock,
            Self::Tock => Self::Tick,
        }
    }
}

/// Snapshot of the database at a specific version including `Generation` and `Subvolume`.
pub struct Snapshot {
    pub(crate) gen: Generation,
    pub(crate) path: PathBuf,
    // FIXME(sproul): replace this by a better wrapper
    #[allow(dead_code)]
    pub(crate) subvolume: Subvolume,
}

pub struct Database {
    // Lock order: `write_gen` must always acquired before `read_gen`.
    read_snapshot: RwLock<Snapshot>,
    txn_lock: Mutex<()>,
    root_path: PathBuf,
    tick_path: PathBuf,
    tock_path: PathBuf,
}

impl Database {
    pub fn create(root_path: PathBuf) -> Result<Self, Error> {
        let tick_path = Self::calc_gen_path(&root_path, Generation::Tick);
        let tock_path = Self::calc_gen_path(&root_path, Generation::Tock);

        let read_snapshot = RwLock::new(Snapshot {
            gen: Generation::Tick,
            path: tick_path.clone(),
            subvolume: Subvolume::create(tick_path.clone(), None)?,
        });

        Ok(Self {
            read_snapshot,
            txn_lock: Mutex::new(()),
            root_path,
            tick_path,
            tock_path,
        })
    }

    pub fn begin_transaction(&self) -> Result<Transaction, Error> {
        let txn_lock = self.txn_lock.lock();

        // Clone the read subvolume, creating a new subvolume for writing.
        // Increment the generation.
        let read_snapshot = self.read_snapshot.read();

        let write_gen = read_snapshot.gen.incremented();
        let write_path = self.gen_path(write_gen).clone();

        // FIXME(sproul): write a better wrapper for this. The `btrfsutil` crate is unsuitable
        // because it frequently resolves subvolumes to paths, which fails unless the CAP_SYS_ADMIN
        // capability is held (it's also completely unnecessary).
        let read_file = File::open(&read_snapshot.path)?;
        let parent_file = File::open(&self.root_path)?;
        let name = CString::new(write_gen.as_ref()).unwrap();
        let res = unsafe {
            btrfs_util_create_snapshot_fd2(
                read_file.as_raw_fd(),
                parent_file.as_raw_fd(),
                name.as_ptr(),
                0,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        assert_eq!(res, btrfs_util_error_BTRFS_UTIL_OK);

        let write_subvolume = Subvolume::get(&write_path).unwrap();

        let write_snapshot = Snapshot {
            gen: write_gen,
            path: write_path,
            subvolume: write_subvolume,
        };

        Ok(Transaction {
            read_snapshot: &self.read_snapshot,
            txn_lock,
            write_snapshot,
            open_tables: vec![],
        })
    }

    /// Return the filesystem path for a given generation.
    fn gen_path(&self, generation: Generation) -> &PathBuf {
        match generation {
            Generation::Tick => &self.tick_path,
            Generation::Tock => &self.tock_path,
        }
    }

    fn calc_gen_path(root_path: &Path, generation: Generation) -> PathBuf {
        root_path.join(generation.as_ref())
    }
}

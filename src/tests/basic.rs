use crate::Database;
use std::path::PathBuf;
use tempfile::tempdir_in;

fn testdir() -> PathBuf {
    PathBuf::from("/mnt/database/")
}

#[test]
fn read_write_read() {
    let root_path = tempdir_in(testdir()).unwrap();
    let db = Database::create(root_path.path().to_path_buf()).unwrap();
    /*
    let root_path = testdir();
    let db = Database::create(root_path.clone()).unwrap();
    */

    let mut txn = db.begin_transaction().unwrap();

    let table_id = txn.create_table("michael").unwrap();
    let t = txn.get_table(table_id).unwrap();
    txn.put(&t, &[0], &[1]).unwrap();
    txn.put(&t, &[1], &[2]).unwrap();

    let v0 = txn.get(&t, &[0]).unwrap().unwrap();
    let v1 = txn.get(&t, &[1]).unwrap().unwrap();

    assert_eq!(v0, &[1]);
    assert_eq!(v1, &[2]);

    txn.commit().unwrap();

    std::mem::forget(root_path);
}

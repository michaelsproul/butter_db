use super::test_root;
use crate::Database;

#[test]
fn read_write_read() {
    let root_path = test_root();
    let db = Database::create(root_path.path().to_path_buf()).unwrap();

    let mut txn = db.begin_transaction().unwrap();

    let tid = txn.create_table("michael").unwrap();
    let t = txn.get_table(tid).unwrap();
    txn.put(&t, &[0], &[1]).unwrap();
    txn.put(&t, &[1], &[2]).unwrap();

    let v0 = txn.get(&t, &[0]).unwrap().unwrap();
    let v1 = txn.get(&t, &[1]).unwrap().unwrap();

    assert_eq!(v0, &[1]);
    assert_eq!(v1, &[2]);

    txn.commit().unwrap();
}

#[test]
fn delete() {
    let root_path = test_root();
    let db = Database::create(root_path.path().to_path_buf()).unwrap();

    let mut txn = db.begin_transaction().unwrap();

    let tid = txn.create_table("t0").unwrap();
    let t = txn.get_table(tid).unwrap();

    txn.delete(&t, &[0]).unwrap();
    txn.put(&t, &[0], &[255, 255, 255, 255]).unwrap();

    txn.commit().unwrap();

    let mut txn = db.begin_transaction().unwrap();
    let tid = txn.open_table("t0").unwrap();
    let t = txn.get_table(tid).unwrap();

    let v0 = txn.get(&t, &[0]).unwrap();
    assert_eq!(v0, Some(vec![255, 255, 255, 255]));

    txn.delete(&t, &[0]).unwrap();
    let v0_deleted = txn.get(&t, &[0]).unwrap();
    assert_eq!(v0_deleted, None);

    txn.commit().unwrap();
}

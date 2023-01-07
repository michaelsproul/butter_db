use super::test_root;
use crate::Database;

#[test]
fn insert_then_iterate() {
    let root_path = test_root();
    let db = Database::create(root_path.path().to_path_buf()).unwrap();

    // Create some data (out of order).
    let mut txn = db.begin_transaction().unwrap();
    let table_id = txn.create_table("test").unwrap();
    let t = txn.get_table(table_id).unwrap();

    txn.put(&t, &[3], &[33]).unwrap();
    txn.put(&t, &[1], &[11]).unwrap();
    txn.put(&t, &[0], &[0]).unwrap();
    txn.put(&t, &[2], &[22]).unwrap();

    // Read the data using a cursor.
    let mut cursor = txn.cursor(t).unwrap();

    assert_eq!(&*cursor.first_key().unwrap().unwrap(), &[0]);

    for i in 0..4 {
        let (k, v) = cursor.get_current().unwrap().unwrap();
        assert_eq!(*k, [i]);
        assert_eq!(*v, [i * 11]);
        cursor.next_key().unwrap();
    }

    // Check `last_key`.
    assert_eq!(*cursor.last_key().unwrap().unwrap(), [3]);
    let (k, v) = cursor.get_current().unwrap().unwrap();
    assert_eq!(*k, [3]);
    assert_eq!(*v, [33]);
}

#[test]
fn empty_db_first_key() {
    let root_path = test_root();
    let db = Database::create(root_path.path().to_path_buf()).unwrap();

    let mut txn = db.begin_transaction().unwrap();
    let table_id = txn.create_table("test").unwrap();
    let t = txn.get_table(table_id).unwrap();

    let mut cursor = txn.cursor(t).unwrap();

    // First key should be `None`.
    assert_eq!(cursor.first_key().unwrap(), None);
}

#[test]
fn empty_db_last_key() {
    let root_path = test_root();
    let db = Database::create(root_path.path().to_path_buf()).unwrap();

    let mut txn = db.begin_transaction().unwrap();
    let table_id = txn.create_table("test").unwrap();
    let t = txn.get_table(table_id).unwrap();

    let mut cursor = txn.cursor(t).unwrap();

    // Last key should be `None`.
    assert_eq!(cursor.last_key().unwrap(), None);
}

#[test]
fn empty_db_iter() {
    let root_path = test_root();
    let db = Database::create(root_path.path().to_path_buf()).unwrap();

    let mut txn = db.begin_transaction().unwrap();
    let table_id = txn.create_table("test").unwrap();
    let t = txn.get_table(table_id).unwrap();

    let mut cursor = txn.cursor(t).unwrap();

    assert_eq!(cursor.next_key().unwrap(), None);
    assert_eq!(cursor.get_current().unwrap(), None);
}

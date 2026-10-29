use super::_test_data::*;
use super::*;
use std::ptr;
use std::sync::{Arc, Mutex};

#[test]
fn test_new() {
    let manager = DataConnManager::new();
    assert!(manager.vec.is_empty());
    assert!(manager.index_map.is_empty());
    assert_eq!(manager.ordered_names.len(), 0);
    assert!(!manager.committed);
}

#[test]
fn test_with_commit_order() {
    let manager = DataConnManager::with_commit_order(&["bar", "baz", "foo"]);

    assert_eq!(manager.vec.len(), 3);
    assert!(manager.vec[0].is_none());
    assert!(manager.vec[1].is_none());
    assert!(manager.vec[2].is_none());
    assert_eq!(manager.index_map.len(), 3);
    assert_eq!(*manager.index_map.get("foo").unwrap(), 2);
    assert_eq!(*manager.index_map.get("bar").unwrap(), 0);
    assert_eq!(*manager.index_map.get("baz").unwrap(), 1);
    assert_eq!(manager.ordered_names.len(), 3);
    assert_eq!(manager.ordered_names[0], "bar".into());
    assert_eq!(manager.ordered_names[1], "baz".into());
    assert_eq!(manager.ordered_names[2], "foo".into());
    assert!(!manager.committed);
}

#[test]
fn test_with_empty_commit_order() {
    let manager = DataConnManager::with_commit_order(&[]);

    assert_eq!(manager.vec.len(), 0);
    assert_eq!(manager.index_map.len(), 0);
    assert_eq!(manager.ordered_names.len(), 0);
    assert!(!manager.committed);
}

#[test]
fn test_new_and_add() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    let mut manager = DataConnManager::new();

    assert!(manager.vec.is_empty());
    assert!(manager.index_map.is_empty());
    assert_eq!(manager.ordered_names.len(), 0);
    assert!(!manager.committed);

    let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
    let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
    let nnptr0 = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
    let ssnnptr = SendSyncNonNull::new(nnptr0);
    manager.add(ssnnptr);

    assert_eq!(manager.vec.len(), 1);
    assert_eq!(manager.vec[0].clone().unwrap().non_null_ptr, nnptr0);
    assert_eq!(manager.index_map.len(), 1);
    assert_eq!(*manager.index_map.get("foo").unwrap(), 0);
    assert_eq!(manager.ordered_names.len(), 0);
    assert!(!manager.committed);

    let conn = AsyncDataConn::new(2, logger.clone(), Fail::None);
    let boxed = Box::new(DataConnContainer::new("bar".to_string(), Box::new(conn)));
    let nnptr1 = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
    let ssnnptr = SendSyncNonNull::new(nnptr1);
    manager.add(ssnnptr);

    assert_eq!(manager.vec.len(), 2);
    assert_eq!(manager.vec[0].clone().unwrap().non_null_ptr, nnptr0);
    assert_eq!(manager.vec[1].clone().unwrap().non_null_ptr, nnptr1);
    assert_eq!(manager.index_map.len(), 2);
    assert_eq!(*manager.index_map.get("foo").unwrap(), 0);
    assert_eq!(*manager.index_map.get("bar").unwrap(), 1);
    assert_eq!(manager.ordered_names.len(), 0);
    assert!(!manager.committed);
}

#[test]
fn test_new_and_add_when_overlapping_name() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    let mut manager = DataConnManager::new();

    assert!(manager.vec.is_empty());
    assert!(manager.index_map.is_empty());
    assert_eq!(manager.ordered_names.len(), 0);
    assert!(!manager.committed);

    let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
    let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
    let nnptr0 = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
    let ssnnptr = SendSyncNonNull::new(nnptr0);
    manager.add(ssnnptr);

    assert_eq!(manager.vec.len(), 1);
    assert_eq!(manager.vec[0].clone().unwrap().non_null_ptr, nnptr0);
    assert_eq!(manager.index_map.len(), 1);
    assert_eq!(*manager.index_map.get("foo").unwrap(), 0);
    assert_eq!(manager.ordered_names.len(), 0);
    assert!(!manager.committed);

    let conn = AsyncDataConn::new(2, logger.clone(), Fail::None);
    let boxed = Box::new(DataConnContainer::new("foo".to_string(), Box::new(conn)));
    let nnptr1 = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
    let ssnnptr = SendSyncNonNull::new(nnptr1);
    manager.add(ssnnptr);

    assert_eq!(manager.vec.len(), 1);
    assert_eq!(manager.vec[0].clone().unwrap().non_null_ptr, nnptr0);
    assert_eq!(manager.index_map.len(), 1);
    assert_eq!(*manager.index_map.get("foo").unwrap(), 0);
    assert_eq!(manager.ordered_names.len(), 0);
    assert!(!manager.committed);
}

#[test]
fn test_with_commit_order_and_add() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    let mut manager = DataConnManager::with_commit_order(&["bar", "baz", "foo"]);

    assert_eq!(manager.vec.len(), 3);
    assert!(manager.vec[0].is_none());
    assert!(manager.vec[1].is_none());
    assert!(manager.vec[2].is_none());
    assert_eq!(manager.index_map.len(), 3);
    assert_eq!(*manager.index_map.get("foo").unwrap(), 2);
    assert_eq!(*manager.index_map.get("bar").unwrap(), 0);
    assert_eq!(*manager.index_map.get("baz").unwrap(), 1);
    assert_eq!(manager.ordered_names.len(), 3);
    assert_eq!(manager.ordered_names[0], "bar".into());
    assert_eq!(manager.ordered_names[1], "baz".into());
    assert_eq!(manager.ordered_names[2], "foo".into());
    assert!(!manager.committed);

    let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
    let boxed = Box::new(DataConnContainer::new("foo".to_string(), Box::new(conn)));
    let nnptr0 = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
    let ssnnptr = SendSyncNonNull::new(nnptr0);
    manager.add(ssnnptr);

    assert_eq!(manager.vec.len(), 3);
    assert!(manager.vec[0].is_none());
    assert!(manager.vec[1].is_none());
    assert_eq!(manager.vec[2].clone().unwrap().non_null_ptr, nnptr0);
    assert_eq!(manager.index_map.len(), 3);
    assert_eq!(*manager.index_map.get("foo").unwrap(), 2);
    assert_eq!(*manager.index_map.get("bar").unwrap(), 0);
    assert_eq!(*manager.index_map.get("baz").unwrap(), 1);
    assert_eq!(manager.ordered_names.len(), 3);
    assert_eq!(manager.ordered_names[0], "bar".into());
    assert_eq!(manager.ordered_names[1], "baz".into());
    assert_eq!(manager.ordered_names[2], "foo".into());
    assert!(!manager.committed);

    let conn = AsyncDataConn::new(2, logger.clone(), Fail::None);
    let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
    let nnptr1 = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
    let ssnnptr = SendSyncNonNull::new(nnptr1);
    manager.add(ssnnptr);

    assert_eq!(manager.vec.len(), 3);
    assert_eq!(manager.vec[0].clone().unwrap().non_null_ptr, nnptr1);
    assert!(manager.vec[1].is_none());
    assert_eq!(manager.vec[2].clone().unwrap().non_null_ptr, nnptr0);
    assert_eq!(manager.index_map.len(), 3);
    assert_eq!(*manager.index_map.get("foo").unwrap(), 2);
    assert_eq!(*manager.index_map.get("bar").unwrap(), 0);
    assert_eq!(*manager.index_map.get("baz").unwrap(), 1);
    assert_eq!(manager.ordered_names.len(), 3);
    assert_eq!(manager.ordered_names[0], "bar".into());
    assert_eq!(manager.ordered_names[1], "baz".into());
    assert_eq!(manager.ordered_names[2], "foo".into());
    assert!(!manager.committed);

    let conn = SyncDataConn::new(3, logger.clone(), Fail::None);
    let boxed = Box::new(DataConnContainer::new("qux", Box::new(conn)));
    let nnptr2 = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
    let ssnnptr = SendSyncNonNull::new(nnptr2);
    manager.add(ssnnptr);

    assert_eq!(manager.vec.len(), 4);
    assert_eq!(manager.vec[0].clone().unwrap().non_null_ptr, nnptr1);
    assert!(manager.vec[1].is_none());
    assert_eq!(manager.vec[2].clone().unwrap().non_null_ptr, nnptr0);
    assert_eq!(manager.vec[3].clone().unwrap().non_null_ptr, nnptr2);
    assert_eq!(manager.index_map.len(), 4);
    assert_eq!(*manager.index_map.get("foo").unwrap(), 2);
    assert_eq!(*manager.index_map.get("bar").unwrap(), 0);
    assert_eq!(*manager.index_map.get("baz").unwrap(), 1);
    assert_eq!(*manager.index_map.get("qux").unwrap(), 3);
    assert_eq!(manager.ordered_names.len(), 3);
    assert_eq!(manager.ordered_names[0], "bar".into());
    assert_eq!(manager.ordered_names[1], "baz".into());
    assert_eq!(manager.ordered_names[2], "foo".into());
    assert!(!manager.committed);
}

#[test]
fn test_with_commit_order_and_add_when_overlapping_name() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    let mut manager = DataConnManager::with_commit_order(&["bar", "baz", "foo"]);

    assert_eq!(manager.vec.len(), 3);
    assert!(manager.vec[0].is_none());
    assert!(manager.vec[1].is_none());
    assert!(manager.vec[2].is_none());
    assert_eq!(manager.index_map.len(), 3);
    assert_eq!(*manager.index_map.get("foo").unwrap(), 2);
    assert_eq!(*manager.index_map.get("bar").unwrap(), 0);
    assert_eq!(*manager.index_map.get("baz").unwrap(), 1);
    assert_eq!(manager.ordered_names.len(), 3);
    assert_eq!(manager.ordered_names[0], "bar".into());
    assert_eq!(manager.ordered_names[1], "baz".into());
    assert_eq!(manager.ordered_names[2], "foo".into());
    assert!(!manager.committed);

    let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
    let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
    let nnptr0 = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
    let ssnnptr = SendSyncNonNull::new(nnptr0);
    manager.add(ssnnptr);

    assert_eq!(manager.vec.len(), 3);
    assert!(manager.vec[0].is_none());
    assert!(manager.vec[1].is_none());
    assert_eq!(manager.vec[2].clone().unwrap().non_null_ptr, nnptr0);
    assert_eq!(manager.index_map.len(), 3);
    assert_eq!(*manager.index_map.get("foo").unwrap(), 2);
    assert_eq!(*manager.index_map.get("bar").unwrap(), 0);
    assert_eq!(*manager.index_map.get("baz").unwrap(), 1);
    assert_eq!(manager.ordered_names.len(), 3);
    assert_eq!(manager.ordered_names[0], "bar".into());
    assert_eq!(manager.ordered_names[1], "baz".into());
    assert_eq!(manager.ordered_names[2], "foo".into());
    assert!(!manager.committed);

    let conn = AsyncDataConn::new(2, logger.clone(), Fail::None);
    let boxed = Box::new(DataConnContainer::new("foo".to_string(), Box::new(conn)));
    let nnptr1 = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
    let ssnnptr = SendSyncNonNull::new(nnptr1);
    manager.add(ssnnptr);

    assert_eq!(manager.vec.len(), 3);
    assert!(manager.vec[0].is_none());
    assert!(manager.vec[1].is_none());
    assert_eq!(manager.vec[2].clone().unwrap().non_null_ptr, nnptr0);
    assert_eq!(manager.index_map.len(), 3);
    assert_eq!(*manager.index_map.get("foo").unwrap(), 2);
    assert_eq!(*manager.index_map.get("bar").unwrap(), 0);
    assert_eq!(*manager.index_map.get("baz").unwrap(), 1);
    assert_eq!(manager.ordered_names.len(), 3);
    assert_eq!(manager.ordered_names[0], "bar".into());
    assert_eq!(manager.ordered_names[1], "baz".into());
    assert_eq!(manager.ordered_names[2], "foo".into());
    assert!(!manager.committed);
}

#[test]
fn preserve_commit_order_after_close() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::with_commit_order(&["baz", "bar"]);
        assert_eq!(manager.vec.len(), 2);
        assert!(manager.vec[0].is_none());
        assert!(manager.vec[1].is_none());
        assert_eq!(manager.index_map.len(), 2);
        assert_eq!(*manager.index_map.get("bar").unwrap(), 1);
        assert_eq!(*manager.index_map.get("baz").unwrap(), 0);

        let conn1 = SyncDataConn::new(1, logger.clone(), Fail::None);
        let boxed1 = Box::new(DataConnContainer::new("foo", Box::new(conn1)));
        let nnptr1 = ptr::NonNull::from(Box::leak(boxed1)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr1);
        manager.add(ssnnptr);
        assert_eq!(manager.vec.len(), 3);
        assert!(manager.vec[0].is_none());
        assert!(manager.vec[1].is_none());
        assert_eq!(manager.vec[2].clone().unwrap().non_null_ptr, nnptr1);
        assert_eq!(manager.index_map.len(), 3);
        assert_eq!(*manager.index_map.get("bar").unwrap(), 1);
        assert_eq!(*manager.index_map.get("baz").unwrap(), 0);
        assert_eq!(*manager.index_map.get("foo").unwrap(), 2);

        let conn2 = SyncDataConn::new(2, logger.clone(), Fail::None);
        let boxed2 = Box::new(DataConnContainer::new("bar", Box::new(conn2)));
        let nnptr2 = ptr::NonNull::from(Box::leak(boxed2)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr2);
        manager.add(ssnnptr);
        assert_eq!(manager.vec.len(), 3);
        assert!(manager.vec[0].is_none());
        assert_eq!(manager.vec[1].clone().unwrap().non_null_ptr, nnptr2);
        assert_eq!(manager.vec[2].clone().unwrap().non_null_ptr, nnptr1);
        assert_eq!(manager.index_map.len(), 3);
        assert_eq!(*manager.index_map.get("bar").unwrap(), 1);
        assert_eq!(*manager.index_map.get("baz").unwrap(), 0);
        assert_eq!(*manager.index_map.get("foo").unwrap(), 2);

        let conn3 = SyncDataConn::new(3, logger.clone(), Fail::None);
        let boxed3 = Box::new(DataConnContainer::new("baz", Box::new(conn3)));
        let nnptr3 = ptr::NonNull::from(Box::leak(boxed3)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr3);
        manager.add(ssnnptr);
        assert_eq!(manager.vec.len(), 3);
        assert_eq!(manager.vec[0].clone().unwrap().non_null_ptr, nnptr3);
        assert_eq!(manager.vec[1].clone().unwrap().non_null_ptr, nnptr2);
        assert_eq!(manager.vec[2].clone().unwrap().non_null_ptr, nnptr1);
        assert_eq!(manager.index_map.len(), 3);
        assert_eq!(*manager.index_map.get("bar").unwrap(), 1);
        assert_eq!(*manager.index_map.get("baz").unwrap(), 0);
        assert_eq!(*manager.index_map.get("foo").unwrap(), 2);

        let conn4 = SyncDataConn::new(4, logger.clone(), Fail::None);
        let boxed4 = Box::new(DataConnContainer::new("qux", Box::new(conn4)));
        let nnptr4 = ptr::NonNull::from(Box::leak(boxed4)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr4);
        manager.add(ssnnptr);
        assert_eq!(manager.vec.len(), 4);
        assert_eq!(manager.vec[0].clone().unwrap().non_null_ptr, nnptr3);
        assert_eq!(manager.vec[1].clone().unwrap().non_null_ptr, nnptr2);
        assert_eq!(manager.vec[2].clone().unwrap().non_null_ptr, nnptr1);
        assert_eq!(manager.vec[3].clone().unwrap().non_null_ptr, nnptr4);
        assert_eq!(manager.index_map.len(), 4);
        assert_eq!(*manager.index_map.get("bar").unwrap(), 1);
        assert_eq!(*manager.index_map.get("baz").unwrap(), 0);
        assert_eq!(*manager.index_map.get("foo").unwrap(), 2);
        assert_eq!(*manager.index_map.get("qux").unwrap(), 3);

        manager.close();
        assert_eq!(manager.vec.len(), 2);
        assert!(manager.vec[0].is_none());
        assert!(manager.vec[1].is_none());
        assert_eq!(manager.index_map.len(), 2);
        assert_eq!(*manager.index_map.get("bar").unwrap(), 1);
        assert_eq!(*manager.index_map.get("baz").unwrap(), 0);
    }

    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "SyncDataConn::new 2",
            "SyncDataConn::new 3",
            "SyncDataConn::new 4",
            "SyncDataConn::close 4",
            "SyncDataConn::drop 4",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
            "SyncDataConn::close 2",
            "SyncDataConn::drop 2",
            "SyncDataConn::close 3",
            "SyncDataConn::drop 3",
        ]
    );
}

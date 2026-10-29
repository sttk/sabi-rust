use super::_test_data::*;
use super::*;
use crate::data_conn::_test_data::*;
use std::sync::{Arc, Mutex};

#[test]
fn test_of_new() {
    let manager = DataSrcManager::new(true);
    assert!(manager.local);
    assert_eq!(manager.vec_unready.len(), 0);
    assert_eq!(manager.vec_ready.len(), 0);

    let manager = DataSrcManager::new(false);
    assert!(!manager.local);
    assert_eq!(manager.vec_unready.len(), 0);
    assert_eq!(manager.vec_ready.len(), 0);
}

#[test]
fn test_of_add() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        let mut manager = DataSrcManager::new(true);

        let ds = SyncDataSrc::new(1, logger.clone(), Fail::None);
        manager.add("foo", ds);

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 1);
        assert_eq!(manager.vec_ready.len(), 0);

        assert_eq!(
            unsafe { manager.vec_unready[0].non_null_ptr.as_ref().name.clone() },
            "foo".into()
        );

        let ds = AsyncDataSrc::new(2, logger.clone(), Fail::None);
        manager.add("bar", ds);

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 2);
        assert_eq!(manager.vec_ready.len(), 0);

        assert_eq!(
            unsafe { manager.vec_unready[0].non_null_ptr.as_ref().name.clone() },
            "foo".into()
        );
        assert_eq!(
            unsafe { manager.vec_unready[1].non_null_ptr.as_ref().name.clone() },
            "bar".into()
        );
    }

    assert_eq!(
        *logger.lock().unwrap(),
        vec![
            "SyncDataSrc::new 1",
            "AsyncDataSrc::new 2",
            "AsyncDataSrc::drop 2",
            "SyncDataSrc::drop 1",
        ],
    );
}

#[test]
fn test_of_remove() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        let mut manager = DataSrcManager::new(true);

        let ds1 = SyncDataSrc::new(1, logger.clone(), Fail::None);
        let boxed = Box::new(DataSrcContainer::new("foo", ds1, true));
        let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataSrcContainer>();
        manager.vec_unready.push(SendSyncNonNull::new(ptr));

        let ds2 = AsyncDataSrc::new(2, logger.clone(), Fail::None);
        let boxed = Box::new(DataSrcContainer::new("bar", ds2, true));
        let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataSrcContainer>();
        manager.vec_unready.push(SendSyncNonNull::new(ptr));

        let ds3 = SyncDataSrc::new(3, logger.clone(), Fail::None);
        let boxed = Box::new(DataSrcContainer::new("baz", ds3, true));
        let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataSrcContainer>();
        manager.vec_ready.push(SendSyncNonNull::new(ptr));

        let ds4 = AsyncDataSrc::new(4, logger.clone(), Fail::None);
        let boxed = Box::new(DataSrcContainer::new("qux", ds4, true));
        let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataSrcContainer>();
        manager.vec_ready.push(SendSyncNonNull::new(ptr));

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 2);
        assert_eq!(manager.vec_ready.len(), 2);

        manager.remove("baz");
        manager.remove("foo");
        manager.remove("qux");
        manager.remove("bar");
    }

    assert_eq!(
        *logger.lock().unwrap(),
        vec![
            "SyncDataSrc::new 1",
            "AsyncDataSrc::new 2",
            "SyncDataSrc::new 3",
            "AsyncDataSrc::new 4",
            "SyncDataSrc::close 3",
            "SyncDataSrc::drop 3",
            "SyncDataSrc::drop 1",
            "AsyncDataSrc::close 4",
            "AsyncDataSrc::drop 4",
            "AsyncDataSrc::drop 2",
        ],
    );
}

#[test]
fn test_of_close() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        let mut manager = DataSrcManager::new(true);

        let ds1 = SyncDataSrc::new(1, logger.clone(), Fail::None);
        let boxed = Box::new(DataSrcContainer::new("foo", ds1, true));
        let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataSrcContainer>();
        manager.vec_unready.push(SendSyncNonNull::new(ptr));

        let ds2 = AsyncDataSrc::new(2, logger.clone(), Fail::None);
        let boxed = Box::new(DataSrcContainer::new("bar", ds2, true));
        let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataSrcContainer>();
        manager.vec_unready.push(SendSyncNonNull::new(ptr));

        let ds3 = SyncDataSrc::new(3, logger.clone(), Fail::None);
        let boxed = Box::new(DataSrcContainer::new("baz", ds3, true));
        let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataSrcContainer>();
        manager.vec_ready.push(SendSyncNonNull::new(ptr));

        let ds4 = AsyncDataSrc::new(4, logger.clone(), Fail::None);
        let boxed = Box::new(DataSrcContainer::new("qux", ds4, true));
        let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataSrcContainer>();
        manager.vec_ready.push(SendSyncNonNull::new(ptr));

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 2);
        assert_eq!(manager.vec_ready.len(), 2);

        manager.close();
    }

    assert_eq!(
        *logger.lock().unwrap(),
        vec![
            "SyncDataSrc::new 1",
            "AsyncDataSrc::new 2",
            "SyncDataSrc::new 3",
            "AsyncDataSrc::new 4",
            "AsyncDataSrc::close 4",
            "AsyncDataSrc::drop 4",
            "SyncDataSrc::close 3",
            "SyncDataSrc::drop 3",
            "AsyncDataSrc::drop 2",
            "SyncDataSrc::drop 1",
        ],
    );
}

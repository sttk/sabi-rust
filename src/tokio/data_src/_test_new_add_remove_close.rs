use super::_test_data::*;
use super::*;
use crate::tokio::data_conn::_test_data::*;

#[tokio::test]
async fn test_of_new() {
    let manager = DataSrcManager::new(true);
    assert!(manager.local);
    assert_eq!(manager.vec_unready.len(), 0);
    assert_eq!(manager.vec_ready.len(), 0);

    let manager = DataSrcManager::new(false);
    assert!(!manager.local);
    assert_eq!(manager.vec_unready.len(), 0);
    assert_eq!(manager.vec_ready.len(), 0);
}

#[tokio::test]
async fn test_of_prepend() {
    let logger = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

    {
        let mut vec = Vec::<SendSyncNonNull<DataSrcContainer>>::new();

        let ds = SyncDataSrc::new(1, logger.clone(), Fail::None);
        let boxed = Box::new(DataSrcContainer::new("foo", ds, true));
        let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataSrcContainer>();
        vec.push(SendSyncNonNull::new(ptr));

        let ds = AsyncDataSrc::new(2, logger.clone(), Fail::None);
        let boxed = Box::new(DataSrcContainer::new("bar", ds, true));
        let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataSrcContainer>();
        vec.push(SendSyncNonNull::new(ptr));

        let mut manager = DataSrcManager::new(true);
        manager.prepend(vec);

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

        let mut vec = Vec::<SendSyncNonNull<DataSrcContainer>>::new();

        let ds = SyncDataSrc::new(3, logger.clone(), Fail::None);
        let boxed = Box::new(DataSrcContainer::new("baz", ds, true));
        let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataSrcContainer>();
        vec.push(SendSyncNonNull::new(ptr));

        let ds = AsyncDataSrc::new(4, logger.clone(), Fail::None);
        let boxed = Box::new(DataSrcContainer::new("qux", ds, true));
        let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataSrcContainer>();
        vec.push(SendSyncNonNull::new(ptr));

        manager.prepend(vec);

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 4);
        assert_eq!(manager.vec_ready.len(), 0);

        assert_eq!(
            unsafe { manager.vec_unready[0].non_null_ptr.as_ref().name.clone() },
            "baz".into()
        );
        assert_eq!(
            unsafe { manager.vec_unready[1].non_null_ptr.as_ref().name.clone() },
            "qux".into()
        );
        assert_eq!(
            unsafe { manager.vec_unready[2].non_null_ptr.as_ref().name.clone() },
            "foo".into()
        );
        assert_eq!(
            unsafe { manager.vec_unready[3].non_null_ptr.as_ref().name.clone() },
            "bar".into()
        );
    }

    // Give some time for drop to be called
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    {
        let locked = logger.lock().unwrap();
        assert!(locked.contains(&"SyncDataSrc::new 1".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::new 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 3".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::new 4".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::drop 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 1".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::drop 4".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 3".to_string()));
    }
}

#[tokio::test]
async fn test_of_add() {
    let logger = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

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

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    {
        let locked = logger.lock().unwrap();
        assert!(locked.contains(&"SyncDataSrc::new 1".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::new 2".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::drop 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 1".to_string()));
    }
}

#[tokio::test]
async fn test_of_remove() {
    let logger = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

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

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    {
        let locked = logger.lock().unwrap();
        assert!(locked.contains(&"SyncDataSrc::new 1".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::new 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 3".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::new 4".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 1".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::close 4".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::drop 4".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::drop 2".to_string()));
    }
}

#[tokio::test]
async fn test_of_close() {
    let logger = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

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

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    {
        let locked = logger.lock().unwrap();
        assert!(locked.contains(&"SyncDataSrc::new 1".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::new 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 3".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::new 4".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::close 4".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::drop 4".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 3".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::drop 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 1".to_string()));
    }
}

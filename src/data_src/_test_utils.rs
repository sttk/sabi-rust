use super::_test_data::*;
use crate::data_conn::_test_data::*;
use crate::*;
use std::ptr;
use std::sync::{Arc, Mutex};

#[test]
fn test_of_prepend() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

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

    assert_eq!(
        *logger.lock().unwrap(),
        vec![
            "SyncDataSrc::new 1",
            "AsyncDataSrc::new 2",
            "SyncDataSrc::new 3",
            "AsyncDataSrc::new 4",
            "AsyncDataSrc::drop 2",
            "SyncDataSrc::drop 1",
            "AsyncDataSrc::drop 4",
            "SyncDataSrc::drop 3",
        ],
    );
}

#[test]
fn test_of_copy_ds_ready_to_map_with_add_and_setup() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut errors = Vec::new();

    let mut index_map = HashMap::<Arc<str>, (bool, usize)>::new();

    let manager = DataSrcManager::new(true);
    manager.copy_ds_ready_to_map(&mut index_map);
    assert!(index_map.is_empty());

    let mut manager = DataSrcManager::new(true);
    let ds1 = SyncDataSrc::new(1, logger.clone(), Fail::None);
    manager.add("foo", ds1);
    manager.setup(&mut errors);
    assert!(errors.is_empty());
    manager.copy_ds_ready_to_map(&mut index_map);
    assert_eq!(index_map.len(), 1);
    assert_eq!(index_map.get("foo").unwrap(), &(true, 0));

    let mut manager = DataSrcManager::new(false);
    let ds2 = AsyncDataSrc::new(2, logger.clone(), Fail::None);
    let ds3 = SyncDataSrc::new(3, logger.clone(), Fail::None);
    manager.add("bar", ds2);
    manager.add("baz", ds3);
    manager.setup(&mut errors);
    assert!(errors.is_empty());
    manager.copy_ds_ready_to_map(&mut index_map);
    assert_eq!(index_map.len(), 3);
    assert_eq!(index_map.get("foo").unwrap(), &(true, 0));
    assert_eq!(index_map.get("bar").unwrap(), &(false, 0));
    assert_eq!(index_map.get("baz").unwrap(), &(false, 1));
}

#[test]
fn test_of_copy_ds_ready_to_map_with_add_and_remove_and_setup() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut errors = Vec::new();

    let mut manager = DataSrcManager::new(true);

    let ds1 = SyncDataSrc::new(1, logger.clone(), Fail::None);
    manager.add("foo", ds1);

    let ds2 = SyncDataSrc::new(2, logger.clone(), Fail::None);
    manager.add("bar", ds2);

    manager.setup(&mut errors);
    assert!(errors.is_empty());

    let mut index_map = HashMap::<Arc<str>, (bool, usize)>::new();
    manager.copy_ds_ready_to_map(&mut index_map);
    assert_eq!(index_map.len(), 2);
    assert_eq!(index_map.get("foo").unwrap(), &(true, 0));
    assert_eq!(index_map.get("bar").unwrap(), &(true, 1));

    manager.remove("foo");

    manager.setup(&mut errors);
    assert!(errors.is_empty());

    let mut index_map = HashMap::<Arc<str>, (bool, usize)>::new();
    manager.copy_ds_ready_to_map(&mut index_map);
    assert_eq!(index_map.len(), 1);
    assert_eq!(index_map.get("bar").unwrap(), &(true, 0));
}

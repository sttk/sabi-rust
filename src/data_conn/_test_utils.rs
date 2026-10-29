use super::_test_data::*;
use super::*;
use std::ptr;
use std::sync::{Arc, Mutex};

#[test]
fn test_find_by_name_but_none() {
    let manager = DataConnManager::new();
    assert!(manager.find_by_name("foo").is_none());
    assert!(manager.find_by_name("bar").is_none());
}

#[test]
fn test_find_by_name_and_found() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    let mut manager = DataConnManager::new();

    let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
    let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
    let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
    let ssnnptr = SendSyncNonNull::new(nnptr);
    manager.add(ssnnptr);

    let conn = AsyncDataConn::new(2, logger.clone(), Fail::None);
    let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
    let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
    let ssnnptr = SendSyncNonNull::new(nnptr);
    manager.add(ssnnptr);

    if let Some(ssnnptr) = manager.find_by_name("foo") {
        let name = unsafe { (*ssnnptr.non_null_ptr.as_ptr()).name.clone() };
        assert_eq!(name.as_ref(), "foo");
    } else {
        panic!();
    }

    if let Some(ssnnptr) = manager.find_by_name("bar") {
        let name = unsafe { (*ssnnptr.non_null_ptr.as_ptr()).name.clone() };
        assert_eq!(name.as_ref(), "bar");
    } else {
        panic!();
    }
}

#[test]
fn test_find_by_name_of_ordered_dataconn() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    let mut manager = DataConnManager::with_commit_order(&["baz", "qux", "foo"]);

    let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
    let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
    let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
    let ssnnptr = SendSyncNonNull::new(nnptr);
    manager.add(ssnnptr);

    let conn = SyncDataConn::new(2, logger.clone(), Fail::None);
    let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
    let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
    let ssnnptr = SendSyncNonNull::new(nnptr);
    manager.add(ssnnptr);

    let conn = SyncDataConn::new(3, logger.clone(), Fail::None);
    let boxed = Box::new(DataConnContainer::new("baz", Box::new(conn)));
    let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
    let ssnnptr = SendSyncNonNull::new(nnptr);
    manager.add(ssnnptr);

    if let Some(ssnnptr) = manager.find_by_name("foo") {
        let name = unsafe { (*ssnnptr.non_null_ptr.as_ptr()).name.clone() };
        assert_eq!(name.as_ref(), "foo");
    } else {
        panic!();
    }

    if let Some(ssnnptr) = manager.find_by_name("bar") {
        let name = unsafe { (*ssnnptr.non_null_ptr.as_ptr()).name.clone() };
        assert_eq!(name.as_ref(), "bar");
    } else {
        panic!();
    }

    if let Some(ssnnptr) = manager.find_by_name("baz") {
        let name = unsafe { (*ssnnptr.non_null_ptr.as_ptr()).name.clone() };
        assert_eq!(name.as_ref(), "baz");
    } else {
        panic!();
    }

    assert!(manager.find_by_name("qux").is_none());
}

#[test]
fn test_find_by_name_for_coverage() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    let mut manager = DataConnManager::new();
    let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
    let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
    let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
    let ssnnptr = SendSyncNonNull::new(nnptr);
    manager.add(ssnnptr);

    manager.index_map.insert("foo".into(), 10);
    assert!(manager.find_by_name("foo").is_none());

    manager.index_map.insert("bar".into(), 0);
    assert!(manager.find_by_name("bar").is_none());
}

#[test]
fn test_to_typed_ptr() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    let mut manager = DataConnManager::new();

    let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
    let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
    let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
    let ssnnptr = SendSyncNonNull::new(nnptr);
    manager.add(ssnnptr);

    let conn = AsyncDataConn::new(2, logger.clone(), Fail::None);
    let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
    let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
    let ssnnptr = SendSyncNonNull::new(nnptr);
    manager.add(ssnnptr);

    let ssnnptr = manager.find_by_name("foo").unwrap();
    if let Ok(typed_ssnnptr) = DataConnManager::to_typed_ptr::<SyncDataConn>(&ssnnptr) {
        assert_eq!(
            any::type_name_of_val(&typed_ssnnptr),
            "*mut sabi::DataConnContainer<sabi::data_conn::_test_data::SyncDataConn>"
        );
        assert_eq!(unsafe { (*typed_ssnnptr).name.clone() }, "foo".into());
    } else {
        panic!();
    }

    let ssnnptr = manager.find_by_name("bar").unwrap();
    if let Ok(typed_ssnnptr) = DataConnManager::to_typed_ptr::<AsyncDataConn>(&ssnnptr) {
        assert_eq!(
            any::type_name_of_val(&typed_ssnnptr),
            "*mut sabi::DataConnContainer<sabi::data_conn::_test_data::AsyncDataConn>"
        );
        assert_eq!(unsafe { (*typed_ssnnptr).name.clone() }, "bar".into());
    } else {
        panic!();
    }
}

#[test]
fn test_to_typed_ptr_but_fail() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    let mut manager = DataConnManager::new();

    let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
    let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
    let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
    let ssnnptr = SendSyncNonNull::new(nnptr);
    manager.add(ssnnptr);

    let conn = AsyncDataConn::new(2, logger.clone(), Fail::None);
    let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
    let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
    let ssnnptr = SendSyncNonNull::new(nnptr);
    manager.add(ssnnptr);

    let ssnnptr = manager.find_by_name("foo").unwrap();
    if let Err(err) = DataConnManager::to_typed_ptr::<AsyncDataConn>(&ssnnptr) {
        match err.reason::<DataConnError>() {
            Ok(DataConnError::FailToCastDataConn { name, target_type }) => {
                assert_eq!(name.as_ref(), "foo");
                assert_eq!(*target_type, "sabi::data_conn::_test_data::AsyncDataConn");
            }
            _ => panic!(),
        }
    } else {
        panic!();
    }

    let ssnnptr = manager.find_by_name("bar").unwrap();
    if let Err(err) = DataConnManager::to_typed_ptr::<SyncDataConn>(&ssnnptr) {
        match err.reason::<DataConnError>() {
            Ok(DataConnError::FailToCastDataConn { name, target_type }) => {
                assert_eq!(name.as_ref(), "bar");
                assert_eq!(*target_type, "sabi::data_conn::_test_data::SyncDataConn");
            }
            _ => panic!(),
        }
    } else {
        panic!();
    }
}

#[test]
fn test_new_failure_reports() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    let mut manager = DataConnManager::new();

    let vec = manager.new_failure_reports();
    assert_eq!(vec.len(), 0);

    let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
    let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
    let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
    let ssnnptr = SendSyncNonNull::new(nnptr);
    manager.add(ssnnptr);

    let conn = AsyncDataConn::new(2, logger.clone(), Fail::None);
    let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
    let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
    let ssnnptr = SendSyncNonNull::new(nnptr);
    manager.add(ssnnptr);

    let vec = manager.new_failure_reports();
    assert_eq!(vec.len(), 2);

    let report = &vec[0];
    assert_eq!(report.data_conn_name, "foo".into());
    assert_eq!(
        report.data_conn_type,
        "sabi::data_conn::_test_data::SyncDataConn"
    );

    let report = &vec[1];
    assert_eq!(report.data_conn_name, "bar".into());
    assert_eq!(
        report.data_conn_type,
        "sabi::data_conn::_test_data::AsyncDataConn"
    );
}

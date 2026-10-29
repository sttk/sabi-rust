use super::_test_data::*;
use super::*;
use crate::data_conn::_test_data::*;

use std::sync::{Arc, Mutex};

#[test]
fn test_of_create_data_conn_and_ok() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut errors = Vec::new();

    let mut manager = DataSrcManager::new(true);
    let ds1 = SyncDataSrc::new(1, logger.clone(), Fail::None);
    manager.add("foo", ds1);
    manager.setup(&mut errors);

    if let Ok(boxed) = manager.create_data_conn::<SyncDataConn>(0, "foo") {
        assert_eq!(boxed.name.clone(), "foo".into());
    } else {
        panic!();
    }
}

#[test]
fn test_of_create_data_conn_but_not_found() {
    let mut errors = Vec::new();

    let mut manager = DataSrcManager::new(true);
    manager.setup(&mut errors);

    if let Err(err) = manager.create_data_conn::<SyncDataConn>(0, "foo") {
        match err.reason::<DataSrcError>() {
            Ok(DataSrcError::NoDataSrcToCreateDataConn {
                name,
                data_conn_type,
            }) => {
                assert_eq!(*name, "foo".into());
                assert_eq!(*data_conn_type, "sabi::data_conn::_test_data::SyncDataConn");
            }
            _ => panic!(),
        }
    } else {
        panic!();
    }
}

#[test]
fn test_of_create_data_conn_but_fail_to_cast() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut errors = Vec::new();

    let mut manager = DataSrcManager::new(true);
    let ds1 = SyncDataSrc::new(1, logger.clone(), Fail::None);
    manager.add("foo", ds1);
    manager.setup(&mut errors);

    if let Err(err) = manager.create_data_conn::<AsyncDataConn>(0, "foo") {
        match err.reason::<DataSrcError>() {
            Ok(DataSrcError::FailToCastDataConn { name, target_type }) => {
                assert_eq!(*name, "foo".into());
                assert_eq!(*target_type, "sabi::data_conn::_test_data::AsyncDataConn");
            }
            _ => panic!(),
        }
    } else {
        panic!();
    }
}

#[test]
fn test_of_create_data_conn_but_fail_to_create() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut errors = Vec::new();

    let mut manager = DataSrcManager::new(true);
    let ds1 = SyncDataSrc::new(1, logger.clone(), Fail::CreateDataConn);
    manager.add("foo", ds1);
    manager.setup(&mut errors);

    if let Err(err) = manager.create_data_conn::<SyncDataConn>(0, "foo") {
        match err.reason::<DataSrcError>() {
            Ok(DataSrcError::FailToCreateDataConn {
                name,
                data_conn_type,
            }) => {
                assert_eq!(*name, "foo".into());
                assert_eq!(*data_conn_type, "sabi::data_conn::_test_data::SyncDataConn");
            }
            _ => panic!(),
        }
    } else {
        panic!();
    }
}

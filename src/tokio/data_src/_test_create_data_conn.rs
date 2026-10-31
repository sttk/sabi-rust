use super::_test_data::*;
use super::*;
use crate::tokio::data_conn::_test_data::*;

#[tokio::test]
async fn test_of_create_data_conn_and_ok() {
    let logger = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let mut errors = Vec::new();

    let mut manager = DataSrcManager::new(true);
    let ds1 = SyncDataSrc::new(1, logger.clone(), Fail::None);
    manager.add("foo", ds1);
    manager.setup_async(&mut errors).await;

    if let Ok(boxed) = manager
        .create_data_conn_async::<SyncDataConn>(0, "foo")
        .await
    {
        assert_eq!(boxed.name.clone(), "foo".into());
    } else {
        panic!();
    }
}

#[tokio::test]
async fn test_of_create_data_conn_but_not_found() {
    let mut errors = Vec::new();

    let mut manager = DataSrcManager::new(true);
    manager.setup_async(&mut errors).await;

    if let Err(err) = manager
        .create_data_conn_async::<SyncDataConn>(0, "foo")
        .await
    {
        match err.reason::<DataSrcError>() {
            Ok(DataSrcError::NoDataSrcToCreateDataConn {
                name,
                data_conn_type,
            }) => {
                assert_eq!(*name, "foo".into());
                assert_eq!(
                    *data_conn_type,
                    "sabi::tokio::data_conn::_test_data::SyncDataConn"
                );
            }
            _ => panic!(),
        }
    } else {
        panic!();
    }
}

#[tokio::test]
async fn test_of_create_data_conn_but_fail_to_cast() {
    let logger = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let mut errors = Vec::new();

    let mut manager = DataSrcManager::new(true);
    let ds1 = SyncDataSrc::new(1, logger.clone(), Fail::None);
    manager.add("foo", ds1);
    manager.setup_async(&mut errors).await;

    if let Err(err) = manager
        .create_data_conn_async::<AsyncDataConn>(0, "foo")
        .await
    {
        match err.reason::<DataSrcError>() {
            Ok(DataSrcError::FailToCastDataConn { name, target_type }) => {
                assert_eq!(*name, "foo".into());
                assert_eq!(
                    *target_type,
                    "sabi::tokio::data_conn::_test_data::AsyncDataConn"
                );
            }
            _ => panic!(),
        }
    } else {
        panic!();
    }
}

#[tokio::test]
async fn test_of_create_data_conn_but_fail_to_create() {
    let logger = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let mut errors = Vec::new();

    let mut manager = DataSrcManager::new(true);
    let ds1 = SyncDataSrc::new(1, logger.clone(), Fail::CreateDataConn);
    manager.add("foo", ds1);
    manager.setup_async(&mut errors).await;

    if let Err(err) = manager
        .create_data_conn_async::<SyncDataConn>(0, "foo")
        .await
    {
        match err.reason::<DataSrcError>() {
            Ok(DataSrcError::FailToCreateDataConn {
                name,
                data_conn_type,
            }) => {
                assert_eq!(*name, "foo".into());
                assert_eq!(
                    *data_conn_type,
                    "sabi::tokio::data_conn::_test_data::SyncDataConn"
                );
            }
            _ => panic!(),
        }
    } else {
        panic!();
    }
}

use crate::tokio::data_conn::_test_data::*;
use crate::tokio::data_src::_test_data::*;
use crate::tokio::*;

use std::sync::{Arc, Mutex};

#[tokio::test]
async fn test_start_with_no_data_src() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        struct A;
        let mut runner = LogicRunner::new(DataHub::<A>::new());

        let r = runner.start_async().await;
        assert!(r.end().is_ok());
    }

    assert_eq!(logger.lock().unwrap().len(), 0);
}

#[tokio::test]
async fn test_start_with_some_data_srcs() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        struct A;
        let mut runner = LogicRunner::new(DataHub::<A>::new());
        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

        let runner = runner.start_async().await;
        assert!(runner.end().is_ok());
    }

    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataSrc::new 1",
            "SyncDataSrc::new 2",
            "SyncDataSrc::setup_async 1",
            "SyncDataSrc::setup_async 2",
            "SyncDataSrc::close 2",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::close 1",
            "SyncDataSrc::drop 1",
        ]
    );
}

#[tokio::test]
async fn test_start_but_failed_to_begin() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        struct A;
        let mut runner = LogicRunner::new(DataHub::<A>::new());
        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Setup));
        runner.uses("baz", SyncDataSrc::new(3, logger.clone(), Fail::None));

        if let Err(err) = runner.start_async().await.end() {
            match err.reason::<DataHubError>() {
                Ok(DataHubError::FailToSetupLocalDataSrcs { errors }) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].index, 1);
                    assert_eq!(errors[0].name, "bar".into());
                    assert_eq!(errors[0].err.reason::<String>().unwrap(), "XXX");
                }
                _ => panic!(),
            }
        } else {
            panic!();
        }
    }

    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataSrc::new 1",
            "SyncDataSrc::new 2",
            "SyncDataSrc::new 3",
            "SyncDataSrc::setup_async 1",
            "SyncDataSrc::setup_async 2 failed",
            "SyncDataSrc::close 1",
            "SyncDataSrc::drop 3",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::drop 1",
        ]
    );
}

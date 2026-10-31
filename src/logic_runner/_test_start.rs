use crate::data_conn::_test_data::*;
use crate::data_src::_test_data::*;
use crate::{DataHub, DataHubError, LogicRunner};
use std::sync::{Arc, Mutex};

#[test]
fn test_start_with_no_data_src() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        struct A;
        let mut runner = LogicRunner::new(DataHub::<A>::new());

        let r = runner.start();
        assert!(r.end().is_ok());
    }

    assert_eq!(logger.lock().unwrap().len(), 0);
}

#[test]
fn test_start_with_some_data_srcs() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        struct A;
        let mut runner = LogicRunner::new(DataHub::<A>::new());
        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

        let r = runner.start();
        assert!(r.end().is_ok());
    }

    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataSrc::new 1",
            "SyncDataSrc::new 2",
            "SyncDataSrc::setup 1",
            "SyncDataSrc::setup 2",
            "SyncDataSrc::close 2",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::close 1",
            "SyncDataSrc::drop 1",
        ]
    );
}

#[test]
fn test_start_but_failed_to_begin() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        struct A;
        let mut runner = LogicRunner::new(DataHub::<A>::new());
        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Setup));
        runner.uses("baz", SyncDataSrc::new(3, logger.clone(), Fail::None));

        if let Err(err) = runner.start().end() {
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
            "SyncDataSrc::setup 1",
            "SyncDataSrc::setup 2 failed",
            "SyncDataSrc::close 1",
            "SyncDataSrc::drop 3",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::drop 1",
        ]
    );
}

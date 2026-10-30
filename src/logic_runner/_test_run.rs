use crate::data_conn::_test_data::*;
use crate::data_src::_test_data::*;
use crate::{DataAcc, DataHub, DataHubError, LogicRunner};
use std::sync::{Arc, Mutex};

#[test]
fn test_run_with_no_data_src() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        struct Abc;
        let mut runner = LogicRunner::new(DataHub::<Abc>::new());

        assert!(runner
            .run(|_data| {
                logger.lock().unwrap().push("Run the logic".to_string());
                Ok(())
            })
            .is_ok());

        assert!(runner.hub.run.local_data_src_manager.vec_unready.is_empty());
        assert!(runner.hub.run.local_data_src_manager.vec_ready.is_empty());
        assert!(runner.hub.run.local_data_src_manager.local);
        assert_eq!(runner.hub.run.data_src_map.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
        assert!(!runner.hub.run.fixed);
    }

    assert_eq!(*logger.lock().unwrap(), &["Run the logic"]);
}

#[test]
fn test_run_with_some_data_srcs() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        struct Abc;
        let mut runner = LogicRunner::new(DataHub::<Abc>::new());

        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

        assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 2);
        assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
        assert_eq!(runner.hub.run.local_data_src_manager.local, true);
        assert_eq!(runner.hub.run.data_src_map.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
        assert_eq!(runner.hub.run.fixed, false);

        assert!(runner
            .run(|data| {
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                let _conn2 = data.get_data_conn::<SyncDataConn>("bar")?;
                logger.lock().unwrap().push("Run the logic".to_string());
                Ok(())
            })
            .is_ok());

        assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 0);
        assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 2);
        assert_eq!(runner.hub.run.local_data_src_manager.local, true);
        assert_eq!(runner.hub.run.data_src_map.len(), 2);
        assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
        assert_eq!(runner.hub.run.fixed, false);
    }

    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataSrc::new 1",
            "SyncDataSrc::new 2",
            "SyncDataSrc::setup 1",
            "SyncDataSrc::setup 2",
            "SyncDataSrc::create_data_conn 1",
            "SyncDataConn::new 1",
            "SyncDataSrc::create_data_conn 2",
            "SyncDataConn::new 2",
            "Run the logic",
            "SyncDataConn::close 2",
            "SyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
            "SyncDataSrc::close 2",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::close 1",
            "SyncDataSrc::drop 1",
        ]
    );
}

#[test]
fn test_run_but_failed_to_begin() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        struct A;
        let mut runner = LogicRunner::new(DataHub::<A>::new());
        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Setup));
        runner.uses("baz", SyncDataSrc::new(3, logger.clone(), Fail::None));

        assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 3);
        assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
        assert_eq!(runner.hub.run.local_data_src_manager.local, true);
        assert_eq!(runner.hub.run.data_src_map.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
        assert_eq!(runner.hub.run.fixed, false);

        if let Err(err) = runner.run(|_| {
            logger.lock().unwrap().push("Run the logic".to_string());
            Ok(())
        }) {
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

#[test]
fn test_run_but_failed_to_run_logic() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        struct A;
        let mut runner = LogicRunner::new(DataHub::<A>::new());
        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

        if let Err(err) = runner.run(|_| {
            logger
                .lock()
                .unwrap()
                .push("Run the logic but failed".to_string());
            Err(errs::Err::new("logic error".to_string()))
        }) {
            match err.reason::<String>() {
                Ok(s) => assert_eq!(s, "logic error"),
                _ => panic!("{err:?}"),
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
            "SyncDataSrc::setup 1",
            "SyncDataSrc::setup 2",
            "Run the logic but failed",
            "SyncDataSrc::close 2",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::close 1",
            "SyncDataSrc::drop 1",
        ]
    );
}

use crate::data_conn::_test_data::*;
use crate::data_src::_test_data::*;
use crate::{DataAcc, DataConnError, DataHub, DataHubError, DataSrcError, LogicRunner};
use std::sync::{Arc, Mutex};

#[test]
fn test_get_data_conn_by_creating_but_failed_to_cast() {
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

        if let Err(err) = runner.txn(|data| {
            let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
            let _conn2 = data.get_data_conn::<AsyncDataConn>("bar")?;
            logger.lock().unwrap().push("Run the logic".to_string());
            Ok(())
        }) {
            match err.reason::<DataSrcError>() {
                Ok(DataSrcError::FailToCastDataConn { name, target_type }) => {
                    assert_eq!(*name, "bar".into());
                    assert_eq!(*target_type, "sabi::data_conn::_test_data::AsyncDataConn");
                }
                _ => panic!("{err:?}"),
            }
        } else {
            panic!();
        }

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
            "SyncDataConn::rollback 1",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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
fn test_get_data_conn_by_using_cache() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        struct Abc;
        let mut runner = LogicRunner::new(DataHub::<Abc>::with_commit_order(&["bar", "foo"]));

        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

        assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 2);
        assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
        assert_eq!(runner.hub.run.local_data_src_manager.local, true);
        assert_eq!(runner.hub.run.data_src_map.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 2);
        assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 2);
        assert_eq!(runner.hub.run.fixed, false);

        assert!(runner
            .txn(|data| {
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                let _conn2 = data.get_data_conn::<SyncDataConn>("bar")?;

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
        assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 2);
        assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 2);
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
            "SyncDataConn::pre_commit 2",
            "SyncDataConn::pre_commit 1",
            "SyncDataConn::commit 2",
            "SyncDataConn::commit 1",
            "SyncDataConn::post_commit 2",
            "SyncDataConn::post_commit 1",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
            "SyncDataConn::close 2",
            "SyncDataConn::drop 2",
            "SyncDataSrc::close 2",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::close 1",
            "SyncDataSrc::drop 1",
        ]
    );
}

#[test]
fn test_get_data_conn_by_using_cache_but_failed_to_cast() {
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

        if let Err(err) = runner.txn(|data| {
            let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
            let _conn2 = data.get_data_conn::<SyncDataConn>("bar")?;

            let _conn2 = data.get_data_conn::<AsyncDataConn>("bar")?;
            logger.lock().unwrap().push("Run the logic".to_string());
            Ok(())
        }) {
            match err.reason::<DataConnError>() {
                Ok(DataConnError::FailToCastDataConn { name, target_type }) => {
                    assert_eq!(*name, "bar".into());
                    assert_eq!(*target_type, "sabi::data_conn::_test_data::AsyncDataConn");
                }
                _ => panic!("{err:?}"),
            }
        } else {
            panic!();
        }

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
            "SyncDataConn::rollback 1",
            "SyncDataConn::rollback 2",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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
fn test_get_data_conn_but_no_corresponding_data_src() {
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

        if let Err(err) = runner.txn(|data| {
            let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
            let _conn2 = data.get_data_conn::<AsyncDataConn>("xxx")?;
            logger.lock().unwrap().push("Run the logic".to_string());
            Ok(())
        }) {
            match err.reason::<DataHubError>() {
                Ok(DataHubError::NoDataSrcToCreateDataConn {
                    name,
                    data_conn_type,
                }) => {
                    assert_eq!(*name, "xxx".into());
                    assert_eq!(
                        *data_conn_type,
                        "sabi::data_conn::_test_data::AsyncDataConn"
                    );
                }
                _ => panic!("{err:?}"),
            }
        } else {
            panic!();
        }

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
            "SyncDataConn::rollback 1",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
            "SyncDataSrc::close 2",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::close 1",
            "SyncDataSrc::drop 1",
        ]
    );
}

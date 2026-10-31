use crate::tokio::data_conn::_test_data::*;
use crate::tokio::data_src::_test_data::*;
use crate::tokio::*;

use std::sync::{Arc, Mutex};

#[tokio::test]
async fn test_get_data_conn_by_creating_but_failed_to_cast() {
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

        let logger_clone = logger.clone();

        if let Err(err) = runner
            .txn_async(move |data| {
                let logger_clone2 = logger_clone.clone();
                Box::pin(async move {
                    let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                    let _conn2 = data.get_data_conn_async::<AsyncDataConn>("bar").await?;
                    logger_clone2
                        .lock()
                        .unwrap()
                        .push("Run the logic".to_string());
                    Ok(())
                })
            })
            .await
        {
            match err.reason::<DataSrcError>() {
                Ok(DataSrcError::FailToCastDataConn { name, target_type }) => {
                    assert_eq!(*name, "bar".into());
                    assert_eq!(
                        *target_type,
                        "sabi::tokio::data_conn::_test_data::AsyncDataConn"
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
            "SyncDataSrc::setup_async 1",
            "SyncDataSrc::setup_async 2",
            "SyncDataSrc::create_data_conn_async 1",
            "SyncDataConn::new 1",
            "SyncDataConn::rollback_async 1",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
            "SyncDataSrc::close 2",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::close 1",
            "SyncDataSrc::drop 1",
        ]
    );
}

#[tokio::test]
async fn test_get_data_conn_by_using_cache() {
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

        let logger_clone = logger.clone();

        assert!(runner
            .txn_async(move |data| {
                let logger_clone2 = logger_clone.clone();
                Box::pin(async move {
                    let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                    let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;

                    let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                    let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                    logger_clone2
                        .lock()
                        .unwrap()
                        .push("Run the logic".to_string());
                    Ok(())
                })
            })
            .await
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
            "SyncDataSrc::setup_async 1",
            "SyncDataSrc::setup_async 2",
            "SyncDataSrc::create_data_conn_async 1",
            "SyncDataConn::new 1",
            "SyncDataSrc::create_data_conn_async 2",
            "SyncDataConn::new 2",
            "Run the logic",
            "SyncDataConn::pre_commit_async 2",
            "SyncDataConn::pre_commit_async 1",
            "SyncDataConn::commit_async 2",
            "SyncDataConn::commit_async 1",
            "SyncDataConn::post_commit_async 2",
            "SyncDataConn::post_commit_async 1",
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

#[tokio::test]
async fn test_get_data_conn_by_using_cache_but_failed_to_cast() {
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

        let logger_clone = logger.clone();

        if let Err(err) = runner
            .txn_async(move |data| {
                let logger_clone2 = logger_clone.clone();
                Box::pin(async move {
                    let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                    let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;

                    let _conn2 = data.get_data_conn_async::<AsyncDataConn>("bar").await?;
                    logger_clone2
                        .lock()
                        .unwrap()
                        .push("Run the logic".to_string());
                    Ok(())
                })
            })
            .await
        {
            match err.reason::<DataConnError>() {
                Ok(DataConnError::FailToCastDataConn { name, target_type }) => {
                    assert_eq!(*name, "bar".into());
                    assert_eq!(
                        *target_type,
                        "sabi::tokio::data_conn::_test_data::AsyncDataConn"
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
            "SyncDataSrc::setup_async 1",
            "SyncDataSrc::setup_async 2",
            "SyncDataSrc::create_data_conn_async 1",
            "SyncDataConn::new 1",
            "SyncDataSrc::create_data_conn_async 2",
            "SyncDataConn::new 2",
            "SyncDataConn::rollback_async 1",
            "SyncDataConn::rollback_async 2",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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

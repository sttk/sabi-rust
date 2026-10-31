use crate::tokio::data_conn::_test_data::*;
use crate::tokio::data_src::_test_data::*;
use crate::tokio::*;

use std::sync::{Arc, Mutex};

#[tokio::test]
async fn test_txn_with_no_data_srcs() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        struct Abc;
        let mut runner = LogicRunner::new(DataHub::<Abc>::new());

        let logger_clone = logger.clone();
        assert!(runner
            .txn_async(move |_data| {
                let logger_clone2 = logger_clone.clone();
                Box::pin(async move {
                    logger_clone2
                        .lock()
                        .unwrap()
                        .push("Run the logic".to_string());
                    Ok(())
                })
            })
            .await
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

#[tokio::test]
async fn test_txn_with_some_data_srcs() {
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
        assert!(runner
            .txn_async(move |data| {
                let logger_clone2 = logger_clone.clone();
                Box::pin(async move {
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
            "Run the logic",
            "SyncDataConn::pre_commit_async 1",
            "SyncDataConn::pre_commit_async 2",
            "SyncDataConn::commit_async 1",
            "SyncDataConn::commit_async 2",
            "SyncDataConn::post_commit_async 1",
            "SyncDataConn::post_commit_async 2",
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

#[tokio::test]
async fn test_txn_but_failed_to_begin() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        struct Abc;
        let mut runner = LogicRunner::new(DataHub::<Abc>::new());

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

        let logger_clone = logger.clone();

        if let Err(err) = runner
            .txn_async(move |data| {
                let logger_clone2 = logger_clone.clone();
                Box::pin(async move {
                    let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                    let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                    let _conn3 = data.get_data_conn_async::<SyncDataConn>("baz").await?;
                    logger_clone2
                        .lock()
                        .unwrap()
                        .push("Run the logic".to_string());
                    Ok(())
                })
            })
            .await
        {
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

        assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 3);
        assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
        assert_eq!(runner.hub.run.local_data_src_manager.local, true);
        assert_eq!(runner.hub.run.data_src_map.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
        assert_eq!(runner.hub.run.fixed, false);
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

#[tokio::test]
async fn test_txn_but_failed_to_run_logic() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        struct Abc;
        let mut runner = LogicRunner::new(DataHub::<Abc>::new());
        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

        let logger_clone = logger.clone();

        if let Err(err) = runner
            .txn_async(move |data| {
                let logger_clone2 = logger_clone.clone();
                Box::pin(async move {
                    let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                    let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                    logger_clone2
                        .lock()
                        .unwrap()
                        .push("Run the logic but failed".to_string());
                    Err(errs::Err::new("logic error".to_string()))
                })
            })
            .await
        {
            match err.reason::<String>() {
                Ok(s) => assert_eq!(s, "logic error"),
                _ => panic!(),
            }
        } else {
            panic!();
        }
    }

    assert_eq!(
        logger.lock().unwrap()[..],
        [
            "SyncDataSrc::new 1",
            "SyncDataSrc::new 2",
            "SyncDataSrc::setup_async 1",
            "SyncDataSrc::setup_async 2",
            "SyncDataSrc::create_data_conn_async 1",
            "SyncDataConn::new 1",
            "SyncDataSrc::create_data_conn_async 2",
            "SyncDataConn::new 2",
            "Run the logic but failed",
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

#[tokio::test]
async fn test_txn_but_failed_to_pre_commit() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        struct Abc;
        let mut runner = LogicRunner::new(DataHub::<Abc>::new());
        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::PreCommit));
        runner.uses("baz", SyncDataSrc::new(3, logger.clone(), Fail::None));

        let logger_clone = logger.clone();

        if let Err(err) = runner
            .txn_async(move |data| {
                let logger_clone2 = logger_clone.clone();
                Box::pin(async move {
                    let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                    let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                    let _conn3 = data.get_data_conn_async::<SyncDataConn>("baz").await?;
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
                Ok(DataConnError::FailToPreCommitDataConn { errors }) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].index, 1);
                    assert_eq!(errors[0].name, "bar".into());
                    assert_eq!(errors[0].err.reason::<String>().unwrap(), "zzz");
                }
                _ => panic!("{err:?}"),
            }
        } else {
            panic!();
        }
    }

    #[cfg(unix)]
    assert_eq!(
        logger.lock().unwrap()[..],
        [
            "SyncDataSrc::new 1",
            "SyncDataSrc::new 2",
            "SyncDataSrc::new 3",
            "SyncDataSrc::setup_async 1",
            "SyncDataSrc::setup_async 2",
            "SyncDataSrc::setup_async 3",
            "SyncDataSrc::create_data_conn_async 1",
            "SyncDataConn::new 1",
            "SyncDataSrc::create_data_conn_async 2",
            "SyncDataConn::new 2",
            "SyncDataSrc::create_data_conn_async 3",
            "SyncDataConn::new 3",
            "Run the logic",
            "SyncDataConn::pre_commit_async 1",
            "SyncDataConn::pre_commit_async 2 failed",
            "SyncDataConn::rollback_async 1",
            "SyncDataConn::rollback_async 2",
            "SyncDataConn::rollback_async 3",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src/tokio/data_conn/_test_data.rs, line = 94 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src/tokio/data_conn/_test_data.rs, line = 94 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::on_txn_failure_async 3",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src/tokio/data_conn/_test_data.rs, line = 94 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::close 3",
            "SyncDataConn::drop 3",
            "SyncDataConn::close 2",
            "SyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
            "SyncDataSrc::close 3",
            "SyncDataSrc::drop 3",
            "SyncDataSrc::close 2",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::close 1",
            "SyncDataSrc::drop 1",
        ]
    );
    #[cfg(windows)]
    assert_eq!(
        logger.lock().unwrap()[..],
        [
            "SyncDataSrc::new 1",
            "SyncDataSrc::new 2",
            "SyncDataSrc::new 3",
            "SyncDataSrc::setup_async 1",
            "SyncDataSrc::setup_async 2",
            "SyncDataSrc::setup_async 3",
            "SyncDataSrc::create_data_conn_async 1",
            "SyncDataConn::new 1",
            "SyncDataSrc::create_data_conn_async 2",
            "SyncDataConn::new 2",
            "SyncDataSrc::create_data_conn_async 3",
            "SyncDataConn::new 3",
            "Run the logic",
            "SyncDataConn::pre_commit_async 1",
            "SyncDataConn::pre_commit_async 2 failed",
            "SyncDataConn::rollback_async 1",
            "SyncDataConn::rollback_async 2",
            "SyncDataConn::rollback_async 3",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src\\tokio\\data_conn\\_test_data.rs, line = 94 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src\\tokio\\data_conn\\_test_data.rs, line = 94 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::on_txn_failure_async 3",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src\\tokio\\data_conn\\_test_data.rs, line = 94 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::close 3",
            "SyncDataConn::drop 3",
            "SyncDataConn::close 2",
            "SyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
            "SyncDataSrc::close 3",
            "SyncDataSrc::drop 3",
            "SyncDataSrc::close 2",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::close 1",
            "SyncDataSrc::drop 1",
        ]
    );
}

#[tokio::test]
async fn test_txn_but_failed_to_commit() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        struct Abc;
        let mut runner = LogicRunner::new(DataHub::<Abc>::new());
        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Commit));
        runner.uses("baz", SyncDataSrc::new(3, logger.clone(), Fail::None));

        let logger_clone = logger.clone();

        if let Err(err) = runner
            .txn_async(move |data| {
                let logger_clone2 = logger_clone.clone();
                Box::pin(async move {
                    let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                    let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                    let _conn3 = data.get_data_conn_async::<SyncDataConn>("baz").await?;
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
                Ok(DataConnError::FailToCommitDataConn { errors }) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].index, 1);
                    assert_eq!(errors[0].name, "bar".into());
                    assert_eq!(errors[0].err.reason::<String>().unwrap(), "ZZZ");
                }
                _ => panic!("{err:?}"),
            }
        } else {
            panic!();
        }
    }

    #[cfg(unix)]
    assert_eq!(
        logger.lock().unwrap()[0..38],
        [
            "SyncDataSrc::new 1",
            "SyncDataSrc::new 2",
            "SyncDataSrc::new 3",
            "SyncDataSrc::setup_async 1",
            "SyncDataSrc::setup_async 2",
            "SyncDataSrc::setup_async 3",
            "SyncDataSrc::create_data_conn_async 1",
            "SyncDataConn::new 1",
            "SyncDataSrc::create_data_conn_async 2",
            "SyncDataConn::new 2",
            "SyncDataSrc::create_data_conn_async 3",
            "SyncDataConn::new 3",
            "Run the logic",
            "SyncDataConn::pre_commit_async 1",
            "SyncDataConn::pre_commit_async 2",
            "SyncDataConn::pre_commit_async 3",
            "SyncDataConn::commit_async 1",
            "SyncDataConn::commit_async 2 failed",
            "SyncDataConn::rollback_async 2",
            "SyncDataConn::rollback_async 3",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/tokio/data_conn/_test_data.rs, line = 68 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/tokio/data_conn/_test_data.rs, line = 68 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::on_txn_failure_async 3",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/tokio/data_conn/_test_data.rs, line = 68 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::close 3",
            "SyncDataConn::drop 3",
            "SyncDataConn::close 2",
            "SyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
            "SyncDataSrc::close 3",
            "SyncDataSrc::drop 3",
            "SyncDataSrc::close 2",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::close 1",
            "SyncDataSrc::drop 1",
        ]
    );
    #[cfg(windows)]
    assert_eq!(
        logger.lock().unwrap()[0..38],
        [
            "SyncDataSrc::new 1",
            "SyncDataSrc::new 2",
            "SyncDataSrc::new 3",
            "SyncDataSrc::setup_async 1",
            "SyncDataSrc::setup_async 2",
            "SyncDataSrc::setup_async 3",
            "SyncDataSrc::create_data_conn_async 1",
            "SyncDataConn::new 1",
            "SyncDataSrc::create_data_conn_async 2",
            "SyncDataConn::new 2",
            "SyncDataSrc::create_data_conn_async 3",
            "SyncDataConn::new 3",
            "Run the logic",
            "SyncDataConn::pre_commit_async 1",
            "SyncDataConn::pre_commit_async 2",
            "SyncDataConn::pre_commit_async 3",
            "SyncDataConn::commit_async 1",
            "SyncDataConn::commit_async 2 failed",
            "SyncDataConn::rollback_async 2",
            "SyncDataConn::rollback_async 3",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src\\tokio\\data_conn\\_test_data.rs, line = 68 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src\\tokio\\data_conn\\_test_data.rs, line = 68 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::on_txn_failure_async 3",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src\\tokio\\data_conn\\_test_data.rs, line = 68 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::close 3",
            "SyncDataConn::drop 3",
            "SyncDataConn::close 2",
            "SyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
            "SyncDataSrc::close 3",
            "SyncDataSrc::drop 3",
            "SyncDataSrc::close 2",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::close 1",
            "SyncDataSrc::drop 1",
        ]
    );
}

#[tokio::test]
async fn test_txn_but_failed_to_post_commit() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        struct Abc;
        let mut runner = LogicRunner::new(DataHub::<Abc>::new());
        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::PostCommit));
        runner.uses("baz", SyncDataSrc::new(3, logger.clone(), Fail::None));

        let logger_clone = logger.clone();

        if let Err(err) = runner
            .txn_async(move |data| {
                let logger_clone2 = logger_clone.clone();
                Box::pin(async move {
                    let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                    let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                    let _conn3 = data.get_data_conn_async::<SyncDataConn>("baz").await?;
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
                Ok(DataConnError::FailToPostCommitDataConn { errors }) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].index, 1);
                    assert_eq!(errors[0].name, "bar".into());
                    assert_eq!(errors[0].err.reason::<String>().unwrap(), "!!!");
                }
                _ => panic!("{err:?}"),
            }
        } else {
            panic!();
        }
    }

    #[cfg(unix)]
    assert_eq!(
        logger.lock().unwrap()[..],
        [
            "SyncDataSrc::new 1",
            "SyncDataSrc::new 2",
            "SyncDataSrc::new 3",
            "SyncDataSrc::setup_async 1",
            "SyncDataSrc::setup_async 2",
            "SyncDataSrc::setup_async 3",
            "SyncDataSrc::create_data_conn_async 1",
            "SyncDataConn::new 1",
            "SyncDataSrc::create_data_conn_async 2",
            "SyncDataConn::new 2",
            "SyncDataSrc::create_data_conn_async 3",
            "SyncDataConn::new 3",
            "Run the logic",
            "SyncDataConn::pre_commit_async 1",
            "SyncDataConn::pre_commit_async 2",
            "SyncDataConn::pre_commit_async 3",
            "SyncDataConn::commit_async 1",
            "SyncDataConn::commit_async 2",
            "SyncDataConn::commit_async 3",
            "SyncDataConn::post_commit_async 1",
            "SyncDataConn::post_commit_async 2 failed",
            "SyncDataConn::post_commit_async 3",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/data_conn/_test_data.rs, line = 121 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
            "SyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/data_conn/_test_data.rs, line = 121 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
            "SyncDataConn::on_txn_failure_async 3",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/data_conn/_test_data.rs, line = 121 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
            "SyncDataConn::close 3",
            "SyncDataConn::drop 3",
            "SyncDataConn::close 2",
            "SyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
            "SyncDataSrc::close 3",
            "SyncDataSrc::drop 3",
            "SyncDataSrc::close 2",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::close 1",
            "SyncDataSrc::drop 1",
        ]
    );
    #[cfg(windows)]
    assert_eq!(
        logger.lock().unwrap()[..],
        [
            "SyncDataSrc::new 1",
            "SyncDataSrc::new 2",
            "SyncDataSrc::new 3",
            "SyncDataSrc::setup_async 1",
            "SyncDataSrc::setup_async 2",
            "SyncDataSrc::setup_async 3",
            "SyncDataSrc::create_data_conn_async 1",
            "SyncDataConn::new 1",
            "SyncDataSrc::create_data_conn_async 2",
            "SyncDataConn::new 2",
            "SyncDataSrc::create_data_conn_async 3",
            "SyncDataConn::new 3",
            "Run the logic",
            "SyncDataConn::pre_commit_async 1",
            "SyncDataConn::pre_commit_async 2",
            "SyncDataConn::pre_commit_async 3",
            "SyncDataConn::commit_async 1",
            "SyncDataConn::commit_async 2",
            "SyncDataConn::commit_async 3",
            "SyncDataConn::post_commit_async 1",
            "SyncDataConn::post_commit_async 2 failed",
            "SyncDataConn::post_commit_async 3",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\data_conn\\_test_data.rs, line = 121 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
            "SyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\data_conn\\_test_data.rs, line = 121 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
            "SyncDataConn::on_txn_failure_async 3",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\data_conn\\_test_data.rs, line = 121 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
            "SyncDataConn::close 3",
            "SyncDataConn::drop 3",
            "SyncDataConn::close 2",
            "SyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
            "SyncDataSrc::close 3",
            "SyncDataSrc::drop 3",
            "SyncDataSrc::close 2",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::close 1",
            "SyncDataSrc::drop 1",
        ]
    );
}

#[tokio::test]
async fn test_txn_but_failed_to_rollback() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        struct Abc;
        let mut runner = LogicRunner::new(DataHub::<Abc>::new());
        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Rollback));
        runner.uses("baz", SyncDataSrc::new(3, logger.clone(), Fail::None));

        let logger_clone = logger.clone();

        if let Err(err) = runner
            .txn_async(move |data| {
                let logger_clone2 = logger_clone.clone();
                Box::pin(async move {
                    let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                    let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                    let _conn3 = data.get_data_conn_async::<SyncDataConn>("baz").await?;
                    logger_clone2
                        .lock()
                        .unwrap()
                        .push("Run the logic but failed".to_string());
                    Err(errs::Err::new("fail".to_string()))
                })
            })
            .await
        {
            match err.reason::<String>() {
                Ok(s) => assert_eq!(s, "fail"),
                _ => panic!("{err:?}"),
            }
        } else {
            panic!();
        }
    }

    #[cfg(unix)]
    assert_eq!(
        logger.lock().unwrap()[0..34],
        [
            "SyncDataSrc::new 1",
            "SyncDataSrc::new 2",
            "SyncDataSrc::new 3",
            "SyncDataSrc::setup_async 1",
            "SyncDataSrc::setup_async 2",
            "SyncDataSrc::setup_async 3",
            "SyncDataSrc::create_data_conn_async 1",
            "SyncDataConn::new 1",
            "SyncDataSrc::create_data_conn_async 2",
            "SyncDataConn::new 2",
            "SyncDataSrc::create_data_conn_async 3",
            "SyncDataConn::new 3",
            "Run the logic but failed",
            "SyncDataConn::rollback_async 1",
            "SyncDataConn::rollback_async 2 failed",
            "SyncDataConn::rollback_async 3",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/data_conn/_test_data.rs, line = 149 }) }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/data_conn/_test_data.rs, line = 149 }) }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::on_txn_failure_async 3",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/data_conn/_test_data.rs, line = 149 }) }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::close 3",
            "SyncDataConn::drop 3",
            "SyncDataConn::close 2",
            "SyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
            "SyncDataSrc::close 3",
            "SyncDataSrc::drop 3",
            "SyncDataSrc::close 2",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::close 1",
            "SyncDataSrc::drop 1",
        ]
    );
    #[cfg(windows)]
    assert_eq!(
        logger.lock().unwrap()[0..34],
        [
            "SyncDataSrc::new 1",
            "SyncDataSrc::new 2",
            "SyncDataSrc::new 3",
            "SyncDataSrc::setup_async 1",
            "SyncDataSrc::setup_async 2",
            "SyncDataSrc::setup_async 3",
            "SyncDataSrc::create_data_conn_async 1",
            "SyncDataConn::new 1",
            "SyncDataSrc::create_data_conn_async 2",
            "SyncDataConn::new 2",
            "SyncDataSrc::create_data_conn_async 3",
            "SyncDataConn::new 3",
            "Run the logic but failed",
            "SyncDataConn::rollback_async 1",
            "SyncDataConn::rollback_async 2 failed",
            "SyncDataConn::rollback_async 3",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\data_conn\\_test_data.rs, line = 149 }) }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\data_conn\\_test_data.rs, line = 149 }) }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::on_txn_failure_async 3",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\data_conn\\_test_data.rs, line = 149 }) }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::close 3",
            "SyncDataConn::drop 3",
            "SyncDataConn::close 2",
            "SyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
            "SyncDataSrc::close 3",
            "SyncDataSrc::drop 3",
            "SyncDataSrc::close 2",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::close 1",
            "SyncDataSrc::drop 1",
        ]
    );
}

#[tokio::test]
async fn test_txn_with_commit_order() {
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

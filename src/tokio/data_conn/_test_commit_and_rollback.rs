use super::_test_data::*;
use super::*;
use std::panic::panic_any;
use std::ptr;
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn test_commit_and_rollback_ok() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
        let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let conn = AsyncDataConn::new(2, logger.clone(), Fail::None);
        let boxed = Box::new(DataConnContainer::new("bar".to_string(), Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let mut reports = manager.new_failure_reports();
        assert!(manager.commit_async(&mut reports).await.is_ok());
        manager.rollback_async(reports).await;
    }

    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::pre_commit_async 1",
            "AsyncDataConn::pre_commit_async 2",
            "SyncDataConn::commit_async 1",
            "AsyncDataConn::commit_async 2",
            "SyncDataConn::post_commit_async 1",
            "AsyncDataConn::post_commit_async 2",
            "SyncDataConn::on_txn_failure_async 1",
            &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}]"),
            "AsyncDataConn::on_txn_failure_async 2",
            &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}]"),
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
}

#[tokio::test]
async fn test_commit_with_order_and_rollback_ok() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::with_commit_order(&["bar", "baz", "foo"]);

        let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
        let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let conn = AsyncDataConn::new(2, logger.clone(), Fail::None);
        let boxed = Box::new(DataConnContainer::new("bar".to_string(), Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let conn = SyncDataConn::new(3, logger.clone(), Fail::None);
        let boxed = Box::new(DataConnContainer::new("qux", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let mut reports = manager.new_failure_reports();
        assert!(manager.commit_async(&mut reports).await.is_ok());
        manager.rollback_async(reports).await;
    }

    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::new 3",
            "SyncDataConn::pre_commit_async 1",
            "SyncDataConn::pre_commit_async 3",
            "AsyncDataConn::pre_commit_async 2",
            "SyncDataConn::commit_async 1",
            "SyncDataConn::commit_async 3",
            "AsyncDataConn::commit_async 2",
            "SyncDataConn::post_commit_async 1",
            "SyncDataConn::post_commit_async 3",
            "AsyncDataConn::post_commit_async 2",
            "SyncDataConn::on_txn_failure_async 1",
            &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"qux\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}]"),
            "SyncDataConn::on_txn_failure_async 3",
            &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"qux\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}]"),
            "AsyncDataConn::on_txn_failure_async 2",
            &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"qux\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}]"),
            "SyncDataConn::close 3",
            "SyncDataConn::drop 3",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
        ]
    );
}

#[tokio::test]
async fn test_commit_and_rollback_but_fail_first_sync_pre_commit() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = SyncDataConn::new(1, logger.clone(), Fail::PreCommit);
        let boxed = Box::new(DataConnContainer::new("foo".to_string(), Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let conn = AsyncDataConn::new(2, logger.clone(), Fail::PreCommit);
        let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let mut reports = manager.new_failure_reports();
        if let Err(e) = manager.commit_async(&mut reports).await {
            match e.reason::<DataConnError>() {
                Ok(DataConnError::FailToPreCommitDataConn { errors }) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].index, 0);
                    assert_eq!(errors[0].name, "foo".into());
                    assert_eq!(errors[0].err.reason::<String>().unwrap(), "zzz");
                }
                _ => panic!(),
            }
        } else {
            panic!();
        }
        manager.rollback_async(reports).await;
    }

    #[cfg(unix)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::pre_commit_async 1 failed",
            "SyncDataConn::rollback_async 1",
            "AsyncDataConn::rollback_async 2",
            "SyncDataConn::on_txn_failure_async 1",
            &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"zzz\", file = src/tokio/data_conn/_test_data.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", 94),
            "AsyncDataConn::on_txn_failure_async 2",
            &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"zzz\", file = src/tokio/data_conn/_test_data.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", 94),
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
    #[cfg(windows)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::pre_commit_async 1 failed",
            "SyncDataConn::rollback_async 1",
            "AsyncDataConn::rollback_async 2",
            "SyncDataConn::on_txn_failure_async 1",
            &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"zzz\", file = src\\tokio\\data_conn\\_test_data.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", 94),
            "AsyncDataConn::on_txn_failure_async 2",
            &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"zzz\", file = src\\tokio\\data_conn\\_test_data.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", 94),
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
}

#[tokio::test]
async fn test_commit_and_rollback_but_fail_first_async_pre_commit() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = SyncDataConn::new(1, logger.clone(), Fail::PreCommit);
        let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let conn = AsyncDataConn::new(2, logger.clone(), Fail::PreCommit);
        let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let mut reports = manager.new_failure_reports();
        if let Err(e) = manager.commit_async(&mut reports).await {
            match e.reason::<DataConnError>() {
                Ok(DataConnError::FailToPreCommitDataConn { errors }) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].index, 0);
                    assert_eq!(errors[0].name, "foo".into());
                    assert_eq!(errors[0].err.reason::<String>().unwrap(), "zzz");
                }
                _ => panic!(),
            }
        } else {
            panic!();
        }
        manager.rollback_async(reports).await;
    }

    #[cfg(unix)]
    assert_eq!(*logger.lock().unwrap(), &[
        "SyncDataConn::new 1",
        "AsyncDataConn::new 2",
        "SyncDataConn::pre_commit_async 1 failed",
        "SyncDataConn::rollback_async 1",
        "AsyncDataConn::rollback_async 2",
        "SyncDataConn::on_txn_failure_async 1",
        "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src/tokio/data_conn/_test_data.rs, line = 94 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
        "AsyncDataConn::on_txn_failure_async 2",
        "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src/tokio/data_conn/_test_data.rs, line = 94 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
        "AsyncDataConn::close 2",
        "AsyncDataConn::drop 2",
        "SyncDataConn::close 1",
        "SyncDataConn::drop 1",
    ]);
    #[cfg(windows)]
    assert_eq!(*logger.lock().unwrap(), &[
        "SyncDataConn::new 1",
        "AsyncDataConn::new 2",
        "SyncDataConn::pre_commit_async 1 failed",
        "SyncDataConn::rollback_async 1",
        "AsyncDataConn::rollback_async 2",
        "SyncDataConn::on_txn_failure_async 1",
        "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src\\tokio\\data_conn\\_test_data.rs, line = 94 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
        "AsyncDataConn::on_txn_failure_async 2",
        "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src\\tokio\\data_conn\\_test_data.rs, line = 94 }), rollback: NoneByRolledBack }}, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
        "AsyncDataConn::close 2",
        "AsyncDataConn::drop 2",
        "SyncDataConn::close 1",
        "SyncDataConn::drop 1",
    ]);
}

#[tokio::test]
async fn test_commit_and_rollback_but_fail_second_pre_commit() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = AsyncDataConn::new(1, logger.clone(), Fail::PreCommit);
        let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let conn = SyncDataConn::new(2, logger.clone(), Fail::None);
        let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let mut reports = manager.new_failure_reports();
        if let Err(e) = manager.commit_async(&mut reports).await {
            match e.reason::<DataConnError>() {
                Ok(DataConnError::FailToPreCommitDataConn { errors }) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].index, 0);
                    assert_eq!(errors[0].name, "foo".into());
                    assert_eq!(errors[0].err.reason::<String>().unwrap(), "yyy");
                }
                _ => panic!(),
            }
        } else {
            panic!();
        }
        manager.rollback_async(reports).await;
    }

    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "AsyncDataConn::new 1",
            "SyncDataConn::new 2",
            "SyncDataConn::pre_commit_async 2",
            "AsyncDataConn::pre_commit_async 1 failed",
            "SyncDataConn::rollback_async 2",
            "AsyncDataConn::rollback_async 1",
            "SyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"yyy\", file = src/tokio/data_conn/_test_data.rs, line = 271 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"yyy\", file = src/tokio/data_conn/_test_data.rs, line = 271 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::close 2",
            "SyncDataConn::drop 2",
            "AsyncDataConn::close 1",
            "AsyncDataConn::drop 1",
        ]
    );
}

#[tokio::test]
async fn test_commit_and_rollback_but_fail_first_sync_commit() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = SyncDataConn::new(1, logger.clone(), Fail::Commit);
        let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let conn = AsyncDataConn::new(2, logger.clone(), Fail::Commit);
        let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let mut reports = manager.new_failure_reports();
        if let Err(e) = manager.commit_async(&mut reports).await {
            match e.reason::<DataConnError>() {
                Ok(DataConnError::FailToCommitDataConn { errors }) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].index, 0);
                    assert_eq!(errors[0].name, "foo".into());
                    assert_eq!(errors[0].err.reason::<String>().unwrap(), "ZZZ");
                }
                _ => panic!(),
            }
        } else {
            panic!();
        }
        manager.rollback_async(reports).await;
    }

    #[cfg(unix)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::pre_commit_async 1",
            "AsyncDataConn::pre_commit_async 2",
            "SyncDataConn::commit_async 1 failed",
            "SyncDataConn::rollback_async 1",
            "AsyncDataConn::rollback_async 2",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/tokio/data_conn/_test_data.rs, line = 68 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/tokio/data_conn/_test_data.rs, line = 68 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
    #[cfg(windows)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::pre_commit_async 1",
            "AsyncDataConn::pre_commit_async 2",
            "SyncDataConn::commit_async 1 failed",
            "SyncDataConn::rollback_async 1",
            "AsyncDataConn::rollback_async 2",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src\\tokio\\data_conn\\_test_data.rs, line = 68 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src\\tokio\\data_conn\\_test_data.rs, line = 68 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
}

#[tokio::test]
async fn test_commit_and_rollback_but_fail_first_async_commit() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = AsyncDataConn::new(1, logger.clone(), Fail::Commit);
        let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let conn = SyncDataConn::new(2, logger.clone(), Fail::None);
        let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let mut reports = manager.new_failure_reports();
        if let Err(e) = manager.commit_async(&mut reports).await {
            match e.reason::<DataConnError>() {
                Ok(DataConnError::FailToCommitDataConn { errors }) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].index, 0);
                    assert_eq!(errors[0].name, "foo".into());
                    assert_eq!(errors[0].err.reason::<String>().unwrap(), "YYY");
                }
                _ => panic!(),
            }
        } else {
            panic!();
        }
        manager.rollback_async(reports).await;
    }

    #[cfg(unix)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "AsyncDataConn::new 1",
            "SyncDataConn::new 2",
            "SyncDataConn::pre_commit_async 2",
            "AsyncDataConn::pre_commit_async 1",
            "SyncDataConn::commit_async 2",
            "AsyncDataConn::commit_async 1 failed",
            "AsyncDataConn::rollback_async 1",
            "SyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src/tokio/data_conn/_test_data.rs, line = 241 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
            "AsyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src/tokio/data_conn/_test_data.rs, line = 241 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
            "SyncDataConn::close 2",
            "SyncDataConn::drop 2",
            "AsyncDataConn::close 1",
            "AsyncDataConn::drop 1",
        ]
    );
    #[cfg(windows)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "AsyncDataConn::new 1",
            "SyncDataConn::new 2",
            "SyncDataConn::pre_commit_async 2",
            "AsyncDataConn::pre_commit_async 1",
            "SyncDataConn::commit_async 2",
            "AsyncDataConn::commit_async 1 failed",
            "AsyncDataConn::rollback_async 1",
            "SyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src\\tokio\\data_conn\\_test_data.rs, line = 241 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
            "AsyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src\\tokio\\data_conn\\_test_data.rs, line = 241 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
            "SyncDataConn::close 2",
            "SyncDataConn::drop 2",
            "AsyncDataConn::close 1",
            "AsyncDataConn::drop 1",
        ]
    );
}

#[tokio::test]
async fn test_commit_and_rollback_but_fail_second_commit() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
        let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let conn = AsyncDataConn::new(2, logger.clone(), Fail::Commit);
        let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let mut reports = manager.new_failure_reports();
        if let Err(e) = manager.commit_async(&mut reports).await {
            match e.reason::<DataConnError>() {
                Ok(DataConnError::FailToCommitDataConn { errors }) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].index, 1);
                    assert_eq!(errors[0].name, "bar".into());
                    assert_eq!(errors[0].err.reason::<String>().unwrap(), "YYY");
                }
                _ => panic!(),
            }
        } else {
            panic!();
        }
        manager.rollback_async(reports).await;
    }

    #[cfg(unix)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::pre_commit_async 1",
            "AsyncDataConn::pre_commit_async 2",
            "SyncDataConn::commit_async 1",
            "AsyncDataConn::commit_async 2 failed",
            "AsyncDataConn::rollback_async 2",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src/tokio/data_conn/_test_data.rs, line = 241 }), rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src/tokio/data_conn/_test_data.rs, line = 241 }), rollback: NoneByRolledBack }]",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
    #[cfg(windows)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::pre_commit_async 1",
            "AsyncDataConn::pre_commit_async 2",
            "SyncDataConn::commit_async 1",
            "AsyncDataConn::commit_async 2 failed",
            "AsyncDataConn::rollback_async 2",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src\\tokio\\data_conn\\_test_data.rs, line = 241 }), rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src\\tokio\\data_conn\\_test_data.rs, line = 241 }), rollback: NoneByRolledBack }]",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
}

#[tokio::test]
async fn test_commit_and_rollback_but_fail_first_sync_post_commit() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = SyncDataConn::new(1, logger.clone(), Fail::PostCommit);
        let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let conn = AsyncDataConn::new(2, logger.clone(), Fail::PostCommit);
        let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let mut reports = manager.new_failure_reports();
        if let Err(e) = manager.commit_async(&mut reports).await {
            match e.reason::<DataConnError>() {
                Ok(DataConnError::FailToPostCommitDataConn { errors }) => {
                    assert_eq!(errors.len(), 2);
                    assert_eq!(errors[0].index, 0);
                    assert_eq!(errors[0].name, "foo".into());
                    assert_eq!(errors[0].err.reason::<String>().unwrap(), "!!!");
                    assert_eq!(errors[1].index, 1);
                    assert_eq!(errors[1].name, "bar".into());
                    assert_eq!(errors[1].err.reason::<String>().unwrap(), "!!!");
                }
                _ => panic!(),
            }
        } else {
            panic!();
        }
        manager.rollback_async(reports).await;
    }

    #[cfg(unix)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::pre_commit_async 1",
            "AsyncDataConn::pre_commit_async 2",
            "SyncDataConn::commit_async 1",
            "AsyncDataConn::commit_async 2",
            "SyncDataConn::post_commit_async 1 failed",
            "AsyncDataConn::post_commit_async 2 failed",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/data_conn/_test_data.rs, line = 121 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/data_conn/_test_data.rs, line = 301 }), rollback: NoneByNotRolledBack }]",
            "AsyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/data_conn/_test_data.rs, line = 121 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/data_conn/_test_data.rs, line = 301 }), rollback: NoneByNotRolledBack }]",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
    #[cfg(windows)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::pre_commit_async 1",
            "AsyncDataConn::pre_commit_async 2",
            "SyncDataConn::commit_async 1",
            "AsyncDataConn::commit_async 2",
            "SyncDataConn::post_commit_async 1 failed",
            "AsyncDataConn::post_commit_async 2 failed",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\data_conn\\_test_data.rs, line = 121 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\data_conn\\_test_data.rs, line = 301 }), rollback: NoneByNotRolledBack }]",
            "AsyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\data_conn\\_test_data.rs, line = 121 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\data_conn\\_test_data.rs, line = 301 }), rollback: NoneByNotRolledBack }]",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
}

#[tokio::test]
async fn test_commit_and_rollback_but_fail_first_async_post_commit() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = AsyncDataConn::new(2, logger.clone(), Fail::PostCommit);
        let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let conn = SyncDataConn::new(1, logger.clone(), Fail::PostCommit);
        let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let mut reports = manager.new_failure_reports();
        if let Err(e) = manager.commit_async(&mut reports).await {
            match e.reason::<DataConnError>() {
                Ok(DataConnError::FailToPostCommitDataConn { errors }) => {
                    assert_eq!(errors.len(), 2);
                    assert_eq!(errors[0].index, 1);
                    assert_eq!(errors[0].name, "foo".into());
                    assert_eq!(errors[0].err.reason::<String>().unwrap(), "!!!");
                    assert_eq!(errors[1].index, 0);
                    assert_eq!(errors[1].name, "bar".into());
                    assert_eq!(errors[1].err.reason::<String>().unwrap(), "!!!");
                }
                _ => panic!(),
            }
        } else {
            panic!();
        }
        manager.rollback_async(reports).await;
    }

    #[cfg(unix)]
    assert_eq!(*logger.lock().unwrap(), &[
        "AsyncDataConn::new 2",
        "SyncDataConn::new 1",
        "SyncDataConn::pre_commit_async 1",
        "AsyncDataConn::pre_commit_async 2",
        "SyncDataConn::commit_async 1",
        "AsyncDataConn::commit_async 2",
        "SyncDataConn::post_commit_async 1 failed",
        "AsyncDataConn::post_commit_async 2 failed",
        "SyncDataConn::on_txn_failure_async 1",
        "TxnFailureReports=[TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/data_conn/_test_data.rs, line = 301 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/data_conn/_test_data.rs, line = 121 }), rollback: NoneByNotRolledBack }]",
        "AsyncDataConn::on_txn_failure_async 2",
        "TxnFailureReports=[TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/data_conn/_test_data.rs, line = 301 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/data_conn/_test_data.rs, line = 121 }), rollback: NoneByNotRolledBack }]",
        "SyncDataConn::close 1",
        "SyncDataConn::drop 1",
        "AsyncDataConn::close 2",
        "AsyncDataConn::drop 2",
    ]);
    #[cfg(windows)]
    assert_eq!(*logger.lock().unwrap(), &[
        "AsyncDataConn::new 2",
        "SyncDataConn::new 1",
        "SyncDataConn::pre_commit_async 1",
        "AsyncDataConn::pre_commit_async 2",
        "SyncDataConn::commit_async 1",
        "AsyncDataConn::commit_async 2",
        "SyncDataConn::post_commit_async 1 failed",
        "AsyncDataConn::post_commit_async 2 failed",
        "SyncDataConn::on_txn_failure_async 1",
        "TxnFailureReports=[TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\data_conn\\_test_data.rs, line = 301 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\data_conn\\_test_data.rs, line = 121 }), rollback: NoneByNotRolledBack }]",
        "AsyncDataConn::on_txn_failure_async 2",
        "TxnFailureReports=[TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\data_conn\\_test_data.rs, line = 301 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\data_conn\\_test_data.rs, line = 121 }), rollback: NoneByNotRolledBack }]",
        "SyncDataConn::close 1",
        "SyncDataConn::drop 1",
        "AsyncDataConn::close 2",
        "AsyncDataConn::drop 2",
    ]);
}

#[tokio::test]
async fn test_commit_and_rollback_but_fail_second_post_commit() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = AsyncDataConn::new(2, logger.clone(), Fail::None);
        let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let conn = SyncDataConn::new(1, logger.clone(), Fail::PostCommit);
        let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let mut reports = manager.new_failure_reports();
        if let Err(e) = manager.commit_async(&mut reports).await {
            match e.reason::<DataConnError>() {
                Ok(DataConnError::FailToPostCommitDataConn { errors }) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].index, 1);
                    assert_eq!(errors[0].name, "foo".into());
                    assert_eq!(errors[0].err.reason::<String>().unwrap(), "!!!");
                }
                _ => panic!(),
            }
        } else {
            panic!();
        }
        manager.rollback_async(reports).await;
    }

    #[cfg(unix)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "AsyncDataConn::new 2",
            "SyncDataConn::new 1",
            "SyncDataConn::pre_commit_async 1",
            "AsyncDataConn::pre_commit_async 2",
            "SyncDataConn::commit_async 1",
            "AsyncDataConn::commit_async 2",
            "SyncDataConn::post_commit_async 1 failed",
            "AsyncDataConn::post_commit_async 2",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/data_conn/_test_data.rs, line = 121 }), rollback: NoneByNotRolledBack }]",
            "AsyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/data_conn/_test_data.rs, line = 121 }), rollback: NoneByNotRolledBack }]",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
        ]
    );
    #[cfg(windows)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "AsyncDataConn::new 2",
            "SyncDataConn::new 1",
            "SyncDataConn::pre_commit_async 1",
            "AsyncDataConn::pre_commit_async 2",
            "SyncDataConn::commit_async 1",
            "AsyncDataConn::commit_async 2",
            "SyncDataConn::post_commit_async 1 failed",
            "AsyncDataConn::post_commit_async 2",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\data_conn\\_test_data.rs, line = 121 }), rollback: NoneByNotRolledBack }]",
            "AsyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\data_conn\\_test_data.rs, line = 121 }), rollback: NoneByNotRolledBack }]",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
        ]
    );
}

#[tokio::test]
async fn test_commit_and_rollback_but_fail_second_post_commit_and_contains_no_commit() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
        let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let conn = AsyncDataConn::new(2, logger.clone(), Fail::PostCommit);
        let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let conn = NoCommitDataConn::new(3, logger.clone());
        let boxed = Box::new(DataConnContainer::new("baz", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let mut reports = manager.new_failure_reports();

        if let Err(e) = manager.commit_async(&mut reports).await {
            match e.reason::<DataConnError>() {
                Ok(DataConnError::FailToPostCommitDataConn { errors }) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].index, 1);
                    assert_eq!(errors[0].name, "bar".into());
                    assert_eq!(errors[0].err.reason::<String>().unwrap(), "!!!");
                }
                _ => panic_any(e),
            }
        } else {
            panic!();
        }
        manager.rollback_async(reports).await;
    }

    #[cfg(unix)]
    assert_eq!(
        &logger.lock().unwrap()[0..24],
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "NoCommitDataConn::new 3",
            "SyncDataConn::pre_commit_async 1",
            "NoCommitDataConn::pre_commit_async 3",
            "AsyncDataConn::pre_commit_async 2",
            "SyncDataConn::commit_async 1",
            "NoCommitDataConn::commit_async 3",
            "AsyncDataConn::commit_async 2",
            "SyncDataConn::post_commit_async 1",
            "NoCommitDataConn::post_commit_async 3",
            "AsyncDataConn::post_commit_async 2 failed",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/data_conn/_test_data.rs, line = 301 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::NoCommitDataConn\", cause: NoneByUncommitted, rollback: NoneByNotRolledBack }]",
            "NoCommitDataConn::on_txn_failure_async 3",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/data_conn/_test_data.rs, line = 301 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::NoCommitDataConn\", cause: NoneByUncommitted, rollback: NoneByNotRolledBack }]",
            "AsyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/data_conn/_test_data.rs, line = 301 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::NoCommitDataConn\", cause: NoneByUncommitted, rollback: NoneByNotRolledBack }]",
            "NoCommitDataConn::close 3",
            "NoCommitDataConn::drop 3",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
    #[cfg(windows)]
    assert_eq!(
        &logger.lock().unwrap()[0..24],
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "NoCommitDataConn::new 3",
            "SyncDataConn::pre_commit_async 1",
            "NoCommitDataConn::pre_commit_async 3",
            "AsyncDataConn::pre_commit_async 2",
            "SyncDataConn::commit_async 1",
            "NoCommitDataConn::commit_async 3",
            "AsyncDataConn::commit_async 2",
            "SyncDataConn::post_commit_async 1",
            "NoCommitDataConn::post_commit_async 3",
            "AsyncDataConn::post_commit_async 2 failed",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\data_conn\\_test_data.rs, line = 301 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::NoCommitDataConn\", cause: NoneByUncommitted, rollback: NoneByNotRolledBack }]",
            "NoCommitDataConn::on_txn_failure_async 3",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\data_conn\\_test_data.rs, line = 301 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::NoCommitDataConn\", cause: NoneByUncommitted, rollback: NoneByNotRolledBack }]",
            "AsyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\data_conn\\_test_data.rs, line = 301 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::data_conn::_test_data::NoCommitDataConn\", cause: NoneByUncommitted, rollback: NoneByNotRolledBack }]",
            "NoCommitDataConn::close 3",
            "NoCommitDataConn::drop 3",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
}

#[tokio::test]
async fn test_commit_and_rollback_and_first_commit_failed_and_second_rollback_failed() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = SyncDataConn::new(1, logger.clone(), Fail::Commit);
        let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let conn = AsyncDataConn::new(2, logger.clone(), Fail::Rollback);
        let boxed = Box::new(DataConnContainer::new("foo".to_string(), Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let mut reports = manager.new_failure_reports();

        if let Err(e) = manager.commit_async(&mut reports).await {
            match e.reason::<DataConnError>() {
                Ok(DataConnError::FailToCommitDataConn { errors }) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].index, 0);
                    assert_eq!(errors[0].name, "bar".into());
                    assert_eq!(errors[0].err.reason::<String>().unwrap(), "ZZZ");
                }
                _ => panic!(),
            }
        } else {
            panic!();
        }

        manager.rollback_async(reports).await;
    }

    #[cfg(unix)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::pre_commit_async 1",
            "AsyncDataConn::pre_commit_async 2",
            "SyncDataConn::commit_async 1 failed",
            "SyncDataConn::rollback_async 1",
            "AsyncDataConn::rollback_async 2 failed",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/tokio/data_conn/_test_data.rs, line = 68 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src/tokio/data_conn/_test_data.rs, line = 333 }) }]",
            "AsyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/tokio/data_conn/_test_data.rs, line = 68 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src/tokio/data_conn/_test_data.rs, line = 333 }) }]",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
    #[cfg(windows)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::pre_commit_async 1",
            "AsyncDataConn::pre_commit_async 2",
            "SyncDataConn::commit_async 1 failed",
            "SyncDataConn::rollback_async 1",
            "AsyncDataConn::rollback_async 2 failed",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src\\tokio\\data_conn\\_test_data.rs, line = 68 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src\\tokio\\data_conn\\_test_data.rs, line = 333 }) }]",
            "AsyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src\\tokio\\data_conn\\_test_data.rs, line = 68 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src\\tokio\\data_conn\\_test_data.rs, line = 333 }) }]",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
}

#[tokio::test]
async fn test_commit_and_rollback_and_secod_commit_failed_and_first_rollback_failed() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = SyncDataConn::new(1, logger.clone(), Fail::Rollback);
        let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let conn = AsyncDataConn::new(2, logger.clone(), Fail::Commit);
        let boxed = Box::new(DataConnContainer::new("bar".to_string(), Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let mut reports = manager.new_failure_reports();

        if let Err(e) = manager.commit_async(&mut reports).await {
            match e.reason::<DataConnError>() {
                Ok(DataConnError::FailToCommitDataConn { errors }) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].index, 1);
                    assert_eq!(errors[0].name, "bar".into());
                    assert_eq!(errors[0].err.reason::<String>().unwrap(), "YYY");
                }
                _ => panic!(),
            }
        } else {
            panic!();
        }

        manager.rollback_async(reports).await;
    }

    #[cfg(unix)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::pre_commit_async 1",
            "AsyncDataConn::pre_commit_async 2",
            "SyncDataConn::commit_async 1",
            "AsyncDataConn::commit_async 2 failed",
            "AsyncDataConn::rollback_async 2",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src/tokio/data_conn/_test_data.rs, line = 241 }), rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src/tokio/data_conn/_test_data.rs, line = 241 }), rollback: NoneByRolledBack }]",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
    #[cfg(windows)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::pre_commit_async 1",
            "AsyncDataConn::pre_commit_async 2",
            "SyncDataConn::commit_async 1",
            "AsyncDataConn::commit_async 2 failed",
            "AsyncDataConn::rollback_async 2",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src\\tokio\\data_conn\\_test_data.rs, line = 241 }), rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src\\tokio\\data_conn\\_test_data.rs, line = 241 }), rollback: NoneByRolledBack }]",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
}

#[tokio::test]
async fn test_commit_and_rollback_and_pre_commit_become_committed_and_ok() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = SyncDataConn::new(1, logger.clone(), Fail::PreCommitBecomeCommitted);
        let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let conn = AsyncDataConn::new(2, logger.clone(), Fail::PreCommitBecomeCommitted);
        let boxed = Box::new(DataConnContainer::new("bar".to_string(), Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let mut reports = manager.new_failure_reports();
        assert!(manager.commit_async(&mut reports).await.is_ok());
        manager.rollback_async(reports).await;
    }

    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::pre_commit_async 1",
            "AsyncDataConn::pre_commit_async 2",
            "SyncDataConn::post_commit_async 1",
            "AsyncDataConn::post_commit_async 2",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
            "AsyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
}

#[tokio::test]
async fn test_commit_and_rollback_and_pre_commit_become_committed_but_failed() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = SyncDataConn::new(1, logger.clone(), Fail::PreCommitBecomeCommitted);
        let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let conn = AsyncDataConn::new(2, logger.clone(), Fail::Commit);
        let boxed = Box::new(DataConnContainer::new("bar".to_string(), Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let mut reports = manager.new_failure_reports();

        if let Err(e) = manager.commit_async(&mut reports).await {
            match e.reason::<DataConnError>() {
                Ok(DataConnError::FailToCommitDataConn { errors }) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].index, 1);
                    assert_eq!(errors[0].name, "bar".into());
                    assert_eq!(errors[0].err.reason::<String>().unwrap(), "YYY");
                }
                _ => panic!(),
            }
        } else {
            panic!();
        }

        manager.rollback_async(reports).await;
    }

    #[cfg(unix)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::pre_commit_async 1",
            "AsyncDataConn::pre_commit_async 2",
            "AsyncDataConn::commit_async 2 failed",
            "AsyncDataConn::rollback_async 2",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src/tokio/data_conn/_test_data.rs, line = 241 }), rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src/tokio/data_conn/_test_data.rs, line = 241 }), rollback: NoneByRolledBack }]",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
    #[cfg(windows)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::pre_commit_async 1",
            "AsyncDataConn::pre_commit_async 2",
            "AsyncDataConn::commit_async 2 failed",
            "AsyncDataConn::rollback_async 2",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src\\tokio\\data_conn\\_test_data.rs, line = 241 }), rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src\\tokio\\data_conn\\_test_data.rs, line = 241 }), rollback: NoneByRolledBack }]",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
}

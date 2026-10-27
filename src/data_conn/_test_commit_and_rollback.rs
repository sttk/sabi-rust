use super::_test_data::*;
use super::*;
use std::sync::{Arc, Mutex};

#[test]
fn test_commit_and_rollback_ok() {
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
        assert!(manager.commit(&mut reports).is_ok());
        manager.rollback(reports);
    }

    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::pre_commit 1",
            "AsyncDataConn::pre_commit 2",
            "SyncDataConn::commit 1",
            "AsyncDataConn::commit 2",
            "SyncDataConn::post_commit 1",
            "AsyncDataConn::post_commit 2",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
}

#[test]
fn test_commit_and_rollback_but_fail_first_sync_pre_commit() {
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

        if let Err(e) = manager.commit(&mut reports) {
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

        manager.rollback(reports);
    }

    #[cfg(unix)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::pre_commit 1 failed",
            "SyncDataConn::rollback 1",
            "AsyncDataConn::rollback 2",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src/data_conn/_test_data.rs, line = 91 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src/data_conn/_test_data.rs, line = 91 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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
            "SyncDataConn::pre_commit 1 failed",
            "SyncDataConn::rollback 1",
            "AsyncDataConn::rollback 2",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src\\data_conn\\_test_data.rs, line = 91 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src\\data_conn\\_test_data.rs, line = 91 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
}

#[test]
fn test_commit_and_rollback_but_fail_first_async_pre_commit() {
}

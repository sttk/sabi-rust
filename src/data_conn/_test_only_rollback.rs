use super::_test_data::*;
use super::*;
use std::ptr;
use std::sync::{Arc, Mutex};

#[test]
fn test_only_rollback_and_first_is_sync() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
        let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let conn = AsyncDataConn::new(2, logger.clone(), Fail::None);
        let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let reports = manager.new_failure_reports();
        manager.rollback(reports);
    }

    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::rollback 1",
            "AsyncDataConn::rollback 2",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
}

#[test]
fn test_only_rollback_and_first_is_async() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = AsyncDataConn::new(1, logger.clone(), Fail::None);
        let boxed = Box::new(DataConnContainer::new("foo".to_string(), Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let conn = SyncDataConn::new(2, logger.clone(), Fail::None);
        let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let reports = manager.new_failure_reports();
        manager.rollback(reports);
    }

    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "AsyncDataConn::new 1",
            "SyncDataConn::new 2",
            "SyncDataConn::rollback 2",
            "AsyncDataConn::rollback 1",
            "SyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::close 2",
            "SyncDataConn::drop 2",
            "AsyncDataConn::close 1",
            "AsyncDataConn::drop 1",
        ]
    );
}

#[test]
fn test_only_rollback_and_second_rollback_failed() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
        let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let conn = AsyncDataConn::new(2, logger.clone(), Fail::Rollback);
        let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let reports = manager.new_failure_reports();
        manager.rollback(reports);
    }

    #[cfg(unix)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::rollback 1",
            "AsyncDataConn::rollback 2 failed",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src/data_conn/_test_data.rs, line = 318 }) }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src/data_conn/_test_data.rs, line = 318 }) }]",
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
            "SyncDataConn::rollback 1",
            "AsyncDataConn::rollback 2 failed",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src\\data_conn\\_test_data.rs, line = 318 }) }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src\\data_conn\\_test_data.rs, line = 318 }) }]",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
}

#[test]
fn test_only_rollback_and_first_rollback_failed() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = SyncDataConn::new(1, logger.clone(), Fail::Rollback);
        let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let conn = AsyncDataConn::new(2, logger.clone(), Fail::None);
        let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let reports = manager.new_failure_reports();
        manager.rollback(reports);
    }

    #[cfg(unix)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::rollback 1 failed",
            "AsyncDataConn::rollback 2",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src/data_conn/_test_data.rs, line = 139 }) }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src/data_conn/_test_data.rs, line = 139 }) }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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
            "SyncDataConn::rollback 1 failed",
            "AsyncDataConn::rollback 2",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src\\data_conn\\_test_data.rs, line = 139 }) }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src\\data_conn\\_test_data.rs, line = 139 }) }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
}

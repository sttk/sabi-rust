use super::_test_data::*;
use super::*;
use std::ptr;
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn test_only_rollback_and_first_is_sync() {
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
        manager.rollback_async(reports).await;
    }

    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::rollback_async 1",
            "AsyncDataConn::rollback_async 2",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
}

#[tokio::test]
async fn test_only_rollback_and_first_is_async() {
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
        manager.rollback_async(reports).await;
    }

    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "AsyncDataConn::new 1",
            "SyncDataConn::new 2",
            "SyncDataConn::rollback_async 2",
            "AsyncDataConn::rollback_async 1",
            "SyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::close 2",
            "SyncDataConn::drop 2",
            "AsyncDataConn::close 1",
            "AsyncDataConn::drop 1",
        ]
    );
}

#[tokio::test]
async fn test_only_rollback_and_second_rollback_failed() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = AsyncDataConn::new(1, logger.clone(), Fail::None);
        let boxed = Box::new(DataConnContainer::new("foo".to_string(), Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let conn = SyncDataConn::new(2, logger.clone(), Fail::Rollback);
        let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let reports = manager.new_failure_reports();
        manager.rollback_async(reports).await;
    }

    #[cfg(unix)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "AsyncDataConn::new 1",
            "SyncDataConn::new 2",
            "SyncDataConn::rollback_async 2 failed",
            "AsyncDataConn::rollback_async 1",
            "SyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/data_conn/_test_data.rs, line = 149 }) }]",
            "AsyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/data_conn/_test_data.rs, line = 149 }) }]",
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
            "SyncDataConn::rollback_async 2 failed",
            "AsyncDataConn::rollback_async 1",
            "SyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\data_conn\\_test_data.rs, line = 149 }) }]",
            "AsyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\data_conn\\_test_data.rs, line = 149 }) }]",
            "SyncDataConn::close 2",
            "SyncDataConn::drop 2",
            "AsyncDataConn::close 1",
            "AsyncDataConn::drop 1",
        ]
    );
}

#[tokio::test]
async fn test_only_rollback_and_first_rollback_failed() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = AsyncDataConn::new(1, logger.clone(), Fail::Rollback);
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
        manager.rollback_async(reports).await;
    }

    #[cfg(unix)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "AsyncDataConn::new 1",
            "SyncDataConn::new 2",
            "SyncDataConn::rollback_async 2",
            "AsyncDataConn::rollback_async 1 failed",
            "SyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src/tokio/data_conn/_test_data.rs, line = 333 }) }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src/tokio/data_conn/_test_data.rs, line = 333 }) }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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
            "SyncDataConn::rollback_async 2",
            "AsyncDataConn::rollback_async 1 failed",
            "SyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src\\tokio\\data_conn\\_test_data.rs, line = 333 }) }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src\\tokio\\data_conn\\_test_data.rs, line = 333 }) }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::close 2",
            "SyncDataConn::drop 2",
            "AsyncDataConn::close 1",
            "AsyncDataConn::drop 1",
        ]
    );
}

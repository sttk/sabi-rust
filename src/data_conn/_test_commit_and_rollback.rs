use super::_test_data::*;
use super::*;
use std::panic::panic_any;
use std::ptr;
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
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src/data_conn/_test_data.rs, line = 90 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src/data_conn/_test_data.rs, line = 90 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src\\data_conn\\_test_data.rs, line = 90 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src\\data_conn\\_test_data.rs, line = 90 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
}

#[test]
fn test_commit_and_rollback_but_fail_first_async_pre_commit() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = AsyncDataConn::new(2, logger.clone(), Fail::PreCommit);
        let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let conn = SyncDataConn::new(1, logger.clone(), Fail::PreCommit);
        let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let mut reports = manager.new_failure_reports();

        if let Err(e) = manager.commit(&mut reports) {
            match e.reason::<DataConnError>() {
                Ok(DataConnError::FailToPreCommitDataConn { errors }) => {
                    assert_eq!(errors.len(), 2);
                    assert_eq!(errors[0].index, 1);
                    assert_eq!(errors[0].name, "foo".into());
                    assert_eq!(errors[0].err.reason::<String>().unwrap(), "zzz");
                    assert_eq!(errors[1].index, 0);
                    assert_eq!(errors[1].name, "bar".into());
                    assert_eq!(errors[1].err.reason::<String>().unwrap(), "yyy");
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
           "AsyncDataConn::new 2",
           "SyncDataConn::new 1",
           "SyncDataConn::pre_commit 1 failed",
           "AsyncDataConn::pre_commit 2 failed",
           "SyncDataConn::rollback 1",
           "AsyncDataConn::rollback 2",
           "SyncDataConn::on_txn_failure 1",
           "TxnFailureReports=[TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"yyy\", file = src/data_conn/_test_data.rs, line = 250 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src/data_conn/_test_data.rs, line = 90 }), rollback: NoneByRolledBack }]",
           "AsyncDataConn::on_txn_failure 2",
           "TxnFailureReports=[TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"yyy\", file = src/data_conn/_test_data.rs, line = 250 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src/data_conn/_test_data.rs, line = 90 }), rollback: NoneByRolledBack }]",
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
           "SyncDataConn::pre_commit 1 failed",
           "AsyncDataConn::pre_commit 2 failed",
           "SyncDataConn::rollback 1",
           "AsyncDataConn::rollback 2",
           "SyncDataConn::on_txn_failure 1",
           "TxnFailureReports=[TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"yyy\", file = src\\data_conn\\_test_data.rs, line = 250 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src\\data_conn\\_test_data.rs, line = 90 }), rollback: NoneByRolledBack }]",
           "AsyncDataConn::on_txn_failure 2",
           "TxnFailureReports=[TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"yyy\", file = src\\data_conn\\_test_data.rs, line = 250 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src\\data_conn\\_test_data.rs, line = 90 }), rollback: NoneByRolledBack }]",
           "SyncDataConn::close 1",
           "SyncDataConn::drop 1",
           "AsyncDataConn::close 2",
           "AsyncDataConn::drop 2",
       ]
   );
}

#[test]
fn test_commit_and_rollback_but_fail_second_pre_commit() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
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

        if let Err(e) = manager.commit(&mut reports) {
            match e.reason::<DataConnError>() {
                Ok(DataConnError::FailToPreCommitDataConn { errors }) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].index, 1);
                    assert_eq!(errors[0].name, "bar".into());
                    assert_eq!(errors[0].err.reason::<String>().unwrap(), "yyy");
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
            "SyncDataConn::pre_commit 1",
            "AsyncDataConn::pre_commit 2 failed",
            "SyncDataConn::rollback 1",
            "AsyncDataConn::rollback 2",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"yyy\", file = src/data_conn/_test_data.rs, line = 250 }), rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"yyy\", file = src/data_conn/_test_data.rs, line = 250 }), rollback: NoneByRolledBack }]",
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
            "SyncDataConn::pre_commit 1",
            "AsyncDataConn::pre_commit 2 failed",
            "SyncDataConn::rollback 1",
            "AsyncDataConn::rollback 2",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"yyy\", file = src\\data_conn\\_test_data.rs, line = 250 }), rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"yyy\", file = src\\data_conn\\_test_data.rs, line = 250 }), rollback: NoneByRolledBack }]",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
}

#[test]
fn test_commit_and_rollback_but_fail_first_sync_commit() {
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

        if let Err(e) = manager.commit(&mut reports) {
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

        manager.rollback(reports);
    }

    #[cfg(unix)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::pre_commit 1",
            "AsyncDataConn::pre_commit 2",
            "SyncDataConn::commit 1 failed",
            "SyncDataConn::rollback 1",
            "AsyncDataConn::rollback 2",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/data_conn/_test_data.rs, line = 68 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/data_conn/_test_data.rs, line = 68 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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
            "SyncDataConn::pre_commit 1",
            "AsyncDataConn::pre_commit 2",
            "SyncDataConn::commit 1 failed",
            "SyncDataConn::rollback 1",
            "AsyncDataConn::rollback 2",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src\\data_conn\\_test_data.rs, line = 68 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src\\data_conn\\_test_data.rs, line = 68 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
}

#[test]
fn test_commit_and_rollback_but_fail_first_async_commit() {
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

        if let Err(e) = manager.commit(&mut reports) {
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

        manager.rollback(reports);
    }

    #[cfg(unix)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "AsyncDataConn::new 1",
            "SyncDataConn::new 2",
            "SyncDataConn::pre_commit 2",
            "AsyncDataConn::pre_commit 1",
            "SyncDataConn::commit 2",
            "AsyncDataConn::commit 1 failed",
            "AsyncDataConn::rollback 1",
            "SyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src/data_conn/_test_data.rs, line = 226 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
            "AsyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src/data_conn/_test_data.rs, line = 226 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
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
            "SyncDataConn::pre_commit 2",
            "AsyncDataConn::pre_commit 1",
            "SyncDataConn::commit 2",
            "AsyncDataConn::commit 1 failed",
            "AsyncDataConn::rollback 1",
            "SyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src\\data_conn\\_test_data.rs, line = 226 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
            "AsyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src\\data_conn\\_test_data.rs, line = 226 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
            "SyncDataConn::close 2",
            "SyncDataConn::drop 2",
            "AsyncDataConn::close 1",
            "AsyncDataConn::drop 1",
        ]
    );
}

#[test]
fn test_commit_and_rollback_but_fail_second_commit() {
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

        if let Err(e) = manager.commit(&mut reports) {
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

        manager.rollback(reports);
    }

    #[cfg(unix)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::pre_commit 1",
            "AsyncDataConn::pre_commit 2",
            "SyncDataConn::commit 1",
            "AsyncDataConn::commit 2 failed",
            "AsyncDataConn::rollback 2",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src/data_conn/_test_data.rs, line = 226 }), rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src/data_conn/_test_data.rs, line = 226 }), rollback: NoneByRolledBack }]",
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
            "SyncDataConn::pre_commit 1",
            "AsyncDataConn::pre_commit 2",
            "SyncDataConn::commit 1",
            "AsyncDataConn::commit 2 failed",
            "AsyncDataConn::rollback 2",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src\\data_conn\\_test_data.rs, line = 226 }), rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src\\data_conn\\_test_data.rs, line = 226 }), rollback: NoneByRolledBack }]",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
}

#[test]
fn test_commit_and_rollback_but_fail_first_sync_post_commit() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = SyncDataConn::new(1, logger.clone(), Fail::PostCommit);
        let boxed = Box::new(DataConnContainer::new("foo".to_string(), Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let conn = AsyncDataConn::new(2, logger.clone(), Fail::PostCommit);
        let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let mut reports = manager.new_failure_reports();

        if let Err(e) = manager.commit(&mut reports) {
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

        manager.rollback(reports);
    }

    #[cfg(unix)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::pre_commit 1",
            "AsyncDataConn::pre_commit 2",
            "SyncDataConn::commit 1",
            "AsyncDataConn::commit 2",
            "SyncDataConn::post_commit 1 failed",
            "AsyncDataConn::post_commit 2 failed",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/data_conn/_test_data.rs, line = 114 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/data_conn/_test_data.rs, line = 286 }), rollback: NoneByNotRolledBack }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/data_conn/_test_data.rs, line = 114 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/data_conn/_test_data.rs, line = 286 }), rollback: NoneByNotRolledBack }]",
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
            "SyncDataConn::pre_commit 1",
            "AsyncDataConn::pre_commit 2",
            "SyncDataConn::commit 1",
            "AsyncDataConn::commit 2",
            "SyncDataConn::post_commit 1 failed",
            "AsyncDataConn::post_commit 2 failed",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\data_conn\\_test_data.rs, line = 114 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\data_conn\\_test_data.rs, line = 286 }), rollback: NoneByNotRolledBack }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\data_conn\\_test_data.rs, line = 114 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\data_conn\\_test_data.rs, line = 286 }), rollback: NoneByNotRolledBack }]",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
}

#[test]
fn test_commit_and_rollback_but_fail_first_async_post_commit() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = AsyncDataConn::new(2, logger.clone(), Fail::PostCommit);
        let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let conn = SyncDataConn::new(1, logger.clone(), Fail::PostCommit);
        let boxed = Box::new(DataConnContainer::new("foo".to_string(), Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let mut reports = manager.new_failure_reports();

        if let Err(e) = manager.commit(&mut reports) {
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

        manager.rollback(reports);
    }

    #[cfg(unix)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "AsyncDataConn::new 2",
            "SyncDataConn::new 1",
            "SyncDataConn::pre_commit 1",
            "AsyncDataConn::pre_commit 2",
            "SyncDataConn::commit 1",
            "AsyncDataConn::commit 2",
            "SyncDataConn::post_commit 1 failed",
            "AsyncDataConn::post_commit 2 failed",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/data_conn/_test_data.rs, line = 286 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/data_conn/_test_data.rs, line = 114 }), rollback: NoneByNotRolledBack }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/data_conn/_test_data.rs, line = 286 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/data_conn/_test_data.rs, line = 114 }), rollback: NoneByNotRolledBack }]",
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
            "SyncDataConn::pre_commit 1",
            "AsyncDataConn::pre_commit 2",
            "SyncDataConn::commit 1",
            "AsyncDataConn::commit 2",
            "SyncDataConn::post_commit 1 failed",
            "AsyncDataConn::post_commit 2 failed",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\data_conn\\_test_data.rs, line = 286 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\data_conn\\_test_data.rs, line = 114 }), rollback: NoneByNotRolledBack }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\data_conn\\_test_data.rs, line = 286 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\data_conn\\_test_data.rs, line = 114 }), rollback: NoneByNotRolledBack }]",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
        ]
    );
}

#[test]
fn test_commit_and_rollback_but_fail_second_post_commit() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
        let boxed = Box::new(DataConnContainer::new("foo".to_string(), Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let conn = AsyncDataConn::new(2, logger.clone(), Fail::PostCommit);
        let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let mut reports = manager.new_failure_reports();

        if let Err(e) = manager.commit(&mut reports) {
            match e.reason::<DataConnError>() {
                Ok(DataConnError::FailToPostCommitDataConn { errors }) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].index, 1);
                    assert_eq!(errors[0].name, "bar".into());
                    assert_eq!(errors[0].err.reason::<String>().unwrap(), "!!!");
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
            "SyncDataConn::pre_commit 1",
            "AsyncDataConn::pre_commit 2",
            "SyncDataConn::commit 1",
            "AsyncDataConn::commit 2",
            "SyncDataConn::post_commit 1",
            "AsyncDataConn::post_commit 2 failed",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/data_conn/_test_data.rs, line = 286 }), rollback: NoneByNotRolledBack }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/data_conn/_test_data.rs, line = 286 }), rollback: NoneByNotRolledBack }]",
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
            "SyncDataConn::pre_commit 1",
            "AsyncDataConn::pre_commit 2",
            "SyncDataConn::commit 1",
            "AsyncDataConn::commit 2",
            "SyncDataConn::post_commit 1",
            "AsyncDataConn::post_commit 2 failed",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\data_conn\\_test_data.rs, line = 286 }), rollback: NoneByNotRolledBack }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\data_conn\\_test_data.rs, line = 286 }), rollback: NoneByNotRolledBack }]",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
}

#[test]
fn test_commit_and_rollback_but_fail_second_post_commit_and_contains_no_commit() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

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

        if let Err(e) = manager.commit(&mut reports) {
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

        manager.rollback(reports);
    }

    #[cfg(unix)]
    assert_eq!(
        &logger.lock().unwrap()[0..24],
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "NoCommitDataConn::new 3",
            "SyncDataConn::pre_commit 1",
            "NoCommitDataConn::pre_commit 3",
            "AsyncDataConn::pre_commit 2",
            "SyncDataConn::commit 1",
            "NoCommitDataConn::commit 3",
            "AsyncDataConn::commit 2",
            "SyncDataConn::post_commit 1",
            "NoCommitDataConn::post_commit 3",
            "AsyncDataConn::post_commit 2 failed",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/data_conn/_test_data.rs, line = 286 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::data_conn::_test_data::NoCommitDataConn\", cause: NoneByUncommitted, rollback: NoneByNotRolledBack }]",
            "NoCommitDataConn::on_txn_failure 3",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/data_conn/_test_data.rs, line = 286 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::data_conn::_test_data::NoCommitDataConn\", cause: NoneByUncommitted, rollback: NoneByNotRolledBack }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/data_conn/_test_data.rs, line = 286 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::data_conn::_test_data::NoCommitDataConn\", cause: NoneByUncommitted, rollback: NoneByNotRolledBack }]",
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
            "SyncDataConn::pre_commit 1",
            "NoCommitDataConn::pre_commit 3",
            "AsyncDataConn::pre_commit 2",
            "SyncDataConn::commit 1",
            "NoCommitDataConn::commit 3",
            "AsyncDataConn::commit 2",
            "SyncDataConn::post_commit 1",
            "NoCommitDataConn::post_commit 3",
            "AsyncDataConn::post_commit 2 failed",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\data_conn\\_test_data.rs, line = 286 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::data_conn::_test_data::NoCommitDataConn\", cause: NoneByUncommitted, rollback: NoneByNotRolledBack }]",
            "NoCommitDataConn::on_txn_failure 3",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\data_conn\\_test_data.rs, line = 286 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::data_conn::_test_data::NoCommitDataConn\", cause: NoneByUncommitted, rollback: NoneByNotRolledBack }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\data_conn\\_test_data.rs, line = 286 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::data_conn::_test_data::NoCommitDataConn\", cause: NoneByUncommitted, rollback: NoneByNotRolledBack }]",
            "NoCommitDataConn::close 3",
            "NoCommitDataConn::drop 3",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
}

#[test]
fn test_commit_and_rollback_and_first_commit_failed_and_second_rollback_failed() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = SyncDataConn::new(1, logger.clone(), Fail::Commit);
        let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let conn = AsyncDataConn::new(2, logger.clone(), Fail::Rollback);
        let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let mut reports = manager.new_failure_reports();
        if let Err(e) = manager.commit(&mut reports) {
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
        manager.rollback(reports);
    }

    #[cfg(unix)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::pre_commit 1",
            "AsyncDataConn::pre_commit 2",
            "SyncDataConn::commit 1 failed",
            "SyncDataConn::rollback 1",
            "AsyncDataConn::rollback 2 failed",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/data_conn/_test_data.rs, line = 68 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src/data_conn/_test_data.rs, line = 318 }) }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/data_conn/_test_data.rs, line = 68 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src/data_conn/_test_data.rs, line = 318 }) }]",
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
            "SyncDataConn::pre_commit 1",
            "AsyncDataConn::pre_commit 2",
            "SyncDataConn::commit 1 failed",
            "SyncDataConn::rollback 1",
            "AsyncDataConn::rollback 2 failed",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src\\data_conn\\_test_data.rs, line = 68 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src\\data_conn\\_test_data.rs, line = 318 }) }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src\\data_conn\\_test_data.rs, line = 68 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src\\data_conn\\_test_data.rs, line = 318 }) }]",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
}

#[test]
fn test_commit_and_rollback_and_secod_commit_failed_and_first_rollback_failed() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = SyncDataConn::new(1, logger.clone(), Fail::Rollback);
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
        if let Err(e) = manager.commit(&mut reports) {
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
        manager.rollback(reports);
    }

    #[cfg(unix)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::pre_commit 1",
            "AsyncDataConn::pre_commit 2",
            "SyncDataConn::commit 1",
            "AsyncDataConn::commit 2 failed",
            "AsyncDataConn::rollback 2",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src/data_conn/_test_data.rs, line = 226 }), rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src/data_conn/_test_data.rs, line = 226 }), rollback: NoneByRolledBack }]",
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
            "SyncDataConn::pre_commit 1",
            "AsyncDataConn::pre_commit 2",
            "SyncDataConn::commit 1",
            "AsyncDataConn::commit 2 failed",
            "AsyncDataConn::rollback 2",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src\\data_conn\\_test_data.rs, line = 226 }), rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src\\data_conn\\_test_data.rs, line = 226 }), rollback: NoneByRolledBack }]",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
}

#[test]
fn test_commit_and_rollback_and_pre_commit_become_committed_and_ok() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = SyncDataConn::new(1, logger.clone(), Fail::PreCommitBecomeCommitted);
        let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
        let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
        let ssnnptr = SendSyncNonNull::new(nnptr);
        manager.add(ssnnptr);

        let conn = AsyncDataConn::new(2, logger.clone(), Fail::PreCommitBecomeCommitted);
        let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
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
fn test_commit_and_rollback_and_pre_commit_become_committed_but_failed() {
    let logger = Arc::new(Mutex::new(Vec::new()));

    {
        let mut manager = DataConnManager::new();

        let conn = SyncDataConn::new(1, logger.clone(), Fail::PreCommitBecomeCommitted);
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
        if let Err(e) = manager.commit(&mut reports) {
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
        manager.rollback(reports);
    }

    #[cfg(unix)]
    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::pre_commit 1",
            "AsyncDataConn::pre_commit 2",
            "AsyncDataConn::commit 2 failed",
            "AsyncDataConn::rollback 2",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src/data_conn/_test_data.rs, line = 226 }), rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src/data_conn/_test_data.rs, line = 226 }), rollback: NoneByRolledBack }]",
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
            "SyncDataConn::pre_commit 1",
            "AsyncDataConn::pre_commit 2",
            "AsyncDataConn::commit 2 failed",
            "AsyncDataConn::rollback 2",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src\\data_conn\\_test_data.rs, line = 226 }), rollback: NoneByRolledBack }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"YYY\", file = src\\data_conn\\_test_data.rs, line = 226 }), rollback: NoneByRolledBack }]",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
        ]
    );
}

#[test]
fn test_commit_with_order_and_rollback_ok() {
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
        assert!(manager.commit(&mut reports).is_ok());
        manager.rollback(reports);
    }

    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataConn::new 1",
            "AsyncDataConn::new 2",
            "SyncDataConn::new 3",
            "SyncDataConn::pre_commit 1",
            "SyncDataConn::pre_commit 3",
            "AsyncDataConn::pre_commit 2", // because of async
            "SyncDataConn::commit 1",
            "SyncDataConn::commit 3",
            "AsyncDataConn::commit 2", // because of async
            "SyncDataConn::post_commit 1",
            "SyncDataConn::post_commit 3",
            "AsyncDataConn::post_commit 2", // because of async
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"qux\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
            "SyncDataConn::on_txn_failure 3",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"qux\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
            "AsyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"qux\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
            "SyncDataConn::close 3",
            "SyncDataConn::drop 3",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
            "AsyncDataConn::close 2",
            "AsyncDataConn::drop 2",
        ]
    );
}

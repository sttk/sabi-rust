use crate::tokio::data_conn::_test_data::*;
use crate::tokio::data_src::_test_data::*;
use crate::tokio::*;

use futures::FutureExt;
use std::panic;
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn test_run_logic_panicked() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

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
    match panic::AssertUnwindSafe(async {
        runner
            .run_async(move |data| {
                let logger = logger_clone.clone();
                Box::pin(async move {
                    let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                    let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                    logger
                        .lock()
                        .unwrap()
                        .push("Run the logic but panicked".to_string());
                    panic!("run logic panic");
                })
            })
            .await
    })
    .catch_unwind()
    .await
    {
        Err(payload) => {
            assert_eq!(*payload.downcast::<&str>().unwrap(), "run logic panic");
        }
        Ok(result) => panic!("{result:?}"),
    }

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.local, true);
    assert_eq!(runner.hub.run.data_src_map.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert_eq!(runner.hub.run.fixed, true);

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
            "Run the logic but panicked",
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
async fn test_run_close_panicked() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    struct Abc;
    let mut runner = LogicRunner::new(DataHub::<Abc>::new());

    runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
    runner.uses(
        "bar",
        SyncDataSrc::new(2, logger.clone(), Fail::ClosePanicked),
    );

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 2);
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.local, true);
    assert_eq!(runner.hub.run.data_src_map.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert_eq!(runner.hub.run.fixed, false);

    let logger_clone = logger.clone();
    match panic::AssertUnwindSafe(async {
        runner
            .run_async(move |data| {
                let logger = logger_clone.clone();
                Box::pin(async move {
                    let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                    let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                    logger.lock().unwrap().push("Run the logic".to_string());
                    Ok(())
                })
            })
            .await
    })
    .catch_unwind()
    .await
    {
        Err(payload) => {
            assert_eq!(*payload.downcast::<&str>().unwrap(), "close panic");
        }
        Ok(result) => panic!("{result:?}"),
    }

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.local, true);
    assert_eq!(runner.hub.run.data_src_map.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert_eq!(runner.hub.run.fixed, true);

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
            "SyncDataConn::close 2 panicked",
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
async fn test_txn_logic_panicked() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

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
    match panic::AssertUnwindSafe(async {
        runner
            .txn_async(move |data| {
                let logger = logger_clone.clone();
                Box::pin(async move {
                    let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                    let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                    logger
                        .lock()
                        .unwrap()
                        .push("Run the logic but panicked".to_string());
                    panic!("run logic panic");
                })
            })
            .await
    })
    .catch_unwind()
    .await
    {
        Err(payload) => {
            assert_eq!(*payload.downcast::<&str>().unwrap(), "run logic panic");
        }
        Ok(result) => panic!("{result:?}"),
    }

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.local, true);
    assert_eq!(runner.hub.run.data_src_map.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert_eq!(runner.hub.run.fixed, true);

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
            "Run the logic but panicked",
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
async fn test_pre_commit_panicked() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    struct Abc;
    let mut runner = LogicRunner::new(DataHub::<Abc>::new());

    runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
    runner.uses(
        "bar",
        SyncDataSrc::new(2, logger.clone(), Fail::PreCommitPanicked),
    );

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 2);
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.local, true);
    assert_eq!(runner.hub.run.data_src_map.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert_eq!(runner.hub.run.fixed, false);

    let logger_clone = logger.clone();
    match panic::AssertUnwindSafe(async {
        runner
            .txn_async(move |data| {
                let logger = logger_clone.clone();
                Box::pin(async move {
                    let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                    let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                    logger.lock().unwrap().push("Run the logic".to_string());
                    Ok(())
                })
            })
            .await
    })
    .catch_unwind()
    .await
    {
        Err(payload) => {
            assert_eq!(
                *payload.downcast::<&str>().unwrap(),
                "pre_commit_async panic"
            );
        }
        Ok(result) => panic!("{result:?}"),
    }

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.local, true);
    assert_eq!(runner.hub.run.data_src_map.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert_eq!(runner.hub.run.fixed, true);

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
            "SyncDataConn::pre_commit_async 2 panicked",
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
async fn test_commit_panicked() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    struct Abc;
    let mut runner = LogicRunner::new(DataHub::<Abc>::new());

    runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
    runner.uses(
        "bar",
        SyncDataSrc::new(2, logger.clone(), Fail::CommitPanicked),
    );

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 2);
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.local, true);
    assert_eq!(runner.hub.run.data_src_map.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert_eq!(runner.hub.run.fixed, false);

    let logger_clone = logger.clone();
    match panic::AssertUnwindSafe(async {
        runner
            .txn_async(move |data| {
                let logger = logger_clone.clone();
                Box::pin(async move {
                    let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                    let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                    logger.lock().unwrap().push("Run the logic".to_string());
                    Ok(())
                })
            })
            .await
    })
    .catch_unwind()
    .await
    {
        Err(payload) => {
            assert_eq!(*payload.downcast::<&str>().unwrap(), "commit_async panic");
        }
        Ok(result) => panic!("{result:?}"),
    }

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.local, true);
    assert_eq!(runner.hub.run.data_src_map.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert_eq!(runner.hub.run.fixed, true);

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
            "SyncDataConn::commit_async 2 panicked",
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
async fn test_post_commit_panicked() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    struct Abc;
    let mut runner = LogicRunner::new(DataHub::<Abc>::new());

    runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
    runner.uses(
        "bar",
        SyncDataSrc::new(2, logger.clone(), Fail::PostCommitPanicked),
    );

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 2);
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.local, true);
    assert_eq!(runner.hub.run.data_src_map.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert_eq!(runner.hub.run.fixed, false);

    let logger_clone = logger.clone();
    match panic::AssertUnwindSafe(async {
        runner
            .txn_async(move |data| {
                let logger = logger_clone.clone();
                Box::pin(async move {
                    let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                    let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                    logger.lock().unwrap().push("Run the logic".to_string());
                    Ok(())
                })
            })
            .await
    })
    .catch_unwind()
    .await
    {
        Err(payload) => {
            assert_eq!(
                *payload.downcast::<&str>().unwrap(),
                "post_commit_async panic"
            );
        }
        Ok(result) => panic!("{result:?}"),
    }

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.local, true);
    assert_eq!(runner.hub.run.data_src_map.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert_eq!(runner.hub.run.fixed, true);

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
            "SyncDataConn::post_commit_async 2 panicked",
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
async fn test_rollback_panicked() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    struct Abc;
    let mut runner = LogicRunner::new(DataHub::<Abc>::new());

    runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
    runner.uses(
        "bar",
        SyncDataSrc::new(2, logger.clone(), Fail::RollbackPanicked),
    );

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 2);
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.local, true);
    assert_eq!(runner.hub.run.data_src_map.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert_eq!(runner.hub.run.fixed, false);

    let logger_clone = logger.clone();
    match panic::AssertUnwindSafe(async {
        runner
            .txn_async(move |data| {
                let logger = logger_clone.clone();
                Box::pin(async move {
                    let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                    let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                    logger
                        .lock()
                        .unwrap()
                        .push("Run the logic but error".to_string());
                    Err(errs::Err::new("logic error"))
                })
            })
            .await
    })
    .catch_unwind()
    .await
    {
        Err(payload) => {
            assert_eq!(*payload.downcast::<&str>().unwrap(), "rollback_async panic");
        }
        Ok(result) => panic!("{result:?}"),
    }

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.local, true);
    assert_eq!(runner.hub.run.data_src_map.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert_eq!(runner.hub.run.fixed, true);

    #[cfg(unix)]
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
            "Run the logic but error",
            "SyncDataConn::rollback_async 1",
            "SyncDataConn::rollback_async 2 panicked",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = &str \"logic error\", file = src/tokio/logic_runner/_test_panic.rs, line = 516 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = &str \"logic error\", file = src/tokio/logic_runner/_test_panic.rs, line = 516 }), rollback: NoneByNotRolledBack }]",
            "SyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = &str \"logic error\", file = src/tokio/logic_runner/_test_panic.rs, line = 516 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = &str \"logic error\", file = src/tokio/logic_runner/_test_panic.rs, line = 516 }), rollback: NoneByNotRolledBack }]",
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
    #[cfg(windows)]
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
            "Run the logic but error",
            "SyncDataConn::rollback_async 1",
            "SyncDataConn::rollback_async 2 panicked",
            "SyncDataConn::on_txn_failure_async 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = &str \"logic error\", file = src/tokio/logic_runner/_test_panic.rs, line = 516 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = &str \"logic error\", file = src/tokio/logic_runner/_test_panic.rs, line = 516 }), rollback: NoneByNotRolledBack }]",
            "SyncDataConn::on_txn_failure_async 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = &str \"logic error\", file = src/tokio/logic_runner/_test_panic.rs, line = 516 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::data_conn::_test_data::SyncDataConn\", cause: LogicFailure(errs::Err { reason = &str \"logic error\", file = src/tokio/logic_runner/_test_panic.rs, line = 516 }), rollback: NoneByNotRolledBack }]",
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
async fn test_txn_close_panicked() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    struct Abc;
    let mut runner = LogicRunner::new(DataHub::<Abc>::new());

    runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
    runner.uses(
        "bar",
        SyncDataSrc::new(2, logger.clone(), Fail::ClosePanicked),
    );

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 2);
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.local, true);
    assert_eq!(runner.hub.run.data_src_map.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert_eq!(runner.hub.run.fixed, false);

    let logger_clone = logger.clone();
    match panic::AssertUnwindSafe(async {
        runner
            .txn_async(move |data| {
                let logger = logger_clone.clone();
                Box::pin(async move {
                    let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                    let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                    logger.lock().unwrap().push("Run the logic".to_string());
                    Ok(())
                })
            })
            .await
    })
    .catch_unwind()
    .await
    {
        Err(payload) => {
            assert_eq!(*payload.downcast::<&str>().unwrap(), "close panic");
        }
        Ok(result) => panic!("{result:?}"),
    }

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.local, true);
    assert_eq!(runner.hub.run.data_src_map.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert_eq!(runner.hub.run.fixed, true);

    #[cfg(unix)]
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
            "SyncDataConn::close 2 panicked",
            "SyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
            "SyncDataSrc::close 2",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::close 1",
            "SyncDataSrc::drop 1",
        ]
    );
    #[cfg(windows)]
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
            "SyncDataConn::close 2 panicked",
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

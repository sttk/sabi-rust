use crate::tokio::data_conn::_test_data::*;
use crate::tokio::data_src::_test_data::*;
use crate::tokio::*;

use std::sync::{Arc, Mutex};

#[tokio::test]
async fn test_logic_txn_and_ok() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));
    {
        struct A;
        let mut runner = LogicRunner::new(DataHub::<A>::new());
        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

        let logger_clone_0 = logger.clone();
        let logger_clone_1 = logger.clone();
        let logger_clone_2 = logger.clone();

        let result = runner
            .begin_txn_async()
            .await
            .run_or_block_async(move |data| {
                let logger = logger_clone_0.clone();
                Box::pin(async move {
                    let _conn = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                    logger.lock().unwrap().push("execute logic-0".to_string());
                    Ok(())
                })
            })
            .await
            .run_async(move |data| {
                let logger = logger_clone_1.clone();
                Box::pin(async move {
                    let _conn = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                    logger.lock().unwrap().push("execute logic-1".to_string());
                    Ok(())
                })
            })
            .await
            .run_force_async(move |data| {
                let logger = logger_clone_2.clone();
                Box::pin(async move {
                    let _conn = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                    let _conn = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                    logger.lock().unwrap().push("execute logic-2".to_string());
                    Ok(())
                })
            })
            .await
            .end_txn_async()
            .await;

        assert!(result.is_ok());
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
            "execute logic-0",
            "SyncDataSrc::create_data_conn_async 2",
            "SyncDataConn::new 2",
            "execute logic-1",
            "execute logic-2",
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
async fn test_logic_run_but_fail_to_run_but_run_forc_runs() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));
    {
        struct A;
        let mut runner = LogicRunner::new(DataHub::<A>::new());

        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

        let logger_clone_0 = logger.clone();
        let logger_clone_1 = logger.clone();
        let logger_clone_2 = logger.clone();
        let logger_clone_3 = logger.clone();

        let result = runner
            .begin_txn_async()
            .await
            .run_async(move |data| {
                let logger = logger_clone_0.clone();
                Box::pin(async move {
                    let _conn = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                    logger.lock().unwrap().push("execute logic-0".to_string());
                    Err(errs::Err::new("logic-0 failed"))
                })
            })
            .await
            .run_async(move |data| {
                let logger = logger_clone_1.clone();
                Box::pin(async move {
                    let _conn = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                    logger.lock().unwrap().push("execute logic-1".to_string());
                    Ok(())
                })
            })
            .await
            .run_or_block_async(move |data| {
                let logger = logger_clone_2.clone();
                Box::pin(async move {
                    let _conn = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                    logger.lock().unwrap().push("execute logic-2".to_string());
                    Ok(())
                })
            })
            .await
            .run_force_async(move |data| {
                let logger = logger_clone_3.clone();
                Box::pin(async move {
                    let _conn = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                    logger.lock().unwrap().push("execute logic-3".to_string());
                    Ok(())
                })
            })
            .await
            .end_txn_async()
            .await;

        if let Err(err) = result {
            match err.reason::<DataHubError>() {
                Ok(DataHubError::FailToRunLogic { errors }) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].index, 0);
                    assert_eq!(errors[0].name, "LogicTxn#run_async(logic-0)".into());
                    assert_eq!(errors[0].err.reason::<&str>().unwrap(), &"logic-0 failed");
                }
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
            "SyncDataSrc::setup_async 1",
            "SyncDataSrc::setup_async 2",
            "SyncDataSrc::create_data_conn_async 1",
            "SyncDataConn::new 1",
            "execute logic-0",
            "execute logic-3",
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
async fn test_runner_and_failed_to_run_force_then_skip_run_but_run_force_runs() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));
    {
        struct A;
        let mut runner = LogicRunner::new(DataHub::<A>::new());
        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

        let logger_clone_0 = logger.clone();
        let logger_clone_1 = logger.clone();
        let logger_clone_2 = logger.clone();
        let logger_clone_3 = logger.clone();

        let result = runner
            .begin_txn_async()
            .await
            .run_force_async(move |data| {
                let logger = logger_clone_0.clone();
                Box::pin(async move {
                    let _conn = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                    logger.lock().unwrap().push("execute logic-0".to_string());
                    Err(errs::Err::new("logic-0 failed"))
                })
            })
            .await
            .run_async(move |data| {
                let logger = logger_clone_1.clone();
                Box::pin(async move {
                    let _conn = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                    logger.lock().unwrap().push("execute logic-1".to_string());
                    Ok(())
                })
            })
            .await
            .run_force_async(move |data| {
                let logger = logger_clone_2.clone();
                Box::pin(async move {
                    let _conn = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                    logger.lock().unwrap().push("execute logic-2".to_string());
                    Ok(())
                })
            })
            .await
            .run_or_block_async(move |data| {
                let logger = logger_clone_3.clone();
                Box::pin(async move {
                    let _conn = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                    logger.lock().unwrap().push("execute logic-3".to_string());
                    Ok(())
                })
            })
            .await
            .end_txn_async()
            .await;

        if let Err(err) = result {
            match err.reason::<DataHubError>() {
                Ok(DataHubError::FailToRunLogic { errors }) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].index, 0);
                    assert_eq!(errors[0].name, "LogicTxn#run_force_async(logic-0)".into());
                    assert_eq!(errors[0].err.reason::<&str>().unwrap(), &"logic-0 failed");
                }
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
            "SyncDataSrc::setup_async 1",
            "SyncDataSrc::setup_async 2",
            "SyncDataSrc::create_data_conn_async 1",
            "SyncDataConn::new 1",
            "execute logic-0",
            "execute logic-2",
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
async fn test_runner_and_failed_to_commit() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));
    {
        struct A;
        let mut runner = LogicRunner::new(DataHub::<A>::new());
        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Commit));
        runner.uses("baz", SyncDataSrc::new(3, logger.clone(), Fail::None));

        let logger_clone_0 = logger.clone();
        let logger_clone_1 = logger.clone();
        let logger_clone_2 = logger.clone();
        let logger_clone_3 = logger.clone();

        let result = runner
            .begin_txn_async()
            .await
            .run_force_async(move |data| {
                let logger = logger_clone_0.clone();
                Box::pin(async move {
                    let _conn = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                    logger.lock().unwrap().push("execute logic-0".to_string());
                    Ok(())
                })
            })
            .await
            .run_async(move |data| {
                let logger = logger_clone_1.clone();
                Box::pin(async move {
                    let _conn = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                    logger.lock().unwrap().push("execute logic-1".to_string());
                    Ok(())
                })
            })
            .await
            .run_force_async(move |data| {
                let logger = logger_clone_2.clone();
                Box::pin(async move {
                    let _conn = data.get_data_conn_async::<SyncDataConn>("baz").await?;
                    logger.lock().unwrap().push("execute logic-2".to_string());
                    Ok(())
                })
            })
            .await
            .run_or_block_async(move |data| {
                let logger = logger_clone_3.clone();
                Box::pin(async move {
                    let _conn = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                    logger.lock().unwrap().push("execute logic-3".to_string());
                    Ok(())
                })
            })
            .await
            .end_txn_async()
            .await;

        if let Err(err) = result {
            match err.reason::<DataConnError>() {
                Ok(DataConnError::FailToCommitDataConn { errors }) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].index, 1);
                    assert_eq!(errors[0].name, "bar".into());
                    assert_eq!(errors[0].err.reason::<String>().unwrap(), &"ZZZ");
                }
                _ => panic!("{err:?}"),
            }
        } else {
            panic!();
        }
    }

    #[cfg(unix)]
    assert_eq!(
        logger.lock().unwrap()[0..41],
        [
            "SyncDataSrc::new 1",
            "SyncDataSrc::new 2",
            "SyncDataSrc::new 3",
            "SyncDataSrc::setup_async 1",
            "SyncDataSrc::setup_async 2",
            "SyncDataSrc::setup_async 3",
            "SyncDataSrc::create_data_conn_async 1",
            "SyncDataConn::new 1",
            "execute logic-0",
            "SyncDataSrc::create_data_conn_async 2",
            "SyncDataConn::new 2",
            "execute logic-1",
            "SyncDataSrc::create_data_conn_async 3",
            "SyncDataConn::new 3",
            "execute logic-2",
            "execute logic-3",
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
        logger.lock().unwrap()[0..41],
        [
            "SyncDataSrc::new 1",
            "SyncDataSrc::new 2",
            "SyncDataSrc::new 3",
            "SyncDataSrc::setup_async 1",
            "SyncDataSrc::setup_async 2",
            "SyncDataSrc::setup_async 3",
            "SyncDataSrc::create_data_conn_async 1",
            "SyncDataConn::new 1",
            "execute logic-0",
            "SyncDataSrc::create_data_conn_async 2",
            "SyncDataConn::new 2",
            "execute logic-1",
            "SyncDataSrc::create_data_conn_async 3",
            "SyncDataConn::new 3",
            "execute logic-2",
            "execute logic-3",
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

mod test_nested_logic_running_in_data_acc {
    use super::*;
    use crate::tokio::{logic, DataAcc};
    use override_macro::{overridable, override_with};

    #[overridable]
    trait GettingDataAcc14: DataAcc {
        async fn get_text_async(&mut self) -> errs::Result<String> {
            let _conn = self.get_data_conn_async::<SyncDataConn>("foo").await?;
            // getting a text by using _conn
            Ok("hello".to_string())
        }
    }

    #[overridable]
    trait SettingDataAcc14: DataAcc {
        async fn set_text_async(&mut self, _text: String) -> errs::Result<()> {
            let _conn = self.get_data_conn_async::<SyncDataConn>("bar").await?;
            // setting a text by using _conn
            Ok(())
        }
    }

    #[overridable]
    trait GetSetDataAcc14: DataAcc + HogeData14 {
        async fn process_text_async(&mut self) -> errs::Result<()> {
            self.start_async()
                .await
                .run_async(logic!(hoge_logic14))
                .await
                .end()
        }
    }

    struct MyScope14;
    impl GettingDataAcc14 for DataHub<MyScope14> {}
    impl SettingDataAcc14 for DataHub<MyScope14> {}
    impl GetSetDataAcc14 for DataHub<MyScope14> {}

    #[trait_variant::make(Send)]
    #[overridable]
    trait HogeData14: Sized {
        async fn get_text_async(&mut self) -> errs::Result<String>;
        async fn set_text_async(&mut self, text: String) -> errs::Result<()>;
    }

    #[override_with(GettingDataAcc14, SettingDataAcc14)]
    impl HogeData14 for DataHub<MyScope14> {}

    async fn hoge_logic14(data: &mut impl HogeData14) -> errs::Result<()> {
        let mut text = data.get_text_async().await?;
        text = text.to_uppercase();
        data.set_text_async(text).await?;
        Ok(())
    }

    #[overridable]
    trait FugaData14 {
        async fn process_text_async(&mut self) -> errs::Result<()>;
    }

    #[override_with(GetSetDataAcc14)]
    impl FugaData14 for DataHub<MyScope14> {}

    async fn fuga_logic14(data: &mut impl FugaData14) -> errs::Result<()> {
        data.process_text_async().await
    }

    #[tokio::test]
    async fn test_txn() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            let mut runner = LogicRunner::new(DataHub::<MyScope14>::new());
            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));
            if let Err(err) = runner
                .begin_txn_async()
                .await
                .run_async(logic!(fuga_logic14))
                .await
                .end_txn_async()
                .await
            {
                panic!("{err:?}");
            }
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
}

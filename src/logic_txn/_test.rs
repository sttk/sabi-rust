use crate::data_conn::_test_data::*;
use crate::data_src::_test_data::*;
use crate::{DataAcc, DataConnError, DataHub, DataHubError, LogicRunner};
use std::sync::{Arc, Mutex};

#[test]
fn test_logic_txn_and_ok() {
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
            .begin_txn()
            .run_or_block(move |data| {
                let _conn = data.get_data_conn::<SyncDataConn>("foo")?;
                logger_clone_0
                    .lock()
                    .unwrap()
                    .push("execute logic-0".to_string());
                Ok(())
            })
            .run(move |data| {
                let _conn = data.get_data_conn::<SyncDataConn>("bar")?;
                logger_clone_1
                    .lock()
                    .unwrap()
                    .push("execute logic-1".to_string());
                Ok(())
            })
            .run_force(move |data| {
                let _conn = data.get_data_conn::<SyncDataConn>("foo")?;
                let _conn = data.get_data_conn::<SyncDataConn>("bar")?;
                logger_clone_2
                    .lock()
                    .unwrap()
                    .push("execute logic-2".to_string());
                Ok(())
            })
            .end_txn();

        assert!(result.is_ok());
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
            "execute logic-0",
            "SyncDataSrc::create_data_conn 2",
            "SyncDataConn::new 2",
            "execute logic-1",
            "execute logic-2",
            "SyncDataConn::pre_commit 1",
            "SyncDataConn::pre_commit 2",
            "SyncDataConn::commit 1",
            "SyncDataConn::commit 2",
            "SyncDataConn::post_commit 1",
            "SyncDataConn::post_commit 2",
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
fn test_logic_run_but_fail_to_run_but_run_force_runs() {
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
            .begin_txn()
            .run(move |data| {
                let _conn = data.get_data_conn::<SyncDataConn>("foo")?;
                logger_clone_0
                    .lock()
                    .unwrap()
                    .push("execute logic-0".to_string());
                Err(errs::Err::new("logic-0 failed"))
            })
            .run(move |data| {
                let _conn = data.get_data_conn::<SyncDataConn>("bar")?;
                logger_clone_1
                    .lock()
                    .unwrap()
                    .push("execute logic-1".to_string());
                Ok(())
            })
            .run_or_block(move |data| {
                let _conn = data.get_data_conn::<SyncDataConn>("bar")?;
                logger_clone_2
                    .lock()
                    .unwrap()
                    .push("execute logic-2".to_string());
                Ok(())
            })
            .run_force(move |data| {
                let _conn = data.get_data_conn::<SyncDataConn>("foo")?;
                logger_clone_3
                    .lock()
                    .unwrap()
                    .push("execute logic-3".to_string());
                Ok(())
            })
            .end_txn();

        if let Err(err) = result {
            match err.reason::<DataHubError>() {
                Ok(DataHubError::FailToRunLogic { errors }) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].index, 0);
                    assert_eq!(errors[0].name, "LogicTxn#run(logic-0)".into());
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
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "execute logic-0",
                "execute logic-3",
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
fn test_runner_but_failed_to_run_or_block_then_skip_even_run_force() {
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
            .begin_txn()
            .run_or_block(move |data| {
                let _conn = data.get_data_conn::<SyncDataConn>("foo")?;
                logger_clone_0
                    .lock()
                    .unwrap()
                    .push("execute logic-0".to_string());
                Err(errs::Err::new("logic-0 failed"))
            })
            .run(move |data| {
                let _conn = data.get_data_conn::<SyncDataConn>("bar")?;
                logger_clone_1
                    .lock()
                    .unwrap()
                    .push("execute logic-1".to_string());
                Ok(())
            })
            .run_force(move |data| {
                let _conn = data.get_data_conn::<SyncDataConn>("foo")?;
                logger_clone_2
                    .lock()
                    .unwrap()
                    .push("execute logic-2".to_string());
                Ok(())
            })
            .run_or_block(move |data| {
                let _conn = data.get_data_conn::<SyncDataConn>("bar")?;
                logger_clone_3
                    .lock()
                    .unwrap()
                    .push("execute logic-3".to_string());
                Ok(())
            })
            .end_txn();

        if let Err(err) = result {
            match err.reason::<DataHubError>() {
                Ok(DataHubError::FailToRunLogic { errors }) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].index, 0);
                    assert_eq!(errors[0].name, "LogicTxn#run_or_block(logic-0)".into());
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
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "execute logic-0",
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
fn test_runner_and_failed_to_run_force_then_skip_run_but_run_force_runs() {
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
            .begin_txn()
            .run_force(move |data| {
                let _conn = data.get_data_conn::<SyncDataConn>("foo")?;
                logger_clone_0
                    .lock()
                    .unwrap()
                    .push("execute logic-0".to_string());
                Err(errs::Err::new("logic-0 failed"))
            })
            .run(move |data| {
                let _conn = data.get_data_conn::<SyncDataConn>("bar")?;
                logger_clone_1
                    .lock()
                    .unwrap()
                    .push("execute logic-1".to_string());
                Ok(())
            })
            .run_force(move |data| {
                let _conn = data.get_data_conn::<SyncDataConn>("foo")?;
                logger_clone_2
                    .lock()
                    .unwrap()
                    .push("execute logic-2".to_string());
                Ok(())
            })
            .run_or_block(move |data| {
                let _conn = data.get_data_conn::<SyncDataConn>("bar")?;
                logger_clone_3
                    .lock()
                    .unwrap()
                    .push("execute logic-3".to_string());
                Ok(())
            })
            .end_txn();

        if let Err(err) = result {
            match err.reason::<DataHubError>() {
                Ok(DataHubError::FailToRunLogic { errors }) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].index, 0);
                    assert_eq!(errors[0].name, "LogicTxn#run_force(logic-0)".into());
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
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "execute logic-0",
                "execute logic-2",
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
fn test_runner_and_failed_to_commit() {
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
            .begin_txn()
            .run_force(move |data| {
                let _conn = data.get_data_conn::<SyncDataConn>("foo")?;
                logger_clone_0
                    .lock()
                    .unwrap()
                    .push("execute logic-0".to_string());
                Ok(())
            })
            .run(move |data| {
                let _conn = data.get_data_conn::<SyncDataConn>("bar")?;
                logger_clone_1
                    .lock()
                    .unwrap()
                    .push("execute logic-1".to_string());
                Ok(())
            })
            .run_force(move |data| {
                let _conn = data.get_data_conn::<SyncDataConn>("baz")?;
                logger_clone_2
                    .lock()
                    .unwrap()
                    .push("execute logic-2".to_string());
                Ok(())
            })
            .run_or_block(move |data| {
                let _conn = data.get_data_conn::<SyncDataConn>("bar")?;
                logger_clone_3
                    .lock()
                    .unwrap()
                    .push("execute logic-3".to_string());
                Ok(())
            })
            .end_txn();

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
            "SyncDataSrc::setup 1",
            "SyncDataSrc::setup 2",
            "SyncDataSrc::setup 3",
            "SyncDataSrc::create_data_conn 1",
            "SyncDataConn::new 1",
            "execute logic-0",
            "SyncDataSrc::create_data_conn 2",
            "SyncDataConn::new 2",
            "execute logic-1",
            "SyncDataSrc::create_data_conn 3",
            "SyncDataConn::new 3",
            "execute logic-2",
            "execute logic-3",
            "SyncDataConn::pre_commit 1",
            "SyncDataConn::pre_commit 2",
            "SyncDataConn::pre_commit 3",
            "SyncDataConn::commit 1",
            "SyncDataConn::commit 2 failed",
            "SyncDataConn::rollback 2",
            "SyncDataConn::rollback 3",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/data_conn/_test_data.rs, line = 68 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/data_conn/_test_data.rs, line = 68 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::on_txn_failure 3",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/data_conn/_test_data.rs, line = 68 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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
            "SyncDataSrc::setup 1",
            "SyncDataSrc::setup 2",
            "SyncDataSrc::setup 3",
            "SyncDataSrc::create_data_conn 1",
            "SyncDataConn::new 1",
            "execute logic-0",
            "SyncDataSrc::create_data_conn 2",
            "SyncDataConn::new 2",
            "execute logic-1",
            "SyncDataSrc::create_data_conn 3",
            "SyncDataConn::new 3",
            "execute logic-2",
            "execute logic-3",
            "SyncDataConn::pre_commit 1",
            "SyncDataConn::pre_commit 2",
            "SyncDataConn::pre_commit 3",
            "SyncDataConn::commit 1",
            "SyncDataConn::commit 2 failed",
            "SyncDataConn::rollback 2",
            "SyncDataConn::rollback 3",
            "SyncDataConn::on_txn_failure 1",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src\\data_conn\\_test_data.rs, line = 68 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::on_txn_failure 2",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src\\data_conn\\_test_data.rs, line = 68 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
            "SyncDataConn::on_txn_failure 3",
            "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src\\data_conn\\_test_data.rs, line = 68 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::data_conn::_test_data::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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
    use crate::DataAcc;
    use override_macro::{overridable, override_with};

    #[overridable]
    trait GettingDataAcc4: DataAcc {
        fn get_text(&mut self) -> errs::Result<String> {
            let _conn = self.get_data_conn::<SyncDataConn>("foo")?;
            // getting a text by using _conn
            Ok("hello".to_string())
        }
    }

    #[overridable]
    trait SettingDataAcc4: DataAcc {
        fn set_text(&mut self, _text: String) -> errs::Result<()> {
            let _conn = self.get_data_conn::<SyncDataConn>("bar")?;
            // setting a text by using _conn
            Ok(())
        }
    }

    #[overridable]
    trait GetSetDataAcc4: DataAcc + HogeData4 {
        fn process_text(&mut self) -> errs::Result<()> {
            self.start().run(hoge_logic4).end()
        }
    }

    struct MyScope4;
    impl GettingDataAcc4 for DataHub<MyScope4> {}
    impl SettingDataAcc4 for DataHub<MyScope4> {}
    impl GetSetDataAcc4 for DataHub<MyScope4> {}

    #[overridable]
    trait HogeData4: Sized {
        fn get_text(&mut self) -> errs::Result<String>;
        fn set_text(&mut self, text: String) -> errs::Result<()>;
    }

    #[override_with(GettingDataAcc4, SettingDataAcc4)]
    impl HogeData4 for DataHub<MyScope4> {}

    fn hoge_logic4(data: &mut impl HogeData4) -> errs::Result<()> {
        let mut text = data.get_text()?;
        text = text.to_uppercase();
        data.set_text(text)?;
        Ok(())
    }

    #[overridable]
    trait FugaData4 {
        fn process_text(&mut self) -> errs::Result<()>;
    }

    #[override_with(GetSetDataAcc4)]
    impl FugaData4 for DataHub<MyScope4> {}

    fn fuga_logic4(data: &mut impl FugaData4) -> errs::Result<()> {
        data.process_text()
    }

    #[test]
    fn test_txn() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            let mut runner = LogicRunner::new(DataHub::<MyScope4>::new());
            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));
            if let Err(err) = runner.begin_txn().run(fuga_logic4).end_txn() {
                panic!("{err:?}");
            }
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
                "SyncDataConn::pre_commit 1",
                "SyncDataConn::pre_commit 2",
                "SyncDataConn::commit 1",
                "SyncDataConn::commit 2",
                "SyncDataConn::post_commit 1",
                "SyncDataConn::post_commit 2",
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

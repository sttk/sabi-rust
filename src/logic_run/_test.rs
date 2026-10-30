use crate::data_conn::_test_data::*;
use crate::data_src::_test_data::*;
use crate::{DataAcc, DataHub, DataHubError, LogicRunner};
use std::sync::{Arc, Mutex};

#[test]
fn test_logic_run_and_ok() {
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
            .start()
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
            .end();

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
            .start()
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
            .end();

        if let Err(err) = result {
            match err.reason::<DataHubError>() {
                Ok(DataHubError::FailToRunLogic { errors }) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].index, 0);
                    assert_eq!(errors[0].name, "LogicRun#run(logic-0)".into());
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
            .start()
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
                let _conn = data.get_data_conn::<SyncDataConn>("bar")?;
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
            .end();

        if let Err(err) = result {
            match err.reason::<DataHubError>() {
                Ok(DataHubError::FailToRunLogic { errors }) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].index, 0);
                    assert_eq!(errors[0].name, "LogicRun#run_or_block(logic-0)".into());
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
            .start()
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
            .end();

        if let Err(err) = result {
            match err.reason::<DataHubError>() {
                Ok(DataHubError::FailToRunLogic { errors }) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].index, 0);
                    assert_eq!(errors[0].name, "LogicRun#run_force(logic-0)".into());
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
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
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
    trait GettingDataAcc3: DataAcc {
        fn get_text(&mut self) -> errs::Result<String> {
            let _conn = self.get_data_conn::<SyncDataConn>("foo")?;
            // getting a text by using _conn
            Ok("hello".to_string())
        }
    }

    #[overridable]
    trait SettingDataAcc3: DataAcc {
        fn set_text(&mut self, _text: String) -> errs::Result<()> {
            let _conn = self.get_data_conn::<SyncDataConn>("bar")?;
            // setting a text by using _conn
            Ok(())
        }
    }

    #[overridable]
    trait GetSetDataAcc3: DataAcc + HogeData3 {
        fn process_text(&mut self) -> errs::Result<()> {
            self.start().run(hoge_logic3).end()
        }
    }

    struct MyScope3;
    impl GettingDataAcc3 for DataHub<MyScope3> {}
    impl SettingDataAcc3 for DataHub<MyScope3> {}
    impl GetSetDataAcc3 for DataHub<MyScope3> {}

    #[overridable]
    trait HogeData3: Sized {
        fn get_text(&mut self) -> errs::Result<String>;
        fn set_text(&mut self, text: String) -> errs::Result<()>;
    }

    #[override_with(GettingDataAcc3, SettingDataAcc3)]
    impl HogeData3 for DataHub<MyScope3> {}

    fn hoge_logic3(data: &mut impl HogeData3) -> errs::Result<()> {
        let mut text = data.get_text()?;
        text = text.to_uppercase();
        data.set_text(text)?;
        Ok(())
    }

    #[overridable]
    trait FugaData3 {
        fn process_text(&mut self) -> errs::Result<()>;
    }

    #[override_with(GetSetDataAcc3)]
    impl FugaData3 for DataHub<MyScope3> {}

    fn fuga_logic3(data: &mut impl FugaData3) -> errs::Result<()> {
        data.process_text()
    }

    #[test]
    fn test_run() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            let mut runner = LogicRunner::new(DataHub::<MyScope3>::new());
            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));
            if let Err(err) = runner.start().run(fuga_logic3).end() {
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

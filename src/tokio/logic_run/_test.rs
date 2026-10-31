use crate::tokio::data_conn::_test_data::*;
use crate::tokio::data_src::_test_data::*;
use crate::tokio::*;

use std::sync::{Arc, Mutex};

#[tokio::test]
async fn test_logic_run_and_ok() {
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
            .start_async()
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
            .end();

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
            .start_async()
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
            .end();

        if let Err(err) = result {
            match err.reason::<DataHubError>() {
                Ok(DataHubError::FailToRunLogic { errors }) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].index, 0);
                    assert_eq!(errors[0].name, "LogicRun#run_async(logic-0)".into());
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
async fn test_runner_but_failed_to_run_or_block_then_skip_even_run_force() {
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
            .start_async()
            .await
            .run_or_block_async(move |data| {
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
                    let _conn = data.get_data_conn_async::<SyncDataConn>("bar").await?;
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
            .end();

        if let Err(err) = result {
            match err.reason::<DataHubError>() {
                Ok(DataHubError::FailToRunLogic { errors }) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].index, 0);
                    assert_eq!(
                        errors[0].name,
                        "LogicRun#run_or_block_async(logic-0)".into()
                    );
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
            .start_async()
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
            .end();

        if let Err(err) = result {
            match err.reason::<DataHubError>() {
                Ok(DataHubError::FailToRunLogic { errors }) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].index, 0);
                    assert_eq!(errors[0].name, "LogicRun#run_force_async(logic-0)".into());
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
    use crate::tokio::{logic, DataAcc};
    use override_macro::{overridable, override_with};

    #[overridable]
    trait GettingDataAcc13: DataAcc {
        async fn get_text_async(&mut self) -> errs::Result<String> {
            let _conn = self.get_data_conn_async::<SyncDataConn>("foo").await?;
            // getting a text by using _conn
            Ok("hello".to_string())
        }
    }

    #[overridable]
    trait SettingDataAcc13: DataAcc {
        async fn set_text_async(&mut self, _text: String) -> errs::Result<()> {
            let _conn = self.get_data_conn_async::<SyncDataConn>("bar").await?;
            // setting a text by using _conn
            Ok(())
        }
    }

    #[overridable]
    trait GetSetDataAcc13: DataAcc + HogeData13 {
        async fn process_text_async(&mut self) -> errs::Result<()> {
            self.start_async()
                .await
                .run_async(logic!(hoge_logic13))
                .await
                .end()
        }
    }

    struct MyScope13;
    impl GettingDataAcc13 for DataHub<MyScope13> {}
    impl SettingDataAcc13 for DataHub<MyScope13> {}
    impl GetSetDataAcc13 for DataHub<MyScope13> {}

    #[trait_variant::make(Send)]
    #[overridable]
    trait HogeData13: Sized {
        async fn get_text_async(&mut self) -> errs::Result<String>;
        async fn set_text_async(&mut self, text: String) -> errs::Result<()>;
    }

    #[override_with(GettingDataAcc13, SettingDataAcc13)]
    impl HogeData13 for DataHub<MyScope13> {}

    async fn hoge_logic13(data: &mut impl HogeData13) -> errs::Result<()> {
        let mut text = data.get_text_async().await?;
        text = text.to_uppercase();
        data.set_text_async(text).await?;
        Ok(())
    }

    #[overridable]
    trait FugaData13 {
        async fn process_text_async(&mut self) -> errs::Result<()>;
    }

    #[override_with(GetSetDataAcc13)]
    impl FugaData13 for DataHub<MyScope13> {}

    async fn fuga_logic13(data: &mut impl FugaData13) -> errs::Result<()> {
        data.process_text_async().await
    }

    #[tokio::test]
    async fn test_run() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            let mut runner = LogicRunner::new(DataHub::<MyScope13>::new());
            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));
            if let Err(err) = runner
                .start_async()
                .await
                .run_async(logic!(fuga_logic13))
                .await
                .end()
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

// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::tokio::{DataHub, DataHubError, ErrEntry, LogicErrAt, LogicRun};

use std::future::Future;
use std::mem;
use std::pin::Pin;

impl<'a, T> LogicRun<'a, T> {
    pub(crate) async fn new_async(hub: &'a mut DataHub<T>, nested: bool) -> LogicRun<'a, T> {
        if !nested {
            if let Err(err) = hub.run.begin_async().await {
                return Self {
                    hub,
                    err: LogicErrAt::Begin { err },
                    index: 0,
                    nested,
                };
            }
        }

        Self {
            hub,
            err: LogicErrAt::Run {
                errors: Vec::with_capacity(0),
            },
            index: 0,
            nested,
        }
    }

    /// Executes an asynchronous logic closure if no previous error has occurred.
    ///
    /// If an error was already encountered in a preceding step, this closure is skipped.
    ///
    /// # Type Parameters
    ///
    /// * `F`: The type of the asynchronous closure returning a pinned future.
    ///
    /// # Parameters
    ///
    /// * `logic_fn`: The business logic closure to execute asynchronously.
    ///
    /// # Returns
    ///
    /// Self for method chaining.
    pub async fn run_async<F>(mut self, mut logic_fn: F) -> Self
    where
        for<'b> F: FnMut(
            &'b mut DataHub<T>,
        ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        let index = self.index;
        self.index = index + 1;

        match self.err {
            LogicErrAt::Run { ref mut errors } => {
                if errors.is_empty() {
                    if let Err(err) = logic_fn(self.hub).await {
                        errors.push(ErrEntry {
                            index,
                            name: format!("LogicRun#run_async(logic-{})", index).into(),
                            err,
                        });
                    }
                }
                self
            }
            _ => self,
        }
    }

    /// Executes an asynchronous logic closure regardless of prior non-blocking errors.
    ///
    /// This closure will be executed even if preceding calls to
    /// [`run_async`](LogicRun::run_async) or [`run_force_async`](LogicRun::run_force_async)
    /// failed, unless a previous call to [`run_or_block_async`](LogicRun::run_or_block_async)
    /// resulted in an error.
    ///
    /// # Type Parameters
    ///
    /// * `F`: The type of the asynchronous closure returning a pinned future.
    ///
    /// # Parameters
    ///
    /// * `logic_fn`: The business logic closure to execute asynchronously.
    ///
    /// # Returns
    ///
    /// Self for method chaining.
    pub async fn run_force_async<F>(mut self, mut logic_fn: F) -> Self
    where
        for<'b> F: FnMut(
            &'b mut DataHub<T>,
        ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        let index = self.index;
        self.index = index + 1;

        match self.err {
            LogicErrAt::Run { ref mut errors } => {
                if let Err(err) = logic_fn(self.hub).await {
                    errors.push(ErrEntry {
                        index,
                        name: format!("LogicRun#run_force_async(logic-{})", index).into(),
                        err,
                    });
                }
                self
            }
            _ => self,
        }
    }

    /// Executes an asynchronous logic closure, blocking all subsequent steps if it fails.
    ///
    /// If no error has occurred so far, this closure is executed. If it returns an error, all
    /// subsequent steps (including [`run_force_async`](LogicRun::run_force_async)) will be
    /// blocked and skipped.
    ///
    /// # Type Parameters
    ///
    /// * `F`: The type of the asynchronous closure returning a pinned future.
    ///
    /// # Parameters
    ///
    /// * `logic_fn`: The business logic closure to execute asynchronously.
    ///
    /// # Returns
    ///
    /// Self for method chaining.
    pub async fn run_or_block_async<F>(mut self, mut logic_fn: F) -> Self
    where
        for<'b> F: FnMut(
            &'b mut DataHub<T>,
        ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        let index = self.index;
        self.index = index + 1;

        match self.err {
            LogicErrAt::Run { ref mut errors } => {
                if errors.is_empty() {
                    if let Err(err) = logic_fn(self.hub).await {
                        errors.push(ErrEntry {
                            index,
                            name: format!("LogicRun#run_or_block_async(logic-{})", index).into(),
                            err,
                        });
                        self.err = LogicErrAt::Block {
                            errors: mem::take(errors),
                        };
                    }
                }
                self
            }
            _ => self,
        }
    }

    /// Finalizes the logic execution chain and releases resources.
    ///
    /// This ends the execution run, closes any open data connections, and returns the result of
    /// the execution.
    ///
    /// # Returns
    ///
    /// * `errs::Result<()>`: The result of the logic function's execution,
    ///   or an error if executing `logic_fn` fails.
    pub fn end(self) -> errs::Result<()> {
        if !self.nested {
            self.hub.run.end();
        }

        match self.err {
            LogicErrAt::Begin { err } => Err(err),
            LogicErrAt::Run { errors } => {
                if errors.is_empty() {
                    Ok(())
                } else {
                    Err(errs::Err::new(DataHubError::FailToRunLogic { errors }))
                }
            }
            LogicErrAt::Block { errors } => {
                Err(errs::Err::new(DataHubError::FailToRunLogic { errors }))
            }
            LogicErrAt::Ended => Ok(()),
        }
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod tests_of_logic_run {
    use super::*;
    use crate::tokio::_test_commons::*;
    use crate::tokio::{DataAcc, LogicRunner};
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
}

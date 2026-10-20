// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::tokio::{DataHub, DataHubError, ErrEntry, LogicErrAt, LogicTxn};

use std::future::Future;
use std::mem;
use std::pin::Pin;

impl<'a, T> LogicTxn<'a, T> {
    pub(crate) async fn new_async(hub: &'a mut DataHub<T>) -> LogicTxn<'a, T> {
        if let Err(err) = hub.run.begin_async().await {
            Self {
                hub,
                err: LogicErrAt::Begin { err },
                index: 0,
            }
        } else {
            Self {
                hub,
                err: LogicErrAt::Run {
                    errors: Vec::with_capacity(0),
                },
                index: 0,
            }
        }
    }

    /// Executes a transactional logic closure asynchronously if no previous error has occurred.
    ///
    /// If an error was already encountered in a preceding step, this closure is skipped.
    ///
    /// # Type Parameters
    ///
    /// * `F`: The type of the asynchronous closure returning a pinned future.
    ///
    /// # Parameters
    ///
    /// * `logic_fn`: The transactional business logic closure to execute asynchronously.
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
        self.index += 1;

        match self.err {
            LogicErrAt::Run { ref mut errors } => {
                if errors.is_empty() {
                    if let Err(err) = logic_fn(self.hub).await {
                        errors.push(ErrEntry {
                            index,
                            name: format!("LogicTxn#run_async(logic-{})", index).into(),
                            err,
                        });
                    }
                }
                self
            }
            _ => self,
        }
    }

    /// Executes a transactional logic closure asynchronously regardless of prior non-blocking
    /// errors.
    ///
    /// This closure will be executed even if preceding calls to
    /// [`run_async`](LogicTxn::run_async) or [`run_force_async`](LogicTxn::run_force_async)
    /// failed, unless a previous call to [`run_or_block_async`](LogicTxn::run_or_block_async)
    /// resulted in an error.
    ///
    /// # Type Parameters
    ///
    /// * `F`: The type of the asynchronous closure returning a pinned future.
    ///
    /// # Parameters
    ///
    /// * `logic_fn`: The transactional business logic closure to execute asynchronously.
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
        self.index += 1;

        match self.err {
            LogicErrAt::Run { ref mut errors } => {
                if let Err(err) = logic_fn(self.hub).await {
                    errors.push(ErrEntry {
                        index,
                        name: format!("LogicTxn#run_force_async(logic-{})", index).into(),
                        err,
                    });
                }
                self
            }
            _ => self,
        }
    }

    /// Executes a transactional logic closure asynchronously, blocking all subsequent steps if it
    /// fails.
    ///
    /// If no error has occurred so far, this closure is executed. If it returns an error, all
    /// subsequent steps (including [`run_force_async`](LogicTxn::run_force_async)) will be
    /// blocked and skipped.
    ///
    /// # Type Parameters
    ///
    /// * `F`: The type of the asynchronous closure returning a pinned future.
    ///
    /// # Parameters
    ///
    /// * `logic_fn`: The transactional business logic closure to execute asynchronously.
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
        self.index += 1;

        match self.err {
            LogicErrAt::Run { ref mut errors } => {
                if errors.is_empty() {
                    if let Err(err) = logic_fn(self.hub).await {
                        errors.push(ErrEntry {
                            index,
                            name: format!("LogicTxn#run_or_block_async(logic-{})", index).into(),
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

    /// Concludes the transaction asynchronously, committing participating data connections or
    /// rolling them back.
    ///
    /// If all executed logic closures succeeded without error, this commits changes on all
    /// participating data connections through their
    /// [`pre_commit_async`](crate::tokio::DataConn::pre_commit_async),
    /// [`commit_async`](crate::tokio::DataConn::commit_async), and
    /// [`post_commit_async`](crate::tokio::DataConn::post_commit_async) lifecycle methods.
    ///
    /// If any error occurred during logic execution or commit phases, all uncommitted
    /// connections are rolled back via
    /// [`rollback_async`](crate::tokio::DataConn::rollback_async) and notified via
    /// [`on_txn_failure_async`](crate::tokio::DataConn::on_txn_failure_async).
    ///
    /// # Returns
    ///
    /// * `errs::Result<()>`: `Ok(())` if the closure and the commit phase succeed,
    ///   or an `errs::Err` if any transaction phase fails.
    pub async fn end_txn_async(self) -> errs::Result<()> {
        match self.err {
            LogicErrAt::Begin { err } => {
                self.hub.run.end();
                Err(err)
            }
            LogicErrAt::Run { errors } => {
                let mut reports = self.hub.run.new_failure_reports();
                if errors.is_empty() {
                    let result = self.hub.run.commit_async(&mut reports).await;
                    if result.is_err() {
                        self.hub.run.rollback_async(reports).await;
                    }
                    self.hub.run.end();
                    result
                } else {
                    self.hub.run.rollback_async(reports).await;
                    self.hub.run.end();
                    Err(errs::Err::new(DataHubError::FailToRunLogic { errors }))
                }
            }
            LogicErrAt::Block { errors } => {
                let reports = self.hub.run.new_failure_reports();
                self.hub.run.rollback_async(reports).await;
                self.hub.run.end();
                Err(errs::Err::new(DataHubError::FailToRunLogic { errors }))
            }
        }
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod tests_of_logic_txn {
    use super::*;
    use crate::tokio::_test_commons::*;
    use crate::tokio::{DataAcc, DataConnError, LogicRunner};
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
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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
                .begin_txn_async()
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
                        assert_eq!(
                            errors[0].name,
                            "LogicTxn#run_or_block_async(logic-0)".into()
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
                "SyncDataConn::rollback_async 1",
                "SyncDataConn::on_txn_failure_async 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/tokio/_test_commons.rs, line = 67 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure_async 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/tokio/_test_commons.rs, line = 67 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure_async 3",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/tokio/_test_commons.rs, line = 67 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src\\tokio\\_test_commons.rs, line = 67 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure_async 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src\\tokio\\_test_commons.rs, line = 67 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure_async 3",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src\\tokio\\_test_commons.rs, line = 67 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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
}

// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::tokio::{DataHub, DataHubError, ErrEntry, LogicErrAt, LogicTxn};

use futures::FutureExt;
use std::future::Future;
use std::pin::Pin;
use std::{mem, panic};

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

    /// Executes an asynchronous logic function if no prior errors have occurred in this
    /// transaction.
    ///
    /// If an error occurred in an earlier step, `logic_fn` is skipped.
    ///
    /// # Type Parameters
    ///
    /// - `F`: The closure type for the async logic function.
    ///
    /// # Arguments
    ///
    /// - `logic_fn`: The asynchronous business logic function to execute. Use the
    ///   [`logic!`](crate::tokio::logic) macro to adapt async functions.
    ///
    /// # Returns
    ///
    /// `Self` for continuing the method chain.
    ///
    /// # Panics
    ///
    /// If `logic_fn` panics, opened data connections are abnormally terminated via `abend()`,
    /// and the panic is resumed.
    pub async fn run_async<F>(mut self, mut logic_fn: F) -> Self
    where
        for<'b> F: FnMut(
            &'b mut DataHub<T>,
        ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        let index = self.index;
        self.index += 1;

        if self.hub.run.is_abnormal() {
            return self;
        }

        match self.err {
            LogicErrAt::Run { ref mut errors } => {
                if errors.is_empty() {
                    match panic::AssertUnwindSafe(async { logic_fn(self.hub).await })
                        .catch_unwind()
                        .await
                    {
                        Ok(Ok(_)) => {}
                        Ok(Err(err)) => {
                            errors.push(ErrEntry {
                                index,
                                name: format!("LogicTxn#run_async(logic-{})", index).into(),
                                err,
                            });
                        }
                        Err(payload) => {
                            self.hub.run.abend();
                            panic::resume_unwind(payload);
                        }
                    }
                }
                self
            }
            _ => self,
        }
    }

    /// Executes an asynchronous logic function even if prior steps produced errors.
    ///
    /// Useful for cleanup or compensating operations that must run regardless of previous
    /// failures, unless execution has been blocked by
    /// [`run_or_block_async`](Self::run_or_block_async).
    ///
    /// # Type Parameters
    ///
    /// - `F`: The closure type for the async logic function.
    ///
    /// # Arguments
    ///
    /// - `logic_fn`: The asynchronous business logic function to execute unconditionally.
    ///
    /// # Returns
    ///
    /// `Self` for continuing the method chain.
    ///
    /// # Panics
    ///
    /// If `logic_fn` panics, opened data connections are abnormally terminated via `abend()`,
    /// and the panic is resumed.
    pub async fn run_force_async<F>(mut self, mut logic_fn: F) -> Self
    where
        for<'b> F: FnMut(
            &'b mut DataHub<T>,
        ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        let index = self.index;
        self.index += 1;

        if self.hub.run.is_abnormal() {
            return self;
        }

        match self.err {
            LogicErrAt::Run { ref mut errors } => {
                match panic::AssertUnwindSafe(async { logic_fn(self.hub).await })
                    .catch_unwind()
                    .await
                {
                    Ok(Ok(_)) => {}
                    Ok(Err(err)) => {
                        errors.push(ErrEntry {
                            index,
                            name: format!("LogicTxn#run_force_async(logic-{})", index).into(),
                            err,
                        });
                    }
                    Err(payload) => {
                        self.hub.run.abend();
                        panic::resume_unwind(payload);
                    }
                }
                self
            }
            _ => self,
        }
    }

    /// Executes an asynchronous logic function, blocking all subsequent steps if it fails.
    ///
    /// If `logic_fn` produces an error, the error is recorded and all subsequent steps
    /// (including [`run_force_async`](Self::run_force_async)) will be skipped.
    ///
    /// # Type Parameters
    ///
    /// - `F`: The closure type for the async logic function.
    ///
    /// # Arguments
    ///
    /// - `logic_fn`: The critical asynchronous business logic function to execute.
    ///
    /// # Returns
    ///
    /// `Self` for continuing the method chain.
    ///
    /// # Panics
    ///
    /// If `logic_fn` panics, opened data connections are abnormally terminated via `abend()`,
    /// and the panic is resumed.
    pub async fn run_or_block_async<F>(mut self, mut logic_fn: F) -> Self
    where
        for<'b> F: FnMut(
            &'b mut DataHub<T>,
        ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        let index = self.index;
        self.index += 1;

        if self.hub.run.is_abnormal() {
            return self;
        }

        match self.err {
            LogicErrAt::Run { ref mut errors } => {
                if errors.is_empty() {
                    match panic::AssertUnwindSafe(async { logic_fn(self.hub).await })
                        .catch_unwind()
                        .await
                    {
                        Ok(Ok(_)) => {}
                        Ok(Err(err)) => {
                            errors.push(ErrEntry {
                                index,
                                name: format!("LogicTxn#run_or_block_async(logic-{})", index)
                                    .into(),
                                err,
                            });
                            self.err = LogicErrAt::Block {
                                errors: mem::take(errors),
                            };
                        }
                        Err(payload) => {
                            self.hub.run.abend();
                            panic::resume_unwind(payload);
                        }
                    }
                }
                self
            }
            _ => self,
        }
    }

    /// Finishes the transaction chain asynchronously, commits or rolls back, and closes data
    /// connections.
    ///
    /// If all executed logic functions succeeded, attempts to commit all opened data connections
    /// asynchronously. If any logic function produced an error or if committing fails, all
    /// connections are rolled back asynchronously. In all cases, opened connections are closed.
    ///
    /// # Returns
    ///
    /// - `Ok(())` on successful execution and commit.
    /// - `Err(errs::Err)` containing [`DataHubError::FailToRunLogic`] if any logic step failed,
    ///   commit error if commit failed, or the initial error if the session failed to begin.
    pub async fn end_txn_async(self) -> errs::Result<()> {
        if self.hub.run.is_abnormal() {
            return Err(errs::Err::new(DataHubError::AbnormalState));
        }

        match panic::AssertUnwindSafe(async {
            match self.err {
                LogicErrAt::Begin { err } => {
                    self.hub.run.end();
                    Err(err)
                }
                LogicErrAt::Run { errors } => {
                    if errors.is_empty() {
                        let mut reports = self.hub.run.new_failure_reports(&Ok(()));
                        let result = self.hub.run.commit_async(&mut reports).await;
                        if result.is_err() {
                            self.hub.run.rollback_async(reports).await;
                        }
                        self.hub.run.end();
                        result
                    } else {
                        let result = Err(errs::Err::new(DataHubError::FailToRunLogic { errors }));
                        let reports = self.hub.run.new_failure_reports(&result);
                        self.hub.run.rollback_async(reports).await;
                        self.hub.run.end();
                        result
                    }
                }
                LogicErrAt::Block { errors } => {
                    let result = Err(errs::Err::new(DataHubError::FailToRunLogic { errors }));
                    let reports = self.hub.run.new_failure_reports(&result);
                    self.hub.run.rollback_async(reports).await;
                    self.hub.run.end();
                    result
                }
            }
        })
        .catch_unwind()
        .await
        {
            Ok(result) => result,
            Err(payload) => {
                self.hub.run.abend();
                panic::resume_unwind(payload);
            }
        }
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test;
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_panic;

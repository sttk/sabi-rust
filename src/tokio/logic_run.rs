// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::tokio::{AsDataHub, DataHubError, ErrEntry, LogicErrAt, LogicRun};

use futures::FutureExt;
use std::future::Future;
use std::pin::Pin;
use std::{mem, panic};

impl<'a, A: AsDataHub> LogicRun<'a, A> {
    pub(crate) async fn new_async(hub: &'a mut A, nested: bool) -> LogicRun<'a, A> {
        if !nested {
            if let Err(err) = hub.as_hub().run.begin_async().await {
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

    /// Executes an asynchronous logic function if no prior errors have occurred in the chain.
    ///
    /// If an error occurred in a preceding step, `logic_fn` is skipped.
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
        for<'b> F: FnMut(&'b mut A) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        let index = self.index;
        self.index = index + 1;

        if self.hub.as_hub().run.is_abnormal() {
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
                                name: format!("LogicRun#run_async(logic-{})", index).into(),
                                err,
                            });
                        }
                        Err(payload) => {
                            self.hub.as_hub().run.abend();
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
        for<'b> F: FnMut(&'b mut A) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        let index = self.index;
        self.index = index + 1;

        if self.hub.as_hub().run.is_abnormal() {
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
                            name: format!("LogicRun#run_force_async(logic-{})", index).into(),
                            err,
                        });
                    }
                    Err(payload) => {
                        self.hub.as_hub().run.abend();
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
        for<'b> F: FnMut(&'b mut A) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        let index = self.index;
        self.index = index + 1;

        if self.hub.as_hub().run.is_abnormal() {
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
                                name: format!("LogicRun#run_or_block_async(logic-{})", index)
                                    .into(),
                                err,
                            });
                            self.err = LogicErrAt::Block {
                                errors: mem::take(errors),
                            };
                        }
                        Err(payload) => {
                            self.hub.as_hub().run.abend();
                            panic::resume_unwind(payload);
                        }
                    }
                }
                self
            }
            _ => self,
        }
    }

    /// Finishes the logic execution chain, releases data connections, and returns the result.
    ///
    /// # Returns
    ///
    /// - `Ok(())` if all executed logic functions completed successfully.
    /// - `Err(errs::Err)` containing [`DataHubError::FailToRunLogic`] with collected errors if any
    ///   step failed, or the initial error if the session failed to begin.
    pub fn end(self) -> errs::Result<()> {
        if self.hub.as_hub().run.is_abnormal() {
            return Ok(());
        }

        if !self.nested {
            match panic::catch_unwind(panic::AssertUnwindSafe(|| {
                self.hub.as_hub().run.end();
            })) {
                Ok(_) => {}
                Err(payload) => {
                    self.hub.as_hub().run.abend();
                    panic::resume_unwind(payload);
                }
            }
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
        }
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test;
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_panic;

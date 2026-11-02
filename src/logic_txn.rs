// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::{DataHub, DataHubError, ErrEntry, LogicErrAt, LogicTxn};

use std::{mem, panic};

impl<'a, T> LogicTxn<'a, T> {
    pub(crate) fn new(hub: &'a mut DataHub<T>) -> LogicTxn<'a, T> {
        if let Err(err) = hub.run.begin() {
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

    /// Executes a business logic function if no prior errors have occurred in this transaction.
    ///
    /// If an error occurred in an earlier step, `logic_fn` is skipped.
    ///
    /// # Type Parameters
    ///
    /// - `F`: The closure type for the logic function
    ///   (`FnMut(&mut DataHub<T>) -> errs::Result<()>`).
    ///
    /// # Arguments
    ///
    /// - `logic_fn`: The business logic function to execute.
    ///
    /// # Returns
    ///
    /// `Self` for continuing the method chain.
    ///
    /// # Panics
    ///
    /// If `logic_fn` panics, opened data connections are abnormally terminated via `abend()`,
    /// and the panic is resumed.
    pub fn run<F>(mut self, mut logic_fn: F) -> Self
    where
        F: FnMut(&mut DataHub<T>) -> errs::Result<()>,
    {
        let index = self.index;
        self.index += 1;

        if self.hub.run.is_abnormal() {
            return self;
        }

        match self.err {
            LogicErrAt::Run { ref mut errors } => {
                if errors.is_empty() {
                    match panic::catch_unwind(panic::AssertUnwindSafe(|| logic_fn(self.hub))) {
                        Ok(Ok(_)) => {}
                        Ok(Err(err)) => {
                            errors.push(ErrEntry {
                                index,
                                name: format!("LogicTxn#run(logic-{})", index).into(),
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

    /// Executes a business logic function even if prior steps produced errors.
    ///
    /// Useful for cleanup or compensating operations that must run regardless of previous
    /// failures, unless execution has been blocked by [`run_or_block`](Self::run_or_block).
    /// Note that if any step failed, the transaction will still be rolled back at the end.
    ///
    /// # Type Parameters
    ///
    /// - `F`: The closure type for the logic function
    ///   (`FnMut(&mut DataHub<T>) -> errs::Result<()>`).
    ///
    /// # Arguments
    ///
    /// - `logic_fn`: The business logic function to execute unconditionally.
    ///
    /// # Returns
    ///
    /// `Self` for continuing the method chain.
    ///
    /// # Panics
    ///
    /// If `logic_fn` panics, opened data connections are abnormally terminated via `abend()`,
    /// and the panic is resumed.
    pub fn run_force<F>(mut self, mut logic_fn: F) -> Self
    where
        F: FnMut(&mut DataHub<T>) -> errs::Result<()>,
    {
        let index = self.index;
        self.index += 1;

        if self.hub.run.is_abnormal() {
            return self;
        }

        match self.err {
            LogicErrAt::Run { ref mut errors } => {
                match panic::catch_unwind(panic::AssertUnwindSafe(|| logic_fn(self.hub))) {
                    Ok(Ok(_)) => {}
                    Ok(Err(err)) => {
                        errors.push(ErrEntry {
                            index,
                            name: format!("LogicTxn#run_force(logic-{})", index).into(),
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

    /// Executes a business logic function, blocking all subsequent steps if it fails.
    ///
    /// If `logic_fn` produces an error, the error is recorded and all subsequent steps
    /// (including [`run_force`](Self::run_force)) will be skipped.
    ///
    /// # Type Parameters
    ///
    /// - `F`: The closure type for the logic function
    ///   (`FnMut(&mut DataHub<T>) -> errs::Result<()>`).
    ///
    /// # Arguments
    ///
    /// - `logic_fn`: The critical business logic function to execute.
    ///
    /// # Returns
    ///
    /// `Self` for continuing the method chain.
    ///
    /// # Panics
    ///
    /// If `logic_fn` panics, opened data connections are abnormally terminated via `abend()`,
    /// and the panic is resumed.
    pub fn run_or_block<F>(mut self, mut logic_fn: F) -> Self
    where
        F: FnMut(&mut DataHub<T>) -> errs::Result<()>,
    {
        let index = self.index;
        self.index += 1;

        if self.hub.run.is_abnormal() {
            return self;
        }

        match self.err {
            LogicErrAt::Run { ref mut errors } => {
                if errors.is_empty() {
                    match panic::catch_unwind(panic::AssertUnwindSafe(|| logic_fn(self.hub))) {
                        Ok(Ok(_)) => {}
                        Ok(Err(err)) => {
                            errors.push(ErrEntry {
                                index,
                                name: format!("LogicTxn#run_or_block(logic-{})", index).into(),
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

    /// Finishes the transaction chain, commits or rolls back, and closes data connections.
    ///
    /// If all executed logic functions succeeded, attempts to commit all opened data connections.
    /// If any logic function produced an error or if committing fails, all connections are
    /// rolled back. In all cases, opened connections are closed.
    ///
    /// # Returns
    ///
    /// - `Ok(())` on successful execution and commit.
    /// - `Err(errs::Err)` containing [`DataHubError::FailToRunLogic`] if any logic step failed,
    ///   commit error if commit failed, or the initial error if the session failed to begin.
    pub fn end_txn(self) -> errs::Result<()> {
        if self.hub.run.is_abnormal() {
            return Ok(());
        }

        match panic::catch_unwind(panic::AssertUnwindSafe(|| match self.err {
            LogicErrAt::Begin { err } => {
                self.hub.run.end();
                Err(err)
            }
            LogicErrAt::Run { errors } => {
                if errors.is_empty() {
                    let mut reports = self.hub.run.new_failure_reports(&Ok(()));
                    let result = self.hub.run.commit(&mut reports);
                    if result.is_err() {
                        self.hub.run.rollback(reports);
                    }
                    self.hub.run.end();
                    result
                } else {
                    let result = Err(errs::Err::new(DataHubError::FailToRunLogic { errors }));
                    let reports = self.hub.run.new_failure_reports(&result);
                    self.hub.run.rollback(reports);
                    self.hub.run.end();
                    result
                }
            }
            LogicErrAt::Block { errors } => {
                let result = Err(errs::Err::new(DataHubError::FailToRunLogic { errors }));
                let reports = self.hub.run.new_failure_reports(&result);
                self.hub.run.rollback(reports);
                self.hub.run.end();
                result
            }
        })) {
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

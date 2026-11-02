// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::{DataConn, DataHub, DataHubError, DataSrc, LogicRun, LogicRunner, LogicTxn};

use std::panic;
use std::sync::Arc;

impl<T: 'static> LogicRunner<T> {
    /// Creates a new `LogicRunner` wrapping the specified [`DataHub`].
    ///
    /// # Arguments
    ///
    /// - `hub`: The [`DataHub`] instance to encapsulate.
    ///
    /// # Returns
    ///
    /// A new [`LogicRunner<T>`] instance.
    #[inline]
    pub fn new(hub: DataHub<T>) -> Self {
        Self { hub }
    }

    /// Registers a session-local data source with the runner.
    ///
    /// Local data sources are available only within this runner's session and are closed
    /// when the runner finishes or enters an abnormal state.
    ///
    /// # Type Parameters
    ///
    /// - `S`: The data source type implementing [`DataSrc<C>`].
    /// - `C`: The data connection type produced by `S` ([`DataConn`] + `'static`).
    ///
    /// # Arguments
    ///
    /// - `name`: The identifier name for this data source (convertible to [`Arc<str>`]).
    /// - `ds`: The data source instance to register.
    ///
    /// # Notes
    ///
    /// Must be called before the first logic execution (e.g. before [`run`](Self::run),
    /// [`start`](Self::start), [`txn`](Self::txn), or [`begin_txn`](Self::begin_txn)).
    /// Once logic execution starts, the configuration is fixed and further registrations
    /// are ignored.
    #[inline]
    pub fn uses<S, C>(&mut self, name: impl Into<Arc<str>>, ds: S)
    where
        S: DataSrc<C>,
        C: DataConn + 'static,
    {
        self.hub.run.uses(name, ds);
    }

    /// Unregisters a previously registered session-local data source by name.
    ///
    /// # Arguments
    ///
    /// - `name`: The identifier name of the data source to remove.
    ///
    /// # Notes
    ///
    /// Like [`uses`](Self::uses), this must be called before logic execution begins.
    #[inline]
    pub fn disuses(&mut self, name: impl AsRef<str>) {
        self.hub.run.disuses(name);
    }

    /// Executes a single business logic function without transactional control.
    ///
    /// Automatically initializes local data sources, invokes `logic_fn` with a mutable reference
    /// to the internal [`DataHub`], and closes opened data connections when finished.
    ///
    /// If the runner is already in an abnormal state (e.g. after a previous panic), this method
    /// returns `Ok(())` immediately without executing the logic.
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
    /// - `Ok(())` on successful execution.
    /// - `Err(errs::Err)` if local data sources fail to set up or the logic function returns
    ///   an error.
    ///
    /// # Panics
    ///
    /// If `logic_fn` panics, all opened data connections are abnormally terminated via `abend()`,
    /// marking the runner as abnormal, and the panic is resumed.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// runner.run(my_logic)?;
    /// ```
    #[inline]
    pub fn run<F>(&mut self, mut logic_fn: F) -> errs::Result<()>
    where
        F: FnMut(&mut DataHub<T>) -> errs::Result<()>,
    {
        if self.hub.run.is_abnormal() {
            return Err(errs::Err::new(DataHubError::AbnormalState));
        }

        self.hub.run.begin()?;

        match panic::catch_unwind(panic::AssertUnwindSafe(|| {
            let result = logic_fn(&mut self.hub);
            self.hub.run.end();
            result
        })) {
            Ok(result) => result,
            Err(payload) => {
                self.hub.run.abend();
                panic::resume_unwind(payload);
            }
        }
    }

    /// Initiates a chained execution of logic functions via [`LogicRun`].
    ///
    /// # Returns
    ///
    /// A new [`LogicRun`] builder for chaining logic calls without transaction boundaries.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// runner.start()
    ///     .run(step1)
    ///     .run(step2)
    ///     .end()?;
    /// ```
    #[inline]
    pub fn start(&mut self) -> LogicRun<'_, DataHub<T>> {
        LogicRun::new(&mut self.hub, false)
    }

    /// Executes a business logic function within an automatic transaction boundary.
    ///
    /// If `logic_fn` succeeds (`Ok(())`), all opened data connections are committed.
    /// If `logic_fn` returns an `Err` or commit fails, all opened connections are rolled back.
    /// In all cases, opened connections are closed when the call finishes.
    ///
    /// If the runner is already in an abnormal state, this method returns `Ok(())` immediately.
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
    /// - `Ok(())` on successful execution and commit.
    /// - `Err(errs::Err)` if logic execution or committing fails.
    ///
    /// # Panics
    ///
    /// If `logic_fn` panics, all opened data connections are abnormally terminated via `abend()`,
    /// marking the runner as abnormal, and the panic is resumed.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// runner.txn(my_transactional_logic)?;
    /// ```
    #[inline]
    pub fn txn<F>(&mut self, mut logic_fn: F) -> errs::Result<()>
    where
        F: FnMut(&mut DataHub<T>) -> errs::Result<()>,
    {
        if self.hub.run.is_abnormal() {
            return Err(errs::Err::new(DataHubError::AbnormalState));
        }

        self.hub.run.begin()?;

        match panic::catch_unwind(panic::AssertUnwindSafe(|| {
            let mut result = logic_fn(&mut self.hub);
            let mut reports = self.hub.run.new_failure_reports(&result);
            if result.is_ok() {
                result = self.hub.run.commit(&mut reports);
            }
            if result.is_err() {
                self.hub.run.rollback(reports);
            }
            self.hub.run.end();
            result
        })) {
            Ok(result) => result,
            Err(payload) => {
                self.hub.run.abend();
                panic::resume_unwind(payload);
            }
        }
    }

    /// Begins a chained execution of logic functions within a transaction boundary
    /// via [`LogicTxn`].
    ///
    /// # Returns
    ///
    /// A new [`LogicTxn`] builder for chaining logic calls inside a transaction.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// runner.begin_txn()
    ///     .run(step1)
    ///     .run(step2)
    ///     .end_txn()?;
    /// ```
    #[inline]
    pub fn begin_txn(&mut self) -> LogicTxn<'_, T> {
        LogicTxn::new(&mut self.hub)
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_begin_txn;
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_new_uses_disuses;
#[cfg(test)]
mod _test_panic;
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_run;
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_start;
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_txn;

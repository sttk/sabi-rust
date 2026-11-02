// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::tokio::{DataConn, DataHub, DataSrc, LogicRun, LogicRunner, LogicTxn};

use futures::FutureExt;
use std::future::Future;
use std::panic;
use std::pin::Pin;
use std::sync::Arc;

impl<T: 'static + Send> LogicRunner<T> {
    /// Creates a new asynchronous `LogicRunner` wrapping the specified [`DataHub`].
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
    /// # Type Parameters
    ///
    /// - `S`: The data source type implementing [`DataSrc<C>`] + `'static`.
    /// - `C`: The data connection type produced by `S` ([`DataConn`] + `'static`).
    ///
    /// # Arguments
    ///
    /// - `name`: The identifier name for this data source (convertible to [`Arc<str>`]).
    /// - `ds`: The data source instance to register.
    ///
    /// # Notes
    ///
    /// Must be called before the first logic execution (e.g. before [`run_async`](Self::run_async),
    /// [`start_async`](Self::start_async), [`txn_async`](Self::txn_async), or
    /// [`begin_txn_async`](Self::begin_txn_async)).
    #[inline]
    pub fn uses<S, C>(&mut self, name: impl Into<Arc<str>>, ds: S)
    where
        S: DataSrc<C> + 'static,
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

    /// Executes an asynchronous business logic function without transactional control.
    ///
    /// Automatically initializes local data sources, invokes `logic_fn` with a mutable reference
    /// to the internal [`DataHub`], and closes opened data connections when finished.
    ///
    /// If the runner is already in an abnormal state, this method returns `Ok(())` immediately.
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
    /// runner.run_async(sabi::tokio::logic!(my_async_logic)).await?;
    /// ```
    #[inline]
    pub async fn run_async<F>(&mut self, mut logic_fn: F) -> errs::Result<()>
    where
        for<'b> F: FnMut(
            &'b mut DataHub<T>,
        ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        if self.hub.run.is_abnormal() {
            return Ok(());
        }

        self.hub.run.begin_async().await?;

        match panic::AssertUnwindSafe(async {
            let result = logic_fn(&mut self.hub).await;
            self.hub.run.end();
            result
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

    /// Initiates a chained execution of asynchronous logic functions via [`LogicRun`].
    ///
    /// # Returns
    ///
    /// A new [`LogicRun`] builder for chaining async logic calls without transaction boundaries.
    #[inline]
    pub async fn start_async(&mut self) -> LogicRun<'_, DataHub<T>> {
        LogicRun::new_async(&mut self.hub, false).await
    }

    /// Executes an asynchronous business logic function within an automatic transaction boundary.
    ///
    /// If `logic_fn` succeeds (`Ok(())`), all opened data connections are committed asynchronously.
    /// If `logic_fn` returns an `Err` or commit fails, all opened connections are rolled back
    /// asynchronously. In all cases, opened connections are closed when the call finishes.
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
    /// runner.txn_async(sabi::tokio::logic!(my_async_logic)).await?;
    /// ```
    #[inline]
    pub async fn txn_async<F>(&mut self, mut logic_fn: F) -> errs::Result<()>
    where
        for<'b> F: FnMut(
            &'b mut DataHub<T>,
        ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        if self.hub.run.is_abnormal() {
            return Ok(());
        }

        self.hub.run.begin_async().await?;

        match panic::AssertUnwindSafe(async {
            let mut result = logic_fn(&mut self.hub).await;
            let mut reports = self.hub.run.new_failure_reports(&result);
            if result.is_ok() {
                result = self.hub.run.commit_async(&mut reports).await;
            }
            if result.is_err() {
                self.hub.run.rollback_async(reports).await;
            }
            self.hub.run.end();
            result
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

    /// Begins a chained execution of asynchronous logic functions within a transaction boundary
    /// via [`LogicTxn`].
    ///
    /// # Returns
    ///
    /// A new [`LogicTxn`] builder for chaining async logic calls inside a transaction.
    #[inline]
    pub async fn begin_txn_async(&mut self) -> LogicTxn<'_, T> {
        LogicTxn::new_async(&mut self.hub).await
    }
}

#[macro_export]
#[doc(hidden)]
macro_rules! _logic {
    ($f:expr) => {
        |data| {
            let fut: std::pin::Pin<Box<dyn std::future::Future<Output = errs::Result<()>> + Send>> =
                Box::pin(async move { $f(data).await });
            fut
        }
    };
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_begin_txn;
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_new_uses_disuses;
#[cfg_attr(coverage_nightly, coverage(off))]
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

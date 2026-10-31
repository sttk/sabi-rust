// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::tokio::{DataConn, DataHub, DataSrc, LogicRun, LogicRunner, LogicTxn};

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

impl<T: 'static + Send> LogicRunner<T> {
    #[inline]
    pub fn new(hub: DataHub<T>) -> Self {
        Self { hub }
    }

    #[inline]
    pub fn uses<S, C>(&mut self, name: impl Into<Arc<str>>, ds: S)
    where
        S: DataSrc<C> + 'static,
        C: DataConn + 'static,
    {
        self.hub.run.uses(name, ds);
    }

    #[inline]
    pub fn disuses(&mut self, name: impl AsRef<str>) {
        self.hub.run.disuses(name);
    }

    #[inline]
    pub async fn run_async<F>(&mut self, mut logic_fn: F) -> errs::Result<()>
    where
        for<'b> F: FnMut(
            &'b mut DataHub<T>,
        ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        self.hub.run.begin_async().await?;
        let r = logic_fn(&mut self.hub).await;
        self.hub.run.end();
        r
    }

    #[inline]
    pub async fn start_async(&mut self) -> LogicRun<'_, DataHub<T>> {
        LogicRun::new_async(&mut self.hub, false).await
    }

    #[inline]
    pub async fn txn_async<F>(&mut self, mut logic_fn: F) -> errs::Result<()>
    where
        for<'b> F: FnMut(
            &'b mut DataHub<T>,
        ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        self.hub.run.begin_async().await?;
        let mut r = logic_fn(&mut self.hub).await;
        let mut reports = self.hub.run.new_failure_reports();
        if r.is_ok() {
            r = self.hub.run.commit_async(&mut reports).await;
        }
        if r.is_err() {
            self.hub.run.rollback_async(reports).await;
        }
        self.hub.run.end();
        r
    }

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
mod _test_run;
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_start;
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_txn;

// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::{DataConn, DataHub, DataSrc, LogicRun, LogicRunner, LogicTxn};

use std::sync::Arc;

impl<T: 'static> LogicRunner<T> {
    #[inline]
    pub fn new(hub: DataHub<T>) -> Self {
        Self { hub }
    }

    #[inline]
    pub fn uses<S, C>(&mut self, name: impl Into<Arc<str>>, ds: S)
    where
        S: DataSrc<C>,
        C: DataConn + 'static,
    {
        self.hub.run.uses(name, ds);
    }

    #[inline]
    pub fn disuses(&mut self, name: impl AsRef<str>) {
        self.hub.run.disuses(name);
    }

    #[inline]
    pub fn run<F>(&mut self, mut logic_fn: F) -> errs::Result<()>
    where
        F: FnMut(&mut DataHub<T>) -> errs::Result<()>,
    {
        self.hub.run.begin()?;

        let result = logic_fn(&mut self.hub);
        self.hub.run.end();
        result
    }

    #[inline]
    pub fn start(&mut self) -> LogicRun<'_, DataHub<T>> {
        LogicRun::new(&mut self.hub, false)
    }

    #[inline]
    pub fn txn<F>(&mut self, mut logic_fn: F) -> errs::Result<()>
    where
        F: FnMut(&mut DataHub<T>) -> errs::Result<()>,
    {
        self.hub.run.begin()?;

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
    }

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
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_run;
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_start;
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_txn;

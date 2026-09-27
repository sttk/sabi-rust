// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::{DataAcc, DataConn, DataHub, DataSrc, LogicData, LogicRun, LogicTxn};

use std::sync::Arc;

impl<T: 'static> LogicData<T> {
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
    pub fn run<F>(&mut self, logic_fn: F) -> errs::Result<()>
    where
        F: FnMut(&mut DataHub<T>) -> errs::Result<()>,
    {
        self.hub.run(logic_fn)
    }

    #[inline]
    pub fn start(&mut self) -> LogicRun<'_, T> {
        self.hub.start()
    }

    #[inline]
    pub fn txn<F>(&mut self, logic_fn: F) -> errs::Result<()>
    where
        F: FnMut(&mut DataHub<T>) -> errs::Result<()>,
    {
        self.hub._txn(logic_fn)
    }

    #[inline]
    pub fn begin_txn(&mut self) -> LogicTxn<'_, T> {
        self.hub._begin_txn()
    }
}

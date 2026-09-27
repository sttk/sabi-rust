// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::tokio::{DataAcc, DataConn, DataHub, DataSrc, LogicData, LogicRun, LogicTxn};

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

impl<T: 'static + Send> LogicData<T> {
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
    pub async fn run_async<F>(&mut self, logic_fn: F) -> errs::Result<()>
    where
        for<'b> F: FnMut(
            &'b mut DataHub<T>,
        ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        self.hub.run_async(logic_fn).await
    }

    #[inline]
    pub async fn start_async(&mut self) -> LogicRun<'_, T> {
        self.hub.start_async().await
    }

    #[inline]
    pub async fn txn_async<F>(&mut self, logic_fn: F) -> errs::Result<()>
    where
        for<'b> F: FnMut(
            &'b mut DataHub<T>,
        ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        self.hub._txn_async(logic_fn).await
    }

    #[inline]
    pub async fn begin_txn_async(&mut self) -> LogicTxn<'_, T> {
        self.hub._begin_txn_async().await
    }
}

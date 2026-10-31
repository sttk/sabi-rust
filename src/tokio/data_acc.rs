// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::tokio::{AsDataHub, DataAcc, DataConn, DataHub, LogicRun};

use std::future::Future;
use std::pin::Pin;

impl<T: 'static + Send> AsDataHub for DataHub<T> {
    type D = T;

    fn as_hub(&mut self) -> &mut DataHub<T> {
        self
    }
}

impl<T: 'static + Send> DataAcc for DataHub<T> {
    #[inline]
    async fn get_data_conn_async<C>(&mut self, name: &str) -> errs::Result<&mut C>
    where
        C: DataConn + 'static,
    {
        self.run.get_data_conn_async(name).await
    }

    #[inline]
    async fn run_async<F>(&mut self, mut logic_fn: F) -> errs::Result<()>
    where
        for<'b> F:
            FnMut(&'b mut Self) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        logic_fn(self).await
    }

    #[inline]
    async fn start_async(&mut self) -> LogicRun<'_, Self> {
        LogicRun::new_async(self, true).await
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_aggregation;
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_get_data_conn;
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_nested_logic_running;

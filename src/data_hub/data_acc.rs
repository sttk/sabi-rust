// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::{DataAcc, DataConn, DataHub, LogicRun};

impl<T: 'static> DataAcc for DataHub<T> {
    type D = T;

    #[inline]
    fn get_data_conn<C: DataConn + 'static>(&mut self, name: &str) -> errs::Result<&mut C> {
        self.run.get_data_conn(name)
    }

    #[inline]
    fn run<F>(&mut self, mut logic_fn: F) -> errs::Result<()>
    where
        F: for<'a> FnMut(&'a mut Self) -> errs::Result<()>,
    {
        logic_fn(self)
    }

    #[inline]
    fn start(&mut self) -> LogicRun<'_, T> {
        LogicRun::new(self, true)
    }
}

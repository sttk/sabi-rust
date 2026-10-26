// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::{DataAcc, DataConn, DataHub, LogicRun};

impl<T: 'static> DataAcc for DataHub<T> {
    type D = T;

    #[inline]
    fn get_data_conn<C: DataConn + 'static>(&mut self, name: &str) -> errs::Result<&mut C> {
        DataHub::<T>::get_data_conn(self, name)
    }

    #[inline]
    fn run<F>(&mut self, logic_fn: F) -> errs::Result<()>
    where
        F: FnMut(&mut DataHub<T>) -> errs::Result<()>,
    {
        DataHub::<T>::run_logic(self, logic_fn)
    }

    #[inline]
    fn start(&mut self) -> LogicRun<'_, T> {
        LogicRun::new(self, true)
    }
}

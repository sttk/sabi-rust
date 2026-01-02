// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::{DataAcc, DataConn, DataHub};

impl DataAcc for DataHub {
    fn get_data_conn<C: DataConn + 'static>(
        &mut self,
        name: impl AsRef<str>,
    ) -> errs::Result<&mut C> {
        DataHub::get_data_conn(self, name)
    }
}

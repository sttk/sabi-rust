// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::SendSyncNonNull;

use std::{marker, ptr};

impl<T: Send + Sync> SendSyncNonNull<T> {
    pub(crate) fn new(non_null_ptr: ptr::NonNull<T>) -> Self {
        Self {
            non_null_ptr,
            _phantom: marker::PhantomData,
        }
    }
}

unsafe impl<T: Send + Sync> Send for SendSyncNonNull<T> {}
unsafe impl<T: Send + Sync> Sync for SendSyncNonNull<T> {}

impl<T: Send + Sync> Clone for SendSyncNonNull<T> {
    #[inline(always)]
    fn clone(&self) -> Self {
        SendSyncNonNull::new(self.non_null_ptr)
    }
}

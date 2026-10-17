// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::AutoEnd;

use std::any;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};

impl<'a, T, F: FnOnce(&mut T)> Drop for AutoEnd<'a, T, F> {
    fn drop(&mut self) {
        if let Some(end_fn) = self.end_fn.take() {
            end_fn(self.target);
        }
    }
}

pub(crate) fn run_with_end<T, R>(
    target: &mut T,
    run_fn: impl FnOnce(&mut T) -> R,
    end_fn: impl FnOnce(&mut T),
) -> R {
    let ender = AutoEnd {
        target,
        end_fn: Some(end_fn),
    };
    run_fn(&mut *ender.target)
}

//

pub(crate) fn catch_to_msg<R>(f: impl FnOnce() -> R) -> Result<R, String> {
    catch_unwind(AssertUnwindSafe(f)).map_err(|payload| {
        payload
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "non-string panic payload".to_string())
    })
}

//

pub(crate) struct PanicDeferrer {
    first_payload: Option<Box<dyn any::Any + Send>>,
}

impl PanicDeferrer {
    pub(crate) fn catch<R>(&mut self, f: impl FnOnce() -> errs::Result<R>) {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(f)) {
            self.first_payload.get_or_insert(payload);
        }
    }

    pub(crate) fn resume(self) {
        if let Some(payload) = self.first_payload {
            resume_unwind(payload)
        }
    }
}

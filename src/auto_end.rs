// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::AutoEnd;

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

#[cfg(feature = "tokio")]
pub(crate) async fn run_with_end_async<T, R>(
    target: &mut T,
    run_fn: impl AsyncFnOnce(&mut T) -> R,
    end_fn: impl FnOnce(&mut T),
) -> R {
    let ender = AutoEnd {
        target,
        end_fn: Some(end_fn),
    };
    run_fn(&mut *ender.target).await
}

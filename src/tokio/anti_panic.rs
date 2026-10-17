// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::AutoEnd;

use futures::FutureExt;
use std::any;
use std::future::Future;
use std::panic::{resume_unwind, AssertUnwindSafe};

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

//

pub(crate) async fn catch_to_msg_async<R, Fut>(f: impl FnOnce() -> Fut) -> Result<R, String>
where
    Fut: Future<Output = R>,
{
    AssertUnwindSafe(async { f().await })
        .catch_unwind()
        .await
        .map_err(|payload| {
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
    pub(crate) async fn catch_async<R, Fut>(&mut self, f: impl FnOnce() -> Fut)
    where
        Fut: Future<Output = R>,
    {
        if let Err(payload) = AssertUnwindSafe(async { f().await }).catch_unwind().await {
            self.first_payload.get_or_insert(payload);
        }
    }

    pub(crate) fn resume(self) {
        if let Some(payload) = self.first_payload {
            resume_unwind(payload);
        }
    }
}

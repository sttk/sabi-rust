// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::tokio::{AsyncGroup, ErrEntry};

use futures::future;
use std::future::Future;

impl AsyncGroup {
    pub(crate) fn new() -> Self {
        Self {
            tasks: Vec::new(),
            attrs: Vec::new(),
            index: 0,
            name: Default::default(),
        }
    }

    #[allow(clippy::doc_overindented_list_items)]
    pub fn add<Fut>(&mut self, future: Fut)
    where
        Fut: Future<Output = errs::Result<()>> + Send + 'static,
    {
        self.tasks.push(Box::pin(future));
        self.attrs.push((self.index, self.name.clone()));
    }

    pub(crate) async fn join_and_collect_errors_async(self, errors: &mut Vec<ErrEntry>) {
        if self.tasks.is_empty() {
            return;
        }

        let result_all = future::join_all(self.tasks).await;
        for (i, result) in result_all.into_iter().enumerate() {
            if let Err(err) = result {
                errors.push(ErrEntry {
                    index: self.attrs[i].0,
                    name: self.attrs[i].1.clone(),
                    err,
                });
            }
        }
    }

    pub(crate) async fn join_and_ignore_errors_async(self) {
        let _ = future::join_all(self.tasks).await;
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
pub(crate) mod _test_data;
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_join_and_collect_errors_async;
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_join_and_ignore_errors_async;

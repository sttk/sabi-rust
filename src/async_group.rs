// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::{AsyncGroup, ErrEntry};

use std::sync::Arc;
use std::{mem, thread};

#[derive(Debug)]
pub enum AsyncGroupError {
    ThreadPanicked(String),
}

impl AsyncGroup {
    pub(crate) fn new() -> Self {
        Self {
            handlers: Vec::new(),
            index: 0,
            name: Default::default(),
        }
    }

    pub fn add<F>(&mut self, f: F)
    where
        F: FnOnce() -> errs::Result<()> + Send + 'static,
    {
        self.handlers
            .push((self.index, self.name.clone(), thread::spawn(f)));
    }

    pub(crate) fn join_and_collect_errors(mut self, errors: &mut Vec<ErrEntry>) {
        if self.handlers.is_empty() {
            return;
        }

        let vec = mem::take(&mut self.handlers);

        for h in vec.into_iter() {
            match h.2.join() {
                Ok(r) => {
                    if let Err(e) = r {
                        errors.push(ErrEntry {
                            index: h.0,
                            name: h.1,
                            err: e,
                        });
                    }
                }
                Err(e) => {
                    let s = if let Some(s) = e.downcast_ref::<&'static str>() {
                        s.to_string()
                    } else if let Some(s) = e.downcast_ref::<String>() {
                        s.clone()
                    } else if let Some(s) = e.downcast_ref::<Arc<&str>>() {
                        s.to_string()
                    } else if let Some(s) = e.downcast_ref::<Arc<str>>() {
                        s.to_string()
                    } else {
                        "Unknown panic payload".to_string()
                    };
                    let e = errs::Err::new(AsyncGroupError::ThreadPanicked(s));
                    errors.push(ErrEntry {
                        index: h.0,
                        name: h.1,
                        err: e,
                    });
                }
            }
        }
    }

    pub(crate) fn join_and_ignore_errors(mut self) {
        if self.handlers.is_empty() {
            return;
        }

        let vec = mem::take(&mut self.handlers);

        for h in vec.into_iter() {
            let _ = h.2.join();
        }
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_data;
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_join_and_collect_errors;
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_join_and_ignore_errors;

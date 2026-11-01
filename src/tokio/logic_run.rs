// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::tokio::{AsDataHub, DataHubError, ErrEntry, LogicErrAt, LogicRun};

use futures::FutureExt;
use std::future::Future;
use std::pin::Pin;
use std::{mem, panic};

impl<'a, A: AsDataHub> LogicRun<'a, A> {
    pub(crate) async fn new_async(hub: &'a mut A, nested: bool) -> LogicRun<'a, A> {
        if !nested {
            if let Err(err) = hub.as_hub().run.begin_async().await {
                return Self {
                    hub,
                    err: LogicErrAt::Begin { err },
                    index: 0,
                    nested,
                };
            }
        }

        Self {
            hub,
            err: LogicErrAt::Run {
                errors: Vec::with_capacity(0),
            },
            index: 0,
            nested,
        }
    }

    pub async fn run_async<F>(mut self, mut logic_fn: F) -> Self
    where
        for<'b> F: FnMut(&'b mut A) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        let index = self.index;
        self.index = index + 1;

        if self.hub.as_hub().run.is_abnormal() {
            return self;
        }

        match self.err {
            LogicErrAt::Run { ref mut errors } => {
                if errors.is_empty() {
                    match panic::AssertUnwindSafe(async { logic_fn(self.hub).await })
                        .catch_unwind()
                        .await
                    {
                        Ok(Ok(_)) => {}
                        Ok(Err(err)) => {
                            errors.push(ErrEntry {
                                index,
                                name: format!("LogicRun#run_async(logic-{})", index).into(),
                                err,
                            });
                        }
                        Err(payload) => {
                            self.hub.as_hub().run.abend();
                            panic::resume_unwind(payload);
                        }
                    }
                }
                self
            }
            _ => self,
        }
    }

    pub async fn run_force_async<F>(mut self, mut logic_fn: F) -> Self
    where
        for<'b> F: FnMut(&'b mut A) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        let index = self.index;
        self.index = index + 1;

        if self.hub.as_hub().run.is_abnormal() {
            return self;
        }

        match self.err {
            LogicErrAt::Run { ref mut errors } => {
                match panic::AssertUnwindSafe(async { logic_fn(self.hub).await })
                    .catch_unwind()
                    .await
                {
                    Ok(Ok(_)) => {}
                    Ok(Err(err)) => {
                        errors.push(ErrEntry {
                            index,
                            name: format!("LogicRun#run_force_async(logic-{})", index).into(),
                            err,
                        });
                    }
                    Err(payload) => {
                        self.hub.as_hub().run.abend();
                        panic::resume_unwind(payload);
                    }
                }
                self
            }
            _ => self,
        }
    }

    pub async fn run_or_block_async<F>(mut self, mut logic_fn: F) -> Self
    where
        for<'b> F: FnMut(&'b mut A) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        let index = self.index;
        self.index = index + 1;

        if self.hub.as_hub().run.is_abnormal() {
            return self;
        }

        match self.err {
            LogicErrAt::Run { ref mut errors } => {
                if errors.is_empty() {
                    match panic::AssertUnwindSafe(async { logic_fn(self.hub).await })
                        .catch_unwind()
                        .await
                    {
                        Ok(Ok(_)) => {}
                        Ok(Err(err)) => {
                            errors.push(ErrEntry {
                                index,
                                name: format!("LogicRun#run_or_block_async(logic-{})", index)
                                    .into(),
                                err,
                            });
                            self.err = LogicErrAt::Block {
                                errors: mem::take(errors),
                            };
                        }
                        Err(payload) => {
                            self.hub.as_hub().run.abend();
                            panic::resume_unwind(payload);
                        }
                    }
                }
                self
            }
            _ => self,
        }
    }

    pub fn end(self) -> errs::Result<()> {
        if self.hub.as_hub().run.is_abnormal() {
            return Ok(());
        }

        if !self.nested {
            match panic::catch_unwind(panic::AssertUnwindSafe(|| {
                self.hub.as_hub().run.end();
            })) {
                Ok(_) => {}
                Err(payload) => {
                    self.hub.as_hub().run.abend();
                    panic::resume_unwind(payload);
                }
            }
        }

        match self.err {
            LogicErrAt::Begin { err } => Err(err),
            LogicErrAt::Run { errors } => {
                if errors.is_empty() {
                    Ok(())
                } else {
                    Err(errs::Err::new(DataHubError::FailToRunLogic { errors }))
                }
            }
            LogicErrAt::Block { errors } => {
                Err(errs::Err::new(DataHubError::FailToRunLogic { errors }))
            }
        }
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test;
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_panic;

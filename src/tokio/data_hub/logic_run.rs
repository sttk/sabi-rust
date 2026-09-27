// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::tokio::{DataHub, DataHubError, ErrEntry, LogicErrAt, LogicRun};

use std::future::Future;
use std::mem;
use std::pin::Pin;

impl<'a, T> LogicRun<'a, T> {
    pub(crate) async fn new_async(hub: &'a mut DataHub<T>, nested: bool) -> LogicRun<'a, T> {
        if !nested {
            if let Err(err) = hub.run.begin_async().await {
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

    pub async fn run_aync<F>(mut self, mut logic_fn: F) -> Self
    where
        for<'b> F: FnMut(
            &'b mut DataHub<T>,
        ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        let index = self.index;
        self.index = index + 1;

        match self.err {
            LogicErrAt::Run { ref mut errors } => {
                if errors.is_empty() {
                    if let Err(err) = logic_fn(self.hub).await {
                        errors.push(ErrEntry {
                            index,
                            name: format!("LogicRun#run_async(logic-{})", index).into(),
                            err,
                        });
                    }
                }
                self
            }
            _ => self,
        }
    }

    pub async fn run_force_async<F>(mut self, mut logic_fn: F) -> Self
    where
        for<'b> F: FnMut(
            &'b mut DataHub<T>,
        ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        let index = self.index;
        self.index = index + 1;

        match self.err {
            LogicErrAt::Run { ref mut errors } => {
                if let Err(err) = logic_fn(self.hub).await {
                    errors.push(ErrEntry {
                        index,
                        name: format!("LogicRun#run_async(logic-{})", index).into(),
                        err,
                    });
                }
                self
            }
            _ => self,
        }
    }

    pub async fn run_or_block_aync<F>(mut self, mut logic_fn: F) -> Self
    where
        for<'b> F: FnMut(
            &'b mut DataHub<T>,
        ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        let index = self.index;
        self.index = index + 1;

        match self.err {
            LogicErrAt::Run { ref mut errors } => {
                if errors.is_empty() {
                    if let Err(err) = logic_fn(self.hub).await {
                        errors.push(ErrEntry {
                            index,
                            name: format!("LogicRun#run_async(logic-{})", index).into(),
                            err,
                        });
                        self.err = LogicErrAt::Block {
                            errors: mem::take(errors),
                        };
                    }
                }
                self
            }
            _ => self,
        }
    }

    pub fn end(self) -> errs::Result<()> {
        if !self.nested {
            self.hub.run.end();
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

// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::tokio::{DataHub, DataHubError, ErrEntry, LogicErrAt, LogicTxn};

use std::future::Future;
use std::mem;
use std::pin::Pin;

impl<'a, T> LogicTxn<'a, T> {
    pub(crate) async fn new_async(hub: &'a mut DataHub<T>) -> LogicTxn<'a, T> {
        if let Err(err) = hub.run.begin_async().await {
            Self {
                hub,
                err: LogicErrAt::Begin { err },
                index: 0,
            }
        } else {
            Self {
                hub,
                err: LogicErrAt::Run {
                    errors: Vec::with_capacity(0),
                },
                index: 0,
            }
        }
    }

    pub async fn run_async<F>(mut self, mut logic_fn: F) -> Self
    where
        for<'b> F: FnMut(
            &'b mut DataHub<T>,
        ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        let index = self.index;
        self.index += 1;

        match self.err {
            LogicErrAt::Run { ref mut errors } => {
                if errors.is_empty() {
                    if let Err(err) = logic_fn(self.hub).await {
                        errors.push(ErrEntry {
                            index,
                            name: format!("LogicTxn#run_async(logic-{})", index).into(),
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
        self.index += 1;

        match self.err {
            LogicErrAt::Run { ref mut errors } => {
                if let Err(err) = logic_fn(self.hub).await {
                    errors.push(ErrEntry {
                        index,
                        name: format!("LogicTxn#run_force_async(logic-{})", index).into(),
                        err,
                    });
                }
                self
            }
            _ => self,
        }
    }

    pub async fn run_or_block_async<F>(mut self, mut logic_fn: F) -> Self
    where
        for<'b> F: FnMut(
            &'b mut DataHub<T>,
        ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        let index = self.index;
        self.index += 1;

        match self.err {
            LogicErrAt::Run { ref mut errors } => {
                if errors.is_empty() {
                    if let Err(err) = logic_fn(self.hub).await {
                        errors.push(ErrEntry {
                            index,
                            name: format!("LogicTxn#run_or_block_async(logic-{})", index).into(),
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

    pub async fn end_txn_async(self) -> errs::Result<()> {
        match self.err {
            LogicErrAt::Begin { err } => {
                self.hub.run.end();
                Err(err)
            }
            LogicErrAt::Run { errors } => {
                let mut reports = self.hub.run.new_failure_reports();
                if errors.is_empty() {
                    let result = self.hub.run.commit_async(&mut reports).await;
                    if result.is_err() {
                        self.hub.run.rollback_async(reports).await;
                    }
                    self.hub.run.end();
                    result
                } else {
                    self.hub.run.rollback_async(reports).await;
                    self.hub.run.end();
                    Err(errs::Err::new(DataHubError::FailToRunLogic { errors }))
                }
            }
            LogicErrAt::Block { errors } => {
                let reports = self.hub.run.new_failure_reports();
                self.hub.run.rollback_async(reports).await;
                self.hub.run.end();
                Err(errs::Err::new(DataHubError::FailToRunLogic { errors }))
            }
        }
    }
}

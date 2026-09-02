// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use super::{DataConn, DataHub, DataSrc, Runner, TxnDataHub};
use crate::ErrEntry;

use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Debug)]
pub enum TxnError {
    FailToRunLogics { errors: Vec<ErrEntry> },
}

impl TxnDataHub {
    pub fn new(hub: DataHub) -> Self {
        Self { hub }
    }

    pub fn uses<S, C>(&mut self, name: impl Into<Arc<str>>, ds: S)
    where
        S: DataSrc<C> + 'static,
        C: DataConn + 'static,
    {
        self.hub.uses(name, ds)
    }

    pub fn disuses(&mut self, name: impl AsRef<str>) {
        self.hub.disuses(name)
    }

    pub async fn run_async<F>(&mut self, logic_fn: F) -> errs::Result<()>
    where
        for<'a> F:
            FnMut(&'a mut DataHub) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'a>>,
    {
        self.hub.run_async(logic_fn).await
    }

    pub async fn start_async(&mut self) -> Runner<'_> {
        self.hub.start_async().await
    }

    pub async fn txn_async<F>(&mut self, mut logic_fn: F) -> errs::Result<()>
    where
        for<'a> F:
            FnMut(&'a mut DataHub) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'a>>,
    {
        let mut r = self.hub.begin_async().await;
        if r.is_ok() {
            r = logic_fn(&mut self.hub).await;
        }

        let mut reports = self.hub.new_failure_reports();

        if r.is_ok() {
            r = self.hub.commit_async(&mut reports).await;
        }
        if r.is_err() {
            self.hub.rollback_async(reports).await;
        }

        self.hub.end();
        r
    }

    pub async fn begin_txn_async(&mut self) -> Txn<'_> {
        Txn::new_async(self).await
    }
}

enum TxnErrAt {
    Begin { err: errs::Err },
    Run { errors: Vec<ErrEntry> },
    Block { errors: Vec<ErrEntry> },
}

pub struct Txn<'a> {
    hub: &'a mut DataHub,
    err: TxnErrAt,
    index: usize,
}

impl<'a> Txn<'a> {
    pub async fn new_async(txn_hub: &'a mut TxnDataHub) -> Txn<'a> {
        if let Err(err) = txn_hub.hub.begin_async().await {
            Self {
                hub: &mut txn_hub.hub,
                err: TxnErrAt::Begin { err },
                index: 0,
            }
        } else {
            Self {
                hub: &mut txn_hub.hub,
                err: TxnErrAt::Run {
                    errors: Vec::with_capacity(0),
                },
                index: 0,
            }
        }
    }

    pub async fn run_async<F>(mut self, mut logic_fn: F) -> Self
    where
        for<'b> F:
            FnMut(&'b mut DataHub) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        let index = self.index;
        self.index += 1;

        match self.err {
            TxnErrAt::Run { ref mut errors } => {
                if errors.is_empty() {
                    if let Err(err) = logic_fn(self.hub).await {
                        errors.push(ErrEntry {
                            index,
                            name: format!("Txn#run_async(logic-{})", index).into(),
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
        for<'b> F:
            FnMut(&'b mut DataHub) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        let index = self.index;
        self.index += 1;

        match self.err {
            TxnErrAt::Run { ref mut errors } => {
                if let Err(err) = logic_fn(self.hub).await {
                    errors.push(ErrEntry {
                        index,
                        name: format!("Txn#run_force_async(logic-{})", index).into(),
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
        for<'b> F:
            FnMut(&'b mut DataHub) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        let index = self.index;
        self.index += 1;

        match self.err {
            TxnErrAt::Run { ref mut errors } => {
                if errors.is_empty() {
                    if let Err(err) = logic_fn(self.hub).await {
                        errors.push(ErrEntry {
                            index,
                            name: format!("Txn#run_on_block_async(logic-{})", index).into(),
                            err,
                        });
                        self.err = TxnErrAt::Block {
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
            TxnErrAt::Begin { err } => {
                self.hub.end();
                Err(err)
            }
            TxnErrAt::Run { errors } => {
                let mut reports = self.hub.new_failure_reports();
                if errors.is_empty() {
                    let result = self.hub.commit_async(&mut reports).await;
                    if result.is_err() {
                        self.hub.rollback_async(reports).await;
                    }
                    self.hub.end();
                    result
                } else {
                    self.hub.rollback_async(reports).await;
                    self.hub.end();
                    Err(errs::Err::new(TxnError::FailToRunLogics { errors }))
                }
            }
            TxnErrAt::Block { errors } => {
                let reports = self.hub.new_failure_reports();
                self.hub.rollback_async(reports).await;
                self.hub.end();
                Err(errs::Err::new(TxnError::FailToRunLogics { errors }))
            }
        }
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod tests_of_txn_data_hub {
    use super::*;
    use crate::{AsyncGroup, DataAcc, DataConn, DataSrc, TxnFailureReport};
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};

    #[derive(PartialEq, Clone, Copy)]
    enum Failure {
        None,
        Setup,
        PreCommit,
        Commit,
        PostCommit,
        Rollback,
    }
}

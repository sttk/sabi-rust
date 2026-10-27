// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(unused_features)]

mod async_group;
mod data_conn;
mod non_null;
mod txn_failure;

pub use async_group::AsyncGroupError;
pub use data_conn::DataConnError;

use std::collections::HashMap;
use std::sync::Arc;
use std::{any, cell, marker, ptr, thread};

#[derive(Debug)]
pub struct ErrEntry {
    pub index: usize,
    pub name: Arc<str>,
    pub err: errs::Err,
}

pub struct AsyncGroup {
    handlers: Vec<(usize, Arc<str>, thread::JoinHandle<errs::Result<()>>)>,
    index: usize,
    name: Arc<str>,
}

#[derive(Debug)]
pub enum TxnFailureCause {
    NoneByCommitted,
    NoneByUncommitted,
    LogicFailure(errs::Err),
    CommitFailure(errs::Err),
    PostCommitFailure(errs::Err),
}

#[derive(Debug)]
pub enum TxnFailureRollback {
    NoneByRolledBack,
    NoneByNotRolledBack,
    RollbackFailure(errs::Err),
}

#[derive(Debug, PartialEq)]
pub enum TxnFailureRecovery {
    NoActionRequired,
    RerunLogicAndCommit,
    ResolveCauseThenRerunLogicAndCommit,
    ResolveCauseThenRerunPostCommit,
    ResolveCauseAndInconsistency,
    InvestigateBecauseImpossible,
    ManualRollbackRequired,
}

#[derive(Debug)]
pub struct TxnFailureReport {
    pub data_conn_name: Arc<str>,
    pub data_conn_type: &'static str,
    pub cause: TxnFailureCause,
    pub rollback: TxnFailureRollback,
}

struct SendSyncNonNull<T: Send + Sync> {
    non_null_ptr: ptr::NonNull<T>,
    _phantom: marker::PhantomData<cell::Cell<T>>,
}

pub trait DataConn {
    fn commit(&mut self, ag: &mut AsyncGroup) -> errs::Result<()>;

    #[cfg_attr(coverage_nightly, coverage(off))]
    fn pre_commit(&mut self, ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }

    #[cfg_attr(coverage_nightly, coverage(off))]
    fn post_commit(&mut self, ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }

    fn is_committed(&self) -> bool;

    fn rollback(&mut self, ag: &mut AsyncGroup) -> errs::Result<()>;

    #[cfg_attr(coverage_nightly, coverage(off))]
    fn on_txn_failure(&mut self, ag: &mut AsyncGroup, reports: &[TxnFailureReport]) {}

    fn close(&mut self);
}

struct NoopDataConn {}

#[cfg_attr(coverage_nightly, coverage(off))]
impl DataConn for NoopDataConn {
    fn commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }
    fn is_committed(&self) -> bool {
        false
    }
    fn rollback(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }
    fn close(&mut self) {}
}

#[repr(C)]
struct DataConnContainer<C = NoopDataConn>
where
    C: DataConn + 'static,
{
    drop_fn: fn(*const DataConnContainer),
    is_fn: fn(any::TypeId) -> bool,
    type_fn: fn() -> &'static str,
    commit_fn: fn(*const DataConnContainer, &mut AsyncGroup) -> errs::Result<()>,
    pre_commit_fn: fn(*const DataConnContainer, &mut AsyncGroup) -> errs::Result<()>,
    post_commit_fn: fn(*const DataConnContainer, &mut AsyncGroup) -> errs::Result<()>,
    is_committed_fn: fn(*const DataConnContainer) -> bool,
    rollback_fn: fn(*const DataConnContainer, &mut AsyncGroup) -> errs::Result<()>,
    on_txn_failure_fn: fn(*const DataConnContainer, &mut AsyncGroup, &[TxnFailureReport]),
    close_fn: fn(*const DataConnContainer),

    name: Arc<str>,
    data_conn: Box<C>,
}

struct DataConnManager {
    vec: Vec<Option<SendSyncNonNull<DataConnContainer>>>,
    index_map: HashMap<Arc<str>, usize>,
    ordered_names: Box<[Arc<str>]>,
    committed: bool,
}

// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(unused_features)]

mod async_group;
mod data_conn;
mod data_hub;
mod data_src;
mod non_null;
mod txn_failure;

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_commons;

pub use async_group::AsyncGroupError;
pub use data_conn::DataConnError;
pub use data_hub::DataHubError;
pub use data_src::DataSrcError;

pub use data_src::{create_static_data_src_container, setup, setup_with_order, uses};

#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
#[cfg(feature = "tokio")]
pub mod tokio;

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
    _index: usize,
    _name: Arc<str>,
}

struct SendSyncNonNull<T: Send + Sync> {
    non_null_ptr: ptr::NonNull<T>,
    _phantom: marker::PhantomData<cell::Cell<T>>,
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

#[allow(unused_variables)]
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
    committed: bool,
}

pub trait DataSrc<C>
where
    C: DataConn + 'static,
{
    fn setup(&mut self, ag: &mut AsyncGroup) -> errs::Result<()>;

    fn close(&mut self);

    fn create_data_conn(&mut self) -> errs::Result<Box<C>>;
}

struct NoopDataSrc {}

#[cfg_attr(coverage_nightly, coverage(off))]
impl DataSrc<NoopDataConn> for NoopDataSrc {
    fn setup(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }
    fn close(&mut self) {}
    fn create_data_conn(&mut self) -> errs::Result<Box<NoopDataConn>> {
        Ok(Box::new(NoopDataConn {}))
    }
}

#[repr(C)]
struct DataSrcContainer<S = NoopDataSrc, C = NoopDataConn>
where
    S: DataSrc<C>,
    C: DataConn + 'static,
{
    drop_fn: fn(*const DataSrcContainer),
    setup_fn: fn(*const DataSrcContainer, &mut AsyncGroup) -> errs::Result<()>,
    close_fn: fn(*const DataSrcContainer),
    create_data_conn_fn: fn(*const DataSrcContainer) -> errs::Result<Box<DataConnContainer<C>>>,
    is_data_conn_fn: fn(any::TypeId) -> bool,

    local: bool,
    name: Arc<str>,
    data_src: S,
}

struct DataSrcManager {
    vec_unready: Vec<SendSyncNonNull<DataSrcContainer>>,
    vec_ready: Vec<SendSyncNonNull<DataSrcContainer>>,
    local: bool,
}

pub struct AutoShutdown {}

#[doc(hidden)]
pub struct StaticDataSrcContainer {
    ssnnptr: SendSyncNonNull<DataSrcContainer>,
}

#[doc(hidden)]
pub struct StaticDataSrcRegistration {
    factory: fn() -> StaticDataSrcContainer,
}

pub struct DataHub<T> {
    run: DataRun,
    _phantom: marker::PhantomData<T>,
}

struct DataRun {
    local_data_src_manager: DataSrcManager,
    data_src_map: HashMap<Arc<str>, (bool, usize)>,
    data_conn_manager: DataConnManager,
    fixed: bool,
}

pub trait DataAcc {
    type D: any::Any;

    fn get_data_conn<C: DataConn + 'static>(&mut self, name: &str) -> errs::Result<&mut C>;

    fn run<F>(&mut self, logic_fn: F) -> errs::Result<()>
    where
        F: FnMut(&mut DataHub<Self::D>) -> errs::Result<()>;

    fn start(&mut self) -> LogicRun<'_, Self::D>;
}

pub struct LogicRun<'a, T> {
    hub: &'a mut DataHub<T>,
    err: LogicErrAt,
    index: usize,
    nested: bool,
}

pub struct LogicTxn<'a, T> {
    hub: &'a mut DataHub<T>,
    err: LogicErrAt,
    index: usize,
}

enum LogicErrAt {
    Begin { err: errs::Err },
    Run { errors: Vec<ErrEntry> },
    Block { errors: Vec<ErrEntry> },
}

pub struct LogicData<T> {
    hub: DataHub<T>,
}

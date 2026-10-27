// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(unused_features)]

mod async_group;
mod txn_failure;

pub use async_group::AsyncGroupError;

use std::sync::Arc;
use std::thread;

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

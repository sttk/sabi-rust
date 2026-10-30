// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

mod async_group;
mod data_conn;

pub use data_conn::DataConnError;

use crate::{ErrEntry, SendSyncNonNull, TxnFailureReport};

use std::any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[allow(clippy::type_complexity)]
pub struct AsyncGroup {
    attrs: Vec<(usize, Arc<str>)>,
    tasks: Vec<Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'static>>>,
    index: usize,
    name: Arc<str>,
}

#[allow(async_fn_in_trait)]
pub trait DataConn {
    fn commit_async(
        &mut self,
        ag: &mut AsyncGroup,
    ) -> impl Future<Output = errs::Result<()>> + Send;

    #[allow(unused_variables)] // ag
    #[cfg_attr(coverage_nightly, coverage(off))]
    fn pre_commit_async(
        &mut self,
        ag: &mut AsyncGroup,
    ) -> impl Future<Output = errs::Result<()>> + Send {
        async { Ok(()) }
    }

    #[allow(unused_variables)] // ag
    #[cfg_attr(coverage_nightly, coverage(off))]
    fn post_commit_async(
        &mut self,
        ag: &mut AsyncGroup,
    ) -> impl Future<Output = errs::Result<()>> + Send {
        async { Ok(()) }
    }

    fn is_committed(&self) -> bool;

    fn rollback_async(
        &mut self,
        ag: &mut AsyncGroup,
    ) -> impl Future<Output = errs::Result<()>> + Send;

    #[allow(unused_variables)] // ag
    #[cfg_attr(coverage_nightly, coverage(off))]
    fn on_txn_failure_async(
        &mut self,
        ag: &mut AsyncGroup,
        reports: Arc<[TxnFailureReport]>,
    ) -> impl Future<Output = ()> + Send {
        async {}
    }

    fn close(&mut self);
}

pub(crate) struct NoopDataConn {}

#[cfg_attr(coverage_nightly, coverage(off))]
impl DataConn for NoopDataConn {
    async fn commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }
    fn is_committed(&self) -> bool {
        false
    }
    async fn rollback_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }
    fn close(&mut self) {}
}

#[allow(clippy::type_complexity)]
#[repr(C)]
pub(crate) struct DataConnContainer<C = NoopDataConn>
where
    C: DataConn + 'static,
{
    drop_fn: fn(*const DataConnContainer),
    is_fn: fn(any::TypeId) -> bool,
    type_fn: fn() -> &'static str,

    commit_fn: for<'ag> fn(
        *const DataConnContainer,
        &'ag mut AsyncGroup,
    ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'ag>>,

    pre_commit_fn: for<'ag> fn(
        *const DataConnContainer,
        &'ag mut AsyncGroup,
    ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'ag>>,

    post_commit_fn: for<'ag> fn(
        *const DataConnContainer,
        &'ag mut AsyncGroup,
    )
        -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'ag>>,

    is_committed_fn: fn(*const DataConnContainer) -> bool,

    rollback_fn: for<'ag> fn(
        *const DataConnContainer,
        &'ag mut AsyncGroup,
    ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'ag>>,

    on_txn_failure_fn: for<'ag> fn(
        *const DataConnContainer,
        &'ag mut AsyncGroup,
        Arc<[TxnFailureReport]>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'ag>>,

    close_fn: fn(*const DataConnContainer),

    name: Arc<str>,
    data_conn: Box<C>,
}

pub(crate) struct DataConnManager {
    vec: Vec<Option<SendSyncNonNull<DataConnContainer>>>,
    index_map: HashMap<Arc<str>, usize>,
    ordered_names: Box<[Arc<str>]>,
    committed: bool,
}

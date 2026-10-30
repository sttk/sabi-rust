// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

mod async_group;
mod data_acc;
mod data_conn;
mod data_hub;
mod data_src;
mod global_setup;
mod logic_run;
mod logic_runner;
mod logic_txn;

pub use data_conn::DataConnError;
pub use data_hub::DataHubError;
pub use data_src::DataSrcError;

pub use global_setup::create_static_data_src_container;

use crate::{ErrEntry, LogicErrAt, SendSyncNonNull, TxnFailureReport};

use std::any;
use std::collections::HashMap;
use std::future::Future;
use std::marker;
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

#[trait_variant::make(Send)]
#[allow(unused_variables)] // for rustdoc
pub trait DataSrc<C>
where
    C: DataConn + 'static,
{
    async fn setup_async(&mut self, ag: &mut AsyncGroup) -> errs::Result<()>;

    fn close(&mut self);

    async fn create_data_conn_async(&mut self) -> errs::Result<Box<C>>;
}

pub(crate) struct NoopDataSrc {}

#[cfg_attr(coverage_nightly, coverage(off))]
impl DataSrc<NoopDataConn> for NoopDataSrc {
    async fn setup_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }
    fn close(&mut self) {}
    async fn create_data_conn_async(&mut self) -> errs::Result<Box<NoopDataConn>> {
        Ok(Box::new(NoopDataConn {}))
    }
}

#[allow(clippy::type_complexity)]
#[repr(C)]
pub(crate) struct DataSrcContainer<S = NoopDataSrc, C = NoopDataConn>
where
    S: DataSrc<C>,
    C: DataConn + 'static,
{
    drop_fn: fn(*const DataSrcContainer),
    close_fn: fn(*const DataSrcContainer),
    is_data_conn_fn: fn(any::TypeId) -> bool,

    setup_fn: for<'ag> fn(
        *const DataSrcContainer,
        &'ag mut AsyncGroup,
    ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'ag>>,

    create_data_conn_fn: fn(
        *const DataSrcContainer,
    ) -> Pin<
        Box<dyn Future<Output = errs::Result<Box<DataConnContainer<C>>>> + Send + 'static>,
    >,

    local: bool,
    name: Arc<str>,
    data_src: S,
}

pub(crate) struct DataSrcManager {
    vec_unready: Vec<SendSyncNonNull<DataSrcContainer>>,
    vec_ready: Vec<SendSyncNonNull<DataSrcContainer>>,
    local: bool,
}

#[must_use = "global DataSrcs are closed when this value is dropped; bind it to a named variable"]
pub struct AutoShutdown {}

#[doc(hidden)]
pub struct StaticDataSrcContainer {
    ssnnptr: SendSyncNonNull<DataSrcContainer>,
}

#[doc(hidden)]
pub struct StaticDataSrcRegistration {
    pub(crate) factory: fn() -> StaticDataSrcContainer,
}

pub async fn setup_async() -> errs::Result<AutoShutdown> {
    global_setup::setup_async().await
}

pub async fn setup_with_order_async(names: &'static [&str]) -> errs::Result<AutoShutdown> {
    global_setup::setup_with_order_async(names).await
}

#[inline]
pub async fn uses_async<S, C>(name: impl Into<Arc<str>>, ds: S) -> errs::Result<()>
where
    S: DataSrc<C> + 'static,
    C: DataConn + 'static,
{
    global_setup::uses_async(name, ds).await
}

pub fn uses<S, C>(name: impl Into<Arc<str>>, ds: S) -> errs::Result<()>
where
    S: DataSrc<C> + 'static,
    C: DataConn + 'static,
{
    global_setup::uses(name, ds)
}

#[macro_export]
#[doc(hidden)]
macro_rules! _uses_for_async {
    ($name:tt, $data_src:expr) => {
        const _: () = {
            inventory::submit! {
                $crate::tokio::StaticDataSrcRegistration::new(|| {
                    $crate::tokio::create_static_data_src_container($name, $data_src)
                })
            }
        };
    };
}

#[doc(inline)]
pub use crate::_uses_for_async as uses;

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

pub trait AsDataHub: Send {
    type D: 'static;

    fn as_hub(&mut self) -> &mut DataHub<Self::D>;
}

pub trait DataAcc: AsDataHub + Sized {
    #[allow(async_fn_in_trait)]
    fn get_data_conn_async<C: DataConn + 'static>(
        &mut self,
        name: &str,
    ) -> impl Future<Output = errs::Result<&mut C>> + Send;

    #[allow(async_fn_in_trait)]
    async fn run_async<F>(&mut self, logic_fn: F) -> errs::Result<()>
    where
        for<'b> F:
            FnMut(&'b mut Self) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>;

    #[allow(async_fn_in_trait)]
    async fn start_async(&mut self) -> LogicRun<'_, Self>;
}

#[must_use = "call `end()` to finish the chain and release the data connections"]
pub struct LogicRun<'a, A> {
    hub: &'a mut A,
    err: LogicErrAt,
    index: usize,
    nested: bool,
}

#[must_use = "call `end_txn_async()` to commit or roll back the transaction"]
pub struct LogicTxn<'a, T> {
    hub: &'a mut DataHub<T>,
    err: LogicErrAt,
    index: usize,
}

pub struct LogicRunner<T> {
    hub: DataHub<T>,
}

#[doc(inline)]
pub use crate::_logic as logic;

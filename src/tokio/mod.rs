// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

//! Asynchronous APIs for the Tokio runtime.
//!
//! This module provides asynchronous counterparts to the core `sabi` components, allowing
//! non-blocking data access and transactional control with Tokio.
//!
//! ## Key Types
//!
//! - [`DataSrc`]: Asynchronous factory for creating [`DataConn`] instances.
//! - [`DataConn`]: Asynchronous connection and transaction boundaries.
//! - [`DataHub`]: Central hub managing asynchronous data sources and connections.
//! - [`DataAcc`]: Base trait for asynchronous data access required by logic.
//! - [`LogicRunner`]: Runner for executing asynchronous logic functions.
//! - [`logic!`]: Macro converting an async function into the required pinned closure type.
//!
//! ## Example
//!
//! ```rust,ignore
//! use sabi::tokio::{setup_async, uses, DataAcc, DataHub, LogicRunner, logic};
//!
//! pub trait MyAsyncData: Send {
//!     async fn get_text_async(&mut self) -> errs::Result<String>;
//! }
//!
//! async fn my_async_logic(data: &mut (impl MyAsyncData + Send)) -> errs::Result<()> {
//!     let text = data.get_text_async().await?;
//!     println!("{text}");
//!     Ok(())
//! }
//! ```

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

/// A manager for coordinating concurrent asynchronous tasks and collecting errors.
///
/// Holds boxed futures and awaits all of them concurrently when joined.
#[allow(clippy::type_complexity)]
pub struct AsyncGroup {
    attrs: Vec<(usize, Arc<str>)>,
    tasks: Vec<Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'static>>>,
    index: usize,
    name: Arc<str>,
}

/// Asynchronous trait representing a connection to a data store, providing async transaction
/// boundaries (commit, rollback, close, etc.).
#[allow(async_fn_in_trait)]
pub trait DataConn {
    /// Commits the transaction on this data connection asynchronously.
    ///
    /// # Arguments
    ///
    /// - `ag`: Mutable reference to [`AsyncGroup`] for registering concurrent async tasks.
    ///
    /// # Returns
    ///
    /// A future resolving to:
    /// - `Ok(())` on successful commit.
    /// - `Err(errs::Err)` if the commit fails.
    fn commit_async(
        &mut self,
        ag: &mut AsyncGroup,
    ) -> impl Future<Output = errs::Result<()>> + Send;

    /// Pre-commit phase executed asynchronously immediately before committing.
    ///
    /// Default implementation is a no-op returning `Ok(())`.
    ///
    /// # Arguments
    ///
    /// - `ag`: Mutable reference to [`AsyncGroup`] for registering concurrent async tasks.
    ///
    /// # Returns
    ///
    /// A future resolving to:
    /// - `Ok(())` on success.
    /// - `Err(errs::Err)` if the pre-commit operation fails.
    #[allow(unused_variables)] // ag
    #[cfg_attr(coverage_nightly, coverage(off))]
    fn pre_commit_async(
        &mut self,
        ag: &mut AsyncGroup,
    ) -> impl Future<Output = errs::Result<()>> + Send {
        async { Ok(()) }
    }

    /// Post-commit phase executed asynchronously immediately after committing.
    ///
    /// Default implementation is a no-op returning `Ok(())`.
    ///
    /// # Arguments
    ///
    /// - `ag`: Mutable reference to [`AsyncGroup`] for registering concurrent async tasks.
    ///
    /// # Returns
    ///
    /// A future resolving to:
    /// - `Ok(())` on success.
    /// - `Err(errs::Err)` if the post-commit operation fails.
    #[allow(unused_variables)] // ag
    #[cfg_attr(coverage_nightly, coverage(off))]
    fn post_commit_async(
        &mut self,
        ag: &mut AsyncGroup,
    ) -> impl Future<Output = errs::Result<()>> + Send {
        async { Ok(()) }
    }

    /// Returns whether this data connection has been committed.
    ///
    /// # Returns
    ///
    /// `true` if committed, `false` otherwise.
    fn is_committed(&self) -> bool;

    /// Rolls back the transaction on this data connection asynchronously.
    ///
    /// # Arguments
    ///
    /// - `ag`: Mutable reference to [`AsyncGroup`] for registering concurrent async tasks.
    ///
    /// # Returns
    ///
    /// A future resolving to:
    /// - `Ok(())` on successful rollback.
    /// - `Err(errs::Err)` if the rollback fails.
    fn rollback_async(
        &mut self,
        ag: &mut AsyncGroup,
    ) -> impl Future<Output = errs::Result<()>> + Send;

    /// Callback invoked asynchronously when a transaction fails.
    ///
    /// Receives failure reports for all participating data connections. Default implementation
    /// does nothing.
    ///
    /// # Arguments
    ///
    /// - `ag`: Mutable reference to [`AsyncGroup`] for registering concurrent async tasks.
    /// - `reports`: Arc slice of [`TxnFailureReport`] describing failure causes and outcomes.
    #[allow(unused_variables)] // ag
    #[cfg_attr(coverage_nightly, coverage(off))]
    fn on_txn_failure_async(
        &mut self,
        ag: &mut AsyncGroup,
        reports: Arc<[TxnFailureReport]>,
    ) -> impl Future<Output = ()> + Send {
        async {}
    }

    /// Closes the data connection and releases its underlying resources normally.
    fn close(&mut self);

    /// Abnormally terminates the data connection, typically invoked upon a panic.
    ///
    /// Default implementation calls [`DataConn::close`].
    #[cfg_attr(coverage_nightly, coverage(off))]
    fn abend(&mut self) {
        self.close();
    }
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
    abend_fn: fn(*const DataConnContainer),

    name: Arc<str>,
    data_conn: Box<C>,
}

pub(crate) struct DataConnManager {
    vec: Vec<Option<SendSyncNonNull<DataConnContainer>>>,
    index_map: HashMap<Arc<str>, usize>,
    ordered_names: Box<[Arc<str>]>,
    committed: bool,
}

/// Asynchronous trait representing a data source producing and managing [`DataConn`] instances.
///
/// # Type Parameters
///
/// - `C`: The concrete type of [`DataConn`] produced by this data source
///   ([`DataConn`] + `'static`).
#[trait_variant::make(Send)]
#[allow(unused_variables)] // for rustdoc
pub trait DataSrc<C>
where
    C: DataConn + 'static,
{
    /// Sets up and initializes the data source asynchronously.
    ///
    /// # Arguments
    ///
    /// - `ag`: Mutable reference to [`AsyncGroup`] for registering concurrent async setup tasks.
    ///
    /// # Returns
    ///
    /// - `Ok(())` on successful initialization.
    /// - `Err(errs::Err)` if setup fails.
    async fn setup_async(&mut self, ag: &mut AsyncGroup) -> errs::Result<()>;

    /// Closes the data source and releases its resources.
    fn close(&mut self);

    /// Creates a new asynchronous data connection instance.
    ///
    /// # Returns
    ///
    /// - `Ok(Box<C>)` containing the new connection on success.
    /// - `Err(errs::Err)` if connection creation fails.
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
    is_abnormal: bool,
}

/// A guard struct that automatically closes global data sources when dropped in an async context.
///
/// # Notes
///
/// Bind this to a named variable to prevent immediate dropping:
///
/// ```rust,ignore
/// let _auto_shutdown = sabi::tokio::setup_async().await?;
/// ```
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

/// Initializes and sets up all registered global data sources asynchronously.
///
/// # Returns
///
/// - `Ok(AutoShutdown)` on success.
/// - `Err(errs::Err)` on failure (e.g. if already set up or a data source fails).
///
/// # Notes
///
/// Ensure the returned [`AutoShutdown`] is bound to a named variable and kept alive.
///
/// # Examples
///
/// ```rust,ignore
/// let _auto_shutdown = sabi::tokio::setup_async().await?;
/// ```
pub async fn setup_async() -> errs::Result<AutoShutdown> {
    global_setup::setup_async().await
}

/// Initializes and sets up all registered global data sources asynchronously in the
/// specified order.
///
/// # Arguments
///
/// - `names`: Slice of data source names determining the setup order.
///
/// # Returns
///
/// - `Ok(AutoShutdown)` on success.
/// - `Err(errs::Err)` on failure.
pub async fn setup_with_order_async(names: &'static [&str]) -> errs::Result<AutoShutdown> {
    global_setup::setup_with_order_async(names).await
}

/// Registers a global data source asynchronously, acquiring the lock asynchronously.
///
/// # Type Parameters
///
/// - `S`: The data source type ([`DataSrc<C>`] + `'static`).
/// - `C`: The data connection type ([`DataConn`] + `'static`).
///
/// # Arguments
///
/// - `name`: The identifier name for the data source.
/// - `ds`: The data source instance to register.
///
/// # Returns
///
/// - `Ok(())` on successful registration.
/// - `Err(errs::Err)` if registration fails.
#[inline]
pub async fn uses_async<S, C>(name: impl Into<Arc<str>>, ds: S) -> errs::Result<()>
where
    S: DataSrc<C> + 'static,
    C: DataConn + 'static,
{
    global_setup::uses_async(name, ds).await
}

/// Registers a global data source synchronously using a non-blocking lock attempt.
///
/// Useful during application startup where no lock contention exists.
///
/// # Type Parameters
///
/// - `S`: The data source type ([`DataSrc<C>`] + `'static`).
/// - `C`: The data connection type ([`DataConn`] + `'static`).
///
/// # Arguments
///
/// - `name`: The identifier name for the data source.
/// - `ds`: The data source instance to register.
///
/// # Returns
///
/// - `Ok(())` on successful registration.
/// - `Err(errs::Err)` if registration fails or lock cannot be acquired immediately.
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

/// Statically registers a global data source for async use at compile time via `inventory`.
///
/// # Arguments
///
/// - `$name`: Identifier name for the data source.
/// - `$data_src`: Expression evaluating to the data source instance.
#[doc(inline)]
pub use crate::_uses_for_async as uses;

/// Central hub managing asynchronous data sources and connections.
///
/// # Type Parameters
///
/// - `T`: Marker type defining the data access scope or grouping.
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

/// Trait providing access to the underlying asynchronous [`DataHub`].
pub trait AsDataHub: Send {
    /// The marker type parameter of the associated [`DataHub`].
    type D: 'static;

    /// Returns a mutable reference to the underlying [`DataHub`].
    fn as_hub(&mut self) -> &mut DataHub<Self::D>;
}

/// Asynchronous base trait defining and aggregating data access capabilities for logic.
pub trait DataAcc: AsDataHub + Sized {
    /// Retrieves a mutable reference to an async data connection of type `C` by name.
    ///
    /// # Type Parameters
    ///
    /// - `C`: The expected connection type ([`DataConn`] + `'static`).
    ///
    /// # Arguments
    ///
    /// - `name`: The identifier name of the data connection.
    ///
    /// # Returns
    ///
    /// A future resolving to `Ok(&mut C)` on success, or `Err(errs::Err)` on failure.
    #[allow(async_fn_in_trait)]
    fn get_data_conn_async<C: DataConn + 'static>(
        &mut self,
        name: &str,
    ) -> impl Future<Output = errs::Result<&mut C>> + Send;

    /// Executes an asynchronous business logic function passing `self`.
    ///
    /// # Type Parameters
    ///
    /// - `F`: The async logic function closure type.
    ///
    /// # Arguments
    ///
    /// - `logic_fn`: The async logic function to execute.
    ///
    /// # Returns
    ///
    /// The result of executing the logic function ([`errs::Result<()>`]).
    #[allow(async_fn_in_trait)]
    async fn run_async<F>(&mut self, logic_fn: F) -> errs::Result<()>
    where
        for<'b> F:
            FnMut(&'b mut Self) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>;

    /// Initiates a chained asynchronous logic execution via [`LogicRun`].
    ///
    /// # Returns
    ///
    /// A future resolving to a new [`LogicRun`] builder.
    #[allow(async_fn_in_trait)]
    async fn start_async(&mut self) -> LogicRun<'_, Self>;
}

/// Asynchronous builder for chaining multiple logic executions sequentially without transactions.
///
/// # Type Parameters
///
/// - `'a`: Lifetime of the borrow on the data access provider.
/// - `A`: The data access type implementing [`AsDataHub`].
///
/// # Notes
///
/// Always call [`LogicRun::end`] at the end of the chain.
#[must_use = "call `end()` to finish the chain and release the data connections"]
pub struct LogicRun<'a, A> {
    hub: &'a mut A,
    err: LogicErrAt,
    index: usize,
    nested: bool,
}

/// Asynchronous builder for chaining multiple logic executions sequentially within a transaction.
///
/// # Type Parameters
///
/// - `'a`: Lifetime of the borrow on the [`DataHub`].
/// - `T`: Marker type parameter of the [`DataHub`].
///
/// # Notes
///
/// Always call [`LogicTxn::end_txn_async`] at the end of the chain.
#[must_use = "call `end_txn_async()` to commit or roll back the transaction"]
pub struct LogicTxn<'a, T> {
    hub: &'a mut DataHub<T>,
    err: LogicErrAt,
    index: usize,
}

/// Asynchronous runner that encapsulates a [`DataHub`].
///
/// # Type Parameters
///
/// - `T`: Marker type parameter of the underlying [`DataHub`].
pub struct LogicRunner<T> {
    hub: DataHub<T>,
}

/// Helper macro to adapt an asynchronous logic function into a pinned closure accepted by
/// [`LogicRunner::run_async`], [`LogicRunner::txn_async`], etc.
///
/// # Examples
///
/// ```rust,ignore
/// runner.run_async(sabi::tokio::logic!(my_async_logic)).await?;
/// ```
#[doc(inline)]
pub use crate::_logic as logic;

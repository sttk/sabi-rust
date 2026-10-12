// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

//! Provides Tokio-based asynchronous APIs for the sabi framework.
//!
//! It provides asynchronous versions of the core data-access components,
//! including [`DataSrc`], [`DataConn`], [`DataAcc`], and [`DataHub`].
//!
//! [`LogicRunner`] wraps a [`DataHub`] and provides methods for configuring data sources and
//! executing asynchronous business logic.
//!
//! [`LogicRunner::run_async`] passes the inner [`DataHub`] to a logic closure and executes it
//! asynchronously. Because the argument type of the closure is declared as a logic-specific data
//! access trait, the logic can see only the methods defined by that trait, rather than all of the
//! functionality provided by [`DataHub`].
//!
//! [`LogicRunner::start_async`] creates a [`LogicRun`] builder that can execute multiple logic
//! closures using method chaining. [`LogicRun`] provides three execution methods:
//! [`run_async`][LogicRun::run_async], [`run_force_async`][LogicRun::run_force_async], and
//! [`run_or_block_async`][LogicRun::run_or_block_async].
//! [`run_async`][LogicRun::run_async] executes the logic only if no error has occurred in
//! previous calls, while [`run_force_async`][LogicRun::run_force_async] always executes the logic
//! regardless of whether a previous error has occurred.
//! [`run_or_block_async`][LogicRun::run_or_block_async], like [`run_async`][LogicRun::run_async],
//! executes the logic only if no error has occurred, but if an error occurs during its execution,
//! subsequent calls to [`run_async`][LogicRun::run_async],
//! [`run_force_async`][LogicRun::run_force_async], and
//! [`run_or_block_async`][LogicRun::run_or_block_async] are skipped. Finally, calling
//! [`end`][LogicRun::end] returns the result containing all errors that occurred during execution.
//!
//! [`LogicRunner::txn_async`] executes a logic closure under transaction control and attempts to
//! commit if it succeeds. If the logic or the commit fails, it performs a rollback.
//! [`LogicRunner::begin_txn_async`] creates a [`LogicTxn`] builder that can execute multiple
//! logic closures using method chaining and then perform a commit or rollback with
//! [`end_txn_async`][LogicTxn::end_txn_async]. Like [`LogicRun`], [`LogicTxn`] provides
//! [`run_async`][LogicTxn::run_async], [`run_force_async`][LogicTxn::run_force_async], and
//! [`run_or_block_async`][LogicTxn::run_or_block_async], and their execution conditions are the
//! same as those of [`LogicRun`].
//!
//! [`AsyncGroup`] is used to manage asynchronous tasks associated with data source setup and data
//! connection transaction processing. It allows asynchronous setup, commit, rollback, and related
//! operations to be performed concurrently on the Tokio runtime.

mod async_group;
mod data_conn;
mod data_hub;
mod data_src;

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_commons;

pub use data_conn::DataConnError;
pub use data_hub::DataHubError;
pub use data_src::DataSrcError;

use crate::{ErrEntry, LogicErrAt, SendSyncNonNull, TxnFailureReport};

use std::any;
use std::collections::HashMap;
use std::future::Future;
use std::marker;
use std::pin::Pin;
use std::sync::Arc;

pub use data_src::{
    create_static_data_src_container, setup_async, setup_with_order_async, uses, uses_async,
};

#[doc(inline)]
pub use crate::_logic as logic;

#[doc(inline)]
pub use crate::_uses_for_async as uses;

/// Manages a group of concurrent asynchronous tasks.
///
/// This struct is passed to lifecycle methods such as [`DataSrc::setup_async`],
/// [`DataConn::commit_async`], and [`DataConn::rollback_async`] to allow data sources and
/// connections to perform background work concurrently. Tasks are added via
/// [`AsyncGroup::add`](crate::tokio::AsyncGroup::add).
#[allow(clippy::type_complexity)]
pub struct AsyncGroup {
    attrs: Vec<(usize, Arc<str>)>,
    tasks: Vec<Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'static>>>,
    _index: usize,
    _name: Arc<str>,
}

/// Defines the lifecycle and transactional operations for an asynchronous data connection.
///
/// Implementors represent active connections or transactional units of work (such as database
/// transactions or client sessions). A `DataConn` is created by a [`DataSrc`] and managed by
/// [`DataHub`] during logic execution.
#[allow(unused_variables)]
#[allow(async_fn_in_trait)]
pub trait DataConn {
    /// Commits changes made through this connection asynchronously.
    ///
    /// Background work related to the commit can be added to `ag` to execute concurrently with
    /// other connections.
    ///
    /// # Parameters
    ///
    /// * `ag`: The async group for running concurrent tasks during commit.
    ///
    /// # Errors
    ///
    /// Returns an error if committing the connection fails.
    fn commit_async(
        &mut self,
        ag: &mut AsyncGroup,
    ) -> impl Future<Output = errs::Result<()>> + Send;

    /// Performs pre-commit validation or preparation before any connection commits.
    ///
    /// Default implementation is a no-op returning `Ok(())`.
    ///
    /// # Parameters
    ///
    /// * `ag`: The async group for running concurrent tasks during pre-commit.
    ///
    /// # Errors
    ///
    /// Returns an error if pre-commit checks fail.
    #[cfg_attr(coverage_nightly, coverage(off))]
    fn pre_commit_async(
        &mut self,
        ag: &mut AsyncGroup,
    ) -> impl Future<Output = errs::Result<()>> + Send {
        async { Ok(()) }
    }

    /// Performs post-commit operations after all participating connections have committed.
    ///
    /// Default implementation is a no-op returning `Ok(())`.
    ///
    /// # Parameters
    ///
    /// * `ag`: The async group for running concurrent tasks during post-commit.
    ///
    /// # Errors
    ///
    /// Returns an error if post-commit processing fails.
    #[cfg_attr(coverage_nightly, coverage(off))]
    fn post_commit_async(
        &mut self,
        ag: &mut AsyncGroup,
    ) -> impl Future<Output = errs::Result<()>> + Send {
        async { Ok(()) }
    }

    /// Returns whether this data connection has already successfully committed.
    ///
    /// # Returns
    ///
    /// `true` if the connection has completed its commit; otherwise `false`.
    fn is_committed(&self) -> bool;

    /// Rolls back any changes made through this connection asynchronously.
    ///
    /// Background work related to the rollback can be added to `ag`.
    ///
    /// # Parameters
    ///
    /// * `ag`: The async group for running concurrent tasks during rollback.
    ///
    /// # Errors
    ///
    /// Returns an error if the rollback operation fails.
    fn rollback_async(
        &mut self,
        ag: &mut AsyncGroup,
    ) -> impl Future<Output = errs::Result<()>> + Send;

    /// Notification hook invoked after a transaction failure and rollback have occurred.
    ///
    /// Provides failure reports for all participating connections to allow custom failure
    /// handling, compensation, or logging. Default implementation does nothing.
    ///
    /// # Parameters
    ///
    /// * `ag`: The async group for running concurrent tasks during failure handling.
    /// * `reports`: Diagnostic reports for all data connections in the failed transaction.
    #[cfg_attr(coverage_nightly, coverage(off))]
    fn on_txn_failure_async(
        &mut self,
        ag: &mut AsyncGroup,
        reports: Arc<[TxnFailureReport]>,
    ) -> impl Future<Output = ()> + Send {
        async {}
    }

    /// Closes the connection and releases underlying resources.
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
    committed: bool,
}

/// Defines an asynchronous provider that configures and creates [`DataConn`] instances.
///
/// Implementors represent data sources such as database connection pools or API clients.
/// Data sources can be registered globally via [`uses()`] or locally on a [`LogicRunner`].
///
/// # Type Parameters
///
/// * `C`: The concrete [`DataConn`] type produced by this data source.
#[trait_variant::make(Send)]
#[allow(unused_variables)] // for rustdoc
pub trait DataSrc<C>
where
    C: DataConn + 'static,
{
    /// Initializes and prepares the data source asynchronously for creating connections.
    ///
    /// Background setup tasks can be added to `ag` to execute concurrently with other data
    /// sources.
    ///
    /// # Parameters
    ///
    /// * `ag`: The async group for running concurrent setup tasks.
    ///
    /// # Errors
    ///
    /// Returns an error if setup fails.
    async fn setup_async(&mut self, ag: &mut AsyncGroup) -> errs::Result<()>;

    /// Closes the data source and releases its resources.
    fn close(&mut self);

    /// Creates a new [`DataConn`] instance asynchronously for accessing data.
    ///
    /// # Returns
    ///
    /// A boxed [`DataConn`] implementation instance.
    ///
    /// # Errors
    ///
    /// Returns an error if connection creation fails.
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

/// RAII guard that automatically closes and shuts down global data sources when dropped.
///
/// Returned by [`setup_async`] and [`setup_with_order_async`].
pub struct AutoShutdown {}

/// Container for a statically registered asynchronous data source.
#[doc(hidden)]
pub struct StaticDataSrcContainer {
    pub(crate) ssnnptr: SendSyncNonNull<DataSrcContainer>,
}

/// Registration record for a static asynchronous data source factory.
#[doc(hidden)]
pub struct StaticDataSrcRegistration {
    pub(crate) factory: fn() -> StaticDataSrcContainer,
}

/// The central coordinator providing access to data connections during asynchronous logic
/// execution.
///
/// An instance of `DataHub` is passed to business logic closures executed via
/// [`DataAcc::run_async`], [`LogicRunner::run_async`], [`LogicRunner::txn_async`], etc.
/// It allows retrieving typed [`DataConn`] instances by name.
///
/// # Type Parameters
///
/// * `T`: A marker or context type representing the application domain.
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

/// Trait for accessing data connections and executing business logic against an asynchronous
/// [`DataHub`].
pub trait DataAcc {
    /// The associated domain marker type used by [`DataHub`].
    type D: any::Any;

    /// Retrieves a mutable reference to a named data connection of type `C` asynchronously.
    ///
    /// If the connection has not yet been opened in this execution, it is created from the
    /// matching data source.
    ///
    /// # Type Parameters
    ///
    /// * `C`: The expected [`DataConn`] type.
    ///
    /// # Parameters
    ///
    /// * `name`: The registered name of the data connection or data source.
    ///
    /// # Returns
    ///
    /// A mutable reference to the requested data connection.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * No data source with the specified name exists.
    /// * Creating the connection fails.
    /// * The connection cannot be downcast to the requested type `C`.
    fn get_data_conn_async<C: DataConn + 'static>(
        &mut self,
        name: &str,
    ) -> impl Future<Output = errs::Result<&mut C>> + Send;

    /// Executes a business logic closure using the `DataHub` asynchronously.
    ///
    /// # Type Parameters
    ///
    /// * `F`: The closure type returning a pinned future.
    ///
    /// # Parameters
    ///
    /// * `logic_fn`: The business logic closure to execute.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success.
    ///
    /// # Errors
    ///
    /// Returns an error if local data source setup fails or if `logic_fn` returns an error.
    #[allow(async_fn_in_trait)]
    async fn run_async<F>(&mut self, logic_fn: F) -> errs::Result<()>
    where
        for<'b> F: FnMut(
            &'b mut DataHub<Self::D>,
        ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>;

    /// Starts a builder for executing a chain of non-transactional business logic steps
    /// asynchronously.
    ///
    /// # Returns
    ///
    /// A [`LogicRun`] builder instance.
    #[allow(async_fn_in_trait)]
    async fn start_async(&mut self) -> LogicRun<'_, Self::D>;
}

/// Builder for chaining and executing non-transactional business logic steps on a [`DataHub`]
/// asynchronously.
///
/// Created via [`DataAcc::start_async`] or [`LogicRunner::start_async`]. Provides methods like
/// [`run_async`](LogicRun::run_async), [`run_force_async`](LogicRun::run_force_async),
/// [`run_or_block_async`](LogicRun::run_or_block_async), and [`end`](LogicRun::end).
///
/// # Type Parameters
///
/// * `'a`: The lifetime of the mutable borrow of [`DataHub`].
/// * `T`: The domain marker type of the `DataHub`.
pub struct LogicRun<'a, T> {
    hub: &'a mut DataHub<T>,
    err: LogicErrAt,
    index: usize,
    nested: bool,
}

/// Builder for chaining and executing transactional business logic steps on a [`DataHub`]
/// asynchronously.
///
/// Created via [`LogicRunner::begin_txn_async`]. Provides methods like
/// [`run_async`](LogicTxn::run_async), [`run_force_async`](LogicTxn::run_force_async),
/// [`run_or_block_async`](LogicTxn::run_or_block_async), and
/// [`end_txn_async`](LogicTxn::end_txn_async).
///
/// # Type Parameters
///
/// * `'a`: The lifetime of the mutable borrow of [`DataHub`].
/// * `T`: The domain marker type of the `DataHub`.
pub struct LogicTxn<'a, T> {
    hub: &'a mut DataHub<T>,
    err: LogicErrAt,
    index: usize,
}

/// Configures data sources and executes business logic and transactions on an asynchronous
/// [`DataHub`].
///
/// # Type Parameters
///
/// * `T`: The domain marker type of the underlying [`DataHub`].
pub struct LogicRunner<T> {
    hub: DataHub<T>,
}

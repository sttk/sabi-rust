// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

//! This module provides Tokio-specific implementations for asynchronous data access,
//! including `AsyncGroup` for concurrent task management, `DataConn` for
//! transactional data connections, `DataSrc` for data source management,
//! and `DataHub` as a central orchestrator.
//!
//! It leverages Rust's asynchronous capabilities with the Tokio runtime
//! to enable efficient and concurrent handling of data operations.

mod async_group;
mod data_acc;
mod data_conn;
mod data_hub;
mod data_src;

use crate::{SendSyncNonNull, TxnFailureReport};

use std::any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

pub use data_conn::DataConnError;
pub use data_hub::DataHubError;
pub use data_src::{
    create_static_data_src_container, setup_async, setup_with_order_async, uses, uses_async,
    DataSrcError,
};

/// A convenience macro to easily convert an asynchronous function into a `Pin<Box<dyn Future>>`
/// closure suitable for `DataHub`'s `run_async` or `txn_async` methods.
///
/// This macro simplifies passing async functions by handling the boxing and pinning.
/// The resulting `Future` implements `Send`.
///
/// # Example
///
/// ```ignore
/// async fn my_logic(data: &mut (impl MyData + Send)) -> errs::Result<()> {
///     // ... some logic using data
///     Ok(())
/// }
///
/// #[tokio::main]
/// async fn main() {
///     let mut hub = DataHub::new();
///     hub.txn_async(logic!(my_logic)).await.unwrap();
/// }
/// ```
#[doc(inline)]
pub use crate::_logic as logic;

/// Macro for registering a global data source at the top-level.
///
/// # Parameters
///
/// * `$name` - The name of the data source (must be a string literal).
/// * `$data_src` - The data source instance.
///
/// # Examples
///
/// ```ignore
/// uses!("my_global_source", MyDataSource::new());
/// ```
#[doc(inline)]
pub use crate::_uses_for_async as uses;

/// The structure that allows for the concurrent execution of multiple asynchronous tasks
/// using green-thread and waits for all of them to complete.
///
/// Functions are added using the `add` method and are then run concurrently in separate green-threads.
/// The `AsyncGroup` ensures that all tasks finish before proceeding,
/// and can collect any errors that occur.
#[allow(clippy::type_complexity)]
pub struct AsyncGroup {
    indexes: Vec<usize>,
    tasks: Vec<Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'static>>>,
    _index: usize,
}

/// The asynchronous trait for data connection implementations, providing methods for transaction
/// management.
///
/// Implementors of this trait represent a connection to a data source and define
/// how to commit, rollback, and handle the lifecycle of transactions.
#[allow(async_fn_in_trait)]
#[allow(unused_variables)] // rustdoc
pub trait DataConn {
    /// Attempts to asynchronously commit the changes made within this data connection.
    ///
    /// This is typically the main commit process, executed after `pre_commit_async`.
    ///
    /// # Parameters
    ///
    /// * `ag` - An `AsyncGroup` to which asynchronous tasks related to the commit can be added.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure of the commit operation.
    fn commit_async(
        &mut self,
        ag: &mut AsyncGroup,
    ) -> impl Future<Output = errs::Result<()>> + Send;

    /// Performs preparatory actions before the main commit process.
    ///
    /// This method is called before `commit_async` and can be used for tasks like
    /// validation or preparing data.
    ///
    /// # Parameters
    ///
    /// * `ag` - An `AsyncGroup` to which asynchronous tasks related to pre-commit can be added.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure of the pre-commit operation.
    fn pre_commit_async(
        &mut self,
        ag: &mut AsyncGroup,
    ) -> impl Future<Output = errs::Result<()>> + Send {
        async { Ok(()) }
    }

    /// Performs actions after the main commit process, only if it succeeds.
    ///
    /// This can be used for cleanup or post-transaction logging.
    ///
    /// # Parameters
    ///
    /// * `ag` - An `AsyncGroup` to which asynchronous tasks related to post-commit can be added.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure of the post-commit operation.
    fn post_commit_async(
        &mut self,
        ag: &mut AsyncGroup,
    ) -> impl Future<Output = errs::Result<()>> + Send {
        async { Ok(()) }
    }

    /// Returns whether the transaction on this connection has been successfully committed.
    fn is_committed(&self) -> bool;

    /// Rolls back any changes made within this data connection's transaction.
    ///
    /// This method undoes all operations performed since the beginning of the transaction,
    /// restoring the data service to its state before the transaction began.
    ///
    /// # Parameters
    ///
    /// * `ag` - An `AsyncGroup` to which asynchronous tasks related to rollback can be added.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure of the rollback operation.
    fn rollback_async(
        &mut self,
        ag: &mut AsyncGroup,
    ) -> impl Future<Output = errs::Result<()>> + Send;

    /// An asynchronous lifecycle callback invoked when a transaction fails and a rollback is executed.
    ///
    /// This allows the data connection to handle post-failure tasks or custom logic
    /// based on the provided transaction failure reports.
    ///
    /// # Parameters
    ///
    /// * `ag`: A mutable reference to an [`AsyncGroup`] for asynchronous task execution.
    /// * `reports`: An [`Arc`] slice of [`TxnFailureReport`] containing failure details for all
    ///   connections.
    fn on_txn_failure_async(
        &mut self,
        ag: &mut AsyncGroup,
        reports: Arc<[TxnFailureReport]>,
    ) -> impl Future<Output = ()> + Send {
        async {}
    }

    /// Closes the data connection, releasing any associated resources.
    ///
    /// This method is always called at the end of a transaction, regardless of its outcome.
    fn close(&mut self);
}

pub(crate) struct NoopDataConn {}

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
}

/// The trait that abstracts a data source responsible for managing connections
/// to external data services, such as databases, file systems, or messaging services.
///
/// It receives configuration for connecting to an external data service and then
/// creates and supplies [`DataConn`] instance, representing a single session connection.
#[trait_variant::make(Send)]
#[allow(unused_variables)] // for rustdoc
pub trait DataSrc<C>
where
    C: DataConn + 'static,
{
    /// Performs the asynchronous setup process for the data source.
    ///
    /// This method is responsible for establishing global connections, configuring
    /// connection pools, or performing any necessary initializations required
    /// before [`DataConn`] instances can be created.
    ///
    /// # Parameters
    ///
    /// * `ag`: A mutable reference to an [`AsyncGroup`]. This is used if the setup
    ///   process is potentially time-consuming and can benefit from concurrent
    ///   execution in a separate thread.
    ///
    /// # Returns
    ///
    /// * `errs::Result<()>`: `Ok(())` if the setup is successful, or an [`errs::Err`]
    ///   if any part of the setup fails.
    async fn setup_async(&mut self, ag: &mut AsyncGroup) -> errs::Result<()>;

    /// Closes the data source and releases any globally held resources.
    ///
    /// This method should perform cleanup operations, such as closing global connections
    /// or shutting down connection pools, that were established during the setup process.
    fn close(&mut self);

    /// Asynchronously creates a new [`DataConn`] instance which is a connection per session.
    ///
    /// Each call to this method should yield a distinct [`DataConn`] object tailored
    /// for a single session's operations.
    ///
    /// # Returns
    ///
    /// * `errs::Result<Box<C>>`: `Ok(Box<C>)` containing the newly created [`DataConn`]
    ///   if successful, or an [`errs::Err`] if the connection could not be created.
    async fn create_data_conn_async(&mut self) -> errs::Result<Box<C>>;
}

pub(crate) struct NoopDataSrc {}

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

/// A utility struct that ensures to close and drop global data sources when it goes out of scope.
///
/// This struct implements the `Drop` trait, and its `drop` method handles the closing and
/// dropping of registered global data sources.
/// Therefore, this ensures that these operations are automatically executed at the end of
/// the scope.
///
/// **NOTE:** Do not receive an instance of this struct into an anonymous variable
/// (`let _ = ...`), because an anonymous variable is dropped immediately at that point.
pub struct AutoShutdown {}

/// The struct that acts as a central hub for data input/output operations, integrating
/// multiple *Data* traits (which are passed to business logic functions as their arguments) with
/// [`DataAcc`] traits (which implement default data I/O methods for external services).
///
/// It facilitates data access by providing [`DataConn`] objects, created from
/// both global data sources (registered via the global [`uses!`] macro) and
/// session-local data sources (registered via [`DataHub::uses`] method).
///
/// The [`DataHub`] is capable of performing aggregated transactional operations
/// on all [`DataConn`] objects created from its registered [`DataSrc`] instances.
pub struct DataHub {
    local_data_src_manager: DataSrcManager,
    data_src_map: HashMap<Arc<str>, (bool, usize)>,
    data_conn_manager: DataConnManager,
    fixed: bool,
}

/// This trait provides a mechanism to retrieve a mutable reference to a [`DataConn`] object
/// by name, creating it if necessary.
///
/// It is typically implemented as a derived trait with default methods (using
/// the `override_macro` crate) on [`DataHub`], allowing application logic to
/// interact with data services through an abstract interface.
pub trait DataAcc {
    /// Retrieves a mutable reference to a [`DataConn`] object by name, creating it if necessary.
    ///
    /// This is the core method used by [`DataAcc`] implementations to obtain connections
    /// to external data services. It first checks if a [`DataConn`] with the given name
    /// already exists in the current session. If not, it attempts to find a
    /// corresponding [`DataSrc`] and create a new [`DataConn`] from it.
    ///
    /// # Type Parameters
    ///
    /// * `C`: The concrete type of [`DataConn`] expected.
    ///
    /// # Parameters
    ///
    /// * `name`: The name of the data source/connection to retrieve.
    ///
    /// # Returns
    ///
    /// * `errs::Result<&mut C>`: A mutable reference to the [`DataConn`] instance if successful,
    ///   or an [`errs::Err`] if the data source is not found, or if the retrieved/created
    ///   [`DataConn`] cannot be cast to the specified type `C`.
    #[allow(async_fn_in_trait)]
    fn get_data_conn_async<C: DataConn + 'static>(
        &mut self,
        name: &str,
    ) -> impl Future<Output = errs::Result<&mut C>> + Send;
}

#[doc(hidden)]
pub struct StaticDataSrcContainer {
    pub(crate) ssnnptr: SendSyncNonNull<DataSrcContainer>,
}

#[doc(hidden)]
pub struct StaticDataSrcRegistration {
    pub(crate) factory: fn() -> StaticDataSrcContainer,
}

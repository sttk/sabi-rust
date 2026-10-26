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
//!
//! [`DataAcc::run_async`] and [`DataAcc::start_async`] are used inside a logic closure that is
//! already running under a [`LogicRunner`] to invoke other logic with the same data-access
//! object. They neither set up data sources nor close connections; the nested logic shares the
//! data connections, and the transaction when run via [`LogicRunner::txn_async`], of the
//! enclosing run.
//!
//! Global data sources are registered with the [`uses!`] macro or the [`uses_async`] function,
//! and are set up by [`setup_async`] or [`setup_with_order_async`].

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

pub use data_src::create_static_data_src_container;

/// Asynchronously sets up all globally registered data sources.
///
/// This setup typically involves tasks such as creating connection pools,
/// opening global connections, or performing initial configurations necessary
/// for creating session-specific connections.
///
/// If any data source fails to set up, this function returns an `errs::Err` with
/// [`DataSrcError::FailToSetupGlobalDataSrcs`], containing a vector of the names
/// of the failed data sources and their corresponding `errs::Err` objects. In such a case,
/// all global data sources that were successfully set up are also closed.
///
/// If all data source setups are successful, the [`Result::Ok`] which contains an
/// [`AutoShutdown`] object is returned. This object is designed to close and drop global
/// data sources when it's dropped.
/// Thanks to Rust's ownership mechanism, this ensures that the global data sources are
/// automatically cleaned up when the return value goes out of scope.
///
/// **NOTE:** Do not receive the [`Result`] or its inner object into an anonymous
/// variable using `let _ = ...`.
/// If you do, the inner object is dropped immediately at that point.
///
/// # Returns
///
/// * `Result<AutoShutdown, errs::Err>`: An [`AutoShutdown`] if all global data sources are
///   set up successfully, or an `errs::Err` if any setup fails.
pub async fn setup_async() -> errs::Result<AutoShutdown> {
    data_src::setup_async().await
}

/// Asynchronously sets up all globally registered data sources with a specified order.
///
/// Similar to [`setup_async`], but allows defining the order in which data sources
/// are set up. Data sources not specified in `names` will be set up after the
/// specified ones, in an undefined order.
///
/// This setup typically involves tasks such as creating connection pools,
/// opening global connections, or performing initial configurations necessary
/// for creating session-specific connections.
///
/// If any data source fails to set up, this function returns an `errs::Err` with
/// [`DataSrcError::FailToSetupGlobalDataSrcs`], containing a vector of the names
/// of the failed data sources and their corresponding `errs::Err` objects. In such a case,
/// all global data sources that were successfully set up are also closed.
///
/// If all data source setups are successful, the [`Result::Ok`] which contains an
/// [`AutoShutdown`] object is returned. This object is designed to close and drop global
/// data sources when it's dropped.
/// Thanks to Rust's ownership mechanism, this ensures that the global data sources are
/// automatically cleaned up when the return value goes out of scope.
///
/// **NOTE:** Do not receive the [`Result`] or its inner object into an anonymous
/// variable using `let _ = ...`.
/// If you do, the inner object is dropped immediately at that point.
///
/// # Parameters
///
/// * `names` - A `static` slice of data source name specifying the desired setup order.
///
/// # Returns
///
/// * `Result<AutoShutdown, errs::Err>`: An [`AutoShutdown`] if all global data sources are
///   set up successfully, or an `errs::Err` if any setup fails.
pub async fn setup_with_order_async(names: &'static [&str]) -> errs::Result<AutoShutdown> {
    data_src::setup_with_order_async(names).await
}

/// Registers a global data source, making it available throughout the application.
///
/// This function associates a given [`DataSrc`] implementation with a unique name.
/// This name will later be used to retrieve session-specific [`DataConn`] instances from this
/// data source.
///
/// Global data sources are managed by a singleton and can be set up once for the application's
/// lifetime.
/// If `setup_async` or `setup_with_order_async` has already been called, this function will return
/// an `errs::Err`.
/// If another Tokio task holds the lock of the global data source manager, this function will wait
/// until the lock is released.
///
/// # Parameters
///
/// * `name` - The name to associate with this data source.
/// * `ds` - The data source instance, which must implement `DataSrc` and have a `'static` lifetime.
///
/// # Type Parameters
///
/// * `S` - The type of the data source.
/// * `C` - The type of the data connection provided by the data source.
///
/// # Returns
///
/// * `errs::Result<()>`: [`Ok`] if the data source is successfully registered, or an `errs::Err` if
///   the global data source manager is in an invalid state or setup has already occurred.
#[inline]
pub async fn uses_async<S, C>(name: impl Into<Arc<str>>, ds: S) -> errs::Result<()>
where
    S: DataSrc<C> + 'static,
    C: DataConn + 'static,
{
    data_src::uses_async(name, ds).await
}

/// Registers a global data source, making it available throughout the application.
///
/// This is the synchronous version of [`uses_async`].
///
/// This function associates a given [`DataSrc`] implementation with a unique name.
/// This name will later be used to retrieve session-specific [`DataConn`] instances from this
/// data source.
///
/// Global data sources are managed by a singleton and can be set up once for the application's
/// lifetime.
/// If [`setup_async`] or [`setup_with_order_async`] has already been called, this function will
/// return an `errs::Err`.
/// If another Tokio task holds the lock of the global data source manager, this function will
/// return an error immediately without waiting.
///
/// # Parameters
///
/// * `name` - The name to associate with this data source.
/// * `ds` - The data source instance, which must implement [`DataSrc`] and have a `'static`
///   lifetime.
///
/// # Type Parameters
///
/// * `S` - The type of the data source.
/// * `C` - The type of the data connection provided by the data source.
///
/// # Returns
///
/// * `errs::Result<()>`: [`Ok`] if the data source is successfully registered, or an `errs::Err` if
///   the global data source manager is in an invalid state or setup has already occurred.
pub fn uses<S, C>(name: impl Into<Arc<str>>, ds: S) -> errs::Result<()>
where
    S: DataSrc<C> + 'static,
    C: DataConn + 'static,
{
    data_src::uses(name, ds)
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

/// A convenience macro to convert an asynchronous function or closure into a closure returning
/// `Pin<Box<dyn Future>>`, suitable for [`LogicRunner::run_async`], [`LogicRunner::txn_async`],
/// [`DataAcc::run_async`], and the `run_async`, `run_force_async`, and `run_or_block_async`
/// methods of [`LogicRun`] and [`LogicTxn`].
///
/// This macro simplifies passing async functions by handling the boxing and pinning.
/// The resulting `Future` implements `Send`.
///
/// # Parameters
///
/// * `$f` - An async function or closure that takes the data-access object passed to the logic
///   closure (`&mut DataHub<T>` or a reference to a user-defined data trait) and returns
///   `errs::Result<()>`.
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
///     let mut runner = LogicRunner::new(DataHub::<MyScope>::new());
///     runner.txn_async(logic!(my_logic)).await.unwrap();
/// }
/// ```
#[doc(inline)]
pub use crate::_logic as logic;

use crate::{ErrEntry, LogicErrAt, SendSyncNonNull, TxnFailureReport};

use std::any;
use std::collections::HashMap;
use std::future::Future;
use std::marker;
use std::pin::Pin;
use std::sync::Arc;

/// Manages a group of concurrent tasks executed asynchronously.
///
/// The structure that allows for the concurrent execution of multiple asynchronous tasks
/// and waits for all of them to complete.
///
/// Functions are added using the `add` method and are then run concurrently.
/// The `AsyncGroup` ensures that all tasks finish before proceeding,
/// and can collect any errors that occur.
#[allow(clippy::type_complexity)]
pub struct AsyncGroup {
    attrs: Vec<(usize, Arc<str>)>,
    tasks: Vec<Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'static>>>,
    _index: usize,
    _name: Arc<str>,
}

/// Defines the lifecycle and transactional operations for an asynchronous data connection.
///
/// This asynchronous trait abstracts a connection per session to an external data service, such
/// as a database, file system, or messaging service.
///
/// Its primary purpose is to enable cohesive transaction operations across multiple
/// external data services within a single transaction context. Implementations of this
/// trait provide the concrete input/output operations for their respective data services.
///
/// Methods declared within this trait are designed to handle transactional logic.
/// The [`AsyncGroup`] parameter in various methods allows for concurrent processing
/// when commit or rollback operations are time-consuming.
///
/// # Transaction phases
///
/// When a transaction is committed, [`pre_commit_async`](DataConn::pre_commit_async),
/// [`commit_async`](DataConn::commit_async), and
/// [`post_commit_async`](DataConn::post_commit_async) are executed in this order, each for all
/// participating connections. The pre-commit and commit phases stop at the first failure, and
/// the connections that have not been committed are rolled back by
/// [`rollback_async`](DataConn::rollback_async). The post-commit phase is executed for all
/// connections even if one fails, and no rollback is performed because the connections have
/// already been committed. On any failure,
/// [`on_txn_failure_async`](DataConn::on_txn_failure_async) is invoked on every participating
/// connection.
#[allow(async_fn_in_trait)]
#[allow(unused_variables)] // rustdoc
pub trait DataConn {
    /// Attempts to asynchronously commit the changes made within this data connection's
    /// transaction.
    ///
    /// This method is typically the main commit process, executed after `pre_commit_async`.
    ///
    /// Background work related to the commit can be added to `ag` to execute concurrently with
    /// other connections.
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
    /// This method is called before `commit_async` and can be used for tasks like validation or
    /// preparing data.
    ///
    /// This method provides a timing to execute unusual commit processes or update operations not
    /// supported by transactions beforehand.
    /// This allows other update operations to be rolled back if the operations in this method
    /// fail.
    ///
    /// The default implementation does nothing.
    ///
    /// # Parameters
    ///
    /// * `ag` - An `AsyncGroup` to which asynchronous tasks related to pre-commit can be added.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure of the pre-commit operation.
    #[cfg_attr(coverage_nightly, coverage(off))]
    fn pre_commit_async(
        &mut self,
        ag: &mut AsyncGroup,
    ) -> impl Future<Output = errs::Result<()>> + Send {
        async { Ok(()) }
    }

    /// Performs post-commit operations after all participating connections have committed.
    ///
    /// This method is executed after the transaction commit process has successfully completed
    /// for all [`DataConn`] instances involved in the transaction.
    ///
    /// It provides a moment to perform follow-up actions that depend on a successful commit.
    /// For example, after a database commit, a messaging service's [`DataConn`] might use this
    /// method to send a "transaction completed" message.
    ///
    /// If this method fails, the other connections are not rolled back since they have already
    /// been committed, but [`on_txn_failure_async`](DataConn::on_txn_failure_async) is invoked.
    /// The default implementation does nothing.
    ///
    /// # Parameters
    ///
    /// * `ag` - An `AsyncGroup` to which asynchronous tasks related to post-commit can be added.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure of the post-commit operation.
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

    /// Rolls back any changes made within this data connection's transaction.
    ///
    /// This method undoes all operations performed since the beginning of the transaction,
    /// restoring the data service to its state before the transaction began.
    ///
    /// This method is called only for connections for which
    /// [`is_committed`](DataConn::is_committed) returns `false`.
    ///
    /// Background work related to the rollback can be added to `ag`.
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

    /// Notification hook invoked after a transaction failure and rollback have occurred.
    ///
    /// This allows the data connection to handle post-failure tasks or custom logic
    /// based on the provided transaction failure reports.
    ///
    /// This method provides failure reports for all participating connections to allow custom
    /// failure handling, compensation, or logging. Default implementation does nothing.
    ///
    /// # Parameters
    ///
    /// * `ag`: A mutable reference to an [`AsyncGroup`] for asynchronous task execution.
    /// * `reports`: An [`Arc`] slice of [`TxnFailureReport`] containing failure details for all
    ///   connections.
    #[cfg_attr(coverage_nightly, coverage(off))]
    fn on_txn_failure_async(
        &mut self,
        ag: &mut AsyncGroup,
        reports: Arc<[TxnFailureReport]>,
    ) -> impl Future<Output = ()> + Send {
        async {}
    }

    /// Closes the connection to the external data service.
    ///
    /// This method should release any resources held by the data connection, ensuring
    /// a graceful shutdown of the connection.
    ///
    /// This method is called when the session ends, whether the logic succeeded or failed.
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
    ///   execution as asynchronous tasks on the Tokio runtime.
    ///
    /// # Returns
    ///
    /// * `errs::Result<()>`: `Ok(())` if the setup is successful, or an `errs::Err`
    ///   if any part of the setup fails.
    async fn setup_async(&mut self, ag: &mut AsyncGroup) -> errs::Result<()>;

    /// Closes the data source and releases the resources acquired during the setup process.
    ///
    /// This method should perform cleanup operations, such as closing global connections or
    /// shutting down connection pools, that were established in
    /// [`setup_async`](DataSrc::setup_async).
    /// It is called only for a data source whose setup succeeded. A data source whose setup
    /// failed, or which was not set up, is only dropped.
    fn close(&mut self);

    /// Asynchronously creates a new [`DataConn`] instance which is a connection per session.
    ///
    /// Each call to this method should yield a distinct [`DataConn`] object tailored
    /// for a single session's operations.
    ///
    /// # Returns
    ///
    /// * `errs::Result<Box<C>>`: `Ok(Box<C>)` containing the newly created [`DataConn`]
    ///   if successful, or an `errs::Err` if the connection could not be created.
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

/// A utility struct that ensures to close and drop global data sources when it goes out of scope.
///
/// This struct implements the `Drop` trait, and its `drop` method handles the closing and
/// dropping of registered global data sources.
/// Therefore, this ensures that these operations are automatically executed at the end of
/// the scope.
///
/// **NOTE:** Do not receive an instance of this struct into an anonymous variable
/// (`let _ = ...`), because an anonymous variable is dropped immediately at that point.
#[must_use = "global data sources are closed when this value is dropped; bind it to a named variable"]
pub struct AutoShutdown {}

#[doc(hidden)]
pub struct StaticDataSrcContainer {
    ssnnptr: SendSyncNonNull<DataSrcContainer>,
}

#[doc(hidden)]
pub struct StaticDataSrcRegistration {
    pub(crate) factory: fn() -> StaticDataSrcContainer,
}

/// The central coordinator providing access to data connections during logic execution.
///
/// The struct that acts as a central hub for data input/output operations, integrating
/// multiple *Data* traits (which are passed to business logic functions as their arguments) with
/// [`DataAcc`] traits (which implement default data I/O methods for external services).
///
/// It facilitates data access by providing [`DataConn`] objects, created from
/// both global data sources (registered via the global [`uses!`] macro or the [`uses_async`]
/// function) and session-local data sources (registered via [`LogicRunner::uses`] method).
/// A `DataHub` is normally wrapped by a [`LogicRunner`], which drives its sessions.
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

/// Trait for accessing the concrete [`DataHub`] behind a data-access object.
///
/// It is the supertrait of [`DataAcc`], and lets [`LogicRun`] and the default methods of
/// [`DataAcc`] reach the hub's session management through any data-access type.
/// [`DataHub<T>`] implements it with `D = T`, and users normally do not implement it themselves.
///
/// `Send` is required so that logic futures borrowing the object can run on Tokio tasks.
pub trait AsDataHub: Send {
    /// The domain type managed by the underlying [`DataHub`].
    type D: 'static;

    /// Returns a mutable reference to the concrete hub backing this object.
    ///
    /// The returned reference must correspond exactly to the underlying
    /// [`DataHub`] instance that owns the state for `Self::D`.
    fn as_hub(&mut self) -> &mut DataHub<Self::D>;
}

/// Trait for accessing data connections and executing business logic against a [`DataHub`].
///
/// This trait provides a mechanism to retrieve a mutable reference to a [`DataConn`] object
/// by name, creating it if necessary.
///
/// It is typically implemented as a derived trait with default methods (using
/// the `override_macro` crate) on [`DataHub`], allowing application logic to
/// interact with data services through an abstract interface.
pub trait DataAcc: AsDataHub + Sized {
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
    ///   or an `errs::Err` in the following cases:
    ///     * No data source is registered under `name`
    ///       ([`DataHubError::NoDataSrcToCreateDataConn`]).
    ///     * The data source fails to create the [`DataConn`].
    ///     * The created or cached [`DataConn`] is not of type `C`
    ///       ([`DataSrcError::FailToCastDataConn`] / [`DataConnError::FailToCastDataConn`]).
    #[allow(async_fn_in_trait)]
    fn get_data_conn_async<C: DataConn + 'static>(
        &mut self,
        name: &str,
    ) -> impl Future<Output = errs::Result<&mut C>> + Send;

    /// Executes a logic function with this data-access object.
    ///
    /// The logic function is invoked directly with `&mut Self`. This method neither sets up
    /// data sources nor closes or commits connections; it is intended for calling other logic
    /// from within a logic function already running under [`LogicRunner`]. The nested logic
    /// shares the data connections, and the transaction when run via
    /// [`LogicRunner::txn_async`], of the enclosing run.
    ///
    /// # Type Parameters
    ///
    /// * `F`: The closure type, implementing
    ///   `FnMut(&mut Self) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send>>`.
    ///
    /// # Parameters
    ///
    /// * `logic_fn`: A closure that encapsulates the business logic to be executed.
    ///   It takes a mutable reference to this data-access object as an argument.
    ///
    /// # Returns
    ///
    /// * `errs::Result<()>`: The result of `logic_fn` as it is.
    #[allow(async_fn_in_trait)]
    async fn run_async<F>(&mut self, logic_fn: F) -> errs::Result<()>
    where
        for<'b> F:
            FnMut(&'b mut Self) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>;

    /// Creates a nested [`LogicRun`] for executing multiple logic functions within the
    /// enclosing run.
    ///
    /// The returned [`LogicRun`] does not set up data sources, and its [`end`](LogicRun::end)
    /// does not close the connections, which remain managed by the outer [`LogicRunner`] run.
    /// [`end`](LogicRun::end) returns all errors of the failed steps as
    /// [`DataHubError::FailToRunLogic`].
    ///
    /// # Returns
    ///
    /// * `LogicRun`: The struct that executes logic functions using method chaining.
    #[allow(async_fn_in_trait)]
    async fn start_async(&mut self) -> LogicRun<'_, Self>;
}

/// Builder for chaining and executing non-transactional business logic steps asynchronously.
///
/// Created via [`DataAcc::start_async`] or [`LogicRunner::start_async`]. Provides methods like
/// [`run_async`](LogicRun::run_async), [`run_force_async`](LogicRun::run_force_async),
/// [`run_or_block_async`](LogicRun::run_or_block_async), and [`end`](LogicRun::end).
///
/// A `LogicRun` created by [`LogicRunner::start_async`] owns the session: it sets up local data
/// sources when created, and [`end`](LogicRun::end) closes the data connections. Always call
/// [`end`](LogicRun::end) to finish the chain. One created by [`DataAcc::start_async`] is
/// nested in an enclosing run and does not manage the session.
///
/// # Type Parameters
///
/// * `'a`: The lifetime of the mutable borrow of the data-access object.
/// * `A`: The type of the data-access object passed to logic closures, which is [`DataHub<T>`]
///   or any other type implementing [`AsDataHub`].
#[must_use = "call `end()` to finish the chain and release the data connections"]
pub struct LogicRun<'a, A> {
    hub: &'a mut A,
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
/// Always call [`end_txn_async`](LogicTxn::end_txn_async) to finish the chain. Without it,
/// the transaction is neither committed nor rolled back, and the data connections are not closed.
///
/// # Type Parameters
///
/// * `'a`: The lifetime of the mutable borrow of [`DataHub`].
/// * `T`: The domain marker type of the `DataHub`.
#[must_use = "call `end_txn_async()` to commit or roll back the transaction"]
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

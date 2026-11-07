// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

//! # sabi
//!
//! **sabi** is a lightweight framework designed to thoroughly separate business logic
//! from data access.
//!
//! ## Key Features
//!
//! - **Data-access traits optimized per logic unit**: Each business logic unit (use case)
//!   depends only on a dedicated trait defining the minimal operations it truly requires
//!   (adhering to the Interface Segregation Principle - ISP).
//! - **Direct controller-to-data-access data routing**: Allows controller layer I/O to flow
//!   directly to data access without passing through the logic layer, eliminating redundant
//!   data conversion and "bucket brigade" forwarding.
//! - **Loose coupling and high testability**: Business logic is completely decoupled from
//!   concrete database or ORM implementations, making it trivial to substitute mock
//!   implementations in tests.
//! - **Asynchronous support**: When the `tokio` feature is enabled, asynchronous APIs
//!   are provided under the [`tokio`] module.
//!
//! ## Core Concepts
//!
//! - [`DataSrc`]: Factory and lifecycle manager that creates and initializes data
//!   connections ([`DataConn`]).
//! - [`DataConn`]: Abstraction representing a data store connection and transaction boundaries
//!   (commit, rollback, close, etc.).
//! - [`DataHub`]: Central hub that manages `DataSrc` and `DataConn` instances and provides
//!   access to them.
//! - [`DataAcc`]: Base trait for defining and aggregating data access capabilities
//!   required by logic.
//! - [`LogicRunner`]: Runner that holds a `DataHub` and executes logic functions either
//!   standalone or in transactions.
//! - [`LogicRun`] / [`LogicTxn`]: Builders for chaining multiple logic functions sequentially.
//!
//! ## Basic Usage (Synchronous)
//!
//! ```rust,ignore
//! use sabi::{uses, setup, DataAcc, DataHub, LogicRunner};
//!
//! // 1. Define a data-access trait for the logic and the logic function itself
//! pub trait MyData {
//!     fn get_text(&mut self) -> errs::Result<String>;
//! }
//!
//! pub fn my_logic(data: &mut impl MyData) -> errs::Result<()> {
//!     let text = data.get_text()?;
//!     println!("{text}");
//!     Ok(())
//! }
//!
//! // 2. Define a DataAcc-derived trait providing the operations
//! pub trait MyDataAcc: DataAcc {
//!     fn get_text(&mut self) -> errs::Result<String> {
//!         // let conn = self.get_data_conn::<MyConn>("my_conn")?;
//!         Ok("Hello, sabi!".to_string())
//!     }
//! }
//!
//! // 3. Implement the traits on DataHub
//! struct MyScope;
//! impl MyDataAcc for DataHub<MyScope> {}
//! impl MyData for DataHub<MyScope> {
//!     fn get_text(&mut self) -> errs::Result<String> {
//!         MyDataAcc::get_text(self)
//!     }
//! }
//!
//! // 4. Set up the framework and run the logic
//! fn main() -> errs::Result<()> {
//!     let _auto_shutdown = setup()?;
//!     let mut runner = LogicRunner::new(DataHub::<MyScope>::new());
//!     runner.run(my_logic)?;
//!     Ok(())
//! }
//! ```

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(unused_features)]

/// Asynchronous APIs for the Tokio runtime.
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
#[cfg(feature = "tokio")]
pub mod tokio;

mod async_group;
mod data_acc;
mod data_conn;
mod data_hub;
mod data_src;
mod global_setup;
mod logic_run;
mod logic_runner;
mod logic_txn;
mod non_null;
mod txn_failure;

pub use async_group::AsyncGroupError;
pub use data_conn::DataConnError;
pub use data_hub::DataHubError;
pub use data_src::DataSrcError;

pub use global_setup::create_static_data_src_container;

use std::collections::HashMap;
use std::sync::Arc;
use std::{any, cell, marker, ptr, thread};

/// Information about an error that occurred during concurrent tasks or batch operations.
#[derive(Debug)]
pub struct ErrEntry {
    /// The index number of the task or logic execution that failed.
    pub index: usize,
    /// The name identifying the connection or task where the error occurred.
    pub name: Arc<str>,
    /// The actual error instance.
    pub err: errs::Err,
}

/// A manager for coordinating concurrent tasks across threads, collecting errors and
/// awaiting completion.
///
/// Used during [`DataConn`] commit, pre-commit, post-commit, and rollback operations when multiple
/// data sources perform actions concurrently.
pub struct AsyncGroup {
    handlers: Vec<(usize, Arc<str>, thread::JoinHandle<errs::Result<()>>)>,
    index: usize,
    name: Arc<str>,
}

/// The cause of a transaction failure.
#[derive(Debug)]
pub enum TxnFailureCause {
    /// No failure occurred; the transaction was committed successfully.
    NoneByCommitted,
    /// No failure occurred; the transaction finished before commit without error.
    NoneByUncommitted,
    /// An error occurred during the execution of business logic.
    LogicFailure(errs::Err),
    /// An error occurred during the pre-commit phase ([`DataConn::pre_commit`]).
    PreCommitFailure(errs::Err),
    /// An error occurred during the commit phase ([`DataConn::commit`]).
    CommitFailure(errs::Err),
    /// An error occurred during the post-commit phase ([`DataConn::post_commit`]).
    PostCommitFailure(errs::Err),
}

/// The rollback status of a transaction failure.
#[derive(Debug)]
pub enum TxnFailureRollback {
    /// The rollback completed successfully.
    NoneByRolledBack,
    /// Rollback was not executed (or not needed).
    NoneByNotRolledBack,
    /// An error occurred during the rollback operation ([`DataConn::rollback`]).
    RollbackFailure(errs::Err),
}

/// Recommended recovery action following a transaction failure.
#[derive(Debug, PartialEq)]
pub enum TxnFailureRecovery {
    /// No action is required.
    NoActionRequired,
    /// Rerun the business logic and attempt to commit again.
    RerunLogicAndCommit,
    /// Resolve the root cause first, then rerun the business logic and commit.
    ResolveCauseThenRerunLogicAndCommit,
    /// Resolve the root cause first, then rerun the post-commit phase.
    ResolveCauseThenRerunPostCommit,
    /// Investigate and resolve both the root cause and any data inconsistency.
    ResolveCauseAndInconsistency,
    /// An unexpected or impossible state occurred; thorough investigation is needed.
    InvestigateBecauseImpossible,
    /// Automatic rollback did not succeed or was skipped; manual rollback is required.
    ManualRollbackRequired,
}

/// Report containing the failure cause and rollback outcome for a specific data connection
/// in a transaction.
#[derive(Debug)]
pub struct TxnFailureReport {
    /// The identifier name of the data connection.
    pub data_conn_name: Arc<str>,
    /// The concrete type name of the data connection.
    pub data_conn_type: &'static str,
    /// The cause of the transaction failure.
    pub cause: TxnFailureCause,
    /// The result of the rollback attempt.
    pub rollback: TxnFailureRollback,
}

struct SendSyncNonNull<T: Send + Sync> {
    non_null_ptr: ptr::NonNull<T>,
    _phantom: marker::PhantomData<cell::Cell<T>>,
}

/// Trait representing a connection to a data store, providing transaction boundaries
/// (commit, rollback, close, etc.).
pub trait DataConn {
    /// Commits the transaction on this data connection.
    ///
    /// # Arguments
    ///
    /// - `ag`: Mutable reference to [`AsyncGroup`] for registering asynchronous/concurrent tasks.
    ///
    /// # Returns
    ///
    /// - `Ok(())` on successful commit.
    /// - `Err(errs::Err)` if the commit fails.
    fn commit(&mut self, ag: &mut AsyncGroup) -> errs::Result<()>;

    /// Pre-commit phase executed immediately before committing.
    ///
    /// Default implementation is a no-op returning `Ok(())`.
    ///
    /// # Arguments
    ///
    /// - `ag`: Mutable reference to [`AsyncGroup`] for registering asynchronous/concurrent tasks.
    ///
    /// # Returns
    ///
    /// - `Ok(())` on success.
    /// - `Err(errs::Err)` if the pre-commit operation fails.
    #[allow(unused_variables)] // ag
    #[cfg_attr(coverage_nightly, coverage(off))]
    fn pre_commit(&mut self, ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }

    /// Post-commit phase executed immediately after committing.
    ///
    /// Default implementation is a no-op returning `Ok(())`.
    ///
    /// # Arguments
    ///
    /// - `ag`: Mutable reference to [`AsyncGroup`] for registering asynchronous/concurrent tasks.
    ///
    /// # Returns
    ///
    /// - `Ok(())` on success.
    /// - `Err(errs::Err)` if the post-commit operation fails.
    #[allow(unused_variables)] // ag
    #[cfg_attr(coverage_nightly, coverage(off))]
    fn post_commit(&mut self, ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }

    /// Returns whether this data connection has been committed.
    ///
    /// # Returns
    ///
    /// `true` if committed, `false` otherwise.
    fn is_committed(&self) -> bool;

    /// Rolls back the transaction on this data connection.
    ///
    /// # Arguments
    ///
    /// - `ag`: Mutable reference to [`AsyncGroup`] for registering asynchronous/concurrent tasks.
    ///
    /// # Returns
    ///
    /// - `Ok(())` on successful rollback.
    /// - `Err(errs::Err)` if the rollback operation fails.
    fn rollback(&mut self, ag: &mut AsyncGroup) -> errs::Result<()>;

    /// Callback invoked when a transaction fails.
    ///
    /// Receives failure reports for all participating data connections, allowing custom
    /// compensation or logging. Default implementation does nothing.
    ///
    /// # Arguments
    ///
    /// - `ag`: Mutable reference to [`AsyncGroup`] for registering asynchronous/concurrent tasks.
    /// - `reports`: Slice of [`TxnFailureReport`] describing failure causes and rollback outcomes
    ///   for all connections.
    #[allow(unused_variables)] // ag, reports
    #[cfg_attr(coverage_nightly, coverage(off))]
    fn on_txn_failure(&mut self, ag: &mut AsyncGroup, reports: &[TxnFailureReport]) {}

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
    abend_fn: fn(*const DataConnContainer),

    name: Arc<str>,
    data_conn: Box<C>,
}

struct DataConnManager {
    vec: Vec<Option<SendSyncNonNull<DataConnContainer>>>,
    index_map: HashMap<Arc<str>, usize>,
    ordered_names: Box<[Arc<str>]>,
    committed: bool,
}

/// Trait representing a data source that produces and manages the lifecycle of a [`DataConn`].
///
/// # Type Parameters
///
/// - `C`: The concrete type of [`DataConn`] produced by this data source
///   ([`DataConn`] + `'static`).
pub trait DataSrc<C>
where
    C: DataConn + 'static,
{
    /// Sets up and initializes the data source.
    ///
    /// # Arguments
    ///
    /// - `ag`: Mutable reference to [`AsyncGroup`] for registering asynchronous/concurrent
    ///   setup tasks.
    ///
    /// # Returns
    ///
    /// - `Ok(())` on successful initialization.
    /// - `Err(errs::Err)` if setup fails.
    fn setup(&mut self, ag: &mut AsyncGroup) -> errs::Result<()>;

    /// Closes the data source and releases its resources.
    fn close(&mut self);

    /// Creates a new data connection instance.
    ///
    /// # Returns
    ///
    /// - `Ok(Box<C>)` containing the new connection on success.
    /// - `Err(errs::Err)` if connection creation fails.
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
    is_abnormal: bool,
}

/// A guard struct that automatically closes global data sources when dropped.
///
/// When this value is dropped at the end of its scope, it triggers the cleanup phase and closes
/// all registered global [`DataSrc`] instances.
///
/// # Notes
///
/// Do not write `let _ = setup()?;` because the variable will be dropped immediately, causing
/// global data sources to be closed right away. Bind it to a named variable instead:
///
/// ```rust,ignore
/// let _auto_shutdown = sabi::setup()?;
/// ```
#[must_use = "global DataSrcs are closed when this value is dropped; bind it to a named variable"]
pub struct AutoShutdown {}

#[doc(hidden)]
pub struct StaticDataSrcContainer {
    ssnnptr: SendSyncNonNull<DataSrcContainer>,
}

#[doc(hidden)]
pub struct StaticDataSrcRegistration {
    factory: fn() -> StaticDataSrcContainer,
}

/// Initializes and sets up all registered global data sources.
///
/// # Returns
///
/// - `Ok(AutoShutdown)` on success. When dropped, the returned guard automatically closes the
///   global data sources.
/// - `Err(errs::Err)` if setup fails:
///   - [`DataSrcError::AlreadySetupGlobalDataSrcs`] if global data sources have already been
///     initialized.
///   - [`DataSrcError::DuringSetupGlobalDataSrcs`] if another thread is currently initializing
///     global data sources.
///   - [`DataSrcError::FailToSetupGlobalDataSrcs`] if one or more data sources failed during setup.
///
/// # Notes
///
/// Ensure the returned [`AutoShutdown`] is bound to a named variable and kept alive for the
/// lifetime of the application.
///
/// # Examples
///
/// ```rust,ignore
/// let _auto_shutdown = sabi::setup()?;
/// ```
#[inline]
pub fn setup() -> errs::Result<AutoShutdown> {
    global_setup::setup()
}

/// Initializes and sets up all registered global data sources in the specified order.
///
/// # Arguments
///
/// - `names`: Slice of data source names determining the setup order. Data sources not listed are
///   set up after these in their registration order.
///
/// # Returns
///
/// - `Ok(AutoShutdown)` on success.
/// - `Err(errs::Err)` on failure (see [`setup`]).
#[inline]
pub fn setup_with_order(names: &[&str]) -> errs::Result<AutoShutdown> {
    global_setup::setup_with_order(names)
}

/// Registers a global data source dynamically at runtime.
///
/// # Type Parameters
///
/// - `S`: The type of the data source ([`DataSrc<C>`]).
/// - `C`: The type of data connection produced by `S` ([`DataConn`] + `'static`).
///
/// # Arguments
///
/// - `name`: The identifier name for this data source (convertible to [`Arc<str>`]).
/// - `ds`: The data source instance to register.
///
/// # Returns
///
/// - `Ok(())` on successful registration.
/// - `Err(errs::Err)` with [`DataSrcError::FailToRegisterGlobalDataSrc`] if called after [`setup`]
///   or during cleanup.
///
/// # Notes
///
/// This must be called before [`setup`] or [`setup_with_order`].
#[inline]
pub fn uses<S, C>(name: impl Into<Arc<str>>, ds: S) -> errs::Result<()>
where
    S: DataSrc<C>,
    C: DataConn + 'static,
{
    global_setup::uses(name, ds)
}

/// Statically registers a global data source at compile time using the `inventory` crate.
///
/// The registered data source is collected and initialized when [`setup`] is called.
///
/// # Arguments
///
/// - `$name`: The identifier name of the data source (e.g. a string literal).
/// - `$data_src`: An expression evaluating to the data source instance.
///
/// # Examples
///
/// ```rust,ignore
/// sabi::uses!("db", MyDataSrc::new());
/// ```
#[macro_export]
macro_rules! uses {
    ($name:tt, $data_src:expr) => {
        const _: () = {
            inventory::submit! {
                $crate::StaticDataSrcRegistration::new(|| {
                    $crate::create_static_data_src_container($name, $data_src)
                })
            }
        };
    };
}

/// Central hub that manages data sources and connections, mediating between logic and data access.
///
/// Logic-specific data access traits ([`DataAcc`] derivatives) are implemented on `DataHub<T>`
/// to bridge business logic with underlying data connections.
///
/// # Type Parameters
///
/// - `T`: A marker type defining the data access scope or grouping.
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

/// Trait providing access to the underlying [`DataHub`].
pub trait AsDataHub {
    /// The marker type parameter of the associated [`DataHub`].
    type D: 'static;

    /// Returns a mutable reference to the underlying [`DataHub`].
    fn as_hub(&mut self) -> &mut DataHub<Self::D>;
}

/// Base trait defining and aggregating data access capabilities required by business logic.
///
/// Each use case defines a derived trait inheriting from `DataAcc` that declares only the
/// data operations required by that logic, using [`DataAcc::get_data_conn`] to retrieve
/// connections.
pub trait DataAcc: AsDataHub + Sized {
    /// Retrieves a mutable reference to a data connection of type `C` by name.
    ///
    /// If the connection has not yet been opened in the current session, it is created from
    /// the corresponding registered data source.
    ///
    /// # Type Parameters
    ///
    /// - `C`: The expected concrete connection type ([`DataConn`] + `'static`).
    ///
    /// # Arguments
    ///
    /// - `name`: The name identifying the data connection / data source.
    ///
    /// # Returns
    ///
    /// - `Ok(&mut C)` on success.
    /// - `Err(errs::Err)` if:
    ///   - No matching data source is found: [`DataHubError::NoDataSrcToCreateDataConn`].
    ///   - The connection cannot be downcast to `C`: [`DataConnError::FailToCastDataConn`].
    fn get_data_conn<C: DataConn + 'static>(&mut self, name: &str) -> errs::Result<&mut C>;

    /// Executes a business logic function passing `self` as the data access provider.
    ///
    /// # Type Parameters
    ///
    /// - `F`: The logic function type (`for<'a> FnMut(&'a mut Self) -> errs::Result<()>`).
    ///
    /// # Arguments
    ///
    /// - `logic_fn`: The business logic function to execute.
    ///
    /// # Returns
    ///
    /// The result of executing the logic function ([`errs::Result<()>`]).
    fn run<F>(&mut self, logic_fn: F) -> errs::Result<()>
    where
        F: for<'a> FnMut(&'a mut Self) -> errs::Result<()>;

    /// Initiates a chained logic execution via [`LogicRun`].
    ///
    /// # Returns
    ///
    /// A new [`LogicRun`] builder for chaining logic calls without transaction control.
    fn start(&mut self) -> LogicRun<'_, Self>;
}

enum LogicErrAt {
    Begin { err: errs::Err },
    Run { errors: Vec<ErrEntry> },
    Block { errors: Vec<ErrEntry> },
}

/// Builder for chaining multiple logic executions sequentially without transactional boundaries.
///
/// # Type Parameters
///
/// - `'a`: Lifetime of the borrow on the data access provider.
/// - `A`: The data access type implementing [`AsDataHub`].
///
/// # Notes
///
/// Always call [`LogicRun::end`] at the end of the chain to release data connections and
/// handle errors.
#[must_use = "call `end()` to finish the chain and release the data connections"]
pub struct LogicRun<'a, A> {
    hub: &'a mut A,
    err: LogicErrAt,
    index: usize,
    nested: bool,
}

/// Builder for chaining multiple logic executions sequentially within a transactional boundary.
///
/// # Type Parameters
///
/// - `'a`: Lifetime of the borrow on the [`DataHub`].
/// - `T`: Marker type parameter of the [`DataHub`].
///
/// # Notes
///
/// Always call [`LogicTxn::end_txn`] at the end of the chain to commit or roll back the transaction
/// and close data connections.
#[must_use = "call `end_txn()` to commit or roll back the transaction"]
pub struct LogicTxn<'a, T> {
    hub: &'a mut DataHub<T>,
    err: LogicErrAt,
    index: usize,
}

/// Runner that encapsulates a [`DataHub`] and manages session-local data sources and
/// logic executions.
///
/// Provides methods to register local data sources and run logic functions with or without
/// transactions.
///
/// # Type Parameters
///
/// - `T`: Marker type parameter of the underlying [`DataHub`].
pub struct LogicRunner<T> {
    hub: DataHub<T>,
}

fn panic_to_string(payload: &(dyn any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = payload.downcast_ref::<Arc<&str>>() {
        s.to_string()
    } else if let Some(s) = payload.downcast_ref::<Arc<str>>() {
        s.to_string()
    } else if let Some(s) = payload.downcast_ref::<Arc<String>>() {
        s.to_string()
    } else {
        "Unknown panic payload".to_string()
    }
}

// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

//! This crate provides a small framework for Rust designed to separate application logic from
//! data access.
//!
//! In this framework, a logic function takes only a logic-specific data access trait as its
//! argument, and all data access methods required by the logic are defined in this trait.
//!
//! On the other hand, concrete implementations of data access methods are provided as default
//! methods of traits derived from [`DataAcc`], and these traits are often grouped by data service
//! or other units of responsibility.
//!
//! [`DataHub`] bridges these two parts.
//!
//! A [`DataHub`] instance implements all [`DataAcc`]-derived traits required for each session, as
//! well as the data access traits used by the logic. Then, using the
//! [override_macro](https://github.com/sttk/override_macro-rust) crate, the implementations of
//! the methods in the data access traits used by the logic are “overridden” so that they call
//! the default methods of the [`DataAcc`]-derived traits with the same signatures.
//!
//! Rust does not have native method overriding, but this macro provides method calls similar to
//! overriding, allowing logic to access data through small, dedicated interfaces.
//!
//! [`LogicRunner`] wraps a [`DataHub`] and provides methods for configuring data sources and
//! executing business logic.
//!
//! [`LogicRunner::run`] passes the inner [`DataHub`] to a logic closure and executes it. Because
//! the argument type of the closure is declared as a logic-specific data access trait, the logic
//! can see only the methods defined by that trait, rather than all of the functionality provided
//! by [`DataHub`].
//!
//! [`LogicRunner::start`] creates a [`LogicRun`] builder that can execute multiple logic closures
//! using method chaining. [`LogicRun`] provides three execution methods:
//! [`run`][LogicRun::run], [`run_force`][LogicRun::run_force], and
//! [`run_or_block`][LogicRun::run_or_block].
//! [`run`][LogicRun::run] executes the logic only if no error has occurred in previous calls,
//! while [`run_force`][LogicRun::run_force] always executes the logic regardless of whether a
//! previous error has occurred. [`run_or_block`][LogicRun::run_or_block], like
//! [`run`][LogicRun::run], executes the logic only if no error has occurred, but if an error
//! occurs during its execution, subsequent calls to [`run`][LogicRun::run],
//! [`run_force`][LogicRun::run_force], and [`run_or_block`][LogicRun::run_or_block] are skipped.
//! Finally, calling [`end`][LogicRun::end] returns the result containing all errors that occurred
//! during execution.
//!
//! [`LogicRunner::txn`] executes a logic closure under transaction control and attempts to commit
//! if it succeeds. If the logic or the commit fails, it performs a rollback.
//! [`LogicRunner::begin_txn`] creates a [`LogicTxn`] builder that can execute multiple logic
//! closures using method chaining and then perform a commit or rollback with
//! [`end_txn`][LogicTxn::end_txn]. Like [`LogicRun`], [`LogicTxn`] provides
//! [`run`][LogicTxn::run], [`run_force`][LogicTxn::run_force], and
//! [`run_or_block`][LogicTxn::run_or_block], and their execution conditions are the same as those
//! of [`LogicRun`].
//!
//! This framework is designed to naturally lead to structures that follow the SOLID principles,
//! enabling the development of Rust applications that are easy for both humans and AI to implement
//! and understand.

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

/// The asynchronous variant of the framework utilizing the Tokio runtime.
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
#[cfg(feature = "tokio")]
pub mod tokio;

use std::collections::HashMap;
use std::sync::Arc;
use std::{any, cell, marker, ptr, thread};

/// Represents an error record containing contextual information about a failed task,
/// data connection, or data source.
#[derive(Debug)]
pub struct ErrEntry {
    /// The index of the task, connection, or execution step where the error occurred.
    pub index: usize,
    /// The name identifying the component, connection, or step that encountered the error.
    pub name: Arc<str>,
    /// The actual error that occurred.
    pub err: errs::Err,
}

/// Manages a group of concurrent tasks executed synchronously across worker threads.
///
/// This struct is passed to lifecycle methods such as [`DataSrc::setup`], [`DataConn::commit`],
/// and [`DataConn::rollback`] to allow data sources and connections to perform background work
/// concurrently. Tasks are added via [`AsyncGroup::add`](crate::AsyncGroup::add).
pub struct AsyncGroup {
    handlers: Vec<(usize, Arc<str>, thread::JoinHandle<errs::Result<()>>)>,
    _index: usize,
    _name: Arc<str>,
}

struct SendSyncNonNull<T: Send + Sync> {
    non_null_ptr: ptr::NonNull<T>,
    _phantom: marker::PhantomData<cell::Cell<T>>,
}

/// Represents the cause of a transaction failure for a specific data connection.
#[derive(Debug)]
pub enum TxnFailureCause {
    /// Indicates that no failure occurred and the connection committed successfully.
    NoneByCommitted,

    /// Indicates that no failure occurred on this connection, but it was left uncommitted
    /// (for example, rolled back because another participating connection failed).
    NoneByUncommitted,

    /// Indicates that the transaction failed during the execution of business logic or the
    /// [`DataConn::pre_commit`] phase.
    LogicFailure(errs::Err),

    /// Indicates that the transaction failed during the [`DataConn::commit`] phase.
    CommitFailure(errs::Err),

    /// Indicates that the transaction failed during the [`DataConn::post_commit`] phase.
    PostCommitFailure(errs::Err),
}

/// Represents the outcome of attempting to roll back a data connection.
#[derive(Debug)]
pub enum TxnFailureRollback {
    /// Indicates that the connection was rolled back successfully without error.
    NoneByRolledBack,

    /// Indicates that rollback was not performed on this connection (for example, because it
    /// had already successfully committed).
    NoneByNotRolledBack,

    /// Indicates that an error occurred while attempting to roll back the connection.
    RollbackFailure(errs::Err),
}

/// Indicates the recommended recovery action following a transaction failure.
#[derive(Debug, PartialEq)]
pub enum TxnFailureRecovery {
    /// No corrective action is needed.
    NoActionRequired,

    /// The business logic can be safely retried and committed.
    RerunLogicAndCommit,

    /// The underlying error cause should be resolved before retrying the business logic and
    /// committing.
    ResolveCauseThenRerunLogicAndCommit,

    /// The underlying error cause should be resolved, and post-commit processing should be rerun.
    ResolveCauseThenRerunPostCommit,

    /// The underlying cause and data inconsistencies between data sources must be resolved.
    ResolveCauseAndInconsistency,

    /// The reported state is contradictory or invalid; investigation is required.
    InvestigateBecauseImpossible,

    /// The connection committed before failure occurred; manual compensation or rollback
    /// is required.
    ManualRollbackRequired,
}

/// Detailed diagnostic report for a data connection involved in a failed transaction.
#[derive(Debug)]
pub struct TxnFailureReport {
    /// The registered name of the data connection.
    pub data_conn_name: Arc<str>,
    /// The static type name of the data connection.
    pub data_conn_type: &'static str,
    /// The identified cause of failure for this connection.
    pub cause: TxnFailureCause,
    /// The outcome of the rollback operation on this connection.
    pub rollback: TxnFailureRollback,
}

/// Defines the lifecycle and transactional operations for a data connection.
///
/// Implementors represent active connections or transactional units of work (such as database
/// transactions or client sessions). A `DataConn` is created by a [`DataSrc`] and managed by
/// [`DataHub`] during logic execution.
#[allow(unused_variables)]
pub trait DataConn {
    /// Commits changes made through this connection.
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
    fn commit(&mut self, ag: &mut AsyncGroup) -> errs::Result<()>;

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
    fn pre_commit(&mut self, ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
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
    fn post_commit(&mut self, ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }

    /// Returns whether this data connection has already successfully committed.
    ///
    /// # Returns
    ///
    /// `true` if the connection has completed its commit; otherwise `false`.
    fn is_committed(&self) -> bool;

    /// Rolls back any changes made through this connection.
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
    fn rollback(&mut self, ag: &mut AsyncGroup) -> errs::Result<()>;

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
    fn on_txn_failure(&mut self, ag: &mut AsyncGroup, reports: &[TxnFailureReport]) {}

    /// Closes the connection and releases underlying resources.
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

/// Defines a provider that configures and creates [`DataConn`] instances.
///
/// Implementors represent data sources such as database connection pools or API clients.
/// Data sources can be registered globally via [`uses()`] or locally on a [`LogicRunner`].
///
/// # Type Parameters
///
/// * `C`: The concrete [`DataConn`] type produced by this data source.
pub trait DataSrc<C>
where
    C: DataConn + 'static,
{
    /// Initializes and prepares the data source for creating connections.
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
    fn setup(&mut self, ag: &mut AsyncGroup) -> errs::Result<()>;

    /// Closes the data source and releases its resources.
    fn close(&mut self);

    /// Creates a new [`DataConn`] instance for accessing data.
    ///
    /// # Returns
    ///
    /// A boxed [`DataConn`] implementation instance.
    ///
    /// # Errors
    ///
    /// Returns an error if connection creation fails.
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

/// RAII guard that automatically closes and shuts down global data sources when dropped.
///
/// Returned by [`setup`] and [`setup_with_order`].
pub struct AutoShutdown {}

/// Container for a statically registered data source.
#[doc(hidden)]
pub struct StaticDataSrcContainer {
    ssnnptr: SendSyncNonNull<DataSrcContainer>,
}

/// Registration record for a static data source factory.
#[doc(hidden)]
pub struct StaticDataSrcRegistration {
    factory: fn() -> StaticDataSrcContainer,
}

/// The central coordinator providing access to data connections during logic execution.
///
/// An instance of `DataHub` is passed to business logic closures executed via [`DataAcc::run`],
/// [`LogicRunner::run`], [`LogicRunner::txn`], etc. It allows retrieving typed [`DataConn`]
/// instances by name.
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

/// Trait for accessing data connections and executing business logic against a [`DataHub`].
pub trait DataAcc {
    /// The associated domain marker type used by [`DataHub`].
    type D: any::Any;

    /// Retrieves a mutable reference to a named data connection of type `C`.
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
    fn get_data_conn<C: DataConn + 'static>(&mut self, name: &str) -> errs::Result<&mut C>;

    /// Executes a business logic closure using the `DataHub`.
    ///
    /// # Type Parameters
    ///
    /// * `F`: The closure type, implementing `FnMut(&mut DataHub<Self::D>) -> errs::Result<()>`.
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
    fn run<F>(&mut self, logic_fn: F) -> errs::Result<()>
    where
        F: FnMut(&mut DataHub<Self::D>) -> errs::Result<()>;

    /// Starts a builder for executing a chain of non-transactional business logic steps.
    ///
    /// # Returns
    ///
    /// A [`LogicRun`] builder instance.
    fn start(&mut self) -> LogicRun<'_, Self::D>;
}

/// Builder for chaining and executing non-transactional business logic steps on a [`DataHub`].
///
/// Created via [`DataAcc::start`] or [`LogicRunner::start`]. Provides methods like
/// [`run`](LogicRun::run), [`run_force`](LogicRun::run_force),
/// [`run_or_block`](LogicRun::run_or_block), and [`end`](LogicRun::end).
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

/// Builder for chaining and executing transactional business logic steps on a [`DataHub`].
///
/// Created via [`LogicRunner::begin_txn`]. Provides methods like
/// [`run`](LogicTxn::run), [`run_force`](LogicTxn::run_force),
/// [`run_or_block`](LogicTxn::run_or_block), and [`end_txn`](LogicTxn::end_txn).
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

enum LogicErrAt {
    Begin { err: errs::Err },
    Run { errors: Vec<ErrEntry> },
    Block { errors: Vec<ErrEntry> },
}

/// Configures data sources and executes business logic and transactions on a [`DataHub`].
///
/// # Type Parameters
///
/// * `T`: The domain marker type of the underlying [`DataHub`].
pub struct LogicRunner<T> {
    hub: DataHub<T>,
}

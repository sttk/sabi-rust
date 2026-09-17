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
//! The [`DataHub`] implements all, or the [`DataAcc`]-derived traits required for each session, as
//! well as the data access traits used by the logic. Then, using the
//! [override_macro](https://github.com/sttk/override_macro-rust) crate, the
//! implementations of the methods in the data access traits used by the logic are “overridden” so
//! that they call the default methods of the [`DataAcc`]-derived traits with the same signatures.
//!
//! Rust does not have native method overriding, but this macro provides method calls similar to
//! overriding, allowing logic to access data through small, dedicated interfaces.
//!
//! In addition, [`DataHub`] provides methods for executing logic functions.
//!
//! [`DataHub::run`] passes itself to a logic function and executes it. Because the argument type of
//! the logic function is declared as a logic-specific data access trait, the logic can see only
//! the methods defined by this trait, rather than all of the functionality provided by [`DataHub`].
//!
//! [`DataHub::start`] creates a [`Runner`] instance that can execute multiple logic functions using
//! method chaining. [`Runner`] provides three execution methods: [`run`][Runner::run],
//! [`run_force`][Runner::run_force], and [`run_or_block`][Runner::run_or_block].
//! [`run`][Runner::run] executes the logic only if no error has occurred in previous calls, while
//! [`run_force`][Runner::run_force] always executes the logic regardless of whether a previous
//! error has occurred. [`run_or_block`][Runner::run_or_block], like [`run`][Runner::run], executes
//! the logic only if no error has occurred, but if an error occurs during its execution,
//! subsequent calls to [`run`][Runner::run], [`run_force`][Runner::run_force], and
//! [`run_or_block`][Runner::run_or_block] are skipped. Finally, calling [`end`][Runner::end]
//! returns the result containing all errors that occurred during execution.
//!
//! In addition, [`DataHub::for_txn`] and [`DataHub::for_txn_with_commit_order`] creates a
//! [`TxnDataHub`] instance that can execute logic functions under transaction control.
//! [`TxnDataHub::run`] and [`TxnDataHub::start`] work in the same way as the corresponding methods
//! of [`DataHub`]. [`TxnDataHub::txn`] executes a logic function and attempts to commit if it
//! succeeds. If the logic or the commit fails, it performs a rollback.
//! [`TxnDataHub::begin_txn`] creates a [`Txn`] instance that can execute multiple logic
//! functions using method chaining and then perform a commit or rollback with [`Txn::end_txn`].
//! Like [`Runner`], [`Txn`] provides [`run`][Txn::run], [`run_force`][Txn::run_force], and
//! [`run_or_block`][Txn::run_or_block], and their execution conditions are the same as those of
//! [`Runner`].
//!
//! This framework is designed to naturally lead to structures that follow the SOLID principles,
//! enabling the development of Rust applications that are easy for both humans and AI to implement
//! and understand.

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(unused_features)]

mod async_group;
mod data_acc;
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

/// Represents an entry containing an error, along with its context.
///
/// This structure is used to aggregate errors that occur during parallel
/// or asynchronous operations, providing the index of the operation,
/// a descriptive name, and the error itself.
#[derive(Debug)]
pub struct ErrEntry {
    /// The index of the operation or handler that generated the error.
    pub index: usize,
    /// A descriptive name for the operation or context.
    pub name: Arc<str>,
    /// The actual error that occurred.
    pub err: errs::Err,
}

/// The structure that allows for the concurrent execution of multiple functions
/// using `std::thread` and waits for all of them to complete.
///
/// Functions are added using the `add` method and are then run concurrently in separate threads.
/// The `AsyncGroup` ensures that all tasks finish before proceeding,
/// and can collect any errors that occur.
pub struct AsyncGroup {
    handlers: Vec<(usize, Arc<str>, thread::JoinHandle<errs::Result<()>>)>,
    _index: usize,
    _name: Arc<str>,
}

/// The trait that abstracts a connection per session to an external data service,
/// such as a database, file system, or messaging service.
///
/// Its primary purpose is to enable cohesive transaction operations across multiple
/// external data services within a single transaction context. Implementations of this
/// trait provide the concrete input/output operations for their respective data services.
///
/// Methods declared within this trait are designed to handle transactional logic.
/// The [`AsyncGroup`] parameter in various methods allows for concurrent processing
/// when commit or rollback operations are time-consuming.
#[allow(unused_variables)] // rustdoc
pub trait DataConn {
    /// Attempts to commit the changes made within this data connection's transaction.
    ///
    /// This method should encapsulate the logic required to finalize the transaction
    /// for the specific external data service.
    ///
    /// # Parameters
    ///
    /// * `ag`: A mutable reference to an [`AsyncGroup`] for potentially offloading
    ///   time-consuming commit operations to a separate thread.
    ///
    /// # Returns
    ///
    /// * `errs::Result<()>`: `Ok(())` if the commit is successful, or an [`errs::Err`]
    ///   if the commit fails.
    fn commit(&mut self, ag: &mut AsyncGroup) -> errs::Result<()>;

    /// This method is executed before the transaction commit process for all [`DataConn`] instances
    /// involved in the transaction.
    ///
    /// This method provides a timing to execute unusual commit processes or update operations not
    /// supported by transactions beforehand.
    /// This allows other update operations to be rolled back if the operations in this method
    /// fail.
    ///
    /// # Parameters
    ///
    /// * `ag`: A mutable reference to an [`AsyncGroup`]. This can be used to run the pre-commit
    ///   asynchronously in a separate thread.
    ///
    /// # Returns
    ///
    /// * `errs::Result<()>`: `Ok(())` if pre-commit is successful, or an [`errs::Err`] if it fails.
    #[cfg_attr(coverage_nightly, coverage(off))]
    fn pre_commit(&mut self, ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }

    /// This method is executed after the transaction commit process has successfully completed
    /// for all [`DataConn`] instances involved in the transaction.
    ///
    /// It provides a moment to perform follow-up actions that depend on a successful commit.
    /// For example, after a database commit, a messaging service's [`DataConn`] might use this
    /// method to send a "transaction completed" message.
    ///
    /// # Parameters
    ///
    /// * `ag`: A mutable reference to an [`AsyncGroup`] for potentially offloading
    ///   concurrent post-commit operations.
    ///
    /// # Returns
    ///
    /// * `errs::Result<()>`: `Ok(())` if post-commit tasks succeed, or an [`errs::Err`] if they
    ///   fail.
    #[cfg_attr(coverage_nightly, coverage(off))]
    fn post_commit(&mut self, ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
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
    /// * `ag`: A mutable reference to an [`AsyncGroup`]. This can be used to run the rollback
    ///   asynchronously in a separate thread.
    ///
    /// # Returns
    ///
    /// * `errs::Result<()>`: `Ok(())` if the rollback is successful, or an [`errs::Err`] if it
    ///   fails.
    fn rollback(&mut self, ag: &mut AsyncGroup) -> errs::Result<()>;

    /// A lifecycle callback invoked when a transaction fails and a rollback is executed.
    ///
    /// This allows the data connection to handle post-failure tasks or custom logic
    /// based on the provided transaction failure reports.
    ///
    /// # Parameters
    ///
    /// * `ag`: A mutable reference to an [`AsyncGroup`] for asynchronous task execution.
    /// * `reports`: A slice of [`TxnFailureReport`] containing failure details for all connections.
    #[cfg_attr(coverage_nightly, coverage(off))]
    fn on_txn_failure(&mut self, ag: &mut AsyncGroup, reports: &[TxnFailureReport]) {}

    /// Closes the connection to the external data service.
    ///
    /// This method should release any resources held by the data connection, ensuring
    /// a graceful shutdown of the connection.
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

/// The trait that abstracts a data source responsible for managing connections
/// to external data services, such as databases, file systems, or messaging services.
///
/// It receives configuration for connecting to an external data service and then
/// creates and supplies [`DataConn`] instance, representing a single session connection.
#[allow(unused_variables)] // for rustdoc
pub trait DataSrc<C>
where
    C: DataConn + 'static,
{
    /// Performs the setup process for the data source.
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
    fn setup(&mut self, ag: &mut AsyncGroup) -> errs::Result<()>;

    /// Closes the data source and releases any globally held resources.
    ///
    /// This method should perform cleanup operations, such as closing global connections
    /// or shutting down connection pools, that were established during the setup process.
    fn close(&mut self);

    /// Creates a new [`DataConn`] instance which is a connection per session.
    ///
    /// Each call to this method should yield a distinct [`DataConn`] object tailored
    /// for a single session's operations.
    ///
    /// # Returns
    ///
    /// * `errs::Result<Box<C>>`: `Ok(Box<C>)` containing the newly created [`DataConn`]
    ///   if successful, or an [`errs::Err`] if the connection could not be created.
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
    fn get_data_conn<C: DataConn + 'static>(&mut self, name: &str) -> errs::Result<&mut C>;

    /// Executes a logic function with this data-access context.
    ///
    /// The logic function receives a mutable reference to the underlying [`DataHub`].
    ///
    /// # Parameters
    ///
    /// * `logic_fn`: A closure that encapsulates the business logic to be executed.
    ///   It takes a mutable reference to [`DataHub`] as an argument.
    ///
    /// # Returns
    ///
    /// * `errs::Result<()>`: The result of the logic function's execution,
    ///   or an error if executing `logic_fn` fails.
    fn run<F>(&mut self, logic_fn: F) -> errs::Result<()>
    where
        F: FnMut(&mut DataHub) -> errs::Result<()>;

    /// Creates a [`Runner`] for executing multiple logic functions.
    ///
    /// The returned [`Runner`] can execute logic functions using method chaining and collect
    /// errors from their execution.
    ///
    /// # Returns
    ///
    /// * `Runner`: The struct which execute logic functions using method chaining.
    fn start(&mut self) -> Runner<'_>;
}

enum RunnerErrAt {
    Begin { err: errs::Err },
    Run { errors: Vec<ErrEntry> },
    Block { errors: Vec<ErrEntry> },
}

/// Executes multiple logic functions using method chaining.
///
/// A [`Runner`] executes logic functions with a [`DataHub`] and collects errors that occur during
/// execution. It manages the data hub session until [`Runner::end`] is called.
pub struct Runner<'a> {
    hub: &'a mut DataHub,
    err: RunnerErrAt,
    index: usize,
    nested: bool,
}

/// Provides a [`DataHub`] which enables data access and transaction control.
///
/// A [`TxnDataHub`] owns a [`DataHub`] and allows logic functions to be executed either normally
/// or under transaction control.
pub struct TxnDataHub {
    hub: DataHub,
}

/// Executes multiple logic functions within a transaction using method chaining.
///
/// A [`Txn`] begins a transaction when it is created and keeps the transaction active until
/// [`Txn::end_txn`] is called.
pub struct Txn<'a> {
    hub: &'a mut DataHub,
    err: RunnerErrAt,
    index: usize,
}

#[doc(hidden)]
pub struct StaticDataSrcContainer {
    ssnnptr: SendSyncNonNull<DataSrcContainer>,
}

#[doc(hidden)]
pub struct StaticDataSrcRegistration {
    factory: fn() -> StaticDataSrcContainer,
}

struct SendSyncNonNull<T: Send + Sync> {
    non_null_ptr: ptr::NonNull<T>,
    _phantom: marker::PhantomData<cell::Cell<T>>,
}

/// Represents the cause of a transaction failure for a specific data connection.
#[derive(Debug)]
pub enum TxnFailureCause {
    /// No failure occurred, and the transaction was successfully committed.
    NoneByCommitted,
    /// No failure occurred, but the transaction was not committed (e.g., because
    /// another connection in the transaction failed before this one could commit).
    NoneByUncommitted,
    /// The logic execution or pre-commit phase of the data connection failed.
    LogicFailure(errs::Err),
    /// The commit phase of the data connection failed.
    CommitFailure(errs::Err),
    /// The post-commit phase of the data connection failed.
    PostCommitFailure(errs::Err),
}

/// Represents the rollback status of a data connection in a failed transaction.
#[derive(Debug)]
pub enum TxnFailureRollback {
    /// The rollback was executed and succeeded.
    NoneByRolledBack,
    /// Rollback was not executed or not applicable (e.g., because the connection
    /// was already committed).
    NoneByNotRolledBack,
    /// The rollback was executed but failed.
    RollbackFailure(errs::Err),
}

/// Represents the suggested recovery action for a data connection after a transaction failure.
#[derive(Debug, PartialEq)]
pub enum TxnFailureRecovery {
    /// No recovery action is required.
    NoActionRequired,
    /// The transaction was successfully rolled back.
    /// The transaction can be rerun a logic and committed again.
    RerunLogicAndCommit,
    /// The transaction failed to run a logic or pre commit.
    /// After resolving the cause, the transaction can be rerun a logic and commit again.
    ResolveCauseThenRerunLogicAndCommit,
    /// The transaction failed to run post commit.
    /// After resolving the cause, the transaction can be rerun post commit again.
    ResolveCauseThenRerunPostCommit,
    /// The rollback failed and it may be in an inconsistent state.
    /// Resolve the cause and inconsistent state.
    ResolveCauseAndInconsistency,
    /// It is in impossile case under normal conditions.
    /// Investigation of the cause is required.
    InvestigateBecauseImpossible,
    /// The transaction was successfully committed.
    /// It is required to rollback manually.
    ManualRollbackRequired,
}

/// A report detailing the transaction failure cause and rollback status
/// for a specific data connection.
#[derive(Debug)]
pub struct TxnFailureReport {
    /// The name of the data connection.
    pub data_conn_name: Arc<str>,
    /// The type name of the data connection.
    pub data_conn_type: &'static str,
    /// The cause of the transaction failure.
    pub cause: TxnFailureCause,
    /// The rollback status of the data connection.
    pub rollback: TxnFailureRollback,
}

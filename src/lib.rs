// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

//! This crate provides a small framework for Rust, designed to separate application logic
//! from data access.
//!
//! In this framework, the logic exclusively takes a data access trait as its argument,
//! and all necessary data access is defined by a single data access trait.
//! Conversely, the concrete implementations of data access methods are provided as default methods
//! of `DataAcc` derived traits, allowing for flexible grouping, often by data service.
//!
//! The `DataHub` bridges these two parts.
//! It attaches all `DataAcc` derived traits, and then, using the
//! [override_macro](https://github.com/sttk/override_macro-rust) crate, it overrides
//! the methods of the data access trait used by the logic to point to the implementations
//! found in the `DataAcc` derived traits.
//! This clever use of this macro compensates for Rust's lack of native method overriding,
//! allowing the logic to interact with data through an abstract interface.
//!
//! Furthermore, the `DataHub` provides transaction control for data operations performed
//! within the logic.
//! You can execute logic functions with transaction control using its [`DataHub::txn`] method,
//! or without transaction control using its [`DataHub::run`] method.
//!
//! This framework brings clear separation and robustness to Rust application design.

mod async_group;
mod data_acc;
mod data_conn;
mod data_hub;
mod data_src;
mod non_null;

use std::collections::HashMap;
use std::sync::Arc;
use std::{any, cell, marker, ptr, thread};

pub use async_group::AsyncGroupError;
pub use data_conn::DataConnError;
pub use data_hub::DataHubError;
pub use data_src::{create_static_data_src_container, setup, setup_with_order, uses, DataSrcError};

#[cfg(feature = "tokio")]
pub mod tokio;

/// The structure that allows for the concurrent execution of multiple functions
/// using `std::thread` and waits for all of them to complete.
///
/// Functions are added using the `add` method and are then run concurrently in separate threads.
/// The `AsyncGroup` ensures that all tasks finish before proceeding,
/// and can collect any errors that occur.
pub struct AsyncGroup {
    handlers: Vec<(Arc<str>, thread::JoinHandle<errs::Result<()>>)>,
    pub(crate) _name: Arc<str>,
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
    fn post_commit(&mut self, ag: &mut AsyncGroup) {}

    /// Determines whether a "force back" operation is required for this data connection.
    ///
    /// A force back is typically executed if one external data service successfully commits
    /// its changes, but a subsequent external data service within the same transaction fails
    /// its commit. This method indicates if the committed changes of *this* data service
    /// need to be undone (forced back).
    ///
    /// # Returns
    ///
    /// * `bool`: `true` if a force back is needed for this connection, `false` otherwise.
    fn should_force_back(&self) -> bool {
        false
    }

    /// Rolls back any changes made within this data connection's transaction.
    ///
    /// This method undoes all operations performed since the beginning of the transaction,
    /// restoring the data service to its state before the transaction began.
    ///
    /// # Parameters
    ///
    /// * `ag`: A mutable reference to an [`AsyncGroup`] for potentially offloading
    ///   time-consuming rollback operations to a separate thread.
    fn rollback(&mut self, ag: &mut AsyncGroup);

    /// Executes an operation to revert committed changes.
    ///
    /// This method provides an opportunity to undo changes that were successfully committed
    /// to this external data service, typically when a commit fails for *another* data service
    /// within the same distributed transaction, necessitating a rollback of already committed
    /// changes.
    ///
    /// # Parameters
    ///
    /// * `ag`: A mutable reference to an [`AsyncGroup`] for potentially offloading
    ///   concurrent force back operations.
    fn force_back(&mut self, ag: &mut AsyncGroup) {}

    /// Closes the connection to the external data service.
    ///
    /// This method should release any resources held by the data connection, ensuring
    /// a graceful shutdown of the connection.
    fn close(&mut self);
}

struct NoopDataConn {}

impl DataConn for NoopDataConn {
    fn commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }
    fn rollback(&mut self, _ag: &mut AsyncGroup) {}
    fn close(&mut self) {}
}

#[repr(C)]
struct DataConnContainer<C = NoopDataConn>
where
    C: DataConn + 'static,
{
    drop_fn: fn(*const DataConnContainer),
    is_fn: fn(any::TypeId) -> bool,
    commit_fn: fn(*const DataConnContainer, &mut AsyncGroup) -> errs::Result<()>,
    pre_commit_fn: fn(*const DataConnContainer, &mut AsyncGroup) -> errs::Result<()>,
    post_commit_fn: fn(*const DataConnContainer, &mut AsyncGroup),
    should_force_back_fn: fn(*const DataConnContainer) -> bool,
    rollback_fn: fn(*const DataConnContainer, &mut AsyncGroup),
    force_back_fn: fn(*const DataConnContainer, &mut AsyncGroup),
    close_fn: fn(*const DataConnContainer),

    name: Arc<str>,
    data_conn: Box<C>,
}

struct DataConnManager {
    vec: Vec<Option<ptr::NonNull<DataConnContainer>>>,
    index_map: HashMap<Arc<str>, usize>,
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
    fn get_data_conn<C: DataConn + 'static>(
        &mut self,
        name: impl AsRef<str>,
    ) -> errs::Result<&mut C>;
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

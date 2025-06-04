// Copyright (C) 2024-2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use std::any;

use errs::Err;

mod async_group;
pub use async_group::{AsyncGroup, AsyncGroupError};

mod data_acc;
mod data_conn;
mod data_hub;
mod data_src;
pub use data_acc::DataAcc;
pub use data_hub::{setup, shutdown, shutdown_later, uses, DataHub, DataHubError};

/// The trait that abstracts a connection per session to an external data service,
/// such as a database, file system, or messaging service.
///
/// Its primary purpose is to enable cohesive transaction operations across multiple
/// external data services within a single transaction context. Implementations of this
/// trait provide the concrete input/output operations for their respective data services.
///
/// Methods declared within this trait are designed to handle transactional logic.
/// The `AsyncGroup` parameter in various methods allows for asynchronous processing
/// when commit or rollback operations are time-consuming.
#[allow(unused_variables)] // for rustdoc
pub trait DataConn {
    /// Attempts to commit the changes made within this data connection's transaction.
    ///
    /// This method should encapsulate the logic required to finalize the transaction
    /// for the specific external data service.
    ///
    /// # Arguments
    ///
    /// * `ag`: A mutable reference to an `AsyncGroup` for potentially offloading
    ///   time-consuming commit operations to an asynchronous runtime.
    ///
    /// # Returns
    ///
    /// * `Result<(), Err>`: `Ok(())` if the commit is successful, or an `Err`
    ///   if the commit fails.
    fn commit(&mut self, ag: &mut AsyncGroup) -> Result<(), Err>;

    /// This method is executed after the transaction commit process has successfully completed
    /// for all `DataConn` instances involved in the transaction.
    ///
    /// It provides a moment to perform follow-up actions that depend on a successful commit.
    /// For example, after a database commit, a messaging service's `DataConn` might use this
    /// method to send a "transaction completed" message.
    ///
    /// # Arguments
    ///
    /// * `ag`: A mutable reference to an `AsyncGroup` for potentially offloading
    ///   asynchronous post-commit operations.
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
    /// # Arguments
    ///
    /// * `ag`: A mutable reference to an `AsyncGroup` for potentially offloading
    ///   time-consuming rollback operations to an asynchronous runtime.
    fn rollback(&mut self, ag: &mut AsyncGroup);

    /// Executes an operation to revert committed changes.
    ///
    /// This method provides an opportunity to undo changes that were successfully committed
    /// to this external data service, typically when a commit fails for *another* data service
    /// within the same distributed transaction, necessitating a rollback of already committed
    /// changes.
    ///
    /// # Arguments
    ///
    /// * `ag`: A mutable reference to an `AsyncGroup` for potentially offloading
    ///   asynchronous force back operations.
    fn force_back(&mut self, ag: &mut AsyncGroup) {}

    /// Closes the connection to the external data service.
    ///
    /// This method should release any resources held by the data connection, ensuring
    /// a graceful shutdown of the connection.
    fn close(&mut self);
}

struct NoopDataConn {}

impl DataConn for NoopDataConn {
    fn commit(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
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

    commit_fn: fn(*const DataConnContainer, &mut AsyncGroup) -> Result<(), Err>,
    post_commit_fn: fn(*const DataConnContainer, &mut AsyncGroup),
    should_force_back_fn: fn(*const DataConnContainer) -> bool,
    rollback_fn: fn(*const DataConnContainer, &mut AsyncGroup),
    force_back_fn: fn(*const DataConnContainer, &mut AsyncGroup),
    close_fn: fn(*const DataConnContainer),

    prev: *mut DataConnContainer,
    next: *mut DataConnContainer,
    name: String,
    data_conn: Box<C>,
}

/// The trait that abstracts a data source responsible for managing connections
/// to external data services, such as databases, file systems, or messaging services.
///
/// It receives configuration for connecting to an external data service and then
/// creates and supplies `DataConn` instance, representing a single session connection.
#[allow(unused_variables)] // for rustdoc
pub trait DataSrc<C>
where
    C: DataConn + 'static,
{
    /// Performs the setup process for the data source.
    ///
    /// This method is responsible for establishing global connections, configuring
    /// connection pools, or performing any necessary initializations required
    /// before `DataConn` instances can be created.
    ///
    /// # Parameters
    ///
    /// * `ag`: A mutable reference to an `AsyncGroup`. This is used if the setup
    ///   process is potentially time-consuming and can benefit from asynchronous
    ///   execution.
    ///
    /// # Returns
    ///
    /// * `Result<(), Err>`: `Ok(())` if the setup is successful, or an `Err`
    ///   if any part of the setup fails.
    fn setup(&mut self, ag: &mut AsyncGroup) -> Result<(), Err>;

    /// Closes the data source and releases any globally held resources.
    ///
    /// This method should perform cleanup operations, such as closing global connections
    /// or shutting down connection pools, that were established during the `setup` phase.
    fn close(&mut self);

    /// Creates a new `DataConn` instance which is a connection per session.
    ///
    /// Each call to this method should yield a distinct `DataConn` object tailored
    /// for a single session's operations.
    ///
    /// # Returns
    ///
    /// * `Result<Box<C>, Err>`: `Ok(Box<C>)` containing the newly created `DataConn`
    ///   if successful, or an `Err` if the connection could not be created.
    fn create_data_conn(&mut self) -> Result<Box<C>, Err>;
}

struct NoopDataSrc {}

impl DataSrc<NoopDataConn> for NoopDataSrc {
    fn setup(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
        Ok(())
    }
    fn close(&mut self) {}
    fn create_data_conn(&mut self) -> Result<Box<NoopDataConn>, Err> {
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
    setup_fn: fn(*const DataSrcContainer, &mut AsyncGroup) -> Result<(), Err>,
    close_fn: fn(*const DataSrcContainer),
    create_data_conn_fn: fn(*const DataSrcContainer) -> Result<Box<DataConnContainer<C>>, Err>,
    is_data_conn_fn: fn(any::TypeId) -> bool,

    prev: *mut DataSrcContainer,
    next: *mut DataSrcContainer,
    local: bool,
    name: String,

    data_src: S,
}

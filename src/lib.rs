// Copyright (C) 2024-2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

//! This crate provides a small framework for Rust, designed to separate application logic
//! from data access.
//!
//! In this framework, the logic exclusively takes a data access trait as its argument,
//! and all necessary data access is defined by a single data access trait.
//! Conversely, the concrete implementations of data access methods are provided as default methods
//! of DataAcc derived traits, allowing for flexible grouping, often by data service.
//!
//! The `DataHub` bridges these two parts.
//! It attaches all DataAcc derived traits, and then, using the
//! [override_macro](https://github.com/sttk/override_macro-rust) crate, it overrides
//! the methods of the data access trait used by the logic to point to the implementations
//! found in the `DataAcc` derived traits.
//! This clever use of this macro compensates for Rust's lack of native method overriding,
//! allowing the logic to interact with data through an abstract interface.
//!
//! Furthermore, the `DataHub` provides transaction control for data operations performed
//! within the logic.
//! You can execute logic functions with transaction control using the `DataHub#txn` method,
//! or without it using the `DataHub#run` method.
//! This framework brings clear separation and robustness to Rust application design.
//!
//! ## Example
//!
//! The following is a sample code using this framework:
//!
//! ```
//! use sabi::{uses, setup, shutdown_later, AsyncGroup, DataSrc, DataConn, DataAcc, DataHub};
//! use errs::Err;
//! use override_macro::{overridable, override_with};
//!
//! // (1) Implements DataSrc(s) and DataConn(s).
//!
//! struct CertainGlobalDataSrc { /* ... */ }
//!
//! impl DataSrc<CertainGlobalDataConn> for CertainGlobalDataSrc {
//!     fn setup(&mut self, ag: &mut AsyncGroup) -> Result<(), Err> { /* ... */ Ok(()) }
//!     fn close(&mut self) { /* ... */ }
//!     fn create_data_conn(&mut self) -> Result<Box<CertainGlobalDataConn>, Err> {
//!         Ok(Box::new(CertainGlobalDataConn{ /* ... */ }))
//!     }
//! }
//!
//! struct CertainGlobalDataConn { /* ... */ }
//!
//! impl CertainGlobalDataConn { /* .. */ }
//!
//! impl DataConn for CertainGlobalDataConn {
//!     fn commit(&mut self, ag: &mut AsyncGroup) -> Result<(), Err> { /* ... */ Ok(()) }
//!     fn rollback(&mut self, ag: &mut AsyncGroup) { /* ... */ }
//!     fn close(&mut self) { /* ... */ }
//! }
//!
//! struct GettingDataSrc { /* ... */ }
//!
//! impl DataSrc<GettingDataConn> for GettingDataSrc {
//!     fn setup(&mut self, ag: &mut AsyncGroup) -> Result<(), Err> { /* ... */ Ok(()) }
//!     fn close(&mut self) { /* ... */ }
//!     fn create_data_conn(&mut self) -> Result<Box<GettingDataConn>, Err> {
//!         Ok(Box::new(GettingDataConn{ /* ... */ }))
//!     }
//! }
//!
//! struct GettingDataConn { /* ... */ }
//!
//! impl GettingDataConn {
//!     fn get_text(&self) -> Result<String, Err> { /* ... */ Ok("...".to_string()) }
//! }
//!
//! impl DataConn for GettingDataConn {
//!     fn commit(&mut self, ag: &mut AsyncGroup) -> Result<(), Err> { /* ... */ Ok(()) }
//!     fn rollback(&mut self, ag: &mut AsyncGroup) { /* ... */ }
//!     fn close(&mut self) { /* ... */ }
//! }
//!
//! struct SettingDataSrc { /* ... */ }
//!
//! impl DataSrc<SettingDataConn> for SettingDataSrc {
//!     fn setup(&mut self, ag: &mut AsyncGroup) -> Result<(), Err> { /* ... */ Ok(()) }
//!     fn close(&mut self) { /* ... */ }
//!     fn create_data_conn(&mut self) -> Result<Box<SettingDataConn>, Err> {
//!         Ok(Box::new(SettingDataConn{ /* ... */ }))
//!     }
//! }
//!
//! struct SettingDataConn { /* ... */ }
//!
//! impl SettingDataConn {
//!     fn set_text(&self, text: String) -> Result<(), Err> { /* ... */ Ok(()) }
//! }
//!
//! impl DataConn for SettingDataConn {
//!     fn commit(&mut self, ag: &mut AsyncGroup) -> Result<(), Err> { /* ... */ Ok(()) }
//!     fn rollback(&mut self, ag: &mut AsyncGroup) { /* ... */ }
//!     fn close(&mut self) { /* ... */ }
//! }
//!
//! // (2) Implements logic functions and data traits
//!
//! #[overridable]
//! trait MyData {
//!     fn get_text(&mut self) -> Result<String, Err>;
//!     fn set_text(&mut self, text: String) -> Result<(), Err>;
//! }
//!
//! fn my_logic(data: &mut impl MyData) -> Result<(), Err> {
//!     let text = data.get_text()?;
//!     let _ = data.set_text(text)?;
//!     Ok(())
//! }
//!
//! // (3) Implements DataAcc(s)
//!
//! #[overridable]
//! trait CertainGlobalDataAcc: DataAcc { /* ... */ }
//!
//! #[overridable]
//! trait GettingDataAcc: DataAcc {
//!     fn get_text(&mut self) -> Result<String, Err> {
//!         let conn = self.get_data_conn::<GettingDataConn>("bar")?;
//!         conn.get_text()
//!     }
//! }
//!
//! #[overridable]
//! trait SettingDataAcc: DataAcc {
//!     fn set_text(&mut self, text: String) -> Result<(), Err> {
//!         let conn = self.get_data_conn::<SettingDataConn>("baz")?;
//!         conn.set_text(text)
//!     }
//! }
//!
//! // (4) Consolidate data traits and DataAcc traits to a DataHub.
//!
//! impl CertainGlobalDataAcc for DataHub {}
//! impl GettingDataAcc for DataHub {}
//! impl SettingDataAcc for DataHub {}
//!
//! #[override_with(CertainGlobalDataAcc, GettingDataAcc, SettingDataAcc)]
//! impl MyData for DataHub {}
//!
//! // (5) Use the logic functions and the DataHub
//!
//! fn main() {
//!     uses("foo", CertainGlobalDataSrc{});
//!     let _ = setup().unwrap();
//!     let _later = shutdown_later();
//!
//!     let mut data = DataHub::new();
//!     data.uses("bar", GettingDataSrc{});
//!     data.uses("baz", SettingDataSrc{});
//!
//!     let _ = data.txn(my_logic).unwrap();
//! }
//! ```

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
    /// # Parameters
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
    /// # Parameters
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
    /// # Parameters
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
    /// # Parameters
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

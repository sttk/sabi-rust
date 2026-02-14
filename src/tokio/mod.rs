// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

mod async_group;
mod data_acc;
mod data_conn;
mod data_hub;
mod data_src;

use crate::SendSyncNonNull;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::{any, ptr};

pub use data_conn::DataConnError;
pub use data_hub::DataHubError;
pub use data_src::{
    create_static_data_src_container, setup_async, setup_with_order_async, uses_async, DataSrcError,
};

/// Manages a collection of asynchronous tasks, allowing them to be executed concurrently
/// and their results (or errors) collected.
#[allow(clippy::type_complexity)]
pub struct AsyncGroup {
    names: Vec<Arc<str>>,
    tasks: Vec<Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'static>>>,
    pub(crate) _name: Arc<str>,
}

/// A trait for data connection implementations, providing methods for transaction management.
///
/// Implementors of this trait represent a connection to a data source and define
/// how to commit, rollback, and handle the lifecycle of transactions.
#[allow(async_fn_in_trait)]
#[allow(unused_variables)] // rustdoc
pub trait DataConn {
    /// Attempts to commit the changes made within this data connection.
    ///
    /// This is typically the main commit process, executed after `pre_commit_async`.
    ///
    /// # Arguments
    ///
    /// * `ag` - An `AsyncGroup` to which asynchronous tasks related to the commit can be added.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure of the commit operation.
    async fn commit_async(&mut self, ag: &mut AsyncGroup) -> errs::Result<()>;

    /// Performs preparatory actions before the main commit process.
    ///
    /// This method is called before `commit_async` and can be used for tasks like
    /// validation or preparing data.
    ///
    /// # Arguments
    ///
    /// * `ag` - An `AsyncGroup` to which asynchronous tasks related to pre-commit can be added.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure of the pre-commit operation.
    async fn pre_commit_async(&mut self, ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }

    /// Performs actions after the main commit process, only if it succeeds.
    ///
    /// This can be used for cleanup or post-transaction logging. Errors returned from
    /// tasks added to `ag` in this method are ignored.
    ///
    /// # Arguments
    ///
    /// * `ag` - An `AsyncGroup` to which asynchronous tasks related to post-commit can be added.
    async fn post_commit_async(&mut self, ag: &mut AsyncGroup) {}

    /// Indicates whether `force_back_async` should be called instead of `rollback_async`.
    ///
    /// This is typically `true` if `commit_async` has already succeeded for this connection,
    /// implying that changes need to be undone rather than simply discarded.
    ///
    /// # Returns
    ///
    /// `true` if `force_back_async` should be called, `false` otherwise.
    fn should_force_back(&self) -> bool {
        false
    }

    /// Rolls back any changes made within this data connection.
    ///
    /// This method is called if a transaction fails before `commit_async` completes,
    /// or if `should_force_back` returns `false`. Errors returned from tasks added to `ag`
    /// in this method are ignored.
    ///
    /// # Arguments
    ///
    /// * `ag` - An `AsyncGroup` to which asynchronous tasks related to rollback can be added.
    async fn rollback_async(&mut self, ag: &mut AsyncGroup);

    /// Forces the data connection to revert changes that have already been committed.
    ///
    /// This method is called if a transaction fails after `commit_async` has completed
    /// for this connection, and `should_force_back` returns `true`. Errors returned from
    /// tasks added to `ag` in this method are ignored.
    ///
    /// # Arguments
    ///
    /// * `ag` - An `AsyncGroup` to which asynchronous tasks related to force-back can be added.
    async fn force_back_async(&mut self, ag: &mut AsyncGroup) {}

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
    async fn rollback_async(&mut self, _ag: &mut AsyncGroup) {}
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

    commit_fn: for<'ag> fn(
        *const DataConnContainer,
        &'ag mut AsyncGroup,
    ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + 'ag>>,

    pre_commit_fn: for<'ag> fn(
        *const DataConnContainer,
        &'ag mut AsyncGroup,
    ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + 'ag>>,

    post_commit_fn: for<'ag> fn(
        *const DataConnContainer,
        &'ag mut AsyncGroup,
    ) -> Pin<Box<dyn Future<Output = ()> + 'ag>>,

    should_force_back_fn: fn(*const DataConnContainer) -> bool,

    rollback_fn: for<'ag> fn(
        *const DataConnContainer,
        &'ag mut AsyncGroup,
    ) -> Pin<Box<dyn Future<Output = ()> + 'ag>>,

    force_back_fn: for<'ag> fn(
        *const DataConnContainer,
        &'ag mut AsyncGroup,
    ) -> Pin<Box<dyn Future<Output = ()> + 'ag>>,

    close_fn: fn(*const DataConnContainer),

    name: Arc<str>,
    data_conn: Box<C>,
}

pub(crate) struct DataConnManager {
    vec: Vec<Option<ptr::NonNull<DataConnContainer>>>,
    index_map: HashMap<Arc<str>, usize>,
}

/// A trait for data source implementations, responsible for setting up and creating data connections.
///
/// Implementors of this trait define how to prepare a data source and how to instantiate
/// data connections of a specific type `C`.
#[trait_variant::make(Send)]
#[allow(async_fn_in_trait)]
#[allow(unused_variables)] // for rustdoc
pub trait DataSrc<C>
where
    C: DataConn + 'static,
{
    /// Performs asynchronous setup operations for the data source.
    ///
    /// This method is called to initialize the data source before any data connections
    /// are created from it.
    ///
    /// # Arguments
    ///
    /// * `ag` - An `AsyncGroup` to which asynchronous setup tasks can be added.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure of the setup operation.
    async fn setup_async(&mut self, ag: &mut AsyncGroup) -> errs::Result<()>;

    /// Closes the data source, releasing any associated resources.
    ///
    /// This method is always called at the end of the `DataHub`'s lifecycle for this source.
    fn close(&mut self);

    /// Asynchronously creates a new data connection from this data source.
    ///
    /// # Returns
    ///
    /// A `Result` which is `Ok` containing a `Box`ed instance of the data connection `C`,
    /// or an `Err` if the connection creation fails.
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
        Box<dyn Future<Output = errs::Result<Box<DataConnContainer<C>>>> + 'static>,
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

/// A marker struct that can be used to trigger automatic shutdown behavior
/// for resources managed by the `DataHub` when it goes out of scope.
pub struct AutoShutdown {}

/// The central hub for managing data sources and their connections within an application.
///
/// `DataHub` provides mechanisms to register data sources, acquire data connections,
/// and execute transactional or non-transactional asynchronous logic.
pub struct DataHub {
    local_data_src_manager: DataSrcManager,
    data_src_map: HashMap<Arc<str>, (bool, usize)>,
    data_conn_manager: DataConnManager,
    fixed: bool,
}

/// A trait defining the ability to access data connections.
///
/// This trait abstracts the mechanism for retrieving data connections, allowing
/// different implementations (e.g., `DataHub`) to provide connections.
#[allow(async_fn_in_trait)]
pub trait DataAcc {
    /// Asynchronously retrieves a data connection of a specific type.
    ///
    /// # Type Parameters
    ///
    /// * `C` - The expected type of the data connection, which must implement `DataConn` and have a `'static` lifetime.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the data connection to retrieve.
    ///
    /// # Returns
    ///
    /// A `Result` which is `Ok` containing a mutable reference to the data connection
    /// if found, or an `Err` if the connection cannot be retrieved or cast.
    async fn get_data_conn_async<C: DataConn + 'static>(
        &mut self,
        name: impl AsRef<str>,
    ) -> errs::Result<&mut C>;
}

#[doc(hidden)]
pub struct StaticDataSrcContainer {
    pub(crate) ssnnptr: SendSyncNonNull<DataSrcContainer>,
}

#[doc(hidden)]
pub struct StaticDataSrcRegistration {
    pub(crate) factory: fn() -> StaticDataSrcContainer,
}

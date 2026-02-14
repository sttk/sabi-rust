// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use super::data_src::{copy_global_data_srcs_to_map, create_data_conn_from_global_data_src_async};
use super::{DataConn, DataConnContainer, DataConnManager, DataHub, DataSrc, DataSrcManager};

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::{any, ptr};

/// Represents errors that can occur within the `DataHub`.
#[derive(Debug)]
pub enum DataHubError {
    /// An error indicating that one or more local data sources failed during their setup processes.
    FailToSetupLocalDataSrcs {
        /// A vector of errors, each containing the name of the data source and the error itself.
        errors: Vec<(Arc<str>, errs::Err)>,
    },

    /// An error indicating that no suitable data source was found to create a data connection
    /// with the specified name and type.
    NoDataSrcToCreateDataConn {
        /// The name of the data connection that could not be created.
        name: Arc<str>,
        /// The string representation of the data connection type that was requested.
        data_conn_type: &'static str,
    },
}

impl DataHub {
    /// Creates a new `DataHub` instance.
    ///
    /// This initializes the `DataHub` with no local data sources and an empty data connection manager.
    /// Global data sources, if any, are copied into the `data_src_map`.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let mut data_src_map = HashMap::new();
        copy_global_data_srcs_to_map(&mut data_src_map);

        Self {
            local_data_src_manager: DataSrcManager::new(true),
            data_src_map,
            data_conn_manager: DataConnManager::new(),
            fixed: false,
        }
    }

    /// Creates a new `DataHub` instance with a specified commit order for data connections.
    ///
    /// This allows defining the order in which data connections will be committed. Connections
    /// not specified in `names` will be committed after the specified ones, in an undefined order.
    /// Global data sources are copied into the `data_src_map`.
    ///
    /// # Arguments
    ///
    /// * `names` - An array of string slices specifying the desired commit order by data connection name.
    pub fn with_commit_order(names: &[&str]) -> Self {
        let mut data_src_map = HashMap::new();
        copy_global_data_srcs_to_map(&mut data_src_map);

        Self {
            local_data_src_manager: DataSrcManager::new(true),
            data_src_map,
            data_conn_manager: DataConnManager::with_commit_order(names),
            fixed: false,
        }
    }

    /// Registers a local data source with the `DataHub`.
    ///
    /// This method allows adding a custom data source which can provide data connections.
    /// Data sources can only be added before `run_async` or `txn_async` are called.
    ///
    /// # Arguments
    ///
    /// * `name` - The name to associate with this data source.
    /// * `ds` - The data source instance, which must implement `DataSrc` and have a `'static` lifetime.
    ///
    /// # Type Parameters
    ///
    /// * `S` - The type of the data source.
    /// * `C` - The type of the data connection provided by the data source.
    pub fn uses<S, C>(&mut self, name: impl Into<Arc<str>>, ds: S)
    where
        S: DataSrc<C> + 'static,
        C: DataConn + 'static,
    {
        if self.fixed {
            return;
        }
        self.local_data_src_manager.add(name, ds);
    }

    /// Deregisters a local data source from the `DataHub`.
    ///
    /// This removes a data source previously added with `uses`. Data sources can only be
    /// removed before `run_async` or `txn_async` are called.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the data source to remove.
    pub fn disuses(&mut self, name: impl AsRef<str>) {
        if self.fixed {
            return;
        }
        self.data_src_map.remove(name.as_ref());
        self.local_data_src_manager.remove(name);
    }

    #[inline]
    async fn begin_async(&mut self) -> errs::Result<()> {
        self.fixed = true;

        let mut errors = Vec::new();

        self.local_data_src_manager.setup_async(&mut errors).await;
        if errors.is_empty() {
            self.local_data_src_manager
                .copy_ds_ready_to_map(&mut self.data_src_map);
            Ok(())
        } else {
            Err(errs::Err::new(DataHubError::FailToSetupLocalDataSrcs {
                errors,
            }))
        }
    }

    #[inline]
    async fn commit_async(&mut self) -> errs::Result<()> {
        self.data_conn_manager.commit_async().await
    }

    #[inline]
    async fn rollback_async(&mut self) {
        self.data_conn_manager.rollback_async().await
    }

    #[inline]
    fn end(&mut self) {
        self.data_conn_manager.close();
        self.fixed = false;
    }

    /// Executes an asynchronous logic function with the `DataHub` and handles setup and cleanup.
    ///
    /// This method sets up local data sources, runs the provided `logic_fn`, and then
    /// cleans up all data connections and sources. It does *not* automatically commit
    /// or rollback any transactions.
    ///
    /// # Arguments
    ///
    /// * `logic_fn` - An asynchronous function that takes a mutable reference to `DataHub`
    ///                and returns a `Result`. This function contains the application's logic.
    ///
    /// # Type Parameters
    ///
    /// * `F` - The type of the asynchronous logic function.
    ///
    /// # Returns
    ///
    /// A `Result` indicating the success or failure of the `logic_fn` execution or
    /// the setup of data sources.
    #[allow(clippy::doc_overindented_list_items)]
    pub async fn run_async<F>(&mut self, mut logic_fn: F) -> errs::Result<()>
    where
        for<'a> F: FnMut(&'a mut DataHub) -> Pin<Box<dyn Future<Output = errs::Result<()>> + 'a>>,
    {
        let mut r = self.begin_async().await;
        if r.is_ok() {
            r = logic_fn(self).await;
        }
        self.end();
        r
    }

    /// Executes an asynchronous transactional logic function with the `DataHub`, managing
    /// setup, commit, and rollback for data connections.
    ///
    /// This method sets up local data sources, runs the provided `logic_fn`. If the logic
    /// function succeeds, it attempts to commit all created data connections. If either
    /// the logic function or the commit fails, it rolls back all data connections.
    /// Finally, it cleans up all data connections and sources.
    ///
    /// # Arguments
    ///
    /// * `logic_fn` - An asynchronous function that takes a mutable reference to `DataHub`
    ///                and returns a `Result`. This function contains the application's transactional logic.
    ///
    /// # Type Parameters
    ///
    /// * `F` - The type of the asynchronous transactional logic function.
    ///
    /// # Returns
    ///
    /// A `Result` indicating the success or failure of the `logic_fn` execution,
    /// the setup of data sources, or the commit/rollback operations.
    #[allow(clippy::doc_overindented_list_items)]
    pub async fn txn_async<F>(&mut self, mut logic_fn: F) -> errs::Result<()>
    where
        for<'a> F: FnMut(&'a mut DataHub) -> Pin<Box<dyn Future<Output = errs::Result<()>> + 'a>>,
    {
        let mut r = self.begin_async().await;
        if r.is_ok() {
            r = logic_fn(self).await;
        }
        if r.is_ok() {
            r = self.commit_async().await;
        }
        if r.is_err() {
            self.rollback_async().await;
        }
        self.end();
        r
    }

    /// Retrieves an existing data connection or creates a new one if it doesn't exist.
    ///
    /// This asynchronous method first checks if a data connection with the given `name`
    /// and type `C` already exists. If not, it attempts to find a suitable data source
    /// (local or global) to create a new data connection.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the data connection to retrieve or create.
    ///
    /// # Type Parameters
    ///
    /// * `C` - The expected type of the data connection, which must implement `DataConn` and have a `'static` lifetime.
    ///
    /// # Returns
    ///
    /// A `Result` which is `Ok` containing a mutable reference to the data connection
    /// if found or successfully created, or an `Err` if no suitable data source is found
    /// or connection creation fails.
    pub async fn get_data_conn_async<C>(&mut self, name: impl AsRef<str>) -> errs::Result<&mut C>
    where
        C: DataConn + 'static,
    {
        if let Some(nnptr) = self.data_conn_manager.find_by_name(name.as_ref()) {
            let typed_nnptr = DataConnManager::to_typed_ptr::<C>(&nnptr)?;
            return Ok(unsafe { &mut (*typed_nnptr).data_conn });
        }

        if let Some((local, index)) = self.data_src_map.get(name.as_ref()) {
            let boxed = if *local {
                self.local_data_src_manager
                    .create_data_conn_async::<C>(*index, name.as_ref())
                    .await?
            } else {
                create_data_conn_from_global_data_src_async::<C>(*index, name.as_ref()).await?
            };

            let ptr = Box::into_raw(boxed);
            if let Some(nnptr) = ptr::NonNull::new(ptr) {
                self.data_conn_manager
                    .add(nnptr.cast::<DataConnContainer>());

                let typed_ptr = ptr.cast::<DataConnContainer<C>>();
                return Ok(unsafe { &mut (*typed_ptr).data_conn });
            } else {
                // impossible case.
            }
        }

        Err(errs::Err::new(DataHubError::NoDataSrcToCreateDataConn {
            name: name.as_ref().into(),
            data_conn_type: any::type_name::<C>(),
        }))
    }
}

/// A convenience macro to easily convert an asynchronous function into a `Pin<Box<dyn Future>>`
/// closure suitable for `DataHub`'s `run_async` or `txn_async` methods.
///
/// This macro simplifies passing async functions by handling the boxing and pinning.
///
/// # Example
///
/// ```ignore
/// async fn my_logic(data_hub: &mut DataHub) -> errs::Result<()> {
///     // ... some logic using data_hub
///     Ok(())
/// }
///
/// #[tokio::main]
/// async fn main() {
///     let mut hub = DataHub::new();
///     hub.txn_async(logic!(my_logic)).await.unwrap();
/// }
/// ```
#[macro_export]
macro_rules! logic {
    ($f:expr) => {
        |data| Box::pin($f(data))
    };
}

#[cfg(test)]
mod tests_of_data_hub {
    use super::*;
    use crate::tokio::AsyncGroup;
    use std::sync::Mutex;

    #[derive(Clone, Copy, PartialEq)]
    enum Failure {
        None,
        FailToPreCommit,
        FailToCommit,
        FailToSetup,
        FailToCreateDataConn,
    }

    struct MyDataConn {
        id: i8,
        failure: Failure,
        committed: bool,
        logger: Arc<Mutex<Vec<String>>>,
    }
    impl MyDataConn {
        fn new(id: i8, logger: Arc<Mutex<Vec<String>>>, failure: Failure) -> Self {
            logger
                .lock()
                .unwrap()
                .push(format!("MyDataConn::new {}", id));
            Self {
                id,
                failure,
                committed: false,
                logger,
            }
        }
    }
    impl Drop for MyDataConn {
        fn drop(&mut self) {
            self.logger
                .lock()
                .unwrap()
                .push(format!("MyDataConn::drop {}", self.id));
        }
    }
    impl DataConn for MyDataConn {
        async fn pre_commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            if self.failure == Failure::FailToPreCommit {
                self.logger
                    .lock()
                    .unwrap()
                    .push(format!("MyDataConn::pre_commit {} failed", self.id));
                Err(errs::Err::new("pre commit error"))
            } else {
                self.logger
                    .lock()
                    .unwrap()
                    .push(format!("MyDataConn::pre_commit {}", self.id));
                Ok(())
            }
        }
        async fn commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            if self.failure == Failure::FailToCommit {
                self.logger
                    .lock()
                    .unwrap()
                    .push(format!("MyDataConn::commit {} failed", self.id));
                Err(errs::Err::new("commit error"))
            } else {
                self.logger
                    .lock()
                    .unwrap()
                    .push(format!("MyDataConn::commit {}", self.id));
                self.committed = true;
                Ok(())
            }
        }
        async fn post_commit_async(&mut self, _ag: &mut AsyncGroup) {
            self.logger
                .lock()
                .unwrap()
                .push(format!("MyDataConn::post_commit {}", self.id));
        }
        async fn rollback_async(&mut self, _ag: &mut AsyncGroup) {
            self.logger
                .lock()
                .unwrap()
                .push(format!("MyDataConn::rollback {}", self.id));
        }
        async fn force_back_async(&mut self, _ag: &mut AsyncGroup) {
            self.logger
                .lock()
                .unwrap()
                .push(format!("MyDataConn::force_back {}", self.id));
        }
        fn close(&mut self) {
            self.logger
                .lock()
                .unwrap()
                .push(format!("MyDataConn::close {}", self.id));
        }
    }

    struct MyDataSrc {
        id: i8,
        failure: Failure,
        logger: Arc<Mutex<Vec<String>>>,
    }
    impl MyDataSrc {
        fn new(id: i8, logger: Arc<Mutex<Vec<String>>>, failure: Failure) -> Self {
            logger
                .lock()
                .unwrap()
                .push(format!("MyDataSrc::new {}", id));
            Self {
                id,
                failure,
                logger,
            }
        }
    }
    impl Drop for MyDataSrc {
        fn drop(&mut self) {
            self.logger
                .lock()
                .unwrap()
                .push(format!("MyDataSrc::drop {}", self.id));
        }
    }
    impl DataSrc<MyDataConn> for MyDataSrc {
        async fn setup_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            if self.failure == Failure::FailToSetup {
                self.logger
                    .lock()
                    .unwrap()
                    .push(format!("MyDataSrc::setup {} failed", self.id));
                Err(errs::Err::new("setup error".to_string()))
            } else {
                self.logger
                    .lock()
                    .unwrap()
                    .push(format!("MyDataSrc::setup {}", self.id));
                Ok(())
            }
        }
        fn close(&mut self) {
            self.logger
                .lock()
                .unwrap()
                .push(format!("MyDataSrc::close {}", self.id));
        }
        async fn create_data_conn_async(&mut self) -> errs::Result<Box<MyDataConn>> {
            if self.failure == Failure::FailToCreateDataConn {
                self.logger
                    .lock()
                    .unwrap()
                    .push(format!("MyDataSrc::create_data_conn {} failed", self.id));
                return Err(errs::Err::new("eeee".to_string()));
            }
            {
                self.logger
                    .lock()
                    .unwrap()
                    .push(format!("MyDataSrc::create_data_conn {}", self.id));
            }
            let conn = MyDataConn::new(self.id, self.logger.clone(), self.failure);
            Ok(Box::new(conn))
        }
    }

    #[test]
    fn test_new() {
        let hub = DataHub::new();
        assert!(hub.local_data_src_manager.vec_unready.is_empty());
        assert!(hub.local_data_src_manager.vec_ready.is_empty());
        assert!(hub.local_data_src_manager.local);
        assert!(hub.data_src_map.is_empty());
        assert!(hub.data_conn_manager.vec.is_empty());
        assert!(hub.data_conn_manager.index_map.is_empty());
        assert!(!hub.fixed);
    }

    #[test]
    fn test_with_commit_order() {
        let hub = DataHub::with_commit_order(&["bar", "qux", "foo"]);
        assert!(hub.local_data_src_manager.vec_unready.is_empty());
        assert!(hub.local_data_src_manager.vec_ready.is_empty());
        assert!(hub.local_data_src_manager.local);
        assert!(hub.data_src_map.is_empty());
        assert_eq!(hub.data_conn_manager.vec.len(), 3);
        assert_eq!(hub.data_conn_manager.index_map.len(), 3);
        assert!(!hub.fixed);
    }

    #[tokio::test]
    async fn test_uses_and_ok() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        let mut hub = DataHub::new();
        hub.uses("foo", MyDataSrc::new(1, logger.clone(), Failure::None));
        hub.uses("bar", MyDataSrc::new(2, logger.clone(), Failure::None));

        assert_eq!(hub.local_data_src_manager.vec_unready.len(), 2);
        assert!(hub.local_data_src_manager.vec_ready.is_empty());
        assert!(hub.local_data_src_manager.local);
        assert!(hub.data_src_map.is_empty());
        assert_eq!(hub.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.data_conn_manager.index_map.len(), 0);
        assert!(!hub.fixed);

        assert!(hub.begin_async().await.is_ok());

        assert_eq!(hub.local_data_src_manager.vec_unready.len(), 0);
        assert_eq!(hub.local_data_src_manager.vec_ready.len(), 2);
        assert!(hub.local_data_src_manager.local);
        assert_eq!(hub.data_src_map.len(), 2);
        assert_eq!(hub.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.data_conn_manager.index_map.len(), 0);
        assert!(hub.fixed);
    }

    #[tokio::test]
    async fn test_uses_but_already_fixed() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        let mut hub = DataHub::new();
        hub.uses("foo", MyDataSrc::new(1, logger.clone(), Failure::None));

        assert_eq!(hub.local_data_src_manager.vec_unready.len(), 1);
        assert_eq!(hub.local_data_src_manager.vec_ready.len(), 0);
        assert!(hub.local_data_src_manager.local);
        assert_eq!(hub.data_src_map.len(), 0);
        assert_eq!(hub.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.data_conn_manager.index_map.len(), 0);
        assert!(!hub.fixed);

        assert!(hub.begin_async().await.is_ok());

        assert_eq!(hub.local_data_src_manager.vec_unready.len(), 0);
        assert_eq!(hub.local_data_src_manager.vec_ready.len(), 1);
        assert!(hub.local_data_src_manager.local);
        assert_eq!(hub.data_src_map.len(), 1);
        assert_eq!(hub.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.data_conn_manager.index_map.len(), 0);
        assert!(hub.fixed);

        hub.uses("bar", MyDataSrc::new(2, logger.clone(), Failure::None));

        assert_eq!(hub.local_data_src_manager.vec_unready.len(), 0);
        assert_eq!(hub.local_data_src_manager.vec_ready.len(), 1);
        assert!(hub.local_data_src_manager.local);
        assert_eq!(hub.data_src_map.len(), 1);
        assert_eq!(hub.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.data_conn_manager.index_map.len(), 0);
        assert!(hub.fixed);
    }

    #[test]
    fn test_disuses_and_ok() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        let mut hub = DataHub::new();
        hub.uses("foo", MyDataSrc::new(1, logger.clone(), Failure::None));
        hub.uses("bar", MyDataSrc::new(2, logger.clone(), Failure::None));

        assert_eq!(hub.local_data_src_manager.vec_unready.len(), 2);
        assert!(hub.local_data_src_manager.vec_ready.is_empty());
        assert!(hub.local_data_src_manager.local);
        assert!(hub.data_src_map.is_empty());
        assert_eq!(hub.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.data_conn_manager.index_map.len(), 0);
        assert!(!hub.fixed);

        hub.disuses("foo");

        assert_eq!(hub.local_data_src_manager.vec_unready.len(), 1);
        assert!(hub.local_data_src_manager.vec_ready.is_empty());
        assert!(hub.local_data_src_manager.local);
        assert!(hub.data_src_map.is_empty());
        assert_eq!(hub.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.data_conn_manager.index_map.len(), 0);
        assert!(!hub.fixed);

        hub.disuses("bar");

        assert_eq!(hub.local_data_src_manager.vec_unready.len(), 0);
        assert!(hub.local_data_src_manager.vec_ready.is_empty());
        assert!(hub.local_data_src_manager.local);
        assert!(hub.data_src_map.is_empty());
        assert_eq!(hub.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.data_conn_manager.index_map.len(), 0);
        assert!(!hub.fixed);
    }

    #[tokio::test]
    async fn test_disuses_and_fix() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        let mut hub = DataHub::new();
        hub.uses("foo", MyDataSrc::new(1, logger.clone(), Failure::None));
        hub.uses("bar", MyDataSrc::new(2, logger.clone(), Failure::None));

        assert_eq!(hub.local_data_src_manager.vec_unready.len(), 2);
        assert!(hub.local_data_src_manager.vec_ready.is_empty());
        assert!(hub.local_data_src_manager.local);
        assert!(hub.data_src_map.is_empty());
        assert_eq!(hub.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.data_conn_manager.index_map.len(), 0);
        assert!(!hub.fixed);

        hub.disuses("foo");

        assert_eq!(hub.local_data_src_manager.vec_unready.len(), 1);
        assert!(hub.local_data_src_manager.vec_ready.is_empty());
        assert!(hub.local_data_src_manager.local);
        assert!(hub.data_src_map.is_empty());
        assert_eq!(hub.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.data_conn_manager.index_map.len(), 0);
        assert!(!hub.fixed);

        hub.disuses("bar");

        assert_eq!(hub.local_data_src_manager.vec_unready.len(), 0);
        assert!(hub.local_data_src_manager.vec_ready.is_empty());
        assert!(hub.local_data_src_manager.local);
        assert!(hub.data_src_map.is_empty());
        assert_eq!(hub.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.data_conn_manager.index_map.len(), 0);
        assert!(!hub.fixed);

        hub.uses("foo", MyDataSrc::new(1, logger.clone(), Failure::None));
        hub.uses("bar", MyDataSrc::new(2, logger.clone(), Failure::None));

        assert!(hub.begin_async().await.is_ok());

        assert!(hub.local_data_src_manager.vec_unready.is_empty());
        assert_eq!(hub.local_data_src_manager.vec_ready.len(), 2);
        assert!(hub.local_data_src_manager.local);
        assert_eq!(hub.data_src_map.len(), 2);
        assert_eq!(hub.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.data_conn_manager.index_map.len(), 0);
        assert!(hub.fixed);

        hub.uses("baz", MyDataSrc::new(3, logger.clone(), Failure::None));

        assert!(hub.local_data_src_manager.vec_unready.is_empty());
        assert_eq!(hub.local_data_src_manager.vec_ready.len(), 2);
        assert!(hub.local_data_src_manager.local);
        assert_eq!(hub.data_src_map.len(), 2);
        assert_eq!(hub.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.data_conn_manager.index_map.len(), 0);
        assert!(hub.fixed);

        hub.disuses("bar");

        assert!(hub.local_data_src_manager.vec_unready.is_empty());
        assert_eq!(hub.local_data_src_manager.vec_ready.len(), 2);
        assert!(hub.local_data_src_manager.local);
        assert_eq!(hub.data_src_map.len(), 2);
        assert_eq!(hub.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.data_conn_manager.index_map.len(), 0);
        assert!(hub.fixed);

        hub.end();

        assert!(hub.local_data_src_manager.vec_unready.is_empty());
        assert_eq!(hub.local_data_src_manager.vec_ready.len(), 2);
        assert!(hub.local_data_src_manager.local);
        assert_eq!(hub.data_src_map.len(), 2);
        assert_eq!(hub.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.data_conn_manager.index_map.len(), 0);
        assert!(!hub.fixed);

        hub.disuses("bar");

        assert!(hub.local_data_src_manager.vec_unready.is_empty());
        assert_eq!(hub.local_data_src_manager.vec_ready.len(), 1);
        assert!(hub.local_data_src_manager.local);
        assert_eq!(hub.data_src_map.len(), 1);
        assert_eq!(hub.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.data_conn_manager.index_map.len(), 0);
        assert!(!hub.fixed);

        hub.disuses("foo");

        assert!(hub.local_data_src_manager.vec_unready.is_empty());
        assert!(hub.local_data_src_manager.vec_ready.is_empty());
        assert!(hub.local_data_src_manager.local);
        assert_eq!(hub.data_src_map.len(), 0);
        assert_eq!(hub.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.data_conn_manager.index_map.len(), 0);
        assert!(!hub.fixed);
    }

    #[tokio::test]
    async fn test_begin_if_empty() {
        let mut hub = DataHub::new();
        assert!(hub.begin_async().await.is_ok());

        assert!(hub.local_data_src_manager.vec_unready.is_empty());
        assert!(hub.local_data_src_manager.vec_ready.is_empty());
        assert!(hub.local_data_src_manager.local);
        assert_eq!(hub.data_src_map.len(), 0);
        assert_eq!(hub.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.data_conn_manager.index_map.len(), 0);
        assert!(hub.fixed);

        hub.end();

        assert!(hub.local_data_src_manager.vec_unready.is_empty());
        assert!(hub.local_data_src_manager.vec_ready.is_empty());
        assert!(hub.local_data_src_manager.local);
        assert_eq!(hub.data_src_map.len(), 0);
        assert_eq!(hub.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.data_conn_manager.index_map.len(), 0);
        assert!(!hub.fixed);
    }

    #[tokio::test]
    async fn test_begin_and_ok() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            let mut hub = DataHub::new();

            hub.uses("foo", MyDataSrc::new(1, logger.clone(), Failure::None));
            hub.uses("bar", MyDataSrc::new(2, logger.clone(), Failure::None));

            assert_eq!(hub.local_data_src_manager.vec_unready.len(), 2);
            assert_eq!(hub.local_data_src_manager.vec_ready.len(), 0);
            assert_eq!(hub.local_data_src_manager.local, true);
            assert_eq!(hub.data_src_map.len(), 0);
            assert_eq!(hub.data_conn_manager.vec.len(), 0);
            assert_eq!(hub.data_conn_manager.index_map.len(), 0);
            assert_eq!(hub.fixed, false);

            assert_eq!(hub.begin_async().await.is_ok(), true);

            assert_eq!(hub.local_data_src_manager.vec_unready.len(), 0);
            assert_eq!(hub.local_data_src_manager.vec_ready.len(), 2);
            assert_eq!(hub.local_data_src_manager.local, true);
            assert_eq!(hub.data_src_map.len(), 2);
            assert_eq!(hub.data_conn_manager.vec.len(), 0);
            assert_eq!(hub.data_conn_manager.index_map.len(), 0);
            assert_eq!(hub.fixed, true);

            hub.end();

            assert_eq!(hub.local_data_src_manager.vec_unready.len(), 0);
            assert_eq!(hub.local_data_src_manager.vec_ready.len(), 2);
            assert_eq!(hub.local_data_src_manager.local, true);
            assert_eq!(hub.data_src_map.len(), 2);
            assert_eq!(hub.data_conn_manager.vec.len(), 0);
            assert_eq!(hub.data_conn_manager.index_map.len(), 0);
            assert_eq!(hub.fixed, false);
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "MyDataSrc::new 1",
                "MyDataSrc::new 2",
                "MyDataSrc::setup 1",
                "MyDataSrc::setup 2",
                "MyDataSrc::close 2",
                "MyDataSrc::drop 2",
                "MyDataSrc::close 1",
                "MyDataSrc::drop 1",
            ]
        );
    }

    #[tokio::test]
    async fn test_begin_but_failed() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            let mut hub = DataHub::new();

            hub.uses("foo", MyDataSrc::new(1, logger.clone(), Failure::None));
            hub.uses(
                "bar",
                MyDataSrc::new(2, logger.clone(), Failure::FailToSetup),
            );
            hub.uses("baz", MyDataSrc::new(3, logger.clone(), Failure::None));

            assert_eq!(hub.local_data_src_manager.vec_unready.len(), 3);
            assert_eq!(hub.local_data_src_manager.vec_ready.len(), 0);
            assert_eq!(hub.local_data_src_manager.local, true);
            assert_eq!(hub.data_src_map.len(), 0);
            assert_eq!(hub.data_conn_manager.vec.len(), 0);
            assert_eq!(hub.data_conn_manager.index_map.len(), 0);
            assert_eq!(hub.fixed, false);

            if let Err(err) = hub.begin_async().await {
                match err.reason::<DataHubError>() {
                    Ok(DataHubError::FailToSetupLocalDataSrcs { errors }) => {
                        assert_eq!(errors.len(), 1);
                        assert_eq!(errors[0].0, "bar".into());
                        assert_eq!(errors[0].1.reason::<String>().unwrap(), "setup error");
                    }
                    _ => panic!(),
                }
            } else {
                panic!();
            }

            hub.end();
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "MyDataSrc::new 1",
                "MyDataSrc::new 2",
                "MyDataSrc::new 3",
                "MyDataSrc::setup 1",
                "MyDataSrc::setup 2 failed",
                "MyDataSrc::close 2",
                "MyDataSrc::close 1",
                "MyDataSrc::drop 3",
                "MyDataSrc::drop 2",
                "MyDataSrc::drop 1",
            ]
        );
    }

    #[tokio::test]
    async fn test_run_and_ok() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", MyDataSrc::new(1, logger.clone(), Failure::None));
            hub.uses("bar", MyDataSrc::new(2, logger.clone(), Failure::None));

            let logger_clone = logger.clone();
            assert!(hub
                .run_async(|_data| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("execute logic".to_string());
                        Ok(())
                    })
                })
                .await
                .is_ok());
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "MyDataSrc::new 1",
                "MyDataSrc::new 2",
                "MyDataSrc::setup 1",
                "MyDataSrc::setup 2",
                "execute logic",
                "MyDataSrc::close 2",
                "MyDataSrc::drop 2",
                "MyDataSrc::close 1",
                "MyDataSrc::drop 1",
            ]
        );
    }

    #[tokio::test]
    async fn test_run_but_failed() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", MyDataSrc::new(1, logger.clone(), Failure::None));
            hub.uses("bar", MyDataSrc::new(2, logger.clone(), Failure::None));

            let logger_clone = logger.clone();
            if let Err(err) = hub
                .run_async(|_data| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("execute logic but fail".to_string());
                        Err(errs::Err::new("logic error".to_string()))
                    })
                })
                .await
            {
                match err.reason::<String>() {
                    Ok(s) => assert_eq!(s, "logic error"),
                    _ => panic!(),
                }
            } else {
                panic!();
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "MyDataSrc::new 1",
                "MyDataSrc::new 2",
                "MyDataSrc::setup 1",
                "MyDataSrc::setup 2",
                "execute logic but fail",
                "MyDataSrc::close 2",
                "MyDataSrc::drop 2",
                "MyDataSrc::close 1",
                "MyDataSrc::drop 1",
            ]
        );
    }

    #[tokio::test]
    async fn test_txn_and_no_data_access_and_ok() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", MyDataSrc::new(1, logger.clone(), Failure::None));
            hub.uses("bar", MyDataSrc::new(2, logger.clone(), Failure::None));

            let logger_clone = logger.clone();
            assert!(hub
                .txn_async(|_data| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("execute logic".to_string());
                        Ok(())
                    })
                })
                .await
                .is_ok());
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "MyDataSrc::new 1",
                "MyDataSrc::new 2",
                "MyDataSrc::setup 1",
                "MyDataSrc::setup 2",
                "execute logic",
                "MyDataSrc::close 2",
                "MyDataSrc::drop 2",
                "MyDataSrc::close 1",
                "MyDataSrc::drop 1",
            ]
        );
    }

    #[tokio::test]
    async fn test_txn_and_has_data_access_and_ok() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", MyDataSrc::new(1, logger.clone(), Failure::None));
            hub.uses("bar", MyDataSrc::new(2, logger.clone(), Failure::None));

            let logger_clone = logger.clone();
            hub.txn_async(move |data| {
                let logger_clone2 = logger_clone.clone();
                Box::pin(async move {
                    logger_clone2
                        .lock()
                        .unwrap()
                        .push("execute logic".to_string());
                    let _conn1 = data.get_data_conn_async::<MyDataConn>("foo").await?;
                    let _conn2 = data.get_data_conn_async::<MyDataConn>("bar").await?;
                    Ok(())
                })
            })
            .await
            .unwrap()
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "MyDataSrc::new 1",
                "MyDataSrc::new 2",
                "MyDataSrc::setup 1",
                "MyDataSrc::setup 2",
                "execute logic",
                "MyDataSrc::create_data_conn 1",
                "MyDataConn::new 1",
                "MyDataSrc::create_data_conn 2",
                "MyDataConn::new 2",
                "MyDataConn::pre_commit 1",
                "MyDataConn::pre_commit 2",
                "MyDataConn::commit 1",
                "MyDataConn::commit 2",
                "MyDataConn::post_commit 1",
                "MyDataConn::post_commit 2",
                "MyDataConn::close 1",
                "MyDataConn::drop 1",
                "MyDataConn::close 2",
                "MyDataConn::drop 2",
                "MyDataSrc::close 2",
                "MyDataSrc::drop 2",
                "MyDataSrc::close 1",
                "MyDataSrc::drop 1",
            ]
        );
    }

    #[tokio::test]
    async fn test_txn_but_failed() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", MyDataSrc::new(1, logger.clone(), Failure::None));
            hub.uses("bar", MyDataSrc::new(2, logger.clone(), Failure::None));

            let logger_clone = logger.clone();
            if let Err(e) = hub
                .txn_async(move |data| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("execute logic".to_string());
                        let _conn1 = data.get_data_conn_async::<MyDataConn>("foo").await?;
                        let _conn2 = data.get_data_conn_async::<MyDataConn>("bar").await?;
                        Err(errs::Err::new("logic error"))
                    })
                })
                .await
            {
                match e.reason::<&str>() {
                    Ok(s) => assert_eq!(s, &"logic error"),
                    _ => panic!(),
                }
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "MyDataSrc::new 1",
                "MyDataSrc::new 2",
                "MyDataSrc::setup 1",
                "MyDataSrc::setup 2",
                "execute logic",
                "MyDataSrc::create_data_conn 1",
                "MyDataConn::new 1",
                "MyDataSrc::create_data_conn 2",
                "MyDataConn::new 2",
                "MyDataConn::rollback 1",
                "MyDataConn::rollback 2",
                "MyDataConn::close 1",
                "MyDataConn::drop 1",
                "MyDataConn::close 2",
                "MyDataConn::drop 2",
                "MyDataSrc::close 2",
                "MyDataSrc::drop 2",
                "MyDataSrc::close 1",
                "MyDataSrc::drop 1",
            ]
        );
    }

    #[tokio::test]
    async fn test_txn_with_commit_order() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::with_commit_order(&["bar", "foo"]);

            hub.uses("foo", MyDataSrc::new(1, logger.clone(), Failure::None));
            hub.uses("bar", MyDataSrc::new(2, logger.clone(), Failure::None));

            let logger_clone = logger.clone();
            hub.txn_async(move |data| {
                let logger_clone2 = logger_clone.clone();
                Box::pin(async move {
                    logger_clone2
                        .lock()
                        .unwrap()
                        .push("execute logic".to_string());
                    let _conn1 = data.get_data_conn_async::<MyDataConn>("foo").await?;
                    let _conn2 = data.get_data_conn_async::<MyDataConn>("bar").await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "MyDataSrc::new 1",
                "MyDataSrc::new 2",
                "MyDataSrc::setup 1",
                "MyDataSrc::setup 2",
                "execute logic",
                "MyDataSrc::create_data_conn 1",
                "MyDataConn::new 1",
                "MyDataSrc::create_data_conn 2",
                "MyDataConn::new 2",
                "MyDataConn::pre_commit 2",
                "MyDataConn::pre_commit 1",
                "MyDataConn::commit 2",
                "MyDataConn::commit 1",
                "MyDataConn::post_commit 2",
                "MyDataConn::post_commit 1",
                "MyDataConn::close 2",
                "MyDataConn::drop 2",
                "MyDataConn::close 1",
                "MyDataConn::drop 1",
                "MyDataSrc::close 2",
                "MyDataSrc::drop 2",
                "MyDataSrc::close 1",
                "MyDataSrc::drop 1",
            ]
        );
    }

    #[tokio::test]
    async fn test_get_data_conn_and_failed() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", MyDataSrc::new(1, logger.clone(), Failure::None));
            hub.uses("bar", MyDataSrc::new(2, logger.clone(), Failure::None));

            let logger_clone = logger.clone();
            let err = hub
                .txn_async(move |data| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("execute logic".to_string());
                        let _conn1 = data.get_data_conn_async::<MyDataConn>("fxx").await?;
                        Ok(())
                    })
                })
                .await
                .unwrap_err();

            match err.reason::<DataHubError>() {
                Ok(r) => match r {
                    DataHubError::NoDataSrcToCreateDataConn {
                        name,
                        data_conn_type,
                    } => {
                        assert_eq!(name.as_ref(), "fxx");
                        assert_eq!(
                            data_conn_type,
                            &"sabi::tokio::data_hub::tests_of_data_hub::MyDataConn"
                        );
                    }
                    _ => panic!(),
                },
                _ => panic!(),
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "MyDataSrc::new 1",
                "MyDataSrc::new 2",
                "MyDataSrc::setup 1",
                "MyDataSrc::setup 2",
                "execute logic",
                "MyDataSrc::close 2",
                "MyDataSrc::drop 2",
                "MyDataSrc::close 1",
                "MyDataSrc::drop 1",
            ]
        );
    }
}

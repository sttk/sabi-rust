// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use super::data_src::{copy_global_data_srcs_to_map, create_data_conn_from_global_data_src_async};
use super::{
    DataConn, DataConnContainer, DataConnManager, DataHub, DataSrc, DataSrcManager, ErrEntry,
    SendSyncNonNull,
};

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
        errors: Vec<ErrEntry>,
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
    /// # Parameters
    ///
    /// * `names` - An array of string slices specifying the desired commit order by data connection
    ///   name.
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
    /// # Parameters
    ///
    /// * `name` - The name to associate with this data source.
    /// * `ds` - The data source instance, which must implement `DataSrc` and have a `'static`
    ///   lifetime. If this `DataHub` is moved between threads, `ds` must also implement `Send`.
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
    /// # Parameters
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
    /// # Parameters
    ///
    /// * `logic_fn` - An asynchronous function that takes a mutable reference to `DataHub`
    ///                and returns a `Result`. This function contains the application's logic.
    ///                The returned `Future` must implement `Send`.
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
        for<'a> F:
            FnMut(&'a mut DataHub) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'a>>,
    {
        let mut r = self.begin_async().await;
        if r.is_ok() {
            r = logic_fn(self).await;
        }
        self.end();
        r
    }

    /// Executes a given asynchronous logic function within a managed transaction.
    ///
    /// This method starts by asynchronously setting up local data sources, runs the provided closure,
    /// and then attempts to asynchronously commit all open data connections in the session.
    ///
    /// If any error occurs during the execution of the closure or during the commit phase,
    /// it initiates an asynchronous rollback on all data connections and reports the transaction
    /// failure details.
    /// Finally, it cleans up session resources.
    ///
    /// # Parameters
    ///
    /// * `logic_fn`: An asynchronous closure that encapsulates the business logic to be executed.
    ///   It takes a mutable reference to [`DataHub`] as an argument and returns a pinned, boxed
    ///   future.
    ///
    /// # Type Parameters
    ///
    /// * `F` - The type of the asynchronous transactional logic function.
    ///
    /// # Returns
    ///
    /// * `errs::Result<()>`: `Ok(())` if the closure and the commit phase succeed,
    ///   or an [`errs::Err`] if any phase fails.
    #[allow(clippy::doc_overindented_list_items)]
    pub async fn txn_async<F>(&mut self, mut logic_fn: F) -> errs::Result<()>
    where
        for<'a> F:
            FnMut(&'a mut DataHub) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'a>>,
    {
        let mut r = self.begin_async().await;
        if r.is_ok() {
            r = logic_fn(self).await;
        }

        let mut reports = self.data_conn_manager.new_failure_reports();

        if r.is_ok() {
            r = self.data_conn_manager.commit_async(&mut reports).await;
        }
        if r.is_err() {
            self.data_conn_manager.rollback_async(reports).await;
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
    /// # Parameters
    ///
    /// * `name` - The name of the data connection to retrieve or create.
    ///
    /// # Type Parameters
    ///
    /// * `C` - The expected type of the data connection, which must implement `DataConn` and have
    ///   a `'static` lifetime.
    ///
    /// # Returns
    ///
    /// A `Result` which is `Ok` containing a mutable reference to the data connection
    /// if found or successfully created, or an `Err` if no suitable data source is found
    /// or connection creation fails.
    pub async fn get_data_conn_async<C>(&mut self, name: &str) -> errs::Result<&mut C>
    where
        C: DataConn + 'static,
    {
        if let Some(nnptr) = self.data_conn_manager.find_by_name(name) {
            let typed_nnptr = DataConnManager::to_typed_ptr::<C>(&nnptr)?;
            return Ok(unsafe { &mut (*typed_nnptr).data_conn });
        }

        if let Some((local, index)) = self.data_src_map.get(name) {
            let boxed = if *local {
                self.local_data_src_manager
                    .create_data_conn_async::<C>(*index, name)
                    .await?
            } else {
                create_data_conn_from_global_data_src_async::<C>(*index, name).await?
            };

            let ptr = Box::into_raw(boxed);
            if let Some(nnptr) = ptr::NonNull::new(ptr) {
                let ssnnptr = SendSyncNonNull::new(nnptr);
                self.data_conn_manager.add(ssnnptr);

                let typed_ptr = ptr.cast::<DataConnContainer<C>>();
                return Ok(unsafe { &mut (*typed_ptr).data_conn });
            } else {
                // impossible case.
            }
        }

        Err(errs::Err::new(DataHubError::NoDataSrcToCreateDataConn {
            name: name.into(),
            data_conn_type: any::type_name::<C>(),
        }))
    }
}

#[macro_export]
#[doc(hidden)]
macro_rules! _logic {
    ($f:expr) => {
        |data| {
            let fut: std::pin::Pin<Box<dyn std::future::Future<Output = errs::Result<()>> + Send>> =
                Box::pin(async move { $f(data).await });
            fut
        }
    };
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod tests_of_data_hub {
    use super::*;
    use crate::tokio::_test_commons::*;
    use crate::tokio::{DataConnError, DataSrcError};
    use std::sync::Mutex;

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
        hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        hub.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

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
        hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));

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

        hub.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

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
        hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        hub.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

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
        hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        hub.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

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

        hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        hub.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

        assert!(hub.begin_async().await.is_ok());

        assert!(hub.local_data_src_manager.vec_unready.is_empty());
        assert_eq!(hub.local_data_src_manager.vec_ready.len(), 2);
        assert!(hub.local_data_src_manager.local);
        assert_eq!(hub.data_src_map.len(), 2);
        assert_eq!(hub.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.data_conn_manager.index_map.len(), 0);
        assert!(hub.fixed);

        hub.uses("baz", SyncDataSrc::new(3, logger.clone(), Fail::None));

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

            hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            hub.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

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
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[tokio::test]
    async fn test_begin_but_failed() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            let mut hub = DataHub::new();

            hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            hub.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Setup));
            hub.uses("baz", SyncDataSrc::new(3, logger.clone(), Fail::None));

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
                        assert_eq!(errors[0].index, 1);
                        assert_eq!(errors[0].name, "bar".into());
                        assert_eq!(errors[0].err.reason::<String>().unwrap(), "XXX");
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
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::new 3",
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2 failed",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 3",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[tokio::test]
    async fn test_run_and_ok() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            hub.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

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
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "execute logic",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[tokio::test]
    async fn test_run_but_failed() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            hub.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

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
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "execute logic but fail",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[tokio::test]
    async fn test_txn_and_no_data_access_and_ok() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            hub.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

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
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "execute logic",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[tokio::test]
    async fn test_txn_and_has_data_access_and_ok() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            hub.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            let logger_clone = logger.clone();
            hub.txn_async(move |data| {
                let logger_clone2 = logger_clone.clone();
                Box::pin(async move {
                    logger_clone2
                        .lock()
                        .unwrap()
                        .push("execute logic".to_string());
                    let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                    let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                    Ok(())
                })
            })
            .await
            .unwrap()
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "execute logic",
                "SyncDataSrc::create_data_conn_async 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn_async 2",
                "SyncDataConn::new 2",
                "SyncDataConn::pre_commit_async 1",
                "SyncDataConn::pre_commit_async 2",
                "SyncDataConn::commit_async 1",
                "SyncDataConn::commit_async 2",
                "SyncDataConn::post_commit_async 1",
                "SyncDataConn::post_commit_async 2",
                "SyncDataConn::close 2",
                "SyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[tokio::test]
    async fn test_txn_but_failed_to_run_logic() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            hub.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            let logger_clone = logger.clone();
            if let Err(e) = hub
                .txn_async(move |data| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("execute logic".to_string());
                        let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                        let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
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
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "execute logic",
                "SyncDataSrc::create_data_conn_async 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn_async 2",
                "SyncDataConn::new 2",
                "SyncDataConn::rollback_async 1",
                "SyncDataConn::rollback_async 2",
                "SyncDataConn::on_txn_failure_async 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure_async 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::close 2",
                "SyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1"
            ]
        );
    }

    #[tokio::test]
    async fn test_txn_but_failed_to_pre_commit() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::PreCommit));
            hub.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::PreCommit));

            let logger_clone = logger.clone();
            if let Err(e) = hub
                .txn_async(move |data| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("execute logic".to_string());
                        let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                        let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                        Ok(())
                    })
                })
                .await
            {
                match e.reason::<DataConnError>() {
                    Ok(DataConnError::FailToPreCommitDataConn { errors }) => {
                        assert_eq!(errors.len(), 1);
                        assert_eq!(errors[0].index, 0);
                        assert_eq!(errors[0].name, "foo".into());
                        assert_eq!(errors[0].err.reason::<String>().unwrap(), "zzz");
                    }
                    _ => panic!("{e:?}"),
                }
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "execute logic",
                "SyncDataSrc::create_data_conn_async 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn_async 2",
                "SyncDataConn::new 2",
                "SyncDataConn::pre_commit_async 1 failed",
                "SyncDataConn::rollback_async 1",
                "SyncDataConn::rollback_async 2",
                "SyncDataConn::on_txn_failure_async 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src/tokio/_test_commons.rs, line = 80 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure_async 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src/tokio/_test_commons.rs, line = 80 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::close 2",
                "SyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1"
            ]
        );
    }

    #[tokio::test]
    async fn test_txn_but_failed_to_commit() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::Commit));
            hub.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Commit));

            let logger_clone = logger.clone();
            if let Err(e) = hub
                .txn_async(move |data| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("execute logic".to_string());
                        let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                        let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                        Ok(())
                    })
                })
                .await
            {
                match e.reason::<DataConnError>() {
                    Ok(DataConnError::FailToCommitDataConn { errors }) => {
                        assert_eq!(errors.len(), 1);
                        assert_eq!(errors[0].index, 0);
                        assert_eq!(errors[0].name, "foo".into());
                        assert_eq!(errors[0].err.reason::<String>().unwrap(), "ZZZ");
                    }
                    _ => panic!("{e:?}"),
                }
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "execute logic",
                "SyncDataSrc::create_data_conn_async 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn_async 2",
                "SyncDataConn::new 2",
                "SyncDataConn::pre_commit_async 1",
                "SyncDataConn::pre_commit_async 2",
                "SyncDataConn::commit_async 1 failed",
                "SyncDataConn::rollback_async 1",
                "SyncDataConn::rollback_async 2",
                "SyncDataConn::on_txn_failure_async 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/tokio/_test_commons.rs, line = 61 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure_async 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/tokio/_test_commons.rs, line = 61 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::close 2",
                "SyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1"
            ]
        );
    }

    #[tokio::test]
    async fn test_txn_but_failed_to_post_commit() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::PostCommit));
            hub.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::PostCommit));

            let logger_clone = logger.clone();
            if let Err(e) = hub
                .txn_async(move |data| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("execute logic".to_string());
                        let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                        let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                        Ok(())
                    })
                })
                .await
            {
                match e.reason::<DataConnError>() {
                    Ok(DataConnError::FailToPostCommitDataConn { errors }) => {
                        assert_eq!(errors.len(), 2);
                        assert_eq!(errors[0].index, 0);
                        assert_eq!(errors[0].name, "foo".into());
                        assert_eq!(errors[0].err.reason::<String>().unwrap(), "!!!");
                        assert_eq!(errors[1].index, 1);
                        assert_eq!(errors[1].name, "bar".into());
                        assert_eq!(errors[1].err.reason::<String>().unwrap(), "!!!");
                    }
                    _ => panic!("{e:?}"),
                }
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "execute logic",
                "SyncDataSrc::create_data_conn_async 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn_async 2",
                "SyncDataConn::new 2",
                "SyncDataConn::pre_commit_async 1",
                "SyncDataConn::pre_commit_async 2",
                "SyncDataConn::commit_async 1",
                "SyncDataConn::commit_async 2",
                "SyncDataConn::post_commit_async 1 failed",
                "SyncDataConn::post_commit_async 2 failed",
                "SyncDataConn::on_txn_failure_async 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = 98 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = 98 }), rollback: NoneByNotRolledBack }]",
                "SyncDataConn::on_txn_failure_async 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = 98 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = 98 }), rollback: NoneByNotRolledBack }]",
                "SyncDataConn::close 2",
                "SyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1"
            ]
        );
    }

    #[tokio::test]
    async fn test_txn_but_failed_to_rollback() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::Rollback));
            hub.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Rollback));

            let logger_clone = logger.clone();
            if let Err(e) = hub
                .txn_async(move |data| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("execute logic".to_string());
                        let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                        let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                        Err(errs::Err::new("logic error"))
                    })
                })
                .await
            {
                match e.reason::<&str>() {
                    Ok(s) => assert_eq!(s, &"logic error"),
                    _ => panic!("{e:?}"),
                }
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "execute logic",
                "SyncDataSrc::create_data_conn_async 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn_async 2",
                "SyncDataConn::new 2",
                "SyncDataConn::rollback_async 1 failed",
                "SyncDataConn::rollback_async 2 failed",
                "SyncDataConn::on_txn_failure_async 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = 120 }) }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = 120 }) }]",
                "SyncDataConn::on_txn_failure_async 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = 120 }) }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = 120 }) }]",
                "SyncDataConn::close 2",
                "SyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1"
            ]
        );
    }

    #[tokio::test]
    async fn test_txn_with_commit_order() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::with_commit_order(&["bar", "foo"]);

            hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            hub.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            let logger_clone = logger.clone();
            hub.txn_async(move |data| {
                let logger_clone2 = logger_clone.clone();
                Box::pin(async move {
                    logger_clone2
                        .lock()
                        .unwrap()
                        .push("execute logic".to_string());
                    let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                    let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "execute logic",
                "SyncDataSrc::create_data_conn_async 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn_async 2",
                "SyncDataConn::new 2",
                "SyncDataConn::pre_commit_async 2",
                "SyncDataConn::pre_commit_async 1",
                "SyncDataConn::commit_async 2",
                "SyncDataConn::commit_async 1",
                "SyncDataConn::post_commit_async 2",
                "SyncDataConn::post_commit_async 1",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataConn::close 2",
                "SyncDataConn::drop 2",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[tokio::test]
    async fn test_txn_but_fail_to_setup() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::Setup));

            let logger_clone = logger.clone();

            if let Err(e) = hub
                .txn_async(move |_data| {
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
            {
                match e.reason::<DataHubError>() {
                    Ok(DataHubError::FailToSetupLocalDataSrcs { errors }) => {
                        assert_eq!(errors.len(), 1);
                        assert_eq!(errors[0].index, 0);
                        assert_eq!(errors[0].name, "foo".into());
                        assert_eq!(errors[0].err.reason::<String>().unwrap(), "XXX");
                    }
                    _ => panic!(),
                }
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::setup_async 1 failed",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[tokio::test]
    async fn test_get_data_conn_cached() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));

            let logger_clone = logger.clone();

            if let Err(e) = hub
                .txn_async(move |data| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("execute logic".to_string());
                        let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                        let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                        Ok(())
                    })
                })
                .await
            {
                panic!("{e:?}");
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::setup_async 1",
                "execute logic",
                "SyncDataSrc::create_data_conn_async 1",
                "SyncDataConn::new 1",
                "SyncDataConn::pre_commit_async 1",
                "SyncDataConn::commit_async 1",
                "SyncDataConn::post_commit_async 1",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[tokio::test]
    async fn test_get_data_conn_and_no_data_src_to_create_data_conn() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            hub.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            let logger_clone = logger.clone();
            let err = hub
                .txn_async(move |data| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("execute logic".to_string());
                        let _conn1 = data.get_data_conn_async::<SyncDataConn>("fxx").await?;
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
                        assert_eq!(data_conn_type, &"sabi::tokio::_test_commons::SyncDataConn");
                    }
                    _ => panic!(),
                },
                _ => panic!(),
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "execute logic",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[tokio::test]
    async fn test_get_data_conn_and_failed_to_creata_data_conn() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses(
                "foo",
                SyncDataSrc::new(1, logger.clone(), Fail::CreateDataConn),
            );

            let logger_clone = logger.clone();

            let err = hub
                .txn_async(move |data| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("execute logic".to_string());
                        data.get_data_conn_async::<SyncDataConn>("foo").await?;
                        Ok(())
                    })
                })
                .await
                .unwrap_err();

            match err.reason::<DataSrcError>() {
                Ok(DataSrcError::FailToCreateDataConn {
                    name,
                    data_conn_type,
                }) => {
                    assert_eq!(name.as_ref(), "foo");
                    assert_eq!(data_conn_type, &"sabi::tokio::_test_commons::SyncDataConn");
                }
                _ => panic!(),
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::setup_async 1",
                "execute logic",
                "SyncDataSrc::create_data_conn_async 1",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[tokio::test]
    async fn test_get_data_conn_and_failed_to_cast_data_conn() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));

            let logger_clone = logger.clone();

            let err = hub
                .txn_async(move |data| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("execute logic".to_string());
                        if let Err(e) = data.get_data_conn_async::<AsyncDataConn>("foo").await {
                            match e.reason::<DataSrcError>() {
                                Ok(DataSrcError::FailToCastDataConn { name, target_type }) => {
                                    assert_eq!(name.as_ref(), "foo");
                                    assert_eq!(
                                        target_type,
                                        &"sabi::tokio::_test_commons::AsyncDataConn"
                                    );
                                }
                                _ => panic!("{e:?}"),
                            }
                        } else {
                            panic!();
                        }

                        let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;

                        if let Err(e) = data.get_data_conn_async::<AsyncDataConn>("foo").await {
                            match e.reason::<DataConnError>() {
                                Ok(DataConnError::FailToCastDataConn { name, target_type }) => {
                                    assert_eq!(name.as_ref(), "foo");
                                    assert_eq!(
                                        target_type,
                                        &"sabi::tokio::_test_commons::AsyncDataConn"
                                    );
                                    Err(e)
                                }
                                _ => panic!("{e:?}"),
                            }
                        } else {
                            panic!();
                        }
                    })
                })
                .await
                .unwrap_err();

            match err.reason::<DataConnError>() {
                Ok(DataConnError::FailToCastDataConn { name, target_type }) => {
                    assert_eq!(name.as_ref(), "foo");
                    assert_eq!(target_type, &"sabi::tokio::_test_commons::AsyncDataConn");
                }
                _ => panic!("{err:?}"),
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::setup_async 1",
                "execute logic",
                "SyncDataSrc::create_data_conn_async 1",
                "SyncDataConn::new 1",
                "SyncDataConn::rollback_async 1",
                "SyncDataConn::on_txn_failure_async 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1"
            ]
        );
    }

    trait Data {}
    impl Data for DataHub {}

    async fn process_async(_data: &mut impl Data) -> errs::Result<()> {
        Ok(())
    }

    #[tokio::test]
    async fn data_hub_implements_send_trait() {
        let handle = tokio::spawn(async {
            let mut data = DataHub::new();
            data.run_async(_logic!(process_async)).await.unwrap();
        });

        handle.await.unwrap();
    }

    #[tokio::test]
    async fn txn_async_in_spawn() {
        let handle = tokio::spawn(async {
            let mut data = DataHub::new();
            data.txn_async(_logic!(process_async)).await.unwrap();
        });

        handle.await.unwrap();
    }
}

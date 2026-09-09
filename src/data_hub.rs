// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::data_src::{copy_global_data_srcs_to_map, create_data_conn_from_global_data_src};
use crate::{DataConn, DataConnManager, DataHub, DataSrc, DataSrcManager, SendSyncNonNull};

#[allow(unused)] // for rustdoc
use crate::DataAcc;

use crate::{DataConnContainer, ErrEntry};

use std::collections::HashMap;
use std::sync::Arc;
use std::{any, ptr};

/// An enum type representing the reasons for errors that can occur within [`DataHub`] operations.
#[derive(Debug)]
pub enum DataHubError {
    /// Indicates a failure during the setup process of one or more session-local data sources.
    /// Contains a vector of data source names and their corresponding errors.
    FailToSetupLocalDataSrcs {
        /// The vector contains errors that occurred in each [`DataSrc`] object.
        errors: Vec<ErrEntry>,
    },

    /// Indicates that no [`DataSrc`] was found to create a [`DataConn`] for the specified name
    /// and type.
    NoDataSrcToCreateDataConn {
        /// The name of the data source that could not be found.
        name: Arc<str>,

        /// The type name of the [`DataConn`] that was requested.
        data_conn_type: &'static str,
    },
}

impl DataHub {
    /// Creates a new [`DataHub`] instance.
    ///
    /// Upon creation, it collects references to globally set-up data sources
    /// into its internal map for quick access.
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

    /// Creates a new [`DataHub`] instance with a specified commit order for data connections.
    ///
    /// This constructor allows defining a specific order for pre-commit, commit, and post-commit
    /// operations for named data connections. Data connections not specified in `names` will
    /// be processed after the named ones, in their order of acquisition.
    ///
    /// Upon creation, it collects references to globally set-up data sources
    /// into its internal map for quick access.
    ///
    /// # Parameters
    ///
    /// * `names`: A slice of `&str` representing the names of data connections to commit in a
    ///   specific order.
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

    /// Registers a session-local data source with this [`DataHub`] instance.
    ///
    /// This method is similar to the global [`uses!`] macro but registers a data source
    /// that is local to this specific [`DataHub`] session. Once the [`DataHub`]'s state is
    /// "fixed" (while [`DataHub::run`] or [`DataHub::txn`] method is executing),
    /// further calls to `uses` are ignored. However, after the method completes,
    /// the [`DataHub`]'s "fixed" state is reset, allowing for new data sources to be
    /// registered or removed via [`DataHub::disuses`] method in subsequent operations.
    ///
    /// # Parameters
    ///
    /// * `name`: The unique name for the local data source.
    /// * `ds`: The [`DataSrc`] instance to register.
    #[allow(rustdoc::broken_intra_doc_links)]
    pub fn uses<S, C>(&mut self, name: impl Into<Arc<str>>, ds: S)
    where
        S: DataSrc<C>,
        C: DataConn + 'static,
    {
        if self.fixed {
            return;
        }
        self.local_data_src_manager.add(name, ds);
    }

    /// Unregisters and drops a session-local data source by its name.
    ///
    /// This method removes a data source that was previously registered via [`DataHub::uses`].
    /// This operation is ignored if the [`DataHub`]'s state is already "fixed".
    ///
    /// # Parameters
    ///
    /// * `name`: The name of the local data source to unregister.
    pub fn disuses(&mut self, name: impl AsRef<str>) {
        if self.fixed {
            return;
        }
        self.data_src_map.remove(name.as_ref());
        self.local_data_src_manager.remove(name);
    }

    #[inline]
    fn begin(&mut self) -> errs::Result<()> {
        self.fixed = true;

        let mut errors = Vec::new();

        self.local_data_src_manager.setup(&mut errors);
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

    /// Executes a given logic function without transaction control.
    ///
    /// This method sets up local data sources, runs the provided closure,
    /// and then cleans up the [`DataHub`]'s session resources. It does not
    /// perform commit or rollback operations.
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
    pub fn run<F>(&mut self, mut logic_fn: F) -> errs::Result<()>
    where
        F: FnMut(&mut DataHub) -> errs::Result<()>,
    {
        let mut r = self.begin();
        if r.is_ok() {
            r = logic_fn(self);
        }
        self.end();
        r
    }

    /// Executes a given logic function within a managed transaction.
    ///
    /// This method starts by setting up local data sources, runs the provided closure,
    /// and then attempts to commit all open data connections in the session.
    ///
    /// If any error occurs during the execution of the closure or during the commit phase,
    /// it initiates a rollback on all data connections and reports the transaction failure details.
    /// Finally, it cleans up session resources.
    ///
    /// # Parameters
    ///
    /// * `logic_fn`: A closure that encapsulates the business logic to be executed.
    ///   It takes a mutable reference to [`DataHub`] as an argument.
    ///
    /// # Returns
    ///
    /// * `errs::Result<()>`: `Ok(())` if the closure and the commit phase succeed,
    ///   or an [`errs::Err`] if any phase fails.
    pub fn txn<F>(&mut self, mut logic_fn: F) -> errs::Result<()>
    where
        F: FnMut(&mut DataHub) -> errs::Result<()>,
    {
        let mut r = self.begin();
        if r.is_ok() {
            r = logic_fn(self);
        }

        let mut reports = self.data_conn_manager.new_failure_reports();

        if r.is_ok() {
            r = self.data_conn_manager.commit(&mut reports);
        }
        if r.is_err() {
            self.data_conn_manager.rollback(reports);
        }

        self.end();
        r
    }

    /// Retrieves a mutable reference to a [`DataConn`] object by name, creating it if necessary.
    ///
    /// This is the core method used by [`DataAcc`] implementations to obtain connections
    /// to external data services. It first checks if a [`DataConn`] with the given name
    /// already exists in the [`DataHub`]'s session. If not, it attempts to find a
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
    pub fn get_data_conn<C>(&mut self, name: &str) -> errs::Result<&mut C>
    where
        C: DataConn + 'static,
    {
        if let Some(ssnnptr) = self.data_conn_manager.find_by_name(name) {
            let typed_ssnnptr = DataConnManager::to_typed_ptr::<C>(&ssnnptr)?;
            return Ok(unsafe { &mut (*typed_ssnnptr).data_conn });
        }

        if let Some((local, index)) = self.data_src_map.get(name) {
            let boxed = if *local {
                self.local_data_src_manager
                    .create_data_conn::<C>(*index, name)?
            } else {
                create_data_conn_from_global_data_src::<C>(*index, name)?
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

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod tests_of_data_hub {
    use super::*;
    use crate::_test_commons::*;
    use crate::{DataConnError, DataSrcError};
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

    #[test]
    fn test_uses_and_ok() {
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

        assert!(hub.begin().is_ok());

        assert_eq!(hub.local_data_src_manager.vec_unready.len(), 0);
        assert_eq!(hub.local_data_src_manager.vec_ready.len(), 2);
        assert!(hub.local_data_src_manager.local);
        assert_eq!(hub.data_src_map.len(), 2);
        assert_eq!(hub.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.data_conn_manager.index_map.len(), 0);
        assert!(hub.fixed);
    }

    #[test]
    fn test_uses_but_already_fixed() {
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

        assert!(hub.begin().is_ok());

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

    #[test]
    fn test_disuses_and_fix() {
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

        assert!(hub.begin().is_ok());

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

    #[test]
    fn test_begin_if_empty() {
        let mut hub = DataHub::new();
        assert!(hub.begin().is_ok());

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

    #[test]
    fn test_begin_and_ok() {
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

            assert_eq!(hub.begin().is_ok(), true);

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
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_begin_but_failed() {
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

            if let Err(err) = hub.begin() {
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
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2 failed",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 3",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_run_and_ok() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            hub.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            let logger_clone = logger.clone();
            assert!(hub
                .run(move |_data| {
                    logger_clone
                        .lock()
                        .unwrap()
                        .push("execute logic".to_string());
                    Ok(())
                })
                .is_ok());
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "execute logic",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_run_but_failed_to_run_logic() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            hub.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            let logger_clone = logger.clone();
            if let Err(err) = hub.run(move |_data| {
                logger_clone
                    .lock()
                    .unwrap()
                    .push("execute logic but fail".to_string());
                Err(errs::Err::new("logic error".to_string()))
            }) {
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
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "execute logic but fail",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_txn_and_no_data_access_and_ok() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            hub.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            let logger_clone = logger.clone();
            assert!(hub
                .txn(move |_data| {
                    logger_clone
                        .lock()
                        .unwrap()
                        .push("execute logic".to_string());
                    Ok(())
                })
                .is_ok());
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "execute logic",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_txn_and_has_data_access_and_ok() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            hub.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            let logger_clone = logger.clone();
            hub.txn(move |data| {
                logger_clone
                    .lock()
                    .unwrap()
                    .push("execute logic".to_string());
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                let _conn2 = data.get_data_conn::<SyncDataConn>("bar")?;
                Ok(())
            })
            .unwrap();
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "execute logic",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn 2",
                "SyncDataConn::new 2",
                "SyncDataConn::pre_commit 1",
                "SyncDataConn::pre_commit 2",
                "SyncDataConn::commit 1",
                "SyncDataConn::commit 2",
                "SyncDataConn::post_commit 1",
                "SyncDataConn::post_commit 2",
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

    #[test]
    fn test_txn_but_failed_to_run_logic() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            hub.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            let logger_clone = logger.clone();
            if let Err(e) = hub.txn(move |data| {
                logger_clone
                    .lock()
                    .unwrap()
                    .push("execute logic".to_string());
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                let _conn2 = data.get_data_conn::<SyncDataConn>("bar")?;
                Err(errs::Err::new("logic error"))
            }) {
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
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "execute logic",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn 2",
                "SyncDataConn::new 2",
                "SyncDataConn::rollback 1",
                "SyncDataConn::rollback 2",
                "SyncDataConn::on_txn_failure 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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

    #[test]
    fn test_txn_but_failed_to_pre_commit() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::PreCommit));
            hub.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::PreCommit));

            let logger_clone = logger.clone();
            if let Err(e) = hub.txn(move |data| {
                logger_clone
                    .lock()
                    .unwrap()
                    .push("execute logic".to_string());
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                let _conn2 = data.get_data_conn::<SyncDataConn>("bar")?;
                Ok(())
            }) {
                match e.reason::<DataConnError>() {
                    Ok(DataConnError::FailToPreCommitDataConn { errors }) => {
                        assert_eq!(errors.len(), 1);
                        assert_eq!(errors[0].index, 0);
                        assert_eq!(errors[0].name, "foo".into());
                        assert_eq!(errors[0].err.reason::<String>().unwrap(), "zzz");
                    }
                    _ => panic!(),
                }
            }
        }

        #[cfg(unix)]
        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "execute logic",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn 2",
                "SyncDataConn::new 2",
                "SyncDataConn::pre_commit 1 failed",
                "SyncDataConn::rollback 1",
                "SyncDataConn::rollback 2",
                "SyncDataConn::on_txn_failure 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src/_test_commons.rs, line = 75 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src/_test_commons.rs, line = 75 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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
        #[cfg(windows)]
        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "execute logic",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn 2",
                "SyncDataConn::new 2",
                "SyncDataConn::pre_commit 1 failed",
                "SyncDataConn::rollback 1",
                "SyncDataConn::rollback 2",
                "SyncDataConn::on_txn_failure 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src\\_test_commons.rs, line = 75 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src\\_test_commons.rs, line = 75 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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

    #[test]
    fn test_txn_but_failed_to_commit() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::Commit));
            hub.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Commit));

            let logger_clone = logger.clone();
            if let Err(e) = hub.txn(move |data| {
                logger_clone
                    .lock()
                    .unwrap()
                    .push("execute logic".to_string());
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                let _conn2 = data.get_data_conn::<SyncDataConn>("bar")?;
                Ok(())
            }) {
                match e.reason::<DataConnError>() {
                    Ok(DataConnError::FailToCommitDataConn { errors }) => {
                        assert_eq!(errors.len(), 1);
                        assert_eq!(errors[0].index, 0);
                        assert_eq!(errors[0].name, "foo".into());
                        assert_eq!(errors[0].err.reason::<String>().unwrap(), &"ZZZ");
                    }
                    _ => panic!(),
                }
            }
        }

        #[cfg(unix)]
        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "execute logic",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn 2",
                "SyncDataConn::new 2",
                "SyncDataConn::pre_commit 1",
                "SyncDataConn::pre_commit 2",
                "SyncDataConn::commit 1 failed",
                "SyncDataConn::rollback 1",
                "SyncDataConn::rollback 2",
                "SyncDataConn::on_txn_failure 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/_test_commons.rs, line = 59 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/_test_commons.rs, line = 59 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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
        #[cfg(windows)]
        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "execute logic",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn 2",
                "SyncDataConn::new 2",
                "SyncDataConn::pre_commit 1",
                "SyncDataConn::pre_commit 2",
                "SyncDataConn::commit 1 failed",
                "SyncDataConn::rollback 1",
                "SyncDataConn::rollback 2",
                "SyncDataConn::on_txn_failure 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src\\_test_commons.rs, line = 59 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src\\_test_commons.rs, line = 59 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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

    #[test]
    fn test_txn_but_failed_to_post_commit() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::PostCommit));
            hub.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::PostCommit));

            let logger_clone = logger.clone();
            if let Err(e) = hub.txn(move |data| {
                logger_clone
                    .lock()
                    .unwrap()
                    .push("execute logic".to_string());
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                let _conn2 = data.get_data_conn::<SyncDataConn>("bar")?;
                Ok(())
            }) {
                match e.reason::<DataConnError>() {
                    Ok(DataConnError::FailToPostCommitDataConn { errors }) => {
                        assert_eq!(errors.len(), 2);
                        assert_eq!(errors[0].index, 0);
                        assert_eq!(errors[0].name, "foo".into());
                        assert_eq!(errors[0].err.reason::<String>().unwrap(), "!!!",);
                        assert_eq!(errors[1].index, 1);
                        assert_eq!(errors[1].name, "bar".into());
                        assert_eq!(errors[1].err.reason::<String>().unwrap(), "!!!");
                    }
                    _ => panic!(),
                }
            }
        }

        #[cfg(unix)]
        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "execute logic",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn 2",
                "SyncDataConn::new 2",
                "SyncDataConn::pre_commit 1",
                "SyncDataConn::pre_commit 2",
                "SyncDataConn::commit 1",
                "SyncDataConn::commit 2",
                "SyncDataConn::post_commit 1 failed",
                "SyncDataConn::post_commit 2 failed",
                "SyncDataConn::on_txn_failure 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/_test_commons.rs, line = 93 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/_test_commons.rs, line = 93 }), rollback: NoneByNotRolledBack }]",
                "SyncDataConn::on_txn_failure 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/_test_commons.rs, line = 93 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/_test_commons.rs, line = 93 }), rollback: NoneByNotRolledBack }]",
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
        #[cfg(windows)]
        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "execute logic",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn 2",
                "SyncDataConn::new 2",
                "SyncDataConn::pre_commit 1",
                "SyncDataConn::pre_commit 2",
                "SyncDataConn::commit 1",
                "SyncDataConn::commit 2",
                "SyncDataConn::post_commit 1 failed",
                "SyncDataConn::post_commit 2 failed",
                "SyncDataConn::on_txn_failure 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\_test_commons.rs, line = 93 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\_test_commons.rs, line = 93 }), rollback: NoneByNotRolledBack }]",
                "SyncDataConn::on_txn_failure 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\_test_commons.rs, line = 93 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\_test_commons.rs, line = 93 }), rollback: NoneByNotRolledBack }]",
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

    #[test]
    fn test_txn_but_failed_to_rollback() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::Rollback));
            hub.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Rollback));

            let logger_clone = logger.clone();
            if let Err(e) = hub.txn(move |data| {
                logger_clone
                    .lock()
                    .unwrap()
                    .push("execute logic".to_string());
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                let _conn2 = data.get_data_conn::<SyncDataConn>("bar")?;
                Err(errs::Err::new("logic error"))
            }) {
                match e.reason::<&str>() {
                    Ok(s) => assert_eq!(s, &"logic error"),
                    _ => panic!(),
                }
            }
        }

        #[cfg(unix)]
        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "execute logic",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn 2",
                "SyncDataConn::new 2",
                "SyncDataConn::rollback 1 failed",
                "SyncDataConn::rollback 2 failed",
                "SyncDataConn::on_txn_failure 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src/_test_commons.rs, line = 112 }) }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src/_test_commons.rs, line = 112 }) }]",
                "SyncDataConn::on_txn_failure 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src/_test_commons.rs, line = 112 }) }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src/_test_commons.rs, line = 112 }) }]",
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
        #[cfg(windows)]
        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "execute logic",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn 2",
                "SyncDataConn::new 2",
                "SyncDataConn::rollback 1 failed",
                "SyncDataConn::rollback 2 failed",
                "SyncDataConn::on_txn_failure 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src\\_test_commons.rs, line = 112 }) }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src\\_test_commons.rs, line = 112 }) }]",
                "SyncDataConn::on_txn_failure 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src\\_test_commons.rs, line = 112 }) }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src\\_test_commons.rs, line = 112 }) }]",
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

    #[test]
    fn test_txn_with_commit_order() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::with_commit_order(&["bar", "foo"]);

            hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            hub.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            let logger_clone = logger.clone();

            if let Err(e) = hub.txn(move |data| {
                logger_clone
                    .lock()
                    .unwrap()
                    .push("execute logic".to_string());
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                let _conn2 = data.get_data_conn::<SyncDataConn>("bar")?;
                Ok(())
            }) {
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
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "execute logic",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn 2",
                "SyncDataConn::new 2",
                "SyncDataConn::pre_commit 2",
                "SyncDataConn::pre_commit 1",
                "SyncDataConn::commit 2",
                "SyncDataConn::commit 1",
                "SyncDataConn::post_commit 2",
                "SyncDataConn::post_commit 1",
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

    #[test]
    fn test_txn_but_fail_to_setup() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::Setup));

            let logger_clone = logger.clone();

            if let Err(e) = hub.txn(move |_data| {
                logger_clone
                    .lock()
                    .unwrap()
                    .push("execute logic".to_string());
                Ok(())
            }) {
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
                "SyncDataSrc::setup 1 failed",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_get_data_conn_cached() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));

            let logger_clone = logger.clone();

            if let Err(e) = hub.txn(move |data| {
                logger_clone
                    .lock()
                    .unwrap()
                    .push("execute logic".to_string());
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                Ok(())
            }) {
                panic!("{:?}", e);
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::setup 1",
                "execute logic",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "SyncDataConn::pre_commit 1",
                "SyncDataConn::commit 1",
                "SyncDataConn::post_commit 1",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_get_data_conn_and_no_data_src_to_create_data_conn() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            let logger_clone = logger.clone();

            if let Err(e) = hub.txn(move |data| {
                logger_clone
                    .lock()
                    .unwrap()
                    .push("execute logic".to_string());
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                Ok(())
            }) {
                match e.reason::<DataHubError>() {
                    Ok(DataHubError::NoDataSrcToCreateDataConn {
                        name,
                        data_conn_type,
                    }) => {
                        assert_eq!(name.as_ref(), "foo");
                        assert_eq!(data_conn_type, &"sabi::_test_commons::SyncDataConn");
                    }
                    _ => panic!(),
                }
            }
        }

        assert_eq!(*logger.lock().unwrap(), &["execute logic",]);
    }

    #[test]
    fn test_get_data_conn_and_failed_to_creata_data_conn() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses(
                "foo",
                SyncDataSrc::new(1, logger.clone(), Fail::CreateDataConn),
            );

            let logger_clone = logger.clone();

            if let Err(e) = hub.txn(move |data| {
                logger_clone
                    .lock()
                    .unwrap()
                    .push("execute logic".to_string());
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                Ok(())
            }) {
                match e.reason::<DataSrcError>() {
                    Ok(DataSrcError::FailToCreateDataConn {
                        name,
                        data_conn_type,
                    }) => {
                        assert_eq!(name.as_ref(), "foo");
                        assert_eq!(data_conn_type, &"sabi::_test_commons::SyncDataConn");
                    }
                    _ => panic!(),
                }
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::setup 1",
                "execute logic",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_get_data_conn_and_failed_to_cast_data_conn() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));

            let logger_clone = logger.clone();

            if let Err(e) = hub.txn(move |data| {
                logger_clone
                    .lock()
                    .unwrap()
                    .push("execute logic".to_string());
                if let Err(e) = data.get_data_conn::<AsyncDataConn>("foo") {
                    match e.reason::<DataSrcError>() {
                        Ok(DataSrcError::FailToCastDataConn { name, target_type }) => {
                            assert_eq!(name.as_ref(), "foo");
                            assert_eq!(target_type, &"sabi::_test_commons::AsyncDataConn");
                        }
                        _ => panic!("{e:?}"),
                    }
                } else {
                    panic!();
                }

                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;

                if let Err(e) = data.get_data_conn::<AsyncDataConn>("foo") {
                    match e.reason::<DataConnError>() {
                        Ok(DataConnError::FailToCastDataConn { name, target_type }) => {
                            assert_eq!(name.as_ref(), "foo");
                            assert_eq!(target_type, &"sabi::_test_commons::AsyncDataConn");
                            Err(e)
                        }
                        _ => panic!("{e:?}"),
                    }
                } else {
                    panic!();
                }
            }) {
                match e.reason::<DataConnError>() {
                    Ok(DataConnError::FailToCastDataConn { name, target_type }) => {
                        assert_eq!(name.as_ref(), "foo");
                        assert_eq!(target_type, &"sabi::_test_commons::AsyncDataConn");
                    }
                    _ => panic!(),
                }
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::setup 1",
                "execute logic",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "SyncDataConn::rollback 1",
                "SyncDataConn::on_txn_failure 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn data_hub_implements_send_trait() {
        let mut data = DataHub::new();
        let handle = std::thread::spawn(move || {
            data.run(|_data| Ok(())).unwrap();
        });

        handle.join().unwrap();
    }
}

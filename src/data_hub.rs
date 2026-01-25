// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::data_src::{copy_global_data_srcs_to_map, create_data_conn_from_global_data_src};
use crate::{DataConn, DataConnManager, DataHub, DataSrc, DataSrcManager};

#[allow(unused)] // for rustdoc
use crate::DataAcc;

use crate::DataConnContainer;

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
        errors: Vec<(Arc<str>, errs::Err)>,
    },

    /// Indicates a failure during the pre-commit process of one or more [`DataConn`] instances
    /// involved in a transaction.
    /// Contains a vector of data connection names and their corresponding errors.
    FailToPreCommitDataConn {
        /// The vector contains errors that occurred in each [`DataConn`] object.
        errors: Vec<(Arc<str>, errs::Err)>,
    },

    /// Indicates a failure during the commit process of one or more [`DataConn`] instances
    /// involved in a transaction.
    /// Contains a vector of data connection names and their corresponding errors.
    FailToCommitDataConn {
        /// The vector contains errors that occurred in each [`DataConn`] object.
        errors: Vec<(Arc<str>, errs::Err)>,
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

    #[allow(rustdoc::broken_intra_doc_links)]
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
    fn commit(&mut self) -> errs::Result<()> {
        self.data_conn_manager.commit()
    }

    #[inline]
    fn rollback(&mut self) {
        self.data_conn_manager.rollback()
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

    /// Executes a given logic function within a transaction.
    ///
    /// This method first sets up local data sources, then runs the provided closure.
    /// If the closure returns `Ok`, it attempts to commit all changes. If the commit fails,
    /// or if the logic function itself returns an [`errs::Err`], a rollback operation
    /// is performed. After succeeding `pre_commit` and `commit` methods of all [`DataConn`]s,
    /// `post_commit` methods of all [`DataConn`]s are executed.
    /// Finally, it cleans up the [`DataHub`]'s session resources.
    ///
    /// # Parameters
    ///
    /// * `logic_fn`: A closure that encapsulates the business logic to be executed.
    ///   It takes a mutable reference to [`DataHub`] as an argument.
    ///
    /// # Returns
    ///
    /// * `errs::Result<()>`: The final result of the transaction (success or failure of
    ///   logic/commit), or an error if executing `logic_fn` fails.
    pub fn txn<F>(&mut self, mut logic_fn: F) -> errs::Result<()>
    where
        F: FnMut(&mut DataHub) -> errs::Result<()>,
    {
        let mut r = self.begin();
        if r.is_ok() {
            r = logic_fn(self);
        }
        if r.is_ok() {
            r = self.commit();
        }
        if r.is_err() {
            self.rollback();
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
    pub fn get_data_conn<C>(&mut self, name: impl AsRef<str>) -> errs::Result<&mut C>
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
                    .create_data_conn::<C>(*index, name.as_ref())?
            } else {
                create_data_conn_from_global_data_src::<C>(*index, name.as_ref())?
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

#[cfg(test)]
mod tests_of_data_hub {
    use super::*;
    use crate::AsyncGroup;
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
        fn pre_commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
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
        fn commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
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
        fn post_commit(&mut self, _ag: &mut AsyncGroup) {
            self.logger
                .lock()
                .unwrap()
                .push(format!("MyDataConn::post_commit {}", self.id));
        }
        fn rollback(&mut self, _ag: &mut AsyncGroup) {
            self.logger
                .lock()
                .unwrap()
                .push(format!("MyDataConn::rollback {}", self.id));
        }
        fn force_back(&mut self, _ag: &mut AsyncGroup) {
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
        fn setup(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
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
        fn create_data_conn(&mut self) -> errs::Result<Box<MyDataConn>> {
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

    #[test]
    fn test_uses_and_ok() {
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
        hub.uses("foo", MyDataSrc::new(1, logger.clone(), Failure::None));

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

    #[test]
    fn test_disuses_and_fix() {
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

        assert!(hub.begin().is_ok());

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

            hub.uses("foo", MyDataSrc::new(1, logger.clone(), Failure::None));
            hub.uses("bar", MyDataSrc::new(2, logger.clone(), Failure::None));

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

    #[test]
    fn test_begin_but_failed() {
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

            if let Err(err) = hub.begin() {
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

    #[test]
    fn test_run_and_ok() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", MyDataSrc::new(1, logger.clone(), Failure::None));
            hub.uses("bar", MyDataSrc::new(2, logger.clone(), Failure::None));

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

    #[test]
    fn test_run_but_failed() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", MyDataSrc::new(1, logger.clone(), Failure::None));
            hub.uses("bar", MyDataSrc::new(2, logger.clone(), Failure::None));

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

    #[test]
    fn test_txn_and_no_data_access_and_ok() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", MyDataSrc::new(1, logger.clone(), Failure::None));
            hub.uses("bar", MyDataSrc::new(2, logger.clone(), Failure::None));

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

    #[test]
    fn test_txn_and_has_data_access_and_ok() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", MyDataSrc::new(1, logger.clone(), Failure::None));
            hub.uses("bar", MyDataSrc::new(2, logger.clone(), Failure::None));

            let logger_clone = logger.clone();
            hub.txn(move |data| {
                logger_clone
                    .lock()
                    .unwrap()
                    .push("execute logic".to_string());
                let _conn1 = data.get_data_conn::<MyDataConn>("foo")?;
                let _conn2 = data.get_data_conn::<MyDataConn>("bar")?;
                Ok(())
            })
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

    #[test]
    fn test_txn_but_failed() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", MyDataSrc::new(1, logger.clone(), Failure::None));
            hub.uses("bar", MyDataSrc::new(2, logger.clone(), Failure::None));

            let logger_clone = logger.clone();
            if let Err(e) = hub.txn(move |data| {
                logger_clone
                    .lock()
                    .unwrap()
                    .push("execute logic".to_string());
                let _conn1 = data.get_data_conn::<MyDataConn>("foo")?;
                let _conn2 = data.get_data_conn::<MyDataConn>("bar")?;
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

    #[test]
    fn test_txn_with_commit_order() {
        let _logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut _hub = DataHub::new();
        }
    }

    #[test]
    fn test_get_data_conn_and_ok() {
        let _logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut _hub = DataHub::new();
        }
    }

    #[test]
    fn test_get_data_conn_and_failed() {
        let _logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut _hub = DataHub::new();
        }
    }
}

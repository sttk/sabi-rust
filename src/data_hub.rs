// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::data_src::{copy_global_data_srcs_to_map, create_data_conn_from_global_data_src};
use crate::{
    DataConn, DataConnManager, DataHub, DataSrc, DataSrcManager, SendSyncNonNull, TxnFailureReport,
};

#[allow(unused)] // for rustdoc
use crate::DataAcc;

use crate::{DataConnContainer, ErrEntry};

use std::collections::HashMap;
use std::sync::Arc;
use std::{any, mem, ptr};

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

    FailToRunLogics {
        errors: Vec<ErrEntry>,
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
    pub(crate) fn begin(&mut self) -> errs::Result<()> {
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
    pub(crate) fn new_failure_reports(&self) -> Vec<TxnFailureReport> {
        self.data_conn_manager.new_failure_reports()
    }

    #[inline]
    pub(crate) fn commit(&mut self, reports: &mut [TxnFailureReport]) -> errs::Result<()> {
        self.data_conn_manager.commit(reports)
    }

    #[inline]
    pub(crate) fn rollback(&mut self, reports: Vec<TxnFailureReport>) {
        self.data_conn_manager.rollback(reports);
    }

    #[inline]
    pub(crate) fn end(&mut self) {
        self.data_conn_manager.close();
        self.fixed = false;
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

    pub fn start(&mut self) -> Runner<'_> {
        Runner::new(self, false)
    }
}

enum RunnerErrAt {
    Start { err: errs::Err },
    Run { errors: Vec<ErrEntry> },
    Block { errors: Vec<ErrEntry> },
}

pub struct Runner<'a> {
    hub: &'a mut DataHub,
    err: RunnerErrAt,
    index: usize,
    nested: bool,
}

impl<'a> Runner<'a> {
    pub(crate) fn new(hub: &'a mut DataHub, nested: bool) -> Runner<'a> {
        if nested {
            Self {
                hub,
                err: RunnerErrAt::Run {
                    errors: Vec::with_capacity(0),
                },
                index: 0,
                nested,
            }
        } else if let Err(err) = hub.begin() {
            Self {
                hub,
                err: RunnerErrAt::Start { err },
                index: 0,
                nested,
            }
        } else {
            Self {
                hub,
                err: RunnerErrAt::Run {
                    errors: Vec::with_capacity(0),
                },
                index: 0,
                nested,
            }
        }
    }

    pub fn run<F>(mut self, mut logic_fn: F) -> Self
    where
        F: FnMut(&mut DataHub) -> errs::Result<()>,
    {
        let index = self.index;
        self.index = index + 1;

        match self.err {
            RunnerErrAt::Run { ref mut errors } => {
                if errors.is_empty() {
                    if let Err(err) = logic_fn(self.hub) {
                        errors.push(ErrEntry {
                            index,
                            name: format!("Runner#run(logic-{})", index).into(),
                            err,
                        });
                    }
                }
                self
            }
            _ => self,
        }
    }

    pub fn run_force<F>(mut self, mut logic_fn: F) -> Self
    where
        F: FnMut(&mut DataHub) -> errs::Result<()>,
    {
        let index = self.index;
        self.index = index + 1;

        match self.err {
            RunnerErrAt::Run { ref mut errors } => {
                if let Err(err) = logic_fn(self.hub) {
                    errors.push(ErrEntry {
                        index,
                        name: format!("Runner#run_force(logic-{})", index).into(),
                        err,
                    });
                }
                self
            }
            _ => self,
        }
    }

    pub fn run_or_block<F>(mut self, mut logic_fn: F) -> Self
    where
        F: FnMut(&mut DataHub) -> errs::Result<()>,
    {
        let index = self.index;
        self.index = index + 1;

        match self.err {
            RunnerErrAt::Run { ref mut errors } => {
                if errors.is_empty() {
                    if let Err(err) = logic_fn(self.hub) {
                        errors.push(ErrEntry {
                            index,
                            name: format!("Runner#run_or_block(logic-{})", index).into(),
                            err,
                        });
                        self.err = RunnerErrAt::Block {
                            errors: mem::take(errors),
                        };
                    }
                }
                self
            }
            _ => self,
        }
    }

    pub fn end(self) -> errs::Result<()> {
        if !self.nested {
            self.hub.end();
        }

        match self.err {
            RunnerErrAt::Start { err } => Err(err),
            RunnerErrAt::Run { errors } => {
                if errors.is_empty() {
                    Ok(())
                } else {
                    Err(errs::Err::new(DataHubError::FailToRunLogics { errors }))
                }
            }
            RunnerErrAt::Block { errors } => {
                Err(errs::Err::new(DataHubError::FailToRunLogics { errors }))
            }
        }
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod tests_of_data_hub {
    use super::*;
    use crate::{AsyncGroup, DataConnError, DataSrcError, TxnFailureReport};
    use std::sync::Mutex;

    #[derive(Clone, Copy, PartialEq)]
    enum Failure {
        None,
        FailToPreCommit,
        FailToCommit,
        FailToPostCommit,
        FailToRollback,
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
        fn is_committed(&self) -> bool {
            false
        }
        fn post_commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            if self.failure == Failure::FailToPostCommit {
                self.logger
                    .lock()
                    .unwrap()
                    .push(format!("MyDataConn::post_commit {} failed", self.id));
                Err(errs::Err::new("post commit error"))
            } else {
                self.logger
                    .lock()
                    .unwrap()
                    .push(format!("MyDataConn::post_commit {}", self.id));
                Ok(())
            }
        }
        fn rollback(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            if self.failure == Failure::FailToRollback {
                self.logger
                    .lock()
                    .unwrap()
                    .push(format!("MyDataConn::rollback {} failed", self.id));
                Err(errs::Err::new("rollback error"))
            } else {
                self.logger
                    .lock()
                    .unwrap()
                    .push(format!("MyDataConn::rollback {}", self.id));
                Ok(())
            }
        }
        fn on_txn_failure(&mut self, _ag: &mut AsyncGroup, _reports: &[TxnFailureReport]) {
            self.logger
                .lock()
                .unwrap()
                .push(format!("MyDataConn::on_txn_failure {}", self.id));
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

    struct AnotherDataConn {}
    impl DataConn for AnotherDataConn {
        fn pre_commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            Ok(())
        }
        fn commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            Ok(())
        }
        fn is_committed(&self) -> bool {
            false
        }
        fn post_commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            Ok(())
        }
        fn rollback(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            Ok(())
        }
        fn on_txn_failure(&mut self, _ag: &mut AsyncGroup, _reports: &[TxnFailureReport]) {}
        fn close(&mut self) {}
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
                        assert_eq!(errors[0].index, 1);
                        assert_eq!(errors[0].name, "bar".into());
                        assert_eq!(errors[0].err.reason::<String>().unwrap(), "setup error");
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
    fn test_run_but_failed_to_run_logic() {
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
    fn test_get_data_conn_cached() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", MyDataSrc::new(1, logger.clone(), Failure::None));

            let logger_clone = logger.clone();

            if let Err(e) = hub.run(move |data| {
                logger_clone
                    .lock()
                    .unwrap()
                    .push("execute logic".to_string());
                let _conn1 = data.get_data_conn::<MyDataConn>("foo")?;
                let _conn1 = data.get_data_conn::<MyDataConn>("foo")?;
                Ok(())
            }) {
                panic!("{:?}", e);
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "MyDataSrc::new 1",
                "MyDataSrc::setup 1",
                "execute logic",
                "MyDataSrc::create_data_conn 1",
                "MyDataConn::new 1",
                "MyDataConn::close 1",
                "MyDataConn::drop 1",
                "MyDataSrc::close 1",
                "MyDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_get_data_conn_and_no_data_src_to_create_data_conn() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            let logger_clone = logger.clone();

            if let Err(e) = hub.run(move |data| {
                logger_clone
                    .lock()
                    .unwrap()
                    .push("execute logic".to_string());
                let _conn1 = data.get_data_conn::<MyDataConn>("foo")?;
                Ok(())
            }) {
                match e.reason::<DataHubError>() {
                    Ok(DataHubError::NoDataSrcToCreateDataConn {
                        name,
                        data_conn_type,
                    }) => {
                        assert_eq!(name.as_ref(), "foo");
                        assert_eq!(
                            data_conn_type,
                            &"sabi::data_hub::tests_of_data_hub::MyDataConn"
                        );
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
                MyDataSrc::new(1, logger.clone(), Failure::FailToCreateDataConn),
            );

            let logger_clone = logger.clone();

            if let Err(e) = hub.run(move |data| {
                logger_clone
                    .lock()
                    .unwrap()
                    .push("execute logic".to_string());
                let _conn1 = data.get_data_conn::<MyDataConn>("foo")?;
                Ok(())
            }) {
                match e.reason::<DataSrcError>() {
                    Ok(DataSrcError::FailToCreateDataConn {
                        name,
                        data_conn_type,
                    }) => {
                        assert_eq!(name.as_ref(), "foo");
                        assert_eq!(
                            data_conn_type,
                            &"sabi::data_hub::tests_of_data_hub::MyDataConn"
                        );
                    }
                    _ => panic!(),
                }
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "MyDataSrc::new 1",
                "MyDataSrc::setup 1",
                "execute logic",
                "MyDataSrc::create_data_conn 1 failed",
                "MyDataSrc::close 1",
                "MyDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_get_data_conn_and_failed_to_cast_data_conn() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", MyDataSrc::new(1, logger.clone(), Failure::None));

            let logger_clone = logger.clone();

            if let Err(e) = hub.run(move |data| {
                logger_clone
                    .lock()
                    .unwrap()
                    .push("execute logic".to_string());
                if let Err(e) = data.get_data_conn::<AnotherDataConn>("foo") {
                    match e.reason::<DataSrcError>() {
                        Ok(DataSrcError::FailToCastDataConn { name, target_type }) => {
                            assert_eq!(name.as_ref(), "foo");
                            assert_eq!(
                                target_type,
                                &"sabi::data_hub::tests_of_data_hub::AnotherDataConn"
                            );
                        }
                        _ => panic!("{e:?}"),
                    }
                } else {
                    panic!();
                }

                let _conn1 = data.get_data_conn::<MyDataConn>("foo")?;

                if let Err(e) = data.get_data_conn::<AnotherDataConn>("foo") {
                    match e.reason::<DataConnError>() {
                        Ok(DataConnError::FailToCastDataConn { name, target_type }) => {
                            assert_eq!(name.as_ref(), "foo");
                            assert_eq!(
                                target_type,
                                &"sabi::data_hub::tests_of_data_hub::AnotherDataConn"
                            );
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
                        assert_eq!(
                            target_type,
                            &"sabi::data_hub::tests_of_data_hub::AnotherDataConn"
                        );
                    }
                    _ => panic!(),
                }
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "MyDataSrc::new 1",
                "MyDataSrc::setup 1",
                "execute logic",
                "MyDataSrc::create_data_conn 1",
                "MyDataConn::new 1",
                "MyDataConn::close 1",
                "MyDataConn::drop 1",
                "MyDataSrc::close 1",
                "MyDataSrc::drop 1",
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

    #[test]
    fn test_runner_and_ok() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", MyDataSrc::new(1, logger.clone(), Failure::None));
            hub.uses("bar", MyDataSrc::new(2, logger.clone(), Failure::None));

            let logger_clone_0 = logger.clone();
            let logger_clone_1 = logger.clone();
            let logger_clone_2 = logger.clone();

            let result = hub
                .start()
                .run_or_block(move |_data| {
                    logger_clone_0
                        .lock()
                        .unwrap()
                        .push("execute logic-0".to_string());
                    Ok(())
                })
                .run(move |_data| {
                    logger_clone_1
                        .lock()
                        .unwrap()
                        .push("execute logic-1".to_string());
                    Ok(())
                })
                .run_force(move |_data| {
                    logger_clone_2
                        .lock()
                        .unwrap()
                        .push("execute logic-2".to_string());
                    Ok(())
                })
                .end();

            assert!(result.is_ok());
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "MyDataSrc::new 1",
                "MyDataSrc::new 2",
                "MyDataSrc::setup 1",
                "MyDataSrc::setup 2",
                "execute logic-0",
                "execute logic-1",
                "execute logic-2",
                "MyDataSrc::close 2",
                "MyDataSrc::drop 2",
                "MyDataSrc::close 1",
                "MyDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_runner_and_fail_to_start() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", MyDataSrc::new(1, logger.clone(), Failure::None));
            hub.uses(
                "bar",
                MyDataSrc::new(2, logger.clone(), Failure::FailToSetup),
            );

            let logger_clone_0 = logger.clone();
            let logger_clone_1 = logger.clone();
            let logger_clone_2 = logger.clone();

            let result = hub
                .start()
                .run(move |_data| {
                    logger_clone_0
                        .lock()
                        .unwrap()
                        .push("execute logic-0".to_string());
                    Ok(())
                })
                .run_force(move |_data| {
                    logger_clone_1
                        .lock()
                        .unwrap()
                        .push("execute logic-1".to_string());
                    Ok(())
                })
                .run_or_block(move |_data| {
                    logger_clone_2
                        .lock()
                        .unwrap()
                        .push("execute logic-2".to_string());
                    Ok(())
                })
                .end();

            if let Err(err) = result {
                match err.reason::<DataHubError>() {
                    Ok(DataHubError::FailToSetupLocalDataSrcs { errors }) => {
                        assert_eq!(errors.len(), 1);
                        assert_eq!(errors[0].index, 1);
                        assert_eq!(errors[0].name, "bar".into());
                        assert_eq!(errors[0].err.reason::<String>().unwrap(), "setup error");
                    }
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
                "MyDataSrc::setup 2 failed",
                "MyDataSrc::close 1",
                "MyDataSrc::drop 2",
                "MyDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_runner_and_fail_to_run_logics_then_skop_run_and_run_or_block_but_run_force_runs() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", MyDataSrc::new(1, logger.clone(), Failure::None));
            hub.uses("bar", MyDataSrc::new(2, logger.clone(), Failure::None));

            let logger_clone_0 = logger.clone();
            let logger_clone_1 = logger.clone();
            let logger_clone_2 = logger.clone();
            let logger_clone_3 = logger.clone();

            let result = hub
                .start()
                .run(move |_data| {
                    logger_clone_0
                        .lock()
                        .unwrap()
                        .push("execute logic-0".to_string());
                    Err(errs::Err::new("logic-0 failed"))
                })
                .run(move |_data| {
                    logger_clone_1
                        .lock()
                        .unwrap()
                        .push("execute logic-1".to_string());
                    Ok(())
                })
                .run_or_block(move |_data| {
                    logger_clone_2
                        .lock()
                        .unwrap()
                        .push("execute logic-2".to_string());
                    Ok(())
                })
                .run_force(move |_data| {
                    logger_clone_3
                        .lock()
                        .unwrap()
                        .push("execute logic-3".to_string());
                    Ok(())
                })
                .end();

            if let Err(err) = result {
                match err.reason::<DataHubError>() {
                    Ok(DataHubError::FailToRunLogics { errors }) => {
                        assert_eq!(errors.len(), 1);
                        assert_eq!(errors[0].index, 0);
                        assert_eq!(errors[0].name, "Runner#run(logic-0)".into());
                        assert_eq!(errors[0].err.reason::<&str>().unwrap(), &"logic-0 failed");
                    }
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
                "execute logic-0",
                "execute logic-3",
                "MyDataSrc::close 2",
                "MyDataSrc::drop 2",
                "MyDataSrc::close 1",
                "MyDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_runner_and_fail_to_run_or_block_then_skip_run() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", MyDataSrc::new(1, logger.clone(), Failure::None));
            hub.uses("bar", MyDataSrc::new(2, logger.clone(), Failure::None));

            let logger_clone_0 = logger.clone();
            let logger_clone_1 = logger.clone();
            let logger_clone_2 = logger.clone();
            let logger_clone_3 = logger.clone();

            let result = hub
                .start()
                .run_or_block(move |_data| {
                    logger_clone_0
                        .lock()
                        .unwrap()
                        .push("execute logic-0".to_string());
                    Err(errs::Err::new("logic-0 failed"))
                })
                .run(move |_data| {
                    logger_clone_1
                        .lock()
                        .unwrap()
                        .push("execute logic-1".to_string());
                    Ok(())
                })
                .run_force(move |_data| {
                    logger_clone_2
                        .lock()
                        .unwrap()
                        .push("execute logic-2".to_string());
                    Ok(())
                })
                .run_or_block(move |_data| {
                    logger_clone_3
                        .lock()
                        .unwrap()
                        .push("execute logic-3".to_string());
                    Ok(())
                })
                .end();

            if let Err(err) = result {
                match err.reason::<DataHubError>() {
                    Ok(DataHubError::FailToRunLogics { errors }) => {
                        assert_eq!(errors.len(), 1);
                        assert_eq!(errors[0].index, 0);
                        assert_eq!(errors[0].name, "Runner#run_or_block(logic-0)".into());
                        assert_eq!(errors[0].err.reason::<&str>().unwrap(), &"logic-0 failed");
                    }
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
                "execute logic-0",
                "MyDataSrc::close 2",
                "MyDataSrc::drop 2",
                "MyDataSrc::close 1",
                "MyDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_runner_and_fail_to_run_force_then_skip_run_but_run_force_runs() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            let mut hub = DataHub::new();

            hub.uses("foo", MyDataSrc::new(1, logger.clone(), Failure::None));
            hub.uses("bar", MyDataSrc::new(2, logger.clone(), Failure::None));

            let logger_clone_0 = logger.clone();
            let logger_clone_1 = logger.clone();
            let logger_clone_2 = logger.clone();
            let logger_clone_3 = logger.clone();

            let result = hub
                .start()
                .run_force(move |_data| {
                    logger_clone_0
                        .lock()
                        .unwrap()
                        .push("execute logic-0".to_string());
                    Err(errs::Err::new("logic-0 failed"))
                })
                .run(move |_data| {
                    logger_clone_1
                        .lock()
                        .unwrap()
                        .push("execute logic-1".to_string());
                    Ok(())
                })
                .run_force(move |_data| {
                    logger_clone_2
                        .lock()
                        .unwrap()
                        .push("execute logic-2".to_string());
                    Ok(())
                })
                .run_or_block(move |_data| {
                    logger_clone_3
                        .lock()
                        .unwrap()
                        .push("execute logic-3".to_string());
                    Ok(())
                })
                .end();

            if let Err(err) = result {
                match err.reason::<DataHubError>() {
                    Ok(DataHubError::FailToRunLogics { errors }) => {
                        assert_eq!(errors.len(), 1);
                        assert_eq!(errors[0].index, 0);
                        assert_eq!(errors[0].name, "Runner#run_force(logic-0)".into());
                        assert_eq!(errors[0].err.reason::<&str>().unwrap(), &"logic-0 failed");
                    }
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
                "execute logic-0",
                "execute logic-2",
                "MyDataSrc::close 2",
                "MyDataSrc::drop 2",
                "MyDataSrc::close 1",
                "MyDataSrc::drop 1",
            ]
        );
    }
}

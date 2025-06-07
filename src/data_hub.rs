// Copyright (C) 2024-2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use std::any;
use std::collections::HashMap;
use std::sync;

use errs::Err;
use hashbrown;

use crate::data_conn::DataConnList;
use crate::data_src::DataSrcList;
use crate::{AsyncGroup, DataConn, DataConnContainer, DataSrc, DataSrcContainer};

static mut GLOBAL_DATA_SRC_LIST: DataSrcList = DataSrcList::new(false);

#[cfg(not(test))]
static GLOBAL_DATA_SRCS_FIXED: sync::OnceLock<()> = sync::OnceLock::new();
#[cfg(test)]
static GLOBAL_DATA_SRCS_FIXED: sync::atomic::AtomicBool = sync::atomic::AtomicBool::new(false);

/// An enum type representing the reasons for errors that can occur within `DataHub` operations.
#[derive(Debug)]
pub enum DataHubError {
    /// Indicates a failure during the setup process of one or more global data sources.
    /// Contains a map of data source names to their corresponding errors.
    FailToSetupGlobalDataSrcs {
        /// The map contains errors that occurred in each `DataSrc` object.
        errors: HashMap<String, Err>,
    },

    /// Indicates a failure during the setup process of one or more session-local data sources.
    /// Contains a map of data source names to their corresponding errors.
    FailToSetupLocalDataSrcs {
        /// The map contains errors that occurred in each `DataSrc` object.
        errors: HashMap<String, Err>,
    },

    /// Indicates a failure during the commit process of one or more `DataConn` instances
    /// involved in a transaction. Contains a map of data connection names to their errors.
    FailToCommitDataConn {
        /// The map contains errors that occurred in each `DataConn` object.
        errors: HashMap<String, Err>,
    },

    /// Indicates that no `DataSrc` was found to create a `DataConn` for the specified name
    /// and type.
    NoDataSrcToCreateDataConn {
        /// The name of the data source that could not be found.
        name: String,

        /// The type name of the `DataConn` that was requested.
        data_conn_type: &'static str,
    },

    /// Indicates a failure to cast a retrieved `DataConn` to the expected type.
    FailToCastDataConn {
        /// The name of the data connection that failed to cast.
        name: String,

        /// The type name to which the `DataConn` attempted to cast.
        cast_to_type: &'static str,
    },
}

/// Registers a global data source that can be used throughout the application.
///
/// This function associates a given `DataSrc` implementation with a unique name.
/// This name will later be used to retrieve session-specific `DataConn` instances
/// from this data source.
///
/// Global data sources are set up once via the `setup` function and are available
/// to all `DataHub` instances.
///
/// # Parameters
///
/// * `name`: The unique name for the data source.
/// * `ds`: The `DataSrc` instance to register.
pub fn uses<S, C>(name: &str, ds: S)
where
    S: DataSrc<C>,
    C: DataConn + 'static,
{
    #[cfg(not(test))]
    let fixed = GLOBAL_DATA_SRCS_FIXED.get().is_some();
    #[cfg(test)]
    let fixed = GLOBAL_DATA_SRCS_FIXED.load(sync::atomic::Ordering::Relaxed);

    if !fixed {
        #[allow(static_mut_refs)]
        unsafe {
            GLOBAL_DATA_SRC_LIST.add_data_src(name.to_string(), ds);
        }
    }
}

/// Executes the setup process for all globally registered data sources.
///
/// This setup typically involves tasks such as creating connection pools,
/// opening global connections, or performing initial configurations necessary
/// for creating session-specific connections. The setup can run synchronously
/// or asynchronously using an `AsyncGroup` if operations are time-consuming.
///
/// If any data source fails to set up, this function returns an `Err` with
/// `DataHubError::FailToSetupGlobalDataSrcs`, containing a map of the names
/// of the failed data sources and their corresponding `Err` objects. In such a case,
/// all global data sources that were successfully set up are also closed and dropped.
///
/// # Returns
///
/// * `Result<(), Err>`: `Ok(())` if all global data sources are set up successfully,
///   or an `Err` if any setup fails.
pub fn setup() -> Result<(), Err> {
    #[cfg(not(test))]
    let ok = GLOBAL_DATA_SRCS_FIXED.set(()).is_ok();
    #[cfg(test)]
    let ok = GLOBAL_DATA_SRCS_FIXED
        .compare_exchange(
            false,
            true,
            sync::atomic::Ordering::Relaxed,
            sync::atomic::Ordering::Relaxed,
        )
        .is_ok();

    if ok {
        #[allow(static_mut_refs)]
        unsafe {
            let err_map = GLOBAL_DATA_SRC_LIST.setup_data_srcs();
            if err_map.len() > 0 {
                GLOBAL_DATA_SRC_LIST.close_and_drop_data_srcs();
                return Err(Err::new(DataHubError::FailToSetupGlobalDataSrcs {
                    errors: err_map,
                }));
            }
        }
    }

    Ok(())
}

/// Executes the close and drop process for all globally registered data sources.
///
/// This function cleans up all resources associated with the global data sources
/// that were registered via `uses` and set up by `setup`.
pub fn shutdown() {
    #[allow(static_mut_refs)]
    unsafe {
        GLOBAL_DATA_SRC_LIST.close_and_drop_data_srcs();
    }
}

/// Provides an object that, when it goes out of scope,
/// automatically triggers the `shutdown` process for global data sources.
///
/// This leverages Rust's ownership system to ensure global data source cleanup
/// happens reliably at the end of a scope, typically at the end of the application's
/// main function or a test.
///
/// # Returns
///
/// * `impl any::Any`: An opaque object that implements `Drop` and will call `shutdown()`
///   when dropped.
#[must_use = "call `shutdown_later()` and bind its result to a variable to defer cleanup until the variable goes out of scope"]
pub fn shutdown_later() -> impl any::Any {
    AutoShutdown {}
}

struct AutoShutdown {}

impl Drop for AutoShutdown {
    fn drop(&mut self) {
        shutdown();
    }
}

/// The struct that acts as a central hub for data input/output operations, integrating
/// multiple *Data* traits (which are passed to business logic functions as their arguments) with
/// `DataAcc` traits (which implement default data I/O methods for external services).
///
/// It facilitates data access by providing `DataConn` objects, created from
/// both global data sources (registered via the global `uses` function) and
/// session-local data sources (registered via `DataHub::uses` method).
///
/// The `DataHub` is capable of performing aggregated transactional operations
/// on all `DataConn` objects created from its registered `DataSrc` instances.
/// The `run` method executes logic without transaction control, while the `txn`
/// method executes logic within a controlled transaction.
pub struct DataHub {
    local_data_src_list: DataSrcList,
    data_src_map: hashbrown::HashMap<String, *mut DataSrcContainer>,
    data_conn_list: DataConnList,
    data_conn_map: hashbrown::HashMap<String, *mut DataConnContainer>,
    fixed: bool,
}

impl DataHub {
    /// Creates a new `DataHub` instance.
    ///
    /// Upon creation, it attempts to "fix" the global data sources (making them immutable
    /// for further registration) and copies references to already set-up global data
    /// sources into its internal map for quick access.
    pub fn new() -> Self {
        #[cfg(not(test))]
        let _ = GLOBAL_DATA_SRCS_FIXED.set(());
        #[cfg(test)]
        GLOBAL_DATA_SRCS_FIXED.store(true, sync::atomic::Ordering::Relaxed);

        let mut data_src_map = hashbrown::HashMap::new();

        #[allow(static_mut_refs)]
        unsafe {
            GLOBAL_DATA_SRC_LIST.copy_container_ptrs_did_setup_into(&mut data_src_map);
        }

        Self {
            local_data_src_list: DataSrcList::new(true),
            data_src_map,
            data_conn_list: DataConnList::new(),
            data_conn_map: hashbrown::HashMap::new(),
            fixed: false,
        }
    }

    /// Registers a session-local data source with this `DataHub` instance.
    ///
    /// This method is similar to the global `uses` function but registers a data source
    /// that is local to this specific `DataHub` session. Once the `DataHub`'s state is
    /// "fixed" (after `begin` is called internally by `run` or `txn`), further calls
    /// to `uses` are ignored. However, after `run` or `txn` completes, the `DataHub`'s
    /// `fixed` state is reset, allowing for new data sources to be registered or removed
    /// via `disuses` in subsequent operations.
    ///
    /// # Parameters
    ///
    /// * `name`: The unique name for the local data source.
    /// * `ds`: The `DataSrc` instance to register.
    pub fn uses<S, C>(&mut self, name: &str, ds: S)
    where
        S: DataSrc<C>,
        C: DataConn + 'static,
    {
        if self.fixed {
            return;
        }

        self.local_data_src_list.add_data_src(name.to_string(), ds);
    }

    /// Unregisters and drops a session-local data source by its name.
    ///
    /// This method removes a data source that was previously registered via `DataHub::uses`.
    /// This operation is ignored if the `DataHub`'s state is already "fixed".
    ///
    /// # Parameters
    ///
    /// * `name`: The name of the local data source to unregister.
    pub fn disuses(&mut self, name: &str) {
        if self.fixed {
            return;
        }

        let _ = self
            .data_src_map
            .extract_if(|nm, p| unsafe { (*(*p)).local } && nm == name);
        self.local_data_src_list
            .remove_and_drop_local_container_ptr_did_setup_by_name(name);
        self.local_data_src_list
            .remove_and_drop_local_container_ptr_not_setup_by_name(name);
    }

    fn begin(&mut self) -> Result<(), Err> {
        self.fixed = true;

        let err_map = self.local_data_src_list.setup_data_srcs();

        self.local_data_src_list
            .copy_container_ptrs_did_setup_into(&mut self.data_src_map);

        if err_map.len() > 0 {
            return Err(Err::new(DataHubError::FailToSetupLocalDataSrcs {
                errors: err_map,
            }));
        }

        Ok(())
    }

    fn commit(&mut self) -> Result<(), Err> {
        let mut err_map = HashMap::new();
        let mut ag = AsyncGroup::new();

        let mut ptr = self.data_conn_list.head();
        while !ptr.is_null() {
            let commit_fn = unsafe { (*ptr).commit_fn };
            let name = unsafe { &(*ptr).name };
            let next = unsafe { (*ptr).next };

            ag.name = name;

            if let Err(err) = commit_fn(ptr, &mut ag) {
                err_map.insert(name.to_string(), err);
                break;
            }

            ptr = next;
        }

        ag.join_and_put_errors_into(&mut err_map);

        if err_map.is_empty() {
            return Ok(());
        }

        Err(Err::new(DataHubError::FailToCommitDataConn {
            errors: err_map,
        }))
    }

    fn rollback(&mut self) {
        let mut ag = AsyncGroup::new();

        let mut ptr = self.data_conn_list.head();
        while !ptr.is_null() {
            let should_force_back_fn = unsafe { (*ptr).should_force_back_fn };
            let force_back_fn = unsafe { (*ptr).force_back_fn };
            let rollback_fn = unsafe { (*ptr).rollback_fn };
            let name = unsafe { &(*ptr).name };
            let next = unsafe { (*ptr).next };

            ag.name = name;

            if should_force_back_fn(ptr) {
                force_back_fn(ptr, &mut ag);
            } else {
                rollback_fn(ptr, &mut ag);
            }

            ptr = next;
        }

        ag.join_and_ignore_errors();
    }

    fn post_commit(&mut self) {
        let mut ag = AsyncGroup::new();

        let mut ptr = self.data_conn_list.head();
        while !ptr.is_null() {
            let post_commit_fn = unsafe { (*ptr).post_commit_fn };
            let name = unsafe { &(*ptr).name };
            let next = unsafe { (*ptr).next };

            ag.name = name;

            post_commit_fn(ptr, &mut ag);

            ptr = next;
        }

        ag.join_and_ignore_errors();
    }

    fn end(&mut self) {
        self.data_conn_map.clear();
        self.data_conn_list.close_and_drop_data_conns();
        self.fixed = false;
    }

    /// Executes a given logic function without transaction control.
    ///
    /// This method sets up local data sources, runs the provided closure,
    /// and then cleans up the `DataHub`'s session resources. It does not
    /// perform commit or rollback operations.
    ///
    /// # Parameters
    ///
    /// * `logic_fn`: A closure that encapsulates the business logic to be executed.
    ///   It takes a mutable reference to `DataHub` as an argument.
    ///
    /// # Returns
    ///
    /// * `Result<(), Err>`: The result of the logic function's execution,
    ///   or an error if the `DataHub`'s setup fails.
    pub fn run<F>(&mut self, logic_fn: F) -> Result<(), Err>
    where
        F: FnOnce(&mut DataHub) -> Result<(), Err>,
    {
        let r = self.begin();
        if r.is_err() {
            self.end();
            return r;
        }

        let r = logic_fn(self);
        self.end();
        r
    }

    /// Executes a given logic function within a transaction.
    ///
    /// This method first sets up local data sources, then runs the provided closure.
    /// If the closure returns `Ok`, it attempts to commit all changes. If the commit fails,
    /// or if the logic function itself returns an `Err`, a rollback operation
    /// is performed. On successful commit, `post_commit` actions are executed.
    /// Finally, it cleans up the `DataHub`'s session resources.
    ///
    /// # Parameters
    ///
    /// * `logic_fn`: A closure that encapsulates the business logic to be executed.
    ///   It takes a mutable reference to `DataHub` as an argument.
    ///
    /// # Returns
    ///
    /// * `Result<(), Err>`: The final result of the transaction (success or failure of logic/commit),
    ///   or an error if the `DataHub`'s setup fails.
    pub fn txn<F>(&mut self, logic_fn: F) -> Result<(), Err>
    where
        F: FnOnce(&mut DataHub) -> Result<(), Err>,
    {
        let r = self.begin();
        if r.is_err() {
            self.end();
            return r;
        }

        let mut r = logic_fn(self);

        if r.is_ok() {
            r = self.commit();
        }

        if r.is_err() {
            self.rollback();
        } else {
            self.post_commit();
        }

        self.end();
        r
    }

    /// Retrieves a mutable reference to a `DataConn` object by name, creating it if necessary.
    ///
    /// This is the core method used by `DataAcc` implementations to obtain connections
    /// to external data services. It first checks if a `DataConn` with the given name
    /// already exists in the `DataHub`'s session. If not, it attempts to find a
    /// corresponding `DataSrc` and create a new `DataConn` from it.
    ///
    /// # Type Parameters
    ///
    /// * `C`: The concrete type of `DataConn` expected.
    ///
    /// # Parameters
    ///
    /// * `name`: The name of the data source/connection to retrieve.
    ///
    /// # Returns
    ///
    /// * `Result<&mut C, Err>`: A mutable reference to the `DataConn` instance if successful,
    ///   or an `Err` if the data source is not found, or if the retrieved/created `DataConn`
    ///   cannot be cast to the specified type `C`.
    pub fn get_data_conn<C>(&mut self, name: &str) -> Result<&mut C, Err>
    where
        C: DataConn + 'static,
    {
        match self.data_conn_map.get(name) {
            Some(conn_ptr) => {
                let type_id = any::TypeId::of::<C>();
                let is_fn = unsafe { (*(*conn_ptr)).is_fn };
                if !is_fn(type_id) {
                    return Err(Err::new(DataHubError::FailToCastDataConn {
                        name: name.to_string(),
                        cast_to_type: any::type_name::<C>(),
                    }));
                }
                let typed_ptr = (*conn_ptr) as *mut DataConnContainer<C>;
                return Ok(unsafe { &mut ((*typed_ptr).data_conn) });
            }
            None => match self.data_src_map.get(name) {
                Some(src_ptr) => {
                    let type_id = any::TypeId::of::<C>();
                    let is_data_conn_fn = unsafe { (*(*src_ptr)).is_data_conn_fn };
                    if !is_data_conn_fn(type_id) {
                        return Err(Err::new(DataHubError::FailToCastDataConn {
                            name: name.to_string(),
                            cast_to_type: any::type_name::<C>(),
                        }));
                    }

                    let create_data_conn_fn = unsafe { (*(*src_ptr)).create_data_conn_fn };
                    let boxed = create_data_conn_fn(*src_ptr)?;
                    let raw_ptr = Box::into_raw(boxed);
                    let conn_ptr = raw_ptr.cast::<DataConnContainer>();

                    self.data_conn_list.append_container_ptr(conn_ptr);
                    self.data_conn_map.insert(name.to_string(), conn_ptr);

                    let typed_ptr = raw_ptr.cast::<DataConnContainer<C>>();
                    return Ok(unsafe { &mut (*typed_ptr).data_conn });
                }
                None => {
                    return Err(Err::new(DataHubError::NoDataSrcToCreateDataConn {
                        name: name.to_string(),
                        data_conn_type: any::type_name::<C>(),
                    }));
                }
            },
        }
    }
}

impl Drop for DataHub {
    fn drop(&mut self) {
        self.data_conn_map.clear();
        self.data_conn_list.close_and_drop_data_conns();

        self.data_src_map.clear();
        self.local_data_src_list.close_and_drop_data_srcs();
    }
}

#[cfg(test)]
pub(crate) fn clear_global_data_srcs_fixed() {
    GLOBAL_DATA_SRCS_FIXED.store(false, sync::atomic::Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) static TEST_SEQ: sync::LazyLock<sync::Mutex<()>> =
    sync::LazyLock::new(|| sync::Mutex::new(()));

#[cfg(test)]
mod tests_data_hub {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::time;

    struct SyncDataSrc {
        id: i8,
        will_fail_ds: bool,
        will_fail_dc: bool,
        logger: Arc<Mutex<Vec<String>>>,
    }

    impl SyncDataSrc {
        fn new(
            id: i8,
            logger: Arc<Mutex<Vec<String>>>,
            will_fail_ds: bool,
            will_fail_dc: bool,
        ) -> Self {
            Self {
                id,
                will_fail_ds,
                will_fail_dc,
                logger,
            }
        }
    }

    impl Drop for SyncDataSrc {
        fn drop(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("SyncDataSrc {} dropped", self.id));
        }
    }

    impl DataSrc<SyncDataConn> for SyncDataSrc {
        fn setup(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
            let mut logger = self.logger.lock().unwrap();
            if self.will_fail_ds {
                logger.push(format!("SyncDataSrc {} failed to setup", self.id));
                return Err(Err::new("XXX".to_string()));
            }
            logger.push(format!("SyncDataSrc {} setupped", self.id));
            Ok(())
        }

        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("SyncDataSrc {} closed", self.id));
        }

        fn create_data_conn(&mut self) -> Result<Box<SyncDataConn>, Err> {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("SyncDataSrc {} created DataConn", self.id));
            let conn = SyncDataConn::new(self.id, self.logger.clone(), self.will_fail_dc.clone());
            Ok(Box::new(conn))
        }
    }

    struct AsyncDataSrc {
        id: i8,
        will_fail_ds: bool,
        will_fail_dc: bool,
        logger: Arc<Mutex<Vec<String>>>,
    }

    impl AsyncDataSrc {
        fn new(
            id: i8,
            logger: Arc<Mutex<Vec<String>>>,
            will_fail_ds: bool,
            will_fail_dc: bool,
        ) -> Self {
            Self {
                id,
                will_fail_ds,
                will_fail_dc,
                logger,
            }
        }
    }

    impl Drop for AsyncDataSrc {
        fn drop(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("AsyncDataSrc {} dropped", self.id));
        }
    }

    impl DataSrc<AsyncDataConn> for AsyncDataSrc {
        fn setup(&mut self, ag: &mut AsyncGroup) -> Result<(), Err> {
            let will_fail_ds = self.will_fail_ds;
            let logger = self.logger.clone();
            let id = self.id;

            ag.add(async move || {
                // The `.await` must be executed outside the Mutex lock.
                let _ = time::sleep(time::Duration::from_millis(100)).await;

                if will_fail_ds {
                    logger
                        .lock()
                        .unwrap()
                        .push(format!("AsyncDataSrc {} failed to setup", id));
                    return Err(Err::new("YYY".to_string()));
                }

                logger
                    .lock()
                    .unwrap()
                    .push(format!("AsyncDataSrc {} setupped", id));
                Ok(())
            });
            Ok(())
        }

        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("AsyncDataSrc {} closed", self.id));
        }

        fn create_data_conn(&mut self) -> Result<Box<AsyncDataConn>, Err> {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("AsyncDataSrc {} created DataConn", self.id));
            let conn = AsyncDataConn::new(self.id, self.logger.clone(), self.will_fail_dc);
            Ok(Box::new(conn))
        }
    }

    struct SyncDataConn {
        id: i8,
        committed: bool,
        will_fail_dc: bool,
        logger: Arc<Mutex<Vec<String>>>,
    }

    impl SyncDataConn {
        fn new(id: i8, logger: Arc<Mutex<Vec<String>>>, will_fail_dc: bool) -> Self {
            Self {
                id,
                committed: false,
                will_fail_dc,
                logger,
            }
        }
    }

    impl Drop for SyncDataConn {
        fn drop(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("SyncDataConn {} dropped", self.id));
        }
    }

    impl DataConn for SyncDataConn {
        fn commit(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
            let mut logger = self.logger.lock().unwrap();
            if self.will_fail_dc {
                logger.push(format!("SyncDataConn {} failed to commit", self.id));
                return Err(Err::new("ZZZ".to_string()));
            }
            self.committed = true;
            logger.push(format!("SyncDataConn {} committed", self.id));
            Ok(())
        }

        fn post_commit(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("SyncDataConn {} post committed", self.id));
        }

        fn should_force_back(&self) -> bool {
            self.committed
        }

        fn rollback(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("SyncDataConn {} rollbacked", self.id));
        }

        fn force_back(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("SyncDataConn {} forced back", self.id));
        }

        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("SyncDataConn {} closed", self.id));
        }
    }

    struct AsyncDataConn {
        id: i8,
        committed: bool,
        will_fail_dc: bool,
        logger: Arc<Mutex<Vec<String>>>,
    }

    impl AsyncDataConn {
        fn new(id: i8, logger: Arc<Mutex<Vec<String>>>, will_fail_dc: bool) -> Self {
            Self {
                id,
                committed: false,
                will_fail_dc,
                logger,
            }
        }
    }

    impl Drop for AsyncDataConn {
        fn drop(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("AsyncDataConn {} dropped", self.id));
        }
    }

    impl DataConn for AsyncDataConn {
        fn commit(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
            let mut logger = self.logger.lock().unwrap();
            if self.will_fail_dc {
                logger.push(format!("AsyncDataConn {} failed to commit", self.id));
                return Err(Err::new("VVV".to_string()));
            }
            self.committed = true;
            logger.push(format!("AsyncDataConn {} committed", self.id));
            Ok(())
        }

        fn post_commit(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("AsyncDataConn {} post committed", self.id));
        }

        fn should_force_back(&self) -> bool {
            self.committed
        }

        fn rollback(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("AsyncDataConn {} rollbacked", self.id));
        }

        fn force_back(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("AsyncDataConn {} forced back", self.id));
        }

        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("AsyncDataConn {} closed", self.id));
        }
    }

    mod tests_of_global_functions {
        use super::*;

        #[test]
        fn test_uses_and_shutdown() {
            let _unsed = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            #[allow(static_mut_refs)]
            unsafe {
                let ptr = GLOBAL_DATA_SRC_LIST.not_setup_head();
                assert!(ptr.is_null());

                let ptr = GLOBAL_DATA_SRC_LIST.did_setup_head();
                assert!(ptr.is_null());
            }

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            uses("foo", AsyncDataSrc::new(1, logger.clone(), false, false));
            uses("bar", SyncDataSrc::new(2, logger.clone(), false, false));

            #[allow(static_mut_refs)]
            unsafe {
                let mut ptr = GLOBAL_DATA_SRC_LIST.not_setup_head();
                assert!(!ptr.is_null());
                assert_eq!((*ptr).name, "foo");
                ptr = (*ptr).next;
                assert!(!ptr.is_null());
                assert_eq!((*ptr).name, "bar");
                ptr = (*ptr).next;
                assert!(ptr.is_null());

                let ptr = GLOBAL_DATA_SRC_LIST.did_setup_head();
                assert!(ptr.is_null());
            }

            shutdown();

            #[allow(static_mut_refs)]
            unsafe {
                let ptr = GLOBAL_DATA_SRC_LIST.not_setup_head();
                assert!(ptr.is_null());

                let ptr = GLOBAL_DATA_SRC_LIST.did_setup_head();
                assert!(ptr.is_null());
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec!["SyncDataSrc 2 dropped", "AsyncDataSrc 1 dropped"],
            );
        }

        #[test]
        fn test_setup_and_shutdown() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            #[allow(static_mut_refs)]
            unsafe {
                let ptr = GLOBAL_DATA_SRC_LIST.not_setup_head();
                assert!(ptr.is_null());

                let ptr = GLOBAL_DATA_SRC_LIST.did_setup_head();
                assert!(ptr.is_null());
            }

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            uses("foo", AsyncDataSrc::new(1, logger.clone(), false, false));
            uses("bar", SyncDataSrc::new(2, logger.clone(), false, false));

            #[allow(static_mut_refs)]
            unsafe {
                let mut ptr = GLOBAL_DATA_SRC_LIST.not_setup_head();
                assert!(!ptr.is_null());
                assert_eq!((*ptr).name, "foo");
                ptr = (*ptr).next;
                assert!(!ptr.is_null());
                assert_eq!((*ptr).name, "bar");
                ptr = (*ptr).next;
                assert!(ptr.is_null());

                let ptr = GLOBAL_DATA_SRC_LIST.did_setup_head();
                assert!(ptr.is_null());
            }

            let result = setup();
            assert!(result.is_ok());

            #[allow(static_mut_refs)]
            unsafe {
                let ptr = GLOBAL_DATA_SRC_LIST.not_setup_head();
                assert!(ptr.is_null());

                let mut ptr = GLOBAL_DATA_SRC_LIST.did_setup_head();
                assert!(!ptr.is_null());
                assert_eq!((*ptr).name, "foo");
                ptr = (*ptr).next;
                assert!(!ptr.is_null());
                assert_eq!((*ptr).name, "bar");
                ptr = (*ptr).next;
                assert!(ptr.is_null());
            }

            shutdown();

            #[allow(static_mut_refs)]
            unsafe {
                let ptr = GLOBAL_DATA_SRC_LIST.not_setup_head();
                assert!(ptr.is_null());

                let ptr = GLOBAL_DATA_SRC_LIST.did_setup_head();
                assert!(ptr.is_null());
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc 2 setupped",
                    "AsyncDataSrc 1 setupped",
                    "SyncDataSrc 2 closed",
                    "SyncDataSrc 2 dropped",
                    "AsyncDataSrc 1 closed",
                    "AsyncDataSrc 1 dropped",
                ],
            );
        }

        #[test]
        fn test_shutdown_later() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let _later = shutdown_later();

                #[allow(static_mut_refs)]
                unsafe {
                    let ptr = GLOBAL_DATA_SRC_LIST.not_setup_head();
                    assert!(ptr.is_null());

                    let ptr = GLOBAL_DATA_SRC_LIST.did_setup_head();
                    assert!(ptr.is_null());
                }

                uses("foo", AsyncDataSrc::new(1, logger.clone(), false, false));
                uses("bar", SyncDataSrc::new(2, logger.clone(), false, false));

                #[allow(static_mut_refs)]
                unsafe {
                    let mut ptr = GLOBAL_DATA_SRC_LIST.not_setup_head();
                    assert!(!ptr.is_null());
                    assert_eq!((*ptr).name, "foo");
                    ptr = (*ptr).next;
                    assert!(!ptr.is_null());
                    assert_eq!((*ptr).name, "bar");
                    ptr = (*ptr).next;
                    assert!(ptr.is_null());

                    let ptr = GLOBAL_DATA_SRC_LIST.did_setup_head();
                    assert!(ptr.is_null());
                }

                let result = setup();
                assert!(result.is_ok());

                #[allow(static_mut_refs)]
                unsafe {
                    let ptr = GLOBAL_DATA_SRC_LIST.not_setup_head();
                    assert!(ptr.is_null());

                    let mut ptr = GLOBAL_DATA_SRC_LIST.did_setup_head();
                    assert!(!ptr.is_null());
                    assert_eq!((*ptr).name, "foo");
                    ptr = (*ptr).next;
                    assert!(!ptr.is_null());
                    assert_eq!((*ptr).name, "bar");
                    ptr = (*ptr).next;
                    assert!(ptr.is_null());
                }
            }

            #[allow(static_mut_refs)]
            unsafe {
                let ptr = GLOBAL_DATA_SRC_LIST.not_setup_head();
                assert!(ptr.is_null());

                let ptr = GLOBAL_DATA_SRC_LIST.did_setup_head();
                assert!(ptr.is_null());
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc 2 setupped",
                    "AsyncDataSrc 1 setupped",
                    "SyncDataSrc 2 closed",
                    "SyncDataSrc 2 dropped",
                    "AsyncDataSrc 1 closed",
                    "AsyncDataSrc 1 dropped",
                ],
            );
        }

        #[test]
        fn test_fail_to_setup() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let _later = shutdown_later();

            #[allow(static_mut_refs)]
            unsafe {
                let ptr = GLOBAL_DATA_SRC_LIST.not_setup_head();
                assert!(ptr.is_null());

                let ptr = GLOBAL_DATA_SRC_LIST.did_setup_head();
                assert!(ptr.is_null());
            }

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            uses("foo", AsyncDataSrc::new(1, logger.clone(), true, false));
            uses("bar", SyncDataSrc::new(2, logger.clone(), true, false));

            #[allow(static_mut_refs)]
            unsafe {
                let mut ptr = GLOBAL_DATA_SRC_LIST.not_setup_head();
                assert!(!ptr.is_null());
                assert_eq!((*ptr).name, "foo");
                ptr = (*ptr).next;
                assert!(!ptr.is_null());
                assert_eq!((*ptr).name, "bar");
                ptr = (*ptr).next;
                assert!(ptr.is_null());

                let ptr = GLOBAL_DATA_SRC_LIST.did_setup_head();
                assert!(ptr.is_null());
            }

            match setup() {
                Ok(_) => panic!(),
                Err(err) => match err.reason::<DataHubError>() {
                    Ok(r) => match r {
                        DataHubError::FailToSetupGlobalDataSrcs { errors } => {
                            let err = errors.get("foo").unwrap();
                            match err.reason::<String>() {
                                Ok(s) => assert_eq!(s, "YYY"),
                                Err(_) => panic!(),
                            }
                            let err = errors.get("bar").unwrap();
                            match err.reason::<String>() {
                                Ok(s) => assert_eq!(s, "XXX"),
                                Err(_) => panic!(),
                            }
                        }
                        _ => panic!(),
                    },
                    Err(_) => panic!(),
                },
            }

            #[allow(static_mut_refs)]
            unsafe {
                let ptr = GLOBAL_DATA_SRC_LIST.not_setup_head();
                assert!(ptr.is_null());

                let ptr = GLOBAL_DATA_SRC_LIST.did_setup_head();
                assert!(ptr.is_null());
            }

            assert_eq!(
                logger.lock().unwrap().clone(),
                vec![
                    "SyncDataSrc 2 failed to setup",
                    "AsyncDataSrc 1 failed to setup",
                    "AsyncDataSrc 1 dropped",
                    "SyncDataSrc 2 dropped",
                ]
            );
        }

        #[test]
        fn test_cannot_add_global_data_src_after_setup() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            #[allow(static_mut_refs)]
            unsafe {
                let ptr = GLOBAL_DATA_SRC_LIST.not_setup_head();
                assert!(ptr.is_null());

                let ptr = GLOBAL_DATA_SRC_LIST.did_setup_head();
                assert!(ptr.is_null());
            }

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            uses("foo", AsyncDataSrc::new(1, logger.clone(), false, false));

            #[allow(static_mut_refs)]
            unsafe {
                let mut ptr = GLOBAL_DATA_SRC_LIST.not_setup_head();
                assert!(!ptr.is_null());
                assert_eq!((*ptr).name, "foo");
                ptr = (*ptr).next;
                assert!(ptr.is_null());

                let ptr = GLOBAL_DATA_SRC_LIST.did_setup_head();
                assert!(ptr.is_null());
            }

            let result = setup();
            assert!(result.is_ok());

            #[allow(static_mut_refs)]
            unsafe {
                let ptr = GLOBAL_DATA_SRC_LIST.not_setup_head();
                assert!(ptr.is_null());

                let mut ptr = GLOBAL_DATA_SRC_LIST.did_setup_head();
                assert!(!ptr.is_null());
                assert_eq!((*ptr).name, "foo");
                ptr = (*ptr).next;
                assert!(ptr.is_null());
            }

            uses("bar", SyncDataSrc::new(2, logger.clone(), false, false));

            #[allow(static_mut_refs)]
            unsafe {
                let ptr = GLOBAL_DATA_SRC_LIST.not_setup_head();
                assert!(ptr.is_null());

                let mut ptr = GLOBAL_DATA_SRC_LIST.did_setup_head();
                assert!(!ptr.is_null());
                assert_eq!((*ptr).name, "foo");
                ptr = (*ptr).next;
                assert!(ptr.is_null());
            }

            shutdown();

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "AsyncDataSrc 1 setupped",
                    "SyncDataSrc 2 dropped",
                    "AsyncDataSrc 1 closed",
                    "AsyncDataSrc 1 dropped",
                ]
            );
        }

        #[test]
        fn test_do_nothing_if_executing_setup_twice() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            #[allow(static_mut_refs)]
            unsafe {
                let ptr = GLOBAL_DATA_SRC_LIST.not_setup_head();
                assert!(ptr.is_null());

                let ptr = GLOBAL_DATA_SRC_LIST.did_setup_head();
                assert!(ptr.is_null());
            }

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            uses("foo", AsyncDataSrc::new(1, logger.clone(), false, false));

            #[allow(static_mut_refs)]
            unsafe {
                let mut ptr = GLOBAL_DATA_SRC_LIST.not_setup_head();
                assert!(!ptr.is_null());
                assert_eq!((*ptr).name, "foo");
                ptr = (*ptr).next;
                assert!(ptr.is_null());

                let ptr = GLOBAL_DATA_SRC_LIST.did_setup_head();
                assert!(ptr.is_null());
            }

            let result = setup();
            assert!(result.is_ok());

            #[allow(static_mut_refs)]
            unsafe {
                let ptr = GLOBAL_DATA_SRC_LIST.not_setup_head();
                assert!(ptr.is_null());

                let mut ptr = GLOBAL_DATA_SRC_LIST.did_setup_head();
                assert!(!ptr.is_null());
                assert_eq!((*ptr).name, "foo");
                ptr = (*ptr).next;
                assert!(ptr.is_null());
            }

            let result = setup();
            assert!(result.is_ok());

            #[allow(static_mut_refs)]
            unsafe {
                let ptr = GLOBAL_DATA_SRC_LIST.not_setup_head();
                assert!(ptr.is_null());

                let mut ptr = GLOBAL_DATA_SRC_LIST.did_setup_head();
                assert!(!ptr.is_null());
                assert_eq!((*ptr).name, "foo");
                ptr = (*ptr).next;
                assert!(ptr.is_null());
            }

            shutdown();

            assert_eq!(
                logger.lock().unwrap().clone(),
                vec![
                    "AsyncDataSrc 1 setupped",
                    "AsyncDataSrc 1 closed",
                    "AsyncDataSrc 1 dropped",
                ]
            );
        }
    }

    mod tests_of_data_hub_local {
        use super::*;

        #[test]
        fn test_new_and_close_with_no_global_data_srcs() {
            let _unused = TEST_SEQ.lock().unwrap();

            let _later = shutdown_later();

            let hub = DataHub::new();

            assert!(hub.local_data_src_list.not_setup_head().is_null());
            assert!(hub.local_data_src_list.did_setup_head().is_null());
            assert!(hub.data_conn_list.head().is_null());
            assert_eq!(hub.data_src_map.len(), 0);
            assert_eq!(hub.data_conn_map.len(), 0);
            assert_eq!(hub.fixed, false);
        }

        #[test]
        fn test_new_and_close_with_global_data_srcs() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            uses("foo", AsyncDataSrc::new(1, logger.clone(), false, false));
            uses("bar", SyncDataSrc::new(2, logger.clone(), false, false));

            if let Ok(_) = setup() {
                let _later = shutdown_later();

                #[allow(static_mut_refs)]
                unsafe {
                    let ptr = GLOBAL_DATA_SRC_LIST.not_setup_head();
                    assert!(ptr.is_null());

                    let mut ptr = GLOBAL_DATA_SRC_LIST.did_setup_head();
                    assert!(!ptr.is_null());
                    assert_eq!((*ptr).name, "foo");
                    ptr = (*ptr).next;
                    assert!(!ptr.is_null());
                    assert_eq!((*ptr).name, "bar");
                    ptr = (*ptr).next;
                    assert!(ptr.is_null());
                }

                let hub = DataHub::new();

                assert!(hub.local_data_src_list.not_setup_head().is_null());
                assert!(hub.local_data_src_list.did_setup_head().is_null());
                assert!(hub.data_conn_list.head().is_null());
                assert_eq!(hub.data_src_map.len(), 2);
                assert_eq!(hub.data_conn_map.len(), 0);
                assert_eq!(hub.fixed, false);

                #[allow(static_mut_refs)]
                let mut ptr = unsafe { GLOBAL_DATA_SRC_LIST.did_setup_head() };
                assert!(!ptr.is_null());
                ptr = unsafe { (*ptr).next };
                assert!(!ptr.is_null());
                ptr = unsafe { (*ptr).next };
                assert!(ptr.is_null());
                #[allow(static_mut_refs)]
                let ptr = unsafe { GLOBAL_DATA_SRC_LIST.not_setup_head() };
                assert!(ptr.is_null());
            } else {
                panic!();
            }

            #[allow(static_mut_refs)]
            let ptr = unsafe { GLOBAL_DATA_SRC_LIST.did_setup_head() };
            assert!(ptr.is_null());
            #[allow(static_mut_refs)]
            let ptr = unsafe { GLOBAL_DATA_SRC_LIST.not_setup_head() };
            assert!(ptr.is_null());

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc 2 setupped",
                    "AsyncDataSrc 1 setupped",
                    "SyncDataSrc 2 closed",
                    "SyncDataSrc 2 dropped",
                    "AsyncDataSrc 1 closed",
                    "AsyncDataSrc 1 dropped",
                ]
            );
        }

        #[test]
        fn test_uses_and_disuses() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            uses("foo", AsyncDataSrc::new(1, logger.clone(), false, false));
            uses("bar", SyncDataSrc::new(2, logger.clone(), false, false));

            if let Ok(_) = setup() {
                let _later = shutdown_later();

                let mut hub = DataHub::new();

                assert!(hub.local_data_src_list.not_setup_head().is_null());
                assert!(hub.local_data_src_list.did_setup_head().is_null());
                assert!(hub.data_conn_list.head().is_null());
                assert_eq!(hub.data_src_map.len(), 2);
                assert_eq!(hub.data_conn_map.len(), 0);
                assert_eq!(hub.fixed, false);

                hub.uses("baz", SyncDataSrc::new(3, logger.clone(), false, false));
                let mut ptr = hub.local_data_src_list.not_setup_head();
                assert!(!ptr.is_null());
                ptr = unsafe { (*ptr).next };
                assert!(ptr.is_null());
                assert!(hub.local_data_src_list.did_setup_head().is_null());
                assert!(hub.data_conn_list.head().is_null());
                assert_eq!(hub.data_src_map.len(), 2);
                assert_eq!(hub.data_conn_map.len(), 0);
                assert_eq!(hub.fixed, false);

                hub.uses("qux", AsyncDataSrc::new(4, logger.clone(), false, false));
                let mut ptr = hub.local_data_src_list.not_setup_head();
                assert!(!ptr.is_null());
                ptr = unsafe { (*ptr).next };
                assert!(!ptr.is_null());
                ptr = unsafe { (*ptr).next };
                assert!(ptr.is_null());
                assert!(hub.local_data_src_list.did_setup_head().is_null());
                assert!(hub.data_conn_list.head().is_null());
                assert_eq!(hub.data_src_map.len(), 2);
                assert_eq!(hub.data_conn_map.len(), 0);
                assert_eq!(hub.fixed, false);

                hub.disuses("foo"); // do nothing because of global
                hub.disuses("bar"); // do nothing because of global
                let mut ptr = hub.local_data_src_list.not_setup_head();
                assert!(!ptr.is_null());
                ptr = unsafe { (*ptr).next };
                assert!(!ptr.is_null());
                ptr = unsafe { (*ptr).next };
                assert!(ptr.is_null());
                assert!(hub.local_data_src_list.did_setup_head().is_null());
                assert!(hub.data_conn_list.head().is_null());
                assert_eq!(hub.data_src_map.len(), 2);
                assert_eq!(hub.data_conn_map.len(), 0);
                assert_eq!(hub.fixed, false);

                hub.disuses("baz");
                let mut ptr = hub.local_data_src_list.not_setup_head();
                assert!(!ptr.is_null());
                ptr = unsafe { (*ptr).next };
                assert!(ptr.is_null());
                assert!(hub.local_data_src_list.did_setup_head().is_null());
                assert!(hub.data_conn_list.head().is_null());
                assert_eq!(hub.data_src_map.len(), 2);
                assert_eq!(hub.data_conn_map.len(), 0);
                assert_eq!(hub.fixed, false);

                hub.disuses("qux");
                let ptr = hub.local_data_src_list.not_setup_head();
                assert!(ptr.is_null());
                assert!(hub.local_data_src_list.did_setup_head().is_null());
                assert!(hub.data_conn_list.head().is_null());
                assert_eq!(hub.data_src_map.len(), 2);
                assert_eq!(hub.data_conn_map.len(), 0);
                assert_eq!(hub.fixed, false);
            } else {
                panic!();
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc 2 setupped",
                    "AsyncDataSrc 1 setupped",
                    "SyncDataSrc 3 closed",
                    "SyncDataSrc 3 dropped",
                    "AsyncDataSrc 4 closed",
                    "AsyncDataSrc 4 dropped",
                    "SyncDataSrc 2 closed",
                    "SyncDataSrc 2 dropped",
                    "AsyncDataSrc 1 closed",
                    "AsyncDataSrc 1 dropped",
                ]
            );
        }

        #[test]
        fn test_cannot_add_and_remove_data_src_between_begin_and_end() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            if let Ok(_) = setup() {
                let _later = shutdown_later();

                let mut hub = DataHub::new();

                let ptr = hub.local_data_src_list.not_setup_head();
                assert!(ptr.is_null());
                let ptr = hub.local_data_src_list.did_setup_head();
                assert!(ptr.is_null());
                assert!(hub.data_conn_list.head().is_null());
                assert_eq!(hub.data_src_map.len(), 0);
                assert_eq!(hub.data_conn_map.len(), 0);
                assert_eq!(hub.fixed, false);

                hub.uses("baz", SyncDataSrc::new(1, logger.clone(), false, false));

                let mut ptr = hub.local_data_src_list.not_setup_head();
                assert!(!ptr.is_null());
                ptr = unsafe { (*ptr).next };
                assert!(ptr.is_null());
                let ptr = hub.local_data_src_list.did_setup_head();
                assert!(ptr.is_null());
                assert!(hub.data_conn_list.head().is_null());
                assert_eq!(hub.data_src_map.len(), 0);
                assert_eq!(hub.data_conn_map.len(), 0);
                assert_eq!(hub.fixed, false);

                assert!(hub.begin().is_ok());

                let ptr = hub.local_data_src_list.not_setup_head();
                assert!(ptr.is_null());
                let mut ptr = hub.local_data_src_list.did_setup_head();
                assert!(!ptr.is_null());
                ptr = unsafe { (*ptr).next };
                assert!(ptr.is_null());
                assert!(hub.data_conn_list.head().is_null());
                assert_eq!(hub.data_src_map.len(), 1);
                assert_eq!(hub.data_conn_map.len(), 0);
                assert_eq!(hub.fixed, true);

                hub.uses("foo", AsyncDataSrc::new(2, logger.clone(), false, false));

                let ptr = hub.local_data_src_list.not_setup_head();
                assert!(ptr.is_null());
                let mut ptr = hub.local_data_src_list.did_setup_head();
                assert!(!ptr.is_null());
                ptr = unsafe { (*ptr).next };
                assert!(ptr.is_null());
                assert!(hub.data_conn_list.head().is_null());
                assert_eq!(hub.data_src_map.len(), 1);
                assert_eq!(hub.data_conn_map.len(), 0);
                assert_eq!(hub.fixed, true);

                hub.disuses("baz");

                let ptr = hub.local_data_src_list.not_setup_head();
                assert!(ptr.is_null());
                let mut ptr = hub.local_data_src_list.did_setup_head();
                assert!(!ptr.is_null());
                ptr = unsafe { (*ptr).next };
                assert!(ptr.is_null());
                assert!(hub.data_conn_list.head().is_null());
                assert_eq!(hub.data_src_map.len(), 1);
                assert_eq!(hub.data_conn_map.len(), 0);
                assert_eq!(hub.fixed, true);

                hub.end();

                let ptr = hub.local_data_src_list.not_setup_head();
                assert!(ptr.is_null());
                let mut ptr = hub.local_data_src_list.did_setup_head();
                assert!(!ptr.is_null());
                ptr = unsafe { (*ptr).next };
                assert!(ptr.is_null());
                assert!(hub.data_conn_list.head().is_null());
                assert_eq!(hub.data_src_map.len(), 1);
                assert_eq!(hub.data_conn_map.len(), 0);
                assert_eq!(hub.fixed, false);

                hub.uses("foo", AsyncDataSrc::new(2, logger.clone(), false, false));

                let mut ptr = hub.local_data_src_list.not_setup_head();
                assert!(!ptr.is_null());
                ptr = unsafe { (*ptr).next };
                assert!(ptr.is_null());
                let mut ptr = hub.local_data_src_list.did_setup_head();
                assert!(!ptr.is_null());
                ptr = unsafe { (*ptr).next };
                assert!(ptr.is_null());
                assert!(hub.data_conn_list.head().is_null());
                assert_eq!(hub.data_src_map.len(), 1);
                assert_eq!(hub.data_conn_map.len(), 0);
                assert_eq!(hub.fixed, false);

                hub.disuses("baz");

                let mut ptr = hub.local_data_src_list.not_setup_head();
                assert!(!ptr.is_null());
                ptr = unsafe { (*ptr).next };
                assert!(ptr.is_null());
                let ptr = hub.local_data_src_list.did_setup_head();
                assert!(ptr.is_null());
                assert!(hub.data_conn_list.head().is_null());
                assert_eq!(hub.data_src_map.len(), 1);
                assert_eq!(hub.data_conn_map.len(), 0);
                assert_eq!(hub.fixed, false);
            }
        }

        #[test]
        fn test_begin_and_end() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            uses("foo", AsyncDataSrc::new(1, logger.clone(), false, false));
            uses("bar", SyncDataSrc::new(2, logger.clone(), false, false));

            if let Ok(_) = setup() {
                let _later = shutdown_later();

                let mut hub = DataHub::new();

                hub.uses("baz", SyncDataSrc::new(3, logger.clone(), false, false));
                hub.uses("qux", AsyncDataSrc::new(4, logger.clone(), false, false));

                let mut ptr = hub.local_data_src_list.not_setup_head();
                assert!(!ptr.is_null());
                ptr = unsafe { (*ptr).next };
                assert!(!ptr.is_null());
                ptr = unsafe { (*ptr).next };
                assert!(ptr.is_null());
                assert!(hub.local_data_src_list.did_setup_head().is_null());
                assert!(hub.data_conn_list.head().is_null());
                assert_eq!(hub.data_src_map.len(), 2);
                assert_eq!(hub.data_conn_map.len(), 0);
                assert_eq!(hub.fixed, false);

                assert!(hub.begin().is_ok());

                assert!(hub.local_data_src_list.not_setup_head().is_null());
                let mut ptr = hub.local_data_src_list.did_setup_head();
                assert!(!ptr.is_null());
                ptr = unsafe { (*ptr).next };
                assert!(!ptr.is_null());
                ptr = unsafe { (*ptr).next };
                assert!(ptr.is_null());
                assert!(hub.data_conn_list.head().is_null());
                assert_eq!(hub.data_src_map.len(), 4);
                assert_eq!(hub.data_conn_map.len(), 0);
                assert_eq!(hub.fixed, true);

                hub.end();

                assert!(hub.local_data_src_list.not_setup_head().is_null());
                let mut ptr = hub.local_data_src_list.did_setup_head();
                assert!(!ptr.is_null());
                ptr = unsafe { (*ptr).next };
                assert!(!ptr.is_null());
                ptr = unsafe { (*ptr).next };
                assert!(ptr.is_null());
                assert!(hub.data_conn_list.head().is_null());
                assert_eq!(hub.data_src_map.len(), 4);
                assert_eq!(hub.data_conn_map.len(), 0);
                assert_eq!(hub.fixed, false);
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc 2 setupped",
                    "AsyncDataSrc 1 setupped",
                    "SyncDataSrc 3 setupped",
                    "AsyncDataSrc 4 setupped",
                    "AsyncDataSrc 4 closed",
                    "AsyncDataSrc 4 dropped",
                    "SyncDataSrc 3 closed",
                    "SyncDataSrc 3 dropped",
                    "SyncDataSrc 2 closed",
                    "SyncDataSrc 2 dropped",
                    "AsyncDataSrc 1 closed",
                    "AsyncDataSrc 1 dropped",
                ]
            );
        }

        #[test]
        fn test_begin_and_end_but_fail_sync() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            uses("foo", AsyncDataSrc::new(1, logger.clone(), false, false));
            uses("bar", SyncDataSrc::new(2, logger.clone(), false, false));

            if let Ok(_) = setup() {
                let _later = shutdown_later();

                let mut hub = DataHub::new();
                hub.uses("baz", AsyncDataSrc::new(3, logger.clone(), false, false));
                hub.uses("qux", SyncDataSrc::new(4, logger.clone(), true, false));

                if let Err(err) = hub.begin() {
                    match err.reason::<DataHubError>() {
                        Ok(r) => match r {
                            DataHubError::FailToSetupLocalDataSrcs { errors } => {
                                assert_eq!(errors.len(), 1);
                                if let Some(err) = errors.get("qux") {
                                    match err.reason::<String>() {
                                        Ok(s) => assert_eq!(s, "XXX"),
                                        Err(_) => panic!(),
                                    }
                                } else {
                                    panic!();
                                }
                            }
                            _ => panic!(),
                        },
                        Err(_) => panic!(),
                    }
                } else {
                    panic!();
                }

                assert!(hub.local_data_src_list.not_setup_head().is_null());
                let mut ptr = hub.local_data_src_list.did_setup_head();
                assert!(!ptr.is_null());
                ptr = unsafe { (*ptr).next };
                assert!(ptr.is_null());
                assert!(hub.data_conn_list.head().is_null());
                assert_eq!(hub.data_src_map.len(), 3);
                assert_eq!(hub.data_conn_map.len(), 0);
                assert_eq!(hub.fixed, true);

                hub.end();

                assert!(hub.local_data_src_list.not_setup_head().is_null());
                let mut ptr = hub.local_data_src_list.did_setup_head();
                assert!(!ptr.is_null());
                ptr = unsafe { (*ptr).next };
                assert!(ptr.is_null());
                assert!(hub.data_conn_list.head().is_null());
                assert_eq!(hub.data_src_map.len(), 3);
                assert_eq!(hub.data_conn_map.len(), 0);
                assert_eq!(hub.fixed, false);
            } else {
                panic!();
            }
        }

        #[test]
        fn test_begin_and_end_but_fail_async() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            uses("foo", AsyncDataSrc::new(1, logger.clone(), false, false));
            uses("bar", SyncDataSrc::new(2, logger.clone(), false, false));

            if let Ok(_) = setup() {
                let _later = shutdown_later();

                let mut hub = DataHub::new();
                hub.uses("baz", AsyncDataSrc::new(1, logger.clone(), true, false));
                hub.uses("qux", SyncDataSrc::new(2, logger.clone(), false, false));

                if let Err(err) = hub.begin() {
                    match err.reason::<DataHubError>() {
                        Ok(r) => match r {
                            DataHubError::FailToSetupLocalDataSrcs { errors } => {
                                assert_eq!(errors.len(), 1);
                                if let Some(err) = errors.get("baz") {
                                    match err.reason::<String>() {
                                        Ok(s) => assert_eq!(s, "YYY"),
                                        Err(_) => panic!(),
                                    }
                                } else {
                                    panic!();
                                }
                            }
                            _ => panic!(),
                        },
                        Err(_) => panic!(),
                    }
                } else {
                    panic!();
                }

                assert!(hub.local_data_src_list.not_setup_head().is_null());
                let mut ptr = hub.local_data_src_list.did_setup_head();
                assert!(!ptr.is_null());
                ptr = unsafe { (*ptr).next };
                assert!(ptr.is_null());
                assert!(hub.data_conn_list.head().is_null());
                assert_eq!(hub.data_src_map.len(), 3);
                assert_eq!(hub.data_conn_map.len(), 0);
                assert_eq!(hub.fixed, true);

                hub.end();

                assert!(hub.local_data_src_list.not_setup_head().is_null());
                let mut ptr = hub.local_data_src_list.did_setup_head();
                assert!(!ptr.is_null());
                ptr = unsafe { (*ptr).next };
                assert!(ptr.is_null());
                assert!(hub.data_conn_list.head().is_null());
                assert_eq!(hub.data_src_map.len(), 3);
                assert_eq!(hub.data_conn_map.len(), 0);
                assert_eq!(hub.fixed, false);
            } else {
                panic!();
            }
        }

        #[test]
        fn test_commit() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            uses("foo", AsyncDataSrc::new(1, logger.clone(), false, false));
            uses("bar", SyncDataSrc::new(2, logger.clone(), false, false));

            if let Ok(_) = setup() {
                let _later = shutdown_later();

                let mut hub = DataHub::new();
                hub.uses("baz", AsyncDataSrc::new(3, logger.clone(), false, false));
                hub.uses("qux", SyncDataSrc::new(4, logger.clone(), false, false));

                if let Ok(_) = hub.begin() {
                    if let Ok(conn1) = hub.get_data_conn::<AsyncDataConn>("foo") {
                        assert_eq!(
                            any::type_name_of_val(conn1),
                            "sabi::data_hub::tests_data_hub::AsyncDataConn"
                        );
                    } else {
                        panic!();
                    }
                    if let Ok(conn2) = hub.get_data_conn::<SyncDataConn>("bar") {
                        assert_eq!(
                            any::type_name_of_val(conn2),
                            "sabi::data_hub::tests_data_hub::SyncDataConn"
                        );
                    } else {
                        panic!();
                    }
                    if let Ok(conn3) = hub.get_data_conn::<AsyncDataConn>("baz") {
                        assert_eq!(
                            any::type_name_of_val(conn3),
                            "sabi::data_hub::tests_data_hub::AsyncDataConn"
                        );
                    } else {
                        panic!();
                    }
                    if let Ok(conn4) = hub.get_data_conn::<SyncDataConn>("qux") {
                        assert_eq!(
                            any::type_name_of_val(conn4),
                            "sabi::data_hub::tests_data_hub::SyncDataConn"
                        );
                    } else {
                        panic!();
                    }

                    if let Ok(conn1) = hub.get_data_conn::<AsyncDataConn>("foo") {
                        assert_eq!(
                            any::type_name_of_val(conn1),
                            "sabi::data_hub::tests_data_hub::AsyncDataConn"
                        );
                    } else {
                        panic!();
                    }
                    if let Ok(conn2) = hub.get_data_conn::<SyncDataConn>("bar") {
                        assert_eq!(
                            any::type_name_of_val(conn2),
                            "sabi::data_hub::tests_data_hub::SyncDataConn"
                        );
                    } else {
                        panic!();
                    }
                    if let Ok(conn3) = hub.get_data_conn::<AsyncDataConn>("baz") {
                        assert_eq!(
                            any::type_name_of_val(conn3),
                            "sabi::data_hub::tests_data_hub::AsyncDataConn"
                        );
                    } else {
                        panic!();
                    }
                    if let Ok(conn4) = hub.get_data_conn::<SyncDataConn>("qux") {
                        assert_eq!(
                            any::type_name_of_val(conn4),
                            "sabi::data_hub::tests_data_hub::SyncDataConn"
                        );
                    } else {
                        panic!();
                    }

                    assert!(hub.commit().is_ok());
                    hub.end();
                } else {
                    panic!();
                }
            } else {
                panic!();
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc 2 setupped",
                    "AsyncDataSrc 1 setupped",
                    "SyncDataSrc 4 setupped",
                    "AsyncDataSrc 3 setupped",
                    "AsyncDataSrc 1 created DataConn",
                    "SyncDataSrc 2 created DataConn",
                    "AsyncDataSrc 3 created DataConn",
                    "SyncDataSrc 4 created DataConn",
                    "AsyncDataConn 1 committed",
                    "SyncDataConn 2 committed",
                    "AsyncDataConn 3 committed",
                    "SyncDataConn 4 committed",
                    "SyncDataConn 4 closed",
                    "SyncDataConn 4 dropped",
                    "AsyncDataConn 3 closed",
                    "AsyncDataConn 3 dropped",
                    "SyncDataConn 2 closed",
                    "SyncDataConn 2 dropped",
                    "AsyncDataConn 1 closed",
                    "AsyncDataConn 1 dropped",
                    "SyncDataSrc 4 closed",
                    "SyncDataSrc 4 dropped",
                    "AsyncDataSrc 3 closed",
                    "AsyncDataSrc 3 dropped",
                    "SyncDataSrc 2 closed",
                    "SyncDataSrc 2 dropped",
                    "AsyncDataSrc 1 closed",
                    "AsyncDataSrc 1 dropped",
                ],
            );
        }

        #[test]
        fn test_fail_to_cast_new_data_conn() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            uses("foo", AsyncDataSrc::new(1, logger.clone(), false, false));

            if let Ok(_) = setup() {
                let _later = shutdown_later();

                let mut hub = DataHub::new();
                hub.uses("bar", SyncDataSrc::new(2, logger.clone(), false, false));

                if let Ok(_) = hub.begin() {
                    if let Err(err) = hub.get_data_conn::<SyncDataConn>("foo") {
                        match err.reason::<DataHubError>() {
                            Ok(r) => match r {
                                DataHubError::FailToCastDataConn { name, cast_to_type } => {
                                    assert_eq!(name, "foo");
                                    assert_eq!(
                                        *cast_to_type,
                                        "sabi::data_hub::tests_data_hub::SyncDataConn"
                                    );
                                }
                                _ => panic!(),
                            },
                            Err(_) => panic!(),
                        }
                    } else {
                        panic!();
                    }

                    if let Err(err) = hub.get_data_conn::<AsyncDataConn>("bar") {
                        match err.reason::<DataHubError>() {
                            Ok(r) => match r {
                                DataHubError::FailToCastDataConn { name, cast_to_type } => {
                                    assert_eq!(name, "bar");
                                    assert_eq!(
                                        *cast_to_type,
                                        "sabi::data_hub::tests_data_hub::AsyncDataConn"
                                    );
                                }
                                _ => panic!(),
                            },
                            Err(_) => panic!(),
                        }
                    } else {
                        panic!();
                    }
                } else {
                    panic!();
                }
            } else {
                panic!();
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "AsyncDataSrc 1 setupped",
                    "SyncDataSrc 2 setupped",
                    "SyncDataSrc 2 closed",
                    "SyncDataSrc 2 dropped",
                    "AsyncDataSrc 1 closed",
                    "AsyncDataSrc 1 dropped",
                ],
            );
        }

        #[test]
        fn test_fail_to_cast_reused_data_conn() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            uses("foo", AsyncDataSrc::new(1, logger.clone(), false, false));

            if let Ok(_) = setup() {
                let _later = shutdown_later();

                let mut hub = DataHub::new();
                hub.uses("bar", SyncDataSrc::new(2, logger.clone(), false, false));

                if let Ok(_) = hub.begin() {
                    if let Ok(conn1) = hub.get_data_conn::<AsyncDataConn>("foo") {
                        assert_eq!(
                            any::type_name_of_val(conn1),
                            "sabi::data_hub::tests_data_hub::AsyncDataConn"
                        );
                    } else {
                        panic!();
                    }
                    if let Ok(conn2) = hub.get_data_conn::<SyncDataConn>("bar") {
                        assert_eq!(
                            any::type_name_of_val(conn2),
                            "sabi::data_hub::tests_data_hub::SyncDataConn"
                        );
                    } else {
                        panic!();
                    }

                    if let Err(err) = hub.get_data_conn::<SyncDataConn>("foo") {
                        match err.reason::<DataHubError>() {
                            Ok(r) => match r {
                                DataHubError::FailToCastDataConn { name, cast_to_type } => {
                                    assert_eq!(name, "foo");
                                    assert_eq!(
                                        *cast_to_type,
                                        "sabi::data_hub::tests_data_hub::SyncDataConn"
                                    );
                                }
                                _ => panic!(),
                            },
                            Err(_) => panic!(),
                        }
                    } else {
                        panic!();
                    }

                    if let Err(err) = hub.get_data_conn::<AsyncDataConn>("bar") {
                        match err.reason::<DataHubError>() {
                            Ok(r) => match r {
                                DataHubError::FailToCastDataConn { name, cast_to_type } => {
                                    assert_eq!(name, "bar");
                                    assert_eq!(
                                        *cast_to_type,
                                        "sabi::data_hub::tests_data_hub::AsyncDataConn"
                                    );
                                }
                                _ => panic!(),
                            },
                            Err(_) => panic!(),
                        }
                    } else {
                        panic!();
                    }
                } else {
                    panic!();
                }
            } else {
                panic!();
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "AsyncDataSrc 1 setupped",
                    "SyncDataSrc 2 setupped",
                    "AsyncDataSrc 1 created DataConn",
                    "SyncDataSrc 2 created DataConn",
                    "SyncDataConn 2 closed",
                    "SyncDataConn 2 dropped",
                    "AsyncDataConn 1 closed",
                    "AsyncDataConn 1 dropped",
                    "SyncDataSrc 2 closed",
                    "SyncDataSrc 2 dropped",
                    "AsyncDataSrc 1 closed",
                    "AsyncDataSrc 1 dropped",
                ],
            );
        }

        #[test]
        fn test_fail_to_create_data_conn_because_of_no_data_src() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            uses("foo", AsyncDataSrc::new(1, logger.clone(), false, false));

            if let Ok(_) = setup() {
                let _later = shutdown_later();

                let mut hub = DataHub::new();
                hub.uses("bar", SyncDataSrc::new(2, logger.clone(), false, false));

                if let Ok(_) = hub.begin() {
                    if let Err(err) = hub.get_data_conn::<SyncDataConn>("baz") {
                        match err.reason::<DataHubError>() {
                            Ok(r) => match r {
                                DataHubError::NoDataSrcToCreateDataConn {
                                    name,
                                    data_conn_type,
                                } => {
                                    assert_eq!(name, "baz");
                                    assert_eq!(
                                        *data_conn_type,
                                        "sabi::data_hub::tests_data_hub::SyncDataConn"
                                    );
                                }
                                _ => panic!(),
                            },
                            Err(_) => panic!(),
                        }
                    } else {
                        panic!();
                    }

                    if let Err(err) = hub.get_data_conn::<AsyncDataConn>("qux") {
                        match err.reason::<DataHubError>() {
                            Ok(r) => match r {
                                DataHubError::NoDataSrcToCreateDataConn {
                                    name,
                                    data_conn_type,
                                } => {
                                    assert_eq!(name, "qux");
                                    assert_eq!(
                                        *data_conn_type,
                                        "sabi::data_hub::tests_data_hub::AsyncDataConn"
                                    );
                                }
                                _ => panic!(),
                            },
                            Err(_) => panic!(),
                        }
                    } else {
                        panic!();
                    }
                } else {
                    panic!();
                }
            } else {
                panic!();
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "AsyncDataSrc 1 setupped",
                    "SyncDataSrc 2 setupped",
                    "SyncDataSrc 2 closed",
                    "SyncDataSrc 2 dropped",
                    "AsyncDataSrc 1 closed",
                    "AsyncDataSrc 1 dropped",
                ],
            );
        }

        #[test]
        fn test_commit_when_no_data_conn() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            uses("foo", AsyncDataSrc::new(1, logger.clone(), false, false));
            uses("bar", SyncDataSrc::new(2, logger.clone(), false, false));

            if let Ok(_) = setup() {
                let _later = shutdown_later();

                let mut hub = DataHub::new();
                hub.uses("baz", AsyncDataSrc::new(3, logger.clone(), false, false));
                hub.uses("qux", SyncDataSrc::new(4, logger.clone(), false, false));

                if let Ok(_) = hub.begin() {
                    assert!(hub.commit().is_ok());
                    hub.end();
                } else {
                    panic!();
                }
            } else {
                panic!();
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc 2 setupped",
                    "AsyncDataSrc 1 setupped",
                    "SyncDataSrc 4 setupped",
                    "AsyncDataSrc 3 setupped",
                    "SyncDataSrc 4 closed",
                    "SyncDataSrc 4 dropped",
                    "AsyncDataSrc 3 closed",
                    "AsyncDataSrc 3 dropped",
                    "SyncDataSrc 2 closed",
                    "SyncDataSrc 2 dropped",
                    "AsyncDataSrc 1 closed",
                    "AsyncDataSrc 1 dropped",
                ],
            );
        }

        #[test]
        fn test_commit_but_fail_global_sync() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            uses("foo", AsyncDataSrc::new(1, logger.clone(), false, false));
            uses("bar", SyncDataSrc::new(2, logger.clone(), false, true));

            if let Ok(_) = setup() {
                let _later = shutdown_later();

                let mut hub = DataHub::new();
                hub.uses("baz", AsyncDataSrc::new(3, logger.clone(), false, false));
                hub.uses("qux", SyncDataSrc::new(4, logger.clone(), false, false));

                if let Ok(_) = hub.begin() {
                    if let Ok(conn1) = hub.get_data_conn::<AsyncDataConn>("foo") {
                        assert_eq!(
                            any::type_name_of_val(conn1),
                            "sabi::data_hub::tests_data_hub::AsyncDataConn"
                        );
                    } else {
                        panic!();
                    }
                    if let Ok(conn2) = hub.get_data_conn::<SyncDataConn>("bar") {
                        assert_eq!(
                            any::type_name_of_val(conn2),
                            "sabi::data_hub::tests_data_hub::SyncDataConn"
                        );
                    } else {
                        panic!();
                    }
                    if let Ok(conn3) = hub.get_data_conn::<AsyncDataConn>("baz") {
                        assert_eq!(
                            any::type_name_of_val(conn3),
                            "sabi::data_hub::tests_data_hub::AsyncDataConn"
                        );
                    } else {
                        panic!();
                    }
                    if let Ok(conn4) = hub.get_data_conn::<SyncDataConn>("qux") {
                        assert_eq!(
                            any::type_name_of_val(conn4),
                            "sabi::data_hub::tests_data_hub::SyncDataConn"
                        );
                    } else {
                        panic!();
                    }

                    match hub.commit() {
                        Ok(_) => panic!(),
                        Err(err) => match err.reason::<DataHubError>() {
                            Ok(r) => match r {
                                DataHubError::FailToCommitDataConn { errors } => {
                                    assert_eq!(errors.len(), 1);
                                    if let Some(e) = errors.get("bar") {
                                        if let Ok(s) = e.reason::<String>() {
                                            assert_eq!(s, "ZZZ");
                                        } else {
                                            panic!();
                                        }
                                    } else {
                                        panic!();
                                    }
                                }
                                _ => panic!(),
                            },
                            Err(_) => panic!(),
                        },
                    }

                    hub.end();
                } else {
                    panic!();
                }
            } else {
                panic!();
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc 2 setupped",
                    "AsyncDataSrc 1 setupped",
                    "SyncDataSrc 4 setupped",
                    "AsyncDataSrc 3 setupped",
                    "AsyncDataSrc 1 created DataConn",
                    "SyncDataSrc 2 created DataConn",
                    "AsyncDataSrc 3 created DataConn",
                    "SyncDataSrc 4 created DataConn",
                    "AsyncDataConn 1 committed",
                    "SyncDataConn 2 failed to commit",
                    "SyncDataConn 4 closed",
                    "SyncDataConn 4 dropped",
                    "AsyncDataConn 3 closed",
                    "AsyncDataConn 3 dropped",
                    "SyncDataConn 2 closed",
                    "SyncDataConn 2 dropped",
                    "AsyncDataConn 1 closed",
                    "AsyncDataConn 1 dropped",
                    "SyncDataSrc 4 closed",
                    "SyncDataSrc 4 dropped",
                    "AsyncDataSrc 3 closed",
                    "AsyncDataSrc 3 dropped",
                    "SyncDataSrc 2 closed",
                    "SyncDataSrc 2 dropped",
                    "AsyncDataSrc 1 closed",
                    "AsyncDataSrc 1 dropped",
                ],
            );
        }

        #[test]
        fn test_commit_but_fail_global_async() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            uses("foo", AsyncDataSrc::new(1, logger.clone(), false, true));
            uses("bar", SyncDataSrc::new(2, logger.clone(), false, false));

            if let Ok(_) = setup() {
                let _later = shutdown_later();

                let mut hub = DataHub::new();
                hub.uses("baz", AsyncDataSrc::new(3, logger.clone(), false, false));
                hub.uses("qux", SyncDataSrc::new(4, logger.clone(), false, false));

                if let Ok(_) = hub.begin() {
                    if let Ok(conn1) = hub.get_data_conn::<AsyncDataConn>("foo") {
                        assert_eq!(
                            any::type_name_of_val(conn1),
                            "sabi::data_hub::tests_data_hub::AsyncDataConn"
                        );
                    } else {
                        panic!();
                    }
                    if let Ok(conn2) = hub.get_data_conn::<SyncDataConn>("bar") {
                        assert_eq!(
                            any::type_name_of_val(conn2),
                            "sabi::data_hub::tests_data_hub::SyncDataConn"
                        );
                    } else {
                        panic!();
                    }
                    if let Ok(conn3) = hub.get_data_conn::<AsyncDataConn>("baz") {
                        assert_eq!(
                            any::type_name_of_val(conn3),
                            "sabi::data_hub::tests_data_hub::AsyncDataConn"
                        );
                    } else {
                        panic!();
                    }
                    if let Ok(conn4) = hub.get_data_conn::<SyncDataConn>("qux") {
                        assert_eq!(
                            any::type_name_of_val(conn4),
                            "sabi::data_hub::tests_data_hub::SyncDataConn"
                        );
                    } else {
                        panic!();
                    }

                    match hub.commit() {
                        Ok(_) => panic!(),
                        Err(err) => match err.reason::<DataHubError>() {
                            Ok(r) => match r {
                                DataHubError::FailToCommitDataConn { errors } => {
                                    assert_eq!(errors.len(), 1);
                                    if let Some(e) = errors.get("foo") {
                                        if let Ok(s) = e.reason::<String>() {
                                            assert_eq!(s, "VVV");
                                        } else {
                                            panic!();
                                        }
                                    } else {
                                        panic!();
                                    }
                                }
                                _ => panic!(),
                            },
                            Err(_) => panic!(),
                        },
                    }

                    hub.end();
                } else {
                    panic!();
                }
            } else {
                panic!();
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc 2 setupped",
                    "AsyncDataSrc 1 setupped",
                    "SyncDataSrc 4 setupped",
                    "AsyncDataSrc 3 setupped",
                    "AsyncDataSrc 1 created DataConn",
                    "SyncDataSrc 2 created DataConn",
                    "AsyncDataSrc 3 created DataConn",
                    "SyncDataSrc 4 created DataConn",
                    "AsyncDataConn 1 failed to commit",
                    "SyncDataConn 4 closed",
                    "SyncDataConn 4 dropped",
                    "AsyncDataConn 3 closed",
                    "AsyncDataConn 3 dropped",
                    "SyncDataConn 2 closed",
                    "SyncDataConn 2 dropped",
                    "AsyncDataConn 1 closed",
                    "AsyncDataConn 1 dropped",
                    "SyncDataSrc 4 closed",
                    "SyncDataSrc 4 dropped",
                    "AsyncDataSrc 3 closed",
                    "AsyncDataSrc 3 dropped",
                    "SyncDataSrc 2 closed",
                    "SyncDataSrc 2 dropped",
                    "AsyncDataSrc 1 closed",
                    "AsyncDataSrc 1 dropped",
                ],
            );
        }

        #[test]
        fn test_commit_but_fail_local_sync() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            uses("foo", AsyncDataSrc::new(1, logger.clone(), false, false));
            uses("bar", SyncDataSrc::new(2, logger.clone(), false, false));

            if let Ok(_) = setup() {
                let _later = shutdown_later();

                let mut hub = DataHub::new();
                hub.uses("baz", AsyncDataSrc::new(3, logger.clone(), false, false));
                hub.uses("qux", SyncDataSrc::new(4, logger.clone(), false, true));

                if let Ok(_) = hub.begin() {
                    if let Ok(conn1) = hub.get_data_conn::<AsyncDataConn>("foo") {
                        assert_eq!(
                            any::type_name_of_val(conn1),
                            "sabi::data_hub::tests_data_hub::AsyncDataConn"
                        );
                    } else {
                        panic!();
                    }
                    if let Ok(conn2) = hub.get_data_conn::<SyncDataConn>("bar") {
                        assert_eq!(
                            any::type_name_of_val(conn2),
                            "sabi::data_hub::tests_data_hub::SyncDataConn"
                        );
                    } else {
                        panic!();
                    }
                    if let Ok(conn3) = hub.get_data_conn::<AsyncDataConn>("baz") {
                        assert_eq!(
                            any::type_name_of_val(conn3),
                            "sabi::data_hub::tests_data_hub::AsyncDataConn"
                        );
                    } else {
                        panic!();
                    }
                    if let Ok(conn4) = hub.get_data_conn::<SyncDataConn>("qux") {
                        assert_eq!(
                            any::type_name_of_val(conn4),
                            "sabi::data_hub::tests_data_hub::SyncDataConn"
                        );
                    } else {
                        panic!();
                    }

                    match hub.commit() {
                        Ok(_) => panic!(),
                        Err(err) => match err.reason::<DataHubError>() {
                            Ok(r) => match r {
                                DataHubError::FailToCommitDataConn { errors } => {
                                    assert_eq!(errors.len(), 1);
                                    if let Some(e) = errors.get("qux") {
                                        if let Ok(s) = e.reason::<String>() {
                                            assert_eq!(s, "ZZZ");
                                        } else {
                                            panic!();
                                        }
                                    } else {
                                        panic!();
                                    }
                                }
                                _ => panic!(),
                            },
                            Err(_) => panic!(),
                        },
                    }

                    hub.end();
                } else {
                    panic!();
                }
            } else {
                panic!();
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc 2 setupped",
                    "AsyncDataSrc 1 setupped",
                    "SyncDataSrc 4 setupped",
                    "AsyncDataSrc 3 setupped",
                    "AsyncDataSrc 1 created DataConn",
                    "SyncDataSrc 2 created DataConn",
                    "AsyncDataSrc 3 created DataConn",
                    "SyncDataSrc 4 created DataConn",
                    "AsyncDataConn 1 committed",
                    "SyncDataConn 2 committed",
                    "AsyncDataConn 3 committed",
                    "SyncDataConn 4 failed to commit",
                    "SyncDataConn 4 closed",
                    "SyncDataConn 4 dropped",
                    "AsyncDataConn 3 closed",
                    "AsyncDataConn 3 dropped",
                    "SyncDataConn 2 closed",
                    "SyncDataConn 2 dropped",
                    "AsyncDataConn 1 closed",
                    "AsyncDataConn 1 dropped",
                    "SyncDataSrc 4 closed",
                    "SyncDataSrc 4 dropped",
                    "AsyncDataSrc 3 closed",
                    "AsyncDataSrc 3 dropped",
                    "SyncDataSrc 2 closed",
                    "SyncDataSrc 2 dropped",
                    "AsyncDataSrc 1 closed",
                    "AsyncDataSrc 1 dropped",
                ],
            );
        }

        #[test]
        fn test_commit_but_fail_local_async() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            uses("foo", AsyncDataSrc::new(1, logger.clone(), false, false));
            uses("bar", SyncDataSrc::new(2, logger.clone(), false, false));

            if let Ok(_) = setup() {
                let _later = shutdown_later();

                let mut hub = DataHub::new();
                hub.uses("baz", AsyncDataSrc::new(3, logger.clone(), false, true));
                hub.uses("qux", SyncDataSrc::new(4, logger.clone(), false, false));

                if let Ok(_) = hub.begin() {
                    if let Ok(conn1) = hub.get_data_conn::<AsyncDataConn>("foo") {
                        assert_eq!(
                            any::type_name_of_val(conn1),
                            "sabi::data_hub::tests_data_hub::AsyncDataConn"
                        );
                    } else {
                        panic!();
                    }
                    if let Ok(conn2) = hub.get_data_conn::<SyncDataConn>("bar") {
                        assert_eq!(
                            any::type_name_of_val(conn2),
                            "sabi::data_hub::tests_data_hub::SyncDataConn"
                        );
                    } else {
                        panic!();
                    }
                    if let Ok(conn3) = hub.get_data_conn::<AsyncDataConn>("baz") {
                        assert_eq!(
                            any::type_name_of_val(conn3),
                            "sabi::data_hub::tests_data_hub::AsyncDataConn"
                        );
                    } else {
                        panic!();
                    }
                    if let Ok(conn4) = hub.get_data_conn::<SyncDataConn>("qux") {
                        assert_eq!(
                            any::type_name_of_val(conn4),
                            "sabi::data_hub::tests_data_hub::SyncDataConn"
                        );
                    } else {
                        panic!();
                    }

                    match hub.commit() {
                        Ok(_) => panic!(),
                        Err(err) => match err.reason::<DataHubError>() {
                            Ok(r) => match r {
                                DataHubError::FailToCommitDataConn { errors } => {
                                    assert_eq!(errors.len(), 1);
                                    if let Some(e) = errors.get("baz") {
                                        if let Ok(s) = e.reason::<String>() {
                                            assert_eq!(s, "VVV");
                                        } else {
                                            panic!();
                                        }
                                    } else {
                                        panic!();
                                    }
                                }
                                _ => panic!(),
                            },
                            Err(_) => panic!(),
                        },
                    }

                    hub.end();
                } else {
                    panic!();
                }
            } else {
                panic!();
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc 2 setupped",
                    "AsyncDataSrc 1 setupped",
                    "SyncDataSrc 4 setupped",
                    "AsyncDataSrc 3 setupped",
                    "AsyncDataSrc 1 created DataConn",
                    "SyncDataSrc 2 created DataConn",
                    "AsyncDataSrc 3 created DataConn",
                    "SyncDataSrc 4 created DataConn",
                    "AsyncDataConn 1 committed",
                    "SyncDataConn 2 committed",
                    "AsyncDataConn 3 failed to commit",
                    "SyncDataConn 4 closed",
                    "SyncDataConn 4 dropped",
                    "AsyncDataConn 3 closed",
                    "AsyncDataConn 3 dropped",
                    "SyncDataConn 2 closed",
                    "SyncDataConn 2 dropped",
                    "AsyncDataConn 1 closed",
                    "AsyncDataConn 1 dropped",
                    "SyncDataSrc 4 closed",
                    "SyncDataSrc 4 dropped",
                    "AsyncDataSrc 3 closed",
                    "AsyncDataSrc 3 dropped",
                    "SyncDataSrc 2 closed",
                    "SyncDataSrc 2 dropped",
                    "AsyncDataSrc 1 closed",
                    "AsyncDataSrc 1 dropped",
                ],
            );
        }

        #[test]
        fn test_rollback() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            uses("foo", AsyncDataSrc::new(1, logger.clone(), false, false));
            uses("bar", SyncDataSrc::new(2, logger.clone(), false, false));

            if let Ok(_) = setup() {
                let _later = shutdown_later();

                let mut hub = DataHub::new();
                hub.uses("baz", AsyncDataSrc::new(3, logger.clone(), false, false));
                hub.uses("qux", SyncDataSrc::new(4, logger.clone(), false, false));

                if let Ok(_) = hub.begin() {
                    if let Ok(conn1) = hub.get_data_conn::<AsyncDataConn>("foo") {
                        assert_eq!(
                            any::type_name_of_val(conn1),
                            "sabi::data_hub::tests_data_hub::AsyncDataConn"
                        );
                    } else {
                        panic!();
                    }
                    if let Ok(conn2) = hub.get_data_conn::<SyncDataConn>("bar") {
                        assert_eq!(
                            any::type_name_of_val(conn2),
                            "sabi::data_hub::tests_data_hub::SyncDataConn"
                        );
                    } else {
                        panic!();
                    }
                    if let Ok(conn3) = hub.get_data_conn::<AsyncDataConn>("baz") {
                        assert_eq!(
                            any::type_name_of_val(conn3),
                            "sabi::data_hub::tests_data_hub::AsyncDataConn"
                        );
                    } else {
                        panic!();
                    }
                    if let Ok(conn4) = hub.get_data_conn::<SyncDataConn>("qux") {
                        assert_eq!(
                            any::type_name_of_val(conn4),
                            "sabi::data_hub::tests_data_hub::SyncDataConn"
                        );
                    } else {
                        panic!();
                    }

                    hub.rollback();
                    hub.end();
                } else {
                    panic!();
                }
            } else {
                panic!();
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc 2 setupped",
                    "AsyncDataSrc 1 setupped",
                    "SyncDataSrc 4 setupped",
                    "AsyncDataSrc 3 setupped",
                    "AsyncDataSrc 1 created DataConn",
                    "SyncDataSrc 2 created DataConn",
                    "AsyncDataSrc 3 created DataConn",
                    "SyncDataSrc 4 created DataConn",
                    "AsyncDataConn 1 rollbacked",
                    "SyncDataConn 2 rollbacked",
                    "AsyncDataConn 3 rollbacked",
                    "SyncDataConn 4 rollbacked",
                    "SyncDataConn 4 closed",
                    "SyncDataConn 4 dropped",
                    "AsyncDataConn 3 closed",
                    "AsyncDataConn 3 dropped",
                    "SyncDataConn 2 closed",
                    "SyncDataConn 2 dropped",
                    "AsyncDataConn 1 closed",
                    "AsyncDataConn 1 dropped",
                    "SyncDataSrc 4 closed",
                    "SyncDataSrc 4 dropped",
                    "AsyncDataSrc 3 closed",
                    "AsyncDataSrc 3 dropped",
                    "SyncDataSrc 2 closed",
                    "SyncDataSrc 2 dropped",
                    "AsyncDataSrc 1 closed",
                    "AsyncDataSrc 1 dropped",
                ],
            );
        }

        #[test]
        fn test_force_back() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            uses("foo", AsyncDataSrc::new(1, logger.clone(), false, false));
            uses("bar", SyncDataSrc::new(2, logger.clone(), false, false));

            if let Ok(_) = setup() {
                let _later = shutdown_later();

                let mut hub = DataHub::new();
                hub.uses("baz", AsyncDataSrc::new(3, logger.clone(), false, false));
                hub.uses("qux", SyncDataSrc::new(4, logger.clone(), false, false));

                if let Ok(_) = hub.begin() {
                    if let Ok(conn1) = hub.get_data_conn::<AsyncDataConn>("foo") {
                        assert_eq!(
                            any::type_name_of_val(conn1),
                            "sabi::data_hub::tests_data_hub::AsyncDataConn"
                        );
                    } else {
                        panic!();
                    }
                    if let Ok(conn2) = hub.get_data_conn::<SyncDataConn>("bar") {
                        assert_eq!(
                            any::type_name_of_val(conn2),
                            "sabi::data_hub::tests_data_hub::SyncDataConn"
                        );
                    } else {
                        panic!();
                    }
                    if let Ok(conn3) = hub.get_data_conn::<AsyncDataConn>("baz") {
                        assert_eq!(
                            any::type_name_of_val(conn3),
                            "sabi::data_hub::tests_data_hub::AsyncDataConn"
                        );
                    } else {
                        panic!();
                    }
                    if let Ok(conn4) = hub.get_data_conn::<SyncDataConn>("qux") {
                        assert_eq!(
                            any::type_name_of_val(conn4),
                            "sabi::data_hub::tests_data_hub::SyncDataConn"
                        );
                    } else {
                        panic!();
                    }

                    assert!(hub.commit().is_ok());
                    hub.rollback();
                    hub.end();
                } else {
                    panic!();
                }
            } else {
                panic!();
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc 2 setupped",
                    "AsyncDataSrc 1 setupped",
                    "SyncDataSrc 4 setupped",
                    "AsyncDataSrc 3 setupped",
                    "AsyncDataSrc 1 created DataConn",
                    "SyncDataSrc 2 created DataConn",
                    "AsyncDataSrc 3 created DataConn",
                    "SyncDataSrc 4 created DataConn",
                    "AsyncDataConn 1 committed",
                    "SyncDataConn 2 committed",
                    "AsyncDataConn 3 committed",
                    "SyncDataConn 4 committed",
                    "AsyncDataConn 1 forced back",
                    "SyncDataConn 2 forced back",
                    "AsyncDataConn 3 forced back",
                    "SyncDataConn 4 forced back",
                    "SyncDataConn 4 closed",
                    "SyncDataConn 4 dropped",
                    "AsyncDataConn 3 closed",
                    "AsyncDataConn 3 dropped",
                    "SyncDataConn 2 closed",
                    "SyncDataConn 2 dropped",
                    "AsyncDataConn 1 closed",
                    "AsyncDataConn 1 dropped",
                    "SyncDataSrc 4 closed",
                    "SyncDataSrc 4 dropped",
                    "AsyncDataSrc 3 closed",
                    "AsyncDataSrc 3 dropped",
                    "SyncDataSrc 2 closed",
                    "SyncDataSrc 2 dropped",
                    "AsyncDataSrc 1 closed",
                    "AsyncDataSrc 1 dropped",
                ],
            );
        }

        #[test]
        fn test_post_commit() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            uses("foo", AsyncDataSrc::new(1, logger.clone(), false, false));
            uses("bar", SyncDataSrc::new(2, logger.clone(), false, false));

            if let Ok(_) = setup() {
                let _later = shutdown_later();

                let mut hub = DataHub::new();
                hub.uses("baz", AsyncDataSrc::new(3, logger.clone(), false, false));
                hub.uses("qux", SyncDataSrc::new(4, logger.clone(), false, false));

                if let Ok(_) = hub.begin() {
                    if let Ok(conn1) = hub.get_data_conn::<AsyncDataConn>("foo") {
                        assert_eq!(
                            any::type_name_of_val(conn1),
                            "sabi::data_hub::tests_data_hub::AsyncDataConn"
                        );
                    } else {
                        panic!();
                    }
                    if let Ok(conn2) = hub.get_data_conn::<SyncDataConn>("bar") {
                        assert_eq!(
                            any::type_name_of_val(conn2),
                            "sabi::data_hub::tests_data_hub::SyncDataConn"
                        );
                    } else {
                        panic!();
                    }
                    if let Ok(conn3) = hub.get_data_conn::<AsyncDataConn>("baz") {
                        assert_eq!(
                            any::type_name_of_val(conn3),
                            "sabi::data_hub::tests_data_hub::AsyncDataConn"
                        );
                    } else {
                        panic!();
                    }
                    if let Ok(conn4) = hub.get_data_conn::<SyncDataConn>("qux") {
                        assert_eq!(
                            any::type_name_of_val(conn4),
                            "sabi::data_hub::tests_data_hub::SyncDataConn"
                        );
                    } else {
                        panic!();
                    }

                    hub.post_commit();
                    hub.end();
                } else {
                    panic!();
                }
            } else {
                panic!();
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc 2 setupped",
                    "AsyncDataSrc 1 setupped",
                    "SyncDataSrc 4 setupped",
                    "AsyncDataSrc 3 setupped",
                    "AsyncDataSrc 1 created DataConn",
                    "SyncDataSrc 2 created DataConn",
                    "AsyncDataSrc 3 created DataConn",
                    "SyncDataSrc 4 created DataConn",
                    "AsyncDataConn 1 post committed",
                    "SyncDataConn 2 post committed",
                    "AsyncDataConn 3 post committed",
                    "SyncDataConn 4 post committed",
                    "SyncDataConn 4 closed",
                    "SyncDataConn 4 dropped",
                    "AsyncDataConn 3 closed",
                    "AsyncDataConn 3 dropped",
                    "SyncDataConn 2 closed",
                    "SyncDataConn 2 dropped",
                    "AsyncDataConn 1 closed",
                    "AsyncDataConn 1 dropped",
                    "SyncDataSrc 4 closed",
                    "SyncDataSrc 4 dropped",
                    "AsyncDataSrc 3 closed",
                    "AsyncDataSrc 3 dropped",
                    "SyncDataSrc 2 closed",
                    "SyncDataSrc 2 dropped",
                    "AsyncDataSrc 1 closed",
                    "AsyncDataSrc 1 dropped",
                ],
            );
        }
    }
}

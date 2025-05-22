// Copyright (C) 2024-2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use std::any;
use std::collections::HashMap;
use std::ptr;

use errs::Err;

use crate::async_group::AsyncGroup;

/// Is the enum type that represents the possible reasons for errors that can occur while working
/// with a `DataConn`.
#[derive(Debug)]
pub enum DataConnError {
    /// Indicates that some `DataConn` objects failed to commit for a transaction.
    FailToCommitDataConn {
        /// A map that stores `errs::Err` objects for `DataConn` objects failed to execute commit
        /// operation with the names of the data services the failing `DataConn` objects are
        /// connected to.
        errors: HashMap<String, Err>,
    },

    /// Indicates a failure during type casting when attempting to retrieve a struct that
    /// implements the `DataConn` trait.
    FailToCastDataConn {
        /// The name of the data service associated with the `DataConn` object for which the type
        /// cast failed.
        name: String,

        /// The name of the data type to which the cast was attempted.
        cast_to_type: &'static str,
    },
}

/// Is the trait that abstracts a per-session connection to an external data service,
/// such as a database, file system, or messaging service.
///
/// This trait declares methods required to manage the transactional lifecycle, including
/// commit, rollback, and close operations.
/// These methods allow unified transaction handling for each data service within a single
/// transaction.
///
/// The `force_back` method provides a chance to attempt compensating actions when a commit to one
/// service succeeds but another fails, allowing previously committed changes to be reversed.
///
/// The `AsyncGroup` argument of each method is used for asynchronous handling of potentially
/// time-consuming commit or rollback operations.
///
/// Actual input/output operations to the external data services are performed by downcasting the
/// trait object to the concrete struct implementing `DataConn` for the external service and
/// invoking its methods.
pub trait DataConn {
    /// Attempts to commit the transaction.
    ///
    /// # Parameters
    /// - `ag`: An `AsyncGroup` that can be used to manage asynchronous commit operations,
    ///         especially when the commit process is time-consuming and can be performed
    ///         asynchronously.
    fn commit(&mut self, ag: &mut AsyncGroup) -> Result<(), Err>;

    /// Returns true if the transaction has already been committed.
    fn is_committed(&self) -> bool;

    /// Rolls back the transaction.
    ///
    /// # Parameters
    ///
    /// - `ag`: An `AsyncGroup` that can be used to manage asynchronous rollback operations,
    ///         especially when the rollback process is time-consuming and can be performed
    ///         asynchronously.
    fn rollback(&mut self, ag: &mut AsyncGroup);

    /// Attempts to revert a previously committed transaction, used when a later commit fails.
    ///
    /// # Parameters
    /// - `ag`: An `AsyncGroup` that can be used to manage asynchronous force-back operations,
    ///         especially when the force-back process is time-consuming and can be performed
    ///         asynchronously.
    fn force_back(&mut self, ag: &mut AsyncGroup);

    /// Cleans up the connection, potentially closing or releasing resources.
    fn close(&mut self);
}

pub(crate) struct NoopDataConn {}

#[allow(dead_code)]
impl DataConn for NoopDataConn {
    fn commit(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
        Ok(())
    }
    fn is_committed(&self) -> bool {
        false
    }
    fn rollback(&mut self, _ag: &mut AsyncGroup) {}
    fn force_back(&mut self, _ag: &mut AsyncGroup) {}
    fn close(&mut self) {}
}

#[repr(C)]
pub(crate) struct DataConnContainer<C = NoopDataConn>
where
    C: DataConn + 'static,
{
    drop_fn: fn(*const DataConnContainer),
    is_fn: fn(any::TypeId) -> bool,

    commit_fn: fn(*const DataConnContainer, &mut AsyncGroup) -> Result<(), Err>,
    is_committed_fn: fn(*const DataConnContainer) -> bool,
    rollback_fn: fn(*const DataConnContainer, &mut AsyncGroup),
    force_back_fn: fn(*const DataConnContainer, &mut AsyncGroup),
    close_fn: fn(*const DataConnContainer),

    prev: *mut DataConnContainer,
    next: *mut DataConnContainer,
    name: String,
    data_conn: Box<C>,
}

impl<C> DataConnContainer<C>
where
    C: DataConn + 'static,
{
    fn new(name: &str, data_conn: Box<C>) -> Self {
        Self {
            drop_fn: drop_data_conn::<C>,
            is_fn: is_data_conn::<C>,

            commit_fn: commit_data_conn::<C>,
            is_committed_fn: is_committed_data_conn::<C>,
            rollback_fn: rollback_data_conn::<C>,
            force_back_fn: force_back_data_conn::<C>,
            close_fn: close_data_conn::<C>,

            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            name: name.to_string(),
            data_conn,
        }
    }
}

fn drop_data_conn<C>(ptr: *const DataConnContainer)
where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe {
        drop(Box::from_raw(typed_ptr));
    }
}

fn is_data_conn<C>(type_id: any::TypeId) -> bool
where
    C: DataConn + 'static,
{
    any::TypeId::of::<C>() == type_id
}

fn commit_data_conn<C>(ptr: *const DataConnContainer, ag: &mut AsyncGroup) -> Result<(), Err>
where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe { (*typed_ptr).data_conn.commit(ag) }
}

fn is_committed_data_conn<C>(ptr: *const DataConnContainer) -> bool
where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe { (*typed_ptr).data_conn.is_committed() }
}

fn rollback_data_conn<C>(ptr: *const DataConnContainer, ag: &mut AsyncGroup)
where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe { (*typed_ptr).data_conn.rollback(ag) };
}

fn force_back_data_conn<C>(ptr: *const DataConnContainer, ag: &mut AsyncGroup)
where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe { (*typed_ptr).data_conn.force_back(ag) };
}

fn close_data_conn<C>(ptr: *const DataConnContainer)
where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe { (*typed_ptr).data_conn.close() };
}

pub(crate) struct DataConnMap {
    head: *mut DataConnContainer,
    last: *mut DataConnContainer,
    map: HashMap<String, *mut DataConnContainer>,
}

impl DataConnMap {
    pub(crate) fn new() -> Self {
        Self {
            head: ptr::null_mut(),
            last: ptr::null_mut(),
            map: HashMap::new(),
        }
    }

    pub(crate) fn insert<C>(&mut self, name: &str, conn: Box<C>)
    where
        C: DataConn + 'static,
    {
        let boxed = Box::new(DataConnContainer::<C>::new(name, conn));
        let typed_ptr = Box::into_raw(boxed);
        let ptr = typed_ptr.cast::<DataConnContainer>();

        if self.last.is_null() {
            self.head = ptr;
            self.last = ptr;
        } else {
            unsafe {
                (*self.last).next = ptr;
                (*typed_ptr).prev = self.last;
            }
            self.last = ptr;
        }

        self.map.insert(name.to_string(), ptr);
    }

    pub(crate) fn get<C>(&self, name: &str) -> Option<Result<&C, Err>>
    where
        C: DataConn + 'static,
    {
        let ptr = self.map.get(name)?;

        let type_id = any::TypeId::of::<C>();
        let is_fn = unsafe { (*(*ptr)).is_fn };
        if is_fn(type_id) {
            let typed_ptr = (*ptr) as *const DataConnContainer<C>;
            return Some(Ok(unsafe { &((*typed_ptr).data_conn) }));
        }

        Some(Err(Err::new(DataConnError::FailToCastDataConn {
            name: name.to_string(),
            cast_to_type: any::type_name::<C>(),
        })))
    }

    pub(crate) fn commit(&self) -> Result<(), Err> {
        if self.head.is_null() {
            return Ok(());
        }

        let mut ag = AsyncGroup::new();
        let mut err_map = HashMap::new();

        let mut ptr = self.head;
        while !ptr.is_null() {
            let commit_fn = unsafe { (*ptr).commit_fn };
            let name = unsafe { &(*ptr).name };
            let next = unsafe { (*ptr).next };
            ag.name = name;
            if let Err(err) = commit_fn(ptr, &mut ag) {
                err_map.insert(name.to_string(), err);
                break;
            }
            ptr = next
        }

        ag.join(&mut err_map);

        if err_map.is_empty() {
            return Ok(());
        }

        Err(Err::new(DataConnError::FailToCommitDataConn {
            errors: err_map,
        }))
    }

    pub(crate) fn rollback(&self) {
        if self.head.is_null() {
            return;
        }

        let mut ag = AsyncGroup::new();

        let mut ptr = self.head;
        while !ptr.is_null() {
            let is_committed_fn = unsafe { (*ptr).is_committed_fn };
            let rollback_fn = unsafe { (*ptr).rollback_fn };
            let force_back_fn = unsafe { (*ptr).force_back_fn };
            let name = unsafe { &(*ptr).name };
            let next = unsafe { (*ptr).next };
            ag.name = name;
            if is_committed_fn(ptr) {
                force_back_fn(ptr, &mut ag);
            } else {
                rollback_fn(ptr, &mut ag);
            }
            ptr = next;
        }

        let mut err_map = HashMap::new();
        ag.join(&mut err_map);
    }

    pub(crate) fn close(&self) {
        let mut ptr = self.last;
        while !ptr.is_null() {
            let close_fn = unsafe { (*ptr).close_fn };
            let prev = unsafe { (*ptr).prev };
            close_fn(ptr);
            ptr = prev;
        }
    }
}

impl Drop for DataConnMap {
    fn drop(&mut self) {
        let mut ptr = self.last;
        while !ptr.is_null() {
            let drop_fn = unsafe { (*ptr).drop_fn };
            let prev = unsafe { (*ptr).prev };
            drop_fn(ptr);
            ptr = prev;
        }
    }
}

#[cfg(test)]
mod tests_of_data_conn {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::time;

    struct Logger {
        log_vec: Vec<String>,
    }

    impl Logger {
        fn new() -> Self {
            Self {
                log_vec: Vec::new(),
            }
        }

        fn log(&mut self, s: &str) {
            self.log_vec.push(s.to_string());
        }

        fn assert_logs(&self, logs: &[&str]) {
            assert_eq!(self.log_vec.len(), logs.len());
            for i in 0..self.log_vec.len() {
                assert_eq!(self.log_vec[i], logs[i]);
            }
        }

        fn clear(&mut self) {
            self.log_vec.clear();
        }
    }

    struct SyncDataConn {
        committed: bool,
        will_fail: bool,
        logger: Arc<Mutex<Logger>>,
    }

    impl SyncDataConn {
        fn new(logger: Arc<Mutex<Logger>>, will_fail: bool) -> Self {
            Self {
                committed: false,
                will_fail,
                logger: logger,
            }
        }
    }

    impl Drop for SyncDataConn {
        fn drop(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.log("SyncDataConn dropped");
        }
    }

    impl DataConn for SyncDataConn {
        fn commit(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
            let mut logger = self.logger.lock().unwrap();
            if self.will_fail {
                logger.log("SyncDataConn failed to commit");
                return Err(Err::new("XXX".to_string()));
            }
            self.committed = true;
            logger.log("SyncDataConn committed");
            Ok(())
        }
        fn is_committed(&self) -> bool {
            self.committed
        }
        fn rollback(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.log("SyncDataConn rollbacked");
        }
        fn force_back(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.log("SyncDataConn forced back");
        }
        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.log("SyncDataConn closed");
        }
    }

    struct AsyncDataConn {
        committed: Arc<Mutex<bool>>,
        logger: Arc<Mutex<Logger>>,
        will_fail: bool,
    }

    impl AsyncDataConn {
        fn new(logger: Arc<Mutex<Logger>>, will_fail: bool) -> Self {
            Self {
                committed: Arc::new(Mutex::new(false)),
                logger: logger,
                will_fail,
            }
        }
    }

    impl Drop for AsyncDataConn {
        fn drop(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.log("AsyncDataConn dropped");
        }
    }

    impl DataConn for AsyncDataConn {
        fn commit(&mut self, ag: &mut AsyncGroup) -> Result<(), Err> {
            let will_fail = self.will_fail;
            let logger = self.logger.clone();
            let committed = self.committed.clone();

            ag.add(async move || {
                // The `.await` must be executed outside the Mutex lock.
                let _ = time::sleep(time::Duration::from_millis(100)).await;

                if will_fail {
                    logger.lock().unwrap().log("AsyncDataConn failed to commit");
                    return Err(Err::new("YYY".to_string()));
                }

                let mut commit_flag = committed.lock().unwrap();
                *commit_flag = true;
                logger.lock().unwrap().log("AsyncDataConn committed");
                Ok(())
            });
            Ok(())
        }
        fn is_committed(&self) -> bool {
            *self.committed.lock().unwrap()
        }
        fn rollback(&mut self, ag: &mut AsyncGroup) {
            let logger = self.logger.clone();
            ag.add(async move || {
                // The `.await` must be executed outside the Mutex lock.
                let _ = time::sleep(time::Duration::from_millis(100)).await;
                logger.lock().unwrap().log("AsyncDataConn rollbacked");
                Ok(())
            });
        }
        fn force_back(&mut self, ag: &mut AsyncGroup) {
            let logger = self.logger.clone();
            ag.add(async move || {
                // The `.await` must be executed outside the Mutex lock.
                let _ = time::sleep(time::Duration::from_millis(100)).await;
                logger.lock().unwrap().log("AsyncDataConn forced back");
                Ok(())
            });
        }
        fn close(&mut self) {
            self.logger.lock().unwrap().log("AsyncDataConn closed");
        }
    }

    fn create_sync_data_conn(
        logger: Arc<Mutex<Logger>>,
        will_fail: bool,
    ) -> Result<Box<dyn DataConn>, Err> {
        Ok(Box::new(SyncDataConn::new(logger, will_fail)))
    }

    fn create_async_data_conn(
        logger: Arc<Mutex<Logger>>,
        will_fail: bool,
    ) -> Result<Box<dyn DataConn>, Err> {
        Ok(Box::new(AsyncDataConn::new(logger, will_fail)))
    }

    fn commit_and_close(logger: Arc<Mutex<Logger>>) {
        let mut data_conn_map = DataConnMap::new();

        match create_async_data_conn(logger.clone(), false) {
            Ok(conn) => {
                let ptr = Box::into_raw(conn);
                let typed_ptr = ptr as *mut AsyncDataConn;
                let boxed = unsafe { Box::from_raw(typed_ptr) };
                data_conn_map.insert("async", boxed);
            }
            Err(_) => panic!(),
        }

        match create_sync_data_conn(logger, false) {
            Ok(conn) => {
                let ptr = Box::into_raw(conn);
                let typed_ptr = ptr as *mut SyncDataConn;
                let boxed = unsafe { Box::from_raw(typed_ptr) };
                data_conn_map.insert("sync", boxed);
            }
            Err(_) => panic!(),
        }

        match data_conn_map.commit() {
            Ok(_) => {}
            Err(_) => panic!(),
        }

        data_conn_map.close();
    }

    fn rollback_and_close(logger: Arc<Mutex<Logger>>) {
        let mut data_conn_map = DataConnMap::new();

        match create_async_data_conn(logger.clone(), false) {
            Ok(conn) => {
                let ptr = Box::into_raw(conn);
                let typed_ptr = ptr as *mut AsyncDataConn;
                let boxed = unsafe { Box::from_raw(typed_ptr) };
                data_conn_map.insert("async", boxed);
            }
            Err(_) => panic!(),
        }

        match create_sync_data_conn(logger, false) {
            Ok(conn) => {
                let ptr = Box::into_raw(conn);
                let typed_ptr = ptr as *mut SyncDataConn;
                let boxed = unsafe { Box::from_raw(typed_ptr) };
                data_conn_map.insert("sync", boxed);
            }
            Err(_) => panic!(),
        }

        data_conn_map.rollback();
        data_conn_map.close();
    }

    fn force_back_and_close(logger: Arc<Mutex<Logger>>) {
        let mut data_conn_map = DataConnMap::new();

        match create_async_data_conn(logger.clone(), false) {
            Ok(conn) => {
                let ptr = Box::into_raw(conn);
                let typed_ptr = ptr as *mut AsyncDataConn;
                let boxed = unsafe { Box::from_raw(typed_ptr) };
                data_conn_map.insert("async", boxed);
            }
            Err(_) => panic!(),
        }

        match create_sync_data_conn(logger, false) {
            Ok(conn) => {
                let ptr = Box::into_raw(conn);
                let typed_ptr = ptr as *mut SyncDataConn;
                let boxed = unsafe { Box::from_raw(typed_ptr) };
                data_conn_map.insert("sync", boxed);
            }
            Err(_) => panic!(),
        }

        let _ = data_conn_map.commit().unwrap();
        data_conn_map.rollback();
        data_conn_map.close();
    }

    fn fail_to_commit_sync_and_rollback_and_close(logger: Arc<Mutex<Logger>>) {
        let mut data_conn_map = DataConnMap::new();

        match create_async_data_conn(logger.clone(), false) {
            Ok(conn) => {
                let ptr = Box::into_raw(conn);
                let typed_ptr = ptr as *mut AsyncDataConn;
                let boxed = unsafe { Box::from_raw(typed_ptr) };
                data_conn_map.insert("async", boxed);
            }
            Err(_) => panic!(),
        }

        match create_sync_data_conn(logger, true) {
            Ok(conn) => {
                let ptr = Box::into_raw(conn);
                let typed_ptr = ptr as *mut SyncDataConn;
                let boxed = unsafe { Box::from_raw(typed_ptr) };
                data_conn_map.insert("sync", boxed);
            }
            Err(_) => panic!(),
        }

        if let Err(err) = data_conn_map.commit() {
            if let Ok(r) = err.reason::<DataConnError>() {
                if let DataConnError::FailToCommitDataConn { errors } = r {
                    assert_eq!(errors.len(), 1);
                    if let Some(e) = errors.get("sync") {
                        if let Ok(rr) = e.reason::<String>() {
                            assert_eq!(rr, "XXX");
                        } else {
                            panic!();
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
        } else {
            panic!();
        }
        data_conn_map.rollback();
        data_conn_map.close();
    }

    fn fail_to_commit_async_and_rollback_and_close(logger: Arc<Mutex<Logger>>) {
        let mut data_conn_map = DataConnMap::new();

        match create_async_data_conn(logger.clone(), true) {
            Ok(conn) => {
                let ptr = Box::into_raw(conn);
                let typed_ptr = ptr as *mut AsyncDataConn;
                let boxed = unsafe { Box::from_raw(typed_ptr) };
                data_conn_map.insert("async", boxed);
            }
            Err(_) => panic!(),
        }

        match create_sync_data_conn(logger, false) {
            Ok(conn) => {
                let ptr = Box::into_raw(conn);
                let typed_ptr = ptr as *mut SyncDataConn;
                let boxed = unsafe { Box::from_raw(typed_ptr) };
                data_conn_map.insert("sync", boxed);
            }
            Err(_) => panic!(),
        }

        if let Err(err) = data_conn_map.commit() {
            if let Ok(r) = err.reason::<DataConnError>() {
                if let DataConnError::FailToCommitDataConn { errors } = r {
                    assert_eq!(errors.len(), 1);
                    if let Some(e) = errors.get("async") {
                        if let Ok(rr) = e.reason::<String>() {
                            assert_eq!(rr, "YYY");
                        } else {
                            panic!();
                        }
                    } else {
                        panic!();
                    }
                }
            } else {
                panic!();
            }
        } else {
            panic!();
        }
        data_conn_map.rollback();
        data_conn_map.close();
    }

    fn no_data_conn() {
        let data_conn_map = DataConnMap::new();
        let _ = data_conn_map.commit();
        data_conn_map.rollback();
        data_conn_map.close();
    }

    #[test]
    fn test_commit_and_close() {
        let logger = Arc::new(Mutex::new(Logger::new()));

        commit_and_close(logger.clone());
        logger.lock().unwrap().assert_logs(&[
            "SyncDataConn committed",
            "AsyncDataConn committed",
            "SyncDataConn closed",
            "AsyncDataConn closed",
            "SyncDataConn dropped",
            "AsyncDataConn dropped",
        ]);
        logger.lock().unwrap().clear();
    }

    #[test]
    fn test_rollback_and_close() {
        let logger = Arc::new(Mutex::new(Logger::new()));

        rollback_and_close(logger.clone());
        logger.lock().unwrap().assert_logs(&[
            "SyncDataConn rollbacked",
            "AsyncDataConn rollbacked",
            "SyncDataConn closed",
            "AsyncDataConn closed",
            "SyncDataConn dropped",
            "AsyncDataConn dropped",
        ]);
        logger.lock().unwrap().clear();
    }

    #[test]
    fn test_force_back_and_close() {
        let logger = Arc::new(Mutex::new(Logger::new()));

        force_back_and_close(logger.clone());
        logger.lock().unwrap().assert_logs(&[
            "SyncDataConn committed",
            "AsyncDataConn committed",
            "SyncDataConn forced back",
            "AsyncDataConn forced back",
            "SyncDataConn closed",
            "AsyncDataConn closed",
            "SyncDataConn dropped",
            "AsyncDataConn dropped",
        ]);
        logger.lock().unwrap().clear();
    }

    #[test]
    fn test_fail_to_commit_sync_and_rollback_and_close() {
        let logger = Arc::new(Mutex::new(Logger::new()));

        fail_to_commit_sync_and_rollback_and_close(logger.clone());
        logger.lock().unwrap().assert_logs(&[
            "SyncDataConn failed to commit",
            "AsyncDataConn committed",
            "SyncDataConn rollbacked",
            "AsyncDataConn forced back",
            "SyncDataConn closed",
            "AsyncDataConn closed",
            "SyncDataConn dropped",
            "AsyncDataConn dropped",
        ]);
        logger.lock().unwrap().clear();
    }

    #[test]
    fn test_fail_to_commit_async_and_rollback_and_close() {
        let logger = Arc::new(Mutex::new(Logger::new()));

        fail_to_commit_async_and_rollback_and_close(logger.clone());
        logger.lock().unwrap().assert_logs(&[
            "SyncDataConn committed",
            "AsyncDataConn failed to commit",
            "SyncDataConn forced back",
            "AsyncDataConn rollbacked",
            "SyncDataConn closed",
            "AsyncDataConn closed",
            "SyncDataConn dropped",
            "AsyncDataConn dropped",
        ]);
        logger.lock().unwrap().clear();

        no_data_conn();
    }

    #[test]
    fn test_get() {
        let logger = Arc::new(Mutex::new(Logger::new()));

        let mut data_conn_map = DataConnMap::new();

        match create_async_data_conn(logger.clone(), true) {
            Ok(conn) => {
                let ptr = Box::into_raw(conn);
                let typed_ptr = ptr as *mut AsyncDataConn;
                let boxed = unsafe { Box::from_raw(typed_ptr) };
                data_conn_map.insert("async", boxed);
            }
            Err(_) => panic!(),
        }

        match create_sync_data_conn(logger, false) {
            Ok(conn) => {
                let ptr = Box::into_raw(conn);
                let typed_ptr = ptr as *mut SyncDataConn;
                let boxed = unsafe { Box::from_raw(typed_ptr) };
                data_conn_map.insert("sync", boxed);
            }
            Err(_) => panic!(),
        }

        match data_conn_map.get::<AsyncDataConn>("async") {
            Some(result) => match result {
                Ok(_) => {}
                Err(_) => panic!(),
            },
            None => panic!(),
        }
        match data_conn_map.get::<SyncDataConn>("sync") {
            Some(result) => match result {
                Ok(_) => {}
                Err(_) => panic!(),
            },
            None => panic!(),
        }

        match data_conn_map.get::<AsyncDataConn>("sync") {
            Some(result) => match result {
                Ok(_) => panic!(),
                Err(_err) => {}
            },
            None => panic!(),
        }
        match data_conn_map.get::<SyncDataConn>("async") {
            Some(result) => match result {
                Ok(_) => panic!(),
                Err(_err) => {}
            },
            None => panic!(),
        }
        match data_conn_map.get::<AsyncDataConn>("xxx") {
            Some(_) => panic!(),
            None => {}
        }
    }
}
